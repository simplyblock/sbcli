"""Consistency groups: group-wide crash-consistent snapshots for a policy.

A replication policy created with ``consistency_group=True`` owns exactly one
auto-managed :class:`ConsistencyGroup`. Its members are the volumes attached
to the policy; they all live on ONE node/LVS (the group pins placement on the
first attach and enforces it afterwards), because the group snapshot freezes
IO per member blob on that LVS and a cross-LVS "group" would only be as
consistent as its slowest freeze.

The group snapshot itself is ONE SPDK call (``bdev_lvol_snapshot_group``):
IO on every member is parked before the first snapshot and released after the
last, so the resulting set is a single point in time across the group. SPDK
garbage-collects on mid-sequence failure (unfreeze first, then delete the
snapshots already taken), so this controller never sees half a group from a
failed RPC. What this controller owns is everything around that call:
member resolution, the monotonically increasing ``group_seq``, replica
registration, snapshot records, chain linking and replication-task enqueue —
mirroring ``snapshot_controller.add`` step for step for each member.

Membership epochs: a volume attached after the group already ticked joins at
``last_group_seq + 1`` — the first group snapshot that actually contains it.
Earlier generations do not, and a volume detached at seq M is not in
generations after M. :func:`generation_membership_warnings` computes exactly
the two warnings the fail-over path must surface when an operator selects an
older generation.
"""
import time
import uuid as uuid_module
from datetime import datetime

from simplyblock_core import constants, utils
from simplyblock_core import db_controller as db_mod
from simplyblock_core.controllers import snapshot_events, tasks_controller
from simplyblock_core.controllers.snapshot_controller import (
    _find_lvs_leader,
    _rollback_snapshot_bdev,
    lvstore_op_lock,
    object_mutation_lock,
)
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)
db = db_mod.DBController()


class ConsistencyGroupError(Exception):
    pass


# --------------------------------------------------------------------------- #
# Group lifecycle (driven by the policy controller)
# --------------------------------------------------------------------------- #

def create_group_for_policy(policy):
    """Auto-create the group record when a consistency-group policy is made."""
    group = ConsistencyGroup()
    group.uuid = str(uuid_module.uuid4())
    group.cluster_id = policy.cluster_id
    group.policy_id = policy.get_id()
    group.members = {}
    group.write_to_db(db.kv_store)
    logger.info("Created consistency group %s for policy %s",
                group.get_id(), policy.policy_name)
    return group


def delete_group_for_policy(policy_id):
    """Auto-delete with the policy (which is only removable member-free)."""
    group = db.get_consistency_group_for_policy(policy_id)
    if group is not None:
        group.remove(db.kv_store)
        logger.info("Removed consistency group %s of policy %s",
                    group.get_id(), policy_id)


def delete_group(group):
    """Delete a consistency-group record, but ONLY when it has no current
    (open-epoch) member.

    A group with live members is refused: removing it would leave its members
    pointing at a group that no longer exists and orphan its generation history.
    Once a hand-off has emptied a group (fail-over / fail-back moves every member
    to the peer), it is safe to remove -- and removing it is what lets the NEXT
    hand-off mint a FRESH group, correctly node-pinned, instead of reusing a
    stale record whose node pin no longer matches where the clones landed. Live
    2026-09-27: a peer group left pinned to a departed node from an earlier cycle
    made reconstitute_group_after_handoff refuse every clone (single-LVS pin), so
    the clones stayed ungrouped, the demote shipped nothing home, and the group
    fail-back could resolve no members.

    Raises ConsistencyGroupError when the group still has an open member.
    """
    open_members = [lid for lid, m in (group.members or {}).items()
                    if m.get("removed_seq", 0) == 0]
    if open_members:
        raise ConsistencyGroupError(
            f"consistency group {group.group_name or group.uuid[:8]} still has "
            f"{len(open_members)} member(s); detach or hand them off before "
            f"deleting it")
    group.remove(db.kv_store)
    logger.info("Deleted consistency group %s (%s)", group.uuid[:8],
                group.group_name or "-")


def ensure_group(cluster_id, name):
    """Resolve the standalone group named ``name`` in ``cluster_id``, or create
    it. Idempotent by (cluster_id, name): concurrent first volumes that carry
    the same label converge on one group rather than racing two into being
    (design §4.1). The group is unpinned until its first member joins.
    """
    if not name:
        raise ConsistencyGroupError("a consistency group needs a name")
    group = db.get_consistency_group_by_name(cluster_id, name)
    if group is not None:
        return group
    group = ConsistencyGroup()
    group.uuid = str(uuid_module.uuid4())
    group.cluster_id = cluster_id
    group.group_name = name
    group.members = {}
    group.write_to_db(db.kv_store)
    logger.info("Created standalone consistency group %s (%s) in cluster %s",
                group.uuid[:8], name, cluster_id)
    return group


def add_member_to_group(group, lvol):
    """Pin placement and record the member's epoch (design §4.1, §4.2).

    The FIRST member pins the group to its node/LVS. Every later member must
    already live there — the join FAILS otherwise; membership becomes effective
    at the NEXT group snapshot (``joined_seq = last_group_seq + 1``), because no
    earlier group snapshot contains this volume.

    A re-attaching volume gets a fresh epoch: its old history window stays
    recorded under the closed epoch semantics (the entry is replaced, and the
    generation math treats the gap correctly because the new joined_seq
    excludes the detached window).

    A group is capped at ``MAX_CONSISTENCY_GROUP_MEMBERS`` open-epoch members
    (design §4.1): the whole set is frozen for one ``bdev_lvol_snapshot_group``
    call, so an unbounded group would freeze I/O across arbitrarily many volumes.
    """
    members = dict(group.members or {})
    entry = members.get(lvol.get_id())
    already_member = entry is not None and entry.get("removed_seq", 0) == 0
    open_count = sum(1 for m in members.values() if m.get("removed_seq", 0) == 0)
    if not already_member and open_count >= constants.MAX_CONSISTENCY_GROUP_MEMBERS:
        raise ConsistencyGroupError(
            f"consistency group {group.uuid[:8]} already has the maximum "
            f"{constants.MAX_CONSISTENCY_GROUP_MEMBERS} members; "
            f"volume {lvol.get_id()} cannot join")

    # A group with live members enforces its single-LVS pin; a group with NONE
    # (emptied by a hand-off, or brand new) has no live pin and takes this member's
    # node. A pin left over from a departed cycle must never block a hand-off's
    # clones -- they can land on a different node than the previous cycle's members
    # did, and refusing them leaves them ungrouped so reconstitute_group_after_handoff
    # regroups nothing, the demote ships nothing home, and the group fail-back
    # resolves no members (live 2026-09-27: a peer group left pinned to a departed
    # node broke the whole fail-back this way).
    pin_is_live = bool(group.lvs_name) and open_count > 0
    if pin_is_live and (lvol.lvs_name != group.lvs_name
                        or lvol.node_id != group.node_id):
        raise ConsistencyGroupError(
            f"Volume {lvol.get_id()} lives on {lvol.node_id[:8]}/{lvol.lvs_name} "
            f"but consistency group {group.uuid[:8]} is pinned to "
            f"{group.node_id[:8]}/{group.lvs_name}; all members of a "
            f"consistency group must share one LVS")

    if not pin_is_live and (group.node_id != lvol.node_id
                            or group.lvs_name != lvol.lvs_name):
        if group.lvs_name:
            logger.info("Consistency group %s re-pinned from %s/%s to %s/%s "
                        "(no live members; stale pin reset)", group.uuid[:8],
                        (group.node_id or "-")[:8], group.lvs_name,
                        lvol.node_id[:8], lvol.lvs_name)
        else:
            logger.info("Consistency group %s pinned to node %s / %s by its "
                        "first member %s", group.uuid[:8], lvol.node_id[:8],
                        lvol.lvs_name, lvol.get_id())
        group.node_id = lvol.node_id
        group.lvs_name = lvol.lvs_name

    members[lvol.get_id()] = {"joined_seq": group.last_group_seq + 1,
                              "removed_seq": 0}
    group.members = members
    group.write_to_db(db.kv_store)
    logger.info("Volume %s joined consistency group %s at generation %d "
                "(effective from the next group snapshot)",
                lvol.get_id(), group.uuid[:8], group.last_group_seq + 1)
    return group


def join_new_volume(cluster_id, lvol, name):
    """Join a freshly created volume to the standalone group ``name``, creating
    the group when it does not exist (design §4.1, §7.2). The create and clone
    paths both funnel through here after the volume is usable: the placement
    pin and the member cap are enforced by :func:`add_member_to_group`, and a
    refused join raises :class:`ConsistencyGroupError` while leaving the volume
    itself intact for the caller to report.
    """
    group = ensure_group(cluster_id, name)
    add_member_to_group(group, lvol)
    lvol.group_id = group.get_id()
    lvol.write_to_db(db.kv_store)
    return group


def join_existing_volume(group, lvol):
    """Join an EXISTING volume to ``group`` (design §4.5, Phase 4 late join).

    The label path's late join: unlike :func:`join_new_volume`, which funnels
    freshly created volumes whose placement the create path just decided, the
    volume already exists somewhere, so the guards the create path gets for
    free are enforced here explicitly:

    - **Idempotent.** Joining a volume whose epoch in this group is already
      open returns the group unchanged, so a retried label reconcile converges.
    - **One-way (design §4.3).** A volume whose epoch in this group is CLOSED
      is refused: membership windows never reopen, and re-establishment is a
      labeled clone. (The legacy policy attach path replaces closed epochs;
      the label path deliberately does not — design Open Question 5.)
    - **Pool alignment (design §4.5).** Every open member must live in one
      storage pool. The group's subsystem-scoped operations (the frozen cut,
      the migration exclusion) reach every volume sharing a member's
      subsystem, and the control plane guarantees a subsystem never spans
      pools — so a single-pool group's operations can never touch another
      pool's volumes. A cross-pool join would break exactly that.
    - **Placement pin and member cap**, enforced by
      :func:`add_member_to_group` the same way the create path enforces them.
      A join never moves the volume: off the pinned node/LVS means refused.
    """
    entry = (group.members or {}).get(lvol.get_id())
    if entry is not None:
        if entry.get("removed_seq", 0) != 0:
            raise ConsistencyGroupError(
                f"volume {lvol.get_id()} left consistency group "
                f"{group.uuid[:8]} at generation {entry['removed_seq']} and "
                f"cannot rejoin: membership is one-way, re-establish it by "
                f"creating a labeled clone")
        return group

    for member_id, m in (group.members or {}).items():
        if m.get("removed_seq", 0) != 0:
            continue
        try:
            member = db.get_lvol_by_id(member_id)
        except KeyError:
            continue
        if member.pool_uuid != lvol.pool_uuid:
            raise ConsistencyGroupError(
                f"volume {lvol.get_id()} is in pool {lvol.pool_uuid[:8]} but "
                f"consistency group {group.uuid[:8]} members live in pool "
                f"{member.pool_uuid[:8]}; all members of a consistency group "
                f"must share one storage pool")

    group = add_member_to_group(group, lvol)
    lvol.group_id = group.get_id()
    lvol.write_to_db(db.kv_store)
    return group


def add_member(policy, lvol):
    """Policy path: resolve the policy's group, then join ``lvol`` to it."""
    group = db.get_consistency_group_for_policy(policy.get_id())
    if group is None:
        # Policies created before the flag existed, or records lost: fail
        # loudly rather than silently degrading to per-volume snapshots.
        raise ConsistencyGroupError(
            f"Policy {policy.policy_name} declares a consistency group but "
            f"has no group record")
    return add_member_to_group(group, lvol)


def reconstitute_group_after_handoff(source_lvol, dest_lvol, dest_cluster_id):
    """Re-form the consistency group on the destination cluster after a
    fail-over / fail-back / migration hand-off, so the group stays crash-
    consistent across it (design-csi-addons-replication.md §14.4).

    A hand-off clones each member on its own (replicate_lvol_on_target_cluster
    for fail-over, the cutover runner for fail-back/migration), leaving the
    clones ungrouped. The snapshot monitor keys group snapshots off ``group_id``
    and the driver resolves the backend group by membership, so ungrouped clones
    can be neither group-snapshotted nor promoted atomically, and the next
    hand-off degrades to per-volume. This puts each clone back into its group.

    Keyed by group NAME, not id: each cluster owns its own CG record, but both
    carry the same name (the ``storage.simplyblock.io/consistency-group`` label),
    so a fail-back RETURNS the volume to the SAME group it came from -- the
    destination cluster's existing record of that name is reused rather than a
    new one minted. ``add_member_to_group`` re-opens a closed epoch, so a volume
    whose fail-over closed its membership (its source was deleted) rejoins
    cleanly. Group members share one node/LVS (the hand-off co-locates them on
    one replication node, and delta fail-back lands on the original node), which
    satisfies ``add_member_to_group``'s single-LVS pin.

    No-op when the source is not a group member. The caller invokes this
    best-effort: a failure here must never undo the promote/cutover that already
    succeeded.
    """
    src_group_id = getattr(source_lvol, "group_id", "")
    if not src_group_id:
        return None
    try:
        src_group = db.get_consistency_group_by_id(src_group_id)
    except KeyError:
        logger.warning(
            "Group reconstitution skipped: source group %s of %s not found",
            src_group_id, dest_lvol.get_id())
        return None
    group = ensure_group(dest_cluster_id, src_group.group_name)
    add_member_to_group(group, dest_lvol)
    dest_lvol.group_id = group.get_id()
    dest_lvol.write_to_db(db.kv_store)
    logger.info(
        "Consistency group %s (%s) reconstituted on cluster %s: %s rejoined so "
        "the group stays crash-consistent across the hand-off",
        group.uuid[:8], src_group.group_name, dest_cluster_id, dest_lvol.get_id())
    return group


def _reset_generation_if_emptied(group, members):
    """When the last live member leaves, clear the group's epoch history.

    A group with no open epoch is dormant, and ``add_member_to_group`` treats it
    as re-pinnable by the next member. The closed epochs go -- they describe a
    departed cycle -- but the generation COUNTER stays: the group's last
    generation is still its replicated recovery point on both sides (the
    snapshots of it survive the members' deletion), and the next cycle's
    generations must number after it. Resetting the counter to 0 made a later
    generation 1..14 rank below the departed cycle's generation 15 for every
    "newest generation" selection, and two cycles reused the same numbers
    (2026-10-04, WordPress relocate: the source member was deleted after its
    demote, emptying the group). With no epochs left there is nothing for
    ``included_in_seq`` to misread.

    Returns the members map to persist (emptied when no live member remains).
    """
    if any(m.get("removed_seq", 0) == 0 for m in members.values()):
        return members
    if members:
        logger.info("Consistency group %s has no live members; closed epochs cleared, "
                    "generation counter kept at %d", group.uuid[:8], group.last_group_seq)
    return {}


def remove_member_from_group(group, lvol_id):
    """Close the member's epoch at the current generation (detach semantics).

    History-preserving: the member's snapshots in prior generations are
    untouched (design §8.2), only the epoch's ``removed_seq`` is set -- until the
    member is the last one out, at which point the group is reset to generation 0
    (see :func:`_reset_generation_if_emptied`).
    """
    if group is None:
        return
    members = dict(group.members or {})
    entry = members.get(lvol_id)
    if entry and entry.get("removed_seq", 0) == 0:
        if group.last_group_seq < entry.get("joined_seq", 1):
            # No generation ever contained this member, so there is no history
            # to preserve, and a closed-empty window is unrepresentable:
            # removed_seq == 0 is the open-epoch sentinel, so stamping
            # joined_seq - 1 for a first-generation member would read as still
            # open and the member would count as one forever (2026-09-11: a
            # restored group at generation 0 kept every deleted member).
            del members[lvol_id]
            group.members = _reset_generation_if_emptied(group, members)
            group.write_to_db(db.kv_store)
            logger.info("Volume %s left consistency group %s before any "
                        "generation contained it; membership entry dropped",
                        lvol_id, group.uuid[:8])
            return
        entry = dict(entry)
        entry["removed_seq"] = group.last_group_seq
        members[lvol_id] = entry
        logger.info("Volume %s left consistency group %s (included up to "
                    "generation %d)", lvol_id, group.uuid[:8], entry["removed_seq"])
        group.members = _reset_generation_if_emptied(group, members)
        group.write_to_db(db.kv_store)


def remove_member(policy_id, lvol_id):
    """Policy path: resolve the policy's group, then close ``lvol_id``'s epoch."""
    remove_member_from_group(
        db.get_consistency_group_for_policy(policy_id), lvol_id)


def detach_existing_volume(group, lvol_id):
    """Detach a member AND clear the volume's denormalized group pointer.

    The label path's counterpart of :func:`join_existing_volume` (design
    §4.5): the epoch closes one-way via :func:`remove_member_from_group`
    (history-preserving, §8.2), and ``lvol.group_id`` is cleared so the volume
    reads as a non-member (the migration webhook, §9.5, keys off it). The
    group's members map keeps the closed epoch for generation history.
    Idempotent: detaching a non-member is a no-op, and a missing volume record
    only skips the pointer cleanup.
    """
    remove_member_from_group(group, lvol_id)
    try:
        volume = db.get_lvol_by_id(lvol_id)
    except KeyError:
        return
    if volume.group_id:
        volume.group_id = ""
        volume.write_to_db(db.kv_store)


def group_for_lvol(lvol_id):
    """The consistency group this lvol is an open-epoch member of, or None.

    Membership lives in the group's ``members`` map rather than on the lvol, so
    this scans the cluster's groups. Used by the volume delete and read paths
    (design §8.2, §9.5).
    """
    for g in db.get_consistency_groups():
        entry = (g.members or {}).get(lvol_id)
        if entry and entry.get("removed_seq", 0) == 0:
            return g
    return None


def pinned_node_for_group(group):
    """The node a NEW member of ``group`` must be created on, or None."""
    if group is not None and group.node_id:
        return group.node_id
    return None


def pinned_node_for_policy(policy):
    """The node a NEW volume under this policy must be created on, or None."""
    return pinned_node_for_group(
        db.get_consistency_group_for_policy(policy.get_id()))


# --------------------------------------------------------------------------- #
# Generation membership warnings (requirement 4) — pure logic
# --------------------------------------------------------------------------- #

def generation_membership_warnings(group, seq):
    """The two warnings an operator must see when failing over to ``seq``.

    Returns a list of strings:
      * one for current members NOT included in that generation (late
        joiners whose ``joined_seq`` is newer than ``seq``);
      * one for volumes included in that generation that are NO LONGER
        members (their epoch covers ``seq`` but ``removed_seq`` is set).
    Empty list when the generation matches current membership exactly.
    """
    if group is None or not seq:
        return []
    missing = []
    stale = []
    for lvol_id, m in (group.members or {}).items():
        joined = m.get("joined_seq", 1)
        removed = m.get("removed_seq", 0)
        is_current = removed == 0
        included = joined <= seq and (removed == 0 or seq <= removed)
        if is_current and not included:
            missing.append(lvol_id)
        if not is_current and included:
            stale.append(lvol_id)
    warnings = []
    if missing:
        warnings.append(
            "generation %d predates %d current group member(s); NOT included "
            "in this point-in-time: %s" % (seq, len(missing), ", ".join(sorted(missing))))
    if stale:
        warnings.append(
            "generation %d includes %d volume(s) that are no longer group "
            "members: %s" % (seq, len(stale), ", ".join(sorted(stale))))
    return warnings


def warnings_for_snapshot(lvol, snapshot):
    """Convenience for the fail-over path: warnings for the generation the
    chosen snapshot belongs to, [] for non-group snapshots/policies."""
    seq = getattr(snapshot, "group_seq", 0)
    group_id = getattr(snapshot, "group_id", "")
    if not seq or not group_id:
        return []
    try:
        group = db.get_consistency_group_by_id(group_id)
    except KeyError:
        return []
    return generation_membership_warnings(group, seq)


# --------------------------------------------------------------------------- #
# Group replication status roll-up (design-csi-addons-replication.md §14.4/§14.6)
# --------------------------------------------------------------------------- #

# Severity order for rolling a group's health up to its worst member: a group is
# only as healthy as its sickest member. An unknown state ranks worst (5), so a
# state the roll-up does not recognize is never silently treated as healthy.
_GROUP_STATE_SEVERITY = {
    "error": 5,
    "degraded": 4,
    "not_replicating": 3,
    "lagging": 2,
    "replicating": 1,
    "in_sync": 0,
}


def aggregate_group_replication_info(member_infos):
    """Roll a consistency group's per-member replication status up to one group
    verdict: the group's recovery point is its OLDEST member's, its lag and
    health are its WORST member's, and its backlog is the sum, because a group is
    only as protected as its slowest, sickest member. A member with no recovery
    point leaves the whole group without one. Roles report ``none`` unless every
    member agrees, since a healthy group's members share a role.

    Pure function over the dicts ``lvol_controller.get_replication_info`` returns
    (design-csi-addons-replication.md §14.4/§14.6).
    """
    if not member_infos:
        return {
            "member_count": 0,
            "role": "none",
            "state": "not_replicating",
            "last_replicated_at": None,
            "lag_seconds": None,
            "outstanding_count": 0,
            "outstanding_bytes": 0,
            "resyncing": False,
        }

    roles = {info.get("role", "none") for info in member_infos}
    role = roles.pop() if len(roles) == 1 else "none"

    state = max((info.get("state", "not_replicating") for info in member_infos),
                key=lambda s: _GROUP_STATE_SEVERITY.get(s, 5))

    lasts = [info.get("last_replicated_at") for info in member_infos]
    if any(value is None for value in lasts):
        last_replicated_at = None
        lag_seconds = None
    else:
        last_replicated_at = min(lasts)
        lags = [info.get("lag_seconds") for info in member_infos if info.get("lag_seconds") is not None]
        lag_seconds = max(lags) if lags else None

    return {
        "member_count": len(member_infos),
        "role": role,
        "state": state,
        "last_replicated_at": last_replicated_at,
        "lag_seconds": lag_seconds,
        "outstanding_count": sum(int(info.get("outstanding_count", 0) or 0) for info in member_infos),
        "outstanding_bytes": sum(int(info.get("outstanding_bytes", 0) or 0) for info in member_infos),
        "resyncing": any(bool(info.get("resyncing", False)) for info in member_infos),
    }


def attach_group_policy(group, policy_id):
    """Attach a standalone consistency group to a group replication policy so the
    whole group replicates as one unit (design-csi-addons-replication.md §14.4).

    The members already belong to the group -- they joined at provisioning by the
    ``storage.simplyblock.io/consistency-group`` label -- so this links the group
    to the policy and starts each OPEN member replicating. It never re-adds a
    member: ``add_member_to_group`` stamps a fresh ``joined_seq``, which would
    tear the group's snapshot-generation history. Any replication policy works:
    membership is what makes the group crash-consistent, so the snapshot monitor
    ships one GROUP snapshot per interval for its members regardless of a policy
    flag.
    """
    from simplyblock_core.controllers import replication_policy_controller
    try:
        pol = db.get_replication_policy_by_id(policy_id)
    except KeyError:
        pol = None
    if pol is None:
        raise ConsistencyGroupError(f"replication policy {policy_id} not found")
    target = db.get_replication_target_by_id(pol.target_id)

    group = db.get_consistency_group_by_id(group.get_id())
    group.policy_id = pol.get_id()
    group.write_to_db(db.kv_store)

    open_members = [m for m in list_members(group) if not m.get("removed_seq")]
    for member in open_members:
        replication_policy_controller.start_member_replication(
            member["lvol_id"], pol, target)
    logger.info("Consistency group %s attached to replication policy %s "
                "(%d member(s) replicating)", group.uuid[:8], pol.policy_name,
                len(open_members))
    return group


def _policy_family(name):
    """The part of a policy name that is the same in both directions: the name
    before its final ``-to-<site>`` (dr-hub derives ``sb-dr-<plan>-<method>-to-
    <site>`` per direction), else the whole name."""
    head, sep, _ = (name or "").rpartition("-to-")
    return head if sep else (name or "")


def reverse_replication_policy(cluster_id, toward_cluster_id, former_policy_name=""):
    """The active replication policy on *cluster_id* whose target is
    *toward_cluster_id*: the policy a group promoted onto *cluster_id* must
    follow so its data replicates back to where it came from. Among several,
    the one of the same family as the group's former policy wins (the same plan
    and method, the other direction), then the oldest by name for a stable pick.
    None when the cluster has no such policy."""
    candidates = []
    for pol in db.get_replication_policies(cluster_id):
        if getattr(pol, "status", "active") != "active":
            continue
        try:
            target = db.get_replication_target_by_id(pol.target_id)
        except KeyError:
            continue
        if getattr(target, "status", "active") != "active":
            continue
        if target.target_cluster_id == toward_cluster_id:
            candidates.append(pol)
    if not candidates:
        return None
    family = _policy_family(former_policy_name)
    candidates.sort(key=lambda pol: (not family or _policy_family(pol.policy_name) != family,
                                     pol.policy_name))
    return candidates[0]


def attach_reverse_replication(group, toward_cluster_id, former_policy_name=""):
    """After *group* was promoted onto its cluster (a fail-over from the
    replicated copy, or a fail-back), attach it to that cluster's replication
    policy toward *toward_cluster_id*, so its members replicate back and the
    next group snapshot ships there. The per-volume fail-over sets up the same
    reverse replication; csi-addons does not call Enable again after a promote,
    so without this the promoted group replicated nowhere and Ramen waited for
    a first sync forever (2026-10-04, WordPress on site B).

    Idempotent: a group already following a policy is left as it is. Never
    raises: the promote already succeeded, so a missing policy or a failure to
    start a member is logged, not propagated. Returns the policy attached, or
    None."""
    try:
        group = db.get_consistency_group_by_id(group.get_id())
    except KeyError:
        return None
    if getattr(group, "policy_id", ""):
        return None
    pol = reverse_replication_policy(group.cluster_id, toward_cluster_id, former_policy_name)
    if pol is None:
        logger.warning("Consistency group %s on %s promoted, but the cluster has no "
                       "active replication policy toward %s: it does not replicate back",
                       group.group_name, group.cluster_id, toward_cluster_id)
        return None
    try:
        attach_group_policy(group, pol.get_id())
    except Exception as e:                          # noqa: BLE001 -- the promote stands
        logger.error("Consistency group %s promoted, but attaching it to %s failed: %s",
                     group.group_name, pol.policy_name, e)
        return None
    logger.info("Consistency group %s replicates back toward %s under policy %s",
                group.group_name, toward_cluster_id, pol.policy_name)
    return pol


def detach_group_policy(group):
    """Disable group replication: stop every member replicating and unlink the
    group from its policy, WITHOUT dissolving the group. The members stay grouped
    by their label (design-csi-addons-replication.md §14.4). Idempotent: a group
    following no policy still stops each member (a no-op per member) and clears
    the pointer.
    """
    from simplyblock_core.controllers import replication_policy_controller
    group = db.get_consistency_group_by_id(group.get_id())
    for member in list_members(group):
        if member.get("removed_seq"):
            continue
        replication_policy_controller.stop_member_replication(member["lvol_id"])
    group.policy_id = ""
    group.write_to_db(db.kv_store)
    logger.info("Consistency group %s detached from its replication policy",
                group.uuid[:8])
    return group


def aggregate_group_demote(member_results):
    """Roll each member's demote result up to one group verdict (design
    §14.4). ``member_results`` is a list of ``(lvol_id, result)`` where result is
    ``demote_lvol``'s return: a dict ``{"demoted": bool, ...}`` or a
    ``(False, error)`` tuple. The group is demoted only when EVERY member is;
    a member that hard-errors makes the whole group's demote an error. Pure
    function.
    """
    members = []
    error = None
    all_demoted = True
    for lvol_id, result in member_results:
        if isinstance(result, tuple):        # (False, error)
            all_demoted = False
            error = error or f"{lvol_id}: {result[1]}"
            members.append({"lvol_id": lvol_id, "demoted": False, "error": str(result[1])})
            continue
        demoted = bool(result.get("demoted"))
        all_demoted = all_demoted and demoted
        members.append({"lvol_id": lvol_id, "demoted": demoted})
    return {"demoted": all_demoted and error is None, "members": members, "error": error}


def _members_are_superseded_source(members):
    """True when every member is the SOURCE side of a failed-over relationship --
    the recovered old primary after an unplanned failover. Such a source is
    superseded (the peer's clone already holds every post-failover write), so it
    demotes by fencing alone, with nothing to ship.

    The discriminator is the relationship state alone: being the SOURCE of a
    FAILED_OVER relationship. do_replicate is deliberately NOT required to be
    clear -- an UNPLANNED failover shuts the source's data plane down, so
    replication_stop never runs to clear it, and the recovered old primary comes
    back with do_replicate still True. Gating on do_replicate=False missed exactly
    that case, so demote_group fell to the ship-home path and never converged
    ("group demote is still converging" indefinitely, 2026-10-01). A
    planned-relocate source -- the live-pipe case that gate was protecting -- is
    never FAILED_OVER, so the state check already excludes it."""
    from simplyblock_core.controllers import lvol_controller
    from simplyblock_core.models.lvol_model import LVolReplication
    for m in members:
        rep = lvol_controller._replication_for_lvol(db, m.get_id())
        if rep is None or rep.state != LVolReplication.STATE_FAILED_OVER:
            return False
        if not (rep.source_lvol and rep.source_lvol.get_id() == m.get_id()):
            return False
    return True


def demote_group(group):
    """Demote the whole consistency group as ONE crash-consistent unit
    (design-csi-addons-replication.md §14.4).

    The demote generation is a SINGLE ``bdev_lvol_snapshot_group`` -- one atomic
    cut across every member, carrying one ``group_seq`` -- NOT a per-member
    snapshot each. That is what lets the peer's promote clone the whole group
    from one common generation; per-member demote snapshots share no group_seq
    and the peer's group-generation resolution would find no common cut.

    Re-drivable, not queued:
      * First call, ship-home (the current primary demoting for a relocate):
        fence every member, configure each member's fail-back so the reverse pipe
        exists, then take ONE group snapshot and track each member's slice of it.
      * First call, superseded source (the recovered old primary): nothing to
        ship -- demote_lvol fences each and completes at once.
      * Later calls: report done once every member's slice of the demote
        generation has replicated to the peer. This path deliberately does NOT
        reuse demote_lvol's per-member wait, whose retrigger branch would take a
        fresh single-volume snapshot and break the group cut.
    """
    from simplyblock_core.controllers import (
        lvol_controller,
        replication_policy_controller,
    )
    from simplyblock_core.models.lvol_model import LVolReplication
    from simplyblock_core.services import replication_final_step

    group = db.get_consistency_group_by_id(group.get_id())
    members = []
    for m in list_members(group):
        try:
            members.append(db.get_lvol_by_id(m["lvol_id"]))
        except KeyError:
            continue

    # Relocate fail-back: the demote is issued against THIS (origin) group via its
    # origin-pinned handle (cg:<origin>:<id>), but after a fail-over the current
    # primary -- the data that must actually be demoted and shipped home -- lives
    # in the PEER group. So when this group holds no shippable members of its own
    # (empty, or only superseded sources whose writes already moved to the peer),
    # resolve the peer group and demote ITS live clones instead: point each clone's
    # reverse pipe home and take the demote cut there. This is the demote analog of
    # _failback_group (the promote) and the per-volume resolveToLocalReplica the
    # driver runs -- DemoteGroup does neither, so without this the relocate demote
    # no-ops the origin and the fail-back promote loops on "the demote cut has not
    # finished shipping" (live 2026-09-28).
    if not members or _members_are_superseded_source(members):
        try:
            policy = (db.get_replication_policy_by_id(group.policy_id)
                      if group.policy_id else None)
        except KeyError:
            policy = None
        if policy is not None:
            peer = replication_policy_controller._resolve_active_peer_group(group, policy)
            if peer is not None:
                peer_members = []
                for m in list_members(peer):
                    try:
                        peer_members.append(db.get_lvol_by_id(m["lvol_id"]))
                    except KeyError:
                        continue
                if peer_members and not _members_are_superseded_source(peer_members):
                    logger.info("Group demote of %s resolved to peer group %s on "
                                "cluster %s: demoting its %d live member(s) so the "
                                "demote cut ships home",
                                group.group_name, peer.uuid[:8], peer.cluster_id,
                                len(peer_members))
                    group, members = peer, peer_members

    if not members:
        return {"demoted": True, "members": []}

    started = any(m.replication_demote_state in
                  (LVol.REPLICATION_DEMOTE_PENDING, LVol.REPLICATION_DEMOTE_DONE)
                  for m in members)

    if not started:
        # Superseded source: nothing to ship, demote_lvol fences + marks done.
        if _members_are_superseded_source(members):
            results = [(m.get_id(), lvol_controller.demote_lvol(m.get_id()))
                       for m in members]
            return aggregate_group_demote(results)

        # Ship-home: fence every member and point its reverse pipe home BEFORE
        # the group snapshot, so the one generation we take actually replicates.
        for m in members:
            try:
                node = db.get_storage_node_by_id(m.node_id)
                replication_final_step.fence_source_paths(
                    node, node.lvstore, m.nqn, m.ns_id)
            except Exception as e:
                logger.warning("Demote fence of %s failed: %s", m.get_id(), e)
            if not m.do_replicate:
                rep = lvol_controller._replication_for_lvol(db, m.get_id())
                if (rep is not None and rep.state == LVolReplication.STATE_FAILED_OVER
                        and rep.target_lvol and rep.target_lvol.get_id() == m.get_id()):
                    lvol_controller.replication_failback(m.get_id())

        group = db.get_consistency_group_by_id(group.get_id())
        created_ids, err = create_group_snapshot_for_group(group)
        if err:
            return {"demoted": False, "error": err,
                    "members": [{"lvol_id": m.get_id(), "demoted": False, "error": err}
                                for m in members]}

        by_lvol = {}
        for sid in created_ids or []:
            try:
                s = db.get_snapshot_by_id(sid)
            except KeyError:
                continue
            if s.lvol:
                by_lvol[s.lvol.get_id()] = sid
        for m in members:
            sid = by_lvol.get(m.get_id())
            if not sid:
                continue
            m = db.get_lvol_by_id(m.get_id())
            m.replication_demote_snapshot_id = sid
            m.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
            m.write_to_db(db.kv_store)
        return {"demoted": False,
                "members": [{"lvol_id": m.get_id(), "demoted": False} for m in members]}

    # Already taken: report done once every member's slice of the demote
    # generation has replicated. No retrigger -- the group cut is fixed.
    member_status = []
    all_done = True
    for m in members:
        if m.replication_demote_state == LVol.REPLICATION_DEMOTE_DONE:
            member_status.append({"lvol_id": m.get_id(), "demoted": True})
            continue
        replicated = False
        try:
            snap = db.get_snapshot_by_id(m.replication_demote_snapshot_id)
            replicated = bool(snap.target_replicated_snap_uuid)
        except KeyError:
            replicated = False
        if replicated:
            m.replication_demote_state = LVol.REPLICATION_DEMOTE_DONE
            m.write_to_db(db.kv_store)
            member_status.append({"lvol_id": m.get_id(), "demoted": True})
        else:
            all_done = False
            member_status.append({"lvol_id": m.get_id(), "demoted": False})
    return {"demoted": all_done, "members": member_status}


def failback_group(group, source_cluster_id=None):
    """Fail the whole group back: point every member's replication back at the
    source cluster (design-csi-addons-replication.md §14.4). The cutover itself is
    each member's own commit, as at the per-volume level. Returns ``configured:
    False`` with per-member detail if any member could not be configured.
    """
    from simplyblock_core.controllers import lvol_controller
    members = []
    configured = True
    for member in list_members(group):
        lvol_id = member["lvol_id"]
        result = lvol_controller.replication_failback(lvol_id, source_cluster_id=source_cluster_id)
        if isinstance(result, tuple) or not result:
            configured = False
            detail = str(result[1]) if isinstance(result, tuple) else "failed to configure fail-back"
            members.append({"lvol_id": lvol_id, "configured": False, "error": detail})
        else:
            members.append({"lvol_id": lvol_id, "configured": True})
    return {"configured": configured, "members": members}


# --------------------------------------------------------------------------- #
# The group snapshot tick
# --------------------------------------------------------------------------- #

def _precheck_members(group):
    """Health precheck before the freeze (design §5.1).

    Returns (members, None) when every current member is online and on the
    pinned store, or (None, error) naming the members at fault. Prechecking
    beats leaning on the all-or-nothing rollback: it avoids freezing I/O across
    the healthy members for a snapshot one unhealthy member had already doomed,
    and it produces a precise error rather than a mid-sequence RPC failure.
    """
    members = []
    offline = []
    off_store = []
    for lvol_id, m in (group.members or {}).items():
        if m.get("removed_seq", 0) != 0:
            continue
        try:
            lvol = db.get_lvol_by_id(lvol_id)
        except KeyError:
            offline.append(f"{lvol_id[:8]} (no record)")
            continue
        if lvol.status != LVol.STATUS_ONLINE:
            offline.append(f"{lvol_id[:8]} ({lvol.status})")
            continue
        if lvol.lvs_name != group.lvs_name or lvol.node_id != group.node_id:
            off_store.append(f"{lvol_id[:8]} on {lvol.node_id[:8]}/{lvol.lvs_name}")
            continue
        members.append(lvol)
    if offline or off_store:
        parts = []
        if offline:
            parts.append("unhealthy member(s): " + ", ".join(sorted(offline)))
        if off_store:
            parts.append(
                f"member(s) off the pinned store {group.node_id[:8]}/"
                f"{group.lvs_name}: " + ", ".join(sorted(off_store)))
        return None, ("consistency group snapshot refused — " + "; ".join(parts))
    return members, None


def create_group_snapshot(policy_id, snap_type=SnapShot.TYPE_INTERNAL, lock=True):
    """Policy path: resolve the policy's group and snapshot it as one generation."""
    policy = db.get_replication_policy_by_id(policy_id)
    group = db.get_consistency_group_for_policy(policy.get_id())
    if group is None:
        return None, f"Policy {policy_id} has no consistency group"
    return create_group_snapshot_for_group(group, snap_type=snap_type, lock=lock)


def create_group_snapshot_for_group(group, snap_type=SnapShot.TYPE_INTERNAL, lock=True):
    """Take ONE crash-consistent snapshot of every group member.

    Returns (list_of_snapshot_ids, None) or (None, error). All-or-nothing:
    a failure anywhere rolls back every snapshot bdev of this tick (SPDK
    already GC'd if the failure was inside the RPC; registration/record
    failures are rolled back here) and the generation counter does not move.
    """
    members, precheck_err = _precheck_members(group)
    if precheck_err is not None:
        return None, precheck_err
    if not members:
        return None, "consistency group has no members"

    host_node = db.get_storage_node_by_id(group.node_id)
    pool = db.get_pool_by_id(members[0].pool_uuid)
    cluster = db.get_cluster_by_id(pool.cluster_id)

    # Leader + HA member set, same as the single-snapshot path.
    secondary_ids = [host_node.secondary_node_id]
    if host_node.tertiary_node_id:
        secondary_ids.append(host_node.tertiary_node_id)
    all_nodes = [host_node]
    for sid in secondary_ids:
        if not sid:
            continue
        try:
            all_nodes.append(db.get_storage_node_by_id(sid))
        except KeyError:
            pass
    primary_node = _find_lvs_leader(pool.cluster_id, group.lvs_name, all_nodes)
    if not primary_node:
        return None, (f"No leader available for LVS {group.lvs_name} — "
                      f"rejecting the group snapshot until leadership is "
                      f"re-established")
    secondary_nodes = [n for n in all_nodes
                       if n.get_id() != primary_node.get_id()
                       and n.status == StorageNode.STATUS_ONLINE]

    # Serialize concurrent takes on THIS group end to end. Without it, two
    # group snapshots taken with a near-zero gap both read the same
    # last_group_seq and stamp the same group_seq, so one generation number
    # holds two generations of member snapshots and the next is skipped
    # (observed live: seq 11 held 39 member snapshots, seq 12 never existed).
    # object_mutation_lock, keyed on the group, is the same outer lock the
    # single-volume path holds per object; the inner lvstore_op_lock below is
    # taken per single-node RPC inside it. Re-read the group inside the lock
    # so a take that just released it is seen.
    with object_mutation_lock(pool.cluster_id, group.get_id(), enabled=lock):
        group = db.get_consistency_group_by_id(group.get_id())
        group_seq = group.last_group_seq + 1
        now_ts = int(time.time())
        plan = []
        for lvol in members:
            snap_vuid = utils.get_random_snapshot_vuid()
            plan.append({
                "lvol": lvol,
                "vuid": snap_vuid,
                "snap_bdev_name": f"SNAP_{snap_vuid}",
                "snap_name": f"repl_cg_{group.uuid[:8]}_{group_seq}_{lvol.get_id()[:8]}_{now_ts}",
            })

        rpc_client = primary_node.rpc_client()
        logger.info("Consistency group %s: taking generation %d over %d member(s) "
                    "on %s/%s", group.uuid[:8], group_seq, len(plan),
                    primary_node.get_id()[:8], group.lvs_name)

        # ONE lvstore mutation: the whole frozen window is a single RPC.
        with lvstore_op_lock(pool.cluster_id, group.lvs_name,
                             node_id=primary_node.get_id(), enabled=lock):
            ret = rpc_client.bdev_lvol_snapshot_group(
                group.lvs_name,
                [{"lvol_name": f"{p['lvol'].lvs_name}/{p['lvol'].lvol_bdev}",
                  "snapshot_name": p["snap_bdev_name"]} for p in plan])
        if not ret:
            # SPDK unfroze first and garbage-collected the partial snapshots.
            return None, (f"Group snapshot RPC failed on {primary_node.get_id()}; "
                          f"SPDK rolled the partial group back")

        # Bound outside the closure: mypy does not carry the ``group is None``
        # guard's narrowing into nested functions.
        group_lvs_name = group.lvs_name

        def _rollback_all():
            for p in plan:
                _rollback_snapshot_bdev(pool.cluster_id, group_lvs_name,
                                        primary_node, p["snap_bdev_name"],
                                        all_nodes, lock=lock)

        # Everything below mirrors snapshot_controller.add's tail per member:
        # read back uuid/blobid, register on the HA peers, then the record.
        created_ids: list = []
        for p in plan:
            lvol = p["lvol"]
            snap_bdev = rpc_client.bdev_get(f"{group.lvs_name}/{p['snap_bdev_name']}")
            if not snap_bdev:
                _rollback_all()
                return None, (f"group snapshot {p['snap_bdev_name']} not readable "
                              f"after creation")
            p["snap_uuid"] = snap_bdev["uuid"]
            p["blobid"] = snap_bdev["driver_specific"]["lvol"]["blobid"]
            num_allocated = snap_bdev["driver_specific"]["lvol"]["num_allocated_clusters"]
            p["used_size"] = int(num_allocated * cluster.page_size_in_blocks)

            for sec in secondary_nodes:
                from simplyblock_core.storage_node_ops import (
                    queue_for_restart_drain,
                    wait_or_delay_for_restart_gate,
                )
                gate = wait_or_delay_for_restart_gate(sec.get_id(), group.lvs_name)
                if gate == "delay":
                    queue_for_restart_drain(
                        sec.get_id(), group.lvs_name,
                        lambda s=sec, pp=p, lv=lvol: s.rpc_client().bdev_lvol_snapshot_register(
                            f"{group.lvs_name}/{lv.lvol_bdev}", pp["snap_bdev_name"],
                            pp["snap_uuid"], pp["blobid"]),
                        f"register group snapshot {p['snap_bdev_name']} on {sec.get_id()[:8]}")
                    continue
                with lvstore_op_lock(pool.cluster_id, group.lvs_name,
                                     node_id=sec.get_id(), enabled=lock):
                    try:
                        reg = sec.rpc_client().bdev_lvol_snapshot_register(
                            f"{group.lvs_name}/{lvol.lvol_bdev}", p["snap_bdev_name"],
                            p["snap_uuid"], p["blobid"])
                    except RPCException:
                        reg = None
                if not reg:
                    logger.error("Group snapshot register of %s failed on %s; "
                                 "rolling the WHOLE generation back",
                                 p["snap_bdev_name"], sec.get_id())
                    _rollback_all()
                    for snap_id in created_ids:
                        try:
                            rec = db.get_snapshot_by_id(snap_id)
                            rec.remove(db.kv_store)
                        except Exception as e:
                            logger.warning(
                                "Best-effort rollback cleanup failed for snapshot %s: %s",
                                snap_id, e)
                    return None, f"Failed to register group snapshot on {sec.get_id()}"

            snap = SnapShot()
            snap.uuid = str(uuid_module.uuid4())
            snap.data_uuid = str(uuid_module.uuid4())
            snap.snap_uuid = p["snap_uuid"]
            snap.size = lvol.size
            snap.used_size = p["used_size"]
            snap.blobid = p["blobid"]
            snap.pool_uuid = pool.get_id()
            snap.cluster_id = pool.cluster_id
            snap.snap_name = p["snap_name"]
            snap.snap_bdev = f"{group.lvs_name}/{p['snap_bdev_name']}"
            snap.created_at = now_ts
            snap.lvol = lvol
            snap.fabric = lvol.fabric
            snap.vuid = p["vuid"]
            snap.status = SnapShot.STATUS_ONLINE
            snap.snap_type = snap_type
            snap.group_id = group.get_id()
            snap.group_seq = group_seq
            snap.create_dt = str(datetime.now())
            snap.write_to_db(db.kv_store)

            prev = db.get_lvol_latest_snapshot(lvol.get_id(), exclude_uuid=snap.get_id())
            if prev is not None and not prev.next_snap_uuid:
                prev.next_snap_uuid = snap.get_id()
                snap.prev_snap_uuid = prev.get_id()
                prev.write_to_db()
                snap.write_to_db()

            snapshot_events.snapshot_create(snap)
            created_ids.append(snap.get_id())
            p["snap_id"] = snap.get_id()

        # The generation exists in full: bump the counter, then enqueue the
        # per-member replication tasks (transfer machinery is per-snapshot).
        group.last_group_seq = group_seq
        group.write_to_db(db.kv_store)

        for p in plan:
            lvol = p["lvol"]
            if lvol.do_replicate:
                task = tasks_controller.add_snapshot_replication_task(
                    pool.cluster_id, lvol.node_id, p["snap_id"])
                if task:
                    snap = db.get_snapshot_by_id(p["snap_id"])
                    snapshot_events.replication_task_created(snap)

        logger.info("Consistency group %s: generation %d complete (%d snapshots)",
                    group.uuid[:8], group_seq, len(created_ids))
        return created_ids, None


# --------------------------------------------------------------------------- #
# Group-scoped listings, generation delete, and headless clone (design §6, §7)
# --------------------------------------------------------------------------- #

def _group_snapshots(group):
    """Every member snapshot that belongs to this group, any generation."""
    return [s for s in db.get_snapshots(group.cluster_id)
            if s.group_id == group.get_id() and s.group_seq]


def list_members(group):
    """Current members with epoch, placement, and online status (design §10).

    The live-membership counterpart of :func:`list_generations`: the volumes a
    NEW group snapshot would contain, versus which members a past one does.
    """
    rows = []
    for lvol_id, m in (group.members or {}).items():
        if m.get("removed_seq", 0) != 0:
            continue
        try:
            lvol = db.get_lvol_by_id(lvol_id)
            online, node_id, lvs_name = (
                lvol.status == LVol.STATUS_ONLINE, lvol.node_id, lvol.lvs_name)
        except KeyError:
            online, node_id, lvs_name = False, "", ""
        rows.append({
            "lvol_id": lvol_id,
            "joined_seq": m.get("joined_seq", 0),
            "removed_seq": m.get("removed_seq", 0),
            "node_id": node_id or group.node_id,
            "lvs_name": lvs_name or group.lvs_name,
            "online": online,
        })
    return rows


def list_generations(group):
    """Historical per-generation view (design §6.3): one row per ``group_seq``.

    ``expected`` is computed from the membership epochs (``included_in_seq``
    over the membership at that generation), ``present`` from the member
    snapshots that still exist online. A generation whose present count is below
    expected is incomplete (a member snapshot was pruned or hard-deleted, §8)
    and is reported as such rather than discovered at restore time.
    """
    by_seq: dict = {}
    for s in _group_snapshots(group):
        by_seq.setdefault(s.group_seq, []).append(s)
    rows = []
    for seq in sorted(by_seq):
        member_snaps = by_seq[seq]
        expected = sum(1 for lvol_id in (group.members or {})
                       if group.included_in_seq(lvol_id, seq))
        present = sum(1 for s in member_snaps
                      if s.status == SnapShot.STATUS_ONLINE)
        rows.append({
            "group_seq": seq,
            "created_at": min((s.created_at for s in member_snaps), default=0),
            "expected": expected,
            "present": present,
            "complete": expected > 0 and present >= expected,
            "members": [{
                "lvol_id": s.lvol.get_id() if s.lvol else "",
                "snapshot_id": s.get_id(),
                "ready": s.status == SnapShot.STATUS_ONLINE,
            } for s in member_snaps],
        })
    return rows


def delete_generation(group, seq):
    """Delete every member snapshot of generation ``seq`` atomically; never the
    group itself (design §10). Returns (deleted_ids, None) or (None, error).
    """
    from simplyblock_core.controllers import (
        replication_recovery_points,
        snapshot_controller,
    )
    targets = [s for s in _group_snapshots(group) if s.group_seq == seq]
    if not targets:
        return None, f"generation {seq} of group {group.uuid[:8]} not found"
    newest_seq, _, _ = replication_recovery_points.newest_group_generation(group.get_id(), db=db)
    if newest_seq and seq == newest_seq:
        # The group's newest replicated generation is its restore point on both
        # sides: what a promote on the peer, or a relocate back after the demoted
        # sources were deleted, clones the group from (2026-10-04).
        return None, (f"generation {seq} is the newest replicated generation of group "
                      f"{group.uuid[:8]} -- its recovery point; it can be deleted once a "
                      f"newer generation has replicated")
    deleted: list = []
    for s in targets:
        if not snapshot_controller.delete(s.get_id()):
            return None, (f"failed to delete member snapshot {s.get_id()} of "
                          f"generation {seq}; {len(deleted)} already removed")
        deleted.append(s.get_id())
    logger.info("Consistency group %s: deleted generation %d (%d snapshots)",
                group.uuid[:8], seq, len(deleted))
    return deleted, None


def clone_generation(group, seq, into_name=None):
    """Headless group clone (design §7.3): clone every member snapshot of a
    generation into a new volume, optionally forming a new group from the
    clones. A group-forming clone must place every clone on one node/LVS
    (design §7.2), so an off-store clone fails loudly through
    :func:`add_member_to_group`. Returns (new_lvol_ids, None) or (None, error).
    """
    from simplyblock_core.controllers import snapshot_controller
    targets = [s for s in _group_snapshots(group) if s.group_seq == seq]
    if not targets:
        return None, f"generation {seq} of group {group.uuid[:8]} not found"
    new_group = ensure_group(group.cluster_id, into_name) if into_name else None
    created: list = []
    for s in targets:
        src_lvol_id = s.lvol.get_id() if s.lvol else ""
        clone_name = (f"{into_name}_{src_lvol_id[:8]}" if into_name
                      else f"clone_{group.group_name or group.uuid[:8]}_{seq}_{src_lvol_id[:8]}")
        new_id, err = snapshot_controller.clone(s.get_id(), clone_name)
        if not new_id:
            return None, f"failed to clone member snapshot {s.get_id()}: {err}"
        created.append(new_id)
        if new_group is not None:
            add_member_to_group(new_group, db.get_lvol_by_id(new_id))
    logger.info("Consistency group %s: cloned generation %d into %d volume(s)%s",
                group.uuid[:8], seq, len(created),
                f" (new group {into_name})" if into_name else "")
    return created, None
