# coding=utf-8
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

from simplyblock_core import db_controller as db_mod
from simplyblock_core import utils
from simplyblock_core.controllers import snapshot_events, tasks_controller
from simplyblock_core.controllers.snapshot_controller import (
    _find_lvs_leader, _rollback_snapshot_bdev, lvstore_op_lock)
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

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


def add_member(policy, lvol):
    """Enforce requirement 1 and record the member's epoch.

    The FIRST member pins the group to its node/LVS. Every later member must
    already live there — the attachment FAILS otherwise; membership becomes
    effective at the NEXT group snapshot (``joined_seq = last_group_seq + 1``),
    because no earlier group snapshot contains this volume.

    A re-attaching volume gets a fresh epoch: its old history window stays
    recorded under the closed epoch semantics (removed_seq of the old entry
    is preserved only implicitly by the new joined_seq being later — the
    entry is replaced, and the generation math treats the gap correctly
    because the new joined_seq excludes the detached window).
    """
    group = db.get_consistency_group_for_policy(policy.get_id())
    if group is None:
        # Policies created before the flag existed, or records lost: fail
        # loudly rather than silently degrading to per-volume snapshots.
        raise ConsistencyGroupError(
            f"Policy {policy.policy_name} declares a consistency group but "
            f"has no group record")

    if group.lvs_name and (lvol.lvs_name != group.lvs_name
                           or lvol.node_id != group.node_id):
        raise ConsistencyGroupError(
            f"Volume {lvol.get_id()} lives on {lvol.node_id[:8]}/{lvol.lvs_name} "
            f"but consistency group {group.uuid[:8]} of policy "
            f"{policy.policy_name} is pinned to "
            f"{group.node_id[:8]}/{group.lvs_name}; all members of a "
            f"consistency group must share one LVS")

    if not group.lvs_name:
        group.node_id = lvol.node_id
        group.lvs_name = lvol.lvs_name
        logger.info("Consistency group %s pinned to node %s / %s by its first "
                    "member %s", group.uuid[:8], lvol.node_id[:8],
                    lvol.lvs_name, lvol.get_id())

    members = dict(group.members or {})
    members[lvol.get_id()] = {"joined_seq": group.last_group_seq + 1,
                              "removed_seq": 0}
    group.members = members
    group.write_to_db(db.kv_store)
    logger.info("Volume %s joined consistency group %s at generation %d "
                "(effective from the next group snapshot)",
                lvol.get_id(), group.uuid[:8], group.last_group_seq + 1)
    return group


def remove_member(policy_id, lvol_id):
    """Close the member's epoch at the current generation."""
    group = db.get_consistency_group_for_policy(policy_id)
    if group is None:
        return
    members = dict(group.members or {})
    entry = members.get(lvol_id)
    if entry and entry.get("removed_seq", 0) == 0:
        entry = dict(entry)
        entry["removed_seq"] = max(group.last_group_seq, entry.get("joined_seq", 1) - 1)
        members[lvol_id] = entry
        group.members = members
        group.write_to_db(db.kv_store)
        logger.info("Volume %s left consistency group %s (included up to "
                    "generation %d)", lvol_id, group.uuid[:8], entry["removed_seq"])


def pinned_node_for_policy(policy):
    """The node a NEW volume under this policy must be created on, or None."""
    group = db.get_consistency_group_for_policy(policy.get_id())
    if group is not None and group.node_id:
        return group.node_id
    return None


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
# The group snapshot tick
# --------------------------------------------------------------------------- #

def _current_members(group):
    """Live member volumes: attached to the policy, epoch open, usable."""
    members = []
    for lvol_id, m in (group.members or {}).items():
        if m.get("removed_seq", 0) != 0:
            continue
        try:
            lvol = db.get_lvol_by_id(lvol_id)
        except KeyError:
            continue
        if lvol.status != LVol.STATUS_ONLINE:
            logger.warning("Consistency group %s: member %s is %s; the group "
                           "snapshot is skipped this tick (a group snapshot "
                           "missing a member is not a group snapshot)",
                           group.uuid[:8], lvol_id, lvol.status)
            return None
        members.append(lvol)
    return members


def create_group_snapshot(policy_id, snap_type=SnapShot.TYPE_INTERNAL, lock=True):
    """Take ONE crash-consistent snapshot of every group member.

    Returns (list_of_snapshot_ids, None) or (None, error). All-or-nothing:
    a failure anywhere rolls back every snapshot bdev of this tick (SPDK
    already GC'd if the failure was inside the RPC; registration/record
    failures are rolled back here) and the generation counter does not move.
    """
    policy = db.get_replication_policy_by_id(policy_id)
    group = db.get_consistency_group_for_policy(policy.get_id())
    if group is None:
        return None, f"Policy {policy_id} has no consistency group"

    members = _current_members(group)
    if members is None:
        return None, "consistency group member not online"
    if not members:
        return None, "consistency group has no members"

    # Placement invariant (defense in depth: attach enforces it already).
    for lvol in members:
        if lvol.lvs_name != group.lvs_name or lvol.node_id != group.node_id:
            return None, (f"member {lvol.get_id()} is on "
                          f"{lvol.node_id[:8]}/{lvol.lvs_name}, group is pinned "
                          f"to {group.node_id[:8]}/{group.lvs_name}")

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
        snap_bdev = rpc_client.get_bdevs(f"{group.lvs_name}/{p['snap_bdev_name']}")
        if not snap_bdev:
            _rollback_all()
            return None, (f"group snapshot {p['snap_bdev_name']} not readable "
                          f"after creation")
        p["snap_uuid"] = snap_bdev[0]["uuid"]
        p["blobid"] = snap_bdev[0]["driver_specific"]["lvol"]["blobid"]
        num_allocated = snap_bdev[0]["driver_specific"]["lvol"]["num_allocated_clusters"]
        p["used_size"] = int(num_allocated * cluster.page_size_in_blocks)

        for sec in secondary_nodes:
            from simplyblock_core.storage_node_ops import (
                wait_or_delay_for_restart_gate, queue_for_restart_drain)
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
                reg = sec.rpc_client().bdev_lvol_snapshot_register(
                    f"{group.lvs_name}/{lvol.lvol_bdev}", p["snap_bdev_name"],
                    p["snap_uuid"], p["blobid"])
            if not reg:
                logger.error("Group snapshot register of %s failed on %s; "
                             "rolling the WHOLE generation back",
                             p["snap_bdev_name"], sec.get_id())
                _rollback_all()
                for snap_id in created_ids:
                    try:
                        rec = db.get_snapshot_by_id(snap_id)
                        db.unindex_snapshot(rec)
                        rec.remove(db.kv_store)
                    except Exception:
                        pass
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

        db.index_snapshot(snap)
        snapshot_events.snapshot_create(snap)
        created_ids.append(snap.get_id())
        p["snap_id"] = snap.get_id()

    # The generation exists in full: bump the counter, then enqueue the
    # per-member replication tasks (transfer machinery is per-snapshot).
    group = db.get_consistency_group_by_id(group.get_id())
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
