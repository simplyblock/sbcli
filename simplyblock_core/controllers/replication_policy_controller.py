"""Replication targets, policies and volume assignment.

target -> policy -> volume. A source cluster has any number of named targets, a
target has one or more policies (cadence/mode/retention), and a volume follows
at most one policy.

Everything here is decided from records, never from ``Cluster.status``: cluster
status is a health signal that flips on its own (a "dead" source cluster
auto-recovers when its SPDK containers restart), so a decision keyed on it
reverts itself silently.
"""
import uuid as uuid_module

from simplyblock_core import db_controller as db_module, utils
from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVolReplication
from simplyblock_core.models.pool import Pool
from simplyblock_core import snapshot_retention
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot

logger = utils.get_logger(__name__)
db = db_module.DBController()


class ReplicationConfigError(Exception):
    """Invalid or conflicting replication configuration."""


# --------------------------------------------------------------------------- #
# Targets
# --------------------------------------------------------------------------- #

def add_target(cluster_id, target_name, target_cluster_id, target_pool=None, timeout_sec=None):
    """Create a named replication destination for *cluster_id*."""
    db.get_cluster_by_id(cluster_id)                      # raises when unknown
    if target_cluster_id == cluster_id:
        raise ReplicationConfigError("A cluster cannot replicate to itself")
    db.get_cluster_by_id(target_cluster_id)

    for existing in db.get_replication_targets(cluster_id):
        if existing.target_name == target_name:
            raise ReplicationConfigError(
                f"Replication target '{target_name}' already exists on cluster {cluster_id}")

    pool_uuid = ""
    if target_pool:
        # Resolve to a UUID now: storing a NAME is what made the old
        # add_replication fail later with a KeyError despite accepting "id or name".
        pool = db.get_pool_by_id_or_name(target_pool)
        if pool.cluster_id != target_cluster_id:
            raise ReplicationConfigError(
                f"Pool {target_pool} is not on target cluster {target_cluster_id}")
        if pool.status != Pool.STATUS_ACTIVE:
            raise ReplicationConfigError(f"Pool {target_pool} is not active")
        pool_uuid = pool.get_id()

    target = ReplicationTarget()
    target.uuid = str(uuid_module.uuid4())
    target.cluster_id = cluster_id
    target.target_name = target_name
    target.target_cluster_id = target_cluster_id
    target.target_pool_uuid = pool_uuid
    if timeout_sec:
        target.timeout_sec = timeout_sec
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)
    logger.info("Created replication target %s -> cluster %s (%s)",
                target_name, target_cluster_id, target.get_id())
    return target.get_id()


def list_targets(cluster_id=None):
    return db.get_replication_targets(cluster_id)


def remove_target(target_id):
    """Delete a target. Refused while any policy still references it."""
    target = db.get_replication_target_by_id(target_id)
    users = [p for p in db.get_replication_policies(target.cluster_id)
             if p.target_id.split('/')[-1] == target.uuid]
    if users:
        raise ReplicationConfigError(
            f"Replication target {target.target_name} is used by "
            f"{len(users)} policy(ies): {', '.join(p.policy_name for p in users)}")
    target.remove(db.kv_store)
    logger.info("Removed replication target %s", target_id)
    return True


# --------------------------------------------------------------------------- #
# Policies
# --------------------------------------------------------------------------- #

def add_policy(cluster_id, policy_name, target, interval_min=1, mode=None, keep_replicated=None,
               retention_schedule=None, consistency_group=False, rpo_target_seconds=None):
    """Create a policy on *target* (id or name)."""
    db.get_cluster_by_id(cluster_id)
    try:
        tgt = db.get_replication_target_by_id(target)
    except KeyError:
        tgt = db.get_replication_target_by_name(cluster_id, target)
    if tgt.cluster_id != cluster_id:
        raise ReplicationConfigError(
            f"Replication target {target} belongs to cluster {tgt.cluster_id}")

    for existing in db.get_replication_policies(cluster_id):
        if existing.policy_name == policy_name:
            raise ReplicationConfigError(
                f"Replication policy '{policy_name}' already exists on cluster {cluster_id}")

    if mode and mode not in (ReplicationPolicy.MODE_FAILOVER, ReplicationPolicy.MODE_MIGRATION):
        raise ReplicationConfigError(f"Unknown replication mode: {mode}")
    if interval_min is not None and interval_min < 0:
        raise ReplicationConfigError("interval_min cannot be negative")
    if keep_replicated is not None and keep_replicated < ReplicationPolicy.MIN_KEEP_REPLICATED:
        # Fewer than a pair leaves an arriving snapshot with nothing to chain
        # onto, so retention drops segments instead of swap-merging them.
        raise ReplicationConfigError(
            f"keep_replicated must be at least {ReplicationPolicy.MIN_KEEP_REPLICATED}")
    if rpo_target_seconds is not None and rpo_target_seconds < 0:
        raise ReplicationConfigError("rpo_target_seconds cannot be negative")

    if retention_schedule:
        # Validate at ingress: an unparseable schedule silently falling back to
        # flat retention would quietly discard the history the operator asked
        # for, and they would only find out at fail-over.
        try:
            snapshot_retention.parse_schedule(retention_schedule)
        except snapshot_retention.RetentionScheduleError as e:
            raise ReplicationConfigError(f"invalid retention schedule: {e}")

    policy = ReplicationPolicy()
    policy.uuid = str(uuid_module.uuid4())
    policy.cluster_id = cluster_id
    policy.policy_name = policy_name
    policy.target_id = tgt.get_id()
    if interval_min is not None:
        policy.interval_min = interval_min
    if mode:
        policy.mode = mode
    if keep_replicated is not None:
        policy.keep_replicated = keep_replicated
    if retention_schedule is not None:
        policy.retention_schedule = retention_schedule
    if rpo_target_seconds is not None:
        policy.rpo_target_seconds = rpo_target_seconds
    policy.consistency_group = bool(consistency_group)
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.write_to_db(db.kv_store)
    if policy.consistency_group:
        # Auto-created with the policy, auto-deleted with it (requirement 2).
        from simplyblock_core.controllers import consistency_group_controller
        consistency_group_controller.create_group_for_policy(policy)
    logger.info("Created replication policy %s on target %s (%s)",
                policy_name, tgt.target_name, policy.get_id())
    return policy.get_id()


def list_policies(cluster_id=None):
    return db.get_replication_policies(cluster_id)


def remove_policy(policy_id):
    """Delete a policy. Refused while any volume still follows it."""
    policy = db.get_replication_policy_by_id(policy_id)
    users = db.get_lvols_by_replication_policy(policy.get_id())
    if users:
        raise ReplicationConfigError(
            f"Replication policy {policy.policy_name} is followed by "
            f"{len(users)} volume(s); detach them first")
    if getattr(policy, "consistency_group", False):
        from simplyblock_core.controllers import consistency_group_controller
        consistency_group_controller.delete_group_for_policy(policy.get_id())
    policy.remove(db.kv_store)
    logger.info("Removed replication policy %s", policy_id)
    return True


# --------------------------------------------------------------------------- #
# Volume assignment
# --------------------------------------------------------------------------- #

def _active_relationship(lvol_id):
    """The newest LVolReplication whose SOURCE is *lvol_id*, or None."""
    for rep in reversed(db.get_lvol_replication_objects()):
        if rep.source_lvol and rep.source_lvol.get_id() == lvol_id:
            return rep
    return None


def _resolve_policy(policy):
    if not policy or not str(policy).strip():
        # An empty policy is not "no policy": attaching it would clear the
        # volume's replication configuration while claiming to set one.
        raise ReplicationConfigError("A replication policy id or name is required")
    try:
        return db.get_replication_policy_by_id(policy)
    except KeyError:
        pass
    for candidate in db.get_replication_policies():
        if candidate.policy_name == policy:
            return candidate
    raise KeyError(f'ReplicationPolicy {policy} not found')


def start_member_replication(lvol_id, pol, target):
    """Point one volume at *pol* and start replicating to *target*, WITHOUT
    touching consistency-group membership.

    Mirrors :func:`attach_policy`'s tail (set the policy pointer, start
    replication, roll the pointer back on failure), but adds no group member.
    ``attach_policy`` adds the volume to the policy's group first; the
    group-replication path (``consistency_group_controller.attach_group_policy``)
    calls this for members that ALREADY belong to the group, so re-adding them
    would reset their generation epochs (``add_member_to_group`` stamps a fresh
    ``joined_seq``) and tear the group's snapshot history.
    """
    lvol = db.get_lvol_by_id(lvol_id)
    lvol.replication_policy_id = pol.get_id()
    lvol.write_to_db()
    ret = lvol_controller.replication_start(
        lvol_id,
        replication_cluster_id=target.target_cluster_id,
        mode=pol.mode,
        interval_min=pol.interval_min,
        from_policy=True,
    )
    if not ret:
        fresh = db.get_lvol_by_id(lvol_id)
        fresh.replication_policy_id = ""
        fresh.write_to_db()
        raise ReplicationConfigError(
            f"Could not start replication of {lvol_id} to target {target.target_name}")
    return True


def stop_member_replication(lvol_id):
    """Stop one volume replicating and purge its internal replication snapshots,
    WITHOUT touching consistency-group membership.

    Mirrors :func:`detach_policy`'s tail (cutover guard, clear the policy
    pointer, stop streaming, purge internal snapshots), but leaves the volume in
    its group: the group-replication path (``detach_group_policy``) disables
    replication for a group whose members stay grouped by their label. Idempotent
    no-op when the volume follows no policy.
    """
    lvol = db.get_lvol_by_id(lvol_id)
    if not lvol.replication_policy_id:
        logger.info("Volume %s follows no replication policy; stop is a no-op", lvol_id)
        return True
    rep = _active_relationship(lvol_id)
    if rep is not None and rep.state == LVolReplication.STATE_CUTOVER_PENDING:
        raise ReplicationConfigError(
            f"Volume {lvol_id} has a cutover in flight; wait for it to finish "
            f"before stopping its replication")
    lvol.replication_policy_id = ""
    lvol.write_to_db()
    lvol_controller.replication_stop(lvol_id, from_policy=True)
    removed = _purge_internal_replication_snapshots(lvol_id)
    logger.info("Volume %s replication stopped (%d internal replication "
                "snapshot(s) removed)", lvol_id, removed)
    return True


def attach_policy(lvol_id, policy):
    """Put a volume under a policy and start replicating.

    Changing policy is detach-then-attach, so the delta base on the old target is
    dropped and replication to the new target starts FULL. That is intended, but
    it is expensive for a large volume.
    """
    lvol = db.get_lvol_by_id(lvol_id)
    pol = _resolve_policy(policy)
    target = db.get_replication_target_by_id(pol.target_id)

    if pol.status != ReplicationPolicy.STATUS_ACTIVE:
        raise ReplicationConfigError(f"Replication policy {pol.policy_name} is not active")
    if target.status != ReplicationTarget.STATUS_ACTIVE:
        raise ReplicationConfigError(f"Replication target {target.target_name} is not active")

    current = getattr(lvol, 'replication_policy_id', '')
    if current and current.split('/')[-1] == pol.uuid:
        logger.info("Volume %s already follows policy %s", lvol_id, pol.policy_name)
        return True
    if current:
        logger.info("Volume %s changes policy: detaching from %s first", lvol_id, current)
        detach_policy(lvol_id)
        lvol = db.get_lvol_by_id(lvol_id)

    if getattr(pol, "consistency_group", False):
        # Requirement 1: all members share one LVS. Checked BEFORE any state
        # is written, so a failed attachment leaves the volume untouched.
        from simplyblock_core.controllers import consistency_group_controller
        consistency_group_controller.add_member(pol, lvol)

    lvol.replication_policy_id = pol.get_id()
    lvol.write_to_db()
    ret = lvol_controller.replication_start(
        lvol_id,
        replication_cluster_id=target.target_cluster_id,
        mode=pol.mode,
        interval_min=pol.interval_min,
        from_policy=True,
    )
    if not ret:
        # Do not leave the volume pointing at a policy that never started.
        lvol = db.get_lvol_by_id(lvol_id)
        lvol.replication_policy_id = ""
        lvol.write_to_db()
        if getattr(pol, "consistency_group", False):
            from simplyblock_core.controllers import consistency_group_controller
            consistency_group_controller.remove_member(pol.get_id(), lvol_id)
        raise ReplicationConfigError(
            f"Could not start replication of {lvol_id} to target {target.target_name}")
    logger.info("Volume %s now follows policy %s (target %s)",
                lvol_id, pol.policy_name, target.target_name)
    return True


def detach_policy(lvol_id):
    """Take a volume out of its policy and leave no replication residue.

    Stops replication, cancels the queued tasks, and deletes the volume's
    INTERNAL replication snapshots on the source AND on the target. User
    snapshots are never touched, and a target snapshot that a live volume is
    cloned from is kept: deleting it reaches SPDK as
    ``bdev_lvol_delete(sync=False)``, which frees the blocks immediately, so a
    failed-over volume built on it would start reading zeros.
    """
    lvol = db.get_lvol_by_id(lvol_id)

    if not lvol.replication_policy_id:
        # Idempotent no-op: there is no policy to detach and no policy
        # residue to clean. Returning early also keeps a detach from
        # reaching through and stopping a LEGACY (start/stop path) replication
        # the volume may be running, which no policy ever owned.
        logger.info("Volume %s follows no replication policy; detach is a no-op", lvol_id)
        return True

    rep = _active_relationship(lvol_id)
    if rep is not None and rep.state == LVolReplication.STATE_CUTOVER_PENDING:
        raise ReplicationConfigError(
            f"Volume {lvol_id} has a cutover in flight; wait for it to finish "
            f"before detaching the replication policy")

    detached_policy_id = lvol.replication_policy_id
    lvol.replication_policy_id = ""
    lvol.write_to_db()

    if detached_policy_id:
        try:
            pol = db.get_replication_policy_by_id(detached_policy_id)
        except KeyError:
            pol = None
        if pol is not None and getattr(pol, "consistency_group", False):
            from simplyblock_core.controllers import consistency_group_controller
            consistency_group_controller.remove_member(pol.get_id(), lvol_id)

    # Stops streaming and cancels the non-DONE FN_SNAPSHOT_REPLICATION tasks.
    lvol_controller.replication_stop(lvol_id, from_policy=True)

    removed = _purge_internal_replication_snapshots(lvol_id)
    logger.info("Volume %s detached from its replication policy (%d internal "
                "replication snapshot(s) removed)", lvol_id, removed)
    return True


def _purge_internal_replication_snapshots(lvol_id):
    """Delete the volume's internal replication snapshots, target copy first.

    The volume's NEWEST fully replicated pair -- the source record and its
    target copy -- survives unconditionally: it is the last recovery point,
    and a detach cannot know whether one is about to be needed. An unplanned
    failover reaches this purge with NO demote (nothing was reachable to
    demote) and NO dependent clone (the promote races this very teardown),
    because Ramen deletes the source side's VolumeReplication while flipping
    its VRG to Secondary; with only the demote and clone guards, the purge
    deleted the fail-over point mid-failover and the clone selector
    409-looped forever against a dead source (confirmed live 2026-09-25).
    The pair is released when the volume itself is deleted.
    """
    removed = 0
    handled = set()                               # never issue a delete twice
    demote_snapshot_id = db.get_lvol_by_id(lvol_id).replication_demote_snapshot_id
    newest_replicated_id = ""
    replicated = [
        s for s in db.get_snapshots()
        if not s.deleted and s.lvol and s.lvol.get_id() == lvol_id
        and s.snap_type == SnapShot.TYPE_INTERNAL
        and s.target_replicated_snap_uuid
    ]
    if replicated:
        newest_replicated_id = max(replicated, key=lambda s: s.created_at).get_id()
    for snap in db.get_snapshots():
        if snap.deleted or not snap.lvol or snap.lvol.get_id() != lvol_id:
            continue
        if snap.snap_type != SnapShot.TYPE_INTERNAL:
            continue                                  # user snapshots stay
        if snap.get_id() in handled:
            continue
        target_uuid = snap.target_replicated_snap_uuid or snap.source_replicated_snap_uuid
        if target_uuid and target_uuid not in handled:
            handled.add(target_uuid)
            if _has_dependent_clone(target_uuid):
                logger.info("Keeping replicated snapshot %s: a volume is cloned from it",
                            target_uuid)
            elif snap.get_id() == demote_snapshot_id:
                # This is the exact snapshot demote_lvol fenced the volume on
                # -- the volume is currently demoted and awaiting a pending
                # fail-over, and this target copy is the fail-over point a
                # PromoteVolume call may still need, even with no clone from
                # it yet. Deleting it strands every subsequent fail-over
                # attempt with "No replicated snapshot on target yet" for an
                # otherwise perfectly healthy, still-demoted volume (confirmed
                # live 2026-09-24, Ramen relocate M-02).
                logger.info("Keeping replicated snapshot %s: volume is demoted, "
                            "awaiting a pending fail-over", target_uuid)
            elif snap.get_id() == newest_replicated_id:
                # The newest fully replicated pair is the volume's last
                # recovery point and survives every detach (see docstring).
                logger.info("Keeping replicated snapshot %s: it is the volume's "
                            "newest replicated recovery point", target_uuid)
            else:
                try:
                    db.get_snapshot_by_id(target_uuid)
                except KeyError:
                    pass                              # already gone
                else:
                    if snapshot_controller.delete(target_uuid):
                        removed += 1
                    else:
                        logger.warning("Could not delete remote snapshot copy %s", target_uuid)
        handled.add(snap.get_id())
        if _has_dependent_clone(snap.get_id()):
            logger.info("Keeping source snapshot %s: a volume is cloned from it", snap.get_id())
            continue
        if snap.get_id() == newest_replicated_id:
            # The pair's source half: last_replicated_target_snapshot resolves
            # by SOURCE snapshot id first, so the source record must survive
            # alongside the target copy preserved above.
            logger.info("Keeping source snapshot %s: it is the volume's "
                        "newest replicated recovery point", snap.get_id())
            continue
        if snap.get_id() == demote_snapshot_id:
            # last_replicated_target_snapshot resolves its candidates by
            # SOURCE snapshot id first (each completed replication task names
            # one in task.function_params["snapshot_id"]) and only then reads
            # target_replicated_snap_uuid off that record. Deleting this
            # source copy makes the whole candidate disappear before its
            # (already-preserved, see above) target copy is ever consulted --
            # the exact same "No replicated snapshot on target yet" stranding
            # this function exists to prevent, just reached from the other
            # side of the pair.
            logger.info("Keeping source snapshot %s: volume is demoted, "
                        "awaiting a pending fail-over", snap.get_id())
            continue
        if snapshot_controller.delete(snap.get_id()):
            removed += 1
        else:
            logger.warning("Could not delete internal snapshot %s", snap.get_id())
    return removed


def _has_dependent_clone(snapshot_uuid):
    from simplyblock_core.models.lvol_model import LVol
    for lvol in db.get_mini_lvols():
        if lvol.cloned_from_snap != snapshot_uuid:
            continue
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        return True
    return False


# --------------------------------------------------------------------------- #
# Group fail-over
# --------------------------------------------------------------------------- #

def failover_policy(policy_id):
    """Fail over every volume following *policy_id*. Idempotent per volume.

    Volumes that belong to a consistency group fail over as ONE unit: every
    member is pinned to the same group generation (see
    _resolve_group_failover_generation) instead of each volume's own newest
    replicated snapshot. Membership is what makes the cut crash-consistent —
    a member carries its group id — so group fail-over triggers off that, not
    off a policy flag (the legacy consistency_group flag still triggers it).
    """
    policy = db.get_replication_policy_by_id(policy_id)
    volumes = db.get_lvols_by_replication_policy(policy.get_id())
    pinned = None
    grouped = (getattr(policy, "consistency_group", False)
               or any(getattr(v, "group_id", "") for v in volumes))
    if grouped:
        try:
            _, pinned = _resolve_group_failover_generation(policy, volumes)
        except ReplicationConfigError as e:
            logger.error("Group fail-over of policy %s refused: %s",
                         policy.policy_name, e)
            return [{"lvol_id": v.get_id(), "status": "failed", "detail": str(e)}
                    for v in volumes]
    return _failover_volumes(volumes, f"policy {policy.policy_name}", pinned=pinned)


def failover_target(target_id):
    """Fail over every volume whose policy points at *target_id*."""
    target = db.get_replication_target_by_id(target_id)
    results = []
    for policy in db.get_replication_policies(target.cluster_id):
        if policy.target_id.split('/')[-1] != target.uuid:
            continue
        # Through failover_policy, so a consistency-group policy keeps its
        # group-wide generation pinning on the target-scoped path too.
        results.extend(failover_policy(policy.get_id()))
    return results


def _settled_relationship(lvol_id):
    """The volume's relationship if it is already failed over, else None."""
    rep = _active_relationship(lvol_id)
    if rep is not None and rep.state in (LVolReplication.STATE_FAILED_OVER,
                                         LVolReplication.STATE_CUTOVER_DONE):
        return rep
    return None


def _resolve_group_failover_generation(policy, volumes):
    """The one generation every pending member fails over to.

    A consistency group restores as ONE crash-consistent cut, so every member
    must clone from a snapshot of the SAME group generation. Selecting per
    volume instead (each volume's own newest replicated snapshot) tears the
    group apart the moment a new generation has finished replicating for some
    members only — observed live on 2026-09-07 as a (3, 3, 4) restore.

    A generation qualifies when every member still to be failed over has a
    snapshot of it that is FULLY replicated, by the same rules
    lvol_controller.last_replicated_target_snapshot applies per volume: the
    replication task is DONE (a target record alone proves allocation, not
    data), and the target copy still exists and is not being deleted by
    retention.

    Members that already failed over pin the choice: their clones' group
    generation (target snapshot copies keep group_id/group_seq) is the
    incumbent, and a resumed run must join it rather than resolve afresh —
    a newer generation completing between the two passes would otherwise
    split the group across two cuts.

    Returns (seq, {lvol_id: source_snapshot_id}) for the pending members.
    Raises ReplicationConfigError when no generation qualifies.
    """
    # Resolve the group by membership first (a member carries its group id),
    # falling back to the legacy policy-owned link. A policy's volumes must all
    # sit in one group for the cut to be a single crash-consistent generation.
    group_ids = {getattr(v, "group_id", "") for v in volumes
                 if getattr(v, "group_id", "")}
    if len(group_ids) > 1:
        raise ReplicationConfigError(
            f"Policy {policy.policy_name} spans multiple consistency groups "
            f"{sorted(group_ids)}; refusing a mixed-group fail-over")
    group = None
    if group_ids:
        try:
            group = db.get_consistency_group_by_id(next(iter(group_ids)))
        except KeyError:
            group = None
    else:
        group = db.get_consistency_group_for_policy(policy.get_id())
    if group is None:
        raise ReplicationConfigError(
            f"Policy {policy.policy_name} volumes belong to a consistency group "
            f"but no group record was found")

    pending_ids = []
    incumbent_seqs = set()
    for lvol in volumes:
        rep = _settled_relationship(lvol.get_id())
        if rep is None:
            pending_ids.append(lvol.get_id())
            continue
        clone = getattr(rep, "target_lvol", None)
        if clone is None or not getattr(clone, "cloned_from_snap", ""):
            continue
        try:
            clone_base = db.get_snapshot_by_id(clone.cloned_from_snap)
        except KeyError:
            continue
        if getattr(clone_base, "group_seq", 0):
            incumbent_seqs.add(clone_base.group_seq)

    if not pending_ids:
        return 0, {}
    if len(incumbent_seqs) > 1:
        raise ReplicationConfigError(
            f"Consistency group of policy {policy.policy_name} is already split "
            f"across generations {sorted(incumbent_seqs)}; refusing to fail over "
            f"more members")

    replicated = {
        task.function_params.get("snapshot_id")
        for task in db.get_job_tasks(policy.cluster_id)
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
        and task.status == JobSchedule.STATUS_DONE
    }

    by_seq: dict = {}
    for snap in db.get_snapshots():
        if getattr(snap, "group_id", "") != group.get_id():
            continue
        seq = getattr(snap, "group_seq", 0)
        lvol_id = snap.lvol.get_id() if snap.lvol else ""
        if not seq or lvol_id not in pending_ids:
            continue
        if snap.get_id() not in replicated or not snap.target_replicated_snap_uuid:
            continue
        try:
            target_copy = db.get_snapshot_by_id(snap.target_replicated_snap_uuid)
        except KeyError:
            continue
        if (target_copy.status == SnapShot.STATUS_IN_DELETION
                or getattr(target_copy, "deleted", False)):
            continue
        by_seq.setdefault(seq, {})[lvol_id] = snap.get_id()

    candidates = (sorted(incumbent_seqs) if incumbent_seqs
                  else sorted(by_seq, reverse=True))
    for seq in candidates:
        covered = by_seq.get(seq, {})
        if set(pending_ids) <= set(covered):
            logger.info("Group fail-over of policy %s pinned to generation %d "
                        "(%d pending member(s)%s)", policy.policy_name, seq,
                        len(pending_ids),
                        ", resuming the incumbent" if incumbent_seqs else "")
            return seq, covered

    missing = ""
    if candidates:
        best = candidates[0]
        absent = sorted(set(pending_ids) - set(by_seq.get(best, {})))
        missing = f"; generation {best} lacks {', '.join(absent)}"
    raise ReplicationConfigError(
        f"No group generation of policy {policy.policy_name} is fully "
        f"replicated for all {len(pending_ids)} pending member(s); refusing a "
        f"mixed-generation fail-over{missing}")


def latest_replicated_generation(policy_id: str) -> tuple[int, dict[str, SnapShot]]:
    """The newest consistency-group generation every current member has fully
    replicated, as cloneable objects on the secondary.

    Reuses :func:`_resolve_group_failover_generation`'s refusal rule instead
    of its side effect: a generation qualifies only when every member has a
    snapshot of it that reached the target and is not being pruned, so a
    caller (the test-failover drill, design §14) never addresses a
    mixed-generation cut. Every current member is treated as pending -- this
    describes present-day, un-failed-over steady state, never a resumed
    fail-over that already has clones on the peer.

    Returns ``(group_seq, {lvol_id: target_snapshot})``. Raises
    ``ReplicationConfigError`` when the policy has no consistency group or no
    generation is fully replicated for every member yet.
    """
    policy = db.get_replication_policy_by_id(policy_id)
    if not getattr(policy, "consistency_group", False):
        raise ReplicationConfigError(
            f"Policy {policy.policy_name} has no consistency group")

    volumes = db.get_lvols_by_replication_policy(policy.get_id())
    seq, covered = _resolve_group_failover_generation(policy, volumes)
    if not covered:
        raise ReplicationConfigError(
            f"Policy {policy.policy_name} has no member left to resolve a "
            f"generation for; every member has already failed over")

    members = {}
    for lvol_id, source_snap_id in covered.items():
        source_snap = db.get_snapshot_by_id(source_snap_id)
        # Re-fetched rather than carried from the scan above: the scan proved
        # the target copy existed and was not being pruned at THAT instant,
        # and this is a read with no lock, so a concurrent retention pass
        # remains possible in the window between. Rare enough, and cheap
        # enough to just re-raise on, that a lock is not worth taking for a
        # status read.
        members[lvol_id] = db.get_snapshot_by_id(source_snap.target_replicated_snap_uuid)
    return seq, members


def _failover_volumes(volumes, what, pinned=None):
    """Per-volume results, so a partial failure is visible instead of silent.

    ``pinned`` maps lvol id to the snapshot the volume MUST clone from
    (consistency groups); a plain policy passes None and every volume picks
    its own newest replicated snapshot.
    """
    results = []
    logger.info("Failing over %d volume(s) of %s", len(volumes), what)
    for lvol in volumes:
        lvol_id = lvol.get_id()
        rep = _active_relationship(lvol_id)
        if rep is not None and rep.state in (LVolReplication.STATE_FAILED_OVER,
                                             LVolReplication.STATE_CUTOVER_DONE):
            results.append({"lvol_id": lvol_id, "status": "skipped",
                            "detail": f"already {rep.state}",
                            "target_lvol_id": rep.target_lvol.get_id() if rep.target_lvol else ""})
            continue
        try:
            if pinned is None:
                ret = lvol_controller.replicate_lvol_on_target_cluster(lvol_id)
            elif not pinned.get(lvol_id):
                results.append({"lvol_id": lvol_id, "status": "failed",
                                "detail": "no snapshot of the group's fail-over generation"})
                continue
            else:
                ret = lvol_controller.replicate_lvol_on_target_cluster(
                    lvol_id, pin_snapshot_id=pinned[lvol_id])
        except Exception as e:                       # one volume must not stop the group
            logger.error("Fail-over of %s failed: %s", lvol_id, e)
            results.append({"lvol_id": lvol_id, "status": "failed", "detail": str(e)})
            continue
        if isinstance(ret, tuple):                   # (False, error)
            results.append({"lvol_id": lvol_id, "status": "failed", "detail": str(ret[1])})
        elif not ret:
            results.append({"lvol_id": lvol_id, "status": "failed", "detail": "fail-over returned no volume"})
        elif isinstance(ret, dict):
            results.append({"lvol_id": lvol_id, "status": "failed_over",
                            "target_lvol_id": ret.get("lvol_id", ""),
                            "connection_strings": ret.get("connection_strings", []),
                            "warnings": ret.get("warnings", [])})
        else:
            results.append({"lvol_id": lvol_id, "status": "failed_over", "target_lvol_id": str(ret)})
    return results


def set_cutover_proceed(lvol_id):
    """Signal that the operator has connected the NVMe paths.

    Finds the cutover_pending LVolReplication for *lvol_id* — either as the
    source (migration direction) or as the target (failback direction) — and
    sets cutover_proceed = True so the task runner advances past the wait.

    During failback the replication direction is reversed: the original source
    volume becomes the TARGET of the reverse replication, so _active_relationship
    (which searches by source) would miss it. The fallback search by target_lvol
    handles this case without changing the API surface.

    Returns the replication ID on success, raises KeyError when no matching
    cutover_pending record is found.
    """
    rep = _active_relationship(lvol_id)
    if rep is None or rep.state != LVolReplication.STATE_CUTOVER_PENDING:
        # Failback path: lvol_id is the target of the reverse replication.
        rep = None
        for r in reversed(db.get_lvol_replication_objects()):
            if (r.target_lvol and r.target_lvol.get_id() == lvol_id
                    and r.state == LVolReplication.STATE_CUTOVER_PENDING):
                rep = r
                break
    if rep is None:
        raise KeyError(
            f"No cutover_pending replication found for volume {lvol_id}")
    rep.cutover_proceed = True
    rep.write_to_db(db.kv_store)
    return rep.get_id()


def get_relationship(lvol_id):
    """The replication relationship of *lvol_id*, source or target side.

    This is the source->target mapping an upper layer needs and which no API
    exposed: the ids were only ever returned by the fail-over / commit call
    itself, so a caller that did not keep them could not find the target volume.
    """
    for rep in reversed(db.get_lvol_replication_objects()):
        source_id = rep.source_lvol.get_id() if rep.source_lvol else ""
        target_id = rep.target_lvol.get_id() if rep.target_lvol else ""
        if lvol_id not in (source_id, target_id):
            continue
        # Which side serves the client RIGHT NOW. Until the cutover completes
        # (or a fail-over happens) the source is live; from then on the target
        # is. This look-up works by SOURCE uuid even after the source volume
        # has been deleted (e.g. replication-commit --delete-source): the
        # relationship record embeds both volumes and is never removed with
        # them, so the mapping source->target and the active side stay
        # resolvable for as long as the relationship exists.
        active = ("target" if rep.state in (LVolReplication.STATE_CUTOVER_DONE,
                                            LVolReplication.STATE_FAILED_OVER)
                  else "source")
        return {
            "replication_id": rep.get_id(),
            "source_lvol_id": source_id,
            "target_lvol_id": target_id,
            "source_cluster_id": rep.source_cluster_id,
            "target_cluster_id": rep.target_cluster_id,
            # Pool where the target volume lives — needed by the CSI driver to
            # build the /connect URL when redirecting after delete_source.
            "target_pool_id": getattr(rep.target_lvol, "pool_uuid", "") if rep.target_lvol else "",
            "mode": rep.mode,
            "state": rep.state,
            "direction": rep.direction,
            "target_nqn": rep.target_nqn,
            "target_ns_id": rep.target_ns_id,
            "is_source": lvol_id == source_id,
            "active": active,
            # Chain-resolved: a volume can migrate onward (the target of one
            # relationship becomes the source of the next), so the volume
            # actually serving the data may be several hops away. Resolved
            # transitively; the per-relationship side stays in "active".
            "active_lvol_id": _resolve_active_lvol(
                target_id if active == "target" else source_id),
        }
    return None


def _resolve_active_lvol(lvol_id):
    """Follow completed handoffs to the volume actually serving the data.

    A completed cutover/fail-over hands the active role from its source to its
    target; a volume can hand it on again (chained migration) or hand it BACK
    (fail-back), so recency is GLOBAL: each hop must be strictly newer than
    the hop that led here, otherwise a stale earlier hand-off would be
    replayed for ever (S->T fail-over, then the newer T->S fail-back: from S
    the walk must not follow the old S->T again). Records come ordered oldest
    to newest; the index is the clock. Monotonic time also terminates cycles.
    """
    reps = db.get_lvol_replication_objects()    # sorted oldest -> newest
    current = lvol_id
    after = -1
    for _ in range(64):                          # defensive hop bound
        hop = None
        for i in range(len(reps) - 1, after, -1):
            rep = reps[i]
            src = rep.source_lvol.get_id() if rep.source_lvol else ""
            if src != current:
                continue
            if rep.state in (LVolReplication.STATE_CUTOVER_DONE,
                             LVolReplication.STATE_FAILED_OVER):
                hop = (i, rep.target_lvol.get_id() if rep.target_lvol else "")
            break                                # newest eligible decides
        if hop is None or not hop[1]:
            return current
        after, current = hop[0], hop[1]
    return current
