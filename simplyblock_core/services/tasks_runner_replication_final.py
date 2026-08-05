"""Task runner for cross-cluster replication cutover (FN_REPLICATION_FINAL).

Consumes the task enqueued by migration-commit (D7) and fail-back (D8) and
drives the whole cutover:

  1. SHRINK phase (>=2 rounds): the controller took shrink snapshot #1 at
     commit time; this runner waits until it is replicated AND converted on the
     target (target_replicated_snap_uuid set happens after convert), then
     IMMEDIATELY takes the next shrink snapshot — whose delta covers only the
     wait window — and waits again. Each round shrinks the freeze residual.
  2. CUTOVER phase, immediately after the last round: build the writable
     target clone on the LAST replicated snapshot (guaranteed fresh by the
     shrink), resolve its map_id, record the LVolReplication, and run the
     IO-freeze final step + ANA flip via :mod:`replication_final_step`.

task.function_params:
    lvol_id, src_node_id, tgt_node_id, operation, final_state,
    shrink_round, shrink_snap_id, shrink_deadline
    (+ tgt_lvol_composite/tgt_map_id/tgt_snap_composite/replication_id once
     the cutover phase has prepared them)
"""
import contextlib
import time
import uuid as uuid_lib
from datetime import datetime
from typing import NoReturn

from simplyblock_core import constants, db_controller, utils, xfer_timing
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskDefer,
    TaskRetry,
    serve,
)

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)

db = db_controller.DBController()


def _lvs_cutover_owner(task, lvs_name, tasks):
    """Return the task that should own the active cutover slot, or None if *task* wins.

    Serializes all cutovers globally: only one may run at a time across all
    volumes and lvstores.  Deterministic: the earliest-created claimant wins
    (sorted by create_dt then id), so two tasks that both wrote a claim agree on
    the same winner regardless of interleaving.

    Callers MUST write their own ``cutover_lvs`` claim to DB **before** calling
    this function so they appear in the scan and participate in the sort.

    Returns None     → the caller is the rightful owner; proceed.
    Returns <other>  → that other task owns the slot; the caller should remove
                       its own claim and yield.

    ``tasks`` is the list the driver already read for this pass and handed over
    via ``RunnerSpec.wants_cycle_tasks``. Re-reading it per task would be O(N^2)
    DB reads over the full retention window, which the poll interval cannot
    afford.
    """
    if not lvs_name:
        return None
    owners = []
    for other in tasks:
        if other.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if other.status == JobSchedule.STATUS_DONE or other.canceled:
            continue
        if (other.function_params or {}).get("cutover_lvs") != lvs_name:
            continue
        owners.append(other)
    if not owners:
        return None
    owners.sort(key=lambda t: (str(getattr(t, "create_dt", "")), t.get_id()))
    winner = owners[0]
    return None if winner.get_id() == task.get_id() else winner


def _group_id_for_lvol(lvol):
    """The consistency group *lvol* belongs to, or "".

    A group is owned by a replication policy and pinned to one node/LVS, so a
    volume's group is the group of its policy.
    """
    policy_id = getattr(lvol, "replication_policy_id", "")
    if not policy_id:
        return ""
    try:
        group = db.get_consistency_group_for_policy(policy_id)
    except Exception as e:                              # noqa: BLE001
        logger.warning("Could not resolve the consistency group of %s: %s",
                       lvol.get_id(), e)
        return ""
    return group.get_id() if group else ""


def _release_lvs_claim(task):
    """Let other volumes on this LVS replicate again.

    No write of its own: the driver persists the task on whichever outcome
    follows this call.
    """
    task.function_params.pop("cutover_lvs", None)
    task.function_params.pop("cutover_group", None)


def _record_cutover_done(task):
    """Record a completed cutover and retire the source.

    The driver owns the task row, so this keeps only what the success path has
    always done besides it: the replication state, the failback UUID swap, the
    source's cadence config and the optional ``--delete-source``.
    """
    params = task.function_params
    replication_id = params.get("replication_id")
    final_state = params.get("final_state", LVolReplication.STATE_CUTOVER_DONE)
    rep = None
    if replication_id:
        try:
            rep = db.get_lvol_replication_by_id(replication_id)
            rep.state = final_state
            rep.write_to_db(db.kv_store)
        except Exception as e:
            logger.error(f"Failed to update replication state: {e}")

    failback_source_id = params.get("failback_source_lvol_id")
    if failback_source_id and rep is not None:
        _swap_failback_lvol_uuid(rep, failback_source_id)
        # Remove the stale failed_over LVolReplication that predates the
        # failback. Without this the operator's get_relationship query keeps
        # finding the old record and reports failed_over indefinitely even
        # though IO has already returned to the original source cluster.
        prior_replication_id = params.get("failback_prior_replication_id")
        if prior_replication_id:
            with contextlib.suppress(KeyError):
                prior_rep = db.get_lvol_replication_by_id(prior_replication_id)
                prior_rep.remove(db.kv_store)
                logger.info(
                    "failback: removed stale failed_over replication record %s",
                    prior_replication_id)

    params["end_time"] = int(time.time())
    task.function_result = "cutover done"

    _stop_source_replication(task)
    _delete_source_if_requested(task)
    _release_lvs_claim(task)


def _stop_source_replication(task):
    """The hand-off is complete: the target serves the data from here on, so the
    SOURCE must stop replicating. Nothing else clears its cadence config, and a
    retired source otherwise keeps taking internal snapshots and shipping them
    to the very target it handed off to (observed 2026-08-21: replication_final
    done "cutover done" while the source volumes kept replicating). In-flight
    transfers drain naturally; this stops NEW cadence snapshots at the gate the
    monitor reads (do_replicate / replication_interval_min).
    """
    try:
        src_lvol = db.get_lvol_by_id(task.function_params.get("lvol_id"))
        if src_lvol.do_replicate:
            src_lvol.do_replicate = False
            src_lvol.replication_interval_min = 0
            src_lvol.replication_policy_id = ""
            src_lvol.write_to_db()
            logger.info(f"Cutover done: stopped replication on source "
                        f"volume {src_lvol.get_id()}")
    except KeyError:
        pass          # source already gone (e.g. deleted out of band)
    except Exception as e:
        logger.error(f"Could not stop replication on the source after "
                     f"cutover: {e}")


def _delete_source_if_requested(task):
    """Optional migration semantics: the source volume has served its purpose
    once the client runs on the target, so `replication-commit --delete-source`
    retires it here. The relationship record is what later look-ups
    (target-by-source, active side) resolve through, and it survives the
    volume's deletion.
    """
    if not task.function_params.get("delete_source"):
        return

    src_lvol_id = task.function_params.get("lvol_id")
    try:
        from simplyblock_core.controllers import lvol_controller
        src_lvol = db.get_lvol_by_id(src_lvol_id)
        logger.info(f"Cutover committed with --delete-source: deleting "
                    f"source volume {src_lvol_id}")
        lvol_controller.delete_lvol(src_lvol)
    except Exception as e:
        # The cutover itself succeeded; a failed source delete is reported
        # loudly but does not un-succeed the task.
        logger.error(f"Source volume {src_lvol_id} could not be "
                     f"deleted after the cutover: {e}")


def _cutover_failed(task, err) -> NoReturn:
    """Turn a failed cutover attempt into the driver's retry semantics.

    Raises :class:`TaskDefer` for a transient connectivity failure — suspended,
    cooldown recorded, no retry consumed — and :class:`TaskRetry` otherwise.
    """
    params = task.function_params
    result = err or "cutover failed, retrying"
    # Keep the reason where the max-retry branch cannot overwrite it, and say
    # it out loud: a task that quietly retries to death costs a whole lab run
    # to diagnose (run 20260827_194551 -- 20 tasks, 160 attempts, no log line).
    params["last_error"] = result
    logger.warning("cutover attempt %d/%d failed for lvol %s: %s",
                   task.retry + 1, task.max_retry,
                   params.get("lvol_id"), result)
    # A retry re-claims the LVS on its next pass; holding the claim across the
    # wait would stall every other volume's replication for nothing.
    _release_lvs_claim(task)

    # When the failure happened inside run_cutover (tgt_lvol_composite already
    # set), it is likely a connectivity issue: the target node restarted, or the
    # control plane hasn't yet reflected the down state in DB.  Hammering at the
    # active poll interval burns all retries in seconds — long before the node
    # recovers.  Instead, re-read the node status and add a cooldown.
    if params.get("tgt_lvol_composite"):
        tgt_node_id = params.get("tgt_node_id")
        node_offline = False
        if tgt_node_id:
            try:
                current_tgt = db.get_storage_node_by_id(tgt_node_id)
                node_offline = current_tgt.status != StorageNode.STATUS_ONLINE
            except KeyError:
                node_offline = True
        hub_attempts = params.get("cutover_hub_attempts", 0) + 1
        params["cutover_hub_attempts"] = hub_attempts
        if node_offline or hub_attempts <= constants.REPL_CUTOVER_MAX_HUB_ATTEMPTS:
            # Transient: add a cooldown, do NOT burn task.retry.
            params["cutover_retry_after"] = (
                int(time.time()) + constants.REPL_CUTOVER_HUB_RETRY_COOLDOWN_SEC)
            logger.warning(
                "cutover for lvol %s: connectivity failure (attempt %d, node_offline=%s); "
                "waiting %ds before retry",
                params.get("lvol_id"), hub_attempts, node_offline,
                constants.REPL_CUTOVER_HUB_RETRY_COOLDOWN_SEC)
            raise TaskDefer(result)
        # Exceeded transient cap with node appearing online — real failure.
        logger.warning(
            "cutover for lvol %s: hub attach failed %d times with node appearing "
            "online; treating as real failure and burning a retry",
            params.get("lvol_id"), hub_attempts)
        params.pop("cutover_hub_attempts", None)
        params.pop("cutover_retry_after", None)

    raise TaskRetry(result)


def _swap_failback_lvol_uuid(rep, failback_source_id):
    """After a successful failback cutover, reassign the new clone's UUID to
    the original source lvol's UUID.

    The operator stores the original source lvolID in the ReplicationSlot's
    Spec.VolumeID. Preserving that UUID through the failback means the slot
    stays valid with no update required.

    The old source lvol DB record is removed first (its NVMf subsystem was
    already evicted by _evict_stale_namespace during _create_target_lvol_clone).
    The stale clone record at the old UUID is cleared last so there is never a
    window with two records under the same UUID.
    """
    try:
        new_lvol = db.get_lvol_by_id(rep.target_lvol.get_id())
    except KeyError:
        logger.error(
            "failback UUID swap: new clone %s not found in DB; skipping",
            rep.target_lvol.get_id())
        return

    stale_uuid = new_lvol.get_id()

    # The failover's _stop_source_replication() cleared do_replicate /
    # replication_interval_min /
    # replication_policy_id on the original cluster-1 source to stop it replicating.
    # Those cleared values must NOT be propagated here; instead restore them from
    # the failback source (cluster-2 volume) whose DB record is still intact at
    # this point — _stop_source_replication() clears its fields in a later step.
    failback_src_interval = 0
    failback_src_policy_id = ""
    try:
        failback_src = db.get_lvol_by_id(rep.source_lvol.get_id())
        failback_src_interval = failback_src.replication_interval_min
        failback_src_policy_id = failback_src.replication_policy_id
    except Exception as exc:
        logger.warning(
            "failback UUID swap: could not read failback source %s for interval/policy: %s",
            rep.source_lvol.get_id(), exc)

    # Copy replication_node_id / replication_mode from the original source —
    # these fields were NOT cleared by the failover's _stop_source_replication(),
    # so they still
    # point at the correct cluster-2 target node.
    try:
        old_lvol = db.get_lvol_by_id(failback_source_id)
        new_lvol.replication_node_id = old_lvol.replication_node_id
        new_lvol.replication_mode = old_lvol.replication_mode
        old_lvol.remove(db.kv_store)
    except KeyError:
        logger.warning(
            "failback UUID swap: original source lvol %s already absent from DB",
            failback_source_id)

    # Explicitly re-enable replication on the restored volume.  The original
    # source had do_replicate cleared during failover; failback means it is the
    # active source again and should resume its cadence.
    new_lvol.do_replicate = True
    new_lvol.replication_interval_min = failback_src_interval
    new_lvol.replication_policy_id = failback_src_policy_id

    # Write the clone under the original source UUID.
    new_lvol.uuid = failback_source_id
    new_lvol.write_to_db(db.kv_store)

    # Clear the stale record at the old clone UUID.
    stale = LVol()
    stale.uuid = stale_uuid
    stale.remove(db.kv_store)

    # Keep the relationship's target reference consistent.
    rep.target_lvol.uuid = failback_source_id
    rep.write_to_db(db.kv_store)

    logger.info(
        "failback UUID swap complete: clone %s reassigned to original source UUID %s",
        stale_uuid, failback_source_id)


def _acquire_lvs_claim(task, lvol, tasks):
    """Take the lvstore for this cutover's endgame, or defer while another
    volume holds it.

    Queueing here is cheap: it happens with a nearly-converged delta and NO
    snapshot in hand. The previous design claimed on the task's first pass and
    held the lvstore through the entire catch-up, so queued volumes sat on an
    ageing round-1 snapshot and their "round" measured the queue rather than
    the transfer.
    """
    params = task.function_params
    lvs_name = getattr(lvol, "lvs_name", "")
    own_group = _group_id_for_lvol(lvol)
    owner = _lvs_cutover_owner(task, lvs_name, tasks)
    if owner is not None:
        owner_group = (owner.function_params or {}).get("cutover_group") or ""
        owner_lvol = str((owner.function_params or {}).get("lvol_id"))
        # A consistency group cuts over AS A GROUP, so a sibling member joins
        # the owner rather than queueing behind it.
        if not (own_group and own_group == owner_group):
            params["shrink_deadline"] = (
                int(time.time()) + constants.REPL_CUTOVER_SHRINK_TIMEOUT_SEC)
            raise TaskDefer(f"queued for lvstore {lvs_name} behind "
                            f"{owner_lvol[:8]} (delta already converged)")
    params["cutover_lvs"] = lvs_name
    params["cutover_group"] = own_group
    xfer_timing.stamp("lvs_claim_acquired", lvol=lvol.get_id(), lvs=lvs_name,
                      round=params.get("shrink_round"))
    # Checkpoint, against the driver's usual ownership of task writes: the claim
    # has to be visible to the other hosts' scans for the whole cutover, not
    # only once the driver next writes the task.
    task.write_to_db(db.kv_store)


def _await_cutover_proceed(task, params):
    """Hold until the operator signals that target NVMe paths are connected.

    The operator calls POST .../replication/cutover-proceed once its preconnect
    Job succeeds. REPL_CUTOVER_PROCEED_TIMEOUT_SEC is the safety fallback so a
    cutover still proceeds if the operator is unavailable. Every second spent
    here is a second of writes the FROZEN final step must copy, because the
    cutover clone's base snapshot was taken before it -- with no operator to
    signal, the 120s fallback fired 34 times in soak run 20260827_110415 and
    produced 25-72s freezes. Only wait when the deployment actually has an
    operator posting cutover-proceed.
    """
    replication_id = params.get("replication_id")
    if not (replication_id and constants.REPL_CUTOVER_PROCEED_REQUIRED):
        return

    try:
        rep = db.get_lvol_replication_by_id(replication_id)
    except KeyError:
        logger.warning(
            "replication record %s not found; proceeding with cutover", replication_id)
        return

    if rep.cutover_proceed:
        return

    params.setdefault(
        "cutover_proceed_timeout",
        int(time.time()) + constants.REPL_CUTOVER_PROCEED_TIMEOUT_SEC)
    if int(time.time()) < params["cutover_proceed_timeout"]:
        xfer_timing.stamp("cutover_gate_wait", lvol=params.get("lvol_id"),
                          deadline=params["cutover_proceed_timeout"])
        raise TaskDefer("cutover_pending: waiting for preconnect signal")

    logger.warning(
        "cutover proceed timeout for replication %s; proceeding without signal",
        replication_id)


def task_runner(task: JobSchedule, tasks):
    params = task.function_params
    lvol_id = params.get("lvol_id")
    if not lvol_id:
        raise TaskRetry("missing lvol_id in task params")

    try:
        lvol = db.get_lvol_by_id(lvol_id)
        tgt_node = db.get_storage_node_by_id(params["tgt_node_id"])
    except KeyError as e:
        raise TaskRetry(f"object not found: {e}")

    # The source may be gone entirely (fail-over after cluster loss).
    src_node = None
    try:
        src_node = db.get_storage_node_by_id(params["src_node_id"])
    except KeyError:
        pass

    if tgt_node.status != StorageNode.STATUS_ONLINE:
        logger.warning("cutover for lvol %s waiting: target node %s is %s",
                       lvol_id, tgt_node.get_id(), tgt_node.status)
        params["last_error"] = (
            f"target node {tgt_node.get_id()[:8]} is {tgt_node.status}")
        # Deferred rather than failed: a node being offline is transient, and
        # burning a retry per pass would exhaust them all before it recovers.
        raise TaskDefer("target node not online, waiting")

    # Cooldown set after a hub-attach failure to give the target node time to
    # recover and the control plane time to reflect a down state in DB.
    if int(time.time()) < params.get("cutover_retry_after", 0):
        raise TaskDefer("waiting after connectivity failure")

    if src_node is None:
        raise TaskRetry("source node not found for cutover")

    # One line per runner pass: the spacing between these shows the task
    # scheduler's contribution, which is invisible in any per-phase number.
    xfer_timing.stamp("task_pass", lvol=lvol_id, state=task.status,
                      result=str(task.function_result)[:40].replace(" ", "_"))
    params.setdefault("start_time", int(time.time()))

    # ---- WAIT FOR THE VOLUME TO CATCH UP -------------------------------- #
    # The iterative snapshots ARE the endgame. Until then the volume just
    # replicates on its ordinary cadence: the cutover takes no snapshots of its
    # own and holds nothing, so it adds no load to a cluster that is still
    # catching up -- exactly when it can least afford it.
    if not params.get("cutover_lvs"):
        lag = _replication_lag_sec(lvol)
        # lag is None when the volume has no internal snapshot replicated in the
        # current source→target direction yet (e.g. a freshly set-up failback
        # pair that has not completed even one reverse-direction cycle). Treat
        # it as "no measurement" and proceed: the first shrink round captures all
        # outstanding delta, and _prepare_cutover will surface a proper error if
        # there is truly no base to chain onto. Only block when lag is a real
        # number that exceeds the gate.
        if lag is not None and lag > constants.REPL_CUTOVER_ENDGAME_LAG_SEC:
            xfer_timing.stamp("await_catchup", lvol=lvol_id, ms=lag * 1000.0)
            # Not a failure: no retry burned, and the deadline is pushed out
            # because catching up is legitimate progress, not a stall.
            params["shrink_deadline"] = (
                int(time.time()) + constants.REPL_CUTOVER_SHRINK_TIMEOUT_SEC)
            raise TaskDefer(
                "waiting for replication to catch up before the endgame "
                "(lag %.0fs > %ds)" % (lag, constants.REPL_CUTOVER_ENDGAME_LAG_SEC))

        # Caught up (or no measurement): take the lvstore for the endgame.
        _acquire_lvs_claim(task, lvol, tasks)
        xfer_timing.stamp("endgame_entered", lvol=lvol_id, ms=(lag or 0) * 1000.0)

    # The first iterative snapshot belongs to the endgame, not to task creation:
    # taken here, its delta covers only the catch-up residual.
    if not params.get("shrink_snap_id"):
        _, snap_err = _take_shrink_snapshot(task, lvol)
        if snap_err:
            raise TaskRetry(snap_err)
        # Checkpoint: the snapshot exists now, and a later attempt must chain
        # onto it rather than take a second one.
        task.write_to_db(db.kv_store)

    # ---- SHRINK PHASE --------------------------------------------------- #
    # Skip shrink entirely once the cutover clone is prepared: tgt_lvol_composite
    # being set means shrink already completed on a prior pass and the clone was
    # created from the resulting snapshot. Every retry after a failed run_cutover
    # should jump straight to run_cutover without redoing any shrink rounds.
    if "tgt_lvol_composite" not in params:
        if "shrink_snap_id" in params and params.get("shrink_round", 0) > 0:
            _shrink_step(task, lvol)

    # ---- CUTOVER PHASE (immediately after the last shrink round) -------- #
    # The freeze runs under the claim. A volume whose very first round was
    # already fast enough reaches here without having taken it.
    if not params.get("cutover_lvs"):
        _acquire_lvs_claim(task, lvol, tasks)
    if "tgt_lvol_composite" not in params:
        with xfer_timing.phase("prepare_cutover", lvol=lvol_id):
            _prepare_cutover(task, lvol, src_node, tgt_node)
        params = task.function_params

    _await_cutover_proceed(task, params)

    try:
        with xfer_timing.phase("run_cutover", lvol=lvol_id):
            ok, err = replication_final_step.run_cutover(
                src_node, tgt_node, lvol,
                params["tgt_lvol_composite"], params["tgt_map_id"],
                params["tgt_snap_composite"],
                operation=params.get("operation", "replicate"))
    except Exception as e:
        logger.error(f"Cutover raised: {e}", exc_info=True)
        _cutover_failed(task, str(e))

    if not ok:
        _cutover_failed(task, err)

    _record_cutover_done(task)


def _replication_lag_sec(lvol):
    """How far the target is behind: age of the newest REPLICATED snapshot.

    Returns None when no internal snapshot with target_replicated_snap_uuid
    exists for this volume in the current source→target direction.  The caller
    treats None as "no measurement available" and proceeds rather than blocking:
    this happens legitimately on failback pairs that have not yet completed
    their first reverse-direction replication cycle.

    Measuring lag from ordinary cadence replication costs nothing: the
    alternative (taking rounds to find out how fast a round is) adds snapshot
    and transfer load to a cluster that is still catching up, which is
    precisely when it can least afford it.
    """
    newest = None
    try:
        snaps = db.get_snapshots_by_node_id(lvol.node_id)
    except Exception as e:                                # noqa: BLE001
        logger.warning("Cannot read snapshots of %s to measure replication lag: %s",
                       lvol.get_id(), e)
        return None
    for s in snaps:
        if (s.lvol.get_id() == lvol.get_id()
                and s.snap_type == SnapShot.TYPE_INTERNAL
                and getattr(s, "target_replicated_snap_uuid", "")
                and (newest is None or s.created_at > newest.created_at)):
            newest = s
    if newest is None:
        return None
    return max(0.0, time.time() - float(newest.created_at))


def _take_shrink_snapshot(task, lvol):
    """Snapshot the source and record it as the round in flight."""
    from simplyblock_core.controllers import snapshot_controller
    params = task.function_params
    # The gap since the previous round finished: dead time between a round
    # completing and the next snapshot existing is pure added delta.
    xfer_timing.gap("round_gap_to_next_snapshot",
                    params.get("shrink_round_done_at"), lvol=lvol.get_id(),
                    round=params.get("shrink_round", 0))
    with xfer_timing.phase("take_shrink_snapshot", lvol=lvol.get_id(),
                           round=params.get("shrink_round", 0) + 1):
        new_snap, err = snapshot_controller.add(
            lvol.get_id(), f"repl_commit_{uuid_lib.uuid4()}",
            snap_type=SnapShot.TYPE_INTERNAL)
    if err:
        return None, f"shrink round {params.get('shrink_round', 0) + 1} snapshot failed: {err}"
    params["shrink_round"] = params.get("shrink_round", 0) + 1
    params["shrink_snap_id"] = new_snap
    params["shrink_started_at"] = time.time()
    return new_snap, None


def _shrink_round_done(snap_id):
    """True once the round's snapshot is replicated AND chained on the target.

    target_replicated_snap_uuid is set at replicate-finish, after the target
    copy is chained and converted -- replicated and usable in one signal.
    """
    try:
        return bool(db.get_snapshot_by_id(snap_id).target_replicated_snap_uuid)
    except KeyError:
        return None                                  # disappeared


def _inline_window(last_round_secs):
    """How long a pass may poll inline, given the last round's transfer time.

    The runner is single-threaded, so a flat multi-minute budget would stall
    every other volume's cutover behind this one. Scaling to the last round
    keeps the loop inline exactly where it matters -- near convergence, where
    the next snapshot must follow within milliseconds -- and yields early when
    rounds are still long, which is where yielding costs nothing because the
    freeze is far away regardless.

    The floor is REPL_CUTOVER_MIN_INLINE_SEC rather than zero because yielding
    hands the round back to the pass loop, and being picked up again costs far
    more than the poll it replaces: the requirement is that NO time is lost
    between a transfer completing and the next snapshot starting.
    """
    return min(constants.REPL_CUTOVER_CONVERGE_BUDGET_SEC,
               max(constants.REPL_CUTOVER_MIN_INLINE_SEC, last_round_secs * 3))


def _shrink_step(task, lvol):
    """Converge the delta, then hand straight over to the cutover.

    Returns once the delta is as small as it is going to get and the freeze
    may start IMMEDIATELY. Raises :class:`TaskDefer` while a round is still in
    flight -- waiting on the target is not a failure, so it must not consume a
    retry; the wait is bounded by ``shrink_deadline`` instead.

    This loop deliberately does NOT return to the task scheduler between
    rounds. Each return costs TASK_EXEC_INTERVAL_SEC (10s) before the next
    pass, and every one of those seconds is written by the client and lands in
    the next round -- a floor on the delta that more rounds cannot lower. Here
    a round ends and the next snapshot is taken within
    REPL_CUTOVER_POLL_INTERVAL_SEC, so a round only carries the writes made
    during the previous round's transfer.
    """
    params = task.function_params
    deadline = params.get("shrink_deadline", 0)
    # How long this pass may poll inline before handing the runner back. The
    # runner is single-threaded, so a flat multi-minute budget would stall
    # every OTHER volume's cutover behind this one. Scale it to how long the
    # last round actually took: near convergence the rounds are seconds and we
    # stay inline (which is the whole point -- the next snapshot must follow
    # within milliseconds), while a slow early round yields quickly, and
    # yielding costs nothing there because we are far from the freeze anyway.
    budget_end = time.time() + _inline_window(
        (params.get("shrink_round_times") or [0])[-1])

    while True:
        if int(time.time()) > deadline:
            # Deadline expired: stop adding rounds and fall through to cutover
            # rather than failing and burning a retry.  The freeze that follows
            # is slightly larger than if we had converged, but proceeding is
            # always better than another 900-second wait.
            logger.warning(
                "cutover convergence: lvol=%s shrink deadline expired after %d "
                "rounds; proceeding to cutover", lvol.get_id(),
                params.get("shrink_round", 0))
            return

        snap_id = params["shrink_snap_id"]
        done = _shrink_round_done(snap_id)
        if done is None:
            raise TaskRetry(f"shrink snapshot {snap_id} disappeared")

        if not done:
            if time.time() >= budget_end:
                # Give the pass back so the runner can service other tasks;
                # the round is still in flight and resumes on the next pass.
                raise TaskDefer(f"shrink round {params['shrink_round']}: waiting "
                                f"for {snap_id[:8]} to replicate")
            time.sleep(constants.REPL_CUTOVER_POLL_INTERVAL_SEC)
            continue

        started_at = params.get("shrink_started_at")
        if started_at is None:
            # Unmeasurable round (an older task, or one enqueued without the
            # stamp). Treat it as NOT converged rather than as instant: a
            # missing measurement must never be read as "the delta is small",
            # which is precisely the mistake that kept the freeze at 9-55s.
            elapsed = float("inf")
            logger.warning(
                "cutover convergence: lvol=%s round %d has no start stamp; "
                "taking another round rather than assuming it was fast",
                lvol.get_id(), params["shrink_round"])
        else:
            elapsed = time.time() - started_at
            params.setdefault("shrink_round_times", []).append(round(elapsed, 2))
            logger.info("cutover convergence: lvol=%s round %d transferred in %.2fs",
                        lvol.get_id(), params["shrink_round"], elapsed)
            # Structured twin of the line above. round_total is snapshot-taken
            # to replicated-and-chained, i.e. it INCLUDES all orchestration --
            # compare it against the transfer phase from snapshot_replication
            # to see how much is data movement.
            xfer_timing.stamp("round_total", lvol=lvol.get_id(),
                              snap=params.get("shrink_snap_id"),
                              round=params["shrink_round"],
                              ms=elapsed * 1000.0)
            params["shrink_round_done_at"] = time.time()

        # Every round is an endgame round now -- the lvstore is held before the
        # first iterative snapshot is taken.
        if elapsed <= constants.REPL_CUTOVER_CONVERGE_TARGET_SEC:
            task.function_result = (f"converged in {params['shrink_round']} rounds "
                                    f"(last {elapsed:.2f}s)")
            return

        if params["shrink_round"] >= constants.REPL_CUTOVER_MAX_SHRINK_ROUNDS:
            # Written faster than it replicates. Freezing now is still the best
            # move -- the freeze at least stops the writes -- but say so.
            logger.warning(
                "cutover convergence: lvol=%s did not converge in %d rounds "
                "(last round %.2fs > %.2fs target); freezing anyway",
                lvol.get_id(), params["shrink_round"], elapsed,
                constants.REPL_CUTOVER_CONVERGE_TARGET_SEC)
            task.function_result = (f"not converged after {params['shrink_round']} "
                                    f"rounds (last {elapsed:.2f}s)")
            return

        # IMMEDIATELY take the next snapshot. This is the whole mechanism: the
        # next round carries only what was written while this one transferred,
        # so waiting here -- for the scheduler or for anything else -- puts
        # those seconds straight into the freeze. It happens before any
        # yielding decision for exactly that reason.
        _, err = _take_shrink_snapshot(task, lvol)
        if err:
            raise TaskRetry(err)
        # Checkpoint: the round's snapshot exists now and must not be retaken.
        task.write_to_db(db.kv_store)

        # Re-arm the inline window from what this round just measured: rounds
        # near convergence are short and stay inline, a slow one hands the
        # runner back so other volumes' cutovers are not stuck behind it.
        budget_end = time.time() + _inline_window(
            elapsed if elapsed != float("inf") else 0)
        if time.time() >= budget_end:
            raise TaskDefer(f"shrink round {params['shrink_round'] - 1} done "
                            f"({elapsed:.2f}s); continuing next pass")


def _prepare_cutover(task, lvol, src_node, tgt_node):
    """Build the target clone on the last replicated snapshot and record the
    relationship. Runs once, immediately after the shrink phase, so the base is
    guaranteed to be the freshly replicated shrink snapshot."""
    from simplyblock_core.controllers import lvol_controller

    # Same resolution as fail-over: the destination is the cluster the volume
    # replicates INTO, and the pool comes from its policy's target. The source
    # cluster's outgoing config is only a fallback -- on a fail-back cutover it
    # describes the wrong direction, or is not set at all for a policy-driven
    # volume.
    target_cluster, target_pool_uuid = lvol_controller.resolve_replication_destination(
        db, lvol, tgt_node, src_node)

    new_lvol, snapshot, error = lvol_controller._clone_from_last_replicated(
        db, lvol.get_id(), lvol, tgt_node, target_pool_uuid, src_node.cluster_id,
        for_migration=True)
    if error:
        raise TaskRetry(f"cutover clone failed: {error}")

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.write_to_db(db.kv_store)
    # Expose inaccessible until the final step flips ANA.
    lvol_controller.suspend_lvol(new_lvol.get_id())

    tgt_map_id = lvol_controller._resolve_target_map_id(tgt_node, new_lvol.lvol_bdev)
    if tgt_map_id is None:
        lvol_controller.delete_lvol_from_node(new_lvol, tgt_node)
        db.release_lvol_ns_slot(new_lvol)
        raise TaskRetry("could not resolve target map_id")

    rep = LVolReplication()
    rep.uuid = str(uuid_lib.uuid4())
    rep.create_dt = str(datetime.now())
    rep.source_lvol = lvol
    rep.target_lvol = new_lvol
    rep.source_cluster_id = src_node.cluster_id
    rep.target_cluster_id = target_cluster.get_id()
    rep.mode = lvol.replication_mode
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.direction = LVolReplication.DIRECTION_TO_TARGET
    rep.target_nqn = new_lvol.nqn
    rep.target_ns_id = new_lvol.ns_id
    rep.write_to_db(db.kv_store)

    # Detect failback: if the current replication source (lvol) was previously
    # the TARGET in a completed relationship whose source cluster is now tgt_node,
    # this is a failback cutover. Store the original source UUID so the success
    # path can
    # reassign it to the new clone, keeping the operator's VolumeID valid.
    for prior in db.get_lvol_replication_objects():
        if (prior.target_lvol and prior.target_lvol.get_id() == lvol.get_id()
                and prior.source_cluster_id == tgt_node.cluster_id
                and prior.source_lvol):
            task.function_params["failback_source_lvol_id"] = prior.source_lvol.get_id()
            task.function_params["failback_prior_replication_id"] = prior.get_id()
            logger.info(
                "failback cutover detected: original source UUID %s will be "
                "preserved on new clone %s after cutover",
                prior.source_lvol.get_id(), new_lvol.get_id())
            break

    task.function_params.update({
        "tgt_lvol_composite": new_lvol.top_bdev,
        "tgt_map_id": tgt_map_id,
        "tgt_snap_composite": snapshot.snap_bdev,
        "replication_id": rep.get_id(),
    })
    # Checkpoint the task itself, against the driver's usual ownership of task
    # writes: the clone and the LVolReplication record above already exist, so a
    # crash before the cutover completes must not let the next attempt build a
    # second clone.
    task.write_to_db(db.kv_store)


def _any_cutover_in_flight(tasks):
    """True while some cutover is between its first snapshot and its freeze.

    Only then is a sub-second pass interval worth paying for: that is the
    window where a completed transfer must be picked up immediately.
    """
    for t in tasks:
        if t.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if t.status == JobSchedule.STATUS_DONE or t.canceled:
            continue
        params = t.function_params or {}
        # claimed the lvstore, or already has a round in flight
        if params.get("cutover_lvs") or params.get("shrink_snap_id"):
            return True
    return False


def _poll_interval(tasks):
    """Poll fast only while a cutover is converging.

    Deliberately NOT a sub-second poll: a pass reads the task table (and each
    task), so polling it at 5Hz burns transactions proportional to
    clusters x tasks to learn nothing almost every time. The latency that
    mattered is gone from the hot path instead -- a transfer is awaited and
    finished in the pass that submitted it
    (snapshot_replication._await_transfer_completion, an RPC poll), and a
    converging round stays inside its own inline loop.
    """
    return (constants.REPL_CUTOVER_ACTIVE_POLL_SEC if _any_cutover_in_flight(tasks)
            else constants.TASK_EXEC_INTERVAL_SEC)


SPEC = RunnerSpec(
    name="tasks-runner-replication-final",
    function_names=[JobSchedule.FN_REPLICATION_FINAL],
    handler=task_runner,
    # The lvstore claim has to see the other cutovers in this pass, and the
    # driver has already read them.
    wants_cycle_tasks=True,
    dynamic_interval=_poll_interval,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
