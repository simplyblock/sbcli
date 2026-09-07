"""Release-specific upgrade step: hold JC compression across the upgrade.

Shipped with release RC26.3 (first tag: RC26.3-RC1). DELETE this module —
and the ``resume_is_held`` guards it exports — in the next release. Guarded call sites (grep for ``resume_is_held``):
  * storage_node_ops._recreate_lvstore_on_non_leader_impl (restart resume)
  * storage_node_ops.create_lvstore (initial resume on the secondary)
  * services/tasks_runner_jc_comp.py (FN_JC_COMP_RESUME runner)
  * services/tasks_runner_migration.py (resume after data migration)
  * services/lvol_monitor.py resume_comp (resume after delete-rebalance)
  * cluster_ops.cluster_activate (resume on activation from UNREADY)

Flow:
  1. ``pre_update`` — first step of ``cluster update``: refuses unless every
     storage node is ONLINE, suspends JC compression on every member of
     every LVS group, waits until no compression is running anywhere (rolls
     the suspends back and aborts on timeout/failure), then persists a hold
     flag on the cluster.
  2. While the hold flag is set, every resume path listed above is a no-op,
     so node restarts during the roll never re-activate compression.
  3. ``upgrade_complete`` — backs ``sbctl cluster upgrade-complete``: clears
     the hold, then resumes compression on every LVS group member; a member
     with active node tasks (data migration) gets a FN_JC_COMP_RESUME task
     instead, which the runner executes once the migrations are done.
"""

import time

from simplyblock_core import utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import ReleaseUpgradeError, UpgradePlugin

logger = utils.get_logger(__name__)

STATE_KEY = "jc_comp_hold"

DRAIN_POLL_SEC = 60
DRAIN_MAX_POLLS = 10


def resume_is_held(cluster) -> bool:
    """True while ``cluster update`` has suspended JC compression and
    ``cluster upgrade-complete`` has not run yet. Release-specific guard —
    remove together with this module."""
    return bool(cluster.release_upgrade_state.get(STATE_KEY))


def _lvs_group_members(db, cluster_id):
    """Yield (member_node, jm_vuid) for every member of every LVS group:
    the primary itself plus its secondary and tertiary peers."""
    nodes = {n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
             if n.status != StorageNode.STATUS_REMOVED}
    for primary in nodes.values():
        if not primary.lvstore or not primary.jm_vuid:
            continue
        member_ids = [primary.get_id(), primary.secondary_node_id, primary.tertiary_node_id]
        seen = set()
        for member_id in member_ids:
            if not member_id or member_id in seen or member_id not in nodes:
                continue
            seen.add(member_id)
            yield nodes[member_id], primary.jm_vuid


class JCCompressionUpgrade(UpgradePlugin):
    name = "jc-compression-hold"
    # Covers every build of the release: RC26.3-RC1, -RC2, ..., final.
    to_release = "RC26.3"
    from_release = ""
    STATE_KEY = STATE_KEY

    def pre_update(self, cluster) -> None:
        db = DBController()
        cluster_id = cluster.get_id()

        offline = [n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
                   if n.status not in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_ONLINE]]
        if offline:
            raise ReleaseUpgradeError(
                "cluster update requires every storage node to be online; not online: "
                + ", ".join(f"{n.get_id()} ({n.status})" for n in offline))

        pairs = list(_lvs_group_members(db, cluster_id))

        suspended = []
        try:
            for member, jm_vuid in pairs:
                ret, err = member.rpc_client().jc_suspend_compression(jm_vuid=jm_vuid, suspend=True)
                if ret:
                    suspended.append((member, jm_vuid))
                elif err:
                    logger.info(f"JC compression suspend not applicable on node "
                                f"{member.get_id()}, JM: {jm_vuid}: {err}")
                else:
                    raise ReleaseUpgradeError(
                        f"failed to suspend JC compression on node {member.get_id()}, JM: {jm_vuid}")

            pending = list(suspended)
            for _ in range(DRAIN_MAX_POLLS):
                pending = [(member, jm_vuid) for member, jm_vuid in pending
                           if member.rpc_client().jc_compression_get_status(jm_vuid)]
                if not pending:
                    break
                logger.info("JC compression still running on: "
                            + ", ".join(f"{m.get_id()}/JM:{v}" for m, v in pending)
                            + f", retrying in {DRAIN_POLL_SEC} seconds")
                time.sleep(DRAIN_POLL_SEC)
            if pending:
                raise ReleaseUpgradeError(
                    "timeout waiting for JC compression to finish on: "
                    + ", ".join(f"{m.get_id()}/JM:{v}" for m, v in pending))
        except ReleaseUpgradeError:
            self._rollback(suspended)
            raise
        except Exception as e:
            self._rollback(suspended)
            raise ReleaseUpgradeError(f"failed to suspend JC compression: {e}")

        cluster = db.get_cluster_by_id(cluster_id)
        cluster.release_upgrade_state[STATE_KEY] = {"to_release": self.to_release}
        cluster.write_to_db(db.kv_store)
        logger.info("JC compression suspended cluster-wide; resume is held until "
                    "`cluster upgrade-complete` is run")

    @staticmethod
    def _rollback(suspended) -> None:
        for member, jm_vuid in suspended:
            try:
                member.rpc_client().jc_suspend_compression(jm_vuid=jm_vuid, suspend=False)
            except Exception as e:
                logger.error(f"Failed to roll back JC compression suspend on node "
                             f"{member.get_id()}, JM: {jm_vuid}: {e}")

    def upgrade_complete(self, cluster) -> list:
        db = DBController()
        cluster_id = cluster.get_id()

        offline = [n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
                   if n.status not in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_ONLINE]]
        if offline:
            raise ReleaseUpgradeError(
                "upgrade-complete requires every storage node to be online; not online: "
                + ", ".join(f"{n.get_id()} ({n.status})" for n in offline))

        # Clear the hold first so the resume tasks queued below are not
        # blocked by the resume_is_held guards.
        cluster = db.get_cluster_by_id(cluster_id)
        cluster.release_upgrade_state.pop(STATE_KEY, None)
        cluster.write_to_db(db.kv_store)

        messages = []
        for member, jm_vuid in _lvs_group_members(db, cluster_id):
            member_id = member.get_id()
            if tasks_controller.get_active_node_tasks(cluster_id, member_id):
                # Data migration in flight: resume only once it completes.
                if not tasks_controller.get_jc_comp_task(cluster_id, member_id, jm_vuid=jm_vuid):
                    tasks_controller.add_jc_comp_resume_task(cluster_id, member_id, jm_vuid=jm_vuid)
                messages.append(f"node {member_id} JM:{jm_vuid}: node busy (data migration), "
                                f"resume task queued")
                continue
            try:
                ret, err = member.rpc_client().jc_suspend_compression(jm_vuid=jm_vuid, suspend=False)
            except Exception as e:
                logger.error(e)
                ret, err = False, None
            if ret:
                messages.append(f"node {member_id} JM:{jm_vuid}: compression resumed")
            elif err:
                messages.append(f"node {member_id} JM:{jm_vuid}: resume not applicable: {err}")
            else:
                if not tasks_controller.get_jc_comp_task(cluster_id, member_id, jm_vuid=jm_vuid):
                    tasks_controller.add_jc_comp_resume_task(cluster_id, member_id, jm_vuid=jm_vuid)
                messages.append(f"node {member_id} JM:{jm_vuid}: resume failed, resume task queued")
        return messages
