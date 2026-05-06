# coding=utf-8
"""SPDK-driven :class:`MoveExecutor` for single-node cluster expansion.

This module is the seam between the orchestrator's pure cursor/persistence
loop and the real side-effecting glue. Each :class:`RoleMove` produced by
``compute_role_diff`` is dispatched here and translated into:

* DB back-reference updates so that ``recreate_lvstore_on_sec`` iterates
  exactly the right primaries when called next.
* Calls into existing primitives — ``recreate_lvstore`` for the newcomer's
  primary, ``recreate_lvstore_on_sec`` for sec/tert builds,
  ``teardown_non_leader_lvstore`` for donor cleanup, and
  ``reattach_sibling_failover`` for the sec_2 multipath fix-up when sec_1
  is moved.

There is no JC quorum polling: the synchronous create chain inside
``recreate_lvstore_on_sec`` (``_create_bdev_stack`` →
``connect_to_hublvol`` → ``bdev_examine``) is its own guarantee, exactly
as it is for restart paths today.

Per-move algorithms are documented in ``step6_design.md``.
"""

from simplyblock_core import utils
from simplyblock_core.cluster_expand_orchestrator import (
    MoveExecutor,
    execute_expand_plan,
)
from simplyblock_core.cluster_expand_planner import (
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RoleMove,
    compute_role_diff,
    is_expand_in_progress,
)
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# Map planner role string → (which_sec slot field-suffix on
# storage_node, primary's pointer attribute name). ``"primary"`` is not in
# this map — the create-primary path bypasses it.
_SEC_SLOT_BY_ROLE = {
    ROLE_SECONDARY: ("_1", "secondary_node_id"),
    ROLE_TERTIARY: ("_2", "tertiary_node_id"),
}


class SpdkMoveExecutor(MoveExecutor):
    """Real executor driving SPDK + DB to perform a :class:`RoleMove`.

    Constructed once per ``execute_expand_plan`` invocation. Holds a
    reference to the cluster (for ``distr_*`` config used by
    create-primary moves) and a DBController. Both default to
    fetching/instantiating from the singleton if not provided — primarily
    so unit tests can inject mocks.
    """

    def __init__(self, cluster=None, db_controller=None):
        self._cluster = cluster
        self._db = db_controller
        # Cluster capacity / max_size is needed for create-primary; cached
        # after first compute since capacity does not change mid-expansion.
        self._max_size = None

    # -- helpers ------------------------------------------------------------

    def _db_ctrl(self):
        if self._db is None:
            from simplyblock_core.db_controller import DBController
            self._db = DBController()
        return self._db

    def _get_cluster(self):
        if self._cluster is None:
            raise RuntimeError(
                "SpdkMoveExecutor: cluster not set; cannot read distr_* "
                "config for create-primary moves")
        return self._cluster

    def _get_max_size(self):
        if self._max_size is None:
            cluster = self._get_cluster()
            records = self._db_ctrl().get_cluster_capacity(cluster)
            self._max_size = records[0]['size_total']
        return self._max_size

    # -- dispatch -----------------------------------------------------------

    def execute(self, move: RoleMove) -> None:
        if move.role == ROLE_PRIMARY:
            if not move.is_create:
                raise ValueError(
                    f"SpdkMoveExecutor: primary moves are create-only, got {move}")
            self._execute_create_primary(move)
            return

        if move.role not in _SEC_SLOT_BY_ROLE:
            raise ValueError(f"unknown role in RoleMove: {move.role!r}")

        slot_suffix, primary_ptr_attr = _SEC_SLOT_BY_ROLE[move.role]
        if move.is_create:
            self._execute_create_sec(move, slot_suffix, primary_ptr_attr)
        else:
            self._execute_rehome_sec(move, slot_suffix, primary_ptr_attr)

    # -- create-primary (Phase B) ------------------------------------------

    def _execute_create_primary(self, move: RoleMove) -> None:
        from simplyblock_core import storage_node_ops
        db = self._db_ctrl()
        cluster = self._get_cluster()
        max_size = self._get_max_size()

        snode = db.get_storage_node_by_id(move.to_node_id)
        ok = storage_node_ops.create_lvstore(
            snode,
            cluster.distr_ndcs,
            cluster.distr_npcs,
            cluster.distr_bs,
            cluster.distr_chunk_bs,
            cluster.page_size_in_blocks,
            max_size,
        )
        snode = db.get_storage_node_by_id(snode.get_id())
        if not ok:
            snode.lvstore_status = "failed"
            snode.write_to_db()
            raise RuntimeError(
                f"create_lvstore failed on new primary {snode.get_id()}")
        snode.lvstore_status = "ready"
        snode.write_to_db()

    # -- create-secondary / create-tertiary (Phase B) ----------------------

    def _execute_create_sec(self, move: RoleMove,
                            slot_suffix: str, primary_ptr_attr: str) -> None:
        from simplyblock_core import storage_node_ops
        db = self._db_ctrl()

        holder = db.get_storage_node_by_id(move.to_node_id)
        primary = db.get_storage_node_by_id(move.lvs_primary_node_id)

        # 1. Set holder's back-reference (string field that
        #    recreate_lvstore_on_sec uses to discover its iteration set).
        holder.lvstore_stack_secondary = primary.get_id()
        holder.jm_ids = list(set(primary.jm_ids+holder.jm_ids))
        holder.write_to_db()

        # 2. Update primary's pointer.
        setattr(primary, primary_ptr_attr, holder.get_id())
        primary.write_to_db()

        # 3. Refresh + recreate. The holder is the newcomer (or another
        #    node that just gained this role); recreate iterates by the
        #    back-references we just set, so this is surgical.
        holder = db.get_storage_node_by_id(holder.get_id())
        ok = storage_node_ops.recreate_lvstore_on_non_leader(holder, primary, primary, activation_mode=True)
        if ok is False:
            raise RuntimeError(
                f"recreate_lvstore_on_sec failed on {holder.get_id()} "
                f"for newly-assigned LVS@{primary.get_id()} role={move.role}")

    # -- re-home secondary / re-home tertiary (Phase A) --------------------

    def _execute_rehome_sec(self, move: RoleMove,
                            slot_suffix: str, primary_ptr_attr: str) -> None:
        from simplyblock_core import storage_node_ops
        db = self._db_ctrl()

        donor = db.get_storage_node_by_id(move.from_node_id)
        recipient = db.get_storage_node_by_id(move.to_node_id)
        primary = db.get_storage_node_by_id(move.lvs_primary_node_id)

        # Pre-checks: refuse to act on offline donor/recipient. Aborting
        # here is preferable to half-applying the move and leaving stale
        # pointers behind.
        if donor.status != StorageNode.STATUS_ONLINE:
            raise RuntimeError(
                f"re-home {move.role} for LVS@{primary.get_id()}: donor "
                f"{donor.get_id()} status is {donor.status!r}, expected ONLINE")
        if recipient.status != StorageNode.STATUS_ONLINE:
            raise RuntimeError(
                f"re-home {move.role} for LVS@{primary.get_id()}: recipient "
                f"{recipient.get_id()} status is {recipient.status!r}, expected ONLINE")

        # 1. Update DB so recreate_lvstore_on_sec(recipient) will pick this
        #    primary up in its iteration. Set the recipient back-ref before
        #    flipping the primary's pointer so a crash between the two
        #    leaves the recipient's record claiming to host a role its
        #    primary isn't yet pointing at — recoverable on resume by
        #    re-running the same move.
        recipient.lvstore_stack_secondary = primary.get_id()
        recipient.write_to_db()

        setattr(primary, primary_ptr_attr, recipient.get_id())
        primary.write_to_db()

        # 2. Build the new sec/tert stack on recipient. The function
        #    iterates by DB back-references, which now include this
        #    primary.
        recipient = db.get_storage_node_by_id(recipient.get_id())
        ok = storage_node_ops.recreate_lvstore_on_non_leader(
            recipient, primary, primary, activation_mode=True)
        if ok is False:
            raise RuntimeError(
                f"recreate_lvstore_on_sec failed on recipient "
                f"{recipient.get_id()} during re-home of LVS@{primary.get_id()}")

        # 3. Tear down the donor's stack for this LVS. The primary's
        #    pointer was already moved to the recipient in step 1, so we
        #    pass the slot explicitly — auto-discovery would refuse.
        donor = db.get_storage_node_by_id(donor.get_id())
        primary = db.get_storage_node_by_id(primary.get_id())
        if not storage_node_ops.teardown_non_leader_lvstore(
                donor, primary, slot=slot_suffix):
            raise RuntimeError(
                f"teardown_non_leader_lvstore failed for donor "
                f"{donor.get_id()} of LVS@{primary.get_id()}")

        # 4. Sec_1 moves require the sibling sec_2 to repoint its multipath
        #    failover path from donor to recipient. Sec_2 moves don't have
        #    this problem (sec_1 was unchanged; only sec_2 itself moved).
        if slot_suffix == "_1":
            primary = db.get_storage_node_by_id(primary.get_id())
            if primary.tertiary_node_id:
                sibling = db.get_storage_node_by_id(primary.tertiary_node_id)
                if sibling.status == StorageNode.STATUS_ONLINE:
                    storage_node_ops.reattach_sibling_failover(
                        sibling, primary,
                        old_failover_node=donor,
                        new_failover_node=recipient)
                else:
                    logger.warning(
                        f"re-home sec_1 for LVS@{primary.get_id()}: sibling "
                        f"sec_2 {sibling.get_id()} is {sibling.status!r}, "
                        f"skipping failover reattach (will be re-established "
                        f"when the sibling restarts)")


def integrate_new_node_into_cluster(cluster, new_snode, executor=None,
                                    db_controller=None,
                                    manage_cluster_status=False):
    """Plan and execute the role rebalance to integrate a freshly-added
    node into a running cluster.

    This is the entry point invoked by ``sbctl sn add --expansion``
    *after* the regular ``add_node`` flow (devices, alcemls, JM device,
    SPDK started) has completed and ``new_snode`` is online but holds no
    LVS roles yet.

    Two modes:

    * **Resume** — if ``cluster.expand_state`` already has an in-progress
      plan, that plan resumes from its persisted cursor and ``new_snode``
      is ignored (an interrupted prior expansion takes priority over a
      fresh add).
    * **Fresh plan** — compute the role diff against the current
      rotation, persist the initial state, and drive the orchestrator.

    Parameters
    ----------
    cluster:
        ``Cluster`` model instance. Mutated by the orchestrator
        (``expand_state`` updates persisted as we go).
    new_snode:
        The storage node that just joined. Used only for its UUID — must
        already be ONLINE in DB.
    executor:
        Optional :class:`MoveExecutor` injection point — primarily for
        tests and the planned ``--dry-run`` flag. Defaults to a real
        :class:`SpdkMoveExecutor`.
    db_controller:
        Optional DB handle for tests.
    manage_cluster_status:
        When True, set cluster status to ``IN_EXPANSION`` for the
        duration of the call and back to ``ACTIVE`` on success (or to
        the original status on failure). When False (default), status
        transitions are the caller's responsibility — useful for tests.
    """
    if db_controller is None:
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()

    if executor is None:
        executor = SpdkMoveExecutor(cluster=cluster,
                                    db_controller=db_controller)

    old_status = cluster.status
    if manage_cluster_status:
        from simplyblock_core import cluster_ops
        from simplyblock_core.models.cluster import Cluster
        cluster_ops.set_cluster_status(
            cluster.get_id(), Cluster.STATUS_IN_EXPANSION)

    try:
        # Resume takes priority. The orchestrator's own contract refuses
        # planned_moves on resume, so we don't pass them here.
        if is_expand_in_progress(cluster.expand_state or {}):
            logger.info(
                f"integrate_new_node_into_cluster: resuming in-progress plan "
                f"(new_snode={new_snode.get_id()} ignored until current "
                f"plan completes)")
            execute_expand_plan(cluster, executor)
        else:
            # Build the rotation from existing primaries (excluding the
            # newcomer itself, which has just joined and has no lvstore yet).
            snodes = db_controller.get_storage_nodes_by_cluster_id(
                cluster.get_id())
            existing = [n.get_id() for n in snodes
                        if n.lvstore
                        and n.status == StorageNode.STATUS_ONLINE
                        and n.get_id() != new_snode.get_id()]

            moves = compute_role_diff(
                existing, new_snode.get_id(), cluster.max_fault_tolerance)

            logger.info(
                f"integrate_new_node_into_cluster: planned {len(moves)} "
                f"moves to integrate {new_snode.get_id()} into rotation of "
                f"{len(existing)} existing primaries (FTT="
                f"{cluster.max_fault_tolerance})")

            execute_expand_plan(
                cluster, executor,
                planned_moves=moves, new_node_id=new_snode.get_id())
    except Exception:
        if manage_cluster_status:
            from simplyblock_core import cluster_ops
            cluster_ops.set_cluster_status(cluster.get_id(), old_status)
        raise

    if manage_cluster_status:
        from simplyblock_core import cluster_ops
        from simplyblock_core.models.cluster import Cluster
        cluster_ops.set_cluster_status(
            cluster.get_id(), Cluster.STATUS_ACTIVE)
