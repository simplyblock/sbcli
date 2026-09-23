"""The FDB record may only be removed once the teardown is confirmed.

R26.3 field report: four lvols existed as bdevs in SPDK with no record in FDB.
One had been created and deleted per the cluster log but never passed through
in_deletion; two showed a create and no delete at all; the fourth left no
cluster-log trace whatsoever. Every path behind that shares one shape — the
record removal did not depend on the data-plane teardown having happened:

  * ``process_lvol_delete_finish`` skipped the leader's sync delete outright
    when the leader was merely unreachable or restarting, logged a failure and
    stepped over it, and then removed the record regardless. The leader's sync
    delete is the ONLY operation that removes the blob metadata and unregisters
    the bdev.
  * ``delete_lvol_from_node`` answered ``True`` for nodes it had decided not to
    touch (``check_non_leader_for_operation`` → "skip"/"queue"), so a caller
    could not tell a completed teardown from one never attempted.
  * ``rollback_create_record``'s guard existed on exactly one create path; every
    other create/clone rollback called ``release_lvol_ns_slot`` straight out,
    erasing the record of an attempt whose blob had survived.
  * ``delete_lvol``'s missing-node force branch erased the record without a
    single RPC and without an event — for an HA volume the blob lives on every
    LVS member, whose node records usually still exist.

The state under test IS database state (record present/absent, status
transitions, ``sync_deleted_nodes``), so these belong to the FDB-backed tier.
Mocked here, per the tier's rule, is everything *above* the database: the SPDK
RPC client and the leadership probes, which the integration tier never talks to.
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import lvol_controller as lc
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import lvol_monitor

CLUSTER_ID = "cluster-1"
POOL_ID = "pool-1"
LEADER_ID = "node-leader"
PEER_ID = "node-peer"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _write_cluster(db):
    cl = Cluster()
    cl.uuid = CLUSTER_ID
    cl.status = Cluster.STATUS_ACTIVE
    cl.write_to_db(db.kv_store)
    return cl


def _write_pool(db):
    pool = Pool()
    pool.uuid = POOL_ID
    pool.cluster_id = CLUSTER_ID
    pool.pool_name = "pool"
    pool.status = Pool.STATUS_ACTIVE
    pool.write_to_db(db.kv_store)
    return pool


def _write_node(db, uuid, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = uuid
    node.cluster_id = CLUSTER_ID
    node.status = status
    node.lvstore = "LVS_1"
    node.lvstore_status = "ready"
    node.write_to_db(db.kv_store)
    return node


def _write_lvol(db, uuid="lvol-1", status=LVol.STATUS_IN_DELETION, nodes=None):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.lvol_name = "LVOL_1"
    lvol.lvol_bdev = "LVOL_1"
    lvol.lvs_name = "LVS_1"
    lvol.pool_uuid = POOL_ID
    lvol.node_id = LEADER_ID
    lvol.nodes = nodes if nodes is not None else [LEADER_ID, PEER_ID]
    lvol.ha_type = "ha"
    lvol.status = status
    lvol.bdev_stack = [{"type": "bdev_lvol", "name": "LVOL_1",
                        "params": {"lvs_name": "LVS_1", "name": "LVOL_1"}}]
    lvol.write_to_db(db.kv_store)
    return lvol


def _record_exists(db, uuid):
    try:
        db.get_lvol_by_id(uuid)
        return True
    except KeyError:
        return False


# ---------------------------------------------------------------------------
# process_lvol_delete_finish — the gate
# ---------------------------------------------------------------------------


@pytest.fixture
def finish_env(db):
    """Cluster + pool + leader/peer nodes + one in_deletion lvol."""
    cluster = _write_cluster(db)
    _write_pool(db)
    leader = _write_node(db, LEADER_ID)
    _write_node(db, PEER_ID)
    lvol = _write_lvol(db)
    return cluster, leader, lvol


def _finish(cluster, lvol, *, teardown, absent, peer_cleared=True):
    """Run process_lvol_delete_finish with the data plane stubbed out.

    ``teardown``: ``None`` for a confirmed teardown (delete_lvol_from_node
    returns normally), or an exception instance it should raise instead
    (``PreconditionError`` for deferred, ``RuntimeError`` for failed).
    ``absent``: ``True``/``False`` for lvol_bdev_absent_on_node's bool
    return, or an exception instance for an unverifiable probe.
    """
    with patch.object(lvol_monitor.lvol_controller, "delete_lvol_from_node",
                      side_effect=teardown) as del_node, \
            patch.object(lvol_monitor.lvol_controller, "lvol_bdev_absent_on_node",
                         side_effect=absent if isinstance(absent, Exception) else None,
                         return_value=absent if not isinstance(absent, Exception) else None), \
            patch.object(lvol_monitor.snapshot_controller, "sync_delete_on_peer",
                         return_value=peer_cleared), \
            patch.object(lvol_monitor.snapshot_controller, "lvstore_op_lock",
                         MagicMock()), \
            patch.object(lvol_monitor.tasks_controller, "add_lvol_sync_del_task",
                         MagicMock()), \
            patch.object(lvol_monitor.lvol_events, "lvol_delete", MagicMock()), \
            patch.object(StorageNode, "lvol_del_sync_lock", MagicMock()), \
            patch.object(StorageNode, "lvol_del_sync_lock_reset", MagicMock()), \
            patch.object(StorageNode, "rpc_client", MagicMock()):
        lvol_monitor.process_lvol_delete_finish(cluster, lvol,
                                                leader_independent=True)
    return del_node


class TestFinishGate:

    def test_confirmed_teardown_removes_the_record(self, db, finish_env):
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=None, absent=True)
        assert not _record_exists(db, lvol.get_id())

    def test_a_failed_leader_teardown_keeps_the_record(self, db, finish_env):
        """The headline leak: 'Failed to delete lvol from primary_node' was
        logged and the record was dropped anyway."""
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=RuntimeError("bdev stack not removed"),
                absent=True)
        assert _record_exists(db, lvol.get_id())
        assert db.get_lvol_by_id(lvol.get_id()).status == LVol.STATUS_IN_DELETION

    def test_a_deferred_leader_teardown_keeps_the_record(self, db, finish_env):
        """'skip'/'queue' used to answer True — a node never touched, reported
        as clean. Now it raises PreconditionError, which must not pass the
        gate either -- deferred is not done."""
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=PreconditionError("owed to a task"),
                absent=True)
        assert _record_exists(db, lvol.get_id())

    def test_a_surviving_bdev_keeps_the_record(self, db, finish_env):
        """An acknowledged sync-delete RPC is not proof. The post-condition was
        never checked at all."""
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=None, absent=False)
        assert _record_exists(db, lvol.get_id())

    def test_an_unverifiable_bdev_keeps_the_record(self, db, finish_env):
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=None,
                absent=RPCException("proxy returned non-200"))
        assert _record_exists(db, lvol.get_id())

    def test_no_node_able_to_complete_the_teardown_keeps_the_record(
            self, db, finish_env):
        """With every member unreachable there is nobody to run the sync
        delete. The record must wait, not be dropped over a live bdev.

        (The sibling guard — the leader going non-online BETWEEN the leadership
        selection and the fresh re-read that drives the teardown — is a genuine
        cross-process race on the node's status in FDB and cannot be staged
        deterministically here; it is the branch the old code fell through to
        the record removal on.)"""
        cluster, leader, lvol = finish_env
        for nid in (LEADER_ID, PEER_ID):
            node = db.get_storage_node_by_id(nid)
            node.status = StorageNode.STATUS_UNREACHABLE
            node.write_to_db(db.kv_store)

        del_node = _finish(cluster, lvol, teardown=None, absent=True)
        assert _record_exists(db, lvol.get_id())
        del_node.assert_not_called()

    def test_a_live_peer_still_holding_the_registration_keeps_the_record(
            self, db, finish_env):
        """sync_delete_on_peer's return value was discarded entirely."""
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=None, absent=True,
                peer_cleared=False)
        assert _record_exists(db, lvol.get_id())

    def test_cleared_peers_are_recorded_so_a_retry_does_not_rewalk(
            self, db, finish_env):
        cluster, _leader, lvol = finish_env
        _finish(cluster, lvol, teardown=None, absent=False,
                peer_cleared=True)
        # Record kept (the bdev survived), but the peer's completed leg is
        # remembered so the next pass does not re-walk a clean blob tree.
        assert PEER_ID in db.get_lvol_by_id(lvol.get_id()).sync_deleted_nodes

    def test_an_empty_bdev_stack_still_retires_the_record(self, db, finish_env):
        """A retired landing volume's blob lives on as the converted snapshot
        ON PURPOSE. The gate must not strand it — its bdev is supposed to be
        there."""
        cluster, _leader, lvol = finish_env
        lvol.bdev_stack = []
        lvol.write_to_db(db.kv_store)

        with patch.object(lvol_monitor.lvol_events, "lvol_delete", MagicMock()):
            lvol_monitor.process_lvol_delete_finish(cluster,
                                                    db.get_lvol_by_id(lvol.get_id()))
        assert not _record_exists(db, lvol.get_id())


# ---------------------------------------------------------------------------
# delete_lvol_from_node — a normal return only for a confirmed teardown;
# PreconditionError for deferred, RuntimeError for failed.
# ---------------------------------------------------------------------------


class TestDeleteLvolFromNodeOutcome:

    @pytest.fixture(autouse=True)
    def env(self, db):
        _write_cluster(db)
        _write_pool(db)
        _write_node(db, LEADER_ID)
        self.lvol = _write_lvol(db, nodes=[LEADER_ID])

    def _run(self, verdict):
        with patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                   return_value=verdict), \
                patch.object(lc.tasks_controller, "add_lvol_sync_del_task", MagicMock()), \
                patch.object(StorageNode, "rpc_client", MagicMock()):
            return lc.delete_lvol_from_node(self.lvol.get_id(), LEADER_ID, sync=True)

    def test_skip_is_deferred_not_done(self):
        with pytest.raises(PreconditionError):
            self._run("skip")

    def test_queue_is_deferred_not_done(self):
        with pytest.raises(PreconditionError):
            self._run("queue")

    def test_an_unremovable_bdev_stack_is_failed(self):
        with pytest.raises(RuntimeError), \
                patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                     return_value="proceed"), \
                patch.object(lc, "_remove_lvol_subsys_from_node", return_value=True), \
                patch.object(lc, "_remove_bdev_stack", return_value=False), \
                patch.object(StorageNode, "rpc_client", MagicMock()):
            lc.delete_lvol_from_node(self.lvol.get_id(), LEADER_ID, sync=True)

    def test_a_confirmed_removal_returns_normally(self):
        with patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                   return_value="proceed"), \
                patch.object(lc, "_remove_lvol_subsys_from_node", return_value=True), \
                patch.object(lc, "_remove_bdev_stack", return_value=True), \
                patch.object(StorageNode, "rpc_client", MagicMock()):
            lc.delete_lvol_from_node(self.lvol.get_id(), LEADER_ID, sync=True)  # must not raise


# ---------------------------------------------------------------------------
# rollback_create_record — a blob that outlived the attempt keeps its record
# ---------------------------------------------------------------------------


class TestRollbackCreateRecord:

    def test_an_in_creation_record_is_released(self, db):
        _write_cluster(db)
        _write_pool(db)
        _write_node(db, LEADER_ID)
        lvol = _write_lvol(db, uuid="lvol-rb-1", status=LVol.STATUS_IN_CREATION)

        lc.rollback_create_record(lvol)
        assert not _record_exists(db, "lvol-rb-1")

    def test_an_in_deletion_record_survives(self, db):
        """_fail_after_bdev flips the record to in_deletion when the attempt
        produced a blob. Erasing it here is what stranded the bdev in SPDK."""
        _write_cluster(db)
        _write_pool(db)
        _write_node(db, LEADER_ID)
        lvol = _write_lvol(db, uuid="lvol-rb-2", status=LVol.STATUS_IN_DELETION)

        lc.rollback_create_record(lvol)
        assert _record_exists(db, "lvol-rb-2")
        assert db.get_lvol_by_id("lvol-rb-2").status == LVol.STATUS_IN_DELETION

    def test_an_already_removed_record_is_a_no_op(self, db):
        lvol = LVol()
        lvol.uuid = "lvol-rb-missing"
        lc.rollback_create_record(lvol)  # must not raise


# ---------------------------------------------------------------------------
# _fail_after_bdev — the intent is persisted before any teardown RPC
# ---------------------------------------------------------------------------


class TestFailAfterBdevPersistsIntentFirst:

    def test_intent_survives_a_rollback_whose_rpcs_raise(self, db):
        """The status write used to sit AFTER _remove_bdev_stack inside the
        same try, so a raising rollback never reached it — and the caller then
        erased a record whose blob was still on the node."""
        _write_cluster(db)
        _write_pool(db)
        _write_node(db, LEADER_ID)
        lvol = _write_lvol(db, uuid="lvol-fab-1", status=LVol.STATUS_IN_CREATION)
        lvol.nqn = "nqn.test:lvol:1"
        lvol.top_bdev = "LVS_1/LVOL_1"

        rpc = MagicMock()
        rpc.subsystem_get.side_effect = RuntimeError("connection error")
        with patch.object(lc, "_remove_bdev_stack",
                          side_effect=RuntimeError("connection error")):
            ok, msg = lc._fail_after_bdev(lvol, rpc, "boom")

        assert ok is False and msg == "boom"
        assert db.get_lvol_by_id("lvol-fab-1").status == LVol.STATUS_IN_DELETION

        # And the guarded rollback must then refuse to erase it.
        lc.rollback_create_record(lvol)
        assert _record_exists(db, "lvol-fab-1")


# ---------------------------------------------------------------------------
# delete_lvol — force delete with a missing node record
# ---------------------------------------------------------------------------


class TestForceDeleteWithMissingNode:

    def test_it_tears_down_on_surviving_peers_and_logs_the_event(self, db):
        """This branch erased the record without a single RPC and without an
        event. For an HA volume the blob lives on every LVS member."""
        _write_cluster(db)
        _write_pool(db)
        _write_node(db, PEER_ID)          # peer survives; the primary's record does not
        lvol = _write_lvol(db, uuid="lvol-force-1", status=LVol.STATUS_ONLINE)

        with patch.object(lc, "delete_lvol_from_node",
                          return_value=None) as del_node, \
                patch.object(lc.lvol_events, "lvol_delete", MagicMock()) as ev, \
                patch.object(lc.ops_gate, "assert_object_ops_allowed", MagicMock()), \
                patch("simplyblock_core.controllers.migration_controller."
                      "get_active_migration_for_lvol", return_value=None):
            lc.delete_lvol(lvol, force_delete=True)

        del_node.assert_called_once()
        assert del_node.call_args.args[1] == PEER_ID
        ev.assert_called_once()
        assert not _record_exists(db, "lvol-force-1")


# ---------------------------------------------------------------------------
# The orphan sweep is detect-only
# ---------------------------------------------------------------------------


class TestOrphanSweepIsDetectOnly:

    def test_it_reports_unclaimed_blobs_without_deleting_them(self, db):
        cluster = _write_cluster(db)
        _write_pool(db)
        node = _write_node(db, LEADER_ID)
        lvol = _write_lvol(db, uuid="lvol-orphan-1", status=LVol.STATUS_ONLINE)
        lvol.blobid = 11
        lvol.write_to_db(db.kv_store)

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"uuid": "lvs-uuid"}]
        rpc.bdev_lvs_dump_tree.return_value = {"lvols": [
            {"blobid": 11, "name": "LVOL_1", "uuid": "u1", "ref": 1},   # claimed
            {"blobid": 99, "name": "LVOL_GHOST", "uuid": "u2", "ref": 1},  # orphan
            {"blobid": 7, "name": "hublvol", "uuid": "u3", "ref": 1},   # node's own
        ]}

        with patch.object(StorageNode, "rpc_client", return_value=rpc), \
                patch.object(lvol_monitor.storage_events, "snode_orphaned_objects",
                             MagicMock()) as ev:
            lvol_monitor.sweep_orphan_objects(cluster, [node])

        ev.assert_called_once()
        reported = ev.call_args.args[1]
        assert [o["blobid"] for o in reported] == [99]
        rpc.delete_lvol.assert_not_called()
        assert _record_exists(db, "lvol-orphan-1")

    def test_the_sweep_is_rate_limited(self):
        lvol_monitor._last_orphan_sweep = 0.0
        assert lvol_monitor._orphan_sweep_due() is True
        assert lvol_monitor._orphan_sweep_due() is False, (
            "one full lvol+snapshot read per cluster must not run every cycle")
        assert constants.LVOL_MONITOR_ORPHAN_CHECK_INTERVAL_SEC >= 300
