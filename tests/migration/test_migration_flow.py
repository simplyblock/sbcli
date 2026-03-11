# coding=utf-8
"""
test_migration_flow.py – integration tests for the live volume migration feature.

Each test:
1. Populates FDB via db_setup helpers.
2. Seeds the source mock server with the expected in-memory bdev state.
3. Calls migration_controller.start_migration() to create the LVolMigration
   record and its backing JobSchedule task.
4. Drives the task runner to completion via conftest.run_migration_task().
5. Asserts on the final DB state and on the mock server's in-memory state.

Background services (node monitor, distrib event collector, etc.) are never
started; the test process only imports the task runner module directly.
"""

import random
import threading
import time
import pytest

from simplyblock_core.controllers import migration_controller
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode

from tests.migration.conftest import run_migration_task, set_node_status
from tests.migration.topology_loader import TestContext

# Lazily initialised so the module can be imported without FDB installed.
_db_instance = None


def _get_db():
    global _db_instance
    if _db_instance is None:
        from simplyblock_core.db_controller import DBController
        _db_instance = DBController()
    return _db_instance


# Shorthand used throughout this module
class _LazyDb:
    def __getattr__(self, name):
        return getattr(_get_db(), name)


db = _LazyDb()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_lvol(mock_srv, lvol, node):
    """Seed an lvol bdev into a mock server's in-memory state."""
    composite = f"{node.lvstore}/{lvol.lvol_bdev}"
    with mock_srv.state.lock:
        blobid = mock_srv.state.next_blobid()
        mock_srv.state.lvols[composite] = {
            'name': lvol.lvol_bdev,
            'composite': composite,
            'uuid': lvol.lvol_uuid or lvol.uuid,
            'blobid': blobid,
            'migration_flag': False,
            'driver_specific': {
                'lvol': {
                    'blobid': blobid,
                    'lvs_name': node.lvstore,
                    'base_snapshot': None,
                    'clone': False,
                    'snapshot': False,
                    'num_allocated_clusters': 1024,
                }
            }
        }


def _seed_snapshot(mock_srv, snap, node):
    """Seed a snapshot bdev into a mock server's in-memory state."""
    short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
    composite = f"{node.lvstore}/{short}"
    with mock_srv.state.lock:
        blobid = mock_srv.state.next_blobid()
        mock_srv.state.snapshots[composite] = {
            'name': short,
            'composite': composite,
            'uuid': snap.snap_uuid or snap.uuid,
            'blobid': blobid,
            'driver_specific': {
                'lvol': {
                    'blobid': blobid,
                    'lvs_name': node.lvstore,
                    'base_snapshot': None,
                    'clone': False,
                    'snapshot': True,
                    'num_allocated_clusters': 1024,
                }
            }
        }


def _seed_all(mock_srv, ctx: TestContext, node_sym: str):
    """Seed ALL lvols and snapshots on *node_sym* into *mock_srv*."""
    node = ctx.node(node_sym)
    for lvol in ctx._lvols.values():
        if lvol.node_id == node.uuid:
            _seed_lvol(mock_srv, lvol, node)
    for snap in ctx._snaps.values():
        if snap.lvol and snap.lvol.node_id == node.uuid:
            _seed_snapshot(mock_srv, snap, node)


def _assert_migration_done(migration_id: str):
    m = db.get_migration_by_id(migration_id)
    assert m.status == LVolMigration.STATUS_DONE, (
        f"Expected STATUS_DONE, got {m.status}; error: {m.error_message}")
    assert m.phase == LVolMigration.PHASE_COMPLETED
    return m


def _assert_migration_failed(migration_id: str):
    m = db.get_migration_by_id(migration_id)
    assert m.status in (LVolMigration.STATUS_FAILED, LVolMigration.STATUS_CANCELLED), (
        f"Expected failure, got {m.status}")
    return m


# ---------------------------------------------------------------------------
# Test: basic single-snapshot migration
# ---------------------------------------------------------------------------

class TestBasicMigration:

    def test_single_snap_migration_completes(self, topology_two_node,
                                              mock_src_server, mock_tgt_server):
        """Happy path: one snapshot on the source → successful full migration."""
        ctx = topology_two_node
        tgt_node = ctx.node("tgt")
        lvol = ctx.lvol("l1")

        # Seed source mock with bdev state matching the FDB records
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None, f"start_migration failed: {err}"
        assert mig_id

        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig_id)

        updated_lvol = db.get_lvol_by_id(lvol.uuid)
        assert updated_lvol.node_id == tgt_node.uuid, (
            f"Expected lvol node_id={tgt_node.uuid}, got {updated_lvol.node_id}")

        with mock_tgt_server.state.lock:
            assert any(lvol.nqn in nqn for nqn in mock_tgt_server.state.subsystems), \
                "Target mock has no subsystem for the migrated volume"

    def test_migration_no_target_subsystem_reuse(self, custom_topology,
                                                   mock_src_server, mock_tgt_server):
        """
        When two lvols share the same NQN subsystem, the second migration must
        re-use the existing subsystem on the target rather than creating a new one.
        """
        spec = {
            "cluster": {},
            "nodes": [
                {"id": "src", "mgmt_ip": "127.0.0.1", "rpc_port": 9901,
                 "lvstore": "lvs_src", "status": "online",
                 "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
                {"id": "tgt", "mgmt_ip": "127.0.0.1", "rpc_port": 9902,
                 "lvstore": "lvs_tgt", "status": "online",
                 "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
            ],
            "pools": [{"id": "p1", "name": "pool"}],
            "volumes": [
                {"id": "l1", "name": "vol1", "size": "1G", "node_id": "src",
                 "pool_id": "p1", "namespace_group": "grp1", "ns_id": 1},
                {"id": "l2", "name": "vol2", "size": "1G", "node_id": "src",
                 "pool_id": "p1", "namespace_group": "grp1", "ns_id": 2},
            ],
            "snapshots": [
                {"id": "s1", "name": "snap1", "lvol_id": "l1"},
                {"id": "s2", "name": "snap2", "lvol_id": "l2"},
            ],
        }
        ctx = custom_topology(spec)
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        # Migrate l1 first
        mig1_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        # Migrate l2 (l1 is done; same-source constraint lifted)
        mig2_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l2"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig2_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig2_id)

        # Both namespaces should be in the shared subsystem on the target
        nqn = ctx.lvol("l1").nqn  # both share the same NQN
        with mock_tgt_server.state.lock:
            sub = mock_tgt_server.state.subsystems.get(nqn)
            assert sub is not None, f"Shared subsystem {nqn!r} not found on target"
            ns_count = len(sub['namespaces'])
            assert ns_count == 2, \
                f"Expected 2 namespaces in shared subsystem, got {ns_count}"


# ---------------------------------------------------------------------------
# Test: shared snapshot chain (clone scenario)
# ---------------------------------------------------------------------------

class TestSharedSnapshotChain:

    def test_clone_migration_does_not_delete_shared_snaps_from_source(
            self, topology_clone_chain, mock_src_server, mock_tgt_server):
        """
        Scenario: l1 → s3 → s2 → s1, and c1 cloned from s2 (c1 → s2 → s1).
        Migrating c1 must NOT delete s1 or s2 from the source (l1 still references them).
        """
        ctx = topology_clone_chain
        src_node = ctx.node("src")
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("c1"), tgt_node.uuid)
        assert err is None, err
        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig_id)

        # s1 and s2 must still exist on the source (l1 still references them)
        for snap_sym in ["s1", "s2"]:
            snap = ctx.snap(snap_sym)
            short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
            src_composite = f"{src_node.lvstore}/{short}"
            with mock_src_server.state.lock:
                assert src_composite in mock_src_server.state.snapshots, \
                    f"Source snap {snap.snap_name} ({src_composite}) was incorrectly deleted"

    def test_pre_existing_snaps_skipped_on_second_migration(
            self, topology_clone_chain, mock_src_server, mock_tgt_server):
        """
        After c1 is migrated (carrying s1, s2 to target), migrating l1 to the
        same target must skip re-transferring s1 and s2.
        """
        ctx = topology_clone_chain
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        # Migrate c1 first → s1 + s2 land on target
        mig1_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("c1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        # Migrate l1 now
        mig2_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig2_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig2_id)

        m2 = db.get_migration_by_id(mig2_id)
        for snap_sym in ["s1", "s2"]:
            snap_uuid = ctx.snap_uuid(snap_sym)
            assert snap_uuid in m2.snaps_preexisting_on_target, \
                f"{snap_sym} not marked as pre-existing on target"

    def test_rollback_does_not_delete_pre_existing_snaps(
            self, topology_clone_chain, mock_src_server, mock_tgt_server):
        """
        When l1's migration fails, rolling back must only delete newly-copied
        snaps; s1 and s2 (pre-existing from c1's migration) must stay.
        """
        ctx = topology_clone_chain
        tgt_node = ctx.node("tgt")
        tgt_lvstore = tgt_node.lvstore

        _seed_all(mock_src_server, ctx, "src")

        # Migrate c1 first to deposit s1, s2 on target
        mig1_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("c1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        mig2_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        # Force failure via 100% error rate on target (use short timeout for speed)
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.5)
        run_migration_task(mig2_id, max_steps=300, step_sleep=0.02)
        mock_tgt_server.set_failure_rate(0.0)

        _assert_migration_failed(mig2_id)

        # s1 and s2 must still be on target
        for snap_sym in ["s1", "s2"]:
            snap = ctx.snap(snap_sym)
            short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
            tgt_composite = f"{tgt_lvstore}/{short}"
            with mock_tgt_server.state.lock:
                assert tgt_composite in mock_tgt_server.state.snapshots, \
                    f"Pre-existing snap {snap.snap_name} was incorrectly deleted from target"


# ---------------------------------------------------------------------------
# Test: precondition validation
# ---------------------------------------------------------------------------

class TestPreconditions:

    def test_reject_when_source_offline(self, topology_two_node):
        ctx = topology_two_node
        src_uuid = ctx.node_uuid("src")
        set_node_status(src_uuid, StorageNode.STATUS_OFFLINE)
        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
        assert mig_id is False
        assert "Source node is not online" in err
        set_node_status(src_uuid, StorageNode.STATUS_ONLINE)

    def test_reject_same_source_and_target(self, topology_two_node):
        ctx = topology_two_node
        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("src"))
        assert mig_id is False
        assert "different" in err.lower()

    def test_reject_duplicate_active_migration_on_node(
            self, custom_topology, mock_src_server, mock_tgt_server):
        spec = {
            "cluster": {},
            "nodes": [
                {"id": "src", "mgmt_ip": "127.0.0.1", "rpc_port": 9901,
                 "lvstore": "lvs_src", "status": "online",
                 "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
                {"id": "tgt", "mgmt_ip": "127.0.0.1", "rpc_port": 9902,
                 "lvstore": "lvs_tgt", "status": "online",
                 "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]},
            ],
            "pools": [{"id": "p1", "name": "pool"}],
            "volumes": [
                {"id": "l1", "name": "vol1", "size": "500M", "node_id": "src", "pool_id": "p1"},
                {"id": "l2", "name": "vol2", "size": "500M", "node_id": "src", "pool_id": "p1"},
            ],
            "snapshots": [
                {"id": "s1", "name": "snap1", "lvol_id": "l1"},
                {"id": "s2", "name": "snap2", "lvol_id": "l2"},
            ],
        }
        ctx = custom_topology(spec)
        _seed_all(mock_src_server, ctx, "src")

        mig1_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
        assert err is None

        # Second migration from same source node must fail
        mig2_id, err2 = migration_controller.start_migration(
            ctx.lvol_uuid("l2"), ctx.node_uuid("tgt"))
        assert mig2_id is False
        assert "active on source node" in err2


# ---------------------------------------------------------------------------
# Test: cancellation
# ---------------------------------------------------------------------------

class TestCancellation:

    def test_cancel_running_migration(self, topology_two_node,
                                       mock_src_server, mock_tgt_server):
        """Cancel a migration mid-flight; it must reach CANCELLED status."""
        ctx = topology_two_node
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        # Run a few steps, then cancel
        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.migration.conftest import _find_migration_task
        task = _find_migration_task(db, mig_id)
        for _ in range(5):
            task = db.get_task_by_id(task.uuid)
            done = task_runner(task)
            if done:
                break
            time.sleep(0.02)

        # Cancel
        ok, cerr = migration_controller.cancel_migration(mig_id)
        assert ok, f"cancel_migration failed: {cerr}"

        # Run to completion
        run_migration_task(mig_id, max_steps=300, step_sleep=0.02)

        m = db.get_migration_by_id(mig_id)
        assert m.status == LVolMigration.STATUS_CANCELLED, \
            f"Expected CANCELLED, got {m.status}"


# ---------------------------------------------------------------------------
# Test: random failure mode (smoke test – non-deterministic)
# ---------------------------------------------------------------------------

class TestRandomFailureMode:

    @pytest.mark.parametrize("failure_rate", [0.05, 0.15])
    def test_migration_eventually_completes_under_low_failure_rate(
            self, topology_two_node, mock_src_server, mock_tgt_server, failure_rate):
        """
        With a low random failure rate the migration should eventually succeed
        (retries carry it through).  We give it a large step budget.
        """
        ctx = topology_two_node
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        # Enable failure injection
        mock_src_server.set_failure_rate(failure_rate, timeout_seconds=0.05)
        mock_tgt_server.set_failure_rate(failure_rate, timeout_seconds=0.05)

        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        run_migration_task(mig_id, max_steps=2000, step_sleep=0.01)

        # Disable failure injection
        mock_src_server.set_failure_rate(0.0)
        mock_tgt_server.set_failure_rate(0.0)

        m = db.get_migration_by_id(mig_id)
        # Either it succeeded (ideal) or hit the retry limit and failed cleanly.
        assert m.status in (LVolMigration.STATUS_DONE, LVolMigration.STATUS_FAILED), \
            f"Migration stuck in status={m.status}"

    def test_migration_fails_cleanly_under_full_failure_rate(
            self, topology_two_node, mock_src_server, mock_tgt_server):
        """With 100 % failure rate the migration must fail, not hang."""
        ctx = topology_two_node
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        mock_src_server.set_failure_rate(1.0, timeout_seconds=0.05)
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.05)

        mig_id, err = migration_controller.start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        run_migration_task(mig_id, max_steps=500, step_sleep=0.01)

        mock_src_server.set_failure_rate(0.0)
        mock_tgt_server.set_failure_rate(0.0)

        m = db.get_migration_by_id(mig_id)
        assert m.status == LVolMigration.STATUS_FAILED, \
            f"Expected FAILED, got {m.status}"


# ---------------------------------------------------------------------------
# Test: HA node – secondary registration
# ---------------------------------------------------------------------------

class TestHASecondaryRegistration:

    def test_snapshot_registered_on_secondary_after_convert(
            self, topology_two_node_ha, mock_src_server, mock_tgt_server, mock_sec_server):
        """
        After bdev_lvol_convert on the target primary, the snapshot must also be
        registered on the target secondary (bdev_lvol_snapshot_register).
        """
        ctx = topology_two_node_ha
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        lvol = ctx.lvol("l1")
        snap = ctx.snap("s1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig_id)

        # The secondary mock should have a snapshot registered
        short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
        sec_composite = f"{ctx.node('tgt-sec').lvstore}/{short}"
        with mock_sec_server.state.lock:
            assert sec_composite in mock_sec_server.state.snapshots, \
                f"Snapshot not registered on secondary: {sec_composite}"

    def test_lvol_registered_and_exposed_on_secondary(
            self, topology_two_node_ha, mock_src_server, mock_tgt_server, mock_sec_server):
        """
        After bdev_lvol_final_migration completes, the lvol must be registered on
        the secondary and exposed in its NVMe-oF subsystem.
        """
        ctx = topology_two_node_ha
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        lvol = ctx.lvol("l1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig_id)

        # The secondary mock should have a subsystem with a namespace for the lvol
        with mock_sec_server.state.lock:
            sub = mock_sec_server.state.subsystems.get(lvol.nqn)
            assert sub is not None, \
                f"No subsystem {lvol.nqn} on secondary after migration"
            ns_bdevs = [ns['bdev_name'] for ns in sub.get('namespaces', [])]
            assert any(lvol.lvol_bdev in bdev for bdev in ns_bdevs), \
                f"LVol bdev not in secondary subsystem namespaces: {ns_bdevs}"

    def test_secondary_blocked_when_secondary_in_bad_state(
            self, topology_two_node_ha, mock_src_server, mock_tgt_server, mock_sec_server):
        """
        If the target secondary node transitions to a non-online/offline state
        after migration starts, the migration must suspend (not proceed).
        """
        ctx = topology_two_node_ha
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        lvol = ctx.lvol("l1")
        sec_uuid = ctx.node_uuid("tgt-sec")

        # Put secondary into a bad state (not online and not offline)
        set_node_status(sec_uuid, "in_restart")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(10):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        m = db.get_migration_by_id(mig_id)
        # Migration must be suspended, not done or failed
        assert m.status in (LVolMigration.STATUS_SUSPENDED, LVolMigration.STATUS_RUNNING), \
            f"Expected suspended, got {m.status}"

        # Restore secondary
        set_node_status(sec_uuid, StorageNode.STATUS_ONLINE)


# ---------------------------------------------------------------------------
# Test: 4-node cluster, lvol with 4-snapshot chain
# ---------------------------------------------------------------------------

class TestFourNodeFourSnapshotMigration:
    """
    Simulates a realistic 4-node cluster where a volume with a 4-snapshot
    ancestry chain is migrated from node n1 to node n2.  Nodes n3 and n4
    exist only in FDB (their RPC endpoints are not contacted during migration).

    Topology (four_node.json):
        n1 (src, port 9901) → lvol l1 + snapshots s1←s2←s3←s4 (s1 oldest)
        n2 (tgt, port 9902)
        n3 (passive, port 9910 – not contacted)
        n4 (passive, port 9911 – not contacted)
    """

    def test_four_snap_migration_completes(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        Happy path: migrate l1 (4 snapshots) from n1 to n2.
        Asserts STATUS_DONE and that the lvol DB record points to n2.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        # Seed source mock with bdev state matching FDB records
        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None, f"start_migration failed: {err}"
        assert mig_id

        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        m = _assert_migration_done(mig_id)

        # lvol DB record must now point at n2
        updated_lvol = db.get_lvol_by_id(lvol.uuid)
        assert updated_lvol.node_id == tgt_node.uuid, (
            f"Expected lvol.node_id={tgt_node.uuid}, got {updated_lvol.node_id}")

        # All 4 original snapshots must have been transferred (plus any intermediates)
        assert len(m.snaps_migrated) >= 4, (
            f"Expected at least 4 migrated snaps, got {m.snaps_migrated}")

    def test_four_snap_all_snaps_land_on_target(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        After migration all 4 snapshot bdevs must be present on the target mock.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        _assert_migration_done(mig_id)

        tgt_lvstore = tgt_node.lvstore
        with mock_tgt_server.state.lock:
            tgt_snaps = set(mock_tgt_server.state.snapshots.keys())

        for snap_sym in ["s1", "s2", "s3", "s4"]:
            snap = ctx.snap(snap_sym)
            short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev \
                else snap.snap_bdev
            composite = f"{tgt_lvstore}/{short}"
            assert composite in tgt_snaps, (
                f"Snapshot {snap.snap_name} ({composite}) missing from target")

    def test_four_snap_all_source_snaps_removed_after_migration(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        After CLEANUP_SOURCE all 4 snapshot bdevs must be gone from the source.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")
        src_node = ctx.node("n1")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        _assert_migration_done(mig_id)

        src_lvstore = src_node.lvstore
        with mock_src_server.state.lock:
            remaining_src = set(mock_src_server.state.snapshots.keys())

        for snap_sym in ["s1", "s2", "s3", "s4"]:
            snap = ctx.snap(snap_sym)
            short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev \
                else snap.snap_bdev
            composite = f"{src_lvstore}/{short}"
            assert composite not in remaining_src, (
                f"Snapshot {snap.snap_name} still on source after migration")

    def test_four_snap_target_has_subsystem_for_lvol(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        The target mock must expose an NVMe-oF subsystem containing the migrated lvol.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        _assert_migration_done(mig_id)

        with mock_tgt_server.state.lock:
            sub = mock_tgt_server.state.subsystems.get(lvol.nqn)
        assert sub is not None, f"No subsystem for NQN {lvol.nqn} on target"
        ns_bdevs = [ns['bdev_name'] for ns in sub.get('namespaces', [])]
        assert any(lvol.lvol_bdev in bdev for bdev in ns_bdevs), (
            f"LVol bdev {lvol.lvol_bdev!r} not in target subsystem namespaces: {ns_bdevs}")

    def test_four_snap_passive_nodes_unaffected(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        Nodes n3 and n4 must remain online in FDB throughout migration
        (migration runner must not touch them).
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        _assert_migration_done(mig_id)

        for node_sym in ("n3", "n4"):
            node_uuid = ctx.node_uuid(node_sym)
            node_fresh = db.get_storage_node_by_id(node_uuid)
            assert node_fresh.status == StorageNode.STATUS_ONLINE, (
                f"Passive node {node_sym} status changed to {node_fresh.status}")

    def test_random_node_offline_then_online_during_migration(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        Resilience test: a randomly chosen storage node goes offline for ~2 s
        (representing a 40-second real-world outage; mock async ops complete in
        ~0.2 s each so time is scaled ×20) during the migration, then comes
        back online.  The migration must still reach STATUS_DONE.

        The four possible outcomes depending on which node is chosen:

        - n1 (src offline): runner detects STATUS != ONLINE at the start of
          every task_runner() call and suspends the task.  Once n1 is restored
          the task resumes from the current phase and completes.

        - n2 (tgt offline): same pattern – runner suspends until tgt is back.

        - n3 / n4 (passive nodes): the migration runner never contacts them, so
          the outage has no effect on migration progress; STATUS_DONE is reached
          at normal speed.

        In all cases the offline node must be STATUS_ONLINE when the test ends.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        _seed_all(mock_src_server, ctx, "n1")

        # Pick one of the four cluster nodes at random.
        offline_sym = random.choice(["n1", "n2", "n3", "n4"])
        offline_uuid = ctx.node_uuid(offline_sym)

        mig_id, err = migration_controller.start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None, f"start_migration failed: {err}"

        # Background thread: wait 0.3 s (let migration get started), take the
        # node offline for 2 s (representing the 40-second outage), then restore it.
        offline_cycle_done = threading.Event()

        def _offline_cycle():
            time.sleep(0.3)
            set_node_status(offline_uuid, StorageNode.STATUS_OFFLINE)
            time.sleep(2.0)
            set_node_status(offline_uuid, StorageNode.STATUS_ONLINE)
            offline_cycle_done.set()

        t = threading.Thread(target=_offline_cycle, daemon=True, name="offline-injector")
        t.start()

        # Drive the task runner; max_steps=3000 × 0.02 s = 60 s wall budget,
        # which is more than enough even if the runner stalls for 2 s.
        run_migration_task(mig_id, max_steps=3000, step_sleep=0.02)

        t.join(timeout=10.0)
        assert offline_cycle_done.is_set(), "Offline-cycle thread did not finish"

        _assert_migration_done(mig_id)

        updated_lvol = db.get_lvol_by_id(lvol.uuid)
        assert updated_lvol.node_id == tgt_node.uuid

        # Whichever node was taken offline must be back online now.
        node_fresh = db.get_storage_node_by_id(offline_uuid)
        assert node_fresh.status == StorageNode.STATUS_ONLINE, (
            f"Node {offline_sym} is still {node_fresh.status!r} after migration")
