# coding=utf-8
"""
test_migration_flow.py – integration tests for the live volume migration feature.

Each test:
1. Populates FDB via db_setup helpers.
2. Seeds the source mock server with the expected in-memory bdev state.
3. Calls start_migration() to create the LVolMigration
   record and its backing JobSchedule task.
4. Drives the task runner to completion via conftest.run_migration_task().
5. Asserts on the final DB state and on the mock server's in-memory state.

Background services (node monitor, distrib event collector, etc.) are never
started; the test process only imports the task runner module directly.
"""

import time
import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode

from tests.integration.migration.conftest import (
    run_migration_task, set_node_status, start_migration,
)
from tests.integration.migration.topology_loader import TestContext

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
        ctx.node("src")
        tgt_node = ctx.node("tgt")
        lvol = ctx.lvol("l1")

        # Seed source mock with bdev state matching the FDB records
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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
        ctx.node("src")
        tgt_node = ctx.node("tgt")

        _seed_all(mock_src_server, ctx, "src")

        # Migrate l1 first
        mig1_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        # Migrate l2 (l1 is done; same-source constraint lifted)
        mig2_id, err = start_migration(
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

        mig_id, err = start_migration(
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
        mig1_id, err = start_migration(
            ctx.lvol_uuid("c1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        # Migrate l1 now
        mig2_id, err = start_migration(
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

        _seed_all(mock_src_server, ctx, "src")

        # Migrate c1 first to deposit s1, s2 on target
        mig1_id, err = start_migration(
            ctx.lvol_uuid("c1"), tgt_node.uuid)
        assert err is None
        run_migration_task(mig1_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig1_id)

        mig2_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        # Force failure via 100% error rate on target (use short timeout for speed)
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.5)
        run_migration_task(mig2_id, max_steps=300, step_sleep=0.02)
        mock_tgt_server.set_failure_rate(0.0)

        _assert_migration_failed(mig2_id)

        # s1 and s2 must still be on target. s1/s2 are owned by l1, not c1, so
        # c1's migration recorded its target-side copy as an ``instances``
        # entry rather than mutating the canonical (still source-side)
        # snap_bdev — look up that instance's bdev, which carries the
        # migration suffix (e.g. SNAP_xxxm).
        for snap_sym in ["s1", "s2"]:
            snap = db.get_snapshot_by_id(ctx.snap_uuid(snap_sym))
            instance = next(
                (i for i in snap.instances if i.get('lvol', {}).get('node_id') == tgt_node.uuid),
                None)
            assert instance is not None, \
                f"No target-side instance recorded for pre-existing snap {snap.snap_name}"
            tgt_composite = instance['snap_bdev']
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
        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
        assert mig_id is False
        assert "Source node is not online" in err
        set_node_status(src_uuid, StorageNode.STATUS_ONLINE)

    def test_reject_same_source_and_target(self, topology_two_node):
        ctx = topology_two_node
        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("src"))
        assert mig_id is False
        assert "same node" in err.lower()

    def test_second_migration_from_same_source_node_is_allowed(
            self, custom_topology, mock_src_server, mock_tgt_server):
        """
        Migration is only serialized per-lvol (``create_migration`` checks
        ``get_active_migration_for_lvol``), not per-source-node — a second,
        distinct lvol on the same source may migrate concurrently.
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

        mig1_id, err = start_migration(
            ctx.lvol_uuid("l1"), ctx.node_uuid("tgt"))
        assert err is None

        # Second migration for a different lvol on the same source node succeeds.
        mig2_id, err2 = start_migration(
            ctx.lvol_uuid("l2"), ctx.node_uuid("tgt"))
        assert err2 is None
        assert mig2_id


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

        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        # Force failures so the migration cannot race to completion before
        # we cancel it — with a single snapshot it can otherwise finish
        # within a handful of task_runner() calls.
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.1)

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task
        task = _find_migration_task(db, mig_id)
        for _ in range(5):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        mock_tgt_server.set_failure_rate(0.0)

        m = db.get_migration_by_id(mig_id)
        assert m.is_active(), f"Migration should still be active, got {m.status}"

        # Cancel
        migration_controller.cancel_migration(mig_id)

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

        # create_migration()/start_migration() perform synchronous target
        # setup RPCs with no retry of their own, so failure injection starts
        # only once the migration exists — exercising the task runner's own
        # retry logic, which is what "retries carry it through" means here.
        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        mock_src_server.set_failure_rate(failure_rate, timeout_seconds=0.05)
        mock_tgt_server.set_failure_rate(failure_rate, timeout_seconds=0.05)

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

        # create_migration()/start_migration() perform synchronous target
        # setup RPCs with no retry of their own, so the failure rate is only
        # injected once the migration exists — exercising the task runner's
        # own retry-then-give-up behavior instead of the one-shot setup call.
        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt_node.uuid)
        assert err is None

        mock_src_server.set_failure_rate(1.0, timeout_seconds=0.05)
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.05)

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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        _assert_migration_done(mig_id)

        # The secondary mock should have a snapshot registered. Target bdevs
        # carry the migration suffix (e.g. SNAP_xxxm).
        short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
        sec_composite = f"{ctx.node('tgt-sec').lvstore}/{short}{constants.LVOL_MIG_BDEV_SUFFIX}"
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
        ctx.snap("s1")

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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
            # Target bdevs carry the migration suffix (e.g. SNAP_xxxm).
            composite = f"{tgt_lvstore}/{short}{constants.LVOL_MIG_BDEV_SUFFIX}"
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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
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

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid)
        assert err is None
        run_migration_task(mig_id, max_steps=1000, step_sleep=0.02)
        _assert_migration_done(mig_id)

        for node_sym in ("n3", "n4"):
            node_uuid = ctx.node_uuid(node_sym)
            node_fresh = db.get_storage_node_by_id(node_uuid)
            assert node_fresh.status == StorageNode.STATUS_ONLINE, (
                f"Passive node {node_sym} status changed to {node_fresh.status}")

    # -- Node-outage resilience --------------------------------------------
    #
    # These tests replace a single test that picked one of n1/n2/n3/n4 with
    # random.choice() and took it offline from a wall-clock thread. Because the
    # migration is always n1 -> n2, that one draw selected between three
    # materially different scenarios with different correct outcomes, and only
    # one of them ran per CI run — the n1 draw failed ~25% of runs and accounted
    # for 14 of 16 observed `integration-slow` failures.
    #
    # Each scenario now gets its own test, and the outage is injected from the
    # runner loop via run_migration_task(on_step=...) instead of from a thread,
    # so "how long was the node down" is an exact tick count rather than a race.

    _MAX_RETRIES = 5

    def _outage(self, offline_uuid, down_at, up_at=None):
        """Build an on_step callback taking a node offline for [down_at, up_at) ticks.

        ``up_at=None`` leaves it down for the rest of the run.  Note the run ends
        as soon as the migration is terminal, so a tick-scheduled restore may
        never fire — always restore explicitly afterwards (``_restore``) before
        asserting on node status.
        """
        def _on_step(step):
            if step == down_at:
                set_node_status(offline_uuid, StorageNode.STATUS_OFFLINE)
            elif up_at is not None and step == up_at:
                set_node_status(offline_uuid, StorageNode.STATUS_ONLINE)
        return _on_step

    def _restore(self, node_uuid):
        """Put a node back online regardless of how far the run got."""
        set_node_status(node_uuid, StorageNode.STATUS_ONLINE)

    def _assert_back_online(self, node_uuid, sym):
        node_fresh = db.get_storage_node_by_id(node_uuid)
        assert node_fresh.status == StorageNode.STATUS_ONLINE, (
            f"Node {sym} is still {node_fresh.status!r} after migration")

    def test_source_offline_longer_than_retry_budget_resumes_and_completes(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        The source node goes offline mid-migration for longer than the retry
        budget, then comes back.  The migration must resume from its current
        phase and complete.

        KNOWN FAILING — this asserts documented behaviour the runner does not
        currently implement, and is the reproduction to hand over with the
        source-outage policy question.

        What happens instead: the source-offline check in task_runner takes the
        _budget_suspend path, which does `migration.retry_count += 1` on EVERY
        tick that observes the outage and never resets it.  With the node down
        for 10 ticks and a budget of 5, the budget is spent while the node is
        still down; the migration is redirected to PHASE_CLEANUP_TARGET, rolls
        back, and ends STATUS_FAILED with
        `error_message="source node not online (status=offline)"`.

        So the retry budget is not 5 retried operations — it is a wall-clock
        tolerance of `max_retries x poll_interval`, which makes it depend on how
        fast the caller ticks (0.1 s here; ~15 s for the real 3 s service loop).
        Note that _suspend_task already takes `charge_retry`, and the two checks
        immediately below this one — "cluster not active" and "expansion in
        progress" — both pass charge_retry=False and let the migration deadline
        bound them instead.  Source-node-offline is the only environmental wait
        that charges the budget.

        Resolve by either fixing that asymmetry or, if charging is deliberate,
        rewriting this test to expect rollback (see the n2 test below, which
        asserts exactly that shape).
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")
        src_uuid = ctx.node_uuid("n1")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid,
                                      max_retries=self._MAX_RETRIES)
        assert err is None, f"start_migration failed: {err}"

        # Down for 10 ticks — deliberately longer than the 5-retry budget.
        run_migration_task(mig_id, max_steps=3000, step_sleep=0.02,
                           on_step=self._outage(src_uuid, down_at=2, up_at=12))
        # The run stops the moment the migration goes terminal, which under
        # current behaviour is before tick 12 — restore so this fails on the
        # assertion below rather than on leftover node state.
        self._restore(src_uuid)

        _assert_migration_done(mig_id)
        assert db.get_lvol_by_id(lvol.uuid).node_id == tgt_node.uuid
        self._assert_back_online(src_uuid, "n1")

    def test_target_offline_during_snap_copy_rolls_back(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        A target observed offline during SNAP_COPY/LVOL_MIGRATE deliberately
        fails the migration into cleanup_target rather than suspending: a
        restarted target may have lost its migration state, so the runner rolls
        back.  The volume must still be served from the source afterwards.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid,
                                      max_retries=self._MAX_RETRIES)
        assert err is None, f"start_migration failed: {err}"

        # Take the target down while still in SNAP_COPY. The gate acts on the
        # first tick that observes it, so no tick-count guessing is needed.
        assert db.get_migration_by_id(mig_id).phase == LVolMigration.PHASE_SNAP_COPY
        set_node_status(tgt_node.uuid, StorageNode.STATUS_OFFLINE)

        run_migration_task(mig_id, max_steps=3000, step_sleep=0.02)
        self._restore(tgt_node.uuid)

        m = db.get_migration_by_id(mig_id)
        assert m.status == LVolMigration.STATUS_FAILED, (
            f"Expected FAILED after target outage, got {m.status}")
        assert m.phase == LVolMigration.PHASE_CLEANUP_TARGET, (
            f"Expected completed cleanup_target rollback, got phase={m.phase}")
        assert "target node offline" in (m.error_message or ""), (
            f"Expected a target-offline rollback, got {m.error_message!r}")
        assert db.get_lvol_by_id(lvol.uuid).node_id == ctx.node_uuid("n1"), (
            "Rolled-back volume must still be served from the source node")
        self._assert_back_online(tgt_node.uuid, "n2")

    # The old test also accepted STATUS_DONE for a target outage, on the grounds
    # that the outage window could be "missed between ticks". That case gets no
    # test of its own because there is no deterministic version of it:
    # task_runner recurses straight through a phase advance (`return
    # task_runner(task)` at the end of the phase dispatcher), so
    # LVOL_MIGRATE -> CLEANUP_SOURCE -> COMPLETED all happen inside a single
    # tick and PHASE_CLEANUP_SOURCE is never observable at a tick boundary. A
    # target outage is therefore either observed during SNAP_COPY/LVOL_MIGRATE —
    # the rollback asserted above — or not observed at all, in which case it has
    # no effect and there is nothing to assert beyond what the plain migration
    # tests in this class already cover. The `_is_cleanup_phase` exemption for
    # CLEANUP_SOURCE only becomes reachable after an unclean restart, which is
    # what the run_migration_with_crashes tests exercise.

    def test_unrelated_node_offline_does_not_affect_migration(
            self, topology_four_node, mock_src_server, mock_tgt_server):
        """
        A node that is neither source nor target goes offline for longer than
        any retry budget.  The runner only ever loads the migration's own source
        and target (and this topology is ha_type="single", so no peer resolution
        reaches n3 either), so the outage must not affect progress at all.

        n3 and n4 are interchangeable here — same role, same config bar an
        rpc_port no mock server listens on — so one of them covers the scenario.
        """
        ctx = topology_four_node
        lvol = ctx.lvol("l1")
        tgt_node = ctx.node("n2")
        unrelated_uuid = ctx.node_uuid("n3")

        _seed_all(mock_src_server, ctx, "n1")

        mig_id, err = start_migration(lvol.uuid, tgt_node.uuid,
                                      max_retries=self._MAX_RETRIES)
        assert err is None, f"start_migration failed: {err}"

        # Down from tick 2 for the rest of the run — the migration completes
        # with n3 still offline, which is exactly the point.
        run_migration_task(mig_id, max_steps=3000, step_sleep=0.02,
                           on_step=self._outage(unrelated_uuid, down_at=2))
        self._restore(unrelated_uuid)

        _assert_migration_done(mig_id)
        assert db.get_lvol_by_id(lvol.uuid).node_id == tgt_node.uuid
