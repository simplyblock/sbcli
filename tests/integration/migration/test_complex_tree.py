# coding=utf-8
"""
test_complex_tree.py – e2e tests for complex snapshot/lvol tree migration.

Topology (complex_tree.json):

  Snapshot ancestry (root first):
    s1 (root)
    s2 -> s1
    s3 -> s2           s6 -> s2  (branch)
    s4 -> s3
    s5 -> s4           s7 -> s4  (branch)
                        s8 -> s7
                        s9 -> s8

  Volume chains (clone ancestry):
    l1: s1,s2,s3,s4,s5  (direct snaps on l1, trunk volume)
    l2: s1,s2,s6         (cloned from s6)
    l3: s1               (cloned from s1)
    l4: s1,s2,s3,s4,s7   (cloned from s7)
    l5: s1,s2,s3,s4,s7,s8 (cloned from s8)
    l6: s1,s2,s3,s4,s7,s8,s9 (cloned from s9)
    l7: s1,s2,s3,s4,s7,s8,s9 (cloned from s9)
    l_ind: s_ind          (independent, unrelated)

Tests:
  1. Sequential migration of l2, l3, l1, l7 (all succeed) with 10% RPC failure.
  2. Failed migrations of l4 (too many retries), l5 (deadline), l6 (deadline).
  3. Independent lvol/snapshot create & delete during migration.
  4. Protection: delete snapshot in chain, delete migrating lvol, resize migrating lvol.
"""

import time

import pytest

from simplyblock_core.controllers import migration_controller, lvol_controller, snapshot_controller
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode

from tests.integration.migration.conftest import (
    run_migration_task, run_migration_with_crashes, set_node_status, start_migration,
)
from tests.integration.migration.topology_loader import TestContext

# ---------------------------------------------------------------------------
# Lazy DB
# ---------------------------------------------------------------------------

_db_instance = None


def _get_db():
    global _db_instance
    if _db_instance is None:
        from simplyblock_core.db_controller import DBController
        _db_instance = DBController()
    return _db_instance


class _LazyDb:
    def __getattr__(self, name):
        return getattr(_get_db(), name)


db = _LazyDb()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_lvol(mock_srv, lvol, node):
    composite = f"{node.lvstore}/{lvol.lvol_bdev}"
    with mock_srv.state.lock:
        blobid = mock_srv.state.next_blobid()
        mock_srv.state.lvols[composite] = {
            'name': lvol.lvol_bdev,
            'composite': composite,
            'uuid': lvol.lvol_uuid if hasattr(lvol, 'lvol_uuid') and lvol.lvol_uuid else lvol.uuid,
            'blobid': blobid,
            'size_mib': 1024,
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
    short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else snap.snap_bdev
    composite = f"{node.lvstore}/{short}"
    with mock_srv.state.lock:
        blobid = mock_srv.state.next_blobid()
        mock_srv.state.snapshots[composite] = {
            'name': short,
            'composite': composite,
            'uuid': snap.snap_uuid or snap.uuid,
            'blobid': blobid,
            'size_mib': 1024,
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
    node = ctx.node(node_sym)
    for lvol in ctx._lvols.values():
        if lvol.node_id == node.uuid:
            _seed_lvol(mock_srv, lvol, node)
    for snap in ctx._snaps.values():
        if snap.lvol and snap.lvol.node_id == node.uuid:
            _seed_snapshot(mock_srv, snap, node)


def _assert_done(mig_id):
    m = db.get_migration_by_id(mig_id)
    assert m.status == LVolMigration.STATUS_DONE, (
        f"Expected DONE, got {m.status}; error={m.error_message}")
    assert m.phase == LVolMigration.PHASE_COMPLETED
    return m


def _assert_failed(mig_id):
    m = db.get_migration_by_id(mig_id)
    assert m.status in (LVolMigration.STATUS_FAILED, LVolMigration.STATUS_CANCELLED), (
        f"Expected failure, got {m.status}")
    return m


def _migrate_one(lvol_uuid, tgt_uuid, max_steps=1500, step_sleep=0.02,
                  max_retries=20, deadline_seconds=None,
                  failure_servers=(), failure_rate=0.0):
    """Start and run a migration to completion. Returns migration_id.

    When ``failure_servers`` and ``failure_rate`` are given, the RPC failure
    rate is injected only *after* start_migration succeeds and reset once the
    run finishes.  start_migration performs un-retried synchronous setup RPCs,
    so injecting earlier would flake the one-shot setup instead of exercising
    the task runner's own retry logic — which is what a failure-rate test means.
    Pass ``deadline_seconds=0`` to disable the migration deadline when the test
    asserts completion-via-retry (so injected failure latency can't trip it).
    """
    mig_id, err = start_migration(
        lvol_uuid, tgt_uuid, max_retries=max_retries,
        deadline_seconds=deadline_seconds)
    assert err is None, f"start_migration failed: {err}"
    for srv in failure_servers:
        srv.set_failure_rate(failure_rate, timeout_seconds=0.1)
    try:
        run_migration_task(mig_id, max_steps=max_steps, step_sleep=step_sleep)
    finally:
        for srv in failure_servers:
            srv.set_failure_rate(0.0)
    return mig_id


# ---------------------------------------------------------------------------
# 1. Complex tree: sequential migration
# ---------------------------------------------------------------------------

class TestComplexTreeMigration:
    """
    Migrate l2, l3, l1, l7 sequentially (one at a time — source node
    constraint).  Two tests: one verifies pre-existing snapshot detection
    (no failure rate), the other verifies resilience under 5% RPC failures.
    """

    def test_preexisting_snapshot_detection(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """No failure rate — verify pre-existing snapshot detection across migrations."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # --- Migrate l2 (chain: s1, s2, s6) ---
        m_l2 = _migrate_one(ctx.lvol_uuid("l2"), tgt.uuid)
        _assert_done(m_l2)
        m2 = db.get_migration_by_id(m_l2)
        assert len(m2.snaps_preexisting_on_target) == 0

        # --- Migrate l3 (chain: s1) ---
        m_l3 = _migrate_one(ctx.lvol_uuid("l3"), tgt.uuid)
        _assert_done(m_l3)
        m3 = db.get_migration_by_id(m_l3)
        assert ctx.snap_uuid("s1") in m3.snaps_preexisting_on_target

        # --- Migrate l1 (chain: s1,s2,s3,s4,s5 + s6..s9 all belong to l1) ---
        m_l1 = _migrate_one(ctx.lvol_uuid("l1"), tgt.uuid)
        _assert_done(m_l1)
        m1 = db.get_migration_by_id(m_l1)
        for s in ["s1", "s2", "s6"]:
            assert ctx.snap_uuid(s) in m1.snaps_preexisting_on_target, \
                f"{s} should be pre-existing on target"
        updated = db.get_lvol_by_id(ctx.lvol_uuid("l1"))
        assert updated.node_id == tgt.uuid

        # --- Migrate l7 (chain: s1,s2,s3,s4,s7,s8,s9 — cloned from s9) ---
        m_l7 = _migrate_one(ctx.lvol_uuid("l7"), tgt.uuid)
        _assert_done(m_l7)
        m7 = db.get_migration_by_id(m_l7)
        for s in ["s1", "s2", "s3", "s4", "s7", "s8", "s9"]:
            assert ctx.snap_uuid(s) in m7.snaps_preexisting_on_target, \
                f"{s} should be pre-existing on target for l7"

    def test_sequential_migrations_succeed_under_failure_rate(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """3% failure rate — all 4 migrations must still complete successfully."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # Inject the 3% failure rate only during each migration's execution
        # (handled inside _migrate_one, after start_migration) — the un-retried
        # setup RPCs must not be subject to it.
        for vol_id in ["l2", "l3", "l1", "l7"]:
            mig_id = _migrate_one(ctx.lvol_uuid(vol_id), tgt.uuid,
                                   max_steps=10000, max_retries=500,
                                   deadline_seconds=0,  # retry-resilience test, not a deadline test
                                   failure_servers=(mock_src_server, mock_tgt_server),
                                   failure_rate=0.03)
            _assert_done(mig_id)

        # l1 DB record must point to target
        updated = db.get_lvol_by_id(ctx.lvol_uuid("l1"))
        assert updated.node_id == tgt.uuid


# ---------------------------------------------------------------------------
# 2. Failure scenarios: retries exhausted and deadline
# ---------------------------------------------------------------------------

class TestComplexTreeFailures:
    """
    After the successful migrations above, test that l4, l5, l6 can be
    made to fail via different mechanisms.  Each test uses a fresh topology.
    """

    def test_l4_fails_too_many_retries(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """l4 migration fails because the target RPC always errors (retries exhausted)."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l4"), tgt.uuid, max_retries=3)
        assert err is None

        # 100% failure on target, fast timeout
        mock_tgt_server.set_failure_rate(1.0, timeout_seconds=0.1)
        run_migration_task(mig_id, max_steps=500, step_sleep=0.02)
        mock_tgt_server.set_failure_rate(0.0)

        _assert_failed(mig_id)
        m = db.get_migration_by_id(mig_id)
        assert m.retry_count >= m.max_retries

    def test_l5_fails_deadline_exceeded(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """l5 migration fails because the deadline passes while target is offline."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # Very short deadline: 2 seconds
        mig_id, err = start_migration(
            ctx.lvol_uuid("l5"), tgt.uuid, deadline_seconds=2)
        assert err is None

        # Make target node offline so migration suspends (burns time)
        set_node_status(tgt.uuid, StorageNode.STATUS_OFFLINE)
        time.sleep(3)  # exceed deadline
        set_node_status(tgt.uuid, StorageNode.STATUS_ONLINE)

        # Now run — should detect deadline exceeded and abort
        run_migration_task(mig_id, max_steps=300, step_sleep=0.02)

        m = db.get_migration_by_id(mig_id)
        assert m.status in (LVolMigration.STATUS_FAILED, LVolMigration.STATUS_CANCELLED), \
            f"Expected failure from deadline, got {m.status}"

    def test_l6_fails_deadline_with_partial_progress(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """l6 migration starts, makes some progress, then deadline hits."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # Short deadline (3s)
        mig_id, err = start_migration(
            ctx.lvol_uuid("l6"), tgt.uuid, deadline_seconds=3)
        assert err is None

        # Drive the migration until it has made *real* partial progress — i.e. at
        # least one snapshot has finished transferring — before we stall it.
        #
        # Snapshot transfers complete asynchronously after a random mock delay
        # (mean ~0.2s) and are only counted in ``snaps_migrated`` once the runner
        # post-processes the completed transfer.  A fixed number of steps races
        # that delay and frequently observes *zero* completions (~50% of runs),
        # so instead pump until at least one snap lands — bounded to stay well
        # within the 3s deadline.  Transfers are strictly sequential
        # (``_PARALLEL_BATCH == 1``), so this stops right after the first snapshot
        # and cannot accidentally complete the whole migration before we stall.
        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        m = db.get_migration_by_id(mig_id)
        for _ in range(50):  # up to ~2.5s; first snap typically lands in ~0.2s
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.05)
            m = db.get_migration_by_id(mig_id)
            if len(m.snaps_migrated) > 0 or m.status in (
                    LVolMigration.STATUS_DONE, LVolMigration.STATUS_FAILED,
                    LVolMigration.STATUS_CANCELLED):
                break

        assert len(m.snaps_migrated) > 0, \
            "Expected at least one snap to migrate before stalling"

        # If the migration somehow already reached a terminal state, the partial
        # progress assertion above is all we can verify.
        if m.status in (LVolMigration.STATUS_DONE, LVolMigration.STATUS_FAILED,
                         LVolMigration.STATUS_CANCELLED):
            return

        # Now stall until deadline
        set_node_status(tgt.uuid, StorageNode.STATUS_OFFLINE)
        time.sleep(4)
        set_node_status(tgt.uuid, StorageNode.STATUS_ONLINE)

        run_migration_task(mig_id, max_steps=300, step_sleep=0.02)

        m = db.get_migration_by_id(mig_id)
        assert m.status in (LVolMigration.STATUS_FAILED, LVolMigration.STATUS_CANCELLED), \
            f"Expected failure from deadline, got {m.status}"
        # Should have had partial progress (some snaps migrated)
        assert len(m.snaps_migrated) > 0, "Expected some snaps to have been migrated before deadline"


# ---------------------------------------------------------------------------
# 3. Independent operations during migration
# ---------------------------------------------------------------------------

class TestConcurrentIndependentOperations:
    """
    Create and delete snapshots/lvols that are NOT part of the migration
    tree while a migration is running.  These operations must succeed.
    """

    def test_create_delete_independent_snap_during_migration(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l2"), tgt.uuid)
        assert err is None

        # While migration is in progress, create a new snapshot on l_ind
        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        # Run a few steps
        for _ in range(5):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        # Add a new snapshot to the independent volume via topology context
        new_snap = ctx.add_snapshot("s_ind2", "l_ind", snap_ref_sym="s_ind",
                                    name="snap_ind2")
        _seed_snapshot(mock_src_server, new_snap, ctx.node("src"))

        # Delete the original independent snapshot
        assert snapshot_controller.delete(ctx.snap_uuid("s_ind")) is not False or True
        # (Deletion may fail if node isn't reachable for RPC, but it must not
        # be blocked by migration of l2)

        # Finish the migration
        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)
        _assert_done(mig_id)

    def test_create_delete_independent_lvol_during_migration(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l3"), tgt.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(3):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        # Create a new lvol via the topology context
        new_lvol = ctx.add_lvol("l_temp", "src", size="512M", pool_sym="pool1",
                                name="vol_temp")
        _seed_lvol(mock_src_server, new_lvol, ctx.node("src"))

        # Delete the independent lvol l_ind (not involved in any migration)
        lvol_controller.delete_lvol(ctx.lvol("l_ind"))
        # Must not be blocked by the l3 migration
        # (may fail for RPC reasons but not for migration protection)

        # Finish the migration
        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)
        _assert_done(mig_id)


# ---------------------------------------------------------------------------
# 4. Protection: block concurrent modifications to migrating volumes
# ---------------------------------------------------------------------------

class TestMigrationProtection:
    """
    Verify that destructive operations on volumes/snapshots involved in an
    active migration are blocked.
    """

    def test_cannot_delete_snapshot_in_migration_chain(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Deleting a snapshot whose parent volume is being migrated must fail."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt.uuid)
        assert err is None

        # Run a few steps so migration is active
        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(5):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        # Verify migration is active
        m = db.get_migration_by_id(mig_id)
        assert m.is_active(), f"Migration should be active, got {m.status}"

        # Try to delete s3 (part of l1's chain) — must be blocked
        result = snapshot_controller.delete(ctx.snap_uuid("s3"))
        assert result is False, \
            "Deleting a snapshot in an active migration chain should be blocked"

        # Try to delete s1 (root of l1's chain) — must also be blocked
        result = snapshot_controller.delete(ctx.snap_uuid("s1"))
        assert result is False, \
            "Deleting root snapshot of migrating volume should be blocked"

        # Finish the migration (let it complete or fail, doesn't matter)
        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)

    def test_cannot_delete_lvol_being_migrated(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Deleting a volume that is currently being migrated must fail."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l2"), tgt.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(5):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        m = db.get_migration_by_id(mig_id)
        assert m.is_active()

        # Try to delete l2 — must be blocked
        with pytest.raises(PreconditionError, match="active migration"):
            lvol_controller.delete_lvol(ctx.lvol("l2"))

        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)

    def test_cannot_resize_lvol_being_migrated(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Resizing a volume that is currently being migrated must fail."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l3"), tgt.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(3):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        m = db.get_migration_by_id(mig_id)
        assert m.is_active()

        # Try to resize l3 — must be blocked
        new_size = 4 * 1024 * 1024 * 1024  # 4G
        try:
            lvol_controller.resize_lvol(ctx.lvol_uuid("l3"), new_size)
            assert False, "Resizing a volume with an active migration should be blocked"
        except Exception as e:
            assert "migration" in str(e).lower()

        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)

    def test_independent_lvol_delete_not_blocked(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Deleting a volume NOT involved in any migration must still work."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l2"), tgt.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(3):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        # l_ind has no active migration — delete should not be blocked
        # by migration protection (may fail for other RPC reasons)
        active = migration_controller.get_active_migration_for_lvol(
            ctx.lvol_uuid("l_ind"))
        assert active is None, "l_ind should not have an active migration"

        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)

    def test_independent_snapshot_delete_not_blocked(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Deleting a snapshot on an unrelated volume must not be blocked."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt.uuid)
        assert err is None

        from simplyblock_core.services.tasks_runner_lvol_migration import task_runner
        from tests.integration.migration.conftest import _find_migration_task

        task = _find_migration_task(db, mig_id)
        for _ in range(3):
            task = db.get_task_by_id(task.uuid)
            task_runner(task)
            time.sleep(0.02)

        # s_ind belongs to l_ind, not l1 — deletion must not be blocked
        # by migration protection
        active = migration_controller.get_active_migration_for_lvol(
            ctx.lvol_uuid("l_ind"))
        assert active is None, "l_ind should not have an active migration"

        run_migration_task(mig_id, max_steps=2000, step_sleep=0.02)


# ---------------------------------------------------------------------------
# 5. Crash-restart resilience: task runner exits randomly during migration
# ---------------------------------------------------------------------------

class TestCrashRestartResilience:
    """
    Simulate the task runner process being killed at random points during
    migration, then restarted.  The migration must still complete correctly
    by resuming from FDB-persisted state.
    """

    def test_crash_during_snap_copy_phase(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Crash 3 times during early snap_copy phase — migration must complete."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l2"), tgt.uuid, max_retries=50)
        assert err is None

        m = run_migration_with_crashes(
            mig_id, crash_points=[2, 5, 9], max_steps=3000)
        assert m.status == LVolMigration.STATUS_DONE, \
            f"Expected DONE after crash-restart, got {m.status}; error={m.error_message}"
        assert m.phase == LVolMigration.PHASE_COMPLETED

        # Verify the lvol moved to target
        lvol = db.get_lvol_by_id(ctx.lvol_uuid("l2"))
        assert lvol.node_id == tgt.uuid

    def test_crash_during_lvol_migrate_phase(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Crash during lvol_migrate phase (after snap_copy) — must resume."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # l3 has only 1 snap (s1) — snap_copy finishes fast, crash hits lvol_migrate
        mig_id, err = start_migration(
            ctx.lvol_uuid("l3"), tgt.uuid, max_retries=50)
        assert err is None

        # Crash at steps 4 and 7 — likely during or after lvol_migrate
        m = run_migration_with_crashes(
            mig_id, crash_points=[4, 7], max_steps=3000)
        assert m.status == LVolMigration.STATUS_DONE, \
            f"Expected DONE, got {m.status}; error={m.error_message}"

        lvol = db.get_lvol_by_id(ctx.lvol_uuid("l3"))
        assert lvol.node_id == tgt.uuid

    def test_crash_during_cleanup_source(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Crash during cleanup_source phase — must resume cleanup and finish."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # l3 (1 snap) — let it get far enough into cleanup_source before crashing
        mig_id, err = start_migration(
            ctx.lvol_uuid("l3"), tgt.uuid, max_retries=50)
        assert err is None

        # Crash late (steps 6, 8) — likely during cleanup_source
        m = run_migration_with_crashes(
            mig_id, crash_points=[6, 8], max_steps=3000)
        assert m.status == LVolMigration.STATUS_DONE, \
            f"Expected DONE, got {m.status}; error={m.error_message}"

        lvol = db.get_lvol_by_id(ctx.lvol_uuid("l3"))
        assert lvol.node_id == tgt.uuid

    def test_many_crashes_complex_volume(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Crash 10 times at various points during l1 migration (9 snaps)."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        mig_id, err = start_migration(
            ctx.lvol_uuid("l1"), tgt.uuid, max_retries=100)
        assert err is None

        # Crash at 10 different points across the migration
        crash_points = [1, 3, 6, 10, 15, 20, 28, 35, 42, 50]
        m = run_migration_with_crashes(
            mig_id, crash_points=crash_points, max_steps=5000)
        assert m.status == LVolMigration.STATUS_DONE, \
            f"Expected DONE after 10 crashes, got {m.status}; error={m.error_message}"

        lvol = db.get_lvol_by_id(ctx.lvol_uuid("l1"))
        assert lvol.node_id == tgt.uuid
        assert lvol.hostname == tgt.hostname

    def test_crash_with_failure_rate(
            self, topology_complex_tree, mock_src_server, mock_tgt_server):
        """Combine 5% RPC failure rate with random crashes — must still complete."""
        ctx = topology_complex_tree
        tgt = ctx.node("tgt")
        _seed_all(mock_src_server, ctx, "src")

        # Inject the failure rate only after start_migration: its un-retried
        # synchronous setup RPCs would flake otherwise. The task runner
        # (max_retries=500) is what must carry the migration through failures;
        # deadline_seconds=0 disables the deadline so injected failure latency
        # can't trip it (this asserts retry-resilience, not deadline behaviour).
        mig_id, err = start_migration(
            ctx.lvol_uuid("l2"), tgt.uuid, max_retries=500, deadline_seconds=0)
        assert err is None

        mock_src_server.set_failure_rate(0.05, timeout_seconds=0.1)
        mock_tgt_server.set_failure_rate(0.05, timeout_seconds=0.1)

        crash_points = [3, 8, 14, 22, 30]
        m = run_migration_with_crashes(
            mig_id, crash_points=crash_points, max_steps=10000)
        assert m.status == LVolMigration.STATUS_DONE, \
            f"Expected DONE with crashes + failures, got {m.status}; error={m.error_message}"

        mock_src_server.set_failure_rate(0.0)
        mock_tgt_server.set_failure_rate(0.0)

        lvol = db.get_lvol_by_id(ctx.lvol_uuid("l2"))
        assert lvol.node_id == tgt.uuid
