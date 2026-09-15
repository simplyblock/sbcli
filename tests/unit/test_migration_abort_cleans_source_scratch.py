"""An abandoned migration must not leave its own scratch on the source.

_take_intermediate_snapshot creates real snapshots on the SOURCE node
("_mig_<id>_r<n>") whose only purpose is to shrink the delta to transfer. The
success path deletes them from _handle_cleanup_source; an aborted migration
never reaches that, and _handle_cleanup_target rolled back only the target.

Leaving them is blocking, not untidy. remove_storage_node refuses a node that
has snapshots, so a failed drain left an orphan on the very node being removed
and permanently prevented the operator re-drive that STATUS_REMOVED_FAILED
exists to offer -- with no command able to clear it (`volume migrate-cleanup`
is scoped to the target). Cluster a6e7569d, 2026-09-15, twice:

    Can not remove node a1b050f1-...: 1 snapshot(s) present. Remove them first.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import tasks_runner_lvol_migration as runner


def _snap(uuid, deleted=False, name="_mig_abcd1234_r0"):
    s = MagicMock()
    s.get_id.return_value = uuid
    s.deleted = deleted
    s.snap_name = name
    return s


def _cleanup(intermediates, snaps=None, delete_raises=False):
    """Run the source-scratch cleanup; return the uuids it deleted."""
    deleted = []
    migration = MagicMock()
    migration.intermediate_snaps = list(intermediates)
    known = snaps if snaps is not None else {u: _snap(u) for u in intermediates}

    def _get(uuid):
        if uuid not in known:
            raise KeyError(uuid)
        return known[uuid]

    def _delete(uuid, force_delete=False):
        if delete_raises:
            raise RuntimeError("lvstore leaderless")
        deleted.append(uuid)
        return True

    db = MagicMock()
    db.get_snapshot_by_id.side_effect = _get
    with patch.object(runner, "db", db), \
         patch.object(runner.snapshot_controller, "delete", side_effect=_delete):
        runner._delete_source_intermediates(migration)
    return deleted


class TestAbortDeletesItsOwnScratch(unittest.TestCase):

    def test_intermediates_are_deleted(self):
        self.assertEqual(sorted(_cleanup(["s1", "s2"])), ["s1", "s2"])

    def test_the_delete_is_forced(self):
        """The source node is down -- that is why the drain existed. A
        non-forced delete cannot reach it."""
        migration = MagicMock()
        migration.intermediate_snaps = ["s1"]
        db = MagicMock()
        db.get_snapshot_by_id.return_value = _snap("s1")
        with patch.object(runner, "db", db), \
             patch.object(runner.snapshot_controller, "delete") as dele:
            runner._delete_source_intermediates(migration)
        dele.assert_called_once_with("s1", force_delete=True)

    def test_a_migration_with_no_intermediates_is_unaffected(self):
        self.assertEqual(_cleanup([]), [])

    def test_none_is_tolerated(self):
        migration = MagicMock()
        migration.intermediate_snaps = None
        with patch.object(runner, "db", MagicMock()), \
             patch.object(runner.snapshot_controller, "delete") as dele:
            runner._delete_source_intermediates(migration)
        dele.assert_not_called()

    def test_an_already_deleted_snapshot_is_skipped(self):
        self.assertEqual(
            _cleanup(["s1"], snaps={"s1": _snap("s1", deleted=True)}), [])

    def test_a_snapshot_already_gone_from_the_db_is_skipped(self):
        self.assertEqual(_cleanup(["s1"], snaps={}), [])


class TestItNeverTrapsTheRollback(unittest.TestCase):

    def test_a_failing_delete_is_swallowed(self):
        """A snapshot that will not delete must not keep the migration in
        cleanup for ever -- log it and let the rollback finish."""
        self.assertEqual(_cleanup(["s1"], delete_raises=True), [])

    def test_one_failure_does_not_stop_the_others(self):
        deleted = []
        calls = {"n": 0}

        def _delete(uuid, force_delete=False):
            calls["n"] += 1
            if uuid == "bad":
                raise RuntimeError("nope")
            deleted.append(uuid)

        migration = MagicMock()
        migration.intermediate_snaps = ["bad", "good"]
        db = MagicMock()
        db.get_snapshot_by_id.side_effect = lambda u: _snap(u)
        with patch.object(runner, "db", db), \
             patch.object(runner.snapshot_controller, "delete", side_effect=_delete):
            runner._delete_source_intermediates(migration)
        self.assertEqual(deleted, ["good"])
        self.assertEqual(calls["n"], 2)


class TestTheRollbackCallsIt(unittest.TestCase):

    def test_cleanup_target_deletes_source_scratch(self):
        import inspect
        src = inspect.getsource(runner._handle_cleanup_target)
        self.assertIn("_delete_source_intermediates(migration)", src,
                      "the abort path must clean up what the migration created")

    def test_only_intermediates_are_named(self):
        """snap_migration_plan carries the user's snapshots; the cleanup must
        never reach for it."""
        import inspect
        fn = runner._delete_source_intermediates
        body = inspect.getsource(fn).replace(fn.__doc__ or "", "")
        self.assertIn("migration.intermediate_snaps", body)
        self.assertNotIn("snap_migration_plan", body,
                         "the user's own snapshots are not this function's to delete")


if __name__ == "__main__":
    unittest.main()
