"""_take_intermediate_snapshot must not replay the same snapshot name across
retries of the same (stuck) round.

On failure it jumps ``intermediate_snap_rounds`` straight to max without
advancing further, so a caller that retries (the group-worker barrier, or the
solo migration's own while-loop) re-enters with the round unchanged. A
round-only name (``_mig_<id>_r<round>``) would be replayed identically on
every such retry, colliding with the previous attempt's own SPDK-side
leftover -- a "not leader" rejection can still partially register the bdev.
Live trace 2026-09-28: group worker 38e416d9 retried round 3 under the
identical name "_mig_38e416d9_r3" four times, each retry re-colliding with
itself, until the group gave up. The fix folds a fresh vuid into the name on
every call so a retry always gets a name nothing has used before.
"""
from unittest.mock import MagicMock, patch

from simplyblock_core.services import tasks_runner_lvol_migration as runner


def _migration():
    m = MagicMock()
    m.uuid = "38e416d9-0000-0000-0000-000000000000"
    m.lvol_id = "lvol-1"
    m.intermediate_snap_rounds = 3
    m.max_intermediate_snap_rounds = 3
    m.intermediate_snaps = []
    m.snap_migration_plan = []
    return m


def test_retry_at_same_stuck_round_gets_a_fresh_name():
    migration = _migration()
    seen_names = []

    def _fake_add(lvol_id, snapshot_name, **kwargs):
        seen_names.append(snapshot_name)
        return None, "Failed to create snapshot on node: some-node"

    with patch.object(runner, "db") as mock_db, \
         patch.object(runner.snapshot_controller, "add", side_effect=_fake_add):
        mock_db.next_vuid.side_effect = [101, 102, 103]
        mock_db.kv_store = MagicMock()

        # Three retries of the same (never-advancing) round.
        runner._take_intermediate_snapshot(migration)
        runner._take_intermediate_snapshot(migration)
        runner._take_intermediate_snapshot(migration)

    assert len(seen_names) == 3
    assert len(set(seen_names)) == 3, f"retries reused a name: {seen_names}"
    for name in seen_names:
        assert name.startswith("_mig_38e416d9_r3_")
