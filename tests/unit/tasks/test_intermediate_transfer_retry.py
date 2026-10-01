"""Intermediate-phase retry behaviour of the lvol migration runner.

2026-09-30, run 26 on vm12: every intermediate transfer of a batch was timing
out in SPDK, and each retry took a fresh snapshot instead of re-sending the one
whose transfer failed -- 36 snapshots on one lvstore in two minutes, the round
counter at "10/3". A chain-lock timeout on one of those snapshots then escaped
uncaught and restarted the whole runner.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.exceptions import ChainLockTimeout, PreconditionError
from simplyblock_core.services import tasks_runner_lvol_migration as runner


def _migration(**kw):
    m = MagicMock()
    m.uuid = "m1aaaaaa-0000"
    m.lvol_id = "lvol-1"
    m.intermediate_snap_rounds = 0
    m.max_intermediate_snap_rounds = 3
    m.intermediate_snaps = []
    m.snap_migration_plan = ["planned-1"]
    m.target_snap_bdevs = []
    m.transfer_context = {}
    for k, v in kw.items():
        setattr(m, k, v)
    return m


# --- _take_intermediate_snapshot ---------------------------------------------

def test_a_chain_lock_timeout_defers_instead_of_escaping():
    m = _migration()
    with patch.object(runner.snapshot_controller, "add",
                      side_effect=ChainLockTimeout("Timed out acquiring chain lock on x")):
        assert runner._take_intermediate_snapshot(m) == runner._SNAP_BUSY
    assert m.intermediate_snap_rounds == 0, "a deferred snapshot must not use up a round"
    assert m.snap_migration_plan == ["planned-1"]


def test_the_lock_timeout_is_still_a_precondition_error():
    """Callers that map PreconditionError to a 400 keep doing so."""
    assert issubclass(ChainLockTimeout, PreconditionError)


def test_a_refused_snapshot_closes_the_rounds():
    m = _migration()
    with patch.object(runner.snapshot_controller, "add", return_value=(None, "snapshot limit")):
        assert runner._take_intermediate_snapshot(m) == runner._SNAP_SKIPPED
    assert m.intermediate_snap_rounds == m.max_intermediate_snap_rounds


def test_a_taken_snapshot_is_planned_and_counted():
    m = _migration()
    with patch.object(runner.snapshot_controller, "add", return_value=("snap-new", None)):
        assert runner._take_intermediate_snapshot(m) == runner._SNAP_TAKEN
    assert m.snap_migration_plan[-1] == "snap-new"
    assert m.intermediate_snap_rounds == 1


# --- group intermediate: a failed transfer is retried with the same snapshot --

def _group_patches(take, setup):
    snap = SimpleNamespace(uuid="snap-r0")
    return [
        patch.object(runner, "_get_migration_nic", return_value=("tcp", None)),
        patch.object(runner, "db", MagicMock(**{
            "get_snapshot_by_id.return_value": snap,
            "get_lvol_by_id.return_value": SimpleNamespace(size=5 << 30),
        })),
        patch.object(runner, "_snap_tgt_short_name", return_value="SNAP_1m"),
        patch.object(runner, "_snap_composite", return_value="LVS_2/SNAP_1"),
        patch.object(runner, "_get_target_secondary_node", return_value=(None, None)),
        patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)),
        patch.object(runner, "_take_intermediate_snapshot", take),
        patch.object(runner, "_setup_snap_transfer", setup),
    ]


def _run(migration, src_rpc, tgt_rpc, take, setup):
    ps = _group_patches(take, setup)
    for p in ps:
        p.start()
    try:
        return runner._handle_group_intermediate(
            migration, MagicMock(lvstore="LVS_2"), MagicMock(lvstore="LVS_1"),
            src_rpc, tgt_rpc, target_round=0)
    finally:
        for p in ps:
            p.stop()


def test_a_failed_transfer_is_resent_without_a_new_snapshot():
    m = _migration(
        intermediate_snap_rounds=1,
        snap_migration_plan=["planned-1", "snap-r0"],
        transfer_context={'stage': 'intermediate_transfer',
                          'transfer': {'snap_uuid': 'snap-r0', 'transfer_done': False}})
    src_rpc = MagicMock(**{"bdev_lvol_transfer_stat.return_value": {'transfer_state': 'Failed'}})
    tgt_rpc = MagicMock(**{"get_bdevs.return_value": []})
    take = MagicMock()
    setup = MagicMock(return_value=({'snap_uuid': 'snap-r0'}, None))

    # 1. The poll sees the failure: suspend, and remember which snapshot failed.
    done, suspend, error = _run(m, src_rpc, tgt_rpc, take, setup)
    assert (done, suspend) == (False, True) and "Failed" in error
    assert m.transfer_context == {'stage': 'intermediate_retry', 'snap_uuid': 'snap-r0'}

    # 2. The retry sends the same snapshot again; nothing new is taken.
    done, suspend, error = _run(m, src_rpc, tgt_rpc, take, setup)
    take.assert_not_called()
    args, _ = setup.call_args
    assert args[1] == 1, "the retry must reuse the failed snapshot's plan index"
    assert m.transfer_context['stage'] == 'intermediate_transfer'
    assert m.transfer_context['transfer']['snap_uuid'] == 'snap-r0'
    assert m.intermediate_snap_rounds == 1, "a retry is not a new round"


def test_a_busy_chain_suspends_without_an_error():
    m = _migration()
    take = MagicMock(return_value=runner._SNAP_BUSY)
    done, suspend, error = _run(m, MagicMock(), MagicMock(), take, MagicMock())
    assert (done, suspend, error) == (False, True, None), \
        "a busy lock must suspend without charging the worker's retry budget"


# --- the duplicate-sentinel log bug ------------------------------------------

def test_the_size_log_queries_on_its_default_path():
    rpc = MagicMock(**{"get_bdevs.return_value": [{'num_blocks': 1310720, 'block_size': 4096}]})
    assert runner._log_spdk_bdev_size(rpc, "LVS_2/SNAP_1", "SRC") == 1310720 * 4096
    rpc.get_bdevs.assert_called_once_with("LVS_2/SNAP_1")


# --- batch final step ---------------------------------------------------------

def test_the_batch_final_step_waits_longer_than_spdk_does():
    import inspect

    from simplyblock_core.services import tasks_runner_batch_migration as batch
    src = inspect.getsource(batch)
    assert "final_step_rpc = src_node.rpc_client(timeout=20" in src
