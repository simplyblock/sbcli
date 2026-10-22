"""Migration-plan forecast arithmetic (docs/online-migration.md, G3)."""
from simplyblock_core import constants
from simplyblock_core import migration_forecast as mf

MB = 1024 * 1024


def _vol(lvol_id, lvs, state, remaining):
    return {"lvol_id": lvol_id, "lvs_name": lvs, "state": state,
            "bytes_remaining": remaining}


def test_throughput_ignores_orchestration_dominated_samples():
    samples = [(100 * MB, 10.0), (1 * MB, 0.2), (0, 5.0)]
    assert mf.measured_throughput(samples) == 10 * MB


def test_no_usable_sample_means_no_measurement():
    assert mf.measured_throughput([(MB, 0.1)]) is None
    assert mf.measured_throughput([]) is None


def test_cutover_default_without_measurements():
    assert mf.typical_cutover_seconds([]) == constants.REPL_FORECAST_DEFAULT_CUTOVER_SEC
    assert mf.typical_cutover_seconds([10, 30, 20]) == 20


def test_one_lvstore_transfers_and_cuts_over_in_sequence():
    vols = [_vol("a", "L1", "replicating", 100 * MB),
            _vol("b", "L1", "replicating", 50 * MB)]
    per, overall = mf.forecast(vols, {"L1": 10 * MB}, 5, now=1000)
    # a: 10s transfer + 5s cutover; b: transfer done at 15s, claim free at 15s.
    assert per == {"a": 15, "b": 20}
    assert overall["eta_seconds"] == 20
    assert overall["completes_at"] == 1020
    assert overall["bytes_remaining"] == 150 * MB
    assert overall["throughput_measured"] is True


def test_lvstores_progress_in_parallel():
    vols = [_vol("a", "L1", "replicating", 100 * MB),
            _vol("b", "L2", "replicating", 300 * MB)]
    per, overall = mf.forecast(vols, {"L1": 10 * MB, "L2": 10 * MB}, 0, now=0)
    assert per == {"a": 10, "b": 30}
    assert overall["eta_seconds"] == 30


def test_endgame_volumes_hold_the_claim_first():
    vols = [_vol("a", "L1", "replicating", 0),
            _vol("b", "L1", "cutting_over", 0)]
    per, _ = mf.forecast(vols, {"L1": MB}, 10, now=0)
    assert per == {"b": 10, "a": 20}


def test_terminal_volumes_contribute_nothing():
    vols = [_vol("a", "L1", "done", 0), _vol("b", "L1", "skipped", 10 * MB)]
    per, overall = mf.forecast(vols, {}, 10, now=0)
    assert per == {} and overall["eta_seconds"] == 0 and overall["bytes_remaining"] == 0


def test_default_rate_when_unmeasured_is_flagged():
    vols = [_vol("a", "L1", "replicating", constants.REPL_FORECAST_DEFAULT_BYTES_PER_SEC * 4)]
    per, overall = mf.forecast(vols, {"L1": None}, 0, now=0)
    assert per == {"a": 4}
    assert overall["throughput_measured"] is False
