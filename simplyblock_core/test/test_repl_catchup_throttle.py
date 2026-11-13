"""Catch-up throttle: a volume whose replication persistently cannot catch up
gets its source writes capped, from its own replication progress alone.

bf17 (2026-11-08): the last of eight volumes waited for hours behind seven
already migrated onto the same saturated target; the endgame throttle never
engaged because the volume never reached the endgame.
"""
import inspect
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import tasks_runner_replication_final as final

W = constants.REPL_CATCHUP_WINDOW_SEC


def _task(**params):
    t = JobSchedule()
    t.uuid = "F1"
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.function_params = {"lvol_id": "LV1", **params}
    t.status = JobSchedule.STATUS_RUNNING
    return t


def _lvol(w=0):
    lv = MagicMock()
    lv.get_id.return_value = "LV1"
    lv.rw_ios_per_sec = 0
    lv.rw_mbytes_per_sec = 0
    lv.r_mbytes_per_sec = 0
    lv.w_mbytes_per_sec = w
    return lv


def _record_inline(task, **params):
    task.function_params.update(params)
    return task


def _run(samples, start=1000.0, step=10.0):
    """Feed (lag, backlog) samples into _catchup_window_update; the params."""
    params: dict = {}
    now = start
    for lag, backlog in samples:
        params.update(final._catchup_window_update(params, lag, backlog, now))
        now += step
    return params


def _windows(per_window, n):
    """n windows of samples, each built by per_window(i) -> [(lag, backlog)]."""
    out = []
    for i in range(n):
        out.extend(per_window(i))
    return out


SAMPLES_PER_WINDOW = int(W // 10)


class TestWindows:

    def test_the_first_window_is_only_a_baseline(self):
        p = _run([(500.0, None)] * (SAMPLES_PER_WINDOW + 1))
        assert "catchup_prev" in p
        assert "catchup_strikes" not in p

    def test_a_lag_floor_that_does_not_drop_is_a_strike(self):
        # saw-tooth lag that never gets lower than 400 s
        tooth = [(400.0 + 10 * k, None) for k in range(SAMPLES_PER_WINDOW)]
        p = _run(_windows(lambda i: tooth, 3) + [(400.0, None)])
        assert p["catchup_strikes"] == 2

    def test_a_falling_lag_floor_is_progress(self):
        p = _run(_windows(lambda i: [(1000.0 - 300 * i, None)] * SAMPLES_PER_WINDOW, 3)
                 + [(100.0, None)])
        assert p["catchup_strikes"] == 0

    def test_a_shrinking_backlog_is_progress_even_with_a_flat_lag(self):
        gib = 1 << 30
        p = _run(_windows(lambda i: [(400.0, (20 - 5 * i) * gib)] * SAMPLES_PER_WINDOW, 3)
                 + [(400.0, 5 * gib)])
        assert p["catchup_strikes"] == 0

    def test_progress_resets_the_strikes(self):
        flat = [(400.0, None)] * SAMPLES_PER_WINDOW
        p = _run(flat + flat + flat)
        assert p["catchup_strikes"] == 1
        # the sample that closes a window counts in it
        p = _run(flat + flat + flat + [(100.0, None)])
        assert p["catchup_strikes"] == 0


class TestCap:

    def test_first_cap_is_half_the_measured_write_rate(self):
        stats = [MagicMock(write_bytes_ps=200_000_000)] * 3
        with patch.object(final.db, "get_lvol_stats", return_value=stats):
            assert final._next_catchup_cap({}, _lvol()) == 100

    def test_without_stats_it_starts_at_the_default(self):
        with patch.object(final.db, "get_lvol_stats", side_effect=KeyError("x")):
            assert final._next_catchup_cap({}, _lvol()) == constants.REPL_CATCHUP_THROTTLE_START_MBPS

    def test_later_caps_halve_down_to_the_floor(self):
        assert final._next_catchup_cap({"catchup_cap_mbps": 100}, _lvol()) == 50
        assert final._next_catchup_cap({"catchup_cap_mbps": 1}, _lvol()) == \
            constants.REPL_CUTOVER_THROTTLE_MIN_MBPS

    def test_enough_strikes_cap_the_source(self):
        task = _task(catchup_strikes=constants.REPL_CATCHUP_STRIKES - 1,
                     catchup_prev={"lag": 400.0, "backlog": None},
                     catchup_win={"start": 0.0, "lag": 450.0, "backlog": None})
        stats = [MagicMock(write_bytes_ps=200_000_000)]
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch.object(final.db, "get_lvol_stats", return_value=stats), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=True) as set_lvol:
            task = final._track_catchup(task, _lvol(), 450.0, None, shrink_deadline=1)
        set_lvol.assert_called_once_with("LV1", 0, 0, 0, 100)
        p = task.function_params
        assert p["catchup_cap_mbps"] == 100 and p["throttle_mbps"] == 100
        assert p["catchup_strikes"] == 0 and p["shrink_deadline"] == 1
        assert p["throttle_restore"] == [0, 0, 0, 0]

    def test_the_cap_is_reapplied_after_a_failed_endgame_restored_qos(self):
        # _restore_source_throttle dropped throttle_restore/throttle_mbps
        task = _task(catchup_cap_mbps=25)
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=True) as set_lvol:
            final._track_catchup(task, _lvol(), 450.0, None)
        set_lvol.assert_called_once_with("LV1", 0, 0, 0, 25)

    def test_an_applied_cap_is_not_resent_every_pass(self):
        task = _task(catchup_cap_mbps=25, throttle_mbps=25, throttle_restore=[0, 0, 0, 0])
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol") as set_lvol:
            final._track_catchup(task, _lvol(w=25), 450.0, None)
        set_lvol.assert_not_called()

    def test_the_endgame_only_tightens_a_catchup_cap(self):
        task = _task(throttle_mbps=4, throttle_restore=[0, 0, 0, 0])
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol") as set_lvol, \
                patch.object(constants, "REPL_CUTOVER_THROTTLE_W_MBPS", 8):
            task = final._throttle_source(task, _lvol(w=4))
        set_lvol.assert_not_called()
        assert task.function_params["throttle_restore"] == [0, 0, 0, 0]
        assert task.function_params["throttle_tried"] is True

    def test_the_endgame_tightens_a_looser_catchup_cap_keeping_the_own_qos(self):
        task = _task(throttle_mbps=50, throttle_restore=[1, 2, 3, 0])
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=True) as set_lvol, \
                patch.object(constants, "REPL_CUTOVER_THROTTLE_W_MBPS", 8):
            task = final._throttle_source(task, _lvol(w=50))
        set_lvol.assert_called_once_with("LV1", 0, 0, 0, 8)
        assert task.function_params["throttle_restore"] == [1, 2, 3, 0]
        assert task.function_params["throttle_mbps"] == 8


class TestWiring:

    def test_the_catchup_wait_is_tracked(self):
        src = inspect.getsource(final.task_runner)
        i = src.index("_bound_catchup_after_failed_endgame(task, lag)")
        j = src.index("_track_catchup(task, lvol, lag, backlog")
        assert i < j < src.index("waiting for replication to catch up before the endgame")

    def test_entering_the_endgame_keeps_the_cap(self):
        src = inspect.getsource(final.task_runner)
        i = src.index('"catchup_win", "catchup_prev",')
        assert "catchup_cap_mbps" not in src[i - 200:i + 200]


def test_entering_the_endgame_rearms_the_shrink_deadline():
    src = inspect.getsource(final.task_runner)
    claim = src.index("task = _acquire_lvs_claim(task, lvol, tasks)\n        # The endgame")
    entered = src.index('xfer_timing.stamp("endgame_entered"')
    assert "shrink_deadline=" in src[claim:entered]
    assert "REPL_CUTOVER_SHRINK_TIMEOUT_SEC" in src[claim:entered]
