"""Cutover endgame: source write throttle and the capacity-hold exemption.

bf13 (2026-11-05): a cutover's shrink rounds settled at 6-13 s for 34 rounds
against a 2 s target (fixed cost ~5 s per round, W/T 0.4), and the capacity
hold at 80 % stopped the cadence of every volume, migrating ones included.
"""
import inspect
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.services import tasks_runner_replication_final as final


def _task(**params):
    t = JobSchedule()
    t.uuid = "F1"
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.function_params = {"lvol_id": "LV1", "shrink_round": 3, **params}
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


class TestThrottle:

    def test_the_source_is_capped_and_its_qos_remembered(self):
        task = _task()
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=True) as set_lvol, \
                patch.object(constants, "REPL_CUTOVER_THROTTLE_W_MBPS", 8):
            task = final._throttle_source(task, _lvol())
        set_lvol.assert_called_once_with("LV1", 0, 0, 0, 8)
        assert task.function_params["throttle_restore"] == [0, 0, 0, 0]
        assert task.function_params["throttle_tried"] is True

    def test_once_per_endgame(self):
        task = _task(throttle_tried=True)
        with patch("simplyblock_core.controllers.lvol_controller.set_lvol") as set_lvol:
            final._throttle_source(task, _lvol())
        set_lvol.assert_not_called()

    def test_a_refusal_is_recorded_not_retried(self):
        task = _task()
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=False):
            task = final._throttle_source(task, _lvol())
        assert "throttle_restore" not in task.function_params
        assert task.function_params["throttle_tried"] is True

    def test_a_volume_already_slower_than_the_cap_is_left_alone(self):
        task = _task()
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch("simplyblock_core.controllers.lvol_controller.set_lvol") as set_lvol, \
                patch.object(constants, "REPL_CUTOVER_THROTTLE_W_MBPS", 8):
            final._throttle_source(task, _lvol(w=5))
        set_lvol.assert_not_called()

    def test_releasing_the_claim_keeps_the_cap(self):
        # bf18: lifting it let the volume write at full speed between attempts
        task = _task(throttle_restore=[100, 200, 0, 50], throttle_mbps=8,
                     throttle_tried=True, cutover_lvs="LVS_1")
        dropped = []
        with patch("simplyblock_core.controllers.lvol_controller.set_lvol") as set_lvol,                 patch.object(final, "drop_params",
                             side_effect=lambda t, *n: dropped.extend(n)):
            final._release_lvs_claim(task)
        set_lvol.assert_not_called()
        assert "throttle_restore" not in dropped and "throttle_mbps" not in dropped
        assert "cutover_lvs" in dropped
        # the next endgame applies its (tighter) cap again
        assert "throttle_tried" in dropped

    def test_the_next_attempt_tightens_the_kept_cap(self):
        task = _task(throttle_restore=[1, 2, 3, 0], throttle_mbps=8)
        task.retry = 1
        with patch.object(final, "_record", side_effect=_record_inline),                 patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                      return_value=True) as set_lvol,                 patch.object(constants, "REPL_CUTOVER_THROTTLE_W_MBPS", 8):
            task = final._throttle_source(task, _lvol(w=8))
        set_lvol.assert_called_once_with("LV1", 0, 0, 0, 4)
        assert task.function_params["throttle_restore"] == [1, 2, 3, 0]

    def test_a_source_deleted_with_its_cutover_needs_no_restore(self):
        task = _task(throttle_restore=[0, 0, 0, 0])
        with patch("simplyblock_core.controllers.lvol_controller.set_lvol",
                   side_effect=KeyError("gone")), \
                patch.object(final, "drop_params"):
            final._restore_source_throttle(task)      # no raise

    def test_the_rounds_throttle_before_the_next_snapshot(self):
        src = inspect.getsource(final._shrink_step)
        throttle = src.index("_throttle_source(task, lvol)")
        assert src.index("if params[\"shrink_round\"] >= constants.REPL_CUTOVER_THROTTLE_AFTER_ROUND") < throttle
        assert throttle < src.index("err, task = _take_round(task, lvol)")
        # converged rounds return before it
        assert src.index("if elapsed <= constants.REPL_CUTOVER_CONVERGE_TARGET_SEC") < throttle

    def test_only_the_end_of_the_cutover_lifts_the_cap(self):
        for fn in (final._record_cutover_done, final._cleanup_unfinished_cutover):
            assert "_restore_source_throttle(task)" in inspect.getsource(fn), fn.__name__
        assert "_restore_source_throttle(task)" not in inspect.getsource(final._release_lvs_claim)
        assert "_release_lvs_claim(task)" in inspect.getsource(final._abandon_endgame)


class TestCapacityHoldExemption:

    def _lv(self, pid):
        lv = MagicMock()
        lv.replication_policy_id = pid
        return lv

    def test_only_migrating_volumes_keep_their_cadence(self):
        mig = MagicMock(mode="migration")
        mig.get_id.return_value = "P-MIG"
        fo = MagicMock(mode="failover")
        fo.get_id.return_value = "P-FO"
        lvols = [self._lv("P-MIG"), self._lv("P-FO"), self._lv("")]
        with patch.object(sm.db, "get_replication_policies", return_value=[mig, fo]):
            out = sm._exempt_from_capacity_hold(lvols, "CL")
        assert out == [lvols[0]]

    def test_the_hold_filters_instead_of_returning(self):
        src = inspect.getsource(sm.take_due_internal_snapshots)
        i = src.index("hold = _cadence_capacity_hold(cluster_id)")
        assert "_exempt_from_capacity_hold(repl_lvols, cluster_id)" in src[i:]
        assert "repl_lvols = migrating" in src[i:]
