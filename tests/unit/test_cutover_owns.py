"""The long inline transfer wait is only for a cutover in its endgame.

_await_transfer_completion polls a submitted transfer inline for 300 s instead
of 5 s when the volume "owns a cutover", because near the freeze every second
of delay is freeze time -- and then the volume holds its lvstore, so waiting
starves nothing. A migration plan queues every volume's cutover up front
(require_baseline): such a cutover holds nothing yet, and treating it as an
owner made each initial copy block the single-threaded runner for 300 s while
the other volumes' transfers were not even submitted (EKS bed 2026-10-22).
"""
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr


def _final(lvol_id, **params):
    t = JobSchedule()
    t.uuid = f"t-{lvol_id}"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = JobSchedule.STATUS_SUSPENDED
    t.canceled = False
    t.function_params = {"lvol_id": lvol_id, **params}
    return t


def _db(tasks, monkeypatch):
    monkeypatch.setattr(sr, "db", type("DB", (), {"get_job_tasks": staticmethod(lambda cid: tasks)})())


def test_a_queued_cutover_waiting_for_its_initial_copy_does_not_own(monkeypatch):
    _db([_final("LV1", require_baseline=True)], monkeypatch)
    assert sr._cutover_owns("LV1", "C1") is False


def test_a_cutover_holding_its_lvstore_owns(monkeypatch):
    _db([_final("LV1", cutover_lvs="LVS_1", shrink_snap_id="S1")], monkeypatch)
    assert sr._cutover_owns("LV1", "C1") is True


def test_other_volumes_and_finished_cutovers_do_not_own(monkeypatch):
    done = _final("LV1", cutover_lvs="LVS_1")
    done.status = JobSchedule.STATUS_DONE
    _db([done, _final("LV2", cutover_lvs="LVS_1")], monkeypatch)
    assert sr._cutover_owns("LV1", "C1") is False
