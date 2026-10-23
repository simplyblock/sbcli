"""Which cutover task a new commit of a volume repeats. One queued or running
does; one that finished the cutover does only while the relationship it cut
over is still the volume's: a fail-back back to the source removes that
relationship, and a later move of the same volume is a new cutover."""

from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVolReplication


def _task(uuid, status, result="", replication_id="", lvol_id="LV1", canceled=False):
    params = {"lvol_id": lvol_id}
    if replication_id:
        params["replication_id"] = replication_id
    return SimpleNamespace(uuid=uuid, function_name=JobSchedule.FN_REPLICATION_FINAL, status=status,
                           canceled=canceled, function_result=result, function_params=params)


class _DB:
    def __init__(self, tasks, relationships):
        self.tasks = tasks
        self.relationships = relationships
        self.function_reads = 0

    def get_job_tasks_by_function(self, cluster_id, function_name):
        assert function_name == JobSchedule.FN_REPLICATION_FINAL
        self.function_reads += 1
        return self.tasks

    def get_job_tasks(self, *args, **kwargs):
        raise AssertionError("the whole task history was read")

    def get_lvol_replication_by_id(self, uuid):
        return self.relationships[uuid]


def _rel(state):
    return SimpleNamespace(state=state)


@pytest.fixture
def db(monkeypatch):
    def install(tasks, relationships=None):
        fake = _DB(tasks, relationships or {})
        monkeypatch.setattr(tasks_controller, "db", fake)
        return fake
    return install


def test_a_running_cutover_is_repeated(db):
    fake = db([_task("t1", JobSchedule.STATUS_RUNNING)])
    assert tasks_controller.get_replication_final_task("c1", "LV1") == "t1"
    assert fake.function_reads == 1


def test_a_finished_cutover_is_repeated_while_its_relationship_stands(db):
    db([_task("t1", JobSchedule.STATUS_DONE, "cutover done", "r1")],
       {"r1": _rel(LVolReplication.STATE_CUTOVER_DONE)})
    assert tasks_controller.get_replication_final_task("c1", "LV1") == "t1"


def test_a_finished_cutover_undone_by_a_failback_is_not_repeated(db):
    """A->B, then B->A fail-back: the A->B relationship is gone, and a new
    A->B commit is a new cutover, not a repeat of the first."""
    db([_task("t1", JobSchedule.STATUS_DONE, "cutover done", "r1")], {})
    assert tasks_controller.get_replication_final_task("c1", "LV1") is False


def test_a_cutover_that_ended_without_cutting_over_is_not_repeated(db):
    db([_task("t1", JobSchedule.STATUS_DONE, "max retry reached (8/8)", "r1")],
       {"r1": _rel(LVolReplication.STATE_CUTOVER_PENDING)})
    assert tasks_controller.get_replication_final_task("c1", "LV1") is False


def test_another_volumes_cutover_is_not_repeated(db):
    db([_task("t1", JobSchedule.STATUS_RUNNING, lvol_id="LV2")])
    assert tasks_controller.get_replication_final_task("c1", "LV1") is False
