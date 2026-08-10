"""Which snapshot a cross-cluster fail-over is allowed to clone from.

Lab run 2026-08-10: all 5 failed-over volumes read as ALL ZEROS (no filesystem,
md5 mismatch). Every `cloned_from` parent was missing from the snapshot table
while dozens of replicated snapshots sat in `in_deletion`, and 5 replication
tasks were still `running`. The selector accepted any snapshot whose
`target_replicated_snap_uuid` was merely SET, so an unfinished or
being-deleted target copy could become the fail-over point.
"""
from simplyblock_core.controllers.lvol_controller import _last_replicated_target_snapshot
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot

LVOL_ID = "LV1"
CLUSTER_ID = "CL_src"


class _Lvol:
    def __init__(self, uuid=LVOL_ID):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


def _snap(uuid, created_at, target_uuid):
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.target_replicated_snap_uuid = target_uuid
    s.lvol = _Lvol()
    return s


def _target(uuid, status=SnapShot.STATUS_ONLINE):
    s = SnapShot()
    s.uuid = uuid
    s.status = status
    return s


def _task(snapshot_id, status=JobSchedule.STATUS_DONE):
    t = JobSchedule()
    t.uuid = f"task_{snapshot_id}"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.status = status
    t.function_params = {"snapshot_id": snapshot_id}
    return t


class _FakeDB:
    def __init__(self, tasks, snaps):
        self._tasks = tasks
        self._snaps = {s.get_id(): s for s in snaps}

    def get_job_tasks(self, cluster_id):
        return self._tasks

    def get_snapshot_by_id(self, sid):
        if sid not in self._snaps:
            raise KeyError(f"Snapshot {sid} not found")
        return self._snaps[sid]


def _pick(tasks, snaps):
    return _last_replicated_target_snapshot(_FakeDB(tasks, snaps), LVOL_ID, CLUSTER_ID)


def test_picks_newest_completed():
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_new")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old"), _target("T_new")])
    assert got.get_id() == "T_new"


def test_skips_snapshot_whose_task_is_not_done():
    """The newest snapshot's transfer is still running -> use the older one."""
    old = _snap("S_old", 100, "T_old")
    running = _snap("S_running", 200, "T_running")
    got = _pick([_task("S_old"), _task("S_running", JobSchedule.STATUS_RUNNING)],
                [old, running, _target("T_old"), _target("T_running")])
    assert got.get_id() == "T_old", "an in-flight transfer is not a valid fail-over point"


def test_falls_back_when_newest_target_copy_was_deleted():
    """Retention removed the newest target copy -> fall back, do not orphan."""
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_gone")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old")])          # T_gone absent
    assert got.get_id() == "T_old"


def test_falls_back_when_newest_target_copy_is_in_deletion():
    old = _snap("S_old", 100, "T_old")
    new = _snap("S_new", 200, "T_dying")
    got = _pick([_task("S_old"), _task("S_new")],
                [old, new, _target("T_old"),
                 _target("T_dying", SnapShot.STATUS_IN_DELETION)])
    assert got.get_id() == "T_old"


def test_none_when_no_completed_replication():
    running = _snap("S_running", 200, "T_running")
    got = _pick([_task("S_running", JobSchedule.STATUS_RUNNING)],
                [running, _target("T_running")])
    assert got is None, "better to refuse fail-over than to hand back empty data"


def test_none_when_all_target_copies_gone():
    old = _snap("S_old", 100, "T_old")
    got = _pick([_task("S_old")], [old])
    assert got is None


def test_ignores_other_lvols_and_other_task_types():
    mine = _snap("S_mine", 100, "T_mine")
    theirs = _snap("S_theirs", 300, "T_theirs")
    theirs.lvol = _Lvol("OTHER_LVOL")
    unrelated = _task("S_mine")
    unrelated.function_name = "some_other_function"
    got = _pick([_task("S_mine"), _task("S_theirs"), unrelated],
                [mine, theirs, _target("T_mine"), _target("T_theirs")])
    assert got.get_id() == "T_mine"
