"""Migration plans: bulk online migration to another cluster
(docs/online-migration.md, G1)."""
import pytest

from simplyblock_core.controllers import migration_plan_controller as mpc
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.migration_plan import MigrationPlan
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

GIB = 1024 ** 3


def _lvol(uuid, size=GIB, pool="P1", **kw):
    lv = LVol()
    lv.uuid = uuid
    lv.lvol_name = kw.pop("name", f"vol-{uuid}")
    lv.size = size
    lv.pool_uuid = pool
    lv.node_id = kw.pop("node_id", "S1")
    lv.lvs_name = kw.pop("lvs_name", "LVS_1")
    lv.nqn = kw.pop("nqn", f"nqn:{uuid}")
    lv.status = kw.pop("status", LVol.STATUS_ONLINE)
    for k, v in kw.items():
        setattr(lv, k, v)
    return lv


def _node(uuid, cluster):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster
    n.status = StorageNode.STATUS_ONLINE
    n.max_lvol = 100
    return n


class _FakeDB:
    kv_store = "KV"

    def __init__(self, lvols, nodes=(), reps=(), tasks=(), snaps=()):
        self.lvols = {lv.get_id(): lv for lv in lvols}
        self.nodes = {n.get_id(): n for n in nodes}
        self.reps = list(reps)
        self.tasks = {t.uuid: t for t in tasks}
        self.snaps = list(snaps)

    def get_cluster_by_id(self, cid):
        if cid not in ("C1", "C2"):
            raise KeyError(cid)
        return type("Cluster", (), {"get_id": lambda self: cid})()

    def get_lvols(self, cluster_id=None):
        return list(self.lvols.values())

    def get_lvol_by_id(self, lid):
        return self.lvols[lid]

    def get_storage_node_by_id(self, nid):
        return self.nodes[nid]

    def get_storage_nodes_by_cluster_id(self, cid):
        return [n for n in self.nodes.values() if n.cluster_id == cid]

    def get_lvol_replication_objects(self):
        return self.reps

    def get_lvol_stats(self, lvol, limit=1):
        return []

    def get_task_by_id(self, tid):
        return self.tasks[tid]

    def get_snapshots_by_lvol_id(self, lid):
        return [s for s in self.snaps if s.lvol.get_id() == lid]

    def get_replication_tasks_for_snapshot(self, sid):
        return []


@pytest.fixture
def db(monkeypatch):
    target_nodes = [_node("T1", "C2"), _node("T2", "C2")]
    fake = _FakeDB([], target_nodes)
    monkeypatch.setattr(mpc, "DBController", lambda: fake)
    monkeypatch.setattr(MigrationPlan, "write_to_db", lambda self, kv=None: fake.__dict__.setdefault("written", []).append(self.status))
    from simplyblock_core.controllers import lvol_controller
    monkeypatch.setattr(lvol_controller, "_sibling_replication_node", lambda lv, cl, all_lvols=None: "")
    monkeypatch.setattr(lvol_controller, "_group_replication_node_for", lambda lv, d: "")
    monkeypatch.setattr(lvol_controller, "_chain_counterpart_node", lambda d, lv, c: "")
    return fake


def _states(plan):
    return {v["lvol_id"]: (v["state"], v["error"]) for v in plan.volumes}


def test_whole_cluster_is_selected_largest_first(db):
    db.lvols = {lv.get_id(): lv for lv in [_lvol("a", GIB), _lvol("b", 5 * GIB), _lvol("c", 2 * GIB)]}
    plan = mpc.build_plan("C1", "C2", db=db)
    assert [v["lvol_id"] for v in plan.volumes] == ["b", "c", "a"]
    # Spread over both target nodes by planned load, not piled on one.
    assert [v["target_node_id"] for v in plan.volumes] == ["T1", "T2", "T2"]


def test_pool_and_list_selection(db):
    db.lvols = {lv.get_id(): lv for lv in [_lvol("a", pool="P1"), _lvol("b", pool="P2")]}
    assert [v["lvol_id"] for v in mpc.build_plan("C1", "C2", pool_id="P2", db=db).volumes] == ["b"]
    assert [v["lvol_id"] for v in mpc.build_plan("C1", "C2", lvol_ids=["a"], db=db).volumes] == ["a"]
    with pytest.raises(mpc.MigrationPlanError, match="not on cluster"):
        mpc.build_plan("C1", "C2", lvol_ids=["zzz"], db=db)


def test_skip_reasons(db):
    rep = LVolReplication()
    rep.source_lvol = _lvol("done")
    rep.source_cluster_id = "C1"
    rep.state = LVolReplication.STATE_CUTOVER_DONE
    db.reps = [rep]
    db.nodes["X1"] = _node("X1", "C3")
    db.lvols = {lv.get_id(): lv for lv in [
        _lvol("ok"),
        _lvol("done"),
        _lvol("landing", name="REP_snap1"),
        _lvol("dr", do_replicate=True, replication_mode="failover", replication_node_id="T1"),
        _lvol("other", do_replicate=True, replication_mode="migration", replication_node_id="X1"),
        _lvol("policy", replication_policy_id="pol-1"),
        _lvol("del", status=LVol.STATUS_IN_DELETION),
    ]}
    states = _states(mpc.build_plan("C1", "C2", db=db))
    assert states["ok"] == (MigrationPlan.VOL_REPLICATING, "")
    assert states["done"][1] == "already cut over"
    assert states["landing"][1] == "replication landing volume"
    assert "failover (DR) mode" in states["dr"][1]
    assert "replicates to cluster C3" in states["other"][1]
    assert "policy pol-1" in states["policy"][1]
    assert states["del"][1] == "volume is being deleted"


def test_already_migrating_volume_keeps_its_destination(db):
    db.lvols = {"m": _lvol("m", do_replicate=True, replication_mode="migration",
                            replication_node_id="T2")}
    plan = mpc.build_plan("C1", "C2", db=db)
    assert plan.volumes[0]["target_node_id"] == "T2"


def test_partial_consistency_group_is_skipped(db):
    db.lvols = {lv.get_id(): lv for lv in [_lvol("g1", group_id="G"), _lvol("g2", group_id="G"),
                                           _lvol("solo")]}
    states = _states(mpc.build_plan("C1", "C2", lvol_ids=["g1", "solo"], db=db))
    assert "consistency group G" in states["g1"][1]
    assert states["solo"][0] == MigrationPlan.VOL_REPLICATING
    whole = _states(mpc.build_plan("C1", "C2", db=db))
    assert whole["g1"][0] == whole["g2"][0] == MigrationPlan.VOL_REPLICATING


def test_nothing_movable_is_an_error(db):
    db.lvols = {"x": _lvol("x", name="REP_x")}
    with pytest.raises(mpc.MigrationPlanError, match="landing"):
        mpc.build_plan("C1", "C2", db=db)


def test_own_cluster_and_unknown_cluster_are_refused(db):
    with pytest.raises(mpc.MigrationPlanError, match="own cluster"):
        mpc.build_plan("C1", "C1", db=db)
    with pytest.raises(mpc.MigrationPlanError, match="not found"):
        mpc.build_plan("C1", "C9", db=db)


def test_dry_run_writes_nothing(db, monkeypatch):
    db.lvols = {"a": _lvol("a")}
    from simplyblock_core.controllers import lvol_controller
    monkeypatch.setattr(lvol_controller, "replication_start", lambda *a, **k: pytest.fail("started"))
    monkeypatch.setattr(lvol_controller, "replication_commit", lambda *a, **k: pytest.fail("committed"))
    mpc.build_plan("C1", "C2", db=db)
    assert "written" not in db.__dict__


def test_start_replicates_and_queues_every_commit_with_a_baseline(db, monkeypatch):
    db.lvols = {lv.get_id(): lv for lv in [_lvol("a", GIB), _lvol("b", 3 * GIB),
                                           _lvol("m", do_replicate=True, replication_mode="migration",
                                                 replication_node_id="T1", replication_interval_min=0)]}
    from simplyblock_core.controllers import lvol_controller
    started, commits = [], []
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)

    def _start(lvol_id, replication_cluster_id=None, mode=None, interval_min=None,
               placement_load=None, **k):
        started.append((lvol_id, replication_cluster_id, mode, interval_min, placement_load is not None))
        db.lvols[lvol_id].do_replicate = True
        db.lvols[lvol_id].replication_node_id = "T2"
        return True

    def _commit(lvol_id, delete_source=False, plan_id="", require_baseline=False):
        commits.append((lvol_id, delete_source, bool(plan_id), require_baseline))
        return {"cutover_task_queued": True, "task_id": f"task-{lvol_id}"}
    monkeypatch.setattr(lvol_controller, "replication_start", _start)
    monkeypatch.setattr(lvol_controller, "replication_commit", _commit)
    for lid in ("a", "b", "m"):
        t = JobSchedule()
        t.uuid = f"task-{lid}"
        t.status = JobSchedule.STATUS_NEW
        t.function_params = {}
        db.tasks[t.uuid] = t

    plan = mpc.start_plan("C1", "C2", delete_source=True)

    assert [s[0] for s in started] == ["b", "a"]            # largest first, m already replicates
    assert all(s[1:] == ("C2", "migration", 1, True) for s in started)
    assert [c[0] for c in commits] == ["b", "a", "m"]       # size, then id
    assert all(c[1:] == (True, True, True) for c in commits)
    assert db.lvols["m"].replication_interval_min == 1      # a cadence-less volume gets one
    assert {v["task_id"] for v in plan.volumes} == {"task-a", "task-b", "task-m"}


def test_an_existing_cutover_is_adopted(db, monkeypatch):
    db.lvols = {"a": _lvol("a", do_replicate=True, replication_mode="migration",
                           replication_node_id="T1", replication_interval_min=5)}
    from simplyblock_core.controllers import lvol_controller, tasks_controller
    monkeypatch.setattr(lvol_controller, "replication_commit", lambda *a, **k: (False, "dup"))
    monkeypatch.setattr(tasks_controller, "get_active_replication_final_task", lambda c, l: "old-task")
    t = JobSchedule()
    t.uuid = "old-task"
    t.status = JobSchedule.STATUS_RUNNING
    t.function_params = {}
    db.tasks["old-task"] = t

    plan = mpc.start_plan("C1", "C2")
    assert plan.volumes[0]["task_id"] == "old-task"


def _task(uuid, status=JobSchedule.STATUS_RUNNING, result="", canceled=False, **params):
    t = JobSchedule()
    t.uuid = uuid
    t.status = status
    t.function_result = result
    t.canceled = canceled
    t.function_params = params
    return t


def test_volume_states_follow_the_cutover_task():
    s = mpc._volume_state
    assert s(_task("t"))[0] == MigrationPlan.VOL_REPLICATING
    assert s(_task("t", result="queued for lvstore LVS_1 behind ab12"))[0] == MigrationPlan.VOL_QUEUED
    assert s(_task("t", cutover_lvs="LVS_1"))[0] == MigrationPlan.VOL_CONVERGING
    assert s(_task("t", tgt_lvol_composite="x"))[0] == MigrationPlan.VOL_CUTTING_OVER
    assert s(_task("t", JobSchedule.STATUS_DONE, "cutover done"))[0] == MigrationPlan.VOL_DONE
    assert s(_task("t", JobSchedule.STATUS_DONE, "max retry", last_error="boom")) == (
        MigrationPlan.VOL_FAILED, "boom")
    assert s(_task("t", canceled=True))[0] == MigrationPlan.VOL_CANCELED


def _plan_with(vols):
    plan = MigrationPlan()
    plan.uuid = "P"
    plan.cluster_id = "C1"
    plan.volumes = vols
    return plan


def test_status_aggregates_and_forecasts(db):
    snap = SnapShot()
    snap.uuid = "s1"
    snap.lvol = _lvol("a")
    snap.used_size = 4 * GIB
    snap.target_replicated_snap_uuid = ""
    db.snaps = [snap]
    db.tasks = {"ta": _task("ta"), "tb": _task("tb", JobSchedule.STATUS_DONE, "cutover done",
                                              end_time=200, endgame_started_at=170)}
    plan = _plan_with([
        {"lvol_id": "a", "lvs_name": "LVS_1", "state": "replicating", "task_id": "ta",
         "used_bytes": GIB, "error": ""},
        {"lvol_id": "b", "lvs_name": "LVS_1", "state": "replicating", "task_id": "tb",
         "used_bytes": GIB, "error": ""},
    ])
    overall = mpc._refresh(db, plan)

    a, b = plan.volumes
    assert b["state"] == MigrationPlan.VOL_DONE and b["bytes_remaining"] == 0
    # Nothing of a reached the target yet: its initial copy (the larger of
    # used bytes and its unreplicated snapshot) is all ahead.
    assert a["bytes_remaining"] == 4 * GIB
    assert overall["bytes_remaining"] == 4 * GIB
    assert overall["throughput_measured"] is False
    assert a["eta_seconds"] > 30          # transfer + the measured 30 s cutover
    assert plan.status == MigrationPlan.STATUS_RUNNING


def test_plan_outcome_once_every_volume_is_terminal(db):
    db.tasks = {"ta": _task("ta", JobSchedule.STATUS_DONE, "cutover done", end_time=1),
                "tb": _task("tb", JobSchedule.STATUS_DONE, "gave up")}
    plan = _plan_with([
        {"lvol_id": "a", "state": "replicating", "task_id": "ta", "error": ""},
        {"lvol_id": "b", "state": "replicating", "task_id": "tb", "error": ""},
        {"lvol_id": "c", "state": "skipped", "task_id": "", "error": "landing"},
    ])
    mpc._refresh(db, plan)
    assert plan.status == MigrationPlan.STATUS_PARTIAL and plan.finished_at


def test_cancel_spares_cutovers_that_prepared_their_clone(db, monkeypatch):
    monkeypatch.setattr(JobSchedule, "write_to_db", lambda self, kv=None: None)
    db.tasks = {"ta": _task("ta"), "tb": _task("tb", tgt_lvol_composite="x")}
    plan = _plan_with([
        {"lvol_id": "a", "state": "replicating", "task_id": "ta", "error": ""},
        {"lvol_id": "b", "state": "cutting_over", "task_id": "tb", "error": ""},
    ])
    monkeypatch.setattr(mpc, "get_plan", lambda pid, d=None: plan)

    mpc.cancel_plan("P")

    assert db.tasks["ta"].canceled is True
    assert not db.tasks["tb"].canceled
    assert plan.status == MigrationPlan.STATUS_RUNNING
