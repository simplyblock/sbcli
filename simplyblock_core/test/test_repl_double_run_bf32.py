"""One replication task per snapshot, run by one worker; a half-created
landing volume is deleted only when provably abandoned, and everywhere.

bed bf32 (2026-11-21, cluster 70edb423): two replication workers enqueued the
same chain ancestor (snapshot 4d20399d) 100 ms apart. Each scanned the task
table, found no task, and wrote one: 5571400c and a6f9b7b7. Both ran on the
worker pool at once -- the pool dedupes by task uuid only, and _dispatch only
yielded to a sibling that was already RUNNING. 5571400c created landing
volume 5eeb989b (REP_repl_internal_7438cbe8_1795231066); 2 s into that create
a6f9b7b7 found the record in_creation, took it for a dead attempt's leftover
and deleted it. The delete ran while the creator was still registering
LVOL_2398 on the secondary and tertiary, so those kept a registration nothing
tracked; SPDK reused the blob id and both nodes crashed 15 min later.
"""
import threading
import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr

SNAP = "4d20399d-acad-4a11-a1e1-7d8ebd9a44d8"
REP_NAME = "REP_repl_internal_7438cbe8_1795231066"


class FakeKV:
    """Dict-backed store for the transactionless path (see test_ns_slot_txn)."""

    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)

    def add(self, key, value):
        pass

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        return sorted((k, v) for k, v in self.data.items() if k.startswith(prefix))


@pytest.fixture
def dbc():
    d = DBController()
    saved = d.kv_store
    d.kv_store = FakeKV()
    yield d
    d.kv_store = saved


def _repl_task(uuid, snap=SNAP, status=JobSchedule.STATUS_NEW, date=1795237024, **params):
    t = JobSchedule()
    t.uuid = uuid
    t.cluster_id = "70edb423"
    t.node_id = "node"
    t.date = date
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.function_params = {"snapshot_id": snap, "replicate_to_source": True, **params}
    t.status = status
    t.max_retry = 8
    return t


# -- (1) one task per snapshot: the atomic claim --------------------------------

def test_bf32_second_task_of_a_snapshot_is_refused_by_the_claim(dbc):
    first, created = dbc.ensure_snapshot_replication_task(_repl_task("5571400c"))
    assert (first, created) == ("5571400c", True)
    other, created = dbc.ensure_snapshot_replication_task(_repl_task("a6f9b7b7"))
    assert (other, created) == ("5571400c", False)
    assert dbc.get_task_by_id("5571400c").uuid == "5571400c"
    with pytest.raises(KeyError):
        dbc.get_task_by_id("a6f9b7b7")


def test_bf32_a_done_task_frees_the_snapshot_claim(dbc):
    t = _repl_task("5571400c")
    dbc.ensure_snapshot_replication_task(t)
    t.status = JobSchedule.STATUS_DONE
    t.write_to_db(dbc.kv_store)
    assert dbc.ensure_snapshot_replication_task(_repl_task("a6f9b7b7")) == ("a6f9b7b7", True)


def test_bf32_two_concurrent_enqueues_of_one_ancestor_create_one_task(dbc):
    """The bf32 interleaving: both enqueuers' scans find nothing. The write
    is the claim (one FDB transaction in production; serialized here the way
    FDB's conflict retry serializes it), so only one task is created."""
    txn = threading.Lock()
    real = dbc.ensure_snapshot_replication_task

    def serialized(task):
        with txn:
            return real(task)

    stub = MagicMock()
    stub.get_job_tasks.return_value = []          # both scans: no task yet
    stub.ensure_snapshot_replication_task.side_effect = serialized
    barrier = threading.Barrier(2)
    results = []

    def enqueue():
        barrier.wait()
        results.append(tasks_controller.add_snapshot_replication_task(
            "70edb423", "node", SNAP, replicate_to_source=True))

    with patch.object(tasks_controller, "db", stub):
        threads = [threading.Thread(target=enqueue) for _ in range(2)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(5)
    created = [r for r in results if r]
    assert len(results) == 2 and len(created) == 1, results
    stub.kv_store.assert_not_called()
    assert sum(1 for k in dbc.kv_store.data if k.startswith(b"repl_snap_task/")) == 1


# -- (1) one run per snapshot: the worker pool and _dispatch ----------------------

def _wait_until(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def test_bf32_two_tasks_of_one_snapshot_never_run_at_once_on_the_pool():
    runs, release = [], threading.Event()

    def slow(task):
        runs.append(task.uuid)
        release.wait(5)
        return True

    pool = sr._Workers(4)
    with patch.object(sr, "_dispatch", side_effect=slow):
        pool.submit(_repl_task("5571400c"))
        assert _wait_until(lambda: runs == ["5571400c"])
        pool.submit(_repl_task("a6f9b7b7"))       # same snapshot, other task
        time.sleep(0.1)
        assert runs == ["5571400c"]
        release.set()
        assert _wait_until(lambda: pool.running() == 0)
        pool.submit(_repl_task("a6f9b7b7"))       # the first is done: may run now
        assert _wait_until(lambda: runs == ["5571400c", "a6f9b7b7"])


class _TaskDB:
    def __init__(self, tasks):
        self.tasks = {t.uuid: t for t in tasks}

    def get_replication_tasks_for_snapshot(self, snapshot_id):
        return [t for t in self.tasks.values()
                if t.function_params.get("snapshot_id") == snapshot_id]

    def get_task_by_id(self, uuid):
        return self.tasks[uuid]


@pytest.mark.parametrize("status", [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED])
def test_bf32_only_one_of_two_live_tasks_of_a_snapshot_is_dispatched(status):
    """Neither task was RUNNING in bf32 (the landing create comes first), so
    the old RUNNING-only yield let both through."""
    older = _repl_task("5571400c", status=status, date=100)
    newer = _repl_task("a6f9b7b7", status=status, date=101)
    ran = []
    with patch.object(sr, "db", _TaskDB([older, newer])), \
            patch.object(sr, "task_runner", side_effect=lambda t: ran.append(t.uuid) or True):
        assert sr._dispatch(newer) is True
        assert sr._dispatch(older) is True
    assert ran == ["5571400c"]


def test_bf32_the_task_owning_a_landing_volume_runs_first():
    older = _repl_task("5571400c", date=100)
    newer = _repl_task("a6f9b7b7", date=101, remote_lvol_id="5eeb989b")
    ran = []
    with patch.object(sr, "db", _TaskDB([older, newer])), \
            patch.object(sr, "task_runner", side_effect=lambda t: ran.append(t.uuid) or True):
        sr._dispatch(older)
        sr._dispatch(newer)
    assert ran == ["a6f9b7b7"]


# -- (2) the half-created landing cleanup ----------------------------------------

def _landing(age_sec, status=LVol.STATUS_IN_CREATION):
    lv = LVol()
    lv.uuid = "5eeb989b-5ca6-4ac0-9f44-4cacde14dbb3"
    lv.lvol_name = REP_NAME
    lv.status = status
    lv.node_id = "9154330e"
    lv.nodes = ["9154330e"]                 # the secondaries not recorded yet
    lv.lvs_name = "LVS_10"
    lv.lvol_bdev = "LVOL_2398"
    lv.create_dt = str(datetime.now() - timedelta(seconds=age_sec))
    return lv


def _node(nid, status=StorageNode.STATUS_ONLINE, rpc=None, **kw):
    n = SimpleNamespace(cluster_id="70edb423", status=status,
                        secondary_node_id="", tertiary_node_id="", **kw)
    n.get_id = lambda: nid
    n.rpc_client = lambda **_: rpc
    return n


class _StartDB:
    """Enough of the DB for process_snap_replicate_start to reach the
    landing-volume probe."""

    def __init__(self, landing, tasks, nodes):
        self.landing, self.tasks, self.nodes = landing, tasks, nodes
        self.deleted = False

    def get_storage_node_by_id(self, nid):
        if nid not in self.nodes:
            raise KeyError(nid)
        return self.nodes[nid]

    def get_lvol_by_name(self, name, include_deleted=False):
        if name != REP_NAME or self.deleted:
            raise KeyError(name)
        return self.landing

    def get_lvol_by_id(self, lvol_id):
        if self.deleted or lvol_id != self.landing.get_id():
            raise KeyError(lvol_id)
        return self.landing

    def get_replication_tasks_for_snapshot(self, snapshot_id):
        return [t for t in self.tasks if t.function_params.get("snapshot_id") == snapshot_id]


def _run_start(task, sdb):
    snapshot = SimpleNamespace(
        snap_name="repl_internal_7438cbe8_1795231066", source_replicated_snap_uuid="",
        target_replicated_snap_uuid="", cluster_id="70edb423", size=10 << 30,
        lvol=SimpleNamespace(node_id="SRC", get_id=lambda: "7438cbe8"),
        get_id=lambda: SNAP)
    delete = MagicMock(side_effect=lambda lv, **kw: setattr(sdb, "deleted", True))
    suspended = []
    with patch.object(sr, "db", sdb), \
            patch.object(sr, "_lvs_transfer_hold", return_value=None), \
            patch.object(sr, "_merge_behind_hold", return_value=None), \
            patch.object(sr, "_select_source_node",
                         return_value=(SimpleNamespace(get_id=lambda: "SRC", cluster_id="SRCCL"),
                                       "primary")), \
            patch.object(sr, "_unreplicated_local_ancestor", return_value=("ok", None, "")), \
            patch.object(sr, "_destination_pool_uuid", return_value="POOL"), \
            patch.object(sr, "_counterpart_on_destination", return_value=None), \
            patch.object(sr, "_create_landing_volume") as create, \
            patch.object(sr, "_suspend_for_retry",
                         side_effect=lambda t, msg, **kw: suspended.append(msg)), \
            patch.object(sr.lvol_controller, "delete_lvol", delete),             patch.object(JobSchedule, "write_to_db"), \
            patch("simplyblock_core.controllers.tasks_controller.add_lvol_sync_del_task") as queued:
        sr.process_snap_replicate_start(task, snapshot)
    create.assert_not_called()
    return delete, suspended, queued


def test_bf32_cleanup_does_not_delete_a_landing_volume_another_task_creates():
    """The exact bf32 state: a6f9b7b7 meets 5eeb989b in_creation, 2 s old,
    while 5571400c (live) creates it."""
    me = _repl_task("a6f9b7b7", status=JobSchedule.STATUS_SUSPENDED)
    creator = _repl_task("5571400c")
    sdb = _StartDB(_landing(age_sec=2), [me, creator],
                   {"node": _node("node"), "9154330e": _node("9154330e")})
    delete, suspended, _ = _run_start(me, sdb)
    delete.assert_not_called()
    assert "5571400c" in suspended[0] and "under creation" in suspended[0]


def test_bf32_cleanup_does_not_delete_a_landing_volume_this_runner_creates():
    me = _repl_task("a6f9b7b7", status=JobSchedule.STATUS_SUSPENDED)
    sdb = _StartDB(_landing(age_sec=constants.REPL_LANDING_ABANDON_SEC * 2), [me],
                   {"node": _node("node"), "9154330e": _node("9154330e")})
    with sr._LANDING_CREATING_GUARD:
        sr._LANDING_CREATING[REP_NAME] = 1
    try:
        delete, suspended, _ = _run_start(me, sdb)
    finally:
        with sr._LANDING_CREATING_GUARD:
            sr._LANDING_CREATING.pop(REP_NAME, None)
    delete.assert_not_called()
    assert "running in this runner" in suspended[0]


def test_bf32_cleanup_does_not_delete_a_young_landing_volume():
    """No live sibling and nothing here creating it -- but a create in another
    process (a runner pod being replaced) cannot be seen: age decides."""
    me = _repl_task("a6f9b7b7", status=JobSchedule.STATUS_SUSPENDED)
    sdb = _StartDB(_landing(age_sec=30), [me],
                   {"node": _node("node"), "9154330e": _node("9154330e")})
    delete, suspended, _ = _run_start(me, sdb)
    delete.assert_not_called()
    assert "old" in suspended[0]


def test_bf32_a_landing_volume_mid_create_is_registered_while_add_lvol_ha_runs():
    seen = []
    with patch.object(sr, "_create_landing_volume_unguarded",
                      side_effect=lambda *a: seen.append(dict(sr._LANDING_CREATING)) or ("id", None)):
        assert sr._create_landing_volume(REP_NAME, None, None, None) == ("id", None)
    assert seen == [{REP_NAME: 1}]
    assert REP_NAME not in sr._LANDING_CREATING


class _Rpc:
    def __init__(self, present):
        self.present = present
        self.deleted = []

    def bdev_get(self, name):
        return {"name": name} if self.present else None

    def delete_lvol(self, name, sync=False):
        self.deleted.append((name, sync))
        self.present = False
        return True


def test_bf32_cleanup_deletes_an_abandoned_landing_volume_on_the_secondaries_too():
    """Abandoned: deleted -- and confirmed gone on every lvstore member, even
    the ones the record did not list yet. The secondary that kept LVOL_2398
    registered gets a sync delete; an offline tertiary a durable task."""
    me = _repl_task("a6f9b7b7", status=JobSchedule.STATUS_SUSPENDED)
    prim_rpc, sec_rpc = _Rpc(False), _Rpc(True)
    nodes = {
        "node": _node("node"),
        "9154330e": _node("9154330e", rpc=prim_rpc),
        "8de868b9": _node("8de868b9", rpc=sec_rpc),
        "2aa1550c": _node("2aa1550c", status=StorageNode.STATUS_OFFLINE),
    }
    nodes["9154330e"].secondary_node_id = "8de868b9"
    nodes["9154330e"].tertiary_node_id = "2aa1550c"
    sdb = _StartDB(_landing(age_sec=constants.REPL_LANDING_ABANDON_SEC + 60), [me], nodes)
    delete, suspended, queued = _run_start(me, sdb)
    delete.assert_called_once()
    assert delete.call_args.kwargs == {"force_delete": True}
    assert prim_rpc.deleted == []
    assert sec_rpc.deleted == [("LVS_10/LVOL_2398", True)]
    queued.assert_called_once_with("70edb423", "2aa1550c", "LVS_10/LVOL_2398", "9154330e")
    assert "cleared stale landing volume" in suspended[0]


def test_bf32_a_record_still_in_deletion_is_left_to_the_monitor():
    lv = _landing(age_sec=9999, status=LVol.STATUS_IN_DELETION)
    rpc = _Rpc(True)
    sdb = _StartDB(lv, [], {"9154330e": _node("9154330e", rpc=rpc)})
    with patch.object(sr, "db", sdb):
        assert sr._confirm_landing_gone(lv, ["9154330e"]) == []
    assert rpc.deleted == []
