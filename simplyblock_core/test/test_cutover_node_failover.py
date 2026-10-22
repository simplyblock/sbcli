"""Node loss around the cross-cluster cutover (docs/online-migration.md, G2).

Ordinary snapshot replication already follows the source's online members and
the target's settled leader. The endgame did not: the freeze ran on the source
primary recorded at commit and pushed into the target primary, and with the
source primary offline the fence skipped the whole source -- leaving its peers
optimized, i.e. the volume writable on both clusters once the target lit up.
"""
import pytest

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step as rfs
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer
from simplyblock_core.test.test_replication_final_step import (
    _install_nodes,
    _Lvol,
    _Node,
)


def _ana(events):
    return [(e[1], e[2]) for e in events if e[0] == "ana"]


# ---- final step: paths follow the executing members ----------------------- #

def test_fence_covers_peers_when_the_primary_is_offline(monkeypatch):
    """The primary being gone does not make the source dark: its peers keep
    serving. Skipping the fence left them optimized while the target lit up."""
    events: list = []
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_OFFLINE,
                secondary="S2", tertiary="S3")
    sec = _Node("S2", events, "s2", "lvs_src")
    ter = _Node("S3", events, "s3", "lvs_src")
    _install_nodes(monkeypatch, {"S1": src, "S2": sec, "S3": ter})

    rfs.fence_source_paths(src, "lvs_src", _Lvol.nqn, _Lvol.ns_id, executing=sec)

    # The executing leader goes dark last; the offline primary is not asked.
    assert _ana(events) == [("S3", "inaccessible"), ("S2", "inaccessible")]


def test_restore_reoptimizes_the_executing_leader(monkeypatch):
    events: list = []
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_OFFLINE,
                secondary="S2", tertiary="S3")
    sec = _Node("S2", events, "s2", "lvs_src")
    ter = _Node("S3", events, "s3", "lvs_src")
    _install_nodes(monkeypatch, {"S1": src, "S2": sec, "S3": ter})

    rfs.restore_source_paths(src, "lvs_src", _Lvol.nqn, _Lvol.ns_id, executing=sec)

    assert _ana(events) == [("S2", "optimized"), ("S3", "non_optimized")]


def test_enable_target_optimizes_the_receiving_leader(monkeypatch):
    events: list = []
    tgt = _Node("T1", events, "t1", "lvs_tgt", status=StorageNode.STATUS_OFFLINE,
                secondary="T2", tertiary="T3")
    tsec = _Node("T2", events, "t2", "lvs_tgt")
    tter = _Node("T3", events, "t3", "lvs_tgt")
    _install_nodes(monkeypatch, {"T1": tgt, "T2": tsec, "T3": tter})

    rfs.enable_target_paths(tgt, "lvs_tgt", _Lvol.nqn, _Lvol.ns_id, receiving=tsec)

    assert _ana(events) == [("T2", "optimized"), ("T3", "non_optimized")]


def test_run_cutover_freezes_on_the_source_leader_into_the_target_leader(monkeypatch):
    events: list = []
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_OFFLINE,
                secondary="S2")
    sec = _Node("S2", events, "s2", "lvs_src")
    tgt = _Node("T1", events, "t1", "lvs_tgt", status=StorageNode.STATUS_OFFLINE,
                secondary="T2", tertiary="T3")
    tsec = _Node("T2", events, "t2", "lvs_tgt")
    tter = _Node("T3", events, "t3", "lvs_tgt")
    _install_nodes(monkeypatch, {"S1": src, "S2": sec, "T1": tgt, "T2": tsec, "T3": tter})
    # What the acting secondary needs to provide its hub on the target lvstore.
    tgt.hublvol = type("Hub", (), {"nvmf_port": 4430})()
    tsec.cluster_id = "CL2"
    tsec.expose_bdev = lambda **kw: None

    ok, err = rfs.run_cutover(src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1",
                              src_exec=sec, tgt_exec=tsec)

    assert ok is True and err is None
    # The freeze ran on the acting source leader, through the receiving
    # secondary's hub ON THE TARGET LVSTORE -- not its own transfer hub, whose
    # lvstore would resolve the map id wrongly -- never on the offline primaries.
    peer_hub = rfs.receiving_hub(tsec, tgt)
    assert peer_hub.bdev_name == "lvs_tgt/transferhub_T2"
    assert sec.rpc_client().final_step_gateways == [peer_hub.get_remote_bdev_name()]
    assert src.rpc_client().final_step_gateways == []
    # add_clone on the other ONLINE target members only.
    assert [e[1] for e in events if e[0] == "add_clone"] == ["T3"]
    assert _ana(events)[-2:] == [("T2", "optimized"), ("T3", "non_optimized")]


def test_failed_freeze_on_the_leader_restores_the_leader(monkeypatch):
    events: list = []
    src = _Node("S1", events, "s1", "lvs_src", status=StorageNode.STATUS_OFFLINE,
                secondary="S2")
    sec = _Node("S2", events, "s2", "lvs_src", final_step_state="Failed")
    tgt = _Node("T1", events, "t1", "lvs_tgt")
    _install_nodes(monkeypatch, {"S1": src, "S2": sec, "T1": tgt})

    ok, _ = rfs.run_cutover(src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1",
                            src_exec=sec)

    assert ok is False
    assert _ana(events) == [("S2", "inaccessible"), ("S2", "optimized")]


def test_defaults_keep_the_primary_choreography(monkeypatch):
    """Without explicit executing members nothing changes: primary freezes,
    primary receives."""
    events: list = []
    tsec = _Node("T2", events, "t2", "lvs_tgt")
    tgt = _Node("T1", events, "t1", "lvs_tgt", secondary="T2")
    ssec = _Node("S2", events, "s2", "lvs_src")
    src = _Node("S1", events, "s1", "lvs_src", secondary="S2")
    _install_nodes(monkeypatch, {"S1": src, "S2": ssec, "T1": tgt, "T2": tsec})

    ok, _ = rfs.run_cutover(src, tgt, _Lvol(), "lvs_tgt/LVOL_1", 42, "lvs_tgt/SNAP1")

    assert ok is True
    assert _ana(events) == [("S2", "inaccessible"), ("S1", "inaccessible"),
                            ("T1", "optimized"), ("T2", "non_optimized")]
    assert len(src.rpc_client().final_step_gateways) == 1


# ---- runner: who freezes, who receives ------------------------------------ #

def _node(uuid, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    return n


def _lvol(node_id="S1", nodes=("S1", "S2", "S3"), lvs="lvs_src"):
    lv = LVol()
    lv.uuid = "LV1"
    lv.node_id = node_id
    lv.nodes = list(nodes)
    lv.lvs_name = lvs
    return lv


class _DB:
    kv_store = "KV"

    def __init__(self, nodes, lvols=None, rep=None):
        self._nodes = nodes
        self._lvols = lvols or {}
        self._rep = rep

    def get_storage_node_by_id(self, nid):
        if nid not in self._nodes:
            raise KeyError(nid)
        return self._nodes[nid]

    def get_lvol_by_id(self, lid):
        if lid in self._lvols:
            return self._lvols[lid]
        raise KeyError(lid)

    def get_lvol_replication_by_id(self, rid):
        if self._rep is None:
            raise KeyError(rid)
        return self._rep


def _leaders(monkeypatch, leader_ids):
    from simplyblock_core.controllers import lvol_controller
    monkeypatch.setattr(lvol_controller, "is_node_leader",
                        lambda node, lvs: node.get_id() in leader_ids)


def test_source_is_the_online_leader_not_the_offline_primary(monkeypatch):
    nodes = {"S1": _node("S1", StorageNode.STATUS_OFFLINE), "S2": _node("S2"), "S3": _node("S3")}
    monkeypatch.setattr(runner, "db", _DB(nodes))
    _leaders(monkeypatch, {"S2"})

    node, why = runner._cutover_source(_lvol())

    assert node.get_id() == "S2"
    assert "S1" in why


def test_source_skips_an_online_primary_that_does_not_lead(monkeypatch):
    nodes = {"S1": _node("S1"), "S2": _node("S2"), "S3": _node("S3")}
    monkeypatch.setattr(runner, "db", _DB(nodes))
    _leaders(monkeypatch, {"S3"})

    node, _ = runner._cutover_source(_lvol())

    assert node.get_id() == "S3"


def test_no_source_leader_yields_none(monkeypatch):
    nodes = {"S1": _node("S1", StorageNode.STATUS_OFFLINE), "S2": _node("S2")}
    monkeypatch.setattr(runner, "db", _DB(nodes))
    _leaders(monkeypatch, set())

    node, why = runner._cutover_source(_lvol(nodes=("S1", "S2")))

    assert node is None and why


@pytest.fixture
def _no_db_writes(monkeypatch):
    monkeypatch.setattr(JobSchedule, "write_to_db", lambda self, kv=None: None)
    monkeypatch.setattr(LVolReplication, "write_to_db", lambda self, kv=None: None)


def _task(**params):
    t = JobSchedule()
    t.uuid = "task-1"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = JobSchedule.STATUS_NEW
    t.retry = 0
    t.max_retry = 5
    t.canceled = False
    t.function_params = {
        "lvol_id": "LV1", "src_node_id": "S1", "tgt_node_id": "T1",
        "tgt_lvol_composite": "lvs_tgt/LVOL_1", "tgt_lvol_id": "TLV",
        "tgt_map_id": 42, "tgt_snap_composite": "lvs_tgt/SNAP1",
        "operation": "replicate", "replication_id": "REP1",
        "final_state": LVolReplication.STATE_CUTOVER_DONE,
        "cutover_lvs": "lvs_src", "shrink_snap_id": "S_endgame", "shrink_round": 0,
    }
    t.function_params.update(params)
    return t


def _install_runner(monkeypatch, nodes, tgt_leader):
    rep = LVolReplication()
    rep.cutover_proceed = True
    tgt_lv = _lvol(node_id="T1", nodes=("T1", "T2"), lvs="lvs_tgt")
    tgt_lv.uuid = "TLV"
    monkeypatch.setattr(runner, "db", _DB(nodes, {"LV1": _lvol(), "TLV": tgt_lv}, rep))
    _leaders(monkeypatch, {"S2"})
    from simplyblock_core.services import snapshot_replication
    monkeypatch.setattr(
        snapshot_replication, "_stable_target_leader",
        lambda lv: (nodes[tgt_leader], "") if tgt_leader else (None, "leadership moving"))
    calls = []

    def _run_cutover(src, tgt, lvol, comp, map_id, snap, operation="replicate",
                     src_exec=None, tgt_exec=None):
        calls.append((src.get_id(), tgt.get_id(), src_exec.get_id(), tgt_exec.get_id()))
        return True, None
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    return rep, calls


def test_runner_freezes_on_the_source_leader_into_the_target_primary(monkeypatch, _no_db_writes):
    nodes = {"S1": _node("S1", StorageNode.STATUS_OFFLINE), "S2": _node("S2"), "S3": _node("S3"),
             "T1": _node("T1"), "T2": _node("T2")}
    rep, calls = _install_runner(monkeypatch, nodes, "T1")

    task = _task()
    assert runner.task_runner(task.frozen_view(), []) is None

    assert calls == [("S1", "T1", "S2", "T1")]
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE


def test_runner_receives_on_an_acting_secondary(monkeypatch, _no_db_writes):
    """With the target primary down the settled secondary leader receives,
    through its own hub on the target lvstore (receiving_hub)."""
    nodes = {"S1": _node("S1"), "S2": _node("S2"),
             "T1": _node("T1", StorageNode.STATUS_OFFLINE), "T2": _node("T2")}
    nodes["T1"].secondary_node_id = "T2"
    rep, calls = _install_runner(monkeypatch, nodes, "T2")

    assert runner.task_runner(_task().frozen_view(), []) is None
    assert calls == [("S1", "T1", "S2", "T2")]


def test_runner_never_receives_through_a_tertiary(monkeypatch, _no_db_writes):
    """SPDK never demuxes hub IO on a tertiary: the cutover waits."""
    nodes = {"S1": _node("S1"), "S2": _node("S2"),
             "T1": _node("T1", StorageNode.STATUS_OFFLINE), "T2": _node("T2"), "T3": _node("T3")}
    nodes["T1"].secondary_node_id = "T2"
    nodes["T1"].tertiary_node_id = "T3"
    _, calls = _install_runner(monkeypatch, nodes, "T3")

    with pytest.raises(TaskDefer, match="tertiary"):
        runner.task_runner(_task().frozen_view(), [])
    assert calls == []


def test_runner_defers_while_target_leadership_is_unsettled(monkeypatch, _no_db_writes):
    nodes = {"S1": _node("S1"), "S2": _node("S2"), "T1": _node("T1"), "T2": _node("T2")}
    _, calls = _install_runner(monkeypatch, nodes, None)

    with pytest.raises(TaskDefer, match="target"):
        runner.task_runner(_task().frozen_view(), [])
    assert calls == []


def test_runner_defers_without_a_source_leader(monkeypatch, _no_db_writes):
    nodes = {"S1": _node("S1", StorageNode.STATUS_OFFLINE), "S2": _node("S2"),
             "T1": _node("T1"), "T2": _node("T2")}
    _, calls = _install_runner(monkeypatch, nodes, "T1")
    _leaders(monkeypatch, set())

    with pytest.raises(TaskDefer, match="source"):
        runner.task_runner(_task().frozen_view(), [])
    assert calls == []


def test_offline_target_primary_still_defers_before_the_clone_exists(monkeypatch, _no_db_writes):
    """The clone is created through the target primary; until it exists the
    source is serving, so waiting costs the client nothing."""
    nodes = {"S1": _node("S1"), "T1": _node("T1", StorageNode.STATUS_OFFLINE), "T2": _node("T2")}
    _, calls = _install_runner(monkeypatch, nodes, "T2")
    task = _task()
    del task.function_params["tgt_lvol_composite"]

    with pytest.raises(TaskDefer, match="target node not online"):
        runner.task_runner(task.frozen_view(), [])
    assert calls == []
