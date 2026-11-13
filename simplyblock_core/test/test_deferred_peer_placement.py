"""A volume whose replication destination is a peer control plane that cannot be
reached keeps its replication: only the placement on the peer waits, and the
volume's first replication task makes it once the peer answers.

Bed run 20261113-1148-ebs-splitdr (control-plane split, M3): the fail-over on
site A attached Gitea's group to site A's policy toward site B while site B was
still cut off. replication_start asked site B to place each member, timed out,
and the attach was rolled back. Nothing retried it after the heal, so Gitea was
never protected again, and the fail-over call blocked for 124 s on the peer.
"""
from types import SimpleNamespace

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.peer.client import PeerRefusedError, PeerUnreachableError
from simplyblock_core.services import snapshot_replication as sr

PEER_CLUSTER = "CL_B"


# ------------------------------------------------------------------ replication_start

class _Db:
    def __init__(self, lvol):
        self.lvol = lvol

    def get_lvol_by_id(self, _id):
        return self.lvol

    def get_storage_node_by_id(self, _id):
        return SimpleNamespace(cluster_id="CL_A", get_id=lambda: "N_A")

    def get_cluster_by_id(self, _id):
        return SimpleNamespace(get_id=lambda: "CL_A")

    def get_snapshots(self):
        return []


def _starting(monkeypatch, place):
    lvol = LVol()
    lvol.uuid, lvol.node_id = "LV1", "N_A"
    lvol.replication_policy_id = "CL_A/P"
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _Db(lvol))
    monkeypatch.setattr(lvol_controller, "replication_start_error", lambda *a, **kw: None)
    monkeypatch.setattr(lvol_controller, "_destination_peer", lambda dbc, lv, cluster: object())
    monkeypatch.setattr(lvol_controller, "_sibling_replication_node", lambda lv, cl: "")
    monkeypatch.setattr(lvol_controller, "_group_replication_node_for", lambda lv, dbc: "")
    monkeypatch.setattr(lvol_controller, "_place_on_peer", place)
    return lvol


def _unreachable(*_a):
    raise PeerUnreachableError("POST http://10.70.3.21:15000/api/v2/_peer/clusters/CL_B/place: timed out")


def test_an_unreachable_peer_defers_the_placement(monkeypatch):
    lvol = _starting(monkeypatch, _unreachable)

    assert lvol_controller.replication_start("LV1", replication_cluster_id=PEER_CLUSTER,
                                             from_policy=True) is True
    assert lvol.do_replicate is True
    assert lvol.replication_node_id == "", "no node until the peer places the volume"


def test_a_peer_that_refuses_the_placement_still_fails_the_start(monkeypatch):
    def _refused(*_a):
        raise PeerRefusedError("no member of CL_B can take 1 GiB")

    _starting(monkeypatch, _refused)

    assert lvol_controller.replication_start("LV1", replication_cluster_id=PEER_CLUSTER,
                                             from_policy=True) is False


def test_a_deferred_start_does_not_call_the_peer(monkeypatch):
    calls = []

    def _place(*a):
        calls.append(a)
        return "N_B"

    lvol = _starting(monkeypatch, _place)

    assert lvol_controller.replication_start("LV1", replication_cluster_id=PEER_CLUSTER,
                                             from_policy=True, defer_peer_placement=True) is True
    assert calls == []
    assert (lvol.do_replicate, lvol.replication_node_id) == (True, "")


# ------------------------------------------------------------------ the reverse attach

def test_the_reverse_attach_on_a_fail_over_defers_the_placement(monkeypatch):
    """The promote does not wait on a peer that is likely still cut off."""
    group = SimpleNamespace(get_id=lambda: "CL_A/GA", cluster_id="CL_A", group_name="gitea",
                            policy_id="", uuid="GA")
    pol = SimpleNamespace(get_id=lambda: "CL_A/P", policy_name="to-site-b", target_id="T",
                          mode="failover", interval_min=5)
    monkeypatch.setattr(cgc.db, "get_consistency_group_by_id", lambda _id: group)
    monkeypatch.setattr(cgc.db, "get_replication_policy_by_id", lambda _id: pol)
    monkeypatch.setattr(cgc.db, "get_replication_target_by_id", lambda _id: SimpleNamespace(
        target_cluster_id=PEER_CLUSTER, target_name="site-b"))
    monkeypatch.setattr(cgc, "reverse_replication_policy", lambda *a: pol)
    monkeypatch.setattr(cgc, "_update_group", lambda g, fn: g)
    monkeypatch.setattr(cgc, "list_members", lambda g: [{"lvol_id": "M1"}, {"lvol_id": "M2"}])
    member = LVol()
    monkeypatch.setattr(rpc.db, "get_lvol_by_id", lambda _id: member)
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    starts = []

    def _start(lvol_id, **kw):
        starts.append((lvol_id, kw.get("defer_peer_placement")))
        return True

    monkeypatch.setattr(rpc.lvol_controller, "replication_start", _start)

    assert cgc.attach_reverse_replication(group, PEER_CLUSTER) is pol
    assert starts == [("M1", True), ("M2", True)]


# ------------------------------------------------------------------ the replication task

class _Task:
    def __init__(self):
        self.uuid = "TASK1"
        self.function_params = {"replicate_to_source": False, "snapshot_id": "S1"}
        self.node_id = "N_A"
        self.status = JobSchedule.STATUS_NEW
        self.function_result = ""
        self.retry = 0

    def write_to_db(self, kv=None):
        pass


def _task_for_unplaced_volume(monkeypatch):
    lvol = LVol()
    lvol.uuid, lvol.node_id, lvol.do_replicate = "LV1", "N_A", True
    lvol.replication_policy_id = "CL_A/P"
    snap = SnapShot()
    snap.uuid, snap.lvol, snap.snap_name, snap.size, snap.cluster_id = "S1", lvol, "snap1", 1 << 30, "CL_A"
    monkeypatch.setattr(sr, "_select_source_node",
                        lambda s, exclude=(): (SimpleNamespace(get_id=lambda: "N_A", cluster_id="CL_A"), "primary"))
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor", lambda snode, snapshot, to_source: ("ok", None, ""))
    monkeypatch.setattr(sr, "_descendant_destination_lvol", lambda snapshot: None)
    return _Task(), snap


def test_the_task_of_an_unplaced_volume_waits_for_an_unreachable_peer(monkeypatch):
    task, snap = _task_for_unplaced_volume(monkeypatch)
    monkeypatch.setattr(sr.lvol_controller, "place_on_peer_destination", _unreachable)

    sr.process_snap_replicate_start(task, snap)

    assert task.status == JobSchedule.STATUS_SUSPENDED, "the task waits for the peer, it is not dropped"
    assert task.retry == 0, "a cut-off peer is a wait, not a fault of the transfer"
    assert task.function_params.get("not_before"), "the next attempt backs off"


def test_the_task_of_an_unplaced_volume_places_it_and_replicates(monkeypatch):
    task, snap = _task_for_unplaced_volume(monkeypatch)

    def _place(lvol):
        lvol.replication_node_id = "N_B"
        return lvol

    monkeypatch.setattr(sr.lvol_controller, "place_on_peer_destination", _place)
    monkeypatch.setattr(sr, "_destination_cluster_of", lambda lv: PEER_CLUSTER)
    monkeypatch.setattr(sr, "_pool_hints", lambda lv, src, dst: [])
    landing = {}

    class _Peer:
        def prepare_landing(self, request):
            landing["node_hint"] = request.node_hint
            raise PeerRefusedError("stop")   # past the destination is all this test needs

    monkeypatch.setattr(sr, "_peer_for", lambda lv, cluster_id: _Peer())

    sr.process_snap_replicate_start(task, snap)

    assert landing == {"node_hint": "N_B"}
