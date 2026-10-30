"""Two-node changes stay off the shared fail-over/fail-back paths.

Regular clusters (no ``two_node_arbitration``) must run the restart, shutdown,
port-unblock and dual-node paths exactly as before the two-node work: no extra
DB reads, no extra RPCs, the same aborts. Each gate is checked here with a DB
and RPC client that record every call.
"""

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.arbitration import events as arbitration_events
from simplyblock_core.arbitration.arbiter import Arbiter
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import migration_task_common as mig


class _Cluster:
    def __init__(self, cid="CL", arbitrated=False, single=False):
        self._id = cid
        self.two_node_arbitration = arbitrated
        self.is_single_node = single
        self.distr_ndcs, self.distr_npcs = 2, 1

    def get_id(self):
        return self._id


class _RecordingDB:
    """Records every call; answers only what a test configures."""

    def __init__(self, clusters=(), nodes=()):
        self.calls = []
        self.clusters = {c.get_id(): c for c in clusters}
        self.nodes = list(nodes)

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append(name)
            if name == "get_cluster_by_id":
                return self.clusters[args[0]]
            if name == "get_clusters":
                return list(self.clusters.values())
            if name == "get_storage_nodes_by_cluster_id":
                return list(self.nodes)
            raise AssertionError("unexpected DB call %s" % name)
        return call


class _RPC:
    def __init__(self, answers=None):
        self.calls = []
        self.answers = list(answers or [])

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            if self.answers:
                a = self.answers.pop(0)
                if isinstance(a, Exception):
                    raise a
                return a
            return True
        return call


class _Node:
    def __init__(self, nid, cluster_id="CL", status=StorageNode.STATUS_ONLINE):
        self._id, self.cluster_id, self.status = nid, cluster_id, status
        self.rpc = _RPC()

    def get_id(self):
        return self._id

    def rpc_client(self, *a, **k):
        return self.rpc


@pytest.fixture
def no_db(monkeypatch):
    """DBController() must not be constructed or used."""
    db = _RecordingDB()
    monkeypatch.setattr(storage_node_ops, "DBController", lambda *a, **k: db)
    return db


# ---- restart/shutdown/kill/abort: planned-loss marker (c2, F3) ------------------

def test_planned_loss_on_a_regular_cluster_is_a_noop_without_db(no_db, monkeypatch):
    import simplyblock_core.arbitration.planned as planned
    monkeypatch.setattr(planned, "note_planned_loss",
                        lambda *a, **k: pytest.fail("planned-loss marker on a regular cluster"))
    storage_node_ops._remember_cluster_arbitration(_Cluster(arbitrated=False))
    storage_node_ops._note_planned_loss(_Node("n1"), "restart")
    assert no_db.calls == []


# ---- restart: solo-LVS resync (c1, B1) -----------------------------------------

def test_solo_resync_after_restart_is_skipped_for_a_regular_cluster(no_db, monkeypatch):
    monkeypatch.setattr(storage_node_ops, "resync_solo_lvs_on_leaders",
                        lambda *a, **k: pytest.fail("B1 resync on a regular cluster"))
    storage_node_ops._resync_solo_lvs_after_restart(_Node("n1"), _Cluster(arbitrated=False))
    assert no_db.calls == []


def test_solo_resync_after_restart_runs_on_an_arbitrated_cluster(no_db, monkeypatch):
    seen = []
    monkeypatch.setattr(storage_node_ops, "resync_solo_lvs_on_leaders", seen.append)
    node = _Node("n1")
    storage_node_ops._resync_solo_lvs_after_restart(node, _Cluster(arbitrated=True))
    assert seen == [node]


# ---- restart: the 5 peer-port unblock sites (c3 port guard) ---------------------

def test_port_guard_on_a_regular_cluster_allows_the_unblock_without_db():
    db = _RecordingDB()
    storage_node_ops._remember_cluster_arbitration(_Cluster(arbitrated=False))
    assert storage_node_ops._two_node_peer_port_must_stay_blocked(_Node("peer"), 1, db) == ""
    assert storage_node_ops._may_unblock_peer_port(_Node("peer"), 4420, 1, db, "test") is True
    assert db.calls == []


# ---- jc_set_dual_node: the pre-two-node path for every other cluster -------------

def test_dual_node_flag_on_a_regular_cluster_is_the_original_call(monkeypatch):
    nodes = [_Node("a"), _Node("b"), _Node("c")]
    db = _RecordingDB(clusters=[_Cluster(arbitrated=False)], nodes=nodes)
    monkeypatch.setattr(storage_node_ops, "DBController", lambda *a, **k: db)
    storage_node_ops.apply_jc_dual_node("CL")
    assert db.calls == ["get_storage_nodes_by_cluster_id"]
    for n in nodes:
        assert n.rpc.calls == [("jc_set_dual_node", (False,), {})]


def test_dual_node_flag_on_a_two_member_cluster_without_arbitration(monkeypatch):
    nodes = [_Node("a"), _Node("b")]
    db = _RecordingDB(clusters=[_Cluster(arbitrated=False)], nodes=nodes)
    monkeypatch.setattr(storage_node_ops, "DBController", lambda *a, **k: db)
    storage_node_ops.apply_jc_dual_node("CL")
    for n in nodes:
        assert n.rpc.calls == [("jc_set_dual_node", (True,), {})]


# ---- migration runners: backoff reset ---------------------------------------------

def test_backoff_reset_key_on_a_regular_cluster_reads_nothing(monkeypatch):
    db = _RecordingDB()
    monkeypatch.setattr(mig, "db", db)
    assert mig.backoff_reset_key(_Cluster(arbitrated=False)) is None
    assert db.calls == []


def test_backoff_reset_key_on_an_arbitrated_cluster_tracks_node_statuses(monkeypatch):
    class _Rec:
        epoch, leadership_restored_at = 3, 0
    db = _RecordingDB(nodes=[_Node("a"), _Node("b")])
    db.get_cluster_arbitration = lambda cid: _Rec()
    monkeypatch.setattr(mig, "db", db)
    statuses, leadership = mig.backoff_reset_key(_Cluster(arbitrated=True))
    assert len(statuses) == 2 and leadership == (3, 0)


# ---- distr event collector: no cluster read for the regular event stream --------

def test_only_remote_jm_events_are_legacy_jm_events():
    assert not arbitration_events.is_legacy_jm_event(
        {"storage_ID": 3, "event_type": "error_read", "status": "error_read"})
    assert not arbitration_events.is_legacy_jm_event({"vuid": 3, "status": "x"})
    status = next(iter(arbitration_events.LEGACY_JM_STATUSES))
    assert arbitration_events.is_legacy_jm_event({"status": status, "jm_vuid": 1})


def test_collector_checks_the_event_before_reading_the_cluster():
    import inspect

    from simplyblock_core.services import main_distr_event_collector as c
    src = inspect.getsource(c)
    i_check = src.index("arbitration_events.is_legacy_jm_event(event_dict)")
    i_read = src.index("cluster_of(snode), snode.get_id(), event_dict")
    assert i_check < i_read


# ---- arbiter service: idle back-off without arbitrated clusters -----------------

def test_arbiter_rescans_rarely_without_arbitrated_clusters():
    db = _RecordingDB(clusters=[_Cluster("A"), _Cluster("B")])
    now = [1_000]
    arb = Arbiter(db, clock=lambda: now[0])
    arb.tick()
    arb.renew_all()
    now[0] += 1_000
    arb.tick()
    assert db.calls == ["get_clusters"]
    now[0] += 30_000
    arb.tick()
    assert db.calls == ["get_clusters", "get_clusters"]


def test_arbiter_returns_to_the_fast_loop_once_a_cluster_is_arbitrated():
    db = _RecordingDB(clusters=[_Cluster("A")])
    now = [1_000]
    arb = Arbiter(db, clock=lambda: now[0])
    assert arb.arbitrated_clusters() == []
    db.clusters["A"].two_node_arbitration = True
    now[0] += 30_000
    assert [c.get_id() for c in arb.arbitrated_clusters()] == ["A"]
    assert [c.get_id() for c in arb.arbitrated_clusters()] == ["A"]
    assert db.calls.count("get_clusters") == 3
