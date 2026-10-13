"""Control-plane fixes from the two-node HA integration campaign c1.

- B2: every ``jc_set_dual_node`` call on an arbitrated cluster carries the
  full arbitration config (a bare ``enable`` switched arbitration off on every
  restarted node); no ``preferred`` field anywhere.
- B1: after a peer returns, ``jc_explicit_synchronization`` for each LVS the
  survivor ran ``solo``, on the LVS LEADER only, never on a follower.
- B6: leadership reconciliation restores a lost LVS leadership through the
  guarded leaderless-recovery path, only for a confirmed leaderless LVS.
- The migration runners' in-memory retry backoff resets when leadership or a
  node status changes.
- Clients of an arbitrated cluster connect with keep-alive 4 s.
"""

import time

import pytest

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError
from simplyblock_core.services import migration_task_common as mig
from simplyblock_core.services import task_runner_base as trb
from simplyblock_core.utils.nvme import build_nvme_connect_entry

# ---- fakes --------------------------------------------------------------------

class _RPC:
    def __init__(self, node):
        self.node = node

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.node.calls.append((name, args, kwargs))
            handler = self.node.handlers.get(name)
            if handler is not None:
                return handler(*args, **kwargs)
            return {}
        return call


class _Node:
    def __init__(self, nid, jm_vuid, lvstore, status=StorageNode.STATUS_ONLINE):
        self.uuid, self.jm_vuid, self.lvstore, self.status = nid, jm_vuid, lvstore, status
        self.cluster_id = "CL"
        self.is_secondary_node = False
        self.health_check = True
        self.calls, self.handlers = [], {}

    def get_id(self):
        return self.uuid

    def rpc_client(self, *a, **kw):
        return _RPC(self)


class _Cluster:
    def __init__(self, arbitrated):
        self.two_node_arbitration = arbitrated
        self.client_data_nic = ""
        self.client_qpair_count = 3

    def get_id(self):
        return "CL"


class _DB:
    def __init__(self, nodes, cluster=None):
        self.nodes, self.cluster = nodes, cluster

    def get_storage_nodes_by_cluster_id(self, _cid):
        return list(self.nodes)

    def get_storage_node_by_id(self, nid):
        return next(n for n in self.nodes if n.get_id() == nid)

    def get_cluster_by_id(self, _cid):
        if self.cluster is None:
            raise KeyError("no cluster")
        return self.cluster


def _calls(node, name):
    return [c for c in node.calls if c[0] == name]


@pytest.fixture
def pair(monkeypatch):
    a, b = _Node("A", 1, "LVS_1"), _Node("B", 6, "LVS_6")
    db = _DB([a, b], _Cluster(True))
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: db)
    return a, b, db


# ---- B2: jc_set_dual_node always carries the arbitration config ------------------

def test_dual_node_on_an_arbitrated_cluster_carries_the_full_config(pair):
    a, b, _ = pair
    for n in (a, b):
        n.handlers["jc_set_dual_node"] = lambda *x, **k: True
    storage_node_ops.apply_jc_dual_node("CL")
    for n in (a, b):
        (name, args, kwargs), = _calls(n, "jc_set_dual_node")
        assert args == (True,)
        assert kwargs == {"arbitration": True, "hold_ms": constants.TWO_NODE_HOLD_MS,
                          "lease_ttl_ms": constants.TWO_NODE_LEASE_TTL_MS}
        assert "preferred" not in kwargs


def test_dual_node_without_arbitration_sends_only_enable(pair):
    a, b, db = pair
    db.cluster = _Cluster(False)
    storage_node_ops.apply_jc_dual_node("CL")
    assert _calls(a, "jc_set_dual_node")[0][2] == {}


def test_a_node_without_the_protocol_still_gets_the_plain_flag(pair):
    a, b, _ = pair

    def reject_the_arbitration_fields(enable, **k):
        # RPCClient._request raises a JSON-RPC error rather than returning it.
        if k:
            raise RPCRemoteError(code=-32603, message="decode failed")
        return True

    a.handlers["jc_set_dual_node"] = reject_the_arbitration_fields
    storage_node_ops.apply_jc_dual_node("CL")
    sent = [c[2] for c in _calls(a, "jc_set_dual_node")]
    assert sent[0]["arbitration"] is True and sent[1] == {}


def test_rpc_client_has_no_preferred_parameter():
    import inspect

    from simplyblock_core.rpc_client import RPCClient
    assert "preferred" not in inspect.signature(RPCClient.jc_set_dual_node).parameters


# ---- B1: leader-only explicit sync after the peer returns -----------------------

def _leaders(monkeypatch, mapping):
    """mapping: lvs_name -> node id that reports lvs leadership (or None)."""
    monkeypatch.setattr(lvol_controller, "is_node_leader",
                        lambda node, lvs: mapping.get(lvs) == node.get_id())


def _ha(states):
    return lambda *a, **k: {"lvs": [{"jm_vuid": v, "state": s} for v, s in states.items()]}


def test_sync_runs_on_the_leader_of_each_solo_lvs(pair, monkeypatch):
    a, b, _ = pair                                # B survived, A restarted
    b.handlers["jc_ha_status"] = _ha({1: "solo", 6: "solo"})
    # B still leads LVS_6 (its own); A took LVS_1 back in its restart
    _leaders(monkeypatch, {"LVS_6": "B", "LVS_1": "A"})
    synced = storage_node_ops.resync_solo_lvs_on_leaders(a)
    assert sorted(synced) == [("A", 1), ("B", 6)]
    assert [c[1] for c in _calls(b, "jc_explicit_synchronization")] == [(6,)]
    assert [c[1] for c in _calls(a, "jc_explicit_synchronization")] == [(1,)]


def test_sync_is_never_issued_on_a_follower(pair, monkeypatch):
    a, b, _ = pair
    b.handlers["jc_ha_status"] = _ha({1: "solo"})
    _leaders(monkeypatch, {"LVS_1": None})        # leaderless: nobody is synced
    assert storage_node_ops.resync_solo_lvs_on_leaders(a) == []
    assert not _calls(a, "jc_explicit_synchronization")
    assert not _calls(b, "jc_explicit_synchronization")


def test_sync_is_skipped_when_nothing_is_solo_and_on_bigger_clusters(pair, monkeypatch):
    a, b, db = pair
    b.handlers["jc_ha_status"] = _ha({6: "normal"})
    _leaders(monkeypatch, {"LVS_6": "B"})
    assert storage_node_ops.resync_solo_lvs_on_leaders(a) == []
    db.nodes.append(_Node("C", 9, "LVS_9"))
    b.handlers["jc_ha_status"] = _ha({6: "solo"})
    assert storage_node_ops.resync_solo_lvs_on_leaders(a) == []


def test_sync_tolerates_nodes_without_the_ha_status_rpc(pair, monkeypatch):
    a, b, _ = pair

    def boom(*a_, **k):
        raise RuntimeError("method not found")
    b.handlers["jc_ha_status"] = boom
    assert storage_node_ops.resync_solo_lvs_on_leaders(a) == []


def test_restart_paths_run_the_solo_resync():
    import inspect
    src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
    assert src.count("_resync_solo_lvs_after_restart(snode)") == 2


# ---- B6: leadership reconciliation ---------------------------------------------

def _lvstores(node, leads):
    node.handlers["get_lvstore"] = lambda name: {"name": name, "lvs leadership": name in leads}


def test_leaderless_lvs_is_restored_on_its_primary_only_when_confirmed(pair, monkeypatch):
    a, b, _ = pair
    _lvstores(a, set())                           # LVS_1 lost its leader
    _lvstores(b, {"LVS_6"})
    recovered = []
    monkeypatch.setattr(storage_node_ops, "_recover_leaderless_lvs",
                        lambda cid, nodes, lvs, taker: recovered.append((lvs, taker.get_id())) or taker)
    restored, leaderless = storage_node_ops.reconcile_lvs_leadership("CL", [a, b])
    assert (restored, leaderless, recovered) == ([], ["LVS_1"], [])
    restored, leaderless = storage_node_ops.reconcile_lvs_leadership(
        "CL", [a, b], confirmed={"LVS_1"})
    assert restored == ["LVS_1"] and recovered == [("LVS_1", "A")]


def test_no_restore_when_a_member_cannot_be_asked_or_is_not_online(pair, monkeypatch):
    a, b, _ = pair
    _lvstores(a, set())

    def boom(name):
        raise RuntimeError("unreachable")
    b.handlers["get_lvstore"] = boom
    called = []
    monkeypatch.setattr(storage_node_ops, "_recover_leaderless_lvs",
                        lambda *x: called.append(x))
    assert storage_node_ops.reconcile_lvs_leadership(
        "CL", [a, b], confirmed={"LVS_1", "LVS_6"}) == ([], [])
    _lvstores(b, set())
    b.status = StorageNode.STATUS_OFFLINE
    assert storage_node_ops.reconcile_lvs_leadership("CL", [a, b], confirmed={"LVS_1"}) == ([], [])
    assert called == []


def test_no_restore_for_lvs_the_arbiter_owns_or_an_unhealthy_primary(pair, monkeypatch):
    a, b, _ = pair
    _lvstores(a, set())
    _lvstores(b, set())
    called = []
    monkeypatch.setattr(storage_node_ops, "_recover_leaderless_lvs",
                        lambda *x: called.append(x))
    b.health_check = False
    restored, leaderless = storage_node_ops.reconcile_lvs_leadership(
        "CL", [a, b], skip_jm_vuids={1}, confirmed={"LVS_1", "LVS_6"})
    assert restored == [] and leaderless == [] and called == []


# ---- migration runners: backoff reset on leadership / node status change --------

class _Store:
    kv_store = "KV"

    def __init__(self, tasks):
        self.tasks = tasks

    def get_job_tasks(self, cid, reverse=False):
        return list(self.tasks)

    def get_clusters(self):
        return [_Cluster(True)]


def test_backoff_is_dropped_when_the_reset_key_changes(monkeypatch):
    task = JobSchedule()
    task.uuid, task.cluster_id, task.function_name = "t1", "CL", "fn"
    task.status = JobSchedule.STATUS_SUSPENDED
    keys = iter([("a",), ("a",), ("b",)])
    spec = trb.RunnerSpec(function_names=["fn"], handler=lambda t: None,
                          backoff_reset_key=lambda cl: next(keys))
    runner = trb.TaskRunner(spec)
    cluster = _Cluster(True)
    runner._next_attempt["t1"] = time.time() + 3600
    runner._maybe_reset_backoff(cluster, [task])   # first key: just remembered
    runner._maybe_reset_backoff(cluster, [task])   # unchanged
    assert "t1" in runner._next_attempt
    runner._maybe_reset_backoff(cluster, [task])   # changed
    assert "t1" not in runner._next_attempt


def test_a_failing_reset_key_is_ignored():
    def boom(cl):
        raise RuntimeError("fdb")
    runner = trb.TaskRunner(trb.RunnerSpec(function_names=["fn"], handler=lambda t: None,
                                           backoff_reset_key=boom))
    runner._next_attempt["t1"] = time.time() + 3600
    runner._maybe_reset_backoff(_Cluster(True), [])
    assert "t1" in runner._next_attempt


def test_migration_reset_key_follows_node_status_and_leadership(monkeypatch):
    class Rec:
        epoch, leadership_restored_at = 3, 0

    a, b = _Node("A", 1, "LVS_1"), _Node("B", 6, "LVS_6")
    rec = Rec()

    class DB:
        def get_storage_nodes_by_cluster_id(self, _cid):
            return [a, b]

        def get_cluster_arbitration(self, _cid):
            return rec

    monkeypatch.setattr(mig, "db", DB())
    cl = _Cluster(True)
    k0 = mig.backoff_reset_key(cl)
    rec.leadership_restored_at = 123
    k1 = mig.backoff_reset_key(cl)
    b.status = StorageNode.STATUS_OFFLINE
    k2 = mig.backoff_reset_key(cl)
    assert len({k0, k1, k2}) == 3


def test_every_migration_runner_resets_its_backoff():
    from simplyblock_core.services import (
        tasks_runner_failed_migration,
        tasks_runner_migration,
        tasks_runner_new_dev_migration,
    )
    for module in (tasks_runner_migration, tasks_runner_new_dev_migration,
                   tasks_runner_failed_migration):
        assert module.SPEC.backoff_reset_key is mig.backoff_reset_key


# ---- client keep-alive ----------------------------------------------------------

@pytest.mark.parametrize("arbitrated,expected", [
    (True, constants.TWO_NODE_CLIENT_KEEP_ALIVE_TO),
    (False, constants.LVOL_NVME_KEEP_ALIVE_TO_TCP),
])
def test_keep_alive_for_arbitrated_clusters(arbitrated, expected):
    entry = build_nvme_connect_entry(transport="tcp", ip="10.0.0.1", port=4420, nqn="nqn.x",
                                     ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
                                     cluster=_Cluster(arbitrated), host_entry=None, host_nqn=None)
    assert entry.keep_alive_tmo == expected
    assert f"--keep-alive-tmo={expected}" in entry.connect
    if arbitrated:
        assert expected == 4
        assert entry.fast_io_fail_tmo == 8 and entry.ctrl_loss_tmo == 3600
        assert entry.reconnect_delay == 2
