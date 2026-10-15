"""Campaign c5 fix of the two-node arbiter (docs/design/two-node-arbitration.md 7.13).

07 partition / 08 both links: after the heal unfenced both nodes, both LVS
stayed leaderless (the fence demoted them, and since simplyblock/spdk#76 only
bdev_lvol_set_leader restores leadership and the primary's ANA). The arbiter
now puts each LVS's leadership back on its primary once both nodes serve it
again, and sets ANA with it.

The arbiter is gated per cluster (``two_node_arbitration``); nothing here runs
on any other cluster.
"""

import time
from types import SimpleNamespace

from simplyblock_core import constants
from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.arbitration import decision as d
from simplyblock_core.models.arbitration import ARB_STEADY, LVS_FENCED, LVS_NORMAL
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_port_allow as pa
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    calls,
    env,
)
from tests.unit.test_two_node_c2_fixes import (  # noqa: F401 - fixture
    _partition,
    c2,
    tick,
)
from tests.unit.test_two_node_c3_fixes import (
    _b_back_self_fenced_and_readmitted,
    _b_frozen_and_granted,
)
from tests.unit.test_two_node_decision import A, B


def _primaries(nodes):
    nodes[A].lvstore, nodes[A].is_secondary_node = "LVS_A", False
    nodes[B].lvstore, nodes[B].is_secondary_node = "LVS_B", False


def _wire(a, result=True):
    a.restored = []
    a.notify_log = []
    a.notify = lambda cid, node, code, msg: a.notify_log.append((node.get_id(), code))

    def restore(cid, primary, peer, lvs):
        a.restored.append((primary.get_id(), peer.get_id(), lvs))
        return result() if callable(result) else result
    a.restore_leadership = restore


def _heal_partition(a, db, nodes, clock, jms_healthy=True):
    """07/08: partition, link back, remote JMs resynced, heal unfences both."""
    _partition(a, db, nodes)
    for n in (A, B):
        nodes[n].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    clock.t += constants.TWO_NODE_STABLE_FOR_S * 1000
    tick(a, nodes)                              # HEAL: unfence both
    health = "healthy" if jms_healthy else "unhealthy"
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, health), 4: (LVS_NORMAL, health)}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    tick(a, nodes)


def test_partition_heal_restores_both_lvs_on_their_primaries(c2):  # noqa: F811
    a, db, nodes, clock, readmitted = c2
    _primaries(nodes)
    _wire(a)
    _heal_partition(a, db, nodes, clock)
    assert calls(nodes[A], "jc_unfence") and calls(nodes[B], "jc_unfence")
    assert sorted(a.restored) == [(A, B, "LVS_A"), (B, A, "LVS_B")]
    for lvs, owner in (("LVS_A", A), ("LVS_B", B)):
        entry = db.rec.leadership_restored[lvs]
        assert entry["node"] == owner and entry["ok"] is True and "heal" in entry["why"]
    assert (A, "TWO_NODE_LEADERSHIP_RESTORED") in a.notify_log
    assert (B, "TWO_NODE_LEADERSHIP_RESTORED") in a.notify_log
    assert db.rec.state == ARB_STEADY and readmitted == []


def test_restored_once_then_idle(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    _primaries(nodes)
    _wire(a)
    _heal_partition(a, db, nodes, clock)
    for _ in range(5):
        clock.t += constants.TWO_NODE_LEADERSHIP_RETRY_MS
        tick(a, nodes)
    assert len(a.restored) == 2


def test_waits_until_both_nodes_serve_the_lvs(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    _primaries(nodes)
    _wire(a)
    a.renew_all()
    nodes[A].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    nodes[B].ha = {3: (LVS_NORMAL, "unhealthy"), 4: (LVS_FENCED, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    a.request_leadership(db.rec, [nodes[A], nodes[B]], "test")
    clock.t += constants.TWO_NODE_LEADERSHIP_RETRY_MS
    tick(a, nodes)
    assert a.restored == []                     # B: remote JM of 3 unhealthy, 4 fenced
    nodes[B].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert sorted(x[2] for x in a.restored) == ["LVS_A", "LVS_B"]


def test_failed_restore_is_retried_after_the_interval(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    _primaries(nodes)
    outcomes = iter([False, False, True, True])
    _wire(a, result=lambda: next(outcomes))
    _heal_partition(a, db, nodes, clock)
    assert len(a.restored) == 2 and db.rec.leadership_restored["LVS_A"]["ok"] is False
    tick(a, nodes)                              # inside the retry interval: no new attempt
    assert len(a.restored) == 2
    clock.t += constants.TWO_NODE_LEADERSHIP_RETRY_MS
    tick(a, nodes)
    assert len(a.restored) == 4
    assert all(e["ok"] for e in db.rec.leadership_restored.values())
    clock.t += constants.TWO_NODE_LEADERSHIP_RETRY_MS
    tick(a, nodes)
    assert len(a.restored) == 4                 # done: no more attempts


def test_not_while_partitioned(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    _primaries(nodes)
    _wire(a)
    _partition(a, db, nodes)
    a.request_leadership(db.rec, [nodes[A], nodes[B]], "test")
    for _ in range(3):
        clock.t += constants.TWO_NODE_LEADERSHIP_RETRY_MS
        tick(a, nodes)
    assert a.restored == []


def test_a_new_fence_drops_pending_restorations(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    _primaries(nodes)
    _wire(a, result=False)
    _heal_partition(a, db, nodes, clock)
    assert a.lead_pending[CL]                   # failed: still pending
    _partition(a, db, nodes)                    # fenced again
    assert CL not in a.lead_pending


def test_self_fence_readmission_restores_the_primarys_leadership(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _primaries(nodes)
    _wire(a)
    a.run_async = lambda fn: fn()
    a.devices_ok = lambda _db, _node: True
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert calls(nodes[B], "jc_unfence")
    assert a.restored == []                     # A still solo: not served by both yet
    for n in (A, B):                            # resynced: A back to normal
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert "LVS_B" in [x[2] for x in a.restored]
    assert "re-admission" in db.rec.leadership_restored["LVS_B"]["why"]


def test_secondary_nodes_are_not_queued(c2):  # noqa: F811
    a, db, nodes, clock, _ = c2
    nodes[A].lvstore, nodes[A].is_secondary_node = "LVS_A", False
    nodes[B].lvstore, nodes[B].is_secondary_node = "LVS_A", True
    _wire(a)
    _heal_partition(a, db, nodes, clock)
    assert a.restored == [(A, B, "LVS_A")]


# ---- default_restore_leadership --------------------------------------------------

class _Rpc:
    def __init__(self, node, update_ok=True):
        self.node, self.update_ok = node, update_ok

    def bdev_lvol_update_lvstore(self, lvs):
        self.node.log.append(("update", lvs))
        return self.update_ok

    def bdev_lvol_set_leader(self, lvs, leader):
        self.node.log.append(("set_leader", lvs, leader))
        self.node.leads = leader


class _Node:
    def __init__(self, nid, leads=False, update_ok=True):
        self.nid, self.leads, self.update_ok, self.log = nid, leads, update_ok, []

    def get_id(self):
        return self.nid

    def rpc_client(self, **_):
        return _Rpc(self, self.update_ok)


def _task(fn, node, age_s, status="new"):
    return SimpleNamespace(function_name=fn, node_id=node, status=status, canceled=False,
                           date=int(time.time()) - age_s, uuid="t-" + fn)


def _patch_defaults(monkeypatch, primary, peer, moving=False, lvols=(), tasks=None):
    from simplyblock_core import db_controller as dbc
    from simplyblock_core import storage_node_ops as sno
    from simplyblock_core.controllers import lvol_controller as lc
    by_id = {primary.nid: primary, peer.nid: peer}
    ana = []
    if tasks is None:
        tasks = [_task(JobSchedule.FN_NODE_RESTART, primary.nid, 5)] if moving else []
    monkeypatch.setattr(dbc, "DBController", lambda: SimpleNamespace(
        get_storage_node_by_id=lambda nid: by_id[nid],
        get_lvols_by_node_id=lambda nid: list(lvols),
        get_job_tasks=lambda cid: list(tasks)))
    monkeypatch.setattr(lc, "is_node_leader", lambda node, lvs: node.leads)
    monkeypatch.setattr(sno, "_set_lvol_ana_on_node",
                        lambda lvol, node, state: ana.append((lvol.nqn, node.nid, state)))
    monkeypatch.setattr(sno, "_failback_primary_ana", lambda node: ana.append(("failback", node.nid)))
    return ana


def _lvol(nqn, lvs="LVS_A", ns=1, status="online"):
    return SimpleNamespace(nqn=nqn, lvs_name=lvs, ns_id=ns, status=status)


def test_default_reloads_then_grants_then_sets_ana(monkeypatch):
    p, s = _Node(A), _Node(B)
    ana = _patch_defaults(monkeypatch, p, s, lvols=[_lvol("nqn1"), _lvol("nqn1"),
                                                    _lvol("nqn2", lvs="LVS_OTHER")])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is True
    assert p.log == [("update", "LVS_A"), ("set_leader", "LVS_A", True)]
    assert s.log == []                          # the secondary is never granted
    assert ana == [("nqn1", A, "optimized"), ("failback", A)]


def test_default_leaves_a_survivor_led_lvs_alone(monkeypatch):
    p, s = _Node(A), _Node(B, leads=True)
    ana = _patch_defaults(monkeypatch, p, s)
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is True
    assert p.log == [] and s.log == [] and ana == []


def test_default_refuses_two_leaders(monkeypatch):
    p, s = _Node(A, leads=True), _Node(B, leads=True)
    _patch_defaults(monkeypatch, p, s)
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is False
    assert p.log == [] and s.log == []


def test_default_waits_for_a_restart_or_port_allow_task(monkeypatch):
    p, s = _Node(A), _Node(B)
    _patch_defaults(monkeypatch, p, s, moving=True)
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is False
    assert p.log == []


def test_default_never_grants_on_unreloaded_metadata(monkeypatch):
    p, s = _Node(A, update_ok=False), _Node(B)
    _patch_defaults(monkeypatch, p, s)
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is False
    assert p.log == [("update", "LVS_A")]


def test_default_already_led_only_sets_ana(monkeypatch):
    p, s = _Node(A, leads=True), _Node(B)
    ana = _patch_defaults(monkeypatch, p, s, lvols=[_lvol("nqn1")])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is True
    assert p.log == []
    assert ana == [("nqn1", A, "optimized"), ("failback", A)]


def test_default_a_fresh_port_allow_task_blocks(monkeypatch):
    p, s = _Node(A), _Node(B)
    _patch_defaults(monkeypatch, p, s, tasks=[_task(JobSchedule.FN_PORT_ALLOW, A, 10)])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is False
    assert p.log == []


def test_default_a_port_allow_task_stuck_on_2_node_jms_does_not_block_forever(monkeypatch):
    p, s = _Node(A), _Node(B)
    old = constants.TWO_NODE_LEADERSHIP_TASK_STALE_S + 30
    _patch_defaults(monkeypatch, p, s, tasks=[_task(JobSchedule.FN_PORT_ALLOW, B, old)])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is True
    assert p.log == [("update", "LVS_A"), ("set_leader", "LVS_A", True)]


def test_default_a_restart_task_always_blocks(monkeypatch):
    p, s = _Node(A), _Node(B)
    old = constants.TWO_NODE_LEADERSHIP_TASK_STALE_S + 600
    _patch_defaults(monkeypatch, p, s, tasks=[_task(JobSchedule.FN_NODE_RESTART, A, old)])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is False


def test_default_done_and_other_tasks_do_not_block(monkeypatch):
    p, s = _Node(A), _Node(B)
    _patch_defaults(monkeypatch, p, s, tasks=[
        _task(JobSchedule.FN_NODE_RESTART, A, 5, status=JobSchedule.STATUS_DONE),
        _task(JobSchedule.FN_DEV_MIG, A, 5),
        _task(JobSchedule.FN_PORT_ALLOW, "node-c", 5)])
    assert arb.default_restore_leadership(CL, p, s, "LVS_A") is True


# ---- port-allow: one remote JM on a 2-node cluster ---------------------------------

def _members(n, removed=0):
    out = [SimpleNamespace(status=StorageNode.STATUS_ONLINE) for _ in range(n)]
    out += [SimpleNamespace(status=StorageNode.STATUS_REMOVED) for _ in range(removed)]
    return out


class _CountingDB:
    def __init__(self, members):
        self.members, self.reads = members, 0

    def get_storage_nodes_by_cluster_id(self, _cid):
        self.reads += 1
        return list(self.members)


_NODE = SimpleNamespace(cluster_id=CL)


def test_port_allow_one_remote_jm_is_complete_on_a_two_node_cluster():
    db = _CountingDB(_members(2, removed=1))
    assert pa._single_peer_jm_is_complete(db, _NODE, ["jm-peer"]) is True


def test_port_allow_one_remote_jm_still_defers_on_three_nodes():
    db = _CountingDB(_members(3))
    assert pa._single_peer_jm_is_complete(db, _NODE, ["jm-peer"]) is False


def test_port_allow_two_remote_jms_never_read_the_members():
    db = _CountingDB(_members(3))
    assert pa._single_peer_jm_is_complete(db, _NODE, ["a", "b"]) is False
    assert pa._single_peer_jm_is_complete(db, _NODE, []) is False
    assert db.reads == 0


def test_port_allow_member_read_failure_defers():
    class _Broken:
        def get_storage_nodes_by_cluster_id(self, _cid):
            raise RuntimeError("fdb")
    assert pa._single_peer_jm_is_complete(_Broken(), _NODE, ["jm-peer"]) is False


def test_partition_verdict_kind_is_unchanged():
    # the fix is in the arbiter's heal, not in the decision engine
    assert d.V_PARTITION == "partition"


# ---- storage node monitor: a stale-port unblock must not lift an HA fence ------------

def _monitor_run(monkeypatch, arbitrated, ha_state):
    from unittest.mock import MagicMock, patch

    from simplyblock_core import storage_node_ops as sno
    from simplyblock_core.services import storage_node_monitor as mon
    from simplyblock_core.utils import port_block
    node = MagicMock()
    node.get_id.return_value = "n1"
    node.cluster_id = "cl-mon-%s" % arbitrated
    node.status = StorageNode.STATUS_ONLINE
    node.lvstore, node.jm_vuid = "LVS_6", 6
    rpc = MagicMock()
    rpc.jc_ha_status.return_value = {"lvs": [{"jm_vuid": 6, "state": ha_state}]}
    node.rpc_client.return_value = rpc
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    db.get_cluster_by_id.return_value = SimpleNamespace(
        two_node_arbitration=arbitrated, get_id=lambda: node.cluster_id)
    db.get_cluster_arbitration.return_value = None
    sno._ARBITRATION_FLAG.pop(node.cluster_id, None)
    unblocked = []
    mon._blocked_port_since.clear()
    hc = mon.health_controller
    with patch.object(hc, "_restart_owns_lvs", return_value=False), \
         patch.object(hc, "_check_node_hublvol", return_value=True), \
         patch.object(port_block, "set_port", side_effect=lambda n, p, **kw: unblocked.append(p)), \
         patch.object(mon.time, "monotonic", side_effect=[0.0, 100.0]):
        mon._remediate_stale_port_blocks(db, node, {4426: False}, {4426: "n1"})
    return unblocked, rpc


def test_monitor_leaves_an_ha_fenced_port_on_an_arbitrated_cluster(monkeypatch):
    unblocked, _ = _monitor_run(monkeypatch, True, "fenced")
    assert unblocked == []


def test_monitor_still_lifts_a_stale_block_of_a_normal_lvs_on_an_arbitrated_cluster(monkeypatch):
    unblocked, _ = _monitor_run(monkeypatch, True, "normal")
    assert unblocked == [4426]


def test_monitor_unchanged_on_a_regular_cluster(monkeypatch):
    unblocked, rpc = _monitor_run(monkeypatch, False, "fenced")
    assert unblocked == [4426]
    rpc.jc_ha_status.assert_not_called()
