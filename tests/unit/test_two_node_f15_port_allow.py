"""Campaign c6, F15: port_allow must not lift a fenced LVS's client ports.

With the F14 fix the port-allow task completed on a two-node cluster during
fence_both and unblocked the client ports of both nodes 25-35 s before the
arbiter's unfence. On an arbitrated cluster the task now defers while one of
the node's LVS is HA-fenced or holding (or the HA state cannot be read), right
after reconnecting the remote JMs. A node with two or more remote JMs (a
cluster of three or more) never reaches the check.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core import storage_node_ops as sno
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_port_allow as pa
from simplyblock_core.services.task_runner_base import TaskDefer
from tests._mocks import route_fences


class _RPC:
    def __init__(self, states=None, error=None):
        self.states, self.error, self.calls = states or [], error, 0

    def jc_ha_status(self):
        self.calls += 1
        if self.error:
            raise self.error
        return {"lvs": [{"jm_vuid": v, "state": s} for v, s in self.states]}


def _node(rpc):
    return SimpleNamespace(cluster_id="CL", rpc_client=lambda **kw: rpc)


@pytest.fixture
def arbitrated(monkeypatch):
    def set_flag(flag):
        monkeypatch.setattr(sno, "_cluster_two_node_arbitrated",
                            lambda cluster_id, cluster=None, db=None: flag)
    return set_flag


@pytest.mark.parametrize("state", ["fenced", "holding"])
def test_fenced_or_holding_keeps_the_ports_blocked(arbitrated, state):
    arbitrated(True)
    rpc = _RPC([(1, "normal"), (6, state)])
    assert "LVS 6 is %s" % state in pa._two_node_fence_holds_ports(_node(rpc))


def test_unfenced_lets_the_task_unblock(arbitrated):
    arbitrated(True)
    rpc = _RPC([(1, "normal"), (6, "solo")])
    assert pa._two_node_fence_holds_ports(_node(rpc)) == ""


def test_unknown_ha_state_keeps_the_ports_blocked(arbitrated):
    arbitrated(True)
    rpc = _RPC(error=RuntimeError("timeout"))
    assert "HA state unknown" in pa._two_node_fence_holds_ports(_node(rpc))


def test_regular_cluster_makes_no_status_call(arbitrated):
    arbitrated(False)
    rpc = _RPC([(6, "fenced")])
    assert pa._two_node_fence_holds_ports(_node(rpc)) == ""
    assert rpc.calls == 0


# ---- wiring in exec_port_allow_task ---------------------------------------------

class _DB:
    def __init__(self, node):
        self.node, self.writes = node, 0

    def get_storage_node_by_id(self, _nid):
        return self.node


def _run(monkeypatch, remote_jms, keep):
    node = SimpleNamespace(
        status=StorageNode.STATUS_ONLINE, mgmt_ip="10.0.0.1", data_nics=[],
        cluster_id="CL", remote_devices=[], remote_jm_devices=[],
        write_to_db=lambda: None, rpc_client=lambda **_: MagicMock())
    monkeypatch.setattr(pa, "db", _DB(node))
    monkeypatch.setattr(pa.health_controller, "_check_node_ping", lambda ip: True)
    monkeypatch.setattr(sno, "_connect_to_remote_devs", lambda n, reattach=False: ["dev"])
    monkeypatch.setattr(sno, "_connect_to_remote_jm_devs", lambda n: list(remote_jms))
    monkeypatch.setattr(pa, "_single_peer_jm_is_complete", lambda db, n, jms: True)
    asked = []

    def holds(n):
        asked.append(n)
        return keep
    monkeypatch.setattr(pa, "_two_node_fence_holds_ports", holds)

    class _Past(Exception):
        """Raised by the first step after the new gate."""

    def past(*a, **k):
        raise _Past()
    monkeypatch.setattr(pa.distr_controller, "send_cluster_map_to_node", past, raising=False)
    monkeypatch.setattr(pa.distr_controller, "send_dev_status_event", past, raising=False)
    monkeypatch.setattr(pa.device_controller, "device_set_online", past, raising=False)
    task = SimpleNamespace(node_id="N1", cluster_id="CL", function_params={"port_number": 4426})
    return task, asked, _Past


def test_a_fenced_two_node_task_defers_before_any_unblock(monkeypatch):
    task, asked, _ = _run(monkeypatch, ["jm-peer"], "LVS 6 is fenced on the node")
    unblocked = []
    route_fences([pa.db.node], lambda *a, **k: unblocked.append((a, k)))
    with pytest.raises(TaskDefer, match="Client ports stay blocked: LVS 6 is fenced"):
        pa.exec_port_allow_task(task)
    assert len(asked) == 1 and unblocked == []


def test_two_remote_jms_never_reach_the_gate(monkeypatch):
    task, asked, past = _run(monkeypatch, ["jm-a", "jm-b"], "LVS 6 is fenced on the node")
    try:
        pa.exec_port_allow_task(task)
    except (TaskDefer, past, Exception):
        pass                                     # later steps are not under test
    assert asked == []


def test_an_unfenced_two_node_task_passes_the_gate(monkeypatch):
    task, asked, past = _run(monkeypatch, ["jm-peer"], "")
    try:
        pa.exec_port_allow_task(task)
    except TaskDefer as e:
        assert "Client ports stay blocked" not in str(e)
    except Exception:
        pass                                     # later steps are not under test
    assert len(asked) == 1
