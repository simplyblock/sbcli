"""Two-node event collector: backoff on a repeated RPC or store error, and its
reset on the next success (docs/design/two-node-arbitration.md, section 4;
contract 100 ms doubling to 5 s, same as Go's TestRunBacksOffDoublingToTheCap).
Pure logic, no DB: the module-level ``db`` is faked.
"""

import threading

import pytest

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import two_node_event_collector as collector


class _FakeDB:
    def __init__(self, node):
        self._node = node

    def get_storage_node_by_id(self, node_id):
        return self._node


class _AlwaysFailingNode:
    status = StorageNode.STATUS_ONLINE

    def rpc_client(self, **kwargs):
        raise RPCException("down")


class _FlakyNode:
    """Fails every call except the third, which succeeds with no new events --
    enough to let ``_collect`` reset its backoff before failing again."""

    status = StorageNode.STATUS_ONLINE

    def __init__(self):
        self.calls = 0

    def rpc_client(self, **kwargs):
        self.calls += 1
        if self.calls == 3:
            return _SteadyRPC()
        raise RPCException("down")


class _SteadyRPC:
    def jc_wait_events(self, instance, after_seq, timeout_ms=20000, max_events=256):
        return {"instance": instance, "seq": after_seq, "overflow": False, "events": []}


def test_backoff_doubles_to_the_cap_then_holds(monkeypatch):
    monkeypatch.setattr(collector, "db", _FakeDB(_AlwaysFailingNode()))
    stop = threading.Event()
    sleeps: list[float] = []

    def fake_wait(timeout):
        sleeps.append(timeout)
        if len(sleeps) == 9:
            stop.set()

    monkeypatch.setattr(stop, "wait", fake_wait)
    collector._collect("n1", "cl", stop)

    want = [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0, 5.0]
    assert sleeps == pytest.approx(want)


def test_backoff_resets_after_a_success(monkeypatch):
    node = _FlakyNode()
    monkeypatch.setattr(collector, "db", _FakeDB(node))
    stop = threading.Event()
    sleeps: list[float] = []

    def fake_wait(timeout):
        sleeps.append(timeout)
        if len(sleeps) == 4:
            stop.set()

    monkeypatch.setattr(stop, "wait", fake_wait)
    collector._collect("n1", "cl", stop)

    # calls 1, 2 fail (0.1, 0.2); call 3 succeeds and resets backoff;
    # calls 4, 5 fail again starting back at 0.1.
    assert sleeps == pytest.approx([0.1, 0.2, 0.1, 0.2])
