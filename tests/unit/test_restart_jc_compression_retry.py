"""The restart's ``jc_compression get_status`` precheck on the LVS leader.

While the leader re-levels or replicates the returning node's journal copy,
the data plane answers the status request with a JSON-RPC error (code -5,
"Compression request failed"). The precheck retries it on every cluster, not
only on two-node arbitrated ones, and gives up only after the window.
"""

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError


def _busy():
    return RPCRemoteError("Compression request failed", code=-5)


class _Leader:
    cluster_id = "CL"

    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = 0

    def get_id(self):
        return "leader"

    def rpc_client(self):
        return self

    def jc_compression_get_status(self, _vuid):
        self.calls += 1
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


class _Clock:
    def __init__(self):
        self.t = 0.0

    def sleep(self, s):
        self.t += s

    def now(self):
        return self.t


@pytest.fixture(autouse=True)
def no_arbitration_lookup(monkeypatch):
    """The retry does not depend on the cluster's two-node arbitration flag."""
    monkeypatch.setattr(storage_node_ops, "_cluster_two_node_arbitrated",
                        lambda *a, **k: pytest.fail("arbitration flag consulted"))


def test_busy_leader_on_a_regular_cluster_is_retried_until_it_answers():
    clock = _Clock()
    leader = _Leader([_busy(), False])
    got = storage_node_ops._jc_compression_status_with_retry(
        leader, 1, timeout_s=60, interval_s=2, sleep=clock.sleep, clock=clock.now)
    assert got is False
    assert leader.calls == 2 and clock.t == 2


def test_any_rpc_error_is_retried():
    clock = _Clock()
    leader = _Leader([RPCConnectionError("Could not reach remote"), True])
    got = storage_node_ops._jc_compression_status_with_retry(
        leader, 1, timeout_s=60, interval_s=2, sleep=clock.sleep, clock=clock.now)
    assert got is True and leader.calls == 2


def test_first_answer_is_returned_without_waiting():
    clock = _Clock()
    leader = _Leader([True])
    got = storage_node_ops._jc_compression_status_with_retry(
        leader, 1, sleep=clock.sleep, clock=clock.now)
    assert got is True and leader.calls == 1 and clock.t == 0


def test_error_outlasting_the_window_is_reraised():
    clock = _Clock()
    leader = _Leader([_busy()] * 100)
    with pytest.raises(RPCRemoteError) as exc:
        storage_node_ops._jc_compression_status_with_retry(
            leader, 1, timeout_s=60, interval_s=2, sleep=clock.sleep, clock=clock.now)
    assert exc.value.code == -5
    assert clock.t <= 60 and leader.calls <= 30


def test_window_is_bounded_by_rounds_when_sleep_does_not_advance_the_clock():
    clock = _Clock()
    leader = _Leader([_busy()] * 100)
    with pytest.raises(RPCRemoteError):
        storage_node_ops._jc_compression_status_with_retry(
            leader, 1, timeout_s=60, interval_s=2, sleep=lambda s: None, clock=clock.now)
    assert leader.calls == 30
