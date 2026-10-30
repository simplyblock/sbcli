"""Tests for the restart's ``jc_compression get_status`` check on the LVS leader.

The check retries codes -4, -5 and -7 on every cluster. It also retries -6
(journal stopped on error) on two-node clusters with arbitration enabled.
Every other error is re-raised without another call or wait.

Tests cover successful responses, errors on the first call and after a busy
answer, arbitration lookups, call and wait counts, and a zero retry window.
"""

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError


def _err(code):
    return RPCRemoteError("Compression request failed", code=code)


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


@pytest.fixture
def arbitration(monkeypatch):
    """The cluster's two-node arbitration flag; records every lookup."""
    state = {"flag": False, "lookups": 0}

    def lookup(*_a, **_k):
        state["lookups"] += 1
        return state["flag"]

    monkeypatch.setattr(storage_node_ops, "_cluster_two_node_arbitrated", lookup)
    return state


def _check(leader, clock, **kw):
    kw.setdefault("timeout_s", 60)
    return storage_node_ops._jc_compression_status_with_retry(
        leader, 1, interval_s=2, sleep=clock.sleep, clock=clock.now, **kw)


@pytest.mark.parametrize("code", [-5, -4, -7])
def test_busy_leader_is_retried_until_it_answers(arbitration, code):
    clock = _Clock()
    leader = _Leader([_err(code), False])
    assert _check(leader, clock) is False
    assert leader.calls == 2 and clock.t == 2
    assert arbitration["lookups"] == 0


@pytest.mark.parametrize("error", [
    RPCRemoteError("Method not found", code=-32601),
    RPCRemoteError("No such device", code=-19),
    _err(-8),
    RPCConnectionError("Could not reach remote"),
], ids=["method-not-found", "enodev", "untracked-vuid", "connection-error"])
def test_permanent_error_is_reraised_at_once(arbitration, error):
    clock = _Clock()
    leader = _Leader([error, False])
    with pytest.raises(type(error)):
        _check(leader, clock)
    assert leader.calls == 1 and clock.t == 0
    assert arbitration["lookups"] == 0


@pytest.mark.parametrize("error", [
    RPCRemoteError("Method not found", code=-32601),
    _err(-11),
    _err(-6),
    RPCConnectionError("Could not reach remote"),
], ids=["method-not-found", "journal-closed", "jcerr-regular-cluster", "connection-error"])
def test_permanent_error_after_a_busy_answer_is_reraised_at_once(arbitration, error):
    clock = _Clock()
    leader = _Leader([_err(-5), error, False])
    with pytest.raises(type(error)) as exc:
        _check(leader, clock)
    assert exc.value is error
    assert leader.calls == 2 and clock.t == 2


def test_jcerr_on_a_regular_cluster_is_reraised_at_once(arbitration):
    clock = _Clock()
    leader = _Leader([_err(-6), False])
    with pytest.raises(RPCRemoteError):
        _check(leader, clock)
    assert leader.calls == 1 and clock.t == 0
    assert arbitration["lookups"] == 1


def test_jcerr_on_a_two_node_arbitrated_cluster_is_retried(arbitration):
    arbitration["flag"] = True
    clock = _Clock()
    leader = _Leader([_err(-6), _err(-6), False])
    assert _check(leader, clock) is False
    assert leader.calls == 3 and clock.t == 4
    assert arbitration["lookups"] == 1


def test_first_answer_is_returned_without_waiting(arbitration):
    clock = _Clock()
    leader = _Leader([True])
    assert _check(leader, clock) is True
    assert leader.calls == 1 and clock.t == 0


def test_busy_answer_outlasting_the_window_is_reraised(arbitration):
    clock = _Clock()
    leader = _Leader([_err(-5)] * 100)
    with pytest.raises(RPCRemoteError) as exc:
        _check(leader, clock)
    assert exc.value.code == -5
    assert leader.calls == 30 and clock.t == 58


def test_window_is_bounded_by_rounds_when_sleep_does_not_advance_the_clock(arbitration):
    clock = _Clock()
    leader = _Leader([_err(-5)] * 100)
    with pytest.raises(RPCRemoteError):
        storage_node_ops._jc_compression_status_with_retry(
            leader, 1, timeout_s=60, interval_s=2, sleep=lambda s: None, clock=clock.now)
    assert leader.calls == 30


def test_zero_window_means_no_retry(arbitration):
    clock = _Clock()
    leader = _Leader([_err(-5), False])
    with pytest.raises(RPCRemoteError):
        _check(leader, clock, timeout_s=0)
    assert leader.calls == 1 and clock.t == 0
