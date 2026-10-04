"""Behavioural tests for the node shutdown/restart recovery paths.

Both regressions were found on 2026-08-27 when FoundationDB filled the mgmt
root disk mid-shutdown and a storage node was left unrecoverable:

  1. `sn shutdown --force` blocked for ~9 minutes. Force is documented as
     "terminate immediately", but the kill RPC goes to the agent on the node
     being killed, and SNodeClient retried it with urllib3 backoff
     (0+2+4+8+16+32+64+120+120+120 = 486s) before giving up -- so the escape
     hatch blocked in precisely the situation it exists for.

  2. `sn restart` reported success without restarting anything. The node's
     status read OFFLINE while its SPDK was still running (the status write
     was lost while the DB was unavailable), and spdk_process_start against a
     live process is a no-op: three node_restart tasks and a --force restart
     all returned success while the container kept its original ~1h uptime.

These test the extracted policy/helper directly rather than mocking the whole
restart path, so they assert behaviour (what kwargs are used, what is called,
what happens when the agent is unreachable) instead of source shape.
"""

from simplyblock_core.storage_node_ops import (
    ensure_spdk_stopped,
    kill_client_kwargs,
)


def _urllib3_backoff_seconds(retry, backoff_factor=1, cap=120):
    """Total sleep urllib3 performs for `retry` retries (Retry.get_backoff_time)."""
    return sum(min(cap, backoff_factor * (2 ** (n - 1))) for n in range(1, retry + 1))


class _Recorder:
    """Stands in for SNodeClient: records how it was built and called."""

    def __init__(self, fail=False):
        self.fail = fail
        self.kwargs = None
        self.kill_calls = []

    def __call__(self, **kwargs):
        self.kwargs = kwargs
        return self

    def spdk_process_kill(self, rpc_port, cluster_id):
        if self.fail:
            raise ConnectionRefusedError("agent is not serving")
        self.kill_calls.append((rpc_port, cluster_id))
        return True


# --- 1. --force must not wait on a dead agent ---------------------------

def test_force_kill_does_not_retry_the_connect():
    """A refused connection must fail at once, not back off."""
    assert kill_client_kwargs(force=True)["connect_retry"] == 0


def test_force_kill_backoff_is_seconds_not_minutes():
    forced = kill_client_kwargs(force=True)
    graceful = kill_client_kwargs(force=False)
    forced_wait = _urllib3_backoff_seconds(forced["retry"])
    graceful_wait = _urllib3_backoff_seconds(graceful["retry"])
    # the observed failure: ~486s of backoff before --force gave up
    assert graceful_wait > 400
    assert forced_wait <= 10, f"--force would still block {forced_wait}s"


def test_graceful_kill_keeps_its_patience():
    """Only --force fast-fails; the graceful path is unchanged."""
    graceful = kill_client_kwargs(force=False)
    assert graceful["retry"] == 10
    assert "connect_retry" not in graceful


# --- 2. restart must bounce a live process ------------------------------

def test_restart_kills_a_live_spdk_first():
    client = _Recorder()
    assert ensure_spdk_stopped(client, 8080, "cluster-1") is True
    assert client.kill_calls == [(8080, "cluster-1")]


def test_restart_prekill_uses_the_fast_failing_policy():
    """It must not reintroduce the ~9 minute block into the restart path."""
    client = _Recorder()
    ensure_spdk_stopped(client, 8080, "cluster-1")
    assert client.kwargs["connect_retry"] == 0
    assert _urllib3_backoff_seconds(client.kwargs["retry"]) <= 10


def test_restart_prekill_is_non_fatal_when_nothing_is_running():
    """Nothing to kill is the normal case and must not fail the restart."""
    client = _Recorder(fail=True)
    assert ensure_spdk_stopped(client, 8080, "cluster-1") is False
    assert client.kill_calls == []

