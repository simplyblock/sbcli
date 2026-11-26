"""bf36 R3b (2026-11-26): a node shut down by the test never came back.

Each restart started the node's SPDK pod, slept 5 s and called its first RPC.
On Kubernetes under TLS that RPC goes to <worker>.<spdk-proxy service>, a name
the operator publishes only once the new pod serves an address; it did not
resolve yet ("Name or service not known"), the restart failed and deleted the
pod again, and the name never appeared: 10 attempts in 75 minutes. The restart
now waits for the SPDK RPC endpoint to answer before configuring it.
"""
import inspect
from types import SimpleNamespace
from unittest.mock import patch

from simplyblock_core import storage_node_ops
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError


class _Client:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def get_version(self):
        self.calls += 1
        out = self.outcomes.pop(0) if self.outcomes else {"version": "x"}
        if isinstance(out, Exception):
            raise out
        return out


def _node(client):
    return SimpleNamespace(rpc_client=lambda **kw: client, get_id=lambda: "N1")


def test_it_waits_while_the_name_does_not_resolve_yet():
    client = _Client([RPCConnectionError("Could not reach remote")] * 3 + [{"version": "x"}])
    with patch.object(storage_node_ops.time, "sleep"):
        ok, err = storage_node_ops._wait_spdk_rpc_ready(_node(client), timeout=60, poll=0)
    assert ok and err is None
    assert client.calls == 4


def test_any_answer_means_reachable():
    client = _Client([RPCRemoteError(code=-32601, message="Method not found")])
    ok, _ = storage_node_ops._wait_spdk_rpc_ready(_node(client), timeout=60, poll=0)
    assert ok
    assert client.calls == 1


def test_it_gives_up_after_the_timeout():
    client = _Client([RPCConnectionError("Could not reach remote")] * 1000)
    clock = {"t": 0.0}

    def tick(_s):
        clock["t"] += 10

    with patch.object(storage_node_ops.time, "time", lambda: clock["t"]), \
            patch.object(storage_node_ops.time, "sleep", tick):
        ok, err = storage_node_ops._wait_spdk_rpc_ready(_node(client), timeout=30, poll=10)
    assert not ok and isinstance(err, RPCConnectionError)
    assert client.calls == 4          # t = 0, 10, 20, 30


def test_the_restart_waits_before_its_first_spdk_rpc():
    src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
    start = src.index("spdk_process_start(")
    wait = src.index("_wait_spdk_rpc_ready(snode)", start)
    first_rpc = src.index("iobuf_set_options(", start)
    assert wait < first_rpc
