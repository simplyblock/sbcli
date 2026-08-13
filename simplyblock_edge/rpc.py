# coding=utf-8
"""SPDK JSON-RPC access for edge nodes.

Reuses the core RPCClient (proxy transport, TLS, secret handling, retry
policy) and adds the two AIO wrappers the hyperscale plane never needed.
"""
import time

from simplyblock_core.rpc_client import RPCClient, RPCException
from simplyblock_edge import constants as edge_constants


class EdgeRpcClient(RPCClient):

    # Bounded retry for transport-level failures. RPCClient._request2 does a
    # single POST and collapses ANY transport exception into
    # RPCException("connection error") — no retry despite the constructor's
    # retry parameter. The SPDK proxy closes its side after each response
    # while requests.Session reuses connections (keep-alive), so the SECOND
    # rpc in quick succession can hit a just-closed socket and die. That
    # killed every node add right after the successful get_version liveness
    # check (framework_start_init, both edge clusters, 2026-08-13). Edge RPCs
    # are idempotent by design (_ensure_* guards, "already exists"
    # tolerance), so a short retry is safe.
    CONNECTION_ERROR_RETRIES = 3

    def _request2(self, method, params=None, request_timeout=None):
        last_error = None
        for attempt in range(self.CONNECTION_ERROR_RETRIES):
            try:
                return super()._request2(method, params, request_timeout=request_timeout)
            except RPCException as e:
                if 'connection error' not in str(e.message).lower():
                    raise
                last_error = e
                time.sleep(0.3 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def bdev_aio_create(self, name, filename, block_size=edge_constants.EDGE_AIO_BLOCK_SIZE):
        params = {
            "name": name,
            "filename": filename,
            "block_size": block_size,
        }
        return self._request("bdev_aio_create", params)

    def bdev_aio_delete(self, name):
        return self._request("bdev_aio_delete", {"name": name})


def node_rpc_client(node, timeout=None, retry=None) -> EdgeRpcClient:
    """RPC client for one EdgeNode (spdk proxy at mgmt_ip:rpc_port)."""
    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    if retry is not None:
        kwargs["retry"] = retry
    return EdgeRpcClient(node.mgmt_ip, node.rpc_port,
                         node.rpc_username, node.rpc_password, **kwargs)
