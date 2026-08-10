# coding=utf-8
"""SPDK JSON-RPC access for edge nodes.

Reuses the core RPCClient (proxy transport, TLS, secret handling, retry
policy) and adds the two AIO wrappers the hyperscale plane never needed.
"""
from simplyblock_core.rpc_client import RPCClient
from simplyblock_edge import constants as edge_constants


class EdgeRpcClient(RPCClient):

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
