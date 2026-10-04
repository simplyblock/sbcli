"""Regression test for the mgmt_ip-change eviction in
storage_node_ops.restart_storage_node's "restart on a new address" branch.

Exercises the StorageNode/RPCSessionPool interaction directly rather than
driving the whole restart_storage_node orchestration."""

import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import evict_cached_session


def _make_node(mgmt_ip):
    node = StorageNode()
    node.uuid = "node-1"
    node.mgmt_ip = mgmt_ip
    node.rpc_port = 8080
    node.rpc_username = "user"
    node.rpc_password = SecretStr("pass")
    return node


class TestMgmtIpChangeEvictsPooledSession(unittest.TestCase):

    def test_restart_on_new_address_rebuilds_the_session(self):
        node = _make_node("10.0.0.1")

        with patch("requests.session", side_effect=MagicMock) as mock_session_factory:
            old_client = node.rpc_client()

            old_host, old_port = old_client.host, old_client.port
            node.mgmt_ip = "10.0.0.2"
            evict_cached_session(old_host, old_port)

            new_client = node.rpc_client()
            # Back at the original address: must rebuild too, not resurrect
            # the evicted entry.
            node.mgmt_ip = "10.0.0.1"
            rebuilt_old_client = node.rpc_client()

        self.assertIsNot(new_client.session, old_client.session)
        self.assertIsNot(rebuilt_old_client.session, old_client.session)
        self.assertEqual(mock_session_factory.call_count, 3)


if __name__ == "__main__":
    unittest.main()
