"""The in-cluster addresses a StorageNode dials carry no cluster DNS domain.

Both the storage-node API client and the SPDK RPC client address a pod through
a headless Service while TLS is on. Spelling the address
`<pod>.<service>.<namespace>.svc.cluster.local` resolves only on a cluster
installed with that domain, and a cluster set up with another one resolves none
of them. `<pod>.<service>.<namespace>.svc` is answered through the pod's
resolver search list on any of them, and is the name the operator puts on the
serving certificates.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _node():
    node = StorageNode()
    node.hostname = "worker-1_5000"
    node.rpc_port = 5000
    node.cr_namespace = "simplyblock"
    node.api_endpoint = "10.0.0.1:5000"
    node.mgmt_ip = "10.0.0.1"
    node.rpc_username = "u"
    node.rpc_password = "p"
    return node


class TestNodeAddressesCarryNoClusterDomain(unittest.TestCase):
    def _settings(self, tls_connect):
        settings = MagicMock()
        settings.tls_connect = tls_connect
        return settings

    def test_the_api_client_host_carries_no_cluster_domain(self):
        with patch("simplyblock_core.models.storage_node.Settings",
                   return_value=self._settings("authenticated")), \
             patch("simplyblock_core.models.storage_node.SNodeClient") as client:
            _node().client()

        host = client.call_args[0][0]
        self.assertEqual(
            host, "worker-1.simplyblock-storage-node-api.simplyblock.svc:5000")

    def test_the_rpc_client_host_carries_no_cluster_domain(self):
        with patch("simplyblock_core.models.storage_node.Settings",
                   return_value=self._settings("authenticated")), \
             patch("simplyblock_core.models.storage_node.RPCClient") as client:
            _node().rpc_client()

        host = client.call_args[0][0]
        self.assertEqual(host, "worker-1.simplyblock-spdk-proxy.simplyblock.svc")

    def test_a_plaintext_deployment_still_dials_the_address_it_stored(self):
        with patch("simplyblock_core.models.storage_node.Settings",
                   return_value=self._settings("disabled")), \
             patch("simplyblock_core.models.storage_node.RPCClient") as client:
            _node().rpc_client()

        self.assertEqual(client.call_args[0][0], "10.0.0.1")


if __name__ == "__main__":
    unittest.main()
