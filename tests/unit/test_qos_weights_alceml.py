"""Class shares go to distrib AND alceml.

The rpc wrapper named alceml_set_qos_weights used to send
bdev_distrib_set_qos_weights, so alceml scheduled with its built-in
{75, 25, 0, ...} and replication (wire class 7) got the minimum budget per
round on a busy device (bf17, 2026-11-08).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import qos_controller
from simplyblock_core.rpc_client import (
    RPC_METHOD_NOT_FOUND,
    RPC_UNSUPPORTED,
    RPCClient,
    RPCRemoteError,
)

W = [40, 25, 0, 0, 0, 0, 0, 35]


class TestRpcNames(unittest.TestCase):

    def _client(self):
        c = RPCClient.__new__(RPCClient)
        c._request = MagicMock(return_value=True)
        return c

    def test_alceml_wrapper_sends_the_alceml_rpc(self):
        c = self._client()
        self.assertTrue(c.alceml_set_qos_weights(W))
        c._request.assert_called_once_with("alceml_set_qos_weights", qos_weights=W)

    def test_distrib_wrapper_sends_the_distrib_rpc(self):
        c = self._client()
        self.assertTrue(c.distrib_set_qos_weights(W))
        c._request.assert_called_once_with("bdev_distrib_set_qos_weights", qos_weights=W)

    def test_a_build_without_the_alceml_rpc_is_reported(self):
        c = self._client()
        err = RPCRemoteError.__new__(RPCRemoteError)
        err.code = RPC_METHOD_NOT_FOUND
        c._request.side_effect = err
        self.assertIs(c.alceml_set_qos_weights(W), RPC_UNSUPPORTED)


class TestSetNodeWeights(unittest.TestCase):

    def test_both_levels_are_set(self):
        rpc = MagicMock()
        rpc.distrib_set_qos_weights.return_value = True
        rpc.alceml_set_qos_weights.return_value = True
        self.assertTrue(qos_controller.set_node_qos_weights(rpc, W))
        rpc.distrib_set_qos_weights.assert_called_once_with(W)
        rpc.alceml_set_qos_weights.assert_called_once_with(W)

    def test_an_alceml_failure_does_not_fail_the_node(self):
        for aret in (False, RPC_UNSUPPORTED):
            rpc = MagicMock()
            rpc.distrib_set_qos_weights.return_value = True
            rpc.alceml_set_qos_weights.return_value = aret
            self.assertTrue(qos_controller.set_node_qos_weights(rpc, W))

    def test_distrib_decides(self):
        rpc = MagicMock()
        rpc.distrib_set_qos_weights.return_value = False
        rpc.alceml_set_qos_weights.return_value = True
        self.assertFalse(qos_controller.set_node_qos_weights(rpc, W))

    def test_push_uses_both(self):
        node = MagicMock(status="online")
        node.get_id.return_value = "N1"
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [node]
        with patch.object(qos_controller, "db", db), \
                patch.object(qos_controller, "get_qos_weights_list", return_value=W), \
                patch.object(qos_controller, "set_node_qos_weights",
                             return_value=True) as both:
            self.assertEqual(qos_controller.push_qos_weights("CL"), [])
        both.assert_called_once_with(node.rpc_client.return_value, W)
