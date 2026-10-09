"""Regression tests for the call sites whose recovery the _request3 migration broke.

Commits c324110ea / e6cc5a0cd turned a falsy RPC return into a raise. Call sites
whose recovery lived in an ``if not ret:`` branch silently lost it: the branch
stopped running and the exception propagated past it. These cover the paths
where the recovery is load-bearing rather than just an error report — a ``force``
teardown that must keep going, a per-item loop that records the failure and
carries on, an idempotent create that adopts what a previous pass left behind.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.rpc_client import RPCRemoteError


class TestCryptoKeyAlreadyExists(unittest.TestCase):
    """SPDK answers an error when the key name already exists; on re-activation
    that is the same node re-issuing the same key, and the crypto bdev create
    below still has to run."""

    @patch("simplyblock_core.controllers.lvol_controller.create_kms_connection")
    def test_duplicate_key_error_does_not_abort_create(self, mock_kms):
        from simplyblock_core.controllers import lvol_controller

        mock_kms.return_value.__enter__.return_value.get_data_encryption_keys.return_value = (
            "key1", "key2")
        rpc_client = MagicMock()
        # Base bdev present, crypto bdev not yet — so the idempotency probe
        # does not short-circuit the create.
        rpc_client.bdev_get.side_effect = lambda n: {"name": n} if n == "lvs/lvol1" else None
        rpc_client.lvol_crypto_key_create.side_effect = RPCRemoteError(
            "Key already exists", code=-17)
        rpc_client.lvol_crypto_create.return_value = "crypto_bdev"

        lvol = MagicMock(lvs_name="lvs", lvol_bdev="lvol1", crypto_bdev="crypto_lvol1")
        lvol.get_id.return_value = "lvol-uuid"
        cluster = MagicMock()
        cluster.get_id.return_value = "cluster-uuid"

        ret = lvol_controller._create_crypto_lvol(rpc_client, lvol, cluster)

        rpc_client.lvol_crypto_create.assert_called_once_with(
            "crypto_lvol1", "lvs/lvol1", "key_crypto_lvol1")
        self.assertEqual(ret, "crypto_bdev")


class TestCreateBdevStack(unittest.TestCase):
    """The subsystem-full retry re-enters _create_bdev_stack after the bdev was
    created but add_ns failed, and a mid-stack failure has to reach the stack
    rollback that removes what this pass already built."""

    def _lvol(self, stack):
        lvol = MagicMock(lvs_name="lvs", lvol_bdev="lvol1", lvol_uuid="u", blobid=1,
                         lvol_priority_class=0, bdev_stack=stack)
        lvol.get_id.return_value = "lvol-uuid"
        return lvol

    def test_duplicate_create_adopts_existing_bdev(self):
        from simplyblock_core.controllers import lvol_controller

        rpc_client = MagicMock()
        rpc_client.bdev_get.side_effect = lambda n: {"name": n} if n == "lvs/lvol1" else None
        rpc_client.create_lvol.side_effect = RPCRemoteError("File exists", code=-17)
        snode = MagicMock()
        snode.rpc_client.return_value = rpc_client

        stack = [{"type": "bdev_lvol", "name": "lvol1", "params": {"name": "lvol1"}}]
        ok, err = lvol_controller._create_bdev_stack(self._lvol(stack), snode)

        self.assertTrue(ok, err)
        self.assertEqual(stack[0]["status"], "created")

    @patch("simplyblock_core.controllers.lvol_controller._remove_bdev_stack")
    def test_failure_mid_stack_rolls_back_what_was_created(self, mock_rollback):
        from simplyblock_core.controllers import lvol_controller

        rpc_client = MagicMock()
        rpc_client.bdev_get.return_value = None
        rpc_client.ultra21_lvol_bmap_init.return_value = True
        rpc_client.create_lvol.side_effect = RPCRemoteError("No space left", code=-28)
        snode = MagicMock()
        snode.rpc_client.return_value = rpc_client

        stack = [
            {"type": "bmap_init", "name": "bmap", "params": {}},
            {"type": "bdev_lvol", "name": "lvol1", "params": {"name": "lvol1"}},
        ]
        ok, err = lvol_controller._create_bdev_stack(self._lvol(stack), snode)

        self.assertFalse(ok)
        self.assertIn("No space left", err)
        mock_rollback.assert_called_once()
        self.assertEqual([b["name"] for b in mock_rollback.call_args[0][0]], ["bmap"])


class TestAnaFlipPerNic(unittest.TestCase):
    """A failed ANA flip on one data NIC used to be a warning; the remaining
    NICs still had to be flipped."""

    def test_failure_on_first_nic_still_flips_the_second(self):
        from simplyblock_core import storage_node_ops

        rpc_client = MagicMock()
        rpc_client.nvmf_subsystem_listener_set_ana_state.side_effect = [
            RPCRemoteError("boom", code=-1), True]

        node = MagicMock()
        node.rpc_client.return_value = rpc_client
        node.get_lvol_subsys_port.return_value = 9090
        node.data_nics = [
            MagicMock(ip4_address="10.0.0.1", trtype="TCP"),
            MagicMock(ip4_address="10.0.0.2", trtype="TCP"),
        ]
        lvol = MagicMock(nqn="nqn.test", fabric="tcp", ns_id=1)

        storage_node_ops._set_lvol_ana_on_node(lvol, node, "inaccessible")

        self.assertEqual(
            rpc_client.nvmf_subsystem_listener_set_ana_state.call_count, 2)


if __name__ == "__main__":
    unittest.main()
