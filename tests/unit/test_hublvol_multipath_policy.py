"""
test_hublvol_multipath_policy.py — pins ``ensure_hublvol_active_active``.

A hublvol controller's paths span two axes at once: the LVS leader's data
NICs (ANA ``optimized``) and the failover node's (ANA ``non_optimized``).
The wanted behaviour is active/active *within* the leader and
active/passive *between* leader and failover node.

SPDK gives that with a single ``active_active`` policy, because it
load-balances only within an ANA state: ``_bdev_nvme_find_io_path``
(bdev_nvme.c:1150) returns the first available ``optimized`` path in its
round-robin scan and reaches ``non_optimized`` only when no optimized path
exists. Left unset, the bdev keeps SPDK's creation default ACTIVE_PASSIVE
(bdev_nvme.c:4690) and one NIC of the leader carries every hub IO.

The assertion is deliberately non-fatal — callers gate their rejoin on the
*attach*, not on the policy — so these tests also pin that every failure
mode returns False rather than raising.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.utils import hublvol_reconnect
from simplyblock_core.utils.hublvol_reconnect import (
    HUBLVOL_MP_POLICY,
    HUBLVOL_MP_POLICY_WAIT_TRIES,
    HUBLVOL_MP_SELECTOR,
    ensure_hublvol_active_active,
)

CTRL = "LVS_1/hublvol"
BDEV = "LVS_1/hublvoln1"


class TestEnsureHublvolActiveActive(unittest.TestCase):

    def test_policy_asserted_on_namespace_bdev(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": BDEV}]

        self.assertTrue(ensure_hublvol_active_active(rpc, CTRL, "sec", "secondary"))

        rpc.get_bdevs.assert_called_once_with(BDEV)
        # The policy goes on the namespace bdev, not the controller name.
        args, kwargs = rpc.bdev_nvme_set_multipath_policy.call_args
        self.assertEqual(args[0], BDEV)
        self.assertEqual(args[1], HUBLVOL_MP_POLICY)
        self.assertEqual(HUBLVOL_MP_POLICY, "active_active")

    def test_selector_left_at_spdk_default(self):
        """No explicit selector: SPDK picks round_robin and coerces
        rr_min_io UINT32_MAX -> 1 (bdev_nvme.c:5626), matching the
        remote-device/JM path."""
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": BDEV}]

        ensure_hublvol_active_active(rpc, CTRL)

        kwargs = rpc.bdev_nvme_set_multipath_policy.call_args.kwargs
        self.assertIsNone(HUBLVOL_MP_SELECTOR)
        self.assertIsNone(kwargs.get("selector"))
        self.assertIsNone(kwargs.get("rr_min_io"))

    def test_no_wait_does_not_poll(self):
        """wait=False is for callers inside the LVS-rejoin freeze / port-block
        window: one probe, no sleep, give up rather than spend the budget."""
        rpc = MagicMock()
        rpc.get_bdevs.return_value = []

        with patch.object(hublvol_reconnect.time, "sleep") as sleep:
            ok = ensure_hublvol_active_active(rpc, CTRL, wait=False)

        self.assertFalse(ok)
        self.assertEqual(rpc.get_bdevs.call_count, 1)
        sleep.assert_not_called()
        rpc.bdev_nvme_set_multipath_policy.assert_not_called()

    def test_wait_polls_for_bdev_to_surface(self):
        """The attach can report enabled a few ms before the AER-driven n1
        bdev appears, so a bounded poll precedes the policy call."""
        rpc = MagicMock()
        rpc.get_bdevs.side_effect = [[], [], [{"name": BDEV}]]

        with patch.object(hublvol_reconnect.time, "sleep") as sleep:
            self.assertTrue(ensure_hublvol_active_active(rpc, CTRL))

        self.assertEqual(rpc.get_bdevs.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        rpc.bdev_nvme_set_multipath_policy.assert_called_once()

    def test_wait_gives_up_after_bounded_tries(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = []

        with patch.object(hublvol_reconnect.time, "sleep"):
            self.assertFalse(ensure_hublvol_active_active(rpc, CTRL))

        self.assertEqual(rpc.get_bdevs.call_count, HUBLVOL_MP_POLICY_WAIT_TRIES)
        rpc.bdev_nvme_set_multipath_policy.assert_not_called()

    def test_get_bdevs_raising_is_non_fatal(self):
        rpc = MagicMock()
        rpc.get_bdevs.side_effect = RuntimeError("rpc down")

        self.assertFalse(ensure_hublvol_active_active(rpc, CTRL))
        rpc.bdev_nvme_set_multipath_policy.assert_not_called()

    def test_set_policy_raising_is_non_fatal(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": BDEV}]
        rpc.bdev_nvme_set_multipath_policy.side_effect = RuntimeError("boom")

        self.assertFalse(ensure_hublvol_active_active(rpc, CTRL))

    def test_set_policy_falsy_is_reported(self):
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": BDEV}]
        rpc.bdev_nvme_set_multipath_policy.return_value = None

        self.assertFalse(ensure_hublvol_active_active(rpc, CTRL))


class TestRpcClientPolicyParams(unittest.TestCase):
    """The RPC wrapper must omit selector/rr_min_io when unset so SPDK's
    active_active defaults apply, and must pass them through when given."""

    def _rpc(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        return client

    def test_omits_unset_optional_params(self):
        client = self._rpc()
        client.bdev_nvme_set_multipath_policy(BDEV, "active_active")
        params = client._request.call_args.args[1]
        self.assertEqual(params, {"name": BDEV, "policy": "active_active"})

    def test_passes_selector_and_rr_min_io(self):
        client = self._rpc()
        client.bdev_nvme_set_multipath_policy(
            BDEV, "active_active", selector="queue_depth", rr_min_io=4)
        params = client._request.call_args.args[1]
        self.assertEqual(params["selector"], "queue_depth")
        self.assertEqual(params["rr_min_io"], 4)

    def test_rr_min_io_zero_is_not_dropped(self):
        """0 is invalid to SPDK (-EINVAL) but must reach it as 0 rather than
        being swallowed by a falsy check into a silent default."""
        client = self._rpc()
        client.bdev_nvme_set_multipath_policy(BDEV, "active_active", rr_min_io=0)
        params = client._request.call_args.args[1]
        self.assertEqual(params["rr_min_io"], 0)


if __name__ == "__main__":
    unittest.main()
