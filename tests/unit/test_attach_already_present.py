"""
test_attach_already_present.py — an attach that already succeeded is success.

Multipath soak 2026-08-19, iteration 4. Node 87771a35 was container_killed and
its restart looped offline <-> in_restart for 30+ minutes with

    hublvol LVS_1/hublvol on 87771a35: attach returned falsy for 172.31.96.214
    hublvol LVS_1/hublvol on 87771a35: attach returned falsy for 172.31.97.220
    hublvol LVS_1/hublvol on 87771a35: no path attached
    -> Hublvol reconcile failed -> "Storage node LVStore recovery failed"

while the controller on that very node reported::

    LVS_1/hublvol paths=2
       state=enabled 172.31.96.214:4435
       state=enabled 172.31.97.220:4435

Both paths were up. Two SPDK success shapes are falsy and the caller gated on
truthiness:

  * a path added to an EXISTING multipath controller creates no new bdev, so
    the RPC succeeds and returns an empty name list;
  * re-adding a path that is already present fails with -EALREADY
    ("already exists with the specified network path", bdev_nvme_rpc.c), which
    means the path is there.

Neither is a failure, and treating them as one is self-perpetuating: once the
first attempt attaches the paths, no later attempt can ever report success.
"""

import errno
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.rpc_client import RPCClient


def _client(result, error):
    client = RPCClient.__new__(RPCClient)
    client._request2 = MagicMock(return_value=(result, error))
    return client


def _attach(client):
    return client.bdev_nvme_attach_controller(
        "LVS_1/hublvol", "nqn:hublvol:LVS_1", "172.31.96.214", 4435, "TCP",
        multipath="multipath")


class TestAttachReturnContract(unittest.TestCase):

    def test_already_present_path_is_success(self):
        """-EALREADY is the strongest success signal, not a failure."""
        client = _client(None, {
            "code": -errno.EALREADY,
            "message": ("A controller named LVS_1/hublvol already exists with "
                        "the specified network path")})
        self.assertTrue(_attach(client))

    def test_already_present_matched_by_message_when_code_differs(self):
        client = _client(None, {
            "code": -32603,
            "message": ("A controller named LVS_1/hublvol already exists with "
                        "the specified network path")})
        self.assertTrue(_attach(client))

    def test_empty_result_is_success(self):
        """Adding a path to an existing controller creates no new bdev."""
        client = _client([], None)
        self.assertTrue(_attach(client))

    def test_created_bdev_names_are_passed_through(self):
        """Callers that harvest the bdev name must keep getting the list."""
        client = _client(["LVS_1/hublvoln1"], None)
        self.assertEqual(_attach(client), ["LVS_1/hublvoln1"])

    def test_real_failures_stay_falsy(self):
        for error in (
            {"code": -errno.ECONNREFUSED, "message": "Connection refused"},
            {"code": -errno.EINVAL,
             "message": "already exists, but uses a different subnqn (x)"},
            {"code": -32603, "message": "Internal error"},
        ):
            with self.subTest(error=error):
                self.assertFalse(_attach(_client(None, error)))


class TestReconcileBelievesTheController(unittest.TestCase):
    """Even if some attach path returns falsy, an enabled controller that
    carries the wanted paths must not be reported as "no path attached"."""

    def _coordinator(self):
        from simplyblock_core.utils.hublvol_reconnect import (
            HublvolReconnectCoordinator,
        )
        return HublvolReconnectCoordinator(MagicMock())

    def test_enabled_controller_with_expected_ip_is_treated_as_attached(self):
        from simplyblock_core.utils import hublvol_reconnect as hr

        rpc = MagicMock()
        # every attach reports failure...
        rpc.bdev_nvme_attach_controller.return_value = None
        # ...but the controller has both paths enabled.
        enabled = [
            {"state": "enabled", "trid": {"traddr": "172.31.96.214"}},
            {"state": "enabled", "trid": {"traddr": "172.31.97.220"}},
        ]
        node = MagicMock()
        node.get_id.return_value = "87771a35"
        with patch.object(hr, "_ensure_attach_ready", return_value="attach"), \
                patch.object(hr, "_ctrlrs_from_list", return_value=enabled), \
                patch.object(hr, "_wait_for_settled", return_value=enabled), \
                patch.object(hr.time, "sleep"):
            ok = self._coordinator()._attach_paths_safely(
                rpc, "LVS_1/hublvol", "nqn:hublvol:LVS_1", 4435,
                {"172.31.96.214": "TCP", "172.31.97.220": "TCP"},
                node, "secondary", verify_at_end=False)
        self.assertTrue(
            ok, "an enabled controller carrying the wanted paths must not be "
                "reported as 'no path attached'")

    def test_genuinely_absent_controller_still_fails(self):
        from simplyblock_core.utils import hublvol_reconnect as hr

        rpc = MagicMock()
        rpc.bdev_nvme_attach_controller.return_value = None
        node = MagicMock()
        node.get_id.return_value = "87771a35"
        with patch.object(hr, "_ensure_attach_ready", return_value="attach"), \
                patch.object(hr, "_ctrlrs_from_list", return_value=[]), \
                patch.object(hr, "_wait_for_settled", return_value=[]), \
                patch.object(hr.time, "sleep"):
            ok = self._coordinator()._attach_paths_safely(
                rpc, "LVS_1/hublvol", "nqn:hublvol:LVS_1", 4435,
                {"172.31.96.214": "TCP"}, node, "secondary",
                verify_at_end=False)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
