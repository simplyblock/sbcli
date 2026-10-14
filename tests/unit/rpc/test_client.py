"""Unit tests for RPCClient wrapper methods (e.g. bdev_list, subsystem_list,
subsystem_get). Coverage is partial — add cases here as wrappers grow."""

import errno
import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import (
    RPCClient,
    RPCException,
    RPCRemoteError,
    _session_pool,
)


class TestBdevNvmeControllerList(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_unmatched_name_negative_einval_returns_empty(self, mock_req):
        # Sign SPDK uses for most RPC errors (e.g. bdev_get_bdevs's ENODEV).
        mock_req.side_effect = RPCRemoteError("Controller foo does not exist", code=-errno.EINVAL)
        client = _make_client()
        self.assertEqual(client.bdev_nvme_controller_list("foo"), [])

    @patch.object(RPCClient, "_request")
    def test_unmatched_name_positive_einval_returns_empty(self, mock_req):
        # bdev_nvme_rpc.c:687 sends EINVAL un-negated for this RPC specifically
        # (spdk_jsonrpc_send_error_response_fmt(request, EINVAL, ...)) -- the
        # regression this guards: a sign-only check missed this case entirely.
        mock_req.side_effect = RPCRemoteError("Controller foo does not exist", code=errno.EINVAL)
        client = _make_client()
        self.assertEqual(client.bdev_nvme_controller_list("foo"), [])

    @patch.object(RPCClient, "_request")
    def test_other_rpc_error_propagates(self, mock_req):
        mock_req.side_effect = RPCRemoteError("Something broke", code=-errno.ENODEV)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.bdev_nvme_controller_list("foo")

    @patch.object(RPCClient, "_request")
    def test_no_name_einval_propagates(self, mock_req):
        # The [] translation only applies to a filtered (named) lookup.
        mock_req.side_effect = RPCRemoteError("bad request", code=errno.EINVAL)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.bdev_nvme_controller_list()


def _make_client(**kwargs):
    """Create an RPCClient without hitting the network."""
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=0, **kwargs)


class TestBdevList(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_bdev_list_calls_request_each_time(self, mock_req):
        mock_req.return_value = [{"name": "bdev0"}]
        client = _make_client()

        r1 = client.bdev_list()
        r2 = client.bdev_list()

        # bdev_list uses _request directly (no caching)
        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(r1, r2)
        mock_req.assert_called_with("bdev_get_bdevs")


class TestBdevGet(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_bdev_get_delegates_filtering_to_rpc(self, mock_req):
        mock_req.return_value = [{"name": "LVS_1/LVOL_1"}]
        client = _make_client()

        self.assertEqual(client.bdev_get("LVS_1/LVOL_1")["name"], "LVS_1/LVOL_1")
        mock_req.assert_called_once_with("bdev_get_bdevs", name="LVS_1/LVOL_1")

    @patch.object(RPCClient, "_request")
    def test_bdev_get_filter_miss_returns_none(self, mock_req):
        mock_req.return_value = []
        client = _make_client()
        self.assertIsNone(client.bdev_get("LVS_1/GONE"))

    @patch.object(RPCClient, "_request")
    def test_bdev_get_no_such_device_returns_none(self, mock_req):
        # SPDK returns ENODEV (-19) "No such device" when the bdev is gone;
        # treat it as absent rather than propagating the error.
        mock_req.side_effect = RPCRemoteError("No such device", code=-errno.ENODEV)
        client = _make_client()
        self.assertIsNone(client.bdev_get("LVS_1/GONE"))

    @patch.object(RPCClient, "_request")
    def test_bdev_get_other_rpc_error_propagates(self, mock_req):
        # Generic RPC failures must still surface — an unknown answer is not
        # "absent" (the bug a bare `if not get_bdevs(name)` probe had).
        mock_req.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.bdev_get("LVS_1/LVOL_1")


class TestGetLvstore(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_get_lvstore_delegates_to_rpc(self, mock_req):
        mock_req.return_value = [{"name": "LVS_1"}]
        client = _make_client()

        self.assertEqual(client.get_lvstore("LVS_1"), {"name": "LVS_1"})
        mock_req.assert_called_once_with("bdev_lvol_get_lvstores", lvs_name="LVS_1")

    @patch.object(RPCClient, "_request")
    def test_get_lvstore_filter_miss_returns_none(self, mock_req):
        mock_req.return_value = []
        client = _make_client()
        self.assertIsNone(client.get_lvstore("LVS_GONE"))

    @patch.object(RPCClient, "_request")
    def test_get_lvstore_no_such_device_returns_none(self, mock_req):
        # SPDK answers ENODEV ("No such device") when the lvstore doesn't
        # exist -- expected while it has not (yet, or no longer) recovered.
        mock_req.side_effect = RPCRemoteError("No such device", code=-errno.ENODEV)
        client = _make_client()
        self.assertIsNone(client.get_lvstore("LVS_GONE"))

    @patch.object(RPCClient, "_request")
    def test_get_lvstore_other_rpc_error_propagates(self, mock_req):
        mock_req.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.get_lvstore("LVS_1")


class TestNbdGetDisk(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_nbd_get_disk_delegates_to_rpc(self, mock_req):
        mock_req.return_value = [{"nbd_device": "/dev/nbd0"}]
        client = _make_client()

        self.assertEqual(client.nbd_get_disk("/dev/nbd0"), {"nbd_device": "/dev/nbd0"})
        mock_req.assert_called_once_with("nbd_get_disks", nbd_device="/dev/nbd0")

    @patch.object(RPCClient, "_request")
    def test_nbd_get_disk_filter_miss_returns_none(self, mock_req):
        mock_req.return_value = []
        client = _make_client()
        self.assertIsNone(client.nbd_get_disk("/dev/nbd0"))

    @patch.object(RPCClient, "_request")
    def test_nbd_get_disk_no_such_device_returns_none(self, mock_req):
        # SPDK answers ENODEV ("No such device") once nbd_stop_disk has
        # un-exported the device -- the expected poll result, not a failure.
        mock_req.side_effect = RPCRemoteError("No such device", code=-errno.ENODEV)
        client = _make_client()
        self.assertIsNone(client.nbd_get_disk("/dev/nbd0"))

    @patch.object(RPCClient, "_request")
    def test_nbd_get_disk_other_rpc_error_propagates(self, mock_req):
        mock_req.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.nbd_get_disk("/dev/nbd0")


class TestBdevDistribCreate(unittest.TestCase):
    """bdev_distrib_create probes bdev_get for idempotency before creating.
    A transport/RPC failure on that probe must propagate, not be read as
    "does not exist yet" -- silently falling through to create on top of an
    unknown state is the bug this pins."""

    def _args(self):
        return ("distrib_1", 7001, 2, 1, 1000, 4096, ["jm1"], 4096)

    @patch.object(RPCClient, "bdev_get")
    def test_existing_bdev_short_circuits_create(self, mock_probe):
        mock_probe.return_value = {"name": "distrib_1"}
        client = _make_client()

        with patch.object(client, "_request") as mock_request:
            result = client.bdev_distrib_create(*self._args())

        self.assertEqual(result["name"], "distrib_1")
        mock_request.assert_not_called()

    @patch.object(RPCClient, "bdev_get")
    def test_probe_miss_falls_through_to_create(self, mock_probe):
        mock_probe.return_value = None
        client = _make_client()

        with patch.object(client, "_request", return_value=True) as mock_request:
            self.assertTrue(client.bdev_distrib_create(*self._args()))

        mock_request.assert_called_once()

    @patch.object(RPCClient, "bdev_get")
    def test_probe_rpc_error_propagates_instead_of_creating(self, mock_probe):
        mock_probe.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()

        with patch.object(client, "_request") as mock_request:
            with self.assertRaises(RPCException):
                client.bdev_distrib_create(*self._args())

        mock_request.assert_not_called()


class TestBdevRaidCreate(unittest.TestCase):
    """Same idempotency-probe contract as bdev_distrib_create."""

    @patch.object(RPCClient, "bdev_get")
    def test_existing_bdev_short_circuits_create(self, mock_probe):
        mock_probe.return_value = {"name": "raid_1"}
        client = _make_client()

        with patch.object(client, "_request") as mock_request:
            result = client.bdev_raid_create("raid_1", ["a", "b"], "1")

        self.assertEqual(result["name"], "raid_1")
        mock_request.assert_not_called()

    @patch.object(RPCClient, "bdev_get")
    def test_probe_miss_falls_through_to_create(self, mock_probe):
        mock_probe.return_value = None
        client = _make_client()

        with patch.object(client, "_request", return_value=True) as mock_request:
            self.assertTrue(client.bdev_raid_create("raid_1", ["a", "b"], "1"))

        mock_request.assert_called_once()

    @patch.object(RPCClient, "bdev_get")
    def test_probe_rpc_error_propagates_instead_of_creating(self, mock_probe):
        mock_probe.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()

        with patch.object(client, "_request") as mock_request:
            with self.assertRaises(RPCException):
                client.bdev_raid_create("raid_1", ["a", "b"], "1")

        mock_request.assert_not_called()


class TestSubsystem(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_subsystem_list_calls_request_each_time(self, mock_req):
        mock_req.return_value = [{"nqn": "nqn.test", "namespaces": []}]
        client = _make_client()

        r1 = client.subsystem_list()
        r2 = client.subsystem_list()

        # subsystem_list uses _request directly (no caching)
        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(r1, r2)

    @patch.object(RPCClient, "_request")
    def test_subsystem_get_delegates_filtering_to_rpc(self, mock_req):
        # nvmf_get_subsystems filters server-side, so the RPC returns only the
        # matching subsystem when queried by nqn.
        mock_req.return_value = [{"nqn": "nqn.b", "namespaces": []}]
        client = _make_client()

        self.assertEqual(client.subsystem_get("nqn.b")["nqn"], "nqn.b")
        mock_req.assert_called_once_with("nvmf_get_subsystems", nqn="nqn.b")

    @patch.object(RPCClient, "_request")
    def test_subsystem_get_filter_miss_returns_none(self, mock_req):
        mock_req.return_value = []
        client = _make_client()
        self.assertIsNone(client.subsystem_get("nqn.nonexistent"))

    @patch.object(RPCClient, "_request")
    def test_subsystem_get_no_such_device_returns_none(self, mock_req):
        # SPDK returns ENODEV (-19) "No such device" when the subsystem is gone;
        # treat it as absent rather than propagating the error.
        mock_req.side_effect = RPCRemoteError("No such device", code=-errno.ENODEV)
        client = _make_client()
        self.assertIsNone(client.subsystem_get("nqn.gone"))

    @patch.object(RPCClient, "_request")
    def test_subsystem_get_other_rpc_error_propagates(self, mock_req):
        # Generic RPC failures must still surface.
        mock_req.side_effect = RPCRemoteError("Something broke", code=-errno.EINVAL)
        client = _make_client()
        with self.assertRaises(RPCException):
            client.subsystem_get("nqn.b")


class TestSessionPool(unittest.TestCase):
    """RPCClient no longer builds a fresh requests.Session per instance --
    it fetches (or builds once) a pooled Session keyed by
    (host, port, username, password, tls_connect, retry)."""

    def test_same_identity_and_retry_share_one_session(self):
        with patch("requests.session") as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)
            c2 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)

        mock_session_factory.assert_called_once()
        self.assertIs(c1.session, c2.session)

    def test_different_host_gets_a_different_session(self):
        with patch("requests.session", side_effect=MagicMock) as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)
            c2 = RPCClient("10.0.0.2", 8080, "user", SecretStr("pass"), retry=2)

        self.assertEqual(mock_session_factory.call_count, 2)
        self.assertIsNot(c1.session, c2.session)

    def test_different_password_gets_a_different_session(self):
        with patch("requests.session", side_effect=MagicMock) as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass1"), retry=2)
            c2 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass2"), retry=2)

        self.assertEqual(mock_session_factory.call_count, 2)
        self.assertIsNot(c1.session, c2.session)

    def test_different_retry_gets_a_different_session(self):
        # retry is baked into the mounted urllib3.Retry at build time, so
        # unlike timeout it has to be part of the pool key.
        with patch("requests.session", side_effect=MagicMock) as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)
            c2 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=5)

        self.assertEqual(mock_session_factory.call_count, 2)
        self.assertIsNot(c1.session, c2.session)

    def test_different_timeout_shares_a_session(self):
        # timeout is never baked into the Session, so it must not force a
        # new pooled entry.
        with patch("requests.session") as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2, timeout=1)
            c2 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2, timeout=99)

        mock_session_factory.assert_called_once()
        self.assertIs(c1.session, c2.session)

    def test_evict_forces_a_rebuild(self):
        with patch("requests.session", side_effect=MagicMock) as mock_session_factory:
            c1 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)
            _session_pool.evict("10.0.0.1", 8080)
            c2 = RPCClient("10.0.0.1", 8080, "user", SecretStr("pass"), retry=2)

        self.assertEqual(mock_session_factory.call_count, 2)
        self.assertIsNot(c1.session, c2.session)


class TestBdevLvolS3Merge(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_merge_eexist_treated_as_success_by_default(self, mock_req):
        # A prior call whose RPC connection dropped before the response
        # arrived can leave a matching merge already queued on the data
        # plane; retrying that call must not be treated as a hard failure.
        mock_req.side_effect = RPCRemoteError("The same transfer task already exists.", code=-errno.EEXIST)
        client = _make_client()

        self.assertTrue(client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0"))

    @patch.object(RPCClient, "_request")
    def test_merge_eexist_propagates_when_disallowed(self, mock_req):
        mock_req.side_effect = RPCRemoteError("The same transfer task already exists.", code=-errno.EEXIST)
        client = _make_client()

        with self.assertRaises(RPCRemoteError):
            client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0", allow_exist=False)

    @patch.object(RPCClient, "_request")
    def test_merge_other_rpc_error_propagates_regardless_of_allow_exist(self, mock_req):
        mock_req.side_effect = RPCRemoteError("Cannot find S3 transfer device.", code=-errno.EINVAL)
        client = _make_client()

        with self.assertRaises(RPCRemoteError):
            client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0")

    @patch.object(RPCClient, "_request")
    def test_merge_success_passes_through(self, mock_req):
        mock_req.return_value = True
        client = _make_client()

        self.assertTrue(client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0", lvs_name="lvs0"))
        mock_req.assert_called_once_with("bdev_lvol_s3_merge", s3_id=1, old_s3_id=2, cluster_batch=16,
                                        s3_bdev="s3_lvs0", lvs_name="lvs0")


class TestBdevLvolS3MergeStat(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_merge_stat_calls_request_with_ids(self, mock_req):
        mock_req.return_value = {"transfer_state": "In progress"}
        client = _make_client()

        result = client.bdev_lvol_s3_merge_stat(1, 2)

        self.assertEqual(result["transfer_state"], "In progress")
        mock_req.assert_called_once_with("bdev_lvol_s3_merge_stat", s3_id=1, old_s3_id=2)


if __name__ == "__main__":
    unittest.main()

