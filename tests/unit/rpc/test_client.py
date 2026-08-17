"""Unit tests for RPCClient wrapper methods (e.g. get_bdevs, subsystem_list,
subsystem_get). Coverage is partial — add cases here as wrappers grow."""

import errno
import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient, RPCException, RPCRemoteError, _session_pool


def _make_client(**kwargs):
    """Create an RPCClient without hitting the network."""
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=0, **kwargs)


class TestGetBdevs(unittest.TestCase):

    @patch.object(RPCClient, "_request")
    def test_get_bdevs_calls_request_each_time(self, mock_req):
        mock_req.return_value = [{"name": "bdev0"}]
        client = _make_client()

        r1 = client.get_bdevs()
        r2 = client.get_bdevs()

        # get_bdevs uses _request directly (no caching)
        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(r1, r2)

    @patch.object(RPCClient, "_request")
    def test_get_bdevs_with_name_separate_from_all(self, mock_req):
        mock_req.side_effect = [["all"], ["one"]]
        client = _make_client()

        client.get_bdevs()
        client.get_bdevs(name="bdev0")

        self.assertEqual(mock_req.call_count, 2)


class TestSubsystem(unittest.TestCase):

    @patch.object(RPCClient, "_request3")
    def test_subsystem_list_calls_request_each_time(self, mock_req):
        mock_req.return_value = [{"nqn": "nqn.test", "namespaces": []}]
        client = _make_client()

        r1 = client.subsystem_list()
        r2 = client.subsystem_list()

        # subsystem_list uses _request3 directly (no caching)
        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(r1, r2)

    @patch.object(RPCClient, "_request3")
    def test_subsystem_get_delegates_filtering_to_rpc(self, mock_req):
        # nvmf_get_subsystems filters server-side, so the RPC returns only the
        # matching subsystem when queried by nqn.
        mock_req.return_value = [{"nqn": "nqn.b", "namespaces": []}]
        client = _make_client()

        self.assertEqual(client.subsystem_get("nqn.b")["nqn"], "nqn.b")
        mock_req.assert_called_once_with("nvmf_get_subsystems", nqn="nqn.b")

    @patch.object(RPCClient, "_request3")
    def test_subsystem_get_filter_miss_returns_none(self, mock_req):
        mock_req.return_value = []
        client = _make_client()
        self.assertIsNone(client.subsystem_get("nqn.nonexistent"))

    @patch.object(RPCClient, "_request3")
    def test_subsystem_get_no_such_device_returns_none(self, mock_req):
        # SPDK returns ENODEV (-19) "No such device" when the subsystem is gone;
        # treat it as absent rather than propagating the error.
        mock_req.side_effect = RPCRemoteError("No such device", code=-errno.ENODEV)
        client = _make_client()
        self.assertIsNone(client.subsystem_get("nqn.gone"))

    @patch.object(RPCClient, "_request3")
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

    @patch.object(RPCClient, "_request3")
    def test_merge_eexist_treated_as_success_by_default(self, mock_req):
        # A prior call whose RPC connection dropped before the response
        # arrived can leave a matching merge already queued on the data
        # plane; retrying that call must not be treated as a hard failure.
        mock_req.side_effect = RPCRemoteError("The same transfer task already exists.", code=-errno.EEXIST)
        client = _make_client()

        self.assertTrue(client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0"))

    @patch.object(RPCClient, "_request3")
    def test_merge_eexist_propagates_when_disallowed(self, mock_req):
        mock_req.side_effect = RPCRemoteError("The same transfer task already exists.", code=-errno.EEXIST)
        client = _make_client()

        with self.assertRaises(RPCRemoteError):
            client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0", allow_exist=False)

    @patch.object(RPCClient, "_request3")
    def test_merge_other_rpc_error_propagates_regardless_of_allow_exist(self, mock_req):
        mock_req.side_effect = RPCRemoteError("Cannot find S3 transfer device.", code=-errno.EINVAL)
        client = _make_client()

        with self.assertRaises(RPCRemoteError):
            client.bdev_lvol_s3_merge(1, 2, cluster_batch=16, s3_bdev="s3_lvs0")

    @patch.object(RPCClient, "_request3")
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
        mock_req.assert_called_once_with("bdev_lvol_s3_merge_stat", {"s3_id": 1, "old_s3_id": 2})


if __name__ == "__main__":
    unittest.main()
