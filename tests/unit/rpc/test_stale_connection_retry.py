"""SFAM-2834: a pooled keep-alive socket that died under the client (SPDK/proxy
relaunched, FIN lost in a network outage) made the next POST fail although
SPDK never saw it. RPCClient now resends exactly once on a fresh session when
the connection dropped before any response -- and never on a read timeout,
where the request was delivered and may be executing."""

import http.client
import unittest
from unittest.mock import MagicMock, patch

from pydantic import SecretStr
from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import NewConnectionError, ProtocolError

from simplyblock_core.rpc_client import (
    RPCClient,
    RPCConnectionError,
    _is_dropped_before_response,
    _session_pool,
)


def _aborted(cause):
    # The shape requests' HTTPAdapter raises for a dead keep-alive socket.
    return ConnectionError(ProtocolError("Connection aborted.", cause))


def _ok_response(result=True):
    resp = MagicMock()
    resp.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": result}
    resp.raise_for_status.return_value = None
    return resp


class TestIsDroppedBeforeResponse(unittest.TestCase):

    def test_remote_disconnected(self):
        self.assertTrue(_is_dropped_before_response(
            _aborted(http.client.RemoteDisconnected("Remote end closed connection without response"))))

    def test_reset_by_peer(self):
        self.assertTrue(_is_dropped_before_response(_aborted(ConnectionResetError(104, "reset"))))

    def test_broken_pipe(self):
        self.assertTrue(_is_dropped_before_response(_aborted(BrokenPipeError(32, "pipe"))))

    def test_connection_refused_is_not(self):
        # Connect-phase failures are urllib3's connect= retries' business.
        self.assertFalse(_is_dropped_before_response(
            ConnectionError(NewConnectionError(None, "refused"))))

    def test_read_timeout_is_not(self):
        self.assertFalse(_is_dropped_before_response(ReadTimeout("read timed out")))


class TestStaleRetry(unittest.TestCase):

    def setUp(self):
        _session_pool.clear()
        self.client = RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=0)

    def tearDown(self):
        _session_pool.clear()

    def test_resends_once_on_fresh_session(self):
        stale = self.client.session
        fresh = MagicMock()
        fresh.post.return_value = _ok_response({"name": "alceml_1"})
        with patch.object(stale, "post",
                          side_effect=_aborted(ConnectionResetError(104, "reset"))) as stale_post, \
                patch("simplyblock_core.rpc_client._session_pool.get", return_value=fresh):
            result = self.client._request("bdev_alceml_create", name="alceml_1")
        self.assertEqual(result, {"name": "alceml_1"})
        stale_post.assert_called_once()
        fresh.post.assert_called_once()
        self.assertIs(self.client.session, fresh)

    def test_second_drop_raises(self):
        fresh = MagicMock()
        fresh.post.side_effect = _aborted(ConnectionResetError(104, "reset"))
        with patch.object(self.client.session, "post",
                          side_effect=_aborted(ConnectionResetError(104, "reset"))), \
                patch("simplyblock_core.rpc_client._session_pool.get", return_value=fresh):
            with self.assertRaises(RPCConnectionError):
                self.client._request("bdev_alceml_create", name="alceml_1")
        fresh.post.assert_called_once()

    def test_read_timeout_is_never_resent(self):
        with patch.object(self.client.session, "post", side_effect=ReadTimeout("timed out")) as post:
            with self.assertRaises(RPCConnectionError):
                self.client._request("bdev_alceml_create", name="alceml_1")
        post.assert_called_once()

    def test_refused_is_not_resent(self):
        with patch.object(self.client.session, "post",
                          side_effect=ConnectionError(NewConnectionError(None, "refused"))) as post:
            with self.assertRaises(RPCConnectionError):
                self.client._request("spdk_get_version")
        post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
