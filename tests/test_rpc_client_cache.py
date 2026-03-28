# coding=utf-8
"""
test_rpc_client_cache.py – unit tests for RPCClient._request_cached and
the cached wrappers get_bdevs / subsystem_list.
"""

import time
import threading
import unittest
from unittest.mock import patch

from simplyblock_core.rpc_client import RPCClient, _rpc_cache, _rpc_cache_lock


def _make_client(**kwargs):
    """Create an RPCClient without hitting the network."""
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", "pass", timeout=1, retry=0, **kwargs)


class TestRequestCached(unittest.TestCase):
    """Low-level tests for _request_cached."""

    def setUp(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    def tearDown(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    @patch.object(RPCClient, "_request")
    def test_first_call_misses_cache(self, mock_req):
        mock_req.return_value = [{"name": "bdev0"}]
        client = _make_client()

        result = client._request_cached("bdev_get_bdevs")

        mock_req.assert_called_once_with("bdev_get_bdevs", None)
        self.assertEqual(result, [{"name": "bdev0"}])

    @patch.object(RPCClient, "_request")
    def test_second_call_hits_cache(self, mock_req):
        mock_req.return_value = [{"name": "bdev0"}]
        client = _make_client()

        result1 = client._request_cached("bdev_get_bdevs")
        result2 = client._request_cached("bdev_get_bdevs")

        mock_req.assert_called_once()
        self.assertEqual(result1, result2)

    @patch.object(RPCClient, "_request")
    def test_cache_expires_after_ttl(self, mock_req):
        mock_req.side_effect = [["first"], ["second"]]
        client = _make_client()

        result1 = client._request_cached("bdev_get_bdevs", cache_ttl=0.1)
        time.sleep(0.15)
        result2 = client._request_cached("bdev_get_bdevs", cache_ttl=0.1)

        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(result1, ["first"])
        self.assertEqual(result2, ["second"])

    @patch.object(RPCClient, "_request")
    def test_different_params_are_separate_cache_keys(self, mock_req):
        mock_req.side_effect = [["all_bdevs"], ["one_bdev"]]
        client = _make_client()

        result1 = client._request_cached("bdev_get_bdevs", None)
        result2 = client._request_cached("bdev_get_bdevs", {"name": "bdev0"})

        self.assertEqual(mock_req.call_count, 2)
        self.assertEqual(result1, ["all_bdevs"])
        self.assertEqual(result2, ["one_bdev"])

    @patch.object(RPCClient, "_request")
    def test_different_nodes_are_separate_cache_keys(self, mock_req):
        mock_req.return_value = ["data"]

        with patch("requests.session"):
            client_a = RPCClient("10.0.0.1", 8081, "u", "p", timeout=1, retry=0)
            client_b = RPCClient("10.0.0.2", 8081, "u", "p", timeout=1, retry=0)

        client_a._request_cached("bdev_get_bdevs")
        client_b._request_cached("bdev_get_bdevs")

        self.assertEqual(mock_req.call_count, 2)

    @patch.object(RPCClient, "_request")
    def test_cache_is_shared_across_client_instances(self, mock_req):
        mock_req.return_value = ["data"]
        client1 = _make_client()
        client2 = _make_client()

        client1._request_cached("bdev_get_bdevs")
        client2._request_cached("bdev_get_bdevs")

        # Same ip/port, so second call should be a cache hit
        mock_req.assert_called_once()

    @patch.object(RPCClient, "_request")
    def test_cache_thread_safety(self, mock_req):
        """Multiple threads hitting the cache concurrently should not crash."""
        mock_req.return_value = ["data"]

        client = _make_client()
        errors = []

        def worker():
            try:
                client._request_cached("bdev_get_bdevs", cache_ttl=5)
            except Exception as e:
                errors.append(e)

        # First call to populate cache
        client._request_cached("bdev_get_bdevs", cache_ttl=5)
        initial_count = mock_req.call_count

        # Now 20 threads should all hit cache
        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        # All 20 threads should have hit cache — no additional calls
        self.assertEqual(mock_req.call_count, initial_count)


class TestGetBdevsUsesCache(unittest.TestCase):

    def setUp(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    def tearDown(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    @patch.object(RPCClient, "_request")
    def test_get_bdevs_uses_cache(self, mock_req):
        mock_req.return_value = [{"name": "bdev0"}]
        client = _make_client()

        r1 = client.get_bdevs()
        r2 = client.get_bdevs()

        mock_req.assert_called_once()
        self.assertEqual(r1, r2)

    @patch.object(RPCClient, "_request")
    def test_get_bdevs_with_name_separate_from_all(self, mock_req):
        mock_req.side_effect = [["all"], ["one"]]
        client = _make_client()

        client.get_bdevs()
        client.get_bdevs(name="bdev0")

        self.assertEqual(mock_req.call_count, 2)


class TestSubsystemListUsesCache(unittest.TestCase):

    def setUp(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    def tearDown(self):
        with _rpc_cache_lock:
            _rpc_cache.clear()

    @patch.object(RPCClient, "_request")
    def test_subsystem_list_uses_cache(self, mock_req):
        mock_req.return_value = [{"nqn": "nqn.test", "namespaces": []}]
        client = _make_client()

        r1 = client.subsystem_list()
        r2 = client.subsystem_list()

        mock_req.assert_called_once()
        self.assertEqual(r1, r2)

    @patch.object(RPCClient, "_request")
    def test_subsystem_list_filters_by_nqn(self, mock_req):
        mock_req.return_value = [
            {"nqn": "nqn.a", "namespaces": []},
            {"nqn": "nqn.b", "namespaces": []},
        ]
        client = _make_client()

        result = client.subsystem_list(nqn_name="nqn.b")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["nqn"], "nqn.b")

    @patch.object(RPCClient, "_request")
    def test_subsystem_list_filter_miss_returns_empty(self, mock_req):
        mock_req.return_value = [{"nqn": "nqn.a", "namespaces": []}]
        client = _make_client()

        result = client.subsystem_list(nqn_name="nqn.nonexistent")
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
