# coding=utf-8
"""
test_rpc_client_cache.py – unit tests for the RPCClient read-only wrappers
get_bdevs / subsystem_list / subsystem_get.
"""

import unittest
from unittest.mock import patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient


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
    def test_subsystem_get_filters_by_nqn(self, mock_req):
        mock_req.return_value = [
            {"nqn": "nqn.a", "namespaces": []},
            {"nqn": "nqn.b", "namespaces": []},
        ]
        client = _make_client()
        self.assertEqual(client.subsystem_get("nqn.b")["nqn"], "nqn.b")

    @patch.object(RPCClient, "_request3")
    def test_subsystem_get_filter_miss_returns_empty(self, mock_req):
        mock_req.return_value = [{"nqn": "nqn.a", "namespaces": []}]
        client = _make_client()
        self.assertIsNone(client.subsystem_get("nqn.nonexistent"))


if __name__ == "__main__":
    unittest.main()
