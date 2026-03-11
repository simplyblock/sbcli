# coding=utf-8
"""
test_nvmeof_security.py – unit tests for NVMe-oF TLS / DH-HMAC-CHAP security.

Tests cover:
  - TLS config validation (digests and dhgroups)
  - Security options validation
  - Key generation (PSK, DH-HMAC-CHAP)
  - Cluster model tls_config field
  - LVol model allowed_hosts field
  - RPC client method signatures for subsystem security
  - _build_host_entries helper
  - add_host_to_lvol / remove_host_from_lvol controller logic
  - bdev_nvme_set_options with dhchap params
  - connect_lvol TLS-aware output
"""

import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.controllers.lvol_controller as lvol_ctl
from simplyblock_core import constants
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import (
    generate_psk_key,
    generate_dhchap_key,
    validate_tls_config,
    validate_sec_options,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(tls=False, tls_config=None, nqn="nqn.2023-02.io.simplyblock:test"):
    c = Cluster()
    c.uuid = "cluster-1"
    c.tls = tls
    c.tls_config = tls_config or {}
    c.nqn = nqn
    c.client_qpair_count = 3
    c.client_data_nic = ""
    return c


def _lvol(uuid="lvol-1", node_id="node-1", nqn="nqn:test:lvol-1",
          allowed_hosts=None, nodes=None):
    l = LVol()
    l.uuid = uuid
    l.node_id = node_id
    l.nqn = nqn
    l.status = LVol.STATUS_ONLINE
    l.allowed_hosts = allowed_hosts or []
    l.nodes = nodes or [node_id]
    l.subsys_port = 9090
    l.ns_id = 1
    l.ha_type = "single"
    l.fabric = "tcp"
    return l


def _node(uuid="node-1", status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1"):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.mgmt_ip = "127.0.0.1"
    n.rpc_port = 9901
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.hostname = f"host-{uuid}"
    n.data_nics = []
    return n


# ---------------------------------------------------------------------------
# Validation utils
# ---------------------------------------------------------------------------

class TestValidateTlsConfig(unittest.TestCase):

    def test_valid_config(self):
        cfg = {"params": {
            "dhchap_digests": ["sha384", "sha512"],
            "dhchap_dhgroups": ["ffdhe6144", "ffdhe8192"],
        }}
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)
        self.assertIsNone(err)

    def test_valid_config_flat(self):
        """Config without nested 'params' key."""
        cfg = {
            "dhchap_digests": ["sha256"],
            "dhchap_dhgroups": ["ffdhe2048"],
        }
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)
        self.assertIsNone(err)

    def test_invalid_digest(self):
        cfg = {"params": {"dhchap_digests": ["sha384", "md5"]}}
        ok, err = validate_tls_config(cfg)
        self.assertFalse(ok)
        self.assertIn("md5", err)

    def test_invalid_dhgroup(self):
        cfg = {"params": {"dhchap_dhgroups": ["ffdhe9999"]}}
        ok, err = validate_tls_config(cfg)
        self.assertFalse(ok)
        self.assertIn("ffdhe9999", err)

    def test_null_group_is_valid(self):
        cfg = {"params": {"dhchap_dhgroups": ["null", "ffdhe2048"]}}
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)

    def test_empty_config(self):
        ok, err = validate_tls_config({})
        self.assertTrue(ok)

    def test_case_insensitive_digest(self):
        cfg = {"params": {"dhchap_digests": ["SHA-256"]}}
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)

    def test_all_valid_digests(self):
        cfg = {"params": {"dhchap_digests": ["sha256", "sha384", "sha512"]}}
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)

    def test_all_valid_dhgroups(self):
        cfg = {"params": {"dhchap_dhgroups": constants.VALID_DHCHAP_DHGROUPS[:]}}
        ok, err = validate_tls_config(cfg)
        self.assertTrue(ok)


class TestValidateSecOptions(unittest.TestCase):

    def test_psk_only(self):
        ok, err = validate_sec_options({"psk": True})
        self.assertTrue(ok)

    def test_dhchap_key_only(self):
        ok, err = validate_sec_options({"dhchap_key": True})
        self.assertTrue(ok)

    def test_dhchap_key_and_ctrlr_key(self):
        ok, err = validate_sec_options({"dhchap_key": True, "dhchap_ctrlr_key": True})
        self.assertTrue(ok)

    def test_all_three(self):
        ok, err = validate_sec_options({"dhchap_key": True, "dhchap_ctrlr_key": True, "psk": True})
        self.assertTrue(ok)

    def test_ctrlr_key_without_dhchap_key_rejected(self):
        ok, err = validate_sec_options({"dhchap_ctrlr_key": True})
        self.assertFalse(ok)
        self.assertIn("dhchap_ctrlr_key requires dhchap_key", err)

    def test_invalid_key_rejected(self):
        ok, err = validate_sec_options({"bad_key": True})
        self.assertFalse(ok)
        self.assertIn("bad_key", err)

    def test_empty_valid(self):
        ok, err = validate_sec_options({})
        self.assertTrue(ok)


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

class TestKeyGeneration(unittest.TestCase):

    def test_psk_key_length(self):
        key = generate_psk_key(256)
        self.assertEqual(len(key), 64)  # 256 bits = 32 bytes = 64 hex chars

    def test_psk_key_is_hex(self):
        key = generate_psk_key()
        int(key, 16)  # should not raise

    def test_psk_keys_are_unique(self):
        keys = {generate_psk_key() for _ in range(10)}
        self.assertEqual(len(keys), 10)

    def test_dhchap_key_is_base64(self):
        import base64
        key = generate_dhchap_key()
        base64.b64decode(key)  # should not raise

    def test_dhchap_keys_are_unique(self):
        keys = {generate_dhchap_key() for _ in range(10)}
        self.assertEqual(len(keys), 10)

    def test_dhchap_key_default_length(self):
        import base64
        key = generate_dhchap_key(32)
        raw = base64.b64decode(key)
        self.assertEqual(len(raw), 32)


# ---------------------------------------------------------------------------
# Model fields
# ---------------------------------------------------------------------------

class TestClusterModelTls(unittest.TestCase):

    def test_tls_default_false(self):
        c = Cluster()
        self.assertFalse(c.tls)

    def test_tls_config_default_empty(self):
        c = Cluster()
        self.assertEqual(c.tls_config, {})

    def test_tls_config_stores_dict(self):
        c = Cluster()
        c.tls = True
        c.tls_config = {"params": {"dhchap_digests": ["sha384"]}}
        self.assertTrue(c.tls)
        self.assertEqual(c.tls_config["params"]["dhchap_digests"], ["sha384"])


class TestLVolModelAllowedHosts(unittest.TestCase):

    def test_default_empty(self):
        l = LVol()
        self.assertEqual(l.allowed_hosts, [])

    def test_stores_host_entries(self):
        l = LVol()
        l.allowed_hosts = [
            {"nqn": "nqn:host1", "psk": "abc123"},
            {"nqn": "nqn:host2", "dhchap_key": "key1"},
        ]
        self.assertEqual(len(l.allowed_hosts), 2)
        self.assertEqual(l.allowed_hosts[0]["nqn"], "nqn:host1")
        self.assertEqual(l.allowed_hosts[1]["dhchap_key"], "key1")


# ---------------------------------------------------------------------------
# RPC client method signatures
# ---------------------------------------------------------------------------

class TestRpcClientSignatures(unittest.TestCase):

    def test_subsystem_create_allow_any_host_param(self):
        import inspect
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.subsystem_create)
        self.assertIn("allow_any_host", sig.parameters)
        self.assertTrue(sig.parameters["allow_any_host"].default)

    def test_subsystem_add_host_security_params(self):
        import inspect
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.subsystem_add_host)
        for p in ["psk", "dhchap_key", "dhchap_ctrlr_key"]:
            self.assertIn(p, sig.parameters)
            self.assertIsNone(sig.parameters[p].default)

    def test_subsystem_remove_host_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, "subsystem_remove_host"))

    def test_bdev_nvme_set_options_dhchap_params(self):
        import inspect
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.bdev_nvme_set_options)
        self.assertIn("dhchap_digests", sig.parameters)
        self.assertIn("dhchap_dhgroups", sig.parameters)


# ---------------------------------------------------------------------------
# _build_host_entries
# ---------------------------------------------------------------------------

class TestBuildHostEntries(unittest.TestCase):

    def setUp(self):
        from simplyblock_core.controllers import lvol_controller
        self.fn = lvol_controller._build_host_entries

    def test_no_sec_options(self):
        entries = self.fn(["nqn:host1", "nqn:host2"])
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0], {"nqn": "nqn:host1"})
        self.assertEqual(entries[1], {"nqn": "nqn:host2"})

    def test_psk_auto_generated(self):
        entries = self.fn(["nqn:host1"], sec_options={"psk": True})
        self.assertEqual(len(entries), 1)
        self.assertIn("psk", entries[0])
        self.assertEqual(len(entries[0]["psk"]), 64)  # hex PSK

    def test_dhchap_key_auto_generated(self):
        entries = self.fn(["nqn:host1"], sec_options={"dhchap_key": True})
        self.assertIn("dhchap_key", entries[0])
        self.assertNotIn("dhchap_ctrlr_key", entries[0])

    def test_dhchap_both_keys(self):
        entries = self.fn(["nqn:host1"],
                          sec_options={"dhchap_key": True, "dhchap_ctrlr_key": True})
        self.assertIn("dhchap_key", entries[0])
        self.assertIn("dhchap_ctrlr_key", entries[0])

    def test_all_sec_options(self):
        entries = self.fn(["nqn:host1"],
                          sec_options={"psk": True, "dhchap_key": True, "dhchap_ctrlr_key": True})
        e = entries[0]
        self.assertIn("psk", e)
        self.assertIn("dhchap_key", e)
        self.assertIn("dhchap_ctrlr_key", e)

    def test_invalid_sec_options_returns_error(self):
        result = self.fn(["nqn:host1"], sec_options={"dhchap_ctrlr_key": True})
        self.assertIsInstance(result, tuple)
        self.assertFalse(result[0])

    def test_multiple_hosts_each_get_unique_keys(self):
        entries = self.fn(["nqn:h1", "nqn:h2", "nqn:h3"], sec_options={"psk": True})
        psks = [e["psk"] for e in entries]
        self.assertEqual(len(set(psks)), 3)


# ---------------------------------------------------------------------------
# add_host_to_lvol / remove_host_from_lvol
# ---------------------------------------------------------------------------



def _mock_db_for_host_ops(lvol, node, cluster):
    """Build a mock DBController for add/remove host tests."""
    mock_db = MagicMock()
    mock_db.get_lvol_by_id.return_value = lvol
    mock_db.get_storage_node_by_id.return_value = node
    mock_db.get_cluster_by_id.return_value = cluster
    mock_db.kv_store = MagicMock()
    return mock_db


class TestAddHostToLvol(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_success(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(allowed_hosts=[], nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        with patch.object(lvol, "write_to_db") as mock_write:
            result, err = lvol_ctl.add_host_to_lvol("lvol-1", "nqn:new-host",
                                                      sec_options={"psk": True})
            self.assertIsNone(err)
            self.assertEqual(result["nqn"], "nqn:new-host")
            self.assertIn("psk", result)
            self.assertEqual(len(result["psk"]), 64)

            mock_rpc_inst.subsystem_add_host.assert_called_once()
            call_args = mock_rpc_inst.subsystem_add_host.call_args
            self.assertEqual(call_args[0][0], lvol.nqn)
            self.assertEqual(call_args[0][1], "nqn:new-host")

            # lvol should have the new host appended
            self.assertEqual(len(lvol.allowed_hosts), 1)
            mock_write.assert_called_once()

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_no_tls_rejected(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=False)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        result, err = lvol_ctl.add_host_to_lvol("lvol-1", "nqn:host")
        self.assertFalse(result)
        self.assertIn("TLS is not enabled", err)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_duplicate_host_rejected(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(allowed_hosts=[{"nqn": "nqn:existing"}], nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        result, err = lvol_ctl.add_host_to_lvol("lvol-1", "nqn:existing")
        self.assertFalse(result)
        self.assertIn("already allowed", err)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_rpc_failure(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = False
        MockRPC.return_value = mock_rpc_inst

        result, err = lvol_ctl.add_host_to_lvol("lvol-1", "nqn:host",
                                                  sec_options={"dhchap_key": True})
        self.assertFalse(result)
        self.assertIn("Failed to add host", err)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_with_dhchap_keys(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        result, err = lvol_ctl.add_host_to_lvol(
            "lvol-1", "nqn:host",
            sec_options={"dhchap_key": True, "dhchap_ctrlr_key": True})
        self.assertIsNone(err)
        self.assertIn("dhchap_key", result)
        self.assertIn("dhchap_ctrlr_key", result)

        call_kwargs = mock_rpc_inst.subsystem_add_host.call_args
        self.assertIsNotNone(call_kwargs.kwargs.get("dhchap_key") or
                             call_kwargs[1].get("dhchap_key"))

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_multi_node(self, MockDBCtrl, MockRPC):
        """Host ACL applied to all online nodes."""
        cl = _cluster(tls=True)
        node1 = _node("node-1")
        node1.cluster_id = cl.uuid
        node2 = _node("node-2")
        node2.cluster_id = cl.uuid
        lvol = _lvol(nodes=["node-1", "node-2"])

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_cluster_by_id.return_value = cl
        mock_db.kv_store = MagicMock()

        def get_node(nid):
            return {"node-1": node1, "node-2": node2}[nid]
        mock_db.get_storage_node_by_id.side_effect = get_node
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        result, err = lvol_ctl.add_host_to_lvol("lvol-1", "nqn:host")
        self.assertIsNone(err)
        self.assertEqual(mock_rpc_inst.subsystem_add_host.call_count, 2)


class TestRemoveHostFromLvol(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_success(self, MockDBCtrl, MockRPC):
        node = _node()
        lvol = _lvol(
            allowed_hosts=[{"nqn": "nqn:host1"}, {"nqn": "nqn:host2"}],
            nodes=[node.uuid],
        )

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.kv_store = MagicMock()
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_remove_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        with patch.object(lvol, "write_to_db") as mock_write:
            result, err = lvol_ctl.remove_host_from_lvol("lvol-1", "nqn:host1")
            self.assertIsNone(err)
            self.assertTrue(result)

            mock_rpc_inst.subsystem_remove_host.assert_called_once_with(lvol.nqn, "nqn:host1")
            self.assertEqual(len(lvol.allowed_hosts), 1)
            self.assertEqual(lvol.allowed_hosts[0]["nqn"], "nqn:host2")
            mock_write.assert_called_once()

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_nonexistent_host_rejected(self, MockDBCtrl):
        lvol = _lvol(allowed_hosts=[{"nqn": "nqn:host1"}])

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        MockDBCtrl.return_value = mock_db

        result, err = lvol_ctl.remove_host_from_lvol("lvol-1", "nqn:not-there")
        self.assertFalse(result)
        self.assertIn("not in the allowed list", err)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_rpc_failure(self, MockDBCtrl, MockRPC):
        node = _node()
        lvol = _lvol(
            allowed_hosts=[{"nqn": "nqn:host1"}],
            nodes=[node.uuid],
        )

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.kv_store = MagicMock()
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_remove_host.return_value = False
        MockRPC.return_value = mock_rpc_inst

        result, err = lvol_ctl.remove_host_from_lvol("lvol-1", "nqn:host1")
        self.assertFalse(result)
        self.assertIn("Failed to remove host", err)
        # allowed_hosts NOT modified on failure
        self.assertEqual(len(lvol.allowed_hosts), 1)


# ---------------------------------------------------------------------------
# connect_lvol TLS-aware output
# ---------------------------------------------------------------------------

class TestConnectLvolTls(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_with_tls_includes_flag(self, MockDBCtrl):
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]
        node.active_tcp = True

        lvol = _lvol(
            allowed_hosts=[{"nqn": "nqn:host1"}],
            nodes=[node.uuid],
        )

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl
        MockDBCtrl.return_value = mock_db

        result = lvol_ctl.connect_lvol("lvol-1")
        self.assertTrue(len(result) > 0)
        entry = result[0]
        self.assertTrue(entry.get("tls"))
        self.assertEqual(entry["allowed_hosts"], ["nqn:host1"])
        self.assertIn("--tls", entry["connect"])

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_without_tls_no_flag(self, MockDBCtrl):
        cl = _cluster(tls=False)
        node = _node()
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]
        node.active_tcp = True

        lvol = _lvol(allowed_hosts=[], nodes=[node.uuid])

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl
        MockDBCtrl.return_value = mock_db

        result = lvol_ctl.connect_lvol("lvol-1")
        self.assertTrue(len(result) > 0)
        entry = result[0]
        self.assertNotIn("tls", entry)
        self.assertNotIn("--tls", entry["connect"])


# ---------------------------------------------------------------------------
# bdev_nvme_set_options param passing
# ---------------------------------------------------------------------------

class TestBdevNvmeSetOptionsParams(unittest.TestCase):

    def test_no_dhchap_params_not_in_request(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.bdev_nvme_set_options()
        call_args = client._request.call_args[0]
        params = call_args[1]
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)

    def test_dhchap_params_passed_through(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.bdev_nvme_set_options(
            dhchap_digests=["sha384", "sha512"],
            dhchap_dhgroups=["ffdhe6144"],
        )
        call_args = client._request.call_args[0]
        params = call_args[1]
        self.assertEqual(params["dhchap_digests"], ["sha384", "sha512"])
        self.assertEqual(params["dhchap_dhgroups"], ["ffdhe6144"])


# ---------------------------------------------------------------------------
# subsystem_create allow_any_host
# ---------------------------------------------------------------------------

class TestSubsystemCreateAllowAnyHost(unittest.TestCase):

    def test_default_allow_any_host_true(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.subsystem_create("nqn:test", "serial", "model")
        params = client._request.call_args[0][1]
        self.assertTrue(params["allow_any_host"])

    def test_allow_any_host_false(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.subsystem_create("nqn:test", "serial", "model", allow_any_host=False)
        params = client._request.call_args[0][1]
        self.assertFalse(params["allow_any_host"])


# ---------------------------------------------------------------------------
# subsystem_add_host security params
# ---------------------------------------------------------------------------

class TestSubsystemAddHostParams(unittest.TestCase):

    def _client(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        return client

    def test_basic_no_security(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host")
        params = client._request.call_args[0][1]
        self.assertEqual(params["nqn"], "nqn:sub")
        self.assertEqual(params["host"], "nqn:host")
        self.assertNotIn("psk", params)
        self.assertNotIn("dhchap_key", params)

    def test_with_psk(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host", psk="/tmp/psk.key")
        params = client._request.call_args[0][1]
        self.assertEqual(params["psk"], "/tmp/psk.key")

    def test_with_dhchap_keys(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host",
                                  dhchap_key="key1", dhchap_ctrlr_key="key2")
        params = client._request.call_args[0][1]
        self.assertEqual(params["dhchap_key"], "key1")
        self.assertEqual(params["dhchap_ctrlr_key"], "key2")

    def test_with_all_security(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host",
                                  psk="psk_val", dhchap_key="dk", dhchap_ctrlr_key="dck")
        params = client._request.call_args[0][1]
        self.assertEqual(params["psk"], "psk_val")
        self.assertEqual(params["dhchap_key"], "dk")
        self.assertEqual(params["dhchap_ctrlr_key"], "dck")


# ---------------------------------------------------------------------------
# subsystem_remove_host
# ---------------------------------------------------------------------------

class TestSubsystemRemoveHost(unittest.TestCase):

    def test_remove_host_params(self):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.subsystem_remove_host("nqn:sub", "nqn:host")
        client._request.assert_called_once_with("nvmf_subsystem_remove_host",
                                                 {"nqn": "nqn:sub", "host": "nqn:host"})


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants(unittest.TestCase):

    def test_valid_digests(self):
        self.assertIn("sha256", constants.VALID_DHCHAP_DIGESTS)
        self.assertIn("sha384", constants.VALID_DHCHAP_DIGESTS)
        self.assertIn("sha512", constants.VALID_DHCHAP_DIGESTS)
        self.assertEqual(len(constants.VALID_DHCHAP_DIGESTS), 3)

    def test_valid_dhgroups(self):
        expected = {"null", "ffdhe2048", "ffdhe3072", "ffdhe4096", "ffdhe6144", "ffdhe8192"}
        self.assertEqual(set(constants.VALID_DHCHAP_DHGROUPS), expected)


if __name__ == "__main__":
    unittest.main()
