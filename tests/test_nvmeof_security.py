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

from simplyblock_core import constants
import simplyblock_core.storage_node_ops as snode_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
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


def _pool(uuid="pool-1", sec_options=None):
    p = Pool()
    p.uuid = uuid
    p.status = Pool.STATUS_ACTIVE
    p.sec_options = sec_options or {}
    return p


def _lvol(uuid="lvol-1", node_id="node-1", nqn="nqn:test:lvol-1",
          allowed_hosts=None, nodes=None, pool_uuid="pool-1"):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.nqn = nqn
    lv.status = LVol.STATUS_ONLINE
    lv.allowed_hosts = allowed_hosts or []
    lv.nodes = nodes or [node_id]
    lv.subsys_port = 9090
    lv.ns_id = 1
    lv.ha_type = "single"
    lv.fabric = "tcp"
    lv.pool_uuid = pool_uuid
    return lv


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

class TestGetDhchapGroup(unittest.TestCase):
    """Tests for _get_dhchap_group helper."""

    def test_returns_first_configured_group(self):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        cl = _cluster(tls=True, tls_config={
            "params": {"dhchap_dhgroups": ["ffdhe4096", "ffdhe2048"]}})
        self.assertEqual(_get_dhchap_group(cl), "ffdhe4096")

    def test_returns_null_when_no_groups(self):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        cl = _cluster(tls=True, tls_config={"params": {"dhchap_digests": ["sha256"]}})
        self.assertEqual(_get_dhchap_group(cl), "null")

    def test_returns_null_when_tls_disabled(self):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        cl = _cluster(tls=False)
        self.assertEqual(_get_dhchap_group(cl), "null")

    def test_returns_null_when_cluster_none(self):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        self.assertEqual(_get_dhchap_group(None), "null")

    def test_flat_config_without_params_key(self):
        from simplyblock_core.controllers.lvol_controller import _get_dhchap_group
        cl = _cluster(tls=True, tls_config={"dhchap_dhgroups": ["ffdhe3072"]})
        self.assertEqual(_get_dhchap_group(cl), "ffdhe3072")


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

    def test_dhchap_key_is_dhhc1_format(self):
        key = generate_dhchap_key()
        self.assertTrue(key.startswith("DHHC-1:01:"))
        self.assertTrue(key.endswith(":"))
        # base64 payload is valid
        import base64
        payload = key.split(":")[2]
        base64.b64decode(payload)  # should not raise

    def test_dhchap_keys_are_unique(self):
        keys = {generate_dhchap_key() for _ in range(10)}
        self.assertEqual(len(keys), 10)

    def test_dhchap_key_default_length(self):
        import base64
        key = generate_dhchap_key(32)
        payload = key.split(":")[2]
        raw = base64.b64decode(payload)
        # 32 bytes key + 4 bytes CRC32
        self.assertEqual(len(raw), 36)


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
        lv = LVol()
        self.assertEqual(lv.allowed_hosts, [])

    def test_stores_host_entries(self):
        lv = LVol()
        lv.allowed_hosts = [
            {"nqn": "nqn:host1", "psk": "abc123"},
            {"nqn": "nqn:host2", "dhchap_key": "key1"},
        ]
        self.assertEqual(len(lv.allowed_hosts), 2)
        self.assertEqual(lv.allowed_hosts[0]["nqn"], "nqn:host1")
        self.assertEqual(lv.allowed_hosts[1]["dhchap_key"], "key1")


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

    def test_bdev_nvme_set_options_no_dhchap_params(self):
        """DHCHAP moved to nvmf_set_config – bdev_nvme_set_options must not accept them."""
        import inspect
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.bdev_nvme_set_options)
        self.assertNotIn("dhchap_digests", sig.parameters)
        self.assertNotIn("dhchap_dhgroups", sig.parameters)

    def test_nvmf_set_config_dhchap_params(self):
        """nvmf_set_config must accept dhchap_digests and dhchap_dhgroups."""
        import inspect
        from simplyblock_core.rpc_client import RPCClient
        sig = inspect.signature(RPCClient.nvmf_set_config)
        self.assertIn("dhchap_digests", sig.parameters)
        self.assertIn("dhchap_dhgroups", sig.parameters)
        self.assertIsNone(sig.parameters["dhchap_digests"].default)
        self.assertIsNone(sig.parameters["dhchap_dhgroups"].default)


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



def _mock_db_for_host_ops(lvol, node, cluster, pool=None):
    """Build a mock DBController for add/remove host tests."""
    mock_db = MagicMock()
    mock_db.get_lvol_by_id.return_value = lvol
    mock_db.get_storage_node_by_id.return_value = node
    mock_db.get_cluster_by_id.return_value = cluster
    if pool is not None:
        mock_db.get_pool_by_id.return_value = pool
    else:
        mock_db.get_pool_by_id.side_effect = KeyError("pool not found")
    mock_db.kv_store = MagicMock()
    return mock_db


class TestAddHostToLvol(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_success(self, MockDBCtrl, MockRPC, mock_register):
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        cl = _cluster(tls=True)
        node = _node()
        node.cluster_id = cl.uuid
        pool = _pool(sec_options={"psk": True})
        lvol = _lvol(allowed_hosts=[], nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl, pool=pool)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst
        mock_register.return_value = {"psk": "psk_key_name"}

        with patch.object(lvol, "write_to_db") as mock_write:
            result, err = add_host_to_lvol("lvol-1", "nqn:new-host")
            self.assertIsNone(err)
            self.assertEqual(result["nqn"], "nqn:new-host")
            self.assertIn("psk", result)

            mock_rpc_inst.subsystem_add_host.assert_called_once()
            call_args = mock_rpc_inst.subsystem_add_host.call_args
            self.assertEqual(call_args[0][0], lvol.nqn)
            self.assertEqual(call_args[0][1], "nqn:new-host")

            # lvol should have the new host appended
            self.assertEqual(len(lvol.allowed_hosts), 1)
            mock_write.assert_called_once()

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_without_tls_succeeds(self, MockDBCtrl, MockRPC):
        """Adding a host without TLS is allowed (TLS and DHCHAP are independent)."""
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        cl = _cluster(tls=False)
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        result, err = add_host_to_lvol("lvol-1", "nqn:host")
        self.assertIsNone(err)
        self.assertEqual(result["nqn"], "nqn:host")

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_duplicate_host_rejected(self, MockDBCtrl, MockRPC):
        cl = _cluster(tls=True)
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        node = _node()
        node.cluster_id = cl.uuid
        lvol = _lvol(allowed_hosts=[{"nqn": "nqn:existing"}], nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl)
        MockDBCtrl.return_value = mock_db

        result, err = add_host_to_lvol("lvol-1", "nqn:existing")
        self.assertFalse(result)
        self.assertIn("already allowed", err)

    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_rpc_failure(self, MockDBCtrl, MockRPC, mock_register):
        cl = _cluster(tls=True)
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        node = _node()
        node.cluster_id = cl.uuid
        pool = _pool(sec_options={"dhchap_key": True})
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl, pool=pool)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = False
        MockRPC.return_value = mock_rpc_inst
        mock_register.return_value = {"dhchap_key": "kn"}

        result, err = add_host_to_lvol("lvol-1", "nqn:host")
        self.assertFalse(result)
        self.assertIn("Failed to add host", err)

    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_with_dhchap_keys(self, MockDBCtrl, MockRPC, mock_register):
        cl = _cluster(tls=True)
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        node = _node()
        node.cluster_id = cl.uuid
        pool = _pool(sec_options={"dhchap_key": True, "dhchap_ctrlr_key": True})
        lvol = _lvol(nodes=[node.uuid])

        mock_db = _mock_db_for_host_ops(lvol, node, cl, pool=pool)
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst
        mock_register.return_value = {
            "dhchap_key": "kn_dhchap",
            "dhchap_ctrlr_key": "kn_ctrlr",
        }

        result, err = add_host_to_lvol("lvol-1", "nqn:host")
        self.assertIsNone(err)
        self.assertIn("dhchap_key", result)
        self.assertIn("dhchap_ctrlr_key", result)

        call_kwargs = mock_rpc_inst.subsystem_add_host.call_args
        self.assertEqual(call_kwargs[1].get("dhchap_key"), "kn_dhchap")
        self.assertEqual(call_kwargs[1].get("dhchap_ctrlr_key"), "kn_ctrlr")

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_host_multi_node(self, MockDBCtrl, MockRPC):
        """Host ACL applied to all online nodes."""
        from simplyblock_core.controllers.lvol_controller import add_host_to_lvol
        cl = _cluster(tls=True)
        node1 = _node("node-1")
        node1.cluster_id = cl.uuid
        node2 = _node("node-2")
        node2.cluster_id = cl.uuid
        lvol = _lvol(nodes=["node-1", "node-2"])

        pool = _pool()  # dhchap=False – use legacy path

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_cluster_by_id.return_value = cl
        mock_db.get_pool_by_id.return_value = pool
        mock_db.kv_store = MagicMock()

        def get_node(nid):
            return {"node-1": node1, "node-2": node2}[nid]
        mock_db.get_storage_node_by_id.side_effect = get_node
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_add_host.return_value = True
        MockRPC.return_value = mock_rpc_inst

        result, err = add_host_to_lvol("lvol-1", "nqn:host")
        self.assertIsNone(err)
        self.assertEqual(mock_rpc_inst.subsystem_add_host.call_count, 2)


class TestRemoveHostFromLvol(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_success(self, MockDBCtrl, MockRPC):
        node = _node()
        from simplyblock_core.controllers.lvol_controller import remove_host_from_lvol
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
            result, err = remove_host_from_lvol("lvol-1", "nqn:host1")
            self.assertIsNone(err)
            self.assertTrue(result)

            mock_rpc_inst.subsystem_remove_host.assert_called_once_with(lvol.nqn, "nqn:host1")
            self.assertEqual(len(lvol.allowed_hosts), 1)
            self.assertEqual(lvol.allowed_hosts[0]["nqn"], "nqn:host2")
            mock_write.assert_called_once()

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_nonexistent_host_rejected(self, MockDBCtrl):
        lvol = _lvol(allowed_hosts=[{"nqn": "nqn:host1"}])
        from simplyblock_core.controllers.lvol_controller import remove_host_from_lvol

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        MockDBCtrl.return_value = mock_db

        result, err = remove_host_from_lvol("lvol-1", "nqn:not-there")
        self.assertFalse(result)
        self.assertIn("not in the allowed list", err)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_rpc_failure(self, MockDBCtrl, MockRPC):
        node = _node()
        from simplyblock_core.controllers.lvol_controller import remove_host_from_lvol
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

        result, err = remove_host_from_lvol("lvol-1", "nqn:host1")
        # DB is still updated even on SPDK failure (host may already be gone)
        self.assertTrue(result)
        self.assertIn("Warning", err)
        # allowed_hosts IS modified (DB cleaned up)
        self.assertEqual(len(lvol.allowed_hosts), 0)


# ---------------------------------------------------------------------------
# connect_lvol TLS-aware output
# ---------------------------------------------------------------------------

class TestConnectLvolTls(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_with_psk_includes_tls_flag(self, MockDBCtrl):
        """TLS flag in connect output is driven by PSK in host entry, not cluster.tls."""
        from simplyblock_core.controllers.lvol_controller import connect_lvol
        cl = _cluster(tls=False)
        node = _node()
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]
        node.active_tcp = True

        lvol = _lvol(
            allowed_hosts=[{"nqn": "nqn:host1", "psk": "abcdef1234"}],
            nodes=[node.uuid],
        )

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl
        MockDBCtrl.return_value = mock_db

        result = connect_lvol("lvol-1", host_nqn="nqn:host1")
        self.assertTrue(len(result) > 0)
        entry = result[0]
        self.assertIn("--tls", entry["connect"])

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_without_tls_no_flag(self, MockDBCtrl):
        cl = _cluster(tls=False)
        from simplyblock_core.controllers.lvol_controller import connect_lvol
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

        result = connect_lvol("lvol-1")
        self.assertTrue(len(result) > 0)
        entry = result[0]
        self.assertNotIn("tls", entry)
        self.assertNotIn("--tls", entry["connect"])


# ---------------------------------------------------------------------------
# bdev_nvme_set_options param passing
# ---------------------------------------------------------------------------

class TestBdevNvmeSetOptionsParams(unittest.TestCase):

    def test_no_dhchap_params_not_in_request(self):
        """bdev_nvme_set_options must never send dhchap params (moved to nvmf_set_config)."""
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(return_value=True)
        client.bdev_nvme_set_options()
        call_args = client._request.call_args[0]
        params = call_args[1]
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)


class TestNvmfSetConfigDhchapParams(unittest.TestCase):

    def _client(self):
        from simplyblock_core.rpc_client import RPCClient
        c = RPCClient.__new__(RPCClient)
        c._request = MagicMock(return_value=True)
        return c

    def test_without_dhchap_only_pollers_mask_sent(self):
        client = self._client()
        client.nvmf_set_config("0x1")
        params = client._request.call_args[0][1]
        self.assertEqual(params["poll_groups_mask"], "0x1")
        self.assertNotIn("dhchap_digests", params)
        self.assertNotIn("dhchap_dhgroups", params)

    def test_with_dhchap_digests_and_dhgroups(self):
        client = self._client()
        client.nvmf_set_config("0x3",
                               dhchap_digests=["sha256", "sha384", "sha512"],
                               dhchap_dhgroups=["ffdhe2048"])
        params = client._request.call_args[0][1]
        self.assertEqual(params["poll_groups_mask"], "0x3")
        self.assertEqual(params["dhchap_digests"], ["sha256", "sha384", "sha512"])
        self.assertEqual(params["dhchap_dhgroups"], ["ffdhe2048"])

    def test_fixed_constants_are_sent(self):
        """The fixed DHCHAP_DIGESTS and DHCHAP_DHGROUP constants must match expectations."""
        from simplyblock_core import constants
        client = self._client()
        client.nvmf_set_config("0x1",
                               dhchap_digests=constants.DHCHAP_DIGESTS,
                               dhchap_dhgroups=[constants.DHCHAP_DHGROUP])
        params = client._request.call_args[0][1]
        self.assertIn("sha256", params["dhchap_digests"])
        self.assertIn("sha384", params["dhchap_digests"])
        self.assertIn("sha512", params["dhchap_digests"])
        self.assertEqual(params["dhchap_dhgroups"], ["ffdhe2048"])


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

    def test_dhchap_group_passed(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host",
                                  dhchap_key="dk", dhchap_group="ffdhe2048")
        params = client._request.call_args[0][1]
        self.assertEqual(params["dhchap_group"], "ffdhe2048")

    def test_dhchap_group_null_passed(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host",
                                  dhchap_key="dk", dhchap_group="null")
        params = client._request.call_args[0][1]
        self.assertEqual(params["dhchap_group"], "null")

    def test_dhchap_group_omitted_when_none(self):
        client = self._client()
        client.subsystem_add_host("nqn:sub", "nqn:host", dhchap_key="dk")
        params = client._request.call_args[0][1]
        self.assertNotIn("dhchap_group", params)


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


# ---------------------------------------------------------------------------
# Subsystem recreation on node restart (security preservation)
# ---------------------------------------------------------------------------


class TestReapplyAllowedHosts(unittest.TestCase):
    """Tests for _reapply_allowed_hosts helper in storage_node_ops."""

    def _mock_db(self, tls=False, tls_config=None):
        cl = _cluster(tls=tls, tls_config=tls_config)
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cl
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    def test_reapply_hosts_with_dhchap_keys(self, mock_register, MockDB):
        """Hosts with DHCHAP keys get registered via keyring + subsystem_add_host."""
        MockDB.return_value = self._mock_db(tls=True,
            tls_config={"params": {"dhchap_dhgroups": ["ffdhe2048"]}})
        mock_register.return_value = {
            "dhchap_key": "key_name_dhchap",
            "dhchap_ctrlr_key": "key_name_ctrlr",
        }
        mock_rpc = MagicMock()
        mock_rpc.subsystem_add_host.return_value = True

        node = _node()
        lvol = _lvol(allowed_hosts=[{
            "nqn": "nqn:host1",
            "dhchap_key": "DHHC-1:01:abc:",
            "dhchap_ctrlr_key": "DHHC-1:01:def:",
        }])

        snode_ops._reapply_allowed_hosts(lvol, node, mock_rpc)

        mock_register.assert_called_once_with(
            node, "nqn:host1", lvol.allowed_hosts[0], mock_rpc)
        mock_rpc.subsystem_add_host.assert_called_once_with(
            lvol.nqn, "nqn:host1",
            psk=None,
            dhchap_key="key_name_dhchap",
            dhchap_ctrlr_key="key_name_ctrlr",
            dhchap_group="ffdhe2048",
        )

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_reapply_hosts_without_keys(self, MockDB, _mock_disc, _mock_phase, _mock_handle):
        """Hosts without security keys get added with just the NQN."""
        MockDB.return_value = self._mock_db()
        mock_rpc = MagicMock()
        mock_rpc.subsystem_add_host.return_value = True

        node = _node()
        lvol = _lvol(allowed_hosts=[{"nqn": "nqn:plain-host"}])

        snode_ops._reapply_allowed_hosts(lvol, node, mock_rpc)

        mock_rpc.subsystem_add_host.assert_called_once_with(
            lvol.nqn, "nqn:plain-host")

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    def test_reapply_multiple_hosts(self, mock_register, MockDB, _mock_disc, _mock_phase, _mock_handle):
        """All hosts are re-registered, not just the first one."""
        MockDB.return_value = self._mock_db()
        mock_register.return_value = {"dhchap_key": "kn"}
        mock_rpc = MagicMock()
        mock_rpc.subsystem_add_host.return_value = True

        node = _node()
        lvol = _lvol(allowed_hosts=[
            {"nqn": "nqn:h1", "dhchap_key": "DHHC-1:01:a:"},
            {"nqn": "nqn:h2"},
            {"nqn": "nqn:h3", "dhchap_key": "DHHC-1:01:b:"},
        ])

        snode_ops._reapply_allowed_hosts(lvol, node, mock_rpc)

        self.assertEqual(mock_rpc.subsystem_add_host.call_count, 3)
        self.assertEqual(mock_register.call_count, 2)  # h1 and h3 have keys

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node")
    def test_reapply_with_psk(self, mock_register, MockDB, _mock_disc, _mock_phase, _mock_handle):
        """PSK-only host entry gets keyring registration."""
        MockDB.return_value = self._mock_db()
        mock_register.return_value = {"psk": "psk_key_name"}
        mock_rpc = MagicMock()
        mock_rpc.subsystem_add_host.return_value = True

        node = _node()
        lvol = _lvol(allowed_hosts=[{
            "nqn": "nqn:psk-host",
            "psk": "abcdef1234567890",
        }])

        snode_ops._reapply_allowed_hosts(lvol, node, mock_rpc)

        mock_register.assert_called_once()
        mock_rpc.subsystem_add_host.assert_called_once_with(
            lvol.nqn, "nqn:psk-host",
            psk="psk_key_name",
            dhchap_key=None,
            dhchap_ctrlr_key=None,
            dhchap_group="null",
        )


class TestRecreateSubsystemSecurity(unittest.TestCase):
    """Verify that recreate_lvstore* passes allow_any_host and re-applies hosts."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops._reapply_allowed_hosts")
    @patch("simplyblock_core.storage_node_ops.add_lvol_thread")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_recreate_lvstore_on_non_leader_passes_allow_any_false(
            self, MockDB, MockRPC, mock_tasks, mock_bdev_stack,
            MockFW, mock_tcp_events, mock_add_thread, mock_reapply, _mock_disc, _mock_phase, _mock_handle):
        """recreate_lvstore_on_non_leader sets allow_any_host=False for lvols with allowed_hosts."""
        dhchap_host = {"nqn": "nqn:secured", "dhchap_key": "DHHC-1:01:x:"}

        sec_node = _node("sec-1")
        sec_node.cluster_id = "c1"
        sec_node.api_endpoint = "127.0.0.1:5000"

        primary_node = _node("pri-1")
        primary_node.cluster_id = "c1"
        primary_node.lvstore_status = "ready"
        primary_node.lvstore_stack = []
        primary_node.secondary_node_id = sec_node.uuid
        primary_node.tertiary_node_id = ""
        primary_node.active_rdma = False
        primary_node.jm_vuid = "jm1"
        primary_node.raid = "raid0"
        primary_node.lvol_subsys_port = 4420
        primary_node.rpc_port = 5260
        primary_node.status = StorageNode.STATUS_ONLINE

        lvol_secured = _lvol("lvol-s", allowed_hosts=[dhchap_host],
                             nodes=[primary_node.uuid, sec_node.uuid])
        lvol_secured.lvs_name = "LVS_1"
        lvol_secured.lvol_bdev = "LVOL_1"

        lvol_open = _lvol("lvol-o", allowed_hosts=[],
                          nodes=[primary_node.uuid, sec_node.uuid])
        lvol_open.lvs_name = "LVS_1"
        lvol_open.lvol_bdev = "LVOL_2"

        mock_db = MagicMock()
        mock_db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary_node]
        mock_db.get_lvols_by_node_id.return_value = [lvol_secured, lvol_open]
        MockDB.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_create.return_value = True
        mock_rpc_inst.bdev_examine.return_value = True
        mock_rpc_inst.bdev_wait_for_examine.return_value = True
        MockRPC.return_value = mock_rpc_inst

        # Mock secondary_node.rpc_client() for jc_suspend_compression
        sec_node_rpc = MagicMock()
        sec_node_rpc.jc_suspend_compression.return_value = (True, None)
        with patch.object(sec_node, 'rpc_client', return_value=sec_node_rpc):
            with patch.object(sec_node, 'connect_to_hublvol'):
                with patch.object(primary_node, 'write_to_db'):
                    mock_bdev_stack.return_value = (True, None)
                    mock_fw_inst = MagicMock()
                    MockFW.return_value = mock_fw_inst

                    snode_ops.recreate_lvstore_on_non_leader(sec_node, leader_node=primary_node, primary_node=primary_node)

        # Verify subsystem_create calls
        create_calls = mock_rpc_inst.subsystem_create.call_args_list
        self.assertEqual(len(create_calls), 2)

        # First call: secured lvol -> allow_any_host=False
        _, kwargs1 = create_calls[0]
        self.assertFalse(kwargs1.get("allow_any_host", True))

        # Second call: open lvol -> allow_any_host=True
        _, kwargs2 = create_calls[1]
        self.assertTrue(kwargs2.get("allow_any_host", False))

        # _reapply_allowed_hosts called only for the secured lvol
        mock_reapply.assert_called_once_with(lvol_secured, sec_node, mock_rpc_inst)

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    @patch("simplyblock_core.storage_node_ops._reapply_allowed_hosts")
    def test_recreate_lvol_on_node_reapplies_hosts(self, mock_reapply, MockDBCtrl):
        """recreate_lvol_on_node (in lvol_controller) re-applies allowed hosts."""
        from simplyblock_core.controllers.lvol_controller import recreate_lvol_on_node

        cl = _cluster(tls=True, tls_config={"params": {"dhchap_dhgroups": ["ffdhe2048"]}})
        mock_db = MagicMock()
        mock_db.get_cluster_by_id.return_value = cl
        mock_db.get_pool_by_id.side_effect = KeyError("no pool")  # non-DHCHAP path
        MockDBCtrl.return_value = mock_db

        dhchap_host = {"nqn": "nqn:host", "dhchap_key": "DHHC-1:01:k:"}
        node = _node()
        node.api_endpoint = "127.0.0.1:5000"
        lvol = _lvol(allowed_hosts=[dhchap_host])
        lvol.lvs_name = "LVS_1"
        lvol.lvol_bdev = "LVOL_1"
        lvol.top_bdev = "LVS_1/LVOL_1"
        lvol.guid = "abcd1234"
        lvol.bdev_stack = []
        lvol.lvol_type = ""
        lvol.crypto_bdev = ""

        mock_rpc = MagicMock()
        mock_rpc.subsystem_create.return_value = True
        mock_rpc.nvmf_subsystem_add_ns.return_value = 1
        mock_rpc.nvmf_subsystem_add_listener.return_value = (True, None)
        mock_rpc.ultra21_util_get_malloc_stats.return_value = {}
        mock_rpc.get_bdevs.return_value = [{"uuid": "u1", "driver_specific": {}}]

        with patch("simplyblock_core.controllers.lvol_controller.RPCClient",
                    return_value=mock_rpc):
            with patch("simplyblock_core.controllers.lvol_controller._register_dhchap_keys_on_node",
                        return_value={"dhchap_key": "kn_dhchap"}) as mock_reg:
                recreate_lvol_on_node(lvol, node)

        # subsystem_create should use allow_any_host=False
        mock_rpc.subsystem_create.assert_called_once()
        _, kwargs = mock_rpc.subsystem_create.call_args
        self.assertFalse(kwargs.get("allow_any_host", True))

        # Host re-applied with keyring key names and dhchap_group
        mock_reg.assert_called_once()
        mock_rpc.subsystem_add_host.assert_called_once()
        add_call = mock_rpc.subsystem_add_host.call_args
        self.assertEqual(add_call[0][1], "nqn:host")
        self.assertEqual(add_call[1].get("dhchap_key"), "kn_dhchap")
        self.assertEqual(add_call[1].get("dhchap_group"), "ffdhe2048")


class TestRemoveHostKeyringCleanup(unittest.TestCase):
    """Verify remove_host_from_lvol cleans up keyring keys."""

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_cleans_keyring(self, MockDBCtrl, MockRPC):
        node = _node()
        from simplyblock_core.controllers.lvol_controller import remove_host_from_lvol
        lvol = _lvol(
            allowed_hosts=[{
                "nqn": "nqn:host1",
                "dhchap_key": "DHHC-1:01:abc:",
                "dhchap_ctrlr_key": "DHHC-1:01:def:",
            }],
            nodes=[node.uuid],
        )

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.kv_store = MagicMock()
        MockDBCtrl.return_value = mock_db

        mock_rpc_inst = MagicMock()
        mock_rpc_inst.subsystem_remove_host.return_value = True
        mock_rpc_inst.keyring_file_remove_key.return_value = True
        MockRPC.return_value = mock_rpc_inst

        result, err = remove_host_from_lvol("lvol-1", "nqn:host1")
        self.assertTrue(result)

        # Verify keyring cleanup was called for both key types
        remove_calls = mock_rpc_inst.keyring_file_remove_key.call_args_list
        key_names = [c[0][0] for c in remove_calls]
        safe = "nqn_host1"
        self.assertIn(f"dhchap_key_{safe}", key_names)
        self.assertIn(f"dhchap_ctrlr_key_{safe}", key_names)

    @patch("simplyblock_core.controllers.lvol_controller.RPCClient")
    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_remove_host_succeeds_even_if_spdk_fails(self, MockDBCtrl, MockRPC):
        """DB is updated even if SPDK remove_host returns error (host already gone)."""
        from simplyblock_core.controllers.lvol_controller import remove_host_from_lvol
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
        mock_rpc_inst.subsystem_remove_host.return_value = False  # SPDK error
        MockRPC.return_value = mock_rpc_inst

        result, err = remove_host_from_lvol("lvol-1", "nqn:host1")
        # Should still succeed (DB updated) but with a warning
        self.assertTrue(result)
        self.assertIn("Warning", err)
        # allowed_hosts should be empty now
        self.assertEqual(len(lvol.allowed_hosts), 0)


if __name__ == "__main__":
    unittest.main()
