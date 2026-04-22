# coding=utf-8
"""
test_dhchap_e2e.py – end-to-end tests for DHCHAP key registration flow.

Uses a real SNodeAPI (running locally) and a mock SPDK JSON-RPC server.
The test exercises the full path:

  1. generate_dhchap_key() produces DHHC-1:01:...: keys
  2. SNodeClient.write_key_file() writes key to storage node via SNodeAPI
  3. RPCClient.keyring_file_add_key() registers file in SPDK keyring
  4. RPCClient.subsystem_add_host() references key by name
  5. connect_lvol() produces correct --dhchap-secret / --dhchap-ctrl-secret flags

Requires: SNodeAPI running on localhost:5000 (started by conftest or manually).
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

import requests

# ---------------------------------------------------------------------------
# Mock SPDK JSON-RPC server
# ---------------------------------------------------------------------------

_keyring_keys = {}  # name -> path
_subsystem_hosts = {}  # nqn -> [host_entry, ...]
_dhchap_tmpdir = None  # temp dir for key files, set by _start_snode_api()


class MockSPDKHandler(BaseHTTPRequestHandler):
    """Handles SPDK JSON-RPC 2.0 requests."""

    def log_message(self, format, *args):
        pass  # silence logs

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        method = body.get("method")
        params = body.get("params", {})
        req_id = body.get("id", 1)

        result = self._dispatch(method, params)
        resp = {"jsonrpc": "2.0", "id": req_id}
        if isinstance(result, dict) and "error" in result:
            resp["error"] = result["error"]
        else:
            resp["result"] = result

        payload = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _dispatch(self, method, params):
        if method == "keyring_file_add_key":
            name = params.get("name")
            path = params.get("path")
            if not name or not path:
                return {"error": {"code": -32602, "message": "Invalid parameters"}}
            if not os.path.isfile(path):
                return {"error": {"code": -1, "message": f"File not found: {path}"}}
            _keyring_keys[name] = path
            return True

        if method == "keyring_file_remove_key":
            name = params.get("name")
            _keyring_keys.pop(name, None)
            return True

        if method == "nvmf_create_subsystem":
            nqn = params.get("nqn")
            _subsystem_hosts.setdefault(nqn, [])
            return True

        if method == "nvmf_subsystem_add_host":
            nqn = params.get("nqn")
            host = params.get("host")
            dhchap_key = params.get("dhchap_key")
            dhchap_ctrlr_key = params.get("dhchap_ctrlr_key")
            psk = params.get("psk")

            # Validate that key names reference registered keys
            for key_name in (dhchap_key, dhchap_ctrlr_key, psk):
                if key_name and key_name not in _keyring_keys:
                    return {"error": {"code": -32602,
                                      "message": f"Unable to find key: {key_name}"}}

            _subsystem_hosts.setdefault(nqn, []).append({
                "host": host,
                "dhchap_key": dhchap_key,
                "dhchap_ctrlr_key": dhchap_ctrlr_key,
                "psk": psk,
            })
            return True

        if method == "nvmf_subsystem_remove_host":
            nqn = params.get("nqn")
            host = params.get("host")
            hosts = _subsystem_hosts.get(nqn, [])
            _subsystem_hosts[nqn] = [h for h in hosts if h["host"] != host]
            return True

        if method == "nvmf_get_subsystems":
            return [{"nqn": nqn, "hosts": hosts}
                    for nqn, hosts in _subsystem_hosts.items()]

        return True  # default success for unknown methods


def _start_mock_spdk(port):
    server = HTTPServer(("127.0.0.1", port), MockSPDKHandler)
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


# ---------------------------------------------------------------------------
# SNodeAPI management
# ---------------------------------------------------------------------------

SNODE_API_PORT = 15123  # use non-standard port to avoid conflicts
MOCK_SPDK_PORT = 15124


def _wait_for_http(url, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(0.3)
    return False


def _start_snode_api():
    """Start the SNodeAPI as a subprocess."""
    global _dhchap_tmpdir
    _dhchap_tmpdir = tempfile.mkdtemp(prefix="dhchap_keys_")
    env = os.environ.copy()
    env["WITHOUT_CLOUD_INFO"] = "True"
    env["SIMPLYBLOCK_LOG_LEVEL"] = "ERROR"
    env["FLASK_RUN_PORT"] = str(SNODE_API_PORT)
    env["DHCHAP_KEY_DIR"] = _dhchap_tmpdir
    env["LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", "/home/michael")
    proc = subprocess.Popen(
        [sys.executable, "-c",
         f"""
from flask_openapi3 import OpenAPI
from simplyblock_web.api.internal.storage_node.docker import api
app = OpenAPI(__name__)
app.url_map.strict_slashes = False
app.register_api(api)
app.run(host='127.0.0.1', port={SNODE_API_PORT}, debug=False)
"""],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if not _wait_for_http(f"http://127.0.0.1:{SNODE_API_PORT}/snode/check"):
        proc.kill()
        stdout = proc.stdout.read().decode() if proc.stdout else ""
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(
            f"SNodeAPI failed to start on port {SNODE_API_PORT}.\n"
            f"stdout: {stdout}\nstderr: {stderr}")
    return proc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDHCHAPE2E(unittest.TestCase):

    snode_proc = None
    mock_spdk = None

    @classmethod
    def setUpClass(cls):
        _keyring_keys.clear()
        _subsystem_hosts.clear()
        cls.mock_spdk = _start_mock_spdk(MOCK_SPDK_PORT)
        cls.snode_proc = _start_snode_api()

    @classmethod
    def tearDownClass(cls):
        if cls.snode_proc:
            cls.snode_proc.terminate()
            cls.snode_proc.wait(timeout=5)
        if cls.mock_spdk:
            cls.mock_spdk.shutdown()
        import shutil
        if _dhchap_tmpdir and os.path.isdir(_dhchap_tmpdir):
            shutil.rmtree(_dhchap_tmpdir, ignore_errors=True)

    def setUp(self):
        _keyring_keys.clear()
        _subsystem_hosts.clear()

    # -- Key generation --

    def test_generate_dhchap_key_format(self):
        from simplyblock_core.utils import generate_dhchap_key
        key = generate_dhchap_key()
        self.assertTrue(key.startswith("DHHC-1:01:"))
        self.assertTrue(key.endswith(":"))
        # base64 payload between the colons
        import base64
        payload = key.split(":")[2]
        raw = base64.b64decode(payload)
        # 32 bytes key + 4 bytes CRC32
        self.assertEqual(len(raw), 36)

    def test_generate_dhchap_key_crc_valid(self):
        import base64
        import struct
        import zlib
        from simplyblock_core.utils import generate_dhchap_key
        key = generate_dhchap_key()
        payload = key.split(":")[2]
        raw = base64.b64decode(payload)
        key_bytes = raw[:32]
        stored_crc = struct.unpack('<I', raw[32:])[0]
        computed_crc = zlib.crc32(key_bytes) & 0xFFFFFFFF
        self.assertEqual(stored_crc, computed_crc)

    # -- SNodeAPI write_key_file --

    def test_write_key_file_via_snode_api(self):
        from simplyblock_core.snode_client import SNodeClient
        client = SNodeClient(f"127.0.0.1:{SNODE_API_PORT}")
        result, error = client.write_key_file("test_key_1", "DHHC-1:01:dGVzdA==:")
        self.assertIsNone(error)
        self.assertIn("test_key_1", result)
        # Verify file exists and has correct content
        self.assertTrue(os.path.isfile(result))
        with open(result) as f:
            self.assertEqual(f.read(), "DHHC-1:01:dGVzdA==:")
        # Verify permissions are 0600
        mode = os.stat(result).st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_write_key_file_invalid_name_rejected(self):
        from simplyblock_core.snode_client import SNodeClient
        client = SNodeClient(f"127.0.0.1:{SNODE_API_PORT}")
        result, error = client.write_key_file("../evil", "DHHC-1:01:dGVzdA==:")
        self.assertIsNotNone(error)

    # -- Mock SPDK keyring --

    def test_keyring_file_add_key_via_rpc(self):
        """Write key file via SNodeAPI, then register in mock SPDK keyring."""
        from simplyblock_core.snode_client import SNodeClient
        from simplyblock_core.rpc_client import RPCClient

        snode_client = SNodeClient(f"127.0.0.1:{SNODE_API_PORT}")
        rpc_client = RPCClient("127.0.0.1", MOCK_SPDK_PORT, "", "")

        # Write key file
        key_content = "DHHC-1:01:dGVzdGtleQ==:"
        result, error = snode_client.write_key_file("my_dhchap_key", key_content)
        self.assertIsNone(error)
        key_path = result

        # Register in SPDK keyring
        ret = rpc_client.keyring_file_add_key("my_dhchap_key", key_path)
        self.assertTrue(ret)
        self.assertIn("my_dhchap_key", _keyring_keys)
        self.assertEqual(_keyring_keys["my_dhchap_key"], key_path)

    def test_subsystem_add_host_with_keyring_names(self):
        """Full flow: write keys, register in keyring, add host to subsystem."""
        from simplyblock_core.snode_client import SNodeClient
        from simplyblock_core.rpc_client import RPCClient
        from simplyblock_core.utils import generate_dhchap_key

        snode_client = SNodeClient(f"127.0.0.1:{SNODE_API_PORT}")
        rpc_client = RPCClient("127.0.0.1", MOCK_SPDK_PORT, "", "")

        host_nqn = "nqn.2014-08.org.nvmexpress:uuid:test-host-1"
        subsys_nqn = "nqn.2023-02.io.simplyblock:test:subsys1"

        # Generate keys
        dhchap_key = generate_dhchap_key()
        dhchap_ctrlr_key = generate_dhchap_key()

        # Create subsystem
        rpc_client._request("nvmf_create_subsystem", {"nqn": subsys_nqn})

        # Write key files
        safe_host = host_nqn.replace(":", "_").replace(".", "_")
        key_name = f"dhchap_{safe_host}"
        ctrlr_key_name = f"dhchap_ctrlr_{safe_host}"

        result, err = snode_client.write_key_file(key_name, dhchap_key)
        self.assertIsNone(err)
        dhchap_path = result

        result, err = snode_client.write_key_file(ctrlr_key_name, dhchap_ctrlr_key)
        self.assertIsNone(err)
        ctrlr_path = result

        # Register in keyring
        self.assertTrue(rpc_client.keyring_file_add_key(key_name, dhchap_path))
        self.assertTrue(rpc_client.keyring_file_add_key(ctrlr_key_name, ctrlr_path))

        # Add host with key names (not values)
        ret = rpc_client.subsystem_add_host(
            subsys_nqn, host_nqn,
            dhchap_key=key_name,
            dhchap_ctrlr_key=ctrlr_key_name,
        )
        self.assertTrue(ret)

        # Verify mock SPDK state
        hosts = _subsystem_hosts[subsys_nqn]
        self.assertEqual(len(hosts), 1)
        self.assertEqual(hosts[0]["host"], host_nqn)
        self.assertEqual(hosts[0]["dhchap_key"], key_name)
        self.assertEqual(hosts[0]["dhchap_ctrlr_key"], ctrlr_key_name)

    def test_subsystem_add_host_fails_without_keyring_registration(self):
        """SPDK rejects add_host when dhchap_key name is not in the keyring."""
        from simplyblock_core.rpc_client import RPCClient
        rpc_client = RPCClient("127.0.0.1", MOCK_SPDK_PORT, "", "")

        subsys_nqn = "nqn:test:subsys2"
        rpc_client._request("nvmf_create_subsystem", {"nqn": subsys_nqn})

        # Try to add host with unregistered key name
        ret = rpc_client.subsystem_add_host(
            subsys_nqn, "nqn:host",
            dhchap_key="nonexistent_key",
        )
        self.assertFalse(ret)

    # -- _register_dhchap_keys_on_node helper --

    def test_register_dhchap_keys_on_node_full_flow(self):
        """Test the controller helper that orchestrates SNodeAPI + keyring."""
        from simplyblock_core.controllers.lvol_controller import _register_dhchap_keys_on_node
        from simplyblock_core.rpc_client import RPCClient
        from simplyblock_core.utils import generate_dhchap_key
        from simplyblock_core.models.storage_node import StorageNode

        snode = StorageNode()
        snode.api_endpoint = f"127.0.0.1:{SNODE_API_PORT}"
        snode.mgmt_ip = "127.0.0.1"
        snode.rpc_port = MOCK_SPDK_PORT
        snode.rpc_username = ""
        snode.rpc_password = ""

        rpc_client = RPCClient("127.0.0.1", MOCK_SPDK_PORT, "", "")

        host_nqn = "nqn.2014-08.org.nvmexpress:uuid:e2e-host"
        host_entry = {
            "nqn": host_nqn,
            "dhchap_key": generate_dhchap_key(),
            "dhchap_ctrlr_key": generate_dhchap_key(),
        }

        key_names = _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client)

        self.assertIn("dhchap_key", key_names)
        self.assertIn("dhchap_ctrlr_key", key_names)

        # Verify keys are in the mock SPDK keyring
        self.assertIn(key_names["dhchap_key"], _keyring_keys)
        self.assertIn(key_names["dhchap_ctrlr_key"], _keyring_keys)

        # Verify key files contain the correct DHHC-1 content
        with open(_keyring_keys[key_names["dhchap_key"]]) as f:
            content = f.read()
            self.assertTrue(content.startswith("DHHC-1:01:"))
            self.assertEqual(content, host_entry["dhchap_key"])

    # -- connect_lvol output --

    def test_connect_lvol_includes_dhchap_secrets(self):
        """connect_lvol with host_nqn produces --dhchap-secret flags."""
        from unittest.mock import MagicMock, patch
        from simplyblock_core.controllers import lvol_controller as lvol_ctl
        from simplyblock_core.utils import generate_dhchap_key
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.models.storage_node import StorageNode
        from simplyblock_core.models.cluster import Cluster

        dhchap_key = generate_dhchap_key()
        dhchap_ctrlr_key = generate_dhchap_key()
        host_nqn = "nqn.2014-08.org.nvmexpress:uuid:connect-test"

        cl = Cluster()
        cl.uuid = "cluster-1"
        cl.tls = False
        cl.client_qpair_count = 3
        cl.client_data_nic = ""

        node = StorageNode()
        node.uuid = "node-1"
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]

        lvol = LVol()
        lvol.uuid = "lvol-1"
        lvol.node_id = "node-1"
        lvol.nqn = "nqn:test:lvol-1"
        lvol.allowed_hosts = [{
            "nqn": host_nqn,
            "dhchap_key": dhchap_key,
            "dhchap_ctrlr_key": dhchap_ctrlr_key,
        }]
        lvol.nodes = ["node-1"]
        lvol.subsys_port = 9090
        lvol.ns_id = 1
        lvol.ha_type = "single"
        lvol.fabric = "tcp"

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl

        with patch("simplyblock_core.controllers.lvol_controller.DBController",
                    return_value=mock_db):
            result = lvol_ctl.connect_lvol("lvol-1", host_nqn=host_nqn)

        self.assertTrue(len(result) > 0)
        cmd = result[0]["connect"]
        self.assertIn(f"--hostnqn={host_nqn}", cmd)
        self.assertIn(f"--dhchap-secret={dhchap_key}", cmd)
        self.assertIn(f"--dhchap-ctrl-secret={dhchap_ctrlr_key}", cmd)
        # No --tls since cluster.tls=False and no psk
        self.assertNotIn("--tls", cmd)

    def test_connect_lvol_tls_only_with_psk(self):
        """connect_lvol adds --tls when host entry has psk."""
        from unittest.mock import MagicMock, patch
        from simplyblock_core.controllers import lvol_controller as lvol_ctl
        from simplyblock_core.utils import generate_psk_key
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.models.storage_node import StorageNode
        from simplyblock_core.models.cluster import Cluster

        psk = generate_psk_key()
        host_nqn = "nqn:psk-host"

        cl = Cluster()
        cl.uuid = "cluster-1"
        cl.tls = False
        cl.client_qpair_count = 3
        cl.client_data_nic = ""

        node = StorageNode()
        node.uuid = "node-1"
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]

        lvol = LVol()
        lvol.uuid = "lvol-1"
        lvol.node_id = "node-1"
        lvol.nqn = "nqn:test:lvol-psk"
        lvol.allowed_hosts = [{"nqn": host_nqn, "psk": psk}]
        lvol.nodes = ["node-1"]
        lvol.subsys_port = 9090
        lvol.ns_id = 1
        lvol.ha_type = "single"
        lvol.fabric = "tcp"

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl

        with patch("simplyblock_core.controllers.lvol_controller.DBController",
                    return_value=mock_db):
            result = lvol_ctl.connect_lvol("lvol-1", host_nqn=host_nqn)

        cmd = result[0]["connect"]
        self.assertIn("--tls", cmd)
        self.assertIn(f"--hostnqn={host_nqn}", cmd)
        self.assertTrue(result[0].get("tls"))

    def test_connect_lvol_without_host_nqn_is_rejected_when_acl_exists(self):
        """connect_lvol requires host_nqn when allowed_hosts are configured."""
        from unittest.mock import MagicMock, patch
        from simplyblock_core.controllers import lvol_controller as lvol_ctl
        from simplyblock_core.utils import generate_dhchap_key
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.models.storage_node import StorageNode
        from simplyblock_core.models.cluster import Cluster

        cl = Cluster()
        cl.uuid = "cluster-1"
        cl.tls = False
        cl.client_qpair_count = 3
        cl.client_data_nic = ""

        node = StorageNode()
        node.uuid = "node-1"
        node.cluster_id = cl.uuid
        nic = MagicMock()
        nic.ip4_address = "10.0.0.1"
        nic.trtype = "TCP"
        node.data_nics = [nic]

        lvol = LVol()
        lvol.uuid = "lvol-1"
        lvol.node_id = "node-1"
        lvol.nqn = "nqn:test:lvol-nosec"
        lvol.allowed_hosts = [{
            "nqn": "nqn:some-host",
            "dhchap_key": generate_dhchap_key(),
        }]
        lvol.nodes = ["node-1"]
        lvol.subsys_port = 9090
        lvol.ns_id = 1
        lvol.ha_type = "single"
        lvol.fabric = "tcp"

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_cluster_by_id.return_value = cl

        with patch("simplyblock_core.controllers.lvol_controller.DBController",
                    return_value=mock_db):
            result = lvol_ctl.connect_lvol("lvol-1")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
