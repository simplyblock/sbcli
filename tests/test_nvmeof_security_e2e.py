# coding=utf-8
"""
test_nvmeof_security_e2e.py – end-to-end tests for NVMe-oF TLS host access
control using mock RPC servers.

Tests cover:
  - Mock RPC endpoints: nvmf_subsystem_add_host, nvmf_subsystem_remove_host,
    bdev_nvme_set_options
  - Error injection on new mock RPCs (timeout + error responses)
  - Full flow: create subsystem → add hosts → verify → remove hosts → verify
  - get_host_secret controller function
  - bdev_nvme_set_options with TLS parameters
"""

import json
import random
import unittest
import uuid as _uuid_mod

from tests.migration.mock_rpc_server import MockRpcServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rpc_call(srv: MockRpcServer, method: str, params: dict = None):
    """Make a raw JSON-RPC 2.0 call to a MockRpcServer and return the result."""
    import http.client
    body = json.dumps({
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": 1,
    })
    conn = http.client.HTTPConnection(srv.host, srv.port, timeout=10)
    conn.request("POST", "/", body, {"Content-Type": "application/json"})
    resp = conn.getresponse()
    data = json.loads(resp.read())
    conn.close()
    if "error" in data:
        raise RpcError(data["error"]["code"], data["error"]["message"])
    return data.get("result")


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# A single mock server for all tests in this module (module-scoped)
# ---------------------------------------------------------------------------

_server = None


def setUpModule():
    global _server
    _server = MockRpcServer(
        host="127.0.0.1", port=9950, lvstore="lvs_test", node_id="sec-test",
    )
    _server.start()


def tearDownModule():
    global _server
    if _server:
        _server.stop()
        _server = None


# ---------------------------------------------------------------------------
# Test: Mock RPC endpoints
# ---------------------------------------------------------------------------

class TestMockSubsystemAddHost(unittest.TestCase):
    """Verify the nvmf_subsystem_add_host mock implementation."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)
        # Create a subsystem first
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:test-subsys",
            "serial_number": "SN001",
            "model_number": "MD001",
            "allow_any_host": False,
        })

    def test_add_host_basic(self):
        result = _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:test-subsys",
            "host": "nqn:host-1",
        })
        self.assertTrue(result)
        # Verify in state
        sub = _server.state.subsystems["nqn:test-subsys"]
        self.assertEqual(len(sub["hosts"]), 1)
        self.assertEqual(sub["hosts"][0]["nqn"], "nqn:host-1")

    def test_add_host_with_psk(self):
        result = _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:test-subsys",
            "host": "nqn:host-psk",
            "psk": "abcdef0123456789" * 4,
        })
        self.assertTrue(result)
        sub = _server.state.subsystems["nqn:test-subsys"]
        self.assertEqual(sub["hosts"][0]["psk"], "abcdef0123456789" * 4)

    def test_add_host_with_dhchap(self):
        result = _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:test-subsys",
            "host": "nqn:host-dhchap",
            "dhchap_key": "base64key==",
            "dhchap_ctrlr_key": "base64ctrlrkey==",
        })
        self.assertTrue(result)
        host = _server.state.subsystems["nqn:test-subsys"]["hosts"][0]
        self.assertEqual(host["dhchap_key"], "base64key==")
        self.assertEqual(host["dhchap_ctrlr_key"], "base64ctrlrkey==")

    def test_add_host_with_all_security(self):
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:test-subsys",
            "host": "nqn:host-all",
            "psk": "pskvalue",
            "dhchap_key": "dhchapvalue",
            "dhchap_ctrlr_key": "ctrlrvalue",
        })
        host = _server.state.subsystems["nqn:test-subsys"]["hosts"][0]
        self.assertEqual(host["psk"], "pskvalue")
        self.assertEqual(host["dhchap_key"], "dhchapvalue")
        self.assertEqual(host["dhchap_ctrlr_key"], "ctrlrvalue")

    def test_add_duplicate_host_rejected(self):
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:test-subsys", "host": "nqn:dup-host",
        })
        with self.assertRaises(RpcError) as ctx:
            _rpc_call(_server, "nvmf_subsystem_add_host", {
                "nqn": "nqn:test-subsys", "host": "nqn:dup-host",
            })
        self.assertEqual(ctx.exception.code, -17)

    def test_add_host_subsystem_not_found(self):
        with self.assertRaises(RpcError) as ctx:
            _rpc_call(_server, "nvmf_subsystem_add_host", {
                "nqn": "nqn:nonexistent", "host": "nqn:host",
            })
        self.assertEqual(ctx.exception.code, -2)

    def test_add_multiple_hosts(self):
        for i in range(5):
            _rpc_call(_server, "nvmf_subsystem_add_host", {
                "nqn": "nqn:test-subsys", "host": f"nqn:host-{i}",
            })
        sub = _server.state.subsystems["nqn:test-subsys"]
        self.assertEqual(len(sub["hosts"]), 5)
        nqns = {h["nqn"] for h in sub["hosts"]}
        for i in range(5):
            self.assertIn(f"nqn:host-{i}", nqns)


class TestMockSubsystemRemoveHost(unittest.TestCase):
    """Verify the nvmf_subsystem_remove_host mock implementation."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:rm-subsys",
            "serial_number": "SN002",
            "model_number": "MD002",
            "allow_any_host": False,
        })
        for i in range(3):
            _rpc_call(_server, "nvmf_subsystem_add_host", {
                "nqn": "nqn:rm-subsys", "host": f"nqn:host-{i}",
            })

    def test_remove_host_basic(self):
        result = _rpc_call(_server, "nvmf_subsystem_remove_host", {
            "nqn": "nqn:rm-subsys", "host": "nqn:host-1",
        })
        self.assertTrue(result)
        sub = _server.state.subsystems["nqn:rm-subsys"]
        self.assertEqual(len(sub["hosts"]), 2)
        nqns = {h["nqn"] for h in sub["hosts"]}
        self.assertNotIn("nqn:host-1", nqns)
        self.assertIn("nqn:host-0", nqns)
        self.assertIn("nqn:host-2", nqns)

    def test_remove_nonexistent_host(self):
        with self.assertRaises(RpcError) as ctx:
            _rpc_call(_server, "nvmf_subsystem_remove_host", {
                "nqn": "nqn:rm-subsys", "host": "nqn:not-there",
            })
        self.assertEqual(ctx.exception.code, -2)

    def test_remove_host_subsystem_not_found(self):
        with self.assertRaises(RpcError) as ctx:
            _rpc_call(_server, "nvmf_subsystem_remove_host", {
                "nqn": "nqn:nonexistent", "host": "nqn:host-0",
            })
        self.assertEqual(ctx.exception.code, -2)

    def test_remove_all_hosts(self):
        for i in range(3):
            _rpc_call(_server, "nvmf_subsystem_remove_host", {
                "nqn": "nqn:rm-subsys", "host": f"nqn:host-{i}",
            })
        sub = _server.state.subsystems["nqn:rm-subsys"]
        self.assertEqual(len(sub["hosts"]), 0)


class TestMockBdevNvmeSetOptions(unittest.TestCase):
    """Verify the bdev_nvme_set_options mock implementation."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_set_options_no_tls(self):
        result = _rpc_call(_server, "bdev_nvme_set_options", {})
        self.assertTrue(result)
        self.assertIsNone(_server.state.nvme_options.get("dhchap_digests"))
        self.assertIsNone(_server.state.nvme_options.get("dhchap_dhgroups"))

    def test_set_options_with_digests(self):
        result = _rpc_call(_server, "bdev_nvme_set_options", {
            "dhchap_digests": ["sha256", "sha384"],
        })
        self.assertTrue(result)
        self.assertEqual(
            _server.state.nvme_options["dhchap_digests"],
            ["sha256", "sha384"],
        )

    def test_set_options_with_dhgroups(self):
        result = _rpc_call(_server, "bdev_nvme_set_options", {
            "dhchap_dhgroups": ["ffdhe2048", "ffdhe4096"],
        })
        self.assertTrue(result)
        self.assertEqual(
            _server.state.nvme_options["dhchap_dhgroups"],
            ["ffdhe2048", "ffdhe4096"],
        )

    def test_set_options_with_all_tls(self):
        result = _rpc_call(_server, "bdev_nvme_set_options", {
            "dhchap_digests": ["sha512"],
            "dhchap_dhgroups": ["null", "ffdhe8192"],
        })
        self.assertTrue(result)
        self.assertEqual(
            _server.state.nvme_options["dhchap_digests"], ["sha512"])
        self.assertEqual(
            _server.state.nvme_options["dhchap_dhgroups"],
            ["null", "ffdhe8192"],
        )


class TestMockSubsystemAllowAnyHost(unittest.TestCase):
    """Verify allow_any_host parameter on subsystem creation."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_default_allow_any_host_true(self):
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:any-host",
            "serial_number": "SN",
            "model_number": "MD",
        })
        sub = _server.state.subsystems["nqn:any-host"]
        self.assertTrue(sub["allow_any_host"])

    def test_allow_any_host_false(self):
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:restricted",
            "serial_number": "SN",
            "model_number": "MD",
            "allow_any_host": False,
        })
        sub = _server.state.subsystems["nqn:restricted"]
        self.assertFalse(sub["allow_any_host"])


# ---------------------------------------------------------------------------
# Test: Error injection on new RPCs
# ---------------------------------------------------------------------------

class TestErrorInjection(unittest.TestCase):
    """Verify random error injection works for the new RPC methods."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_add_host_error_injection(self):
        """With 100% error rate, add_host should fail every time."""
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:err-subsys",
            "serial_number": "SN",
            "model_number": "MD",
        })
        _server.set_failure_rate(1.0, timeout_seconds=0.5)

        errors = 0
        for _ in range(5):
            try:
                _rpc_call(_server, "nvmf_subsystem_add_host", {
                    "nqn": "nqn:err-subsys",
                    "host": f"nqn:host-{_uuid_mod.uuid4().hex[:8]}",
                })
            except (RpcError, Exception):
                errors += 1
        # All 5 should fail (timeout or error)
        self.assertEqual(errors, 5)

    def test_remove_host_error_injection(self):
        """With 100% error rate, remove_host should fail every time."""
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:err-rm",
            "serial_number": "SN",
            "model_number": "MD",
        })
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:err-rm", "host": "nqn:host-0",
        })
        _server.set_failure_rate(1.0, timeout_seconds=0.5)

        errors = 0
        for _ in range(5):
            try:
                _rpc_call(_server, "nvmf_subsystem_remove_host", {
                    "nqn": "nqn:err-rm", "host": "nqn:host-0",
                })
            except (RpcError, Exception):
                errors += 1
        self.assertEqual(errors, 5)

    def test_bdev_nvme_set_options_error_injection(self):
        """With 100% error rate, bdev_nvme_set_options should fail."""
        _server.set_failure_rate(1.0, timeout_seconds=0.5)

        errors = 0
        for _ in range(5):
            try:
                _rpc_call(_server, "bdev_nvme_set_options", {
                    "dhchap_digests": ["sha256"],
                })
            except (RpcError, Exception):
                errors += 1
        self.assertEqual(errors, 5)

    def test_partial_error_rate(self):
        """With 50% error rate, some calls should succeed and some should fail."""
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:partial-err",
            "serial_number": "SN",
            "model_number": "MD",
        })
        _server.set_failure_rate(0.5, timeout_seconds=0.5)

        random.seed(42)
        successes = 0
        errors = 0
        for i in range(20):
            try:
                _rpc_call(_server, "nvmf_subsystem_add_host", {
                    "nqn": "nqn:partial-err",
                    "host": f"nqn:host-{_uuid_mod.uuid4().hex[:12]}",
                })
                successes += 1
            except (RpcError, Exception):
                errors += 1
        # With 50% rate over 20 tries, we expect both successes and failures
        self.assertGreater(successes, 0, "Expected at least some successes")
        self.assertGreater(errors, 0, "Expected at least some errors")


# ---------------------------------------------------------------------------
# Test: End-to-end flow (subsystem + hosts lifecycle)
# ---------------------------------------------------------------------------

class TestE2EHostLifecycle(unittest.TestCase):
    """Full flow: create subsystem → add hosts with security → remove hosts."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_full_host_lifecycle(self):
        # 1. Create a restricted subsystem
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:lifecycle",
            "serial_number": "SN-LC",
            "model_number": "MD-LC",
            "allow_any_host": False,
        })
        sub = _server.state.subsystems["nqn:lifecycle"]
        self.assertFalse(sub["allow_any_host"])
        self.assertEqual(len(sub["hosts"]), 0)

        # 2. Add first host with PSK
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-1",
            "psk": "aabbccdd" * 8,
        })
        self.assertEqual(len(sub["hosts"]), 1)
        self.assertEqual(sub["hosts"][0]["psk"], "aabbccdd" * 8)

        # 3. Add second host with DH-HMAC-CHAP
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-2",
            "dhchap_key": "dhchapkey123",
            "dhchap_ctrlr_key": "ctrlrkey456",
        })
        self.assertEqual(len(sub["hosts"]), 2)

        # 4. Add third host with no security
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-3",
        })
        self.assertEqual(len(sub["hosts"]), 3)

        # 5. Also add a namespace to the subsystem (lvol create + add_ns)
        _rpc_call(_server, "bdev_lvol_create", {
            "lvol_name": "vol1",
            "size_in_mib": 1024,
            "lvs_name": "lvs_test",
        })
        nsid = _rpc_call(_server, "nvmf_subsystem_add_ns", {
            "nqn": "nqn:lifecycle",
            "namespace": {"bdev_name": "lvs_test/vol1"},
        })
        self.assertEqual(nsid, 1)

        # 6. Verify full state
        subs = _rpc_call(_server, "nvmf_get_subsystems", {})
        self.assertEqual(len(subs), 1)
        self.assertEqual(len(subs[0]["hosts"]), 3)
        self.assertEqual(len(subs[0]["namespaces"]), 1)

        # 7. Remove host-2
        _rpc_call(_server, "nvmf_subsystem_remove_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-2",
        })
        self.assertEqual(len(sub["hosts"]), 2)
        nqns = {h["nqn"] for h in sub["hosts"]}
        self.assertNotIn("nqn:initiator-2", nqns)
        self.assertIn("nqn:initiator-1", nqns)
        self.assertIn("nqn:initiator-3", nqns)

        # 8. Remove remaining hosts
        _rpc_call(_server, "nvmf_subsystem_remove_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-1",
        })
        _rpc_call(_server, "nvmf_subsystem_remove_host", {
            "nqn": "nqn:lifecycle",
            "host": "nqn:initiator-3",
        })
        self.assertEqual(len(sub["hosts"]), 0)

        # 9. Delete subsystem
        _rpc_call(_server, "nvmf_delete_subsystem", {
            "nqn": "nqn:lifecycle",
        })
        self.assertNotIn("nqn:lifecycle", _server.state.subsystems)

    def test_create_volume_with_restricted_hosts(self):
        """Simulate the full lvol_controller flow: create vol → subsystem → add hosts."""
        # Create lvol
        bdev = _rpc_call(_server, "bdev_lvol_create", {
            "lvol_name": "secured-vol",
            "size_in_mib": 2048,
            "lvs_name": "lvs_test",
        })
        self.assertEqual(bdev, "lvs_test/secured-vol")

        # Create restricted subsystem
        nqn = "nqn.2023-02.io.simplyblock:secured-vol"
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": nqn,
            "serial_number": "ha",
            "model_number": str(_uuid_mod.uuid4()),
            "allow_any_host": False,
        })

        # Add namespace
        _rpc_call(_server, "nvmf_subsystem_add_ns", {
            "nqn": nqn,
            "namespace": {"bdev_name": "lvs_test/secured-vol"},
        })

        # Add allowed hosts with various security modes
        hosts = [
            {"host": "nqn:k8s-node-1", "psk": "abc123" * 10},
            {"host": "nqn:k8s-node-2", "dhchap_key": "dhkey1", "dhchap_ctrlr_key": "ctrlr1"},
            {"host": "nqn:k8s-node-3", "psk": "def456" * 10, "dhchap_key": "dhkey2"},
        ]
        for h in hosts:
            _rpc_call(_server, "nvmf_subsystem_add_host", {"nqn": nqn, **h})

        sub = _server.state.subsystems[nqn]
        self.assertEqual(len(sub["hosts"]), 3)
        self.assertFalse(sub["allow_any_host"])

        # Verify each host's security params
        host_map = {h["nqn"]: h for h in sub["hosts"]}
        self.assertEqual(host_map["nqn:k8s-node-1"]["psk"], "abc123" * 10)
        self.assertIn("dhchap_key", host_map["nqn:k8s-node-2"])
        self.assertIn("dhchap_ctrlr_key", host_map["nqn:k8s-node-2"])
        self.assertIn("psk", host_map["nqn:k8s-node-3"])
        self.assertIn("dhchap_key", host_map["nqn:k8s-node-3"])

    def test_bdev_nvme_set_options_then_subsystem(self):
        """Simulate storage_node_ops flow: set TLS options → create subsystem."""
        # Set TLS options (as storage_node_ops does on node join)
        _rpc_call(_server, "bdev_nvme_set_options", {
            "dhchap_digests": ["sha256", "sha512"],
            "dhchap_dhgroups": ["ffdhe2048", "ffdhe4096"],
        })
        self.assertEqual(
            _server.state.nvme_options["dhchap_digests"],
            ["sha256", "sha512"],
        )

        # Create a restricted subsystem
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:tls-vol",
            "serial_number": "HA",
            "model_number": "MODEL",
            "allow_any_host": False,
        })

        # Add a host with DH-HMAC-CHAP
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:tls-vol",
            "host": "nqn:secure-initiator",
            "dhchap_key": "DHCHAP_KEY_BASE64_VALUE",
        })

        sub = _server.state.subsystems["nqn:tls-vol"]
        self.assertEqual(len(sub["hosts"]), 1)
        self.assertEqual(sub["hosts"][0]["dhchap_key"], "DHCHAP_KEY_BASE64_VALUE")


# ---------------------------------------------------------------------------
# Test: get_host_secret controller function
# ---------------------------------------------------------------------------

class TestGetHostSecret(unittest.TestCase):
    """Test the get_host_secret controller function via mocking."""

    def test_get_secret_success(self):
        from unittest.mock import patch
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.controllers import lvol_controller

        lvol = LVol()
        lvol.uuid = "vol-secret-1"
        lvol.allowed_hosts = [
            {"nqn": "nqn:host-a", "psk": "psk-value-a", "dhchap_key": "dhkey-a"},
            {"nqn": "nqn:host-b", "psk": "psk-value-b"},
        ]

        with patch.object(lvol_controller, "DBController") as MockDB:
            MockDB.return_value.get_lvol_by_id.return_value = lvol

            result, err = lvol_controller.get_host_secret("vol-secret-1", "nqn:host-a")
            self.assertIsNone(err)
            self.assertEqual(result["nqn"], "nqn:host-a")
            self.assertEqual(result["psk"], "psk-value-a")
            self.assertEqual(result["dhchap_key"], "dhkey-a")

    def test_get_secret_host_not_found(self):
        from unittest.mock import patch
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.controllers import lvol_controller

        lvol = LVol()
        lvol.uuid = "vol-secret-2"
        lvol.allowed_hosts = [{"nqn": "nqn:host-x"}]

        with patch.object(lvol_controller, "DBController") as MockDB:
            MockDB.return_value.get_lvol_by_id.return_value = lvol

            result, err = lvol_controller.get_host_secret("vol-secret-2", "nqn:not-there")
            self.assertFalse(result)
            self.assertIn("not in the allowed list", err)

    def test_get_secret_volume_not_found(self):
        from unittest.mock import patch
        from simplyblock_core.controllers import lvol_controller

        with patch.object(lvol_controller, "DBController") as MockDB:
            MockDB.return_value.get_lvol_by_id.side_effect = KeyError("not found")

            result, err = lvol_controller.get_host_secret("no-such-vol", "nqn:host")
            self.assertFalse(result)
            self.assertIn("not found", err)

    def test_get_secret_empty_hosts(self):
        from unittest.mock import patch
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.controllers import lvol_controller

        lvol = LVol()
        lvol.uuid = "vol-empty"
        lvol.allowed_hosts = []

        with patch.object(lvol_controller, "DBController") as MockDB:
            MockDB.return_value.get_lvol_by_id.return_value = lvol

            result, err = lvol_controller.get_host_secret("vol-empty", "nqn:any-host")
            self.assertFalse(result)
            self.assertIn("not in the allowed list", err)

    def test_get_secret_returns_all_fields(self):
        from unittest.mock import patch
        from simplyblock_core.models.lvol_model import LVol
        from simplyblock_core.controllers import lvol_controller

        lvol = LVol()
        lvol.uuid = "vol-all-fields"
        lvol.allowed_hosts = [{
            "nqn": "nqn:full-host",
            "psk": "full-psk",
            "dhchap_key": "full-dhchap",
            "dhchap_ctrlr_key": "full-ctrlr",
        }]

        with patch.object(lvol_controller, "DBController") as MockDB:
            MockDB.return_value.get_lvol_by_id.return_value = lvol

            result, err = lvol_controller.get_host_secret("vol-all-fields", "nqn:full-host")
            self.assertIsNone(err)
            self.assertEqual(result["nqn"], "nqn:full-host")
            self.assertEqual(result["psk"], "full-psk")
            self.assertEqual(result["dhchap_key"], "full-dhchap")
            self.assertEqual(result["dhchap_ctrlr_key"], "full-ctrlr")


# ---------------------------------------------------------------------------
# Test: Multi-host concurrent add/remove stress
# ---------------------------------------------------------------------------

class TestHostStress(unittest.TestCase):
    """Stress test: add and remove many hosts."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_add_100_hosts_then_remove_all(self):
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:stress",
            "serial_number": "SN",
            "model_number": "MD",
            "allow_any_host": False,
        })

        # Add 100 hosts
        for i in range(100):
            _rpc_call(_server, "nvmf_subsystem_add_host", {
                "nqn": "nqn:stress",
                "host": f"nqn:stress-host-{i:03d}",
                "psk": f"psk-{i:03d}",
            })

        sub = _server.state.subsystems["nqn:stress"]
        self.assertEqual(len(sub["hosts"]), 100)

        # Remove every other host
        for i in range(0, 100, 2):
            _rpc_call(_server, "nvmf_subsystem_remove_host", {
                "nqn": "nqn:stress",
                "host": f"nqn:stress-host-{i:03d}",
            })

        self.assertEqual(len(sub["hosts"]), 50)

        # Remove remaining
        for i in range(1, 100, 2):
            _rpc_call(_server, "nvmf_subsystem_remove_host", {
                "nqn": "nqn:stress",
                "host": f"nqn:stress-host-{i:03d}",
            })

        self.assertEqual(len(sub["hosts"]), 0)

    def test_add_remove_with_error_injection(self):
        """Add hosts with 20% error rate, verify resilience."""
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:chaos",
            "serial_number": "SN",
            "model_number": "MD",
            "allow_any_host": False,
        })

        _server.set_failure_rate(0.2, timeout_seconds=0.3)

        added_hosts = set()
        for i in range(50):
            host_nqn = f"nqn:chaos-host-{i:03d}"
            try:
                _rpc_call(_server, "nvmf_subsystem_add_host", {
                    "nqn": "nqn:chaos",
                    "host": host_nqn,
                })
                added_hosts.add(host_nqn)
            except (RpcError, Exception):
                pass  # Expected failures

        # Some hosts should have been added
        sub = _server.state.subsystems["nqn:chaos"]
        actual_hosts = {h["nqn"] for h in sub["hosts"]}
        self.assertEqual(actual_hosts, added_hosts)

        # Now remove with errors
        _server.set_failure_rate(0.2, timeout_seconds=0.3)
        removed_hosts = set()
        for host_nqn in list(added_hosts):
            try:
                _rpc_call(_server, "nvmf_subsystem_remove_host", {
                    "nqn": "nqn:chaos",
                    "host": host_nqn,
                })
                removed_hosts.add(host_nqn)
            except (RpcError, Exception):
                pass

        remaining = {h["nqn"] for h in sub["hosts"]}
        self.assertEqual(remaining, added_hosts - removed_hosts)


# ---------------------------------------------------------------------------
# Test: NVMe-oF get_subsystems includes host list
# ---------------------------------------------------------------------------

class TestGetSubsystemsWithHosts(unittest.TestCase):
    """Verify nvmf_get_subsystems returns hosts in the response."""

    def setUp(self):
        _server.reset_state()
        _server.set_failure_rate(0.0)

    def test_get_subsystems_shows_hosts(self):
        _rpc_call(_server, "nvmf_create_subsystem", {
            "nqn": "nqn:view-hosts",
            "serial_number": "SN",
            "model_number": "MD",
            "allow_any_host": False,
        })
        _rpc_call(_server, "nvmf_subsystem_add_host", {
            "nqn": "nqn:view-hosts",
            "host": "nqn:viewer-1",
            "psk": "view-psk",
        })

        subs = _rpc_call(_server, "nvmf_get_subsystems", {})
        self.assertEqual(len(subs), 1)
        self.assertEqual(len(subs[0]["hosts"]), 1)
        self.assertEqual(subs[0]["hosts"][0]["nqn"], "nqn:viewer-1")
        self.assertFalse(subs[0]["allow_any_host"])


if __name__ == "__main__":
    unittest.main()
