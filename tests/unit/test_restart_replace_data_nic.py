"""`sn restart --data-nics ...` — replace a storage node's data NIC set on restart.

Operators sometimes need to swap a failed or renamed storage NIC for a
different interface without tearing the node out of the cluster and re-adding
it. The restart path already knows how to rebuild `data_nics` when a node moves
host (`--node-addr`); this feature lets the operator change the interface NAMES
too. The requested interfaces are looked up on the live host during the restart
and become the node's new data NICs, so the SPDK restart re-advertises the
NVMe-oF listeners on the new IPs.

Covered here (unit scope — no SPDK/DB):
  * the pure name->IFace resolver used by the restart impl,
  * the CLI surface: `sn restart <id> --data-nics a b` parses and forwards
    `new_data_nics=[...]` to `restart_storage_node`,
  * the v2 API `_RestartParams` model carries and forwards the field,
  * both restart entry points accept the new kwarg.
"""
import inspect
import sys
import unittest
from unittest.mock import patch

from simplyblock_core import storage_node_ops
from simplyblock_cli import cli as cli_module


NODE_ID = "1b8f2c9a-0000-4a00-9000-abcdef012345"

# One node agent info() payload: two usable NICs plus the mgmt one.
NODE_INFO = {
    "network_interface": {
        "eth0": {"ip": "10.0.0.10", "status": "up", "net_type": "ethernet"},
        "eth1": {"ip": "10.0.1.11", "status": "up", "net_type": "ethernet"},
        "eth2": {"ip": "10.0.2.12", "status": "down", "net_type": "ethernet"},
    }
}


class TestBuildDataNicsFromNames(unittest.TestCase):
    def test_resolves_each_name_to_its_live_ip(self):
        ifaces, missing = storage_node_ops._build_data_nics_from_names(
            NODE_INFO, ["eth1"])
        self.assertIsNone(missing)
        self.assertEqual(len(ifaces), 1)
        self.assertEqual(ifaces[0].if_name, "eth1")
        self.assertEqual(ifaces[0].ip4_address, "10.0.1.11")
        self.assertEqual(ifaces[0].net_type, "ethernet")

    def test_preserves_order_and_builds_all(self):
        ifaces, missing = storage_node_ops._build_data_nics_from_names(
            NODE_INFO, ["eth1", "eth0"])
        self.assertIsNone(missing)
        self.assertEqual([i.if_name for i in ifaces], ["eth1", "eth0"])
        self.assertEqual([i.ip4_address for i in ifaces],
                         ["10.0.1.11", "10.0.0.10"])

    def test_unknown_name_reported_as_missing(self):
        ifaces, missing = storage_node_ops._build_data_nics_from_names(
            NODE_INFO, ["eth1", "eth9"])
        self.assertEqual(missing, "eth9")
        # the good one built before the miss is returned so the caller can log,
        # but the caller aborts on `missing` and never assigns a partial set.
        self.assertEqual([i.if_name for i in ifaces], ["eth1"])

    def test_empty_names_yields_nothing(self):
        ifaces, missing = storage_node_ops._build_data_nics_from_names(
            NODE_INFO, [])
        self.assertEqual(ifaces, [])
        self.assertIsNone(missing)

    def test_none_node_info_is_all_missing(self):
        ifaces, missing = storage_node_ops._build_data_nics_from_names(
            None, ["eth1"])
        self.assertEqual(missing, "eth1")
        self.assertEqual(ifaces, [])


def _build_wrapper(dev=False):
    # developer_mode is decided from sys.argv at construction time; private
    # args (e.g. --spdk-image, which the restart handler reads) are only
    # registered on the parser in dev mode.
    argv = ["sbctl"] + (["--dev"] if dev else [])
    with patch.object(sys, "argv", argv):
        cli_module.CLIWrapper.__init__(
            wrapper := cli_module.CLIWrapper.__new__(cli_module.CLIWrapper))
    return wrapper


def _build_parser():
    return _build_wrapper().parser


class TestRestartCliArg(unittest.TestCase):
    def test_data_nics_parses_as_list(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["storage-node", "restart", NODE_ID, "--data-nics", "eth1", "eth2"])
        self.assertEqual(args.data_nics, ["eth1", "eth2"])

    def test_sn_alias_and_single_nic(self):
        parser = _build_parser()
        args = parser.parse_args(
            ["sn", "restart", NODE_ID, "--data-nics", "eth1"])
        self.assertEqual(args.data_nics, ["eth1"])

    def test_omitted_defaults_to_falsy(self):
        parser = _build_parser()
        args = parser.parse_args(["storage-node", "restart", NODE_ID])
        # default '' is falsy, so the impl's `if new_data_nics:` is skipped and
        # the existing NICs are kept.
        self.assertFalse(args.data_nics)


class TestRestartHandlerForwardsDataNics(unittest.TestCase):
    """Drive the real clibase handler (in dev mode, so the private args it reads
    exist) and assert it forwards new_data_nics to restart_storage_node."""

    def _dispatch(self, argv):
        wrapper = _build_wrapper(dev=True)
        args = wrapper.parser.parse_args(argv)
        with patch("simplyblock_cli.clibase.storage_ops.restart_storage_node") as m:
            m.return_value = True
            wrapper.storage_node__restart("restart", args)
        return m

    def test_handler_passes_new_data_nics(self):
        m = self._dispatch(
            ["storage-node", "restart", NODE_ID, "--data-nics", "eth1", "eth2"])
        self.assertTrue(m.called)
        self.assertEqual(m.call_args.kwargs.get("new_data_nics"), ["eth1", "eth2"])

    def test_handler_forwards_empty_when_omitted(self):
        m = self._dispatch(["storage-node", "restart", NODE_ID])
        self.assertFalse(m.call_args.kwargs.get("new_data_nics"))


class TestRestartApiParams(unittest.TestCase):
    def test_restart_params_model_carries_data_nics(self):
        from simplyblock_web.api.v2.cluster.storage_node import _RestartParams
        p = _RestartParams(new_data_nics=["eth1"])
        self.assertEqual(p.new_data_nics, ["eth1"])
        self.assertEqual(_RestartParams().new_data_nics, [])


class TestEntryPointSignatures(unittest.TestCase):
    def test_both_restart_entry_points_accept_new_data_nics(self):
        for fn in (storage_node_ops.restart_storage_node,
                   storage_node_ops._restart_storage_node_impl):
            self.assertIn("new_data_nics",
                          inspect.signature(fn).parameters,
                          f"{fn.__name__} missing new_data_nics")


if __name__ == "__main__":
    unittest.main()
