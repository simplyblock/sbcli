"""`sn restart --data-nics ...` — replace a storage node's data NIC set on restart.

Operators sometimes need to swap a failed or renamed storage NIC for a
different interface without tearing the node out of the cluster and re-adding
it. The restart path already knows how to rebuild `data_nics` when a node moves
host (`--node-addr`); this feature lets the operator change the interface NAMES
too. The requested interfaces are looked up on the live host during the restart
and become the node's new data NICs, so the SPDK restart re-advertises the
NVMe-oF listeners on the new IPs.

Covered here (unit scope — no SPDK/DB):
  * the name->IFace resolver both rebuild paths share, including the fabric
    classification that decides whether an interface may be used at all, and
    the per-call-site policy each path applies to a rejected name,
  * the `--node-addr` rebuild of the EXISTING names against a new host,
  * the CLI surface: `sn restart <id> --data-nics a b` parses and forwards
    `new_data_nics=[...]` to `restart_storage_node`,
  * the v2 API `_RestartParams` model carries and forwards the field.
"""
import sys
import unittest
from unittest.mock import patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.iface import IFace
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


class _FakeSNodeApi:
    """Stands in for the node agent's two interface-classification probes.

    The real ones are HTTP GETs to the node (`SNodeClient.ifc_is_roce` /
    `ifc_is_tcp`), which is the only reason the resolver takes the client at
    all.
    """

    def __init__(self, roce=(), tcp=()):
        self._roce = set(roce)
        self._tcp = set(tcp)

    def ifc_is_roce(self, nic):
        return nic in self._roce

    def ifc_is_tcp(self, nic):
        return nic in self._tcp


TCP_ONLY_HOST = _FakeSNodeApi(tcp=["eth0", "eth1", "eth2"])


class TestResolveDataNics(unittest.TestCase):
    """The one owner of name -> classified IFace, shared by both restart paths
    that rebuild data_nics (`--data-nics` and the `--node-addr` move)."""

    def test_resolves_each_name_to_its_live_ip(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth1"], TCP_ONLY_HOST, True, False)
        self.assertEqual(rejected, [])
        self.assertEqual(len(ifaces), 1)
        self.assertEqual(ifaces[0].if_name, "eth1")
        self.assertEqual(ifaces[0].ip4_address, "10.0.1.11")
        self.assertEqual(ifaces[0].net_type, "ethernet")
        self.assertEqual(ifaces[0].trtype, "TCP")

    def test_preserves_order_and_builds_all(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth1", "eth0"], TCP_ONLY_HOST, True, False)
        self.assertEqual(rejected, [])
        self.assertEqual([i.if_name for i in ifaces], ["eth1", "eth0"])
        self.assertEqual([i.ip4_address for i in ifaces],
                         ["10.0.1.11", "10.0.0.10"])

    def test_roce_interface_classifies_as_rdma(self):
        api = _FakeSNodeApi(roce=["eth1"], tcp=["eth0"])
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth1"], api, True, True)
        self.assertEqual(rejected, [])
        self.assertEqual(ifaces[0].trtype, "RDMA")

    def test_unknown_name_is_rejected_naming_it(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth1", "eth9"], TCP_ONLY_HOST, True, False)
        self.assertEqual([i.if_name for i in ifaces], ["eth1"])
        self.assertEqual([n for n, _ in rejected], ["eth9"])

    def test_unusable_interface_never_reaches_data_nics(self):
        """Regression: a NIC that classifies as neither RDMA nor TCP must never
        be returned as usable.

        IFace.trtype defaults to "TCP", so an unclassified interface is
        indistinguishable from a working TCP one, and the listener loops (e.g.
        _create_jm_stack_on_raid, which does not even guard on ip4_address)
        would advertise a subsystem on it. The original resolver appended every
        requested name unconditionally and only the whole-set "none usable"
        check downstream could catch it — so a MIXED set (one good NIC, one
        bad) passed straight through.
        """
        api = _FakeSNodeApi(tcp=["eth0"])  # eth1 is neither roce nor tcp here
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth0", "eth1"], api, True, False)
        self.assertEqual([i.if_name for i in ifaces], ["eth0"])
        self.assertEqual([n for n, _ in rejected], ["eth1"])

    def test_address_less_interface_is_rejected(self):
        """Present by name is not enough: get_nics_data lists every interface,
        an address-less one included, with ip set to "". That is the NIC a
        moved node most plausibly finds on its new host."""
        info = {"network_interface": {
            "eth1": {"ip": "", "status": "down", "net_type": "ethernet"}}}
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            info, ["eth1"], _FakeSNodeApi(), True, False)
        self.assertEqual(ifaces, [])
        self.assertEqual([n for n, _ in rejected], ["eth1"])

    def test_rdma_only_cluster_rejects_a_plain_tcp_interface(self):
        # fabric_tcp is off, so a non-RoCE NIC has no usable transport at all:
        # a TCP listener would be created against a process with no TCP
        # transport.
        api = _FakeSNodeApi(tcp=["eth1"])
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth1"], api, False, True)
        self.assertEqual(ifaces, [])
        self.assertEqual([n for n, _ in rejected], ["eth1"])

    def test_empty_names_yields_nothing(self):
        self.assertEqual(
            storage_node_ops._resolve_data_nics(
                NODE_INFO, [], TCP_ONLY_HOST, True, False),
            ([], []))

    def test_none_node_info_rejects_every_name(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            None, ["eth1"], TCP_ONLY_HOST, True, False)
        self.assertEqual(ifaces, [])
        self.assertEqual([n for n, _ in rejected], ["eth1"])

    def test_rejection_reason_names_the_interface_state(self):
        # The reason string is what lands in the operator's log, so it has to
        # say enough to act on.
        _, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, ["eth2"], _FakeSNodeApi(), True, False)
        reason = rejected[0][1]
        self.assertIn("10.0.2.12", reason)
        self.assertIn("down", reason)


class TestNodeAddrRebuildUsesTheSameResolver(unittest.TestCase):
    """The `--node-addr` move: same interface NAMES, re-resolved AND
    re-classified on the new host.

    Two regressions are pinned here. The lookup used to be an unguarded
    node_info['network_interface'][name] that raised a bare KeyError out of the
    middle of the restart — on exactly the scenario `--data-nics` exists to
    handle. And the rebuild did no classification at all, so a NIC that existed
    by name but was unusable on the new host inherited IFace.trtype's "TCP"
    default and got a listener on a dead interface.
    """

    def _old_names(self, *names):
        return [IFace({'if_name': n, 'ip4_address': '192.168.0.9'}) for n in names]

    def test_rebuilds_existing_names_against_the_new_hosts_ips(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, [n["if_name"] for n in self._old_names("eth1")],
            TCP_ONLY_HOST, True, False)
        self.assertEqual(rejected, [])
        self.assertEqual([i.if_name for i in ifaces], ["eth1"])
        self.assertEqual(ifaces[0].ip4_address, "10.0.1.11")

    def test_renamed_nic_is_reported_not_raised(self):
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, [n["if_name"] for n in self._old_names("eno1")],
            TCP_ONLY_HOST, True, False)
        self.assertEqual(ifaces, [])
        self.assertEqual([n for n, _ in rejected], ["eno1"])

    def test_one_dead_nic_of_two_leaves_the_node_movable(self):
        # Inherited names, not operator intent: the restart keeps the usable
        # NIC and warns about the other rather than refusing the move. Same
        # rule add_node applies.
        api = _FakeSNodeApi(tcp=["eth0"])
        ifaces, rejected = storage_node_ops._resolve_data_nics(
            NODE_INFO, [n["if_name"] for n in self._old_names("eth0", "eth1")],
            api, True, False)
        self.assertEqual([i.if_name for i in ifaces], ["eth0"])
        self.assertEqual([n for n, _ in rejected], ["eth1"])


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


if __name__ == "__main__":
    unittest.main()
