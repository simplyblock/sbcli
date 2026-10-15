"""The SPDK entrypoint must not flush the host iptables table.

ultra's ``run_distr_with_ssd.sh`` ran ``iptables -F`` before every SPDK
start to clear fences left by a previous SPDK instance. The pod shares the
host network namespace with the CNI, so the flush also wiped cilium's host
chains: every SPDK restart on the ams1 cluster (2026-09/10) dropped
host-to-pod traffic until the cilium agent was restarted.

The entrypoint now deletes DROP/REJECT rules for an explicit port list that
the control plane passes in as ``FENCE_PORTS``. The list is computed here,
from the node record, because the entrypoint only knows its RPC port. It
has to cover what a 25.10.x control plane fenced on that host, since the
online upgrade restarts the node onto an image that no longer runs the
iptables firewall service -- after that nothing else can remove the rules,
and the 26.x health check does not look at iptables any more.
"""
import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from simplyblock_core.snode_client import SNodeClient
from simplyblock_core.utils import port_block
from simplyblock_web.api.internal.storage_node import docker as snode_docker
from simplyblock_web.api.internal.storage_node import kubernetes as snode_k8s

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"


def _node(node_id, lvol_port=0, hublvol_port=0, lvstore_ports=None):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvol_subsys_port = lvol_port
    n.hublvol = MagicMock(nvmf_port=hublvol_port) if hublvol_port else None
    n.lvstore_ports = lvstore_ports or {}
    return n


class TestHostFencePorts(unittest.TestCase):

    def setUp(self):
        self.db = MagicMock()
        self.db.get_primary_storage_nodes_by_secondary_node_id.return_value = []

    def test_legacy_25_10_record_yields_own_lvol_and_hublvol_ports(self):
        # A record written by 25.10.x has no lvstore_ports map, only the
        # node-level fields. Those are exactly the ports 25.10.x fenced.
        node = _node("n1", lvol_port=9114, hublvol_port=9034)
        self.assertEqual(port_block.host_fence_ports(node, self.db), [9034, 9114])

    def test_primaries_backed_by_this_node_are_included(self):
        # 25.10.x fenced the primary's lvol and hublvol ports on the
        # secondary's host at shutdown; the upgrade restart must clear them.
        node = _node("sec", lvol_port=9114, hublvol_port=9034)
        primary = _node("prim", lvol_port=9113, hublvol_port=9033)
        self.db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary]
        self.assertEqual(port_block.host_fence_ports(node, self.db), [9033, 9034, 9113, 9114])
        self.db.get_primary_storage_nodes_by_secondary_node_id.assert_called_once_with("sec")

    def test_per_lvstore_map_is_read_and_deduplicated(self):
        node = _node("n1", lvol_port=9114, hublvol_port=9034, lvstore_ports={
            "LVS_8071": {"lvol_subsys_port": 9114, "hublvol_port": 9034},
            "LVS_4033": {"lvol_subsys_port": 9113, "hublvol_port": 9033},
            "LVS_bad": "not-a-dict",
        })
        self.assertEqual(port_block.host_fence_ports(node, self.db), [9033, 9034, 9113, 9114])

    def test_unassigned_ports_are_skipped(self):
        node = _node("n1", lvol_port=0, hublvol_port=0, lvstore_ports={"LVS": {"lvol_subsys_port": 0}})
        self.assertEqual(port_block.host_fence_ports(node, self.db), [])

    def test_env_rendering_is_space_separated(self):
        self.assertEqual(port_block.fence_ports_env([9033, 9113]), "9033 9113")
        self.assertEqual(port_block.fence_ports_env([]), "")
        self.assertEqual(port_block.fence_ports_env(None), "")


class TestSpdkProcessStartCarriesFencePorts(unittest.TestCase):

    def _params(self, **kwargs):
        client = SNodeClient.__new__(SNodeClient)
        with patch.object(SNodeClient, "_request", return_value=(True, None)) as req:
            client.spdk_process_start("0-1", 1024, cluster_mode="kubernetes", cluster_id="c1", **kwargs)
        return req.call_args.args[2]

    def test_list_is_sent_when_given(self):
        self.assertEqual(self._params(fence_ports=[9034, 9114])["fence_ports"], [9034, 9114])

    def test_omitted_when_empty_so_old_node_apis_see_the_same_request(self):
        self.assertNotIn("fence_ports", self._params())
        self.assertNotIn("fence_ports", self._params(fence_ports=[]))


class TestStorageNodeApiAcceptsFencePorts(unittest.TestCase):

    _base: ClassVar[dict] = dict(server_ip="10.0.0.10", rpc_port=8080, rpc_username="u", rpc_password="p",
                 l_cores="0-1", namespace="sb", cluster_ip="10.0.0.20", cluster_mode="kubernetes",
                 cluster_id="c1")

    def test_kubernetes_params_default_to_no_ports(self):
        self.assertEqual(snode_k8s.SPDKParams(**self._base).fence_ports, [])
        self.assertEqual(snode_k8s.SPDKParams(fence_ports=[9114, 9034], **self._base).fence_ports, [9114, 9034])

    def test_docker_params_default_to_no_ports(self):
        self.assertEqual(snode_docker.SPDKParams(**self._base).fence_ports, [])
        self.assertEqual(snode_docker.SPDKParams(fence_ports=[9114], **self._base).fence_ports, [9114])


class TestTemplateExposesFencePorts(unittest.TestCase):

    def test_env_var_is_rendered_into_the_spdk_container(self):
        env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
        rendered = env.get_template("storage_deploy_spdk.yaml.j2").render(
            SPDK_IMAGE="spdk:test", L_CORES="0-1", SPDK_MEM=1024, CORES=2, SERVER_IP="10.0.0.10",
            RPC_PORT=8080, RPC_USERNAME="admin", RPC_PASSWORD="secret", HOSTNAME="node-a",
            NAMESPACE="simplyblock", SIMPLYBLOCK_DOCKER_IMAGE="proxy:test", GRAYLOG_SERVER_IP="10.0.0.20",
            MODE="kubernetes", CLUSTER_ID="cluster1", SSD_PCIE="none", PCI_ALLOWED="", TOTAL_HP="",
            NSOCKET=0, FW_PORT=50001, FENCE_PORTS="9034 9114", CPU_TOPOLOGY_ENABLED=False,
            MEM_MEGA=1536, MEM2_MEGA=1024, TLS_ENABLED=False, TLS_PROVIDER="cert-manager",
        )
        self.assertIn("- name: FENCE_PORTS\n          value: \"9034 9114\"", rendered)
