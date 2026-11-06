"""cluster create bootstrap ordering (SFAM-2847).

Every simplyblock service in the `app` stack exits on an unconfigured
FoundationDB. Deploying them before `configure new single ssd` made the stack
wait oscillate until the 720s script timeout, so no fresh docker cluster could
bootstrap. The required order is: monitoring stack, FDB services, configure
the database, simplyblock services -- and only then the cluster object.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

import yaml

from simplyblock_core import cluster_ops, scripts

SCRIPTS_DIR = os.path.dirname(scripts.__file__)


class _StopAfterBootstrap(Exception):
    pass


def _create_cluster():
    return cluster_ops.create_cluster(
        blk_size=4096, page_size_in_blocks=2097152, cli_pass=None,
        cap_warn=0, cap_crit=0, prov_cap_warn=0, prov_cap_crit=0,
        ifname="eth0", mgmt_ip=None, log_del_interval="3d", metrics_retention_period="7d",
        contact_point="", grafana_endpoint="", distr_ndcs=1, distr_npcs=1,
        distr_bs=4096, distr_chunk_bs=4096, ha_type="ha", mode="docker",
        enable_node_affinity=False, qpair_count=0, client_qpair_count=0,
        max_queue_size=128, inflight_io_threshold=4, disable_monitoring=True,
        strict_node_anti_affinity=False, name=None, tls_secret=None,
        ingress_host_source=None, dns_name=None, fabric="tcp",
        is_single_node=False, client_data_nic="")


class TestCreateClusterDeployOrder(unittest.TestCase):

    def test_db_is_configured_between_fdb_and_app_deploy(self):
        calls = []

        def deploy_stack(*args, phase):
            calls.append(f"deploy:{phase}")
            if phase == "app":
                raise _StopAfterBootstrap

        fake_scripts = MagicMock()
        fake_scripts.deploy_stack.side_effect = deploy_stack
        fake_scripts.set_db_config_single.side_effect = lambda: calls.append("configure_db")
        docker_client = MagicMock()
        docker_client.swarm.attrs = {}

        with patch.object(cluster_ops, "scripts", fake_scripts), \
                patch.object(cluster_ops.utils, "get_iface_ip", return_value="10.0.0.1"), \
                patch.object(cluster_ops.docker, "DockerClient", return_value=docker_client), \
                patch.object(cluster_ops, "DeployConfig") as deploy_config:
            with self.assertRaises(_StopAfterBootstrap):
                _create_cluster()

        self.assertEqual(calls, ["deploy:monitoring", "deploy:fdb", "configure_db", "deploy:app"])
        # Nothing is written to the database before the services are up.
        deploy_config.return_value.write_to_db.assert_not_called()


class TestDeployStackPhase(unittest.TestCase):

    def test_phase_is_passed_to_the_script(self):
        with patch.object(scripts.subprocess, "check_output", return_value="") as run:
            scripts.deploy_stack("pass", "10.0.0.1", "img", "graylog", "cid",
                                 "3d", "7d", "INFO", "", "False", phase="fdb")
        self.assertEqual(run.call_args.args[0][-1], "fdb")

    def test_unknown_phase_is_rejected(self):
        with patch.object(scripts.subprocess, "check_output") as run:
            with self.assertRaises(ValueError):
                scripts.deploy_stack("pass", "10.0.0.1", "img", "graylog", "cid",
                                     "3d", "7d", "INFO", "", "False", phase="all")
        run.assert_not_called()


class TestComposeSplit(unittest.TestCase):
    """The FDB tier lives in its own file so it can be deployed first, but both
    files are deployed as stack `app`: names and volumes must not move."""

    def _load(self, name):
        with open(os.path.join(SCRIPTS_DIR, name), encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_fdb_services_only_in_fdb_file(self):
        fdb = self._load("docker-compose-swarm-fdb.yml")["services"]
        app = self._load("docker-compose-swarm.yml")["services"]
        self.assertEqual(set(fdb), {"fdb-server", "fdb-server1", "fdb-server2", "fdb-server3",
                                    "fdb-server4", "fdb-backup-agent"})
        self.assertFalse(set(fdb) & set(app))

    def test_fdb_file_declares_the_fdb_volumes(self):
        fdb = self._load("docker-compose-swarm-fdb.yml")
        used = {v.split(":")[0] for svc in fdb["services"].values()
                for v in svc.get("volumes", []) if not v.startswith("/")}
        self.assertTrue(used)
        self.assertLessEqual(used, set(fdb["volumes"]))

    def test_deploy_script_waits_for_every_fdb_server(self):
        with open(os.path.join(SCRIPTS_DIR, "deploy_stack.sh"), encoding="utf-8") as f:
            script = f.read()
        fdb = self._load("docker-compose-swarm-fdb.yml")["services"]
        servers = {s for s in fdb if s.startswith("fdb-server")}
        line = next(ln for ln in script.splitlines() if ln.startswith("FDB_SERVERS="))
        self.assertEqual(set(line.split("=", 1)[1].strip('"').split()), servers)
