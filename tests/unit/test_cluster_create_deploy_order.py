"""cluster create bootstrap ordering (SFAM-2847).

Every simplyblock service in the `app` stack exits on an unconfigured
FoundationDB. Deploying them before `configure new single ssd` made the stack
wait oscillate until the 720s script timeout, so no fresh docker cluster could
bootstrap. The required order is: monitoring stack, FDB services, configure
the database, simplyblock services -- and only then the cluster object.
"""

import os
import shutil
import subprocess
import tempfile
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
        self.assertEqual(run.call_args.args[0][-2], "fdb")

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


class TestWaitTimeoutBelowScriptTimeout(unittest.TestCase):
    """stack_deploy_wait.sh must time out before the Python limit kills
    deploy_stack.sh, or its report of what never converged is lost and only a
    bare TimeoutExpired remains (SFAM-2847 follow-up)."""

    def test_wait_timeout_leaves_margin(self):
        self.assertLess(scripts.STACK_WAIT_TIMEOUT_SEC, scripts.SCRIPT_TIMEOUT_SEC - 60)

    def test_wait_timeout_is_passed_to_the_script(self):
        with patch.object(scripts.subprocess, "check_output", return_value="") as run:
            scripts.deploy_stack("pass", "10.0.0.1", "img", "graylog", "cid",
                                 "3d", "7d", "INFO", "", "False", phase="app")
        self.assertEqual(run.call_args.args[0][-1], str(scripts.STACK_WAIT_TIMEOUT_SEC))
        self.assertEqual(run.call_args.kwargs["timeout"], scripts.SCRIPT_TIMEOUT_SEC)

    def test_every_wait_goes_through_wait_stack(self):
        with open(os.path.join(SCRIPTS_DIR, "deploy_stack.sh"), encoding="utf-8") as f:
            calls = [ln for ln in f if "stack_deploy_wait.sh" in ln and not ln.lstrip().startswith("#")]
        self.assertEqual(len(calls), 1)
        self.assertIn('-t "$STACK_WAIT_TIMEOUT"', calls[0])


class TestFailureIsReported(unittest.TestCase):

    def _fail(self, exc):
        with patch.object(scripts.subprocess, "check_output", side_effect=exc), \
                self.assertLogs(scripts.logger, level="ERROR") as logs, \
                self.assertRaises(type(exc)):
            scripts.deploy_stack("pass", "10.0.0.1", "img", "graylog", "cid",
                                 "3d", "7d", "INFO", "", "False", phase="app")
        return "\n".join(logs.output)

    OUTPUT = ("+ export CLUSTER_SECRET=topsecret\n"
              "Service app_WebAppAPI state: replicating 0/1\n"
              "Error: Timeout exceeded\n")

    def test_wait_timeout_reason_is_logged_without_trace_lines(self):
        logged = self._fail(scripts.subprocess.CalledProcessError(1, ["bash"], output=self.OUTPUT))
        self.assertIn("deploy_stack.sh failed with return code 1", logged)
        self.assertIn("Error: Timeout exceeded", logged)
        self.assertIn("app_WebAppAPI state: replicating 0/1", logged)
        self.assertNotIn("topsecret", logged)

    def test_python_timeout_still_reports_partial_output(self):
        logged = self._fail(scripts.subprocess.TimeoutExpired(["bash"], 720, output=self.OUTPUT))
        self.assertIn("deploy_stack.sh killed after", logged)
        self.assertIn("replicating 0/1", logged)
        self.assertNotIn("topsecret", logged)

    def test_other_scripts_keep_failure_output_at_debug(self):
        # set_db_config.sh's own output echoes the FDB connection string.
        err = scripts.subprocess.CalledProcessError(1, ["bash"], output="desc:secret@10.0.0.1:4500\n")
        with patch.object(scripts.subprocess, "check_output", side_effect=err), \
                patch.object(scripts.logger, "error") as error, \
                self.assertRaises(scripts.subprocess.CalledProcessError):
            scripts.set_db_config("desc:secret@10.0.0.1:4500")
        error.assert_not_called()


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class TestDeployStackScript(unittest.TestCase):
    """Run the real deploy_stack.sh against stub docker / stack_deploy_wait.sh."""

    def _run(self, wait_exit, timeout="600"):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("deploy_stack.sh", "docker-compose-swarm.yml", "docker-compose-swarm-fdb.yml"):
                shutil.copy(os.path.join(SCRIPTS_DIR, name), tmp)
            log = os.path.join(tmp, "calls.log")
            stubs = {
                "stack_deploy_wait.sh": f'echo "wait $*" >> {log}\nexit {wait_exit}\n',
                "bin/docker": f'echo "docker $*" >> {log}\n',
            }
            for rel, body in stubs.items():
                path = os.path.join(tmp, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    f.write("#!/usr/bin/env bash\n" + body)
                os.chmod(path, 0o755)
            env = dict(os.environ, PATH=f"{tmp}/bin:{os.environ['PATH']}")
            proc = subprocess.run(
                ["bash", os.path.join(tmp, "deploy_stack.sh"), "pass", "10.0.0.1", "img", "hash", "secret",
                 "cid", "3d", "7d", "INFO", "", "True", "app", timeout],
                env=env, capture_output=True, text=True)
            if not os.path.exists(log):  # the script exited before calling anything
                return proc, ""
            with open(log) as f:
                return proc, f.read()

    def test_wait_gets_the_timeout(self):
        proc, calls = self._run(wait_exit=0)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("wait -t 600 app", calls)

    def test_stuck_stack_prints_diagnostics_and_fails(self):
        proc, calls = self._run(wait_exit=1)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("did not converge within 600s", proc.stdout)
        self.assertIn("docker stack ps", calls)

    def test_missing_timeout_is_rejected(self):
        proc, calls = self._run(wait_exit=0, timeout="")
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn("wait", calls)
