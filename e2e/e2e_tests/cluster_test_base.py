import os
import threading
import time
import boto3
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils, RunnerK8sLog
from utils.k8s_utils import K8sUtils, K8sSbcliUtils
from utils.common_utils import CommonUtils
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
import string
import random
import json


def generate_random_sequence(length):
    letters = string.ascii_uppercase  # A-Z
    numbers = string.digits  # 0-9
    all_chars = letters + numbers  # Allowed characters

    first_char = random.choice(letters)  # First character must be a letter
    remaining_chars = ''.join(random.choices(all_chars, k=length-1))  # Next 14 characters

    return first_char + remaining_chars

class TestClusterBase:
    def __init__(self, **kwargs):
        self.cluster_secret = os.environ.get("CLUSTER_SECRET")
        self.cluster_id = os.environ.get("CLUSTER_ID")

        self.api_base_url = os.environ.get("API_BASE_URL")
        self.client_machines = os.environ.get("CLIENT_IP", "")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{self.cluster_id} {self.cluster_secret}"
        }
        self.bastion_server = os.environ.get("BASTION_SERVER", None)

        self.ssh_obj = SshUtils(bastion_server=self.bastion_server)
        self.logger = setup_logger(__name__)
        self.k8s_test = kwargs.get("k8s_run", False)
        if self.k8s_test and not self.api_base_url:
            # K8s mode: route all sbcli calls through kubectl exec into admin pod.
            # K8sUtils needs the first management node IP from MNODES / K3S_MNODES.
            _mnodes_raw = os.environ.get("MNODES", os.environ.get("K3S_MNODES", ""))
            _mgmt_node = _mnodes_raw.split()[0] if _mnodes_raw.split() else ""
            _k8s = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=_mgmt_node)
            self.sbcli_utils = K8sSbcliUtils(
                k8s=_k8s,
                cluster_id=self.cluster_id or "",
            )
        else:
            self.sbcli_utils = SbcliUtils(
                cluster_api_url=self.api_base_url,
                cluster_id=self.cluster_id,
                cluster_secret=self.cluster_secret
            )
        self.common_utils = CommonUtils(self.sbcli_utils, self.ssh_obj)
        self.mgmt_nodes = None
        self.storage_nodes = None
        self.fio_node = None
        self.ndcs = kwargs.get("ndcs", 1)
        self.npcs = kwargs.get("npcs", 1)
        self.bs = kwargs.get("bs", 4096)
        self.chunk_bs = kwargs.get("chunk_bs", 4096)
        self.pool_name = "test_pool"
        self.lvol_name = f"test_lvl_{generate_random_sequence(4)}"
        self.mount_path = "/mnt/test_location"
        self.nfs_log_base = os.environ.get("NFS_LOG_BASE", "/mnt/nfs_share")
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")
        self.fio_debug = kwargs.get("fio_debug", False)
        self.ec2_resource = None
        self.lvol_crypt_keys = ["7b3695268e2a6611a25ac4b1ee15f27f9bf6ea9783dada66a4a730ebf0492bfd",
                                "78505636c8133d9be42e347f82785b81a879cd8133046f8fc0b36f17b078ad0c"]
        self.log_threads = []
        self.test_name = ""
        self.container_nodes = {}
        self.docker_logs_path = ""
        self.runner_k8s_log = ""
        self.test_start_time_utc = None

    def setup(self):
        """Contains setup required to run the test case
        """
        self.logger.info("Inside setup function")
        retry = 30
        while retry > 0:
            try:
                print("getting all storage nodes")
                self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_lvols()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as e:
                self.logger.debug(f"API call failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attemp exhausted. API failed with: {e}. Exiting")
                    raise e
                self.logger.info(f"Retrying Base APIs before starting tests. Attempt: {30 - retry + 1}")
        if not self.k8s_test:
            for node in self.mgmt_nodes:
                self.logger.info(f"**Connecting to management nodes** - {node}")
                self.ssh_obj.connect(
                    address=node,
                    bastion_server_address=self.bastion_server,
                )
                sleep_n_sec(2)
                self.ssh_obj.set_aio_max_nr(node)
            for node in self.storage_nodes:
                self.logger.info(f"**Connecting to storage nodes** - {node}")
                self.ssh_obj.connect(
                    address=node,
                    bastion_server_address=self.bastion_server,
                )
                sleep_n_sec(2)
                self.ssh_obj.set_aio_max_nr(node)
        if not self.client_machines:
            self.client_machines = f"{self.mgmt_nodes[0]}"

        self.client_machines = self.client_machines.strip().split(" ")
        for client in self.client_machines:
            self.logger.info(f"**Connecting to client machine** - {client}")
            self.ssh_obj.connect(
                address=client,
                bastion_server_address=self.bastion_server,
            )
            sleep_n_sec(2)

        nfs_server = "10.10.10.140"
        nfs_path = "/srv/nfs_share"
        nfs_mount_point = "/mnt/nfs_share"

        if not self.k8s_test:
            for node in self.storage_nodes + self.mgmt_nodes:
                self.ssh_obj.ensure_nfs_mounted(node, nfs_server, nfs_path, nfs_mount_point)
        for node in self.client_machines:
            self.ssh_obj.ensure_nfs_mounted(node, nfs_server, nfs_path, nfs_mount_point)
        self.ssh_obj.ensure_nfs_mounted("localhost", nfs_server, nfs_path, nfs_mount_point, is_local=True)

        self.fio_node = self.client_machines if self.client_machines else [self.mgmt_nodes[0]]

        # Record UTC start time for Graylog log export at teardown
        self.test_start_time_utc = datetime.now(timezone.utc)

        # Construct the logs path with test name and timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        # fresh folder per run on NFS (mounted on client and runner):
        self.docker_logs_path = os.path.join(self.nfs_log_base, f"{self.test_name}-{timestamp}")
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        for node in self.fio_node:
            self.ssh_obj.make_directory(node=node, dir_name=self.log_path)
        
        run_file = os.getenv("RUN_DIR_FILE", None)
        if run_file:
            with open(run_file, "w") as f:
                f.write(self.docker_logs_path)
        
        self.runner_k8s_log = RunnerK8sLog(
                log_dir=self.docker_logs_path,
                test_name=self.test_name
            )

        # command = "python3 -c \"from importlib.metadata import version;print(f'SBCLI Version: {version('''sbcli-dev''')}')\""
        # self.ssh_obj.exec_command(
        #     self.mgmt_nodes[0], command=command
        # )
        self.disconnect_lvols()
        sleep_n_sec(2)
        self.unmount_all(base_path=self.mount_path)
        sleep_n_sec(2)
        for node in self.fio_node:
            self.ssh_obj.unmount_path(node=node,
                                      device=self.mount_path)
            sleep_n_sec(2)
        self.disconnect_lvols()
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_snapshots() if self.k8s_test else \
            self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_lvols()
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_storage_pools()
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        if aws_access_key and aws_secret_key:
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=os.environ.get("AWS_REGION")
            )
            self.ec2_resource = session.resource('ec2')

        os.makedirs(self.docker_logs_path, exist_ok=True)
        if self.k8s_test:
            self.runner_k8s_log.start_logging()
            self.runner_k8s_log.monitor_pod_logs()
        else:
            self.ssh_obj.make_directory(node=node, dir_name=self.docker_logs_path)
            self.ssh_obj.make_directory(node=node, dir_name=self.log_path)
            for node in self.storage_nodes:
                node_log_dir = os.path.join(self.docker_logs_path, node)
                self.ssh_obj.make_directory(node=node, dir_name=node_log_dir)
                containers = self.ssh_obj.get_running_containers(node_ip=node)
                self.container_nodes[node] = containers
                self.ssh_obj.check_tmux_installed(node_ip=node)
                self.ssh_obj.exec_command(node=node, command="sudo tmux kill-server")
                self.ssh_obj.start_resource_monitors(node_ip=node, log_dir=node_log_dir)
                self.ssh_obj.start_docker_logging(node_ip=node, containers=containers,
                                                  log_dir=node_log_dir, test_name=self.test_name)
                self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=node_log_dir)
                self.ssh_obj.start_netstat_dmesg_logging(node_ip=node, log_dir=node_log_dir)
                self.ssh_obj.reset_iptables_in_spdk(node_ip=node)

            for node in self.mgmt_nodes:
                node_log_dir = os.path.join(self.docker_logs_path, node)
                self.ssh_obj.make_directory(node=node, dir_name=node_log_dir)
                containers = self.ssh_obj.get_running_containers(node_ip=node)
                self.container_nodes[node] = containers
                self.ssh_obj.check_tmux_installed(node_ip=node)
                self.ssh_obj.exec_command(node=node, command="sudo tmux kill-server")
                self.ssh_obj.start_resource_monitors(node_ip=node, log_dir=node_log_dir)
                self.ssh_obj.start_docker_logging(node_ip=node, containers=containers,
                                                  log_dir=node_log_dir, test_name=self.test_name)
                self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=node_log_dir)
                self.ssh_obj.start_netstat_dmesg_logging(node_ip=node, log_dir=node_log_dir)

            self.fetch_all_nodes_distrib_log()

        for node in self.fio_node:
            node_log_dir = os.path.join(self.docker_logs_path, node)
            self.ssh_obj.make_directory(node=node, dir_name=node_log_dir)
            self.ssh_obj.check_tmux_installed(node_ip=node)
            self.ssh_obj.exec_command(node=node, command="sudo tmux kill-server")
            self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=node_log_dir)
            self.ssh_obj.start_netstat_dmesg_logging(node_ip=node, log_dir=node_log_dir)
            self.ssh_obj.start_full_journal_dmesg_logging(node_ip=node, log_dir=node_log_dir)

        self.logger.info("Started log monitoring for all storage nodes.")

        if not self.k8s_test:
            self.start_root_monitor()

        sleep_n_sec(120)

    def configure_sysctl_settings(self):
        """Configure TCP kernel parameters on the node."""
        sysctl_commands = [
            'echo "net.core.rmem_max=16777216" | sudo tee -a /etc/sysctl.conf',
            'echo "net.core.rmem_default=87380" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_rmem=4096 87380 16777216" | sudo tee -a /etc/sysctl.conf',
            'echo "net.core.somaxconn=1024" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_max_syn_backlog=4096" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_window_scaling=1" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_retries2=8" | sudo tee -a /etc/sysctl.conf',
            'sudo sysctl -p'
        ]
        if not self.k8s_test:
            for node in self.storage_nodes:
                for cmd in sysctl_commands:
                    self.ssh_obj.exec_command(node, cmd)
        for cmd in sysctl_commands:
            for node in self.fio_node:
                self.ssh_obj.exec_command(node, cmd)
        for node in self.fio_node:
            self.ssh_obj.set_aio_max_nr(node)
    
        self.logger.info("Configured TCP sysctl settings on all the nodes!!")

    def cleanup_logs(self):
        """Cleans logs
        """
        base_path = Path.home()
        for node in self.fio_node:
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.log*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.state*", recursive=True)
        if not self.k8s_test:
            # self.ssh_obj.delete_file_dir(self.mgmt_nodes[0], entity="/etc/simplyblock/*", recursive=True)
            self.ssh_obj.delete_file_dir(self.mgmt_nodes[0], entity=f"{base_path}/*.txt*", recursive=True)
            for node in self.storage_nodes:
                self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/[0-9]*", recursive=True)
                self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/*core*.zst", recursive=True)
                self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/LVS*", recursive=True)
                self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/distrib*", recursive=True)
                self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.txt*", recursive=True)
                self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.log*", recursive=True)

    def stop_docker_logs_collect(self):
        for node in self.storage_nodes:
            self.ssh_obj.stop_container_log_monitor(node)
            pids = self.ssh_obj.find_process_name(
                node=node,
                process_name="docker logs --follow",
                return_pid=True
            )
            for pid in pids:
                self.ssh_obj.kill_processes(node=node, pid=pid)
        
        for node in self.mgmt_nodes:
            self.ssh_obj.stop_container_log_monitor(node)
            pids = self.ssh_obj.find_process_name(
                node=node,
                process_name="docker logs --follow",
                return_pid=True
            )
            for pid in pids:
                self.ssh_obj.kill_processes(node=node, pid=pid)
        self.logger.info("All log monitoring threads stopped.")
    
    def stop_k8s_log_collect(self):
        self.runner_k8s_log.stop_log_monitor()
        self.runner_k8s_log.stop_logging()

    def fetch_all_nodes_distrib_log(self):
        if self.k8s_test:
            k8s_utils = getattr(self, "k8s_utils", None) or getattr(
                getattr(self, "sbcli_utils", None), "k8s", None
            )
            if not k8s_utils:
                self.logger.warning("Skipping distrib log fetch in K8s mode (k8s_utils not available)")
                return
            storage_nodes = self.sbcli_utils.get_storage_nodes()
            for result in storage_nodes["results"]:
                if not result.get("is_secondary_node"):
                    k8s_utils.fetch_distrib_logs_k8s(
                        storage_node_id=result["uuid"],
                        storage_node_ip=result["mgmt_ip"],
                        logs_path=self.docker_logs_path,
                    )
            return
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        all_ok = True
        for result in storage_nodes['results']:
            if result['is_secondary_node'] is False:
                ok = self.ssh_obj.fetch_distrib_logs(result["mgmt_ip"], result["uuid"],
                                                     logs_path=self.docker_logs_path)
                if not ok:
                    all_ok = False
        assert all_ok, "Placement dump validation failed on one or more storage nodes"

    def collect_outage_diagnostics(self, label):
        """Collect management details + lvstore dumps + distrib placement dumps
        for ALL storage nodes, right before an outage or right after recovery.

        Args:
            label: e.g. "pre_outage", "post_recovery", "pre_outage_node_<id>"
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        tag = f"_{label}_{timestamp}"
        self.logger.info(f"[diagnostics] === Collecting outage diagnostics: {label} at {timestamp} ===")

        # 1. Collect management details (cluster/sn/lvol/pool lists etc.)
        try:
            self.collect_management_details(suffix=tag)
        except Exception as e:
            self.logger.warning(f"[diagnostics] collect_management_details failed: {e}")

        # 2. Collect dump_lvstore + distrib placement for ALL nodes in parallel
        try:
            self._collect_all_node_dumps_parallel(tag)
        except Exception as e:
            self.logger.warning(f"[diagnostics] _collect_all_node_dumps_parallel failed: {e}")

        self.logger.info(f"[diagnostics] === Completed outage diagnostics: {label} at {timestamp} ===")

    def _collect_all_node_dumps_parallel(self, tag):
        """Collect dump_lvstore + fetch_distrib_logs for ALL storage nodes in parallel.

        Handles both k8s and non-k8s environments. Each node's dumps are collected
        in a separate thread for speed. The dumps are stored in a tagged subdirectory
        so pre-outage and post-recovery dumps are clearly separated.

        Args:
            tag: suffix for directory naming, e.g. "_pre_outage_20240408_143000"
        """
        try:
            storage_nodes = self.sbcli_utils.get_storage_nodes()
            nodes = storage_nodes.get("results", [])
        except Exception as e:
            self.logger.warning(f"[node_dumps] Cannot get storage nodes: {e}")
            return

        if not nodes:
            self.logger.warning("[node_dumps] No storage nodes found")
            return

        dump_dir = os.path.join(self.docker_logs_path, f"node_dumps{tag}")
        os.makedirs(dump_dir, exist_ok=True)

        threads = []
        for node_info in nodes:
            node_id = node_info["uuid"]
            node_ip = node_info.get("mgmt_ip", "")
            t = threading.Thread(
                target=self._collect_single_node_dump,
                args=(node_id, node_ip, dump_dir),
                daemon=True,
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=600)

        self.logger.info(f"[node_dumps] Completed parallel dumps for {len(nodes)} nodes → {dump_dir}")

    def _collect_single_node_dump(self, node_id, node_ip, dump_dir):
        """Collect dump_lvstore and distrib placement dump for a single node.

        Args:
            node_id: Storage node UUID
            node_ip: Storage node management IP
            dump_dir: Directory to store dump files
        """
        self.logger.info(f"[node_dump] Starting dump for node {node_id} ({node_ip})")
        try:
            if self.k8s_test:
                k8s_obj = getattr(self, 'k8s_utils', None) or getattr(
                    getattr(self, 'sbcli_utils', None), 'k8s', None
                )
                if not k8s_obj:
                    self.logger.warning(f"[node_dump] k8s_utils not available for node {node_id}")
                    return
                sbcli_cmd = getattr(
                    getattr(self, 'sbcli_utils', None), 'sbcli_cmd',
                    os.environ.get("SBCLI_CMD", "sbcli-dev")
                )
                try:
                    k8s_obj.dump_lvstore_k8s(
                        storage_node_id=node_id,
                        storage_node_ip=node_ip,
                        logs_path=dump_dir,
                        sbcli_cmd=sbcli_cmd,
                    )
                except Exception as e:
                    self.logger.warning(f"[node_dump] dump_lvstore_k8s failed for {node_id}: {e}")
                try:
                    k8s_obj.fetch_distrib_logs_k8s(
                        storage_node_id=node_id,
                        storage_node_ip=node_ip,
                        logs_path=dump_dir,
                    )
                except Exception as e:
                    self.logger.warning(f"[node_dump] fetch_distrib_logs_k8s failed for {node_id}: {e}")
            else:
                try:
                    self.ssh_obj.dump_lvstore(
                        node_ip=self.mgmt_nodes[0],
                        storage_node_id=node_id,
                    )
                except Exception as e:
                    self.logger.warning(f"[node_dump] dump_lvstore failed for {node_id}: {e}")
                try:
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=node_ip,
                        storage_node_id=node_id,
                        logs_path=dump_dir,
                    )
                except Exception as e:
                    self.logger.warning(f"[node_dump] fetch_distrib_logs failed for {node_id}: {e}")
        except Exception as e:
            self.logger.warning(f"[node_dump] Failed for node {node_id} ({node_ip}): {e}")
        self.logger.info(f"[node_dump] Completed dump for node {node_id} ({node_ip})")

    def _collect_management_details_k8s(self, suffix: str):
        """Collect management details via kubectl exec (k8s mode)."""
        base_path = os.path.join(self.docker_logs_path, "mgmt_details")
        os.makedirs(base_path, exist_ok=True)
        k8s = self.sbcli_utils.k8s

        cmds = [
            (f"cluster_list{suffix}.txt", f"{self.base_cmd} cluster list"),
            (f"sn_list{suffix}.txt", f"{self.base_cmd} sn list"),
            (f"sn_list{suffix}.json", f"{self.base_cmd} sn list --json"),
            (f"lvol_list{suffix}.txt", f"{self.base_cmd} lvol list"),
            (f"snapshot_list{suffix}.txt", f"{self.base_cmd} snapshot list"),
            (f"pool_list{suffix}.txt", f"{self.base_cmd} pool list"),
        ]
        if self.cluster_id:
            cmds += [
                (f"cluster_status{suffix}.txt", f"{self.base_cmd} cluster status {self.cluster_id}"),
                (f"cluster_list_tasks{suffix}.txt", f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0"),
                (f"cluster_capacity{suffix}.txt", f"{self.base_cmd} cluster get-capacity {self.cluster_id}"),
                (f"cluster_show{suffix}.txt", f"{self.base_cmd} cluster show {self.cluster_id}"),
                (f"cluster_get_logs{suffix}.txt", f"{self.base_cmd} cluster get-logs {self.cluster_id} --limit 0"),
            ]
        for filename, cmd in cmds:
            try:
                out, _ = k8s.exec_sbcli(cmd)
                with open(os.path.join(base_path, filename), "w") as fh:
                    fh.write(out or "")
            except Exception as e:
                self.logger.warning(f"[k8s collect_mgmt] {cmd}: {e}")

        # Collect subtasks for all master tasks (mirrors docker path behaviour)
        if self.cluster_id:
            try:
                tasks = k8s.get_cluster_tasks(self.cluster_id)
                for task in tasks:
                    tid = task["id"]
                    try:
                        out, _ = k8s.exec_sbcli(
                            f"{self.base_cmd} cluster get-subtasks {tid}"
                        )
                        if out and out.strip():
                            fname = f"subtask_{tid}{suffix}.txt"
                            with open(os.path.join(base_path, fname), "w") as fh:
                                fh.write(out)
                    except Exception as e:
                        self.logger.warning(f"[k8s collect_mgmt] get-subtasks {tid}: {e}")
            except Exception as e:
                self.logger.warning(f"[k8s collect_mgmt] subtask collection: {e}")

        try:
            storage_nodes = self.sbcli_utils.get_storage_nodes()
            for i, result in enumerate(storage_nodes["results"], 1):
                for fname, cmd in [
                    (f"node{i}_list_devices{suffix}.txt", f"{self.base_cmd} sn list-devices {result['uuid']}"),
                    (f"node{i}_check{suffix}.txt", f"{self.base_cmd} sn check {result['uuid']}"),
                    (f"node{i}_get{suffix}.txt", f"{self.base_cmd} sn get {result['uuid']}"),
                ]:
                    try:
                        out, _ = k8s.exec_sbcli(cmd)
                        with open(os.path.join(base_path, fname), "w") as fh:
                            fh.write(out or "")
                    except Exception as e:
                        self.logger.warning(f"[k8s collect_mgmt] {cmd}: {e}")
        except Exception as e:
            self.logger.warning(f"[k8s collect_mgmt] storage node loop: {e}")

        # Collect journalctl + dmesg final snapshot from client/fio nodes (accessible via SSH)
        for node in self.client_machines:
            try:
                node_log_dir = os.path.join(self.docker_logs_path, node)
                os.makedirs(node_log_dir, exist_ok=True)
                self.ssh_obj.exec_command(
                    node,
                    f"journalctl -k --no-tail >& {node_log_dir}/jounalctl_{node}{suffix}.txt"
                )
                self.ssh_obj.exec_command(
                    node,
                    f"dmesg -T >& {node_log_dir}/dmesg_{node}{suffix}.txt"
                )
                self.logger.info(f"[k8s collect_mgmt] journalctl+dmesg collected for client {node}")
            except Exception as e:
                self.logger.warning(f"[k8s collect_mgmt] journalctl/dmesg for {node}: {e}")

    def collect_management_details(self, post_teardown=False, suffix=None):
        if suffix is None:
            suffix = "_pre_teardown" if not post_teardown else "_post_teardown"
        if self.k8s_test:
            self._collect_management_details_k8s(suffix)
            return

        base_path = os.path.join(self.docker_logs_path, self.mgmt_nodes[0])
        cmd = f"{self.base_cmd} cluster list >& {base_path}/cluster_list{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster status {self.cluster_id} >& {base_path}/cluster_status{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster get-logs {self.cluster_id} --limit 0 >& {base_path}/cluster_get_logs{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0 >& {base_path}/cluster_list_tasks{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)

        # Collect subtasks for balancing_on_restart tasks
        try:
            tasks_out, _ = self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0],
                command=f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0"
            )
            for line in (tasks_out or "").splitlines():
                if "balancing_on_restart" not in line:
                    continue
                parts = [p.strip() for p in line.split("|")]
                # Table rows have a leading empty cell from '| id | ...'
                # Column layout: | id | function | status | ...
                tid = next((p for p in parts if p and p != "id"), None)
                if not tid:
                    continue
                sub_cmd = f"{self.base_cmd} cluster get-subtasks {tid} >& {base_path}/subtask_{tid}{suffix}.txt"
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=sub_cmd)
        except Exception as e:
            self.logger.warning(f"Failed to collect subtasks: {e}")

        cmd = f"{self.base_cmd} sn list >& {base_path}/sn_list{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)

        cmd = f"{self.base_cmd} sn list --json >& {base_path}/sn_list{suffix}.json"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)

        cmd = f"{self.base_cmd} cluster get-capacity {self.cluster_id} >& {base_path}/cluster_capacity{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)

        cmd = f"{self.base_cmd} cluster show {self.cluster_id} >& {base_path}/cluster_show{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} lvol list >& {base_path}/lvol_list{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} snapshot list >& {base_path}/snapshot_list{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} pool list >& {base_path}/pool_list{suffix}.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        node=1
        for result in storage_nodes['results']:
            cmd = f"{self.base_cmd} sn list-devices {result['uuid']} >& {base_path}/node{node}_list_devices{suffix}.txt"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            cmd = f"{self.base_cmd} sn check {result['uuid']} >& {base_path}/node{node}_check{suffix}.txt"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            cmd = f"{self.base_cmd} sn get {result['uuid']} >& {base_path}/node{node}_get{suffix}.txt"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            node+=1
        all_nodes = self.storage_nodes + self.mgmt_nodes + self.client_machines
        for node in all_nodes:
            base_path = os.path.join(self.docker_logs_path, node)
            cmd = f"journalctl -k --no-tail >& {base_path}/jounalctl_{node}-final.txt"

            self.ssh_obj.exec_command(node, cmd)
            cmd = f"dmesg -T >& {base_path}/dmesg_{node}-final.txt"
            self.ssh_obj.exec_command(node, cmd)

        try:
            if hasattr(self, "_stop_spdk_mem_thread"):
                self.logger.info("[SPDK-MEM] Stopping SPDK mem stats thread")
                self._stop_spdk_mem_thread = True

            if hasattr(self, "spdk_mem_thread"):
                thread = self.spdk_mem_thread
                if thread and thread.is_alive():
                    thread.join(timeout=120)
                    if thread.is_alive():
                        self.logger.warning(
                            "[SPDK-MEM] SPDK mem stats thread did not stop cleanly"
                        )
        except Exception as e:
            # Teardown must NEVER fail the test
            self.logger.warning(
                f"[SPDK-MEM] Exception during mem stats teardown: {str(e)}"
            )

    def _fetch_spdk_mem_stats_for_node(self, storage_node_ip, storage_node_id):
        """
        Fetch SPDK memory stats via env_dpdk_get_mem_stats.

        Behavior:
        - Runs RPC inside SPDK container
        - Reads JSON response written on host
        - Extracts dump filename OR defaults to /tmp/spdk_mem_dump.txt
        - Creates stable copy inside container
        - docker cp stable file to host
        - Moves JSON + TXT to mounted log path
        - Cleans up container temp files
        """

        self.logger.info(
            f"[DEBUG][SPDK-MEM] START node_id={storage_node_id}, ip={storage_node_ip}"
        )

        try:
            # ---------------------------------------------------------------
            # 1. Prepare paths
            # ---------------------------------------------------------------
            timestamp = time.strftime("%d-%m-%y-%H-%M-%S")

            host_json = f"/tmp/spdk_mem_stats_{storage_node_ip}_{timestamp}.json"
            host_txt = f"/tmp/spdk_mem_dump_{storage_node_ip}_{timestamp}.txt"

            final_dir = f"{self.docker_logs_path}/{storage_node_ip}/spdk_mem_stats"
            final_json = f"{final_dir}/spdk_mem_stats_{timestamp}.json"
            final_txt = f"{final_dir}/spdk_mem_dump_{timestamp}.txt"
            final_huge = f"{final_dir}/host_hugepages_{timestamp}.txt"

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Creating final directory: {final_dir}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo mkdir -p '{final_dir}'"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Writing host hugepage stats → {final_huge}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"cat /proc/meminfo | grep -i hug > '{final_huge}' || true"
            )

            # ---------------------------------------------------------------
            # 2. Find SPDK container
            # ---------------------------------------------------------------
            find_container_cmd = (
                "sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' || true"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Finding SPDK container on {storage_node_ip}"
            )

            container_out, _ = self.ssh_obj.exec_command(
                node=storage_node_ip,
                command=find_container_cmd
            )

            container_name = container_out.strip()

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Container discovery result: '{container_name}'"
            )

            if not container_name:
                self.logger.info(
                    "[DEBUG][SPDK-MEM] No SPDK container found, skipping node"
                )
                return

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Paths prepared | host_json={host_json}, host_txt={host_txt}"
            )

            # ---------------------------------------------------------------
            # 3. Run SPDK RPC (JSON redirected on HOST)
            # ---------------------------------------------------------------
            rpc_cmd = (
                f"sudo docker exec {container_name} "
                f"python spdk/scripts/rpc.py "
                f"-s /mnt/ramdisk/{container_name}/spdk.sock "
                f"env_dpdk_get_mem_stats > {host_json}"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Executing RPC: {rpc_cmd}"
            )

            self.ssh_obj.exec_command(storage_node_ip, rpc_cmd)

            # ---------------------------------------------------------------
            # 4. Parse JSON (fallback if needed)
            # ---------------------------------------------------------------
            container_txt = "/tmp/spdk_mem_dump.txt"  # DEFAULT

            self.logger.info(
                "[DEBUG][SPDK-MEM] Reading RPC JSON file"
            )

            json_out, _ = self.ssh_obj.exec_command(
                storage_node_ip,
                f"cat {host_json} || true"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] RPC JSON content: {json_out.strip()}"
            )

            try:
                data = json.loads(json_out)
                if isinstance(data, dict) and data.get("filename"):
                    container_txt = data["filename"]
                    self.logger.info(
                        f"[DEBUG][SPDK-MEM] Using filename from RPC: {container_txt}"
                    )
                else:
                    self.logger.info(
                        f"[DEBUG][SPDK-MEM] No filename in RPC, defaulting to {container_txt}"
                    )
            except Exception:
                self.logger.info(
                    f"[DEBUG][SPDK-MEM] JSON parse failed, defaulting to {container_txt}"
                )

            # ---------------------------------------------------------------
            # 5. Create stable copy INSIDE container
            # ---------------------------------------------------------------
            container_txt_tmp = f"{container_txt}.{timestamp}.copy"

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Creating stable container copy: {container_txt_tmp}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo docker exec {container_name} "
                f"cp {container_txt} {container_txt_tmp}"
            )

            # ---------------------------------------------------------------
            # 6. docker cp stable file → host (timeout protected)
            # ---------------------------------------------------------------
            self.logger.info(
                f"[DEBUG][SPDK-MEM] Copying dump to host: {host_txt}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo timeout 30 docker cp "
                f"{container_name}:{container_txt_tmp} {host_txt}"
            )

            # ---------------------------------------------------------------
            # 7. Move files to mounted log path
            # ---------------------------------------------------------------

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Moving JSON → {final_json}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo mv '{host_json}' '{final_json}'"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] Moving TXT → {final_txt}"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo mv '{host_txt}' '{final_txt}'"
            )

            # ---------------------------------------------------------------
            # 8. Cleanup container temp files
            # ---------------------------------------------------------------
            self.logger.info(
                "[DEBUG][SPDK-MEM] Cleaning container temp files"
            )

            self.ssh_obj.exec_command(
                storage_node_ip,
                f"sudo docker exec {container_name} "
                f"rm -f {container_txt_tmp}"
            )

            self.logger.info(
                f"[DEBUG][SPDK-MEM] SUCCESS node={storage_node_ip}"
            )

        except Exception as e:
            self.logger.info(
                f"[DEBUG][SPDK-MEM] FAILURE node={storage_node_ip} error={str(e)}"
            )

    
    def _fetch_spdk_mem_stats_for_node_k8s(self, storage_node_ip, storage_node_id):
        """Collect memory stats from SPDK pod via kubectl exec (k8s mode)."""
        try:
            k8s = self.sbcli_utils.k8s
            timestamp = time.strftime("%d-%m-%y-%H-%M-%S")
            final_dir = f"{self.docker_logs_path}/{storage_node_ip}/spdk_mem_stats"
            os.makedirs(final_dir, exist_ok=True)

            pod_name = k8s.get_spdk_pod_name(storage_node_ip)

            meminfo_out, _ = k8s._exec_kubectl(
                f"kubectl exec {pod_name} -c spdk-container -n {k8s.namespace} -- "
                f"cat /proc/meminfo",
                supress_logs=True,
            )
            meminfo_file = f"{final_dir}/meminfo_{timestamp}.txt"
            with open(meminfo_file, "w") as f:
                f.write(meminfo_out)

            free_out, _ = k8s._exec_kubectl(
                f"kubectl exec {pod_name} -c spdk-container -n {k8s.namespace} -- "
                f"free -m",
                supress_logs=True,
            )
            free_file = f"{final_dir}/free_{timestamp}.txt"
            with open(free_file, "w") as f:
                f.write(free_out)

            self.logger.info(
                f"[SPDK-MEM-K8s] Saved mem stats for {storage_node_ip} (pod={pod_name})"
            )
        except Exception as e:
            self.logger.info(
                f"[SPDK-MEM-K8s] FAILURE node={storage_node_ip} error={e}"
            )

    def _spdk_mem_stats_worker(self, interval_sec=60):
        """
        Background thread that collects SPDK mem stats every minute
        from all storage nodes.
        """
        self.logger.info("[SPDK-MEM] SPDK mem stats thread started")

        while not getattr(self, "_stop_spdk_mem_thread", False):
            try:
                for node_id in self.sn_nodes_with_sec:
                    try:
                        node_details = self.sbcli_utils.get_storage_node_details(node_id)
                        node_ip = node_details[0]["mgmt_ip"]
                    except Exception:
                        # Node may be offline during outage
                        continue

                    if self.k8s_test:
                        self._fetch_spdk_mem_stats_for_node_k8s(
                            storage_node_ip=node_ip,
                            storage_node_id=node_id
                        )
                    else:
                        self._fetch_spdk_mem_stats_for_node(
                            storage_node_ip=node_ip,
                            storage_node_id=node_id
                        )

            except Exception as e:
                self.logger.info(
                    f"[SPDK-MEM] Worker loop exception: {str(e)}"
                )

            time.sleep(interval_sec)

        self.logger.info("[SPDK-MEM] SPDK mem stats thread stopped")

            
    def teardown(self, delete_lvols=True, close_ssh=True):
        """Contains teradown required post test case execution
        """
        self.logger.info("Inside teardown function")

        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        for node in fio_nodes:
            self.ssh_obj.exec_command(node=node,
                                      command="sudo tmux kill-server")
            self.ssh_obj.kill_processes(node=node,
                                        process_name="fio")

        self.stop_root_monitor()

        retry_check = 100
        while retry_check:
            exit_while = True
            for node in fio_nodes:
                fio_process = self.ssh_obj.find_process_name(
                    node=node,
                    process_name="fio --name"
                )
                exit_while = exit_while and len(fio_process) <= 2
            if exit_while:
                break
            else:
                self.logger.info(f"Fio process should exit after kill. Still waiting: {fio_process}")
                retry_check -= 1
                sleep_n_sec(10)

        if retry_check <= 0:
            self.logger.info("FIO did not exit completely after kill and wait. "
                             "Some hanging mount points could be present. "
                             "Needs manual cleanup.")
        if delete_lvols:
            try:
                lvols = self.sbcli_utils.list_lvols()
                self.unmount_all(base_path=self.mount_path)
                self.unmount_all(base_path="/mnt/")
                sleep_n_sec(2)
                for node in fio_nodes:
                    self.ssh_obj.unmount_path(node=node,
                                            device=self.mount_path)
                sleep_n_sec(2)
                if lvols is not None:
                    for _, lvol_id in lvols.items():
                        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                        nqn = lvol_details[0]["nqn"]
                        for node in fio_nodes:
                            self.ssh_obj.unmount_path(node=node,
                                                    device=self.mount_path)
                            sleep_n_sec(2)
                            self.ssh_obj.exec_command(node=node,
                                                    command=f"sudo nvme disconnect -n {nqn}")
                            sleep_n_sec(2)
                    self.disconnect_lvols()
                    sleep_n_sec(2)
                self.sbcli_utils.delete_all_lvols()
                sleep_n_sec(2)
                if not self.k8s_test:
                    self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
                    sleep_n_sec(2)
                self.sbcli_utils.delete_all_storage_pools()
                sleep_n_sec(2)
                latest_util = self.get_latest_cluster_util()
                size_used = latest_util["size_used"]
                if size_used >= 500 * 1024 * 1024:
                    self.logger.warning(f"Cluster capacity more than 500MB after cleanup: {size_used // (1024 * 1024)}MB")
                # for node in self.fio_node:
                #     self.ssh_obj.remove_dir(node, "/mnt/*")
            except Exception as _:
                self.logger.info(traceback.format_exc())

        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.exec_command(node=node,
                                          command="sudo tmux kill-server")
                result = self.ssh_obj.check_remote_spdk_logs_for_keyword(node_ip=node,
                                                                         log_dir=self.docker_logs_path,
                                                                         test_name=self.test_name)
                for file, lines in result.items():
                    if lines:
                        self.logger.info(f"\n{file}:")
                        for line in lines:
                            self.logger.info(f"  -> {line}")

            self.ssh_obj.copy_logs_and_configs_to_nfs(
                logs_path=self.docker_logs_path, storage_nodes=self.storage_nodes
            )
        if close_ssh and not self.k8s_test:
            for node, ssh in self.ssh_obj.ssh_connections.items():
                self.logger.info(f"Closing node ssh connection for {node}")
                ssh.close()

        try:
            if self.ec2_resource:
                instance_id = self.common_utils.get_instance_id_by_name(ec2_resource=self.ec2_resource,
                                                                        instance_name="e2e-new-instance")
                if instance_id:
                    self.common_utils.terminate_instance(ec2_resource=self.ec2_resource,
                                                         instance_id=instance_id)
        except Exception as e:
            self.logger.info(f"Error while deleting instance: {e}")
            self.logger.info(traceback.format_exc())

    def get_logs_path(self):
        """Print logs path on nfs
        """
        self.logger.info(f"Logs Path: {self.docker_logs_path}")

    # ------------------------------------------------------------------
    # Graylog log export helpers
    # ------------------------------------------------------------------

    def _build_graylog_session(self):
        """Create a requests.Session pre-configured for Graylog API auth."""
        import requests
        session = requests.Session()
        session.auth = ("admin", self.cluster_secret)
        session.headers.update({
            "X-Requested-By": "sb-log-collector",
            "Accept": "application/json",
        })
        return session

    def _graylog_base_url(self):
        """Return the Graylog API base URL for the first management node."""
        mgmt_ip = self.mgmt_nodes[0]
        if self.k8s_test:
            return f"http://{mgmt_ip}:9000/api"
        return f"http://{mgmt_ip}/graylog/api"

    def _graylog_discover_containers(self, session, base_url, from_iso, to_iso):
        """Discover all unique (container_name, source) pairs in the time window.

        Strategy (in order):
          1. OpenSearch nested terms aggregation (reliable, finds ALL pairs)
          2. Graylog time-slice sampling (last resort)

        Returns:
            list[tuple[str, str]]: (container_name, source) pairs,
            or empty list on failure.
        """
        cname_field = (
            "kubernetes_container_name" if self.k8s_test else "container_name"
        )

        # ------ 1. OpenSearch nested terms aggregation ------
        self.logger.info(
            "[graylog-export] Trying OpenSearch terms aggregation for discovery"
        )
        try:
            os_url = self._opensearch_base_url()
            os_session = self._build_opensearch_session()
            from_ms = int(
                datetime.fromisoformat(
                    from_iso.replace("Z", "+00:00")
                ).timestamp() * 1000
            )
            to_ms = int(
                datetime.fromisoformat(
                    to_iso.replace("Z", "+00:00")
                ).timestamp() * 1000
            )
            os_pairs = self._os_discover_containers(
                os_session, os_url, from_ms, to_ms
            )
            if os_pairs:
                return os_pairs
        except Exception as exc:
            self.logger.warning(
                f"[graylog-export] OpenSearch discovery failed: {exc}"
            )

        # ------ 2. Graylog search — time-slice sampling (last resort) ------
        # Large offsets cause Graylog 500 errors, so we slice the time
        # window into chunks and sample the first page of each chunk.
        self.logger.info(
            "[graylog-export] Falling back to Graylog time-slice discovery"
        )
        search_url = f"{base_url}/search/universal/absolute"
        pairs = set()

        t_start = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
        t_end = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))
        total_minutes = (t_end - t_start).total_seconds() / 60

        # Use ~10 slices, minimum 1 minute each
        num_slices = max(1, min(20, int(total_minutes / 5)))
        slice_delta = (t_end - t_start) / num_slices

        self.logger.info(
            f"[graylog-export] Sampling {num_slices} time slices "
            f"across {total_minutes:.0f} minutes"
        )

        for i in range(num_slices):
            s_from = t_start + slice_delta * i
            s_to = t_start + slice_delta * (i + 1)
            s_from_iso = s_from.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            s_to_iso = s_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params = {
                "query": "*",
                "from": s_from_iso,
                "to": s_to_iso,
                "limit": 500,
                "offset": 0,
                "sort": "timestamp:asc",
                "fields": f"timestamp,source,{cname_field}",
            }
            try:
                resp = session.get(search_url, params=params, timeout=60)
                if resp.ok:
                    messages = resp.json().get("messages", [])
                    for m in messages:
                        msg = m.get("message", {})
                        name = msg.get(cname_field, "")
                        source = msg.get("source", "")
                        if name:
                            pairs.add((name, source))
                    self.logger.info(
                        f"[graylog-export] Slice {i+1}/{num_slices}: "
                        f"{len(messages)} msgs, "
                        f"{len(pairs)} unique (container, source) pairs so far"
                    )
                else:
                    self.logger.warning(
                        f"[graylog-export] Slice {i+1}/{num_slices} "
                        f"returned HTTP {resp.status_code}"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[graylog-export] Slice {i+1}/{num_slices} "
                    f"failed: {exc}"
                )

        if pairs:
            self.logger.info(
                f"[graylog-export] Discovered {len(pairs)} "
                f"(container, source) pairs via time-slice sampling"
            )
            return list(pairs)

        self.logger.warning(
            "[graylog-export] No container names found via any method"
        )
        return []

    @staticmethod
    def _gl_escape(value):
        """Escape Lucene special characters (dots) in Graylog field queries."""
        return value.replace(".", "\\.")

    # ------------------------------------------------------------------
    # OpenSearch helpers (fallback when Graylog endpoints are unavailable)
    # ------------------------------------------------------------------

    def _opensearch_base_url(self):
        """Return the OpenSearch base URL for the first management node."""
        mgmt_ip = self.mgmt_nodes[0]
        return f"http://{mgmt_ip}/opensearch"

    def _build_opensearch_session(self):
        """Create a requests.Session for OpenSearch (no auth needed)."""
        import requests
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})
        return session

    def _os_get_index(self, session, os_url):
        """Discover graylog indices in OpenSearch.

        Returns a comma-separated string of index names, or '_all'.
        """
        try:
            r = session.get(
                f"{os_url}/_cat/indices?h=index&format=json", timeout=10
            )
            r.raise_for_status()
            indices = sorted(
                i["index"]
                for i in r.json()
                if i["index"].startswith("graylog")
                and not i["index"].startswith(".")
            )
            if indices:
                return ",".join(indices)
        except Exception as exc:
            self.logger.warning(
                f"[graylog-export] Could not discover OpenSearch indices: {exc}"
            )
        return "_all"

    def _os_probe(self, session, os_url, index, from_ms, to_ms):
        """Probe OpenSearch to discover field names and doc count.

        Returns dict with: ts_field, cname_field, window_count
        """
        result = {
            "ts_field": "timestamp",
            "cname_field": "container_name",
            "window_count": 0,
        }

        # Sample document to detect field names
        try:
            r = session.post(
                f"{os_url}/{index}/_search",
                json={"size": 1, "query": {"match_all": {}}},
                timeout=10,
            )
            if r.ok:
                hits = r.json().get("hits", {}).get("hits", [])
                if hits:
                    src = hits[0].get("_source", {})
                    if "@timestamp" in src:
                        result["ts_field"] = "@timestamp"
                    for candidate in (
                        "container_name", "container_id", "containerName",
                        "_container_name", "docker_container_name",
                    ):
                        if candidate in src:
                            result["cname_field"] = candidate
                            break
        except Exception as exc:
            self.logger.warning(
                f"[graylog-export] OpenSearch probe (sample) failed: {exc}"
            )

        # Count in time window
        ts = result["ts_field"]
        try:
            r = session.post(
                f"{os_url}/{index}/_count",
                json={
                    "query": {
                        "range": {
                            ts: {
                                "gte": from_ms, "lte": to_ms,
                                "format": "epoch_millis",
                            }
                        }
                    }
                },
                timeout=10,
            )
            if r.ok:
                result["window_count"] = r.json().get("count", 0)
        except Exception as exc:
            self.logger.warning(
                f"[graylog-export] OpenSearch probe (count) failed: {exc}"
            )

        return result

    def _os_discover_containers(self, session, os_url, from_ms, to_ms):
        """Discover (container_name, source) pairs via OpenSearch aggregation.

        Uses a nested terms aggregation: container_name -> source.
        Tries ``field.keyword`` first, then the raw field name.

        Returns:
            list[tuple[str, str]]: (container_name, source) pairs,
            or empty list on failure.
        """
        index = self._os_get_index(session, os_url)
        probe = self._os_probe(session, os_url, index, from_ms, to_ms)

        self.logger.info(
            f"[graylog-export] OpenSearch: index={index}  "
            f"ts_field={probe['ts_field']}  "
            f"cname_field={probe['cname_field']}  "
            f"docs_in_window={probe['window_count']}"
        )

        if probe["window_count"] == 0:
            self.logger.warning(
                "[graylog-export] OpenSearch: no documents in time window"
            )
            return []

        cname_f = probe["cname_field"]
        # Try .keyword first, then raw field name
        for suffix in [".keyword", ""]:
            agg_cname = f"{cname_f}{suffix}"
            agg_source = f"source{suffix}"
            body = {
                "size": 0,
                "query": {
                    "range": {
                        probe["ts_field"]: {
                            "gte": from_ms, "lte": to_ms,
                            "format": "epoch_millis",
                        }
                    }
                },
                "aggs": {
                    "containers": {
                        "terms": {
                            "field": agg_cname,
                            "size": 500,
                        },
                        "aggs": {
                            "sources": {
                                "terms": {
                                    "field": agg_source,
                                    "size": 100,
                                }
                            }
                        },
                    }
                },
            }
            try:
                r = session.post(
                    f"{os_url}/{index}/_search", json=body, timeout=30
                )
                if r.ok:
                    ctr_buckets = (
                        r.json()
                        .get("aggregations", {})
                        .get("containers", {})
                        .get("buckets", [])
                    )
                    if ctr_buckets:
                        pairs = []
                        for cb in ctr_buckets:
                            cname = cb["key"]
                            src_buckets = (
                                cb.get("sources", {}).get("buckets", [])
                            )
                            if src_buckets:
                                for sb in src_buckets:
                                    pairs.append((cname, sb["key"]))
                            else:
                                pairs.append((cname, ""))
                        self.logger.info(
                            f"[graylog-export] OpenSearch discovered "
                            f"{len(ctr_buckets)} containers, "
                            f"{len(pairs)} (container, source) pairs "
                            f"(field={agg_cname})"
                        )
                        return pairs
                    else:
                        self.logger.info(
                            f"[graylog-export] OpenSearch terms agg on "
                            f"'{agg_cname}' returned no buckets, trying next"
                        )
                else:
                    self.logger.info(
                        f"[graylog-export] OpenSearch terms agg on "
                        f"'{agg_cname}' returned HTTP {r.status_code}, "
                        f"trying next"
                    )
            except Exception as exc:
                self.logger.info(
                    f"[graylog-export] OpenSearch terms agg on "
                    f"'{agg_cname}' failed: {exc}, trying next"
                )

        self.logger.warning(
            "[graylog-export] OpenSearch terms aggregation found no containers"
        )
        return []

    def _os_fetch_container_logs(
        self, session, os_url, container_name, source,
        from_iso, to_iso, out_path, probe_cache=None,
    ):
        """Fetch logs from OpenSearch using the scroll API.

        Adapted from scripts/collect_logs.py opensearch_fetch_all().
        Returns number of lines written.
        """
        import requests as _requests

        PAGE_SIZE = 1000

        from_ms = int(
            datetime.fromisoformat(
                from_iso.replace("Z", "+00:00")
            ).timestamp() * 1000
        )
        to_ms = int(
            datetime.fromisoformat(
                to_iso.replace("Z", "+00:00")
            ).timestamp() * 1000
        )

        # One-time probe (cached across calls)
        if probe_cache is None:
            probe_cache = {}
        if "index" not in probe_cache:
            probe_cache["index"] = self._os_get_index(session, os_url)
            probe_cache["probe"] = self._os_probe(
                session, os_url, probe_cache["index"], from_ms, to_ms
            )

        index = probe_cache["index"]
        probe = probe_cache["probe"]
        ts_f = probe["ts_field"]
        cname_f = probe["cname_field"]

        # Build query
        esc = container_name.replace("/", "\\/").replace(":", "\\:")
        must_clauses = [
            {
                "range": {
                    ts_f: {
                        "gte": from_ms, "lte": to_ms,
                        "format": "epoch_millis",
                    }
                }
            },
            {
                "query_string": {
                    "default_field": cname_f,
                    "query": f"*{esc}*",
                    "analyze_wildcard": True,
                }
            },
        ]
        if source:
            must_clauses.append({
                "query_string": {
                    "default_field": "source",
                    "query": f'"{source}"',
                }
            })
        body = {
            "query": {"bool": {"must": must_clauses}},
            "sort": [{ts_f: {"order": "asc"}}],
            "size": PAGE_SIZE,
            "_source": [ts_f, "source", cname_f, "level", "message"],
        }

        def _fmt(src):
            ts = src.get("timestamp", src.get(ts_f, ""))
            s = src.get("source", "")
            cname = src.get("container_name", src.get(cname_f, ""))
            lvl = src.get("level", "")
            text = str(src.get("message", "")).replace("\n", "\\n")
            return f"{ts}  src={s}  ctr={cname}  lvl={lvl}  {text}"

        init_url = f"{os_url}/{index}/_search?scroll=2m"
        written = 0

        try:
            r = session.post(init_url, json=body, timeout=60)
            if not r.ok:
                self.logger.warning(
                    f"[graylog-export] OpenSearch scroll failed for "
                    f"{container_name}: HTTP {r.status_code} {r.text[:300]}"
                )
                Path(out_path).touch()
                return 0
        except _requests.RequestException as exc:
            self.logger.warning(
                f"[graylog-export] OpenSearch scroll failed for "
                f"{container_name}: {exc}"
            )
            Path(out_path).touch()
            return 0

        data = r.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        total = (
            total.get("value", total) if isinstance(total, dict)
            else int(total or 0)
        )

        with open(out_path, "w") as fh:
            while hits:
                for h in hits:
                    src = h.get("_source", {})
                    if ts_f != "timestamp":
                        src["timestamp"] = src.get(ts_f, "")
                    if cname_f != "container_name":
                        src["container_name"] = src.get(cname_f, "")
                    fh.write(_fmt(src) + "\n")
                    written += 1
                if len(hits) < PAGE_SIZE or not scroll_id:
                    break
                try:
                    sc_r = session.post(
                        f"{os_url}/_search/scroll",
                        json={"scroll": "2m", "scroll_id": scroll_id},
                        timeout=60,
                    )
                    sc_r.raise_for_status()
                    sc_data = sc_r.json()
                    scroll_id = sc_data.get("_scroll_id", scroll_id)
                    hits = sc_data.get("hits", {}).get("hits", [])
                except _requests.RequestException as exc:
                    self.logger.warning(
                        f"[graylog-export] Scroll continuation failed for "
                        f"{container_name}: {exc}"
                    )
                    break

        # Release scroll context
        if scroll_id:
            try:
                session.delete(
                    f"{os_url}/_search/scroll",
                    json={"scroll_id": scroll_id},
                    timeout=10,
                )
            except Exception:
                pass

        return written

    def _graylog_fetch_container_logs(
        self, session, base_url, container_name, source,
        from_iso, to_iso, out_path,
    ):
        """Fetch all log lines for *container_name* on *source* and write to *out_path*.

        Uses paginated Graylog search (page size 1000).  If the total exceeds
        100 000, the window is split into 10-minute sub-windows — the same
        strategy used by ``simplyblock_core/scripts/collect_logs.py``.

        Returns:
            int: number of lines written.
        """
        import requests as _requests

        PAGE_SIZE = 1000
        MAX_RESULT_WINDOW = 100_000

        # The query uses the mode-specific field name, but the fields list
        # and formatter always use "container_name" — matching collect_logs.py.
        cname_query_field = (
            "kubernetes_container_name" if self.k8s_test else "container_name"
        )
        search_url = f"{base_url}/search/universal/absolute"
        # Use wildcard so partial names work (e.g. "spdk_8080" matches
        # "/spdk_8080", "SNodeAPI" matches "simplyblock_SNodeAPI.1.xyz")
        esc_name = self._gl_escape(container_name)
        query = f'{cname_query_field}:*{esc_name}*'
        if source:
            query += f' AND source:"{source}"'

        def _fetch_page(q, f_iso, t_iso, limit, offset):
            params = {
                "query": q, "from": f_iso, "to": t_iso,
                "limit": limit, "offset": offset,
                "sort": "timestamp:asc",
                "fields": "timestamp,source,container_name,level,message",
            }
            try:
                resp = session.get(
                    search_url, params=params, timeout=90,
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
            except _requests.RequestException as exc:
                self.logger.warning(
                    f"[graylog-export] page request failed "
                    f"(offset={offset}): {exc}"
                )
                return None, 0

            if not resp.text.strip():
                self.logger.warning(
                    f"[graylog-export] empty response "
                    f"(offset={offset}, status={resp.status_code})"
                )
                return None, 0
            try:
                data = resp.json()
            except ValueError as exc:
                self.logger.warning(
                    f"[graylog-export] invalid JSON "
                    f"(offset={offset}): {exc}"
                )
                return None, 0
            return data.get("messages", []), data.get("total_results", 0)

        def _fmt(msg):
            ts = msg.get("timestamp", "")
            src = msg.get("source", "")
            cname = msg.get("container_name", "")
            lvl = msg.get("level", "")
            text = str(msg.get("message", "")).replace("\n", "\\n")
            return f"{ts}  src={src}  ctr={cname}  lvl={lvl}  {text}"

        def _write_window(fh, q, f_iso, t_iso):
            written = 0
            offset = 0
            msgs, total = _fetch_page(q, f_iso, t_iso, 1, 0)
            if msgs is None:
                return 0
            while offset < total:
                msgs, _ = _fetch_page(q, f_iso, t_iso, PAGE_SIZE, offset)
                if not msgs:
                    break
                for m in msgs:
                    fh.write(_fmt(m.get("message", {})) + "\n")
                    written += 1
                offset += len(msgs)
                if len(msgs) < PAGE_SIZE:
                    break
            return written

        # Probe total result count
        msgs, total = _fetch_page(query, from_iso, to_iso, 1, 0)
        if msgs is None:
            Path(out_path).touch()
            return 0

        written = 0
        with open(out_path, "w") as fh:
            if total <= MAX_RESULT_WINDOW:
                written = _write_window(fh, query, from_iso, to_iso)
            else:
                # Split into 10-minute sub-windows
                self.logger.info(
                    f"[graylog-export] {container_name}: >100k entries, "
                    f"using 10-min sub-windows"
                )
                t = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
                t_end = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))
                chunk = timedelta(minutes=10)
                while t < t_end:
                    chunk_end = min(t + chunk, t_end)
                    c_from = t.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    c_to = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    written += _write_window(fh, query, c_from, c_to)
                    t = chunk_end

        return written

    def export_graylog_logs(self):
        """Export all container logs from Graylog / OpenSearch for the test window.

        Each (container, source) pair gets its own file under
        ``<docker_logs_path>/graylog_logs/<container>__<source>.log``.
        This ensures containers like SNodeAPI that run on every node get
        separate log files per node.

        Strategy:
          - Discovery returns (container_name, source) pairs.
          - Fetch via Graylog when reachable, otherwise OpenSearch scroll API.

        This method is fully resilient -- all exceptions are caught and logged
        so that it never disrupts the teardown sequence.
        """
        try:
            import requests  # noqa: F811
        except ImportError:
            self.logger.warning(
                "[graylog-export] 'requests' library not available, skipping"
            )
            return

        if not getattr(self, "mgmt_nodes", None):
            self.logger.warning(
                "[graylog-export] No management nodes available, skipping"
            )
            return
        if not self.cluster_secret:
            self.logger.warning(
                "[graylog-export] No cluster secret available, skipping"
            )
            return
        if not self.test_start_time_utc:
            self.logger.warning(
                "[graylog-export] test_start_time_utc not set, skipping"
            )
            return
        if not self.docker_logs_path:
            self.logger.warning(
                "[graylog-export] docker_logs_path not set, skipping"
            )
            return

        try:
            from_iso = self.test_start_time_utc.strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            to_iso = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )

            self.logger.info(
                f"[graylog-export] Exporting logs: {from_iso} -> {to_iso}"
            )

            base_url = self._graylog_base_url()
            session = self._build_graylog_session()
            os_url = self._opensearch_base_url()
            os_session = self._build_opensearch_session()

            # Check OpenSearch reachability
            opensearch_ok = False
            try:
                r = os_session.get(
                    f"{os_url}/_cluster/health", timeout=10
                )
                if r.status_code == 200:
                    opensearch_ok = True
                    self.logger.info("[graylog-export] OpenSearch is reachable")
                else:
                    self.logger.warning(
                        f"[graylog-export] OpenSearch returned HTTP "
                        f"{r.status_code}"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[graylog-export] OpenSearch unreachable ({exc})"
                )

            # Check Graylog reachability (used for fallback discovery)
            graylog_ok = False
            try:
                r = session.get(f"{base_url}/system", timeout=10)
                if r.status_code == 200:
                    graylog_ok = True
                    self.logger.info("[graylog-export] Graylog is reachable")
            except Exception:
                pass

            if not opensearch_ok and not graylog_ok:
                self.logger.warning(
                    "[graylog-export] Neither OpenSearch nor Graylog "
                    "is reachable, skipping export"
                )
                return

            # Discover (container_name, source) pairs
            # _graylog_discover_containers tries:
            #   1) OpenSearch nested agg  2) Graylog time-slice sampling
            pairs = self._graylog_discover_containers(
                session, base_url, from_iso, to_iso
            )

            if not pairs:
                self.logger.warning(
                    "[graylog-export] No containers discovered, "
                    "skipping export"
                )
                return

            # Create output directory
            graylog_dir = os.path.join(self.docker_logs_path, "graylog_logs")
            os.makedirs(graylog_dir, exist_ok=True)

            # Prefer OpenSearch scroll for fetching (handles large time
            # windows reliably); fall back to Graylog only if needed.
            use_opensearch = opensearch_ok
            self.logger.info(
                f"[graylog-export] Fetching logs for {len(pairs)} "
                f"(container, source) pairs -> {graylog_dir}  "
                f"(via {'OpenSearch' if use_opensearch else 'Graylog'})"
            )

            def _safe(s):
                return (
                    s.replace("/", "_").replace("\\", "_")
                    .replace(":", "_").strip("_")
                ) or "unnamed"

            total_lines = 0
            os_probe_cache = {}  # shared across OpenSearch calls
            for container_name, source in sorted(pairs):
                # Build filename: container__source.log
                safe_cname = _safe(container_name)
                if source:
                    safe_source = _safe(source)
                    fname = f"{safe_cname}__{safe_source}.log"
                else:
                    fname = f"{safe_cname}.log"
                out_path = os.path.join(graylog_dir, fname)

                label = f"{container_name}@{source}" if source else container_name
                try:
                    if use_opensearch:
                        n = self._os_fetch_container_logs(
                            os_session, os_url,
                            container_name, source,
                            from_iso, to_iso, out_path,
                            probe_cache=os_probe_cache,
                        )
                    else:
                        n = self._graylog_fetch_container_logs(
                            session, base_url,
                            container_name, source,
                            from_iso, to_iso, out_path,
                        )
                    total_lines += n
                    self.logger.info(
                        f"[graylog-export]   {label}: {n} lines"
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[graylog-export] Failed to fetch {label}: {exc}"
                    )

            self.logger.info(
                f"[graylog-export] Complete: {total_lines} total lines "
                f"from {len(pairs)} (container, source) pairs"
            )

        except Exception as exc:
            self.logger.warning(
                f"[graylog-export] Unexpected error, skipping: {exc}"
            )

    def _get_all_nodes(self):
        """Return ordered, de-duplicated list of mgmt + storage nodes."""
        nodes = []
        if getattr(self, "mgmt_nodes", None):
            nodes.extend(self.mgmt_nodes)
        if getattr(self, "storage_nodes", None):
            nodes.extend(self.storage_nodes)
        seen = set()
        ordered = []
        for n in nodes:
            if n not in seen:
                seen.add(n)
                ordered.append(n)
        return ordered

    def cleanup_root_when_high_usage(self, threshold: int = None):
        """
        For each mgmt/storage node, if /root usage >= threshold,
        delete /root/distrib_* , /root/bdev_* , and /etc/simplyblock/LVS_* ONLY on that node.

        threshold: percentage int. Default from env ROOT_DISK_THRESHOLD or 80.
        """
        thr = threshold if threshold is not None else int(os.getenv("ROOT_DISK_THRESHOLD", "80"))

        def _get_root_usage_pct(node: str) -> int:
            # POSIX-safe: df -P /root -> 2nd line, 5th col (Use%)
            cmd = r"df -P /root | awk 'NR==2{print $5}' | tr -dc '0-9'"
            out, _ = self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)
            s = (out or "").strip()
            try:
                return int(s)
            except Exception:
                self.logger.warning(f"Could not parse /root usage for {node!r} from output: {out!r}")
                return -1

        for node in self._get_all_nodes():
            used = _get_root_usage_pct(node)
            if used < 0:
                self.logger.warning(f"[{node}] Skipping cleanup (unknown /root usage).")
                continue

            if used >= thr:
                self.logger.warning(f"[{node}] /root usage {used}% >= {thr}%. Cleaning heavy files...")
                # Safe deletes (handles both files/dirs, globs allowed)
                self.ssh_obj.delete_file_dir(node=node, entity="/root/distrib_*", recursive=True)
                self.ssh_obj.delete_file_dir(node=node, entity="/root/bdev_*", recursive=True)
                self.ssh_obj.delete_file_dir(node=node, entity="/etc/simplyblock/LVS_*", recursive=True)

                # Recheck
                used_after = _get_root_usage_pct(node)
                if used_after >= 0:
                    self.logger.info(f"[{node}] /root usage after cleanup: {used_after}% (was {used}%)")
                else:
                    self.logger.info(f"[{node}] Cleanup done; could not re-check usage.")
            else:
                self.logger.info(f"[{node}] /root usage {used}% < {thr}%. No cleanup needed.")

    def start_root_monitor(self, interval_minutes: int = None, threshold: int = None):
        """
        Start a background thread that checks /root usage periodically
        and cleans if usage >= threshold on a per-node basis.

        interval_minutes: int, default from env ROOT_MONITOR_INTERVAL_MIN or 60
        threshold: int %, default from env ROOT_DISK_THRESHOLD or 80
        """
        if self.k8s_test:
            return
        if hasattr(self, "_root_monitor_thread") and getattr(self, "_root_monitor_thread").is_alive():
            self.logger.info("Root monitor already running; skipping start.")
            return

        poll_mins = interval_minutes if interval_minutes is not None else int(os.getenv("ROOT_MONITOR_INTERVAL_MIN", "60"))
        thr = threshold if threshold is not None else int(os.getenv("ROOT_DISK_THRESHOLD", "80"))

        self._root_monitor_stop = threading.Event()

        def _monitor_loop():
            self.logger.info(
                f"[RootMonitor] Started. interval={poll_mins}m threshold={thr}% nodes={len(self._get_all_nodes())}"
            )
            while not self._root_monitor_stop.is_set():
                try:
                    self.cleanup_root_when_high_usage(thr)
                except Exception as e:
                    self.logger.error(f"[RootMonitor] Error during cleanup: {e}")
                # Sleep in 10s slices so we can stop promptly
                total = poll_mins * 60
                step = 10
                waited = 0
                while waited < total and not self._root_monitor_stop.is_set():
                    time.sleep(step)
                    waited += step
            self.logger.info("[RootMonitor] Exiting.")

        t = threading.Thread(target=_monitor_loop, name="RootMonitor", daemon=True)
        t.start()
        self._root_monitor_thread = t

    def stop_root_monitor(self):
        """Gracefully stop the background /root monitor."""
        if hasattr(self, "_root_monitor_stop") and self._root_monitor_stop:
            self._root_monitor_stop.set()
        if hasattr(self, "_root_monitor_thread") and self._root_monitor_thread:
            self._root_monitor_thread.join(timeout=5)
        self.logger.info("Stopped background root monitor.")


    def validations(self, node_uuid, node_status, device_status, lvol_status,
                    health_check_status, device_health_check):
        """Validates node, devices, lvol status with expected status

        Args:
            node_uuid (str): UUID of node to validate
            node_status (str): Expected node status
            device_status (str): Expected device status
            lvol_status (str): Expected lvol status
            health_check_status (bool): Expected health check status
        """
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node_uuid)
        self.logger.info(f"Storage Node Details: {node_details}")
        self.sbcli_utils.get_device_details(storage_node_id=node_uuid)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)


        if isinstance(node_status, list):
            if node_details[0]["status"] in ["down"]:
                self.logger.info("Waiting for node to come online!")
                sleep_n_sec(120)
            assert node_details[0]["status"] in node_status, \
                f"Node {node_uuid} is not in {node_status} state. Actual: {node_details[0]['status']}"
        else:
            if node_details[0]["status"] == "down":
                self.logger.info("Waiting for node to come online!")
                sleep_n_sec(120)
            assert node_details[0]["status"] == node_status, \
                f"Node {node_uuid} is not in {node_status} state. Actual: {node_details[0]['status']}"
        
        # TODO: Issue during validations: Uncomment once fixed
        # https://simplyblock.atlassian.net/browse/SFAM-1930
        # https://simplyblock.atlassian.net/browse/SFAM-1929
        # offline_device_detail = self.sbcli_utils.wait_for_device_status(node_id=node_uuid,
        #                                                                 status=device_status,
        #                                                                 timeout=300)
        # for device in offline_device_detail:
        #     # if "jm" in device["jm_bdev"]:
        #     #     assert device["status"] == "JM_DEV", \
        #     #         f"JM Device {device['id']} is not in JM_DEV state. {device['status']}"
        #     # else:
        #     assert device["status"] == device_status, \
        #         f"Device {device['id']} is not in {device_status} state. Actual {device['status']}"
        #     offline_device.append(device['id'])

        # for lvol in lvol_details:
        #     assert lvol["status"] == lvol_status, \
        #         f"Lvol {lvol['id']} is not in {lvol_status} state. Actual: {lvol['status']}"

        # storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        # health_check_status = health_check_status if isinstance(health_check_status, list)\
        #       else [health_check_status]
        # if not device_health_check:
        #     device_health_check = [True, False]
        # device_health_check = device_health_check if isinstance(device_health_check, list)\
        #       else [device_health_check]
        # for node in storage_nodes:
        #     node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
        #     if node["id"] == node_uuid and node_details[0]['status'] == "offline":
        #         node = self.sbcli_utils.wait_for_health_status(node['id'], status=health_check_status,
        #                                                        timeout=300)
        #         assert node["health_check"] in health_check_status, \
        #             f"Node {node['id']} health-check is not {health_check_status}. Actual: {node['health_check']}. Node Status: {node_details[0]['status']}"
        #     else:
        #         node = self.sbcli_utils.wait_for_health_status(node['id'], status=True,
        #                                                        timeout=300)
        #         assert node["health_check"] is True, \
        #             f"Node {node['id']} health-check is not True. Actual:  {node['health_check']}.  Node Status: {node_details[0]['status']}"
        #     if node['id'] == node_uuid:
        #         device_details = offline_device_detail
        #     else:
        #         device_details = self.sbcli_utils.get_device_details(storage_node_id=node['id'])
        #     node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
        #     for device in device_details:
        #         device = self.sbcli_utils.wait_for_health_status(node['id'], status=device_health_check,
        #                                                             device_id=device['id'],
        #                                                             timeout=300)
        #         assert device["health_check"] in device_health_check, \
        #             f"Device {device['id']} health-check is not {device_health_check}. Actual:  {device['health_check']}"

        # TODO: Change cluster map validations
        # command = f"{self.base_cmd} sn get-cluster-map {lvol_details[0]['node_id']}"
        # lvol_cluster_map_details, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
        #                                                         command=command)
        # self.logger.info(f"LVOL Cluster map: {lvol_cluster_map_details}")
        # cluster_map_nodes, cluster_map_devices = self.common_utils.parse_lvol_cluster_map_output(lvol_cluster_map_details)
        
        # for node_id, node in cluster_map_nodes.items():
        #     if node_id == node_uuid:
        #         if isinstance(node_status, list):
        #             assert node["Reported Status"] in node_status, \
        #             f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
        #             assert node["Actual Status"] in node_status, \
        #                 f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
        #         else:
        #             assert node["Reported Status"] == node_status, \
        #             f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
        #             assert node["Actual Status"] == node_status, \
        #                 f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
                    
        #     else:
        #         assert node["Reported Status"] == "online", \
        #             f"Node {node_uuid} is not in online state. Actual: {node['Reported Status']}"
        #         assert node["Actual Status"] == "online", \
        #             f"Node {node_uuid} is not in online state. Actual: {node['Actual Status']}"

        # if device_status is not None:
        #     for device_id, device in cluster_map_devices.items():
        #         if device_id in offline_device:
        #             assert device["Reported Status"] == device_status, \
        #                 f"Device {device_id} is not in {device_status} state. Actual: {device['Reported Status']}"
        #             assert device["Actual Status"] == device_status, \
        #                 f"Device {device_id} is not in {device_status} state. Actual: {device['Actual Status']}"
        #         else:
        #             assert device["Reported Status"] == "online", \
        #                 f"Device {device_id} is not in online state. Actual: {device['Reported Status']}"
        #             assert device["Actual Status"] == "online", \
        #                 f"Device {device_id} is not in online state. {device['Actual Status']}"

    def unmount_all(self, base_path=None):
        """ Unmount all mount points """
        self.logger.info("Unmounting all mount points")
        if not base_path:
            base_path = self.mount_path
        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        for node in fio_nodes:
            mount_points = self.ssh_obj.get_mount_points(node=node, base_path=base_path)
            for mount_point in mount_points:
                if "/mnt/nfs_share" not in mount_point:
                    self.logger.info(f"Unmounting {mount_point}")
                    self.ssh_obj.unmount_path(node=node, device=mount_point)

    def remove_mount_dirs(self):
        """ Remove all mount point directories """
        self.logger.info("Removing all mount point directories")
        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        for node in fio_nodes:
            mount_dirs = self.ssh_obj.get_mount_points(node=node, base_path=self.mount_path)
            for mount_dir in mount_dirs:
                if "/mnt/nfs_share" not in mount_dir:
                    self.logger.info(f"Removing directory {mount_dir}")
                    self.ssh_obj.remove_dir(node=node, dir_path=mount_dir)
    
    def disconnect_lvol(self, lvol_device):
        """Disconnects the logical volume."""
        if isinstance(self.fio_node, list):
            for node in self.fio_node:
                nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=node,
                                                            nqn_filter=lvol_device)
                for nqn in nqn_lvol:
                    self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
                    self.ssh_obj.disconnect_nvme(node=node, nqn_grep=nqn)
        else:
            nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.fio_node,
                                                        nqn_filter=lvol_device)
            for nqn in nqn_lvol:
                self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)

    def disconnect_lvols(self):
        """ Disconnect all NVMe devices with NQN containing 'lvol' """
        self.logger.info("Disconnecting all NVMe devices with NQN containing 'lvol'")
        if isinstance(self.fio_node, list):  
            for node in self.fio_node:
                subsystems = self.ssh_obj.get_nvme_subsystems(node=node, nqn_filter="lvol")
                for subsys in subsystems:
                    self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                    self.ssh_obj.disconnect_nvme(node=node, nqn_grep=subsys)
        else:
            subsystems = self.ssh_obj.get_nvme_subsystems(node=self.fio_node, nqn_filter="lvol")
            for subsys in subsystems:
                self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=subsys)

    def delete_snapshots(self):
        """ Delete all snapshots """
        self.logger.info("Deleting all snapshots")
        snapshots = self.ssh_obj.get_snapshots(node=self.mgmt_nodes[0])
        for snapshot in snapshots:
            self.logger.info(f"Deleting snapshot: {snapshot}")
            delete_snapshot_command = f"{self.base_cmd} snapshot delete {snapshot} --force"
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=delete_snapshot_command)

    def filter_migration_tasks(self, tasks, node_id, timestamp, window_minutes=None):
        """
        Filters `device_migration` tasks for a specific node and timestamp.
        If window_minutes is provided, include tasks with date > (timestamp - window_minutes*60).
        There is NO upper limit; only a lower bound.
        """
        self.logger.info(f"[DEBUG]: Migration TASKS: {tasks}")

        # Lower bound only
        lower = timestamp if window_minutes is None else timestamp - int(window_minutes) * 60

        filtered_tasks = [
            task for task in tasks
            if ('balancing_on' in task['function_name'] or 'migration' in task['function_name'])
            and task['date'] > lower
            and (node_id is None or task['node_id'] == node_id)
        ]
        return filtered_tasks


    def validate_migration_for_node(self, timestamp, timeout, node_id=None, check_interval=60, no_task_ok=False):
        """
        Validate that all `device_migration` tasks for a specific node have completed successfully 
        and check for stuck tasks until the timeout is reached.

        Args:
            timestamp (int): The timestamp to filter tasks created after this time.
            timeout (int): Maximum time in seconds to keep checking for task completion.
            node_id (str): The UUID of the node to check for migration tasks (or None for all nodes).
            check_interval (int): Time interval in seconds to wait between checks.

        Raises:
            RuntimeError: If any migration task failed, is incomplete, is stuck, or if the timeout is reached.
        """
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(seconds=timeout)

        output = None
        while output is None:
            output, _ = self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0],
                command=f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0"
            )
            self.logger.info(f"Data migration output: {output}")
            if no_task_ok:
                return  # Skip checking altogether

        migration_tasks_found = False

        while datetime.now(timezone.utc) < end_time:
            tasks = self.sbcli_utils.get_cluster_tasks(self.cluster_id)
            filtered_tasks = self.filter_migration_tasks(tasks, node_id, timestamp, window_minutes=10)

            if filtered_tasks:
                migration_tasks_found = True
                self.logger.info(f"Checking migration tasks: {filtered_tasks}")

                all_done = True
                completed_count = 0

                for task in filtered_tasks:
                    try:
                        updated_at = datetime.fromisoformat(task['updated_at']).astimezone(timezone.utc)
                    except ValueError as e:
                        self.logger.error(f"Error parsing timestamp for task {task['id']}: {e}")
                        continue

                    if datetime.now(timezone.utc) - updated_at > timedelta(minutes=65) and task["status"] != "done":
                        raise RuntimeError(
                            f"Migration task {task['id']} is stuck (last updated at {updated_at.isoformat()})."
                        )

                    if task['status'] == 'done':
                        completed_count += 1
                    else:
                        all_done = False

                total_tasks = len(filtered_tasks)
                remaining_tasks = total_tasks - completed_count
                self.logger.info(
                    f"Total migration tasks: {total_tasks}, Completed: {completed_count}, Remaining: {remaining_tasks}"
                )

                if all_done:
                    self.logger.info(
                        f"All migration tasks for {'node ' + node_id if node_id else 'the cluster'} "
                        f"completed successfully without any stuck tasks."
                    )
                    return
            else:
                self.logger.info(f"No migration tasks found yet, retrying after {check_interval}s...")

            sleep_n_sec(check_interval)

        # If nothing was found at all even after timeout
        if not migration_tasks_found and not no_task_ok:
            raise RuntimeError(
                f"No migration tasks found for {'node ' + node_id if node_id else 'the cluster'} "
                f"after the specified timestamp {timestamp} and function containing device migration!"
            )

        # If tasks were found but not completed
        raise RuntimeError(
            f"Timeout reached: Not all migration tasks completed within the specified timeout of {timeout} seconds."
        )
    
    def check_core_dump(self):
        if self.k8s_test:
            # Core dumps in K8s live inside the spdk-container at /etc/simplyblock/
            k8s_obj = getattr(self.sbcli_utils, 'k8s', None)
            if not k8s_obj:
                self.logger.info("check_core_dump: k8s_utils not available, skipping.")
                return
            for node_ip in self.storage_nodes:
                files = k8s_obj.list_files_in_spdk_pod(node_ip, "/etc/simplyblock/")
                self.logger.info(f"Files in /etc/simplyblock (spdk pod for {node_ip}): {files}")
                if any("core" in f for f in files) and not any("tmp_cores" in f for f in files):
                    cur_date = datetime.now().strftime("%Y-%m-%d")
                    self.logger.info(f"Core dump found in SPDK pod for node {node_ip} at {cur_date}")
            return
        for node in self.storage_nodes:
            files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
            self.logger.info(f"Files in /etc/simplyblock: {files}")
            if "core" in files and "tmp_cores" not in files:
                cur_date = datetime.now().strftime("%Y-%m-%d")
                self.logger.info(f"Core file found on storage node {node} at {cur_date}")

        for node in self.mgmt_nodes:
            files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
            self.logger.info(f"Files in /etc/simplyblock: {files}")
            if "core" in files and "tmp_cores" not in files:
                cur_date = datetime.now().strftime("%Y-%m-%d")
                self.logger.info(f"Core file found on management node {node} at {cur_date}")

    def get_latest_cluster_util(self):
        result = self.sbcli_utils.get_cluster_capacity()
        sorted_results = sorted(result, key=lambda x: x["date"], reverse=True)
        latest_entry = sorted_results[0]

        return latest_entry