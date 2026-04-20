#!/usr/bin/env python3
import argparse
import json
import os
import posixpath
import random
import re
import shlex
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

try:
    import paramiko
except ImportError:
    paramiko = None


UUID_RE = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")


OUTAGE_METHODS = (
    "graceful", "forced", "container_kill", "host_reboot",
    "network_outage_20", "network_outage_50",
)
# Methods that leave the node in a state where it recovers on its own
# (no sbctl restart required from the soak driver).
AUTO_RECOVER_METHODS = (
    "container_kill", "host_reboot",
    "network_outage_20", "network_outage_50",
)


def parse_args():
    default_metadata = Path(__file__).with_name("cluster_metadata.json")
    default_log_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description=(
            "Run a long fio soak against an AWS cluster while cycling random "
            "two-node outages with mixed outage methods."
        )
    )
    parser.add_argument("--metadata", default=str(default_metadata), help="Path to cluster metadata JSON.")
    parser.add_argument("--pool", default="pool01", help="Pool name for volume creation.")
    parser.add_argument("--expected-node-count", type=int, default=6, help="Required storage node count.")
    parser.add_argument("--volume-size", default="25G", help="Volume size to create per storage node.")
    parser.add_argument("--runtime", type=int, default=72000, help="fio runtime in seconds.")
    parser.add_argument("--restart-timeout", type=int, default=900, help="Seconds to wait for restarted nodes.")
    parser.add_argument("--rebalance-timeout", type=int, default=7200, help="Seconds to wait for rebalancing.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Poll interval for health checks.")
    parser.add_argument(
        "--shutdown-gap",
        type=int,
        default=0,
        help="Optional delay between shutting down the two selected nodes.",
    )
    parser.add_argument(
        "--log-file",
        default=str(default_log_dir / f"aws_dual_node_outage_soak_{time.strftime('%Y%m%d_%H%M%S')}.log"),
        help="Single log file for script and CLI output.",
    )
    parser.add_argument(
        "--run-on-mgmt",
        action="store_true",
        help="Run management-node commands locally instead of over SSH.",
    )
    parser.add_argument(
        "--ssh-key",
        default="",
        help="Optional SSH private key path override for client connections.",
    )
    parser.add_argument(
        "--methods",
        default=",".join(OUTAGE_METHODS),
        help=(
            "Comma-separated subset of outage methods to pick from per iteration. "
            f"Choices: {','.join(OUTAGE_METHODS)}. "
            "Each iteration picks 2 distinct methods at random."
        ),
    )
    parser.add_argument(
        "--nic-chaos-duration",
        type=int,
        default=20,
        help="Seconds to hold a data NIC down between iterations (multipath only).",
    )
    parser.add_argument(
        "--no-nic-chaos",
        action="store_true",
        help="Disable inter-iteration NIC chaos even on multipath clusters.",
    )
    parser.add_argument(
        "--auto-recover-wait",
        type=int,
        default=900,
        help=(
            "Seconds to wait for a node to return online after a container_kill "
            "or host_reboot outage (no sbctl restart is issued)."
        ),
    )
    args = parser.parse_args()
    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    bad = [m for m in methods if m not in OUTAGE_METHODS]
    if bad:
        parser.error(f"Unknown outage method(s): {bad}. Choices: {list(OUTAGE_METHODS)}")
    if not methods:
        parser.error("At least one outage method must be enabled")
    args.methods = methods
    return args


def load_metadata(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def candidate_key_paths(raw_path):
    expanded = os.path.expanduser(raw_path)
    base = os.path.basename(raw_path.replace("\\", "/"))
    home = Path.home()
    candidates = [
        Path(expanded),
        home / ".ssh" / base,
        home / base,
        Path(r"C:\Users\Michael\.ssh") / base,
        Path(r"C:\Users\Michael\.ssh\sbcli-test.pem"),
        Path(r"C:\ssh") / base,
    ]
    seen = set()
    unique = []
    for candidate in candidates:
        text = str(candidate)
        if text not in seen:
            seen.add(text)
            unique.append(candidate)
    return unique


def resolve_key_path(raw_path):
    for candidate in candidate_key_paths(raw_path):
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        f"Unable to resolve SSH key from metadata path {raw_path!r}. "
        f"Tried: {', '.join(str(p) for p in candidate_key_paths(raw_path))}"
    )


class Logger:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    def log(self, message):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        with self.lock:
            print(line, flush=True)
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def block(self, header, content):
        if content is None:
            return
        text = content.rstrip()
        if not text:
            return
        with self.lock:
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {header}\n")
                handle.write(text + "\n")


class RemoteCommandError(RuntimeError):
    pass


class RemoteHost:
    def __init__(self, hostname, user, key_path, logger, name):
        self.hostname = hostname
        self.user = user
        self.key_path = key_path
        self.logger = logger
        self.name = name
        self.client = None
        self.connect()

    def connect(self):
        if paramiko is None:
            return
        self.close()
        last_error = None
        for attempt in range(1, 16):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(
                    hostname=self.hostname,
                    username=self.user,
                    key_filename=self.key_path,
                    timeout=15,
                    banner_timeout=15,
                    auth_timeout=15,
                    allow_agent=False,
                    look_for_keys=False,
                )
                transport = client.get_transport()
                if transport is not None:
                    transport.set_keepalive(30)
                self.client = client
                return
            except Exception as exc:
                last_error = exc
                self.logger.log(
                    f"{self.name}: SSH attempt {attempt}/15 failed to {self.hostname}: {exc}"
                )
                time.sleep(5)
        raise RemoteCommandError(f"{self.name}: failed to connect to {self.hostname}: {last_error}")

    def run(self, command, timeout=600, check=True, label=None):
        if paramiko is None:
            return self._run_via_ssh_cli(command, timeout=timeout, check=check, label=label)
        if self.client is None:
            self.connect()
        label = label or command
        self.logger.log(f"{self.name}: RUN {label}")
        try:
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            self.logger.log(f"{self.name}: command transport failure for {label}: {exc}; reconnecting once")
            self.connect()
            stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
        self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(
                f"{self.name}: command failed with rc={rc}: {label}"
            )
        return rc, stdout_text, stderr_text

    def _run_via_ssh_cli(self, command, timeout=600, check=True, label=None):
        label = label or command
        self.logger.log(f"{self.name}: RUN {label}")
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-i",
            self.key_path,
            f"{self.user}@{self.hostname}",
            command,
        ]
        try:
            completed = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            stdout_text = exc.stdout or ""
            stderr_text = exc.stderr or ""
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        rc = completed.returncode
        self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
        self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={rc}: {label}")
        return rc, stdout_text, stderr_text

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None


class LocalHost:
    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def run(self, command, timeout=600, check=True, label=None):
        label = label or command
        self.logger.log(f"{self.name}: RUN {label}")
        try:
            completed = subprocess.run(
                ["/bin/bash", "-lc", command],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            stdout_text = exc.stdout or ""
            stderr_text = exc.stderr or ""
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        rc = completed.returncode
        self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
        self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={rc}: {label}")
        return rc, stdout_text, stderr_text

    def close(self):
        return


@dataclass
class FioJob:
    volume_id: str
    volume_name: str
    mount_point: str
    fio_log: str
    rc_file: str
    pid: int


class TestRunError(RuntimeError):
    pass


class SoakRunner:
    def __init__(self, args, metadata, logger):
        self.args = args
        self.metadata = metadata
        self.logger = logger
        self.user = metadata["user"]
        self.key_path = resolve_key_path(args.ssh_key or metadata["key_path"])
        self.run_id = time.strftime("%Y%m%d_%H%M%S")
        if args.run_on_mgmt:
            self.mgmt = LocalHost(logger, "mgmt")
        else:
            self.mgmt = RemoteHost(metadata["mgmt"]["public_ip"], self.user, self.key_path, logger, "mgmt")
        self.client = RemoteHost(metadata["clients"][0]["public_ip"], self.user, self.key_path, logger, "client")
        self.cluster_id = metadata.get("cluster_uuid") or ""
        self.fio_jobs = []
        self.created_volume_ids = []
        # Mixed-outage state
        self.methods = list(args.methods)
        # On multipath clusters, network-layer coverage is provided by the
        # inter-iteration single-NIC chaos. Dropping all data NICs on a node
        # (network_outage_*) is a simple-cluster-only scenario.
        if self._is_multipath():
            filtered = [m for m in self.methods if not m.startswith("network_outage_")]
            dropped = [m for m in self.methods if m not in filtered]
            if dropped:
                self.logger.log(
                    f"multipath cluster detected: excluding {dropped} from outage methods"
                )
            if not filtered:
                raise TestRunError(
                    "No outage methods remain after excluding network_outage_* "
                    "on multipath cluster; pass --methods with at least one "
                    "non-network_outage method"
                )
            self.methods = filtered
        self.node_hosts = {}  # uuid -> RemoteHost (private_ip of storage node)
        self.node_ip_map = self._build_node_ip_map()

    def close(self):
        self.client.close()
        self.mgmt.close()
        for host in self.node_hosts.values():
            try:
                host.close()
            except Exception:
                pass

    def _build_node_ip_map(self):
        """Return {uuid: private_ip} for every storage node we know about."""
        ip_map = {}
        topology = self.metadata.get("topology") or {}
        for node in topology.get("nodes", []):
            uuid = node.get("uuid")
            ip = node.get("management_ip") or node.get("private_ip")
            if uuid and ip:
                ip_map[uuid] = ip
        # Fallback: pair storage_nodes list with sbctl-returned UUIDs by mgmt IP,
        # which is done lazily in _resolve_node_ip below.
        return ip_map

    def _resolve_node_ip(self, uuid):
        """Return the private/mgmt IP for a storage node UUID, refreshing via
        sbctl if we haven't seen it in metadata."""
        ip = self.node_ip_map.get(uuid)
        if ip:
            return ip
        # Try fetching via sbctl sn list JSON.
        nodes = self.sbctl("sn list --json", json_output=True)
        for node in nodes:
            candidate_ip = (
                node.get("Management IP")
                or node.get("Mgmt IP")
                or node.get("mgmt_ip")
                or node.get("management_ip")
            )
            if node.get("UUID") == uuid and candidate_ip:
                self.node_ip_map[uuid] = candidate_ip
                return candidate_ip
        raise TestRunError(f"Cannot resolve storage-node IP for UUID {uuid}")

    def _node_host(self, uuid):
        """Lazily create a RemoteHost for a storage node identified by UUID."""
        if uuid in self.node_hosts:
            return self.node_hosts[uuid]
        ip = self._resolve_node_ip(uuid)
        host = RemoteHost(ip, self.user, self.key_path, self.logger, f"sn[{ip}]")
        self.node_hosts[uuid] = host
        return host

    def sbctl(self, args, timeout=600, json_output=False):
        command = "sudo /usr/local/bin/sbctl -d " + args
        _, stdout_text, stderr_text = self.mgmt.run(
            command,
            timeout=timeout,
            check=True,
            label=f"sbctl {args}",
        )
        if not json_output:
            return stdout_text
        for candidate in (stdout_text, stderr_text, stdout_text + "\n" + stderr_text):
            candidate = candidate.strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
            decoder = json.JSONDecoder()
            final_payloads = []
            list_payloads = []
            dict_payloads = []
            for start, char in enumerate(candidate):
                if char not in "[{":
                    continue
                try:
                    obj, end = decoder.raw_decode(candidate[start:])
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, (dict, list)):
                    continue
                if not candidate[start + end:].strip():
                    final_payloads.append(obj)
                elif isinstance(obj, list):
                    list_payloads.append(obj)
                else:
                    dict_payloads.append(obj)
            if final_payloads:
                return final_payloads[-1]
            if list_payloads:
                return list_payloads[-1]
            if dict_payloads:
                return dict_payloads[-1]
        raise TestRunError(f"Failed to parse JSON from sbctl {args}")

    def ensure_prerequisites(self):
        self.logger.log(f"Using SSH key {self.key_path}")
        self.client.run(
            "if command -v dnf >/dev/null 2>&1; then "
            "sudo dnf install -y nvme-cli fio xfsprogs; "
            "else sudo apt-get update && sudo apt-get install -y nvme-cli fio xfsprogs; fi",
            timeout=1800,
            label="install client packages",
        )
        self.client.run("sudo modprobe nvme_tcp", timeout=60, label="load nvme_tcp")

    def get_cluster_id(self):
        if self.cluster_id:
            return self.cluster_id
        clusters = self.sbctl("cluster list --json", json_output=True)
        if not clusters:
            raise TestRunError("No clusters returned by sbctl cluster list")
        self.cluster_id = clusters[0]["UUID"]
        return self.cluster_id

    def get_nodes(self):
        nodes = self.sbctl("sn list --json", json_output=True)
        parsed = []
        for node in nodes:
            parsed.append(
                {
                    "uuid": node["UUID"],
                    "status": str(node.get("Status", "")).lower(),
                    "mgmt_ip": node.get("Mgmt IP") or node.get("mgmt_ip") or "",
                    "hostname": node.get("Hostname") or "",
                }
            )
        return parsed

    def ensure_expected_nodes(self):
        nodes = self.get_nodes()
        if len(nodes) != self.args.expected_node_count:
            raise TestRunError(
                f"Expected {self.args.expected_node_count} storage nodes, found {len(nodes)}. "
                f"Update metadata or pass --expected-node-count."
            )
        return nodes

    def assert_cluster_not_suspended(self):
        clusters = self.sbctl("cluster list --json", json_output=True)
        if not clusters:
            raise TestRunError("Cluster list returned no rows")
        status = str(clusters[0].get("Status", "")).lower()
        if status == "suspended":
            raise TestRunError("Cluster is suspended")
        return status

    def wait_for_all_online(self, target_nodes=None, timeout=None):
        timeout = timeout or self.args.restart_timeout
        expected = self.args.expected_node_count
        target_nodes = set(target_nodes or [])
        started = time.time()
        while time.time() - started < timeout:
            self.assert_cluster_not_suspended()
            nodes = self.ensure_expected_nodes()
            statuses = {node["uuid"]: node["status"] for node in nodes}
            offline = [uuid for uuid, status in statuses.items() if status != "online"]
            unaffected_bad = [
                uuid for uuid, status in statuses.items()
                if uuid not in target_nodes and status != "online"
            ]
            if unaffected_bad:
                raise TestRunError(
                    "Unaffected nodes are not online: "
                    + ", ".join(f"{uuid}:{statuses[uuid]}" for uuid in unaffected_bad)
                )
            if not offline and len(statuses) == expected:
                return nodes
            self.logger.log(
                "Waiting for all nodes online: "
                + ", ".join(f"{uuid}:{status}" for uuid, status in statuses.items())
            )
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for nodes to return online")

    def wait_for_cluster_stable(self):
        cluster_id = self.get_cluster_id()
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            cluster_list = self.sbctl("cluster list --json", json_output=True)
            status = str(cluster_list[0].get("Status", "")).lower()
            if status == "suspended":
                raise TestRunError("Cluster entered suspended state")
            cluster_info = self.sbctl(f"cluster get {cluster_id}", json_output=True)
            rebalancing = bool(cluster_info.get("is_re_balancing", False))
            nodes = self.ensure_expected_nodes()
            node_statuses = {node["uuid"]: node["status"] for node in nodes}
            if status == "active" and not rebalancing and all(
                state == "online" for state in node_statuses.values()
            ):
                self.logger.log("Cluster stable: ACTIVE, online, not rebalancing")
                return
            self.logger.log(
                "Waiting for cluster stability: "
                f"status={status}, rebalancing={rebalancing}, "
                + ", ".join(f"{uuid}:{state}" for uuid, state in node_statuses.items())
            )
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for cluster rebalancing to finish")

    def get_active_tasks(self):
        cluster_id = self.get_cluster_id()
        script = (
            "import json; "
            "from simplyblock_core import db_controller; "
            "from simplyblock_core.models.job_schedule import JobSchedule; "
            "db = db_controller.DBController(); "
            f"tasks = db.get_job_tasks({cluster_id!r}, reverse=False); "
            "out = [t.get_clean_dict() for t in tasks "
            "if t.status != JobSchedule.STATUS_DONE and not getattr(t, 'canceled', False)]; "
            "print(json.dumps(out))"
        )
        out = self.mgmt.run(
            f"sudo python3 -c {shlex.quote(script)}",
            timeout=60,
            label="list active tasks",
        )[1].strip()
        return json.loads(out or "[]")

    def wait_for_no_active_tasks(self, reason):
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            self.assert_cluster_not_suspended()
            active_tasks = self.get_active_tasks()
            if not active_tasks:
                return
            details = ", ".join(
                f"{task.get('function_name')}:{task.get('status')}:{task.get('node_id') or task.get('device_id')}"
                for task in active_tasks
            )
            self.logger.log(f"Waiting before {reason}; active tasks: {details}")
            time.sleep(self.args.poll_interval)
        raise TestRunError(f"Timed out waiting for active tasks to finish before {reason}")

    @staticmethod
    def _is_data_migration_task(task):
        function_name = str(task.get("function_name", "")).lower()
        task_name = str(task.get("task_name", "")).lower()
        task_type = str(task.get("task_type", "")).lower()
        haystack = " ".join([function_name, task_name, task_type])
        markers = (
            "migration",
            "rebalanc",
            "sync",
        )
        return any(marker in haystack for marker in markers)

    def wait_for_data_migration_complete(self, reason):
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            self.assert_cluster_not_suspended()
            active_tasks = self.get_active_tasks()
            migration_tasks = [task for task in active_tasks if self._is_data_migration_task(task)]
            if not migration_tasks:
                return
            details = ", ".join(
                f"{task.get('function_name')}:{task.get('status')}:{task.get('node_id') or task.get('device_id')}"
                for task in migration_tasks
            )
            self.logger.log(f"Waiting before {reason}; data migration tasks: {details}")
            time.sleep(self.args.poll_interval)
        raise TestRunError(
            f"Timed out waiting for data migration tasks to finish before {reason}"
        )

    def sbctl_allow_failure(self, args, timeout=600):
        command = "sudo /usr/local/bin/sbctl -d " + args
        rc, stdout_text, stderr_text = self.mgmt.run(
            command,
            timeout=timeout,
            check=False,
            label=f"sbctl {args}",
        )
        return rc, stdout_text, stderr_text

    def shutdown_with_migration_retry(self, node_id):
        while True:
            rc, stdout_text, stderr_text = self.sbctl_allow_failure(
                f"sn shutdown {node_id}",
                timeout=300,
            )
            if rc == 0:
                return
            output = f"{stdout_text}\n{stderr_text}".lower()
            retry_markers = (
                "migration",
                "migrat",
                "rebalanc",
                "active task",
                "running task",
                "in_progress",
                "in progress",
            )
            if any(marker in output for marker in retry_markers):
                self.logger.log(
                    f"Shutdown of {node_id} blocked by migration/rebalance/task; retrying in 15s"
                )
                time.sleep(15)
                continue
            raise RemoteCommandError(
                f"mgmt: command failed with rc={rc}: sbctl sn shutdown {node_id}"
            )

    def prepare_client(self):
        mount_root = posixpath.join("/home", self.user, f"aws_outage_soak_{self.run_id}")
        command = (
            "sudo pkill -f '[f]io --name=aws_dual_soak_' || true\n"
            f"sudo mkdir -p {shlex.quote(mount_root)}\n"
            f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} {shlex.quote(mount_root)}\n"
        )
        self.client.run(f"bash -lc {shlex.quote(command)}", timeout=120, label="prepare client workspace")
        return mount_root

    def extract_uuid(self, text):
        for line in reversed(text.splitlines()):
            stripped = line.strip()
            if UUID_RE.fullmatch(stripped):
                return stripped
        raise TestRunError(f"Failed to extract standalone UUID from output: {text}")

    def create_volumes(self, nodes):
        self.logger.log(
            f"Creating {len(nodes)} volumes of size {self.args.volume_size}, one per storage node"
        )
        volumes = []
        for index, node in enumerate(nodes, start=1):
            volume_name = f"aws_dual_soak_{self.run_id}_v{index}"
            volume_id = None
            started = time.time()
            while time.time() - started < self.args.rebalance_timeout:
                self.wait_for_cluster_stable()
                output = self.sbctl(
                    f"lvol add {volume_name} {self.args.volume_size} {self.args.pool} --host-id {node['uuid']}"
                )
                if "ERROR:" in output or "LVStore is being recreated" in output:
                    self.logger.log(f"Volume create for {volume_name} deferred: {output.strip()}")
                    time.sleep(self.args.poll_interval)
                    continue
                volume_id = self.extract_uuid(output)
                break
            if volume_id is None:
                raise TestRunError(f"Timed out creating volume {volume_name} on node {node['uuid']}")
            self.created_volume_ids.append(volume_id)
            volumes.append(
                {
                    "index": index,
                    "volume_name": volume_name,
                    "volume_id": volume_id,
                    "node_uuid": node["uuid"],
                }
            )
            self.logger.log(
                f"Created volume {volume_name} ({volume_id}) on node {node['uuid']}"
            )
        return volumes

    def connect_and_mount_volumes(self, volumes, mount_root):
        self.logger.log("Connecting volumes to client and preparing filesystems")
        for volume in volumes:
            connect_output = self.sbctl(f"lvol connect {volume['volume_id']}")
            connect_commands = []
            for line in connect_output.splitlines():
                stripped = line.strip()
                if stripped.startswith("sudo nvme connect"):
                    connect_commands.append(stripped)
            if not connect_commands:
                raise TestRunError(f"No nvme connect command returned for {volume['volume_id']}")
            successful_connects = 0
            failed_connects = []
            for connect_cmd in connect_commands:
                try:
                    self.client.run(connect_cmd, timeout=120, label=f"connect {volume['volume_id']}")
                    successful_connects += 1
                except TestRunError as exc:
                    failed_connects.append(str(exc))
                    self.logger.log(f"Path connect failed for {volume['volume_id']}: {exc}")
            if successful_connects == 0:
                raise TestRunError(
                    f"No nvme paths connected for {volume['volume_id']}: {'; '.join(failed_connects)}"
                )
            if failed_connects:
                self.logger.log(
                    f"Continuing with {successful_connects}/{len(connect_commands)} connected paths "
                    f"for {volume['volume_id']}"
                )
            volume["mount_point"] = posixpath.join(mount_root, f"vol{volume['index']}")
            volume["fio_log"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.log")
            volume["rc_file"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.rc")
            find_and_mount = (
                "set -euo pipefail\n"
                f"dev=$(readlink -f /dev/disk/by-id/*{volume['volume_id']}* | head -n 1)\n"
                "if [ -z \"$dev\" ]; then\n"
                f"  echo 'Failed to locate NVMe device for {volume['volume_id']}' >&2\n"
                "  exit 1\n"
                "fi\n"
                f"sudo mkfs.xfs -f \"$dev\"\n"
                f"sudo mkdir -p {shlex.quote(volume['mount_point'])}\n"
                f"sudo mount \"$dev\" {shlex.quote(volume['mount_point'])}\n"
                f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} {shlex.quote(volume['mount_point'])}\n"
            )
            self.client.run(
                f"bash -lc {shlex.quote(find_and_mount)}",
                timeout=600,
                label=f"format and mount {volume['volume_id']}",
            )

    def start_fio(self, volumes):
        self.logger.log("Starting fio on all mounted volumes in parallel")
        fio_jobs = []
        for volume in volumes:
            fio_name = f"aws_dual_soak_{volume['index']}"
            start_script = (
                "set -euo pipefail\n"
                f"rm -f {shlex.quote(volume['rc_file'])}\n"
                "nohup bash -lc "
                + shlex.quote(
                    f"cd {shlex.quote(volume['mount_point'])} && "
                    f"fio --name={fio_name} --directory={shlex.quote(volume['mount_point'])} "
                    "--direct=1 --rw=randrw --bs=4K --group_reporting --time_based "
                    f"--numjobs=4 --iodepth=4 --size=4G --runtime={self.args.runtime} "
                    "--ioengine=aiolib --max_latency=5s "
                    f"--output={shlex.quote(volume['fio_log'])}; "
                    "rc=$?; "
                    f"echo $rc > {shlex.quote(volume['rc_file'])}"
                )
                + " >/dev/null 2>&1 & echo $!"
            )
            _, stdout_text, _ = self.client.run(
                f"bash -lc {shlex.quote(start_script)}",
                timeout=60,
                label=f"start fio {volume['volume_id']}",
            )
            pid_text = stdout_text.strip().splitlines()[-1]
            pid = int(pid_text)
            fio_jobs.append(
                FioJob(
                    volume_id=volume["volume_id"],
                    volume_name=volume["volume_name"],
                    mount_point=volume["mount_point"],
                    fio_log=volume["fio_log"],
                    rc_file=volume["rc_file"],
                    pid=pid,
                )
            )
            self.logger.log(f"Started fio for {volume['volume_name']} with pid {pid}")
        self.fio_jobs = fio_jobs
        time.sleep(5)
        self.ensure_fio_running()

    def read_remote_file(self, path):
        rc, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(f'cat {shlex.quote(path)}')}",
            timeout=30,
            check=False,
            label=f"read {path}",
        )
        if rc != 0:
            return ""
        return stdout_text

    def check_fio(self):
        completed = 0
        for job in self.fio_jobs:
            check_script = (
                "set -euo pipefail\n"
                f"if kill -0 {job.pid} 2>/dev/null; then\n"
                "  echo RUNNING\n"
                f"elif [ -f {shlex.quote(job.rc_file)} ]; then\n"
                f"  echo EXITED:$(cat {shlex.quote(job.rc_file)})\n"
                "else\n"
                "  echo MISSING\n"
                "fi\n"
            )
            _, stdout_text, _ = self.client.run(
                f"bash -lc {shlex.quote(check_script)}",
                timeout=30,
                label=f"check fio pid {job.pid}",
            )
            status = stdout_text.strip().splitlines()[-1]
            if status == "RUNNING":
                continue
            if status == "EXITED:0":
                completed += 1
                continue
            tail = self.client.run(
                f"bash -lc {shlex.quote(f'tail -50 {shlex.quote(job.fio_log)}')}",
                timeout=30,
                check=False,
                label=f"tail fio log {job.volume_name}",
            )[1]
            raise TestRunError(
                f"fio job for {job.volume_name} stopped unexpectedly with status {status}. "
                f"Last log lines:\n{tail}"
            )
        return completed == len(self.fio_jobs)

    def ensure_fio_running(self):
        finished_cleanly = self.check_fio()
        if finished_cleanly:
            raise TestRunError("fio completed before outage loop started")

    # ----- outage methods ---------------------------------------------------

    def _forced_shutdown(self, node_id):
        """Shutdown with --force; still retry if blocked by migration."""
        while True:
            rc, stdout_text, stderr_text = self.sbctl_allow_failure(
                f"sn shutdown {node_id} --force",
                timeout=300,
            )
            if rc == 0:
                return
            output = f"{stdout_text}\n{stderr_text}".lower()
            retry_markers = (
                "migration", "migrat", "rebalanc",
                "active task", "running task",
                "in_progress", "in progress",
            )
            if any(m in output for m in retry_markers):
                self.logger.log(
                    f"Forced shutdown of {node_id} blocked by migration/task; retrying in 15s"
                )
                time.sleep(15)
                continue
            raise RemoteCommandError(
                f"mgmt: command failed with rc={rc}: sbctl sn shutdown {node_id} --force"
            )

    def _container_kill(self, node_id):
        """Kill the SPDK container on the storage node's host. Node is expected
        to auto-recover; no sbctl restart is issued."""
        host = self._node_host(node_id)
        cmd = (
            "set -euo pipefail; "
            "cns=$(sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' || true); "
            "if [ -z \"$cns\" ]; then echo 'no spdk_* container found' >&2; exit 0; fi; "
            "for cn in $cns; do echo \"killing $cn\"; sudo docker kill \"$cn\" || true; done"
        )
        host.run(
            f"bash -lc {shlex.quote(cmd)}",
            timeout=120,
            check=False,
            label=f"container_kill {node_id}",
        )

    def _host_reboot(self, node_id):
        """Reboot the storage node's host. Node is expected to auto-recover;
        no sbctl restart is issued."""
        host = self._node_host(node_id)
        # nohup + background + sleep so the shell exit beats reboot cleanly
        cmd = "sudo nohup bash -c 'sleep 2; reboot -f' >/dev/null 2>&1 &"
        try:
            host.run(
                f"bash -lc {shlex.quote(cmd)}",
                timeout=30,
                check=False,
                label=f"host_reboot {node_id}",
            )
        except RemoteCommandError as exc:
            # SSH may drop as the host goes down — not fatal.
            self.logger.log(f"host_reboot {node_id}: ssh terminated as expected: {exc}")
        # Drop the cached SSH client; it's going to die anyway.
        cached = self.node_hosts.pop(node_id, None)
        if cached is not None:
            try:
                cached.close()
            except Exception:
                pass

    # --- Multipath NIC chaos ---

    def _is_multipath(self):
        return bool(self.metadata.get("multipath"))

    def _get_data_nics(self):
        """Return the list of data NIC names (e.g. ['eth1', 'eth2'])."""
        return self.metadata.get("data_nics", [])

    def _get_all_node_ips(self):
        """Return list of (uuid, private_ip) for all storage nodes."""
        result = []
        for sn in self.metadata.get("storage_nodes", []):
            ip = sn.get("private_ip")
            # Find uuid from topology
            uuid = None
            for tn in (self.metadata.get("topology") or {}).get("nodes", []):
                if tn.get("management_ip") == ip:
                    uuid = tn.get("uuid")
                    break
            if ip:
                result.append((uuid, ip))
        return result

    def _disable_nic_on_all_nodes(self, nic_name):
        """Bring down a data NIC on all storage nodes."""
        self.logger.log(f"Multipath NIC chaos: disabling {nic_name} on all nodes")
        for uuid, ip in self._get_all_node_ips():
            try:
                host = self._node_host(uuid) if uuid else RemoteHost(
                    ip, self.user, self.key_path, self.logger, f"sn[{ip}]")
                host.run(f"sudo ip link set {nic_name} down", timeout=10, check=False,
                         label=f"disable {nic_name} on {ip}")
            except Exception as e:
                self.logger.log(f"WARNING: failed to disable {nic_name} on {ip}: {e}")

    def _enable_nic_on_all_nodes(self, nic_name):
        """Bring up a data NIC on all storage nodes."""
        self.logger.log(f"Multipath NIC chaos: re-enabling {nic_name} on all nodes")
        for uuid, ip in self._get_all_node_ips():
            try:
                host = self._node_host(uuid) if uuid else RemoteHost(
                    ip, self.user, self.key_path, self.logger, f"sn[{ip}]")
                host.run(f"sudo ip link set {nic_name} up", timeout=10, check=False,
                         label=f"enable {nic_name} on {ip}")
            except Exception as e:
                self.logger.log(f"WARNING: failed to enable {nic_name} on {ip}: {e}")

    def _ensure_all_data_nics_up(self):
        if not self._is_multipath():
            return
        for nic in self._get_data_nics():
            self._enable_nic_on_all_nodes(nic)

    def _inter_iteration_nic_chaos(self):
        if self.args.no_nic_chaos or not self._is_multipath():
            return
        data_nics = self._get_data_nics()
        if len(data_nics) < 2:
            return
        duration = max(0, self.args.nic_chaos_duration)
        nic = random.choice(data_nics)
        self.logger.log(
            f"Inter-iteration NIC chaos: dropping {nic} on all nodes for {duration}s"
        )
        try:
            self._disable_nic_on_all_nodes(nic)
            time.sleep(duration)
        finally:
            self._enable_nic_on_all_nodes(nic)
        self.wait_for_cluster_stable()

    def _network_outage(self, node_id, duration):
        """Take all data NICs down on one storage node for *duration* seconds,
        then bring them back up. Simulates a transient network partition of
        a single node. Node is expected to auto-recover once the NICs return
        — no sbctl restart is issued."""
        host = self._node_host(node_id)
        nics = self._get_data_nics() or ["eth1"]
        self.logger.log(
            f"network_outage on {node_id}: dropping {nics} for {duration}s"
        )
        for nic in nics:
            try:
                host.run(f"sudo ip link set {nic} down", timeout=10, check=False,
                         label=f"netout down {nic} on {node_id}")
            except Exception as e:
                self.logger.log(f"WARNING: failed to down {nic} on {node_id}: {e}")
        try:
            time.sleep(duration)
        finally:
            for nic in nics:
                try:
                    host.run(f"sudo ip link set {nic} up", timeout=10, check=False,
                             label=f"netout up {nic} on {node_id}")
                except Exception as e:
                    self.logger.log(f"WARNING: failed to up {nic} on {node_id}: {e}")

    def _apply_outage(self, node_id, method):
        self.logger.log(f"Applying outage '{method}' on {node_id}")
        if method == "graceful":
            self.shutdown_with_migration_retry(node_id)
        elif method == "forced":
            self._forced_shutdown(node_id)
        elif method == "container_kill":
            self._container_kill(node_id)
        elif method == "host_reboot":
            self._host_reboot(node_id)
        elif method.startswith("network_outage_"):
            try:
                duration = int(method.rsplit("_", 1)[-1])
            except ValueError:
                raise TestRunError(f"Unknown outage method: {method}")
            self._network_outage(node_id, duration)
        else:
            raise TestRunError(f"Unknown outage method: {method}")

    def _needs_manual_restart(self, method):
        return method not in AUTO_RECOVER_METHODS

    def wait_node_leaves_online(self, node_id, timeout=90, poll=2):
        """Poll sbctl until the control plane observes node_id leaving 'online'.
        Returns True once any non-online status is seen, False on timeout.

        Why this exists: the CP's health-check loop updates status on its own
        cadence. If the soak polls wait_for_all_online *before* the CP has
        noticed the outage, the first poll reports all-online and we return
        while the target is actually still down. The next iteration then
        stacks extra outages on a silently-offline node and breaks the FTT
        budget (see incident: 2026-04-20 iter 17 container_kill on 2870dfa5,
        CP status transition lagged the soak's first sn-list by ~1 s).
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                nodes = self.get_nodes()
            except Exception as exc:
                self.logger.log(f"wait_node_leaves_online: sn list failed ({exc})")
                time.sleep(poll)
                continue
            status = next(
                (n["status"] for n in nodes if n["uuid"] == node_id),
                "unknown",
            )
            if status != "online":
                self.logger.log(
                    f"CP observed {node_id[:8]} leaving online (now {status})"
                )
                return True
            time.sleep(poll)
        return False

    def run_outage_pair(self, node1, node2, method1, method2):
        self.logger.log(
            f"Outage pair: {node1}={method1} and {node2}={method2}"
        )
        # Apply first outage, then optional gap, then second outage.
        self._apply_outage(node1, method1)
        if self.args.shutdown_gap:
            time.sleep(self.args.shutdown_gap)
        self._apply_outage(node2, method2)

        # Issue sbctl restart only for methods that leave the node in a
        # "shutdown" state that the CP won't recover on its own.
        # Retry with backoff: when the other node in the pair used an
        # auto-recover method (container_kill / host_reboot), it may
        # still be in_shutdown or in_restart when we try to restart the
        # manually-recovered peer — the per-cluster guard rejects
        # concurrent restarts. Retrying gives the auto-recovering node
        # time to come back.
        for node_id, method in [(node1, method1), (node2, method2)]:
            if not self._needs_manual_restart(method):
                continue
            deadline = time.time() + self.args.restart_timeout
            while True:
                try:
                    self.sbctl(f"sn restart {node_id}", timeout=300)
                    break
                except Exception as e:
                    if time.time() >= deadline:
                        raise
                    self.logger.log(
                        f"Restart of {node_id} failed ({e}), "
                        f"retrying in 15s (peer may still be recovering)")
                    time.sleep(15)

        # Before we call wait_for_all_online, make sure the control plane has
        # actually observed each auto-recover target leaving 'online' state.
        # Otherwise wait_for_all_online can race the CP: the first sn-list
        # poll may still report the just-killed node as 'online' (stale),
        # all statuses look good, and we return immediately — the node is
        # then in a silent offline state when the next iteration stacks
        # more outages on top, crossing the FTT budget.
        # network_outage_* methods can finish before the CP notices; that's
        # fine (short outages often recover from HA multipath without CP
        # involvement), so we don't fail if the observation window expires.
        for node_id, method in [(node1, method1), (node2, method2)]:
            if method not in AUTO_RECOVER_METHODS:
                continue
            if method.startswith("network_outage_"):
                observed = self.wait_node_leaves_online(node_id, timeout=30)
                if not observed:
                    self.logger.log(
                        f"CP did not observe {node_id[:8]} offline for "
                        f"{method} within 30s (expected for short NIC drops)"
                    )
            else:
                # container_kill, host_reboot: the node IS down; we must see it.
                observed = self.wait_node_leaves_online(node_id, timeout=90)
                if not observed:
                    self.logger.log(
                        f"WARN: CP never observed {node_id[:8]} offline after "
                        f"{method} within 90s; sn-list may be stale"
                    )

        # For auto-recovery methods, allow a longer wait window since the host
        # has to reboot / the container has to come back under its supervisor.
        wait_timeout = self.args.restart_timeout
        if any(
            m in AUTO_RECOVER_METHODS for m in (method1, method2)
        ):
            wait_timeout = max(wait_timeout, self.args.auto_recover_wait)

        self.wait_for_all_online(
            target_nodes={node1, node2}, timeout=wait_timeout
        )
        finished = self.check_fio()
        if finished:
            self.logger.log("fio workload completed successfully after outage cycle")
            return True
        self.wait_for_cluster_stable()
        return False

    def run(self):
        self.ensure_prerequisites()
        nodes = self.ensure_expected_nodes()
        self.wait_for_all_online(timeout=self.args.restart_timeout)
        self.wait_for_cluster_stable()
        mount_root = self.prepare_client()
        volumes = self.create_volumes(nodes)
        self.connect_and_mount_volumes(volumes, mount_root)
        self.start_fio(volumes)

        iteration = 0
        while True:
            iteration += 1
            # After an outage iteration a node typically transitions
            # online → unreachable → down → online before the control
            # plane settles. wait_for_cluster_stable() polls until every
            # node reports "online" AND the cluster is active and not
            # rebalancing, so those transient states are tolerated here.
            # It is called both before and after the migration-wait so
            # we can't race a late transition between the two.
            self.wait_for_cluster_stable()
            self.wait_for_data_migration_complete(
                f"starting outage iteration {iteration}"
            )
            self.wait_for_cluster_stable()
            current_nodes = self.ensure_expected_nodes()
            current_uuids = [node["uuid"] for node in current_nodes]
            # Safety: all data NICs must be up during an outage iteration.
            # NIC chaos runs only in the quiet window between iterations below.
            self._ensure_all_data_nics_up()
            node1, node2 = random.sample(current_uuids, 2)
            # Pick 2 distinct outage methods (or fall back to same if only 1 enabled)
            if len(self.methods) >= 2:
                method1, method2 = random.sample(self.methods, 2)
            else:
                method1 = method2 = self.methods[0]

            self.logger.log(
                f"Starting outage iteration {iteration}: "
                f"{node1}={method1}, {node2}={method2}"
            )
            done = self.run_outage_pair(node1, node2, method1, method2)

            if done:
                self.logger.log(f"Test completed successfully after {iteration} outage iterations")
                return

            self._inter_iteration_nic_chaos()


def main():
    args = parse_args()
    logger = Logger(args.log_file)
    logger.log(f"Logging to {args.log_file}")
    metadata = load_metadata(args.metadata)
    if not metadata.get("clients"):
        raise SystemExit("Metadata file does not contain a client host")

    runner = SoakRunner(args, metadata, logger)
    try:
        runner.run()
    except (RemoteCommandError, TestRunError, ValueError) as exc:
        logger.log(f"ERROR: {exc}")
        sys.exit(1)
    finally:
        runner.close()


if __name__ == "__main__":
    main()
