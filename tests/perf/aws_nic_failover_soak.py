#!/usr/bin/env python3
"""
aws_nic_failover_soak.py — NIC-only multipath failover soak test.

Runs fio with data verification on all volumes while repeatedly taking
one data NIC offline on ALL storage nodes simultaneously.  Each iteration
picks a single NIC (eth1 or eth2) and takes it down on every node at
once.  Different NICs are never mixed in the same iteration.

No node outages, container kills, or restarts are performed — this test
validates that NVMe multipath transparently handles single-path failures
without any IO errors or data corruption.

Prerequisites:
    - Cluster deployed with multipath (2 data NICs per node)
    - cluster_metadata.json with node IPs and data NIC info
"""
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


def parse_args():
    default_metadata = Path(__file__).with_name("cluster_metadata.json")
    default_log_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description=(
            "Run a long fio soak with data verification while cycling "
            "single-NIC outages on all storage nodes simultaneously."
        )
    )
    parser.add_argument("--metadata", default=str(default_metadata), help="Path to cluster metadata JSON.")
    parser.add_argument("--pool", default="pool01", help="Pool name for volume creation.")
    parser.add_argument("--expected-node-count", type=int, default=6, help="Required storage node count.")
    parser.add_argument("--volume-size", default="25G", help="Volume size to create per storage node.")
    parser.add_argument("--runtime", type=int, default=72000, help="fio runtime in seconds.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Poll interval for health checks.")
    parser.add_argument(
        "--log-file",
        default=str(default_log_dir / f"aws_nic_failover_soak_{time.strftime('%Y%m%d_%H%M%S')}.log"),
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
        "--data-nics",
        default="eth1,eth2",
        help="Comma-separated data NIC names on storage nodes (default: eth1,eth2).",
    )
    parser.add_argument(
        "--nic-down-duration",
        type=int,
        default=30,
        help="Seconds to keep the NIC down per iteration (default: 30).",
    )
    parser.add_argument(
        "--settle-time",
        type=int,
        default=30,
        help="Seconds to wait after NIC restore before checking fio (default: 30).",
    )
    parser.add_argument(
        "--iteration-gap",
        type=int,
        default=60,
        help="Seconds between iterations (default: 60).",
    )
    args = parser.parse_args()
    args.data_nics = [n.strip() for n in args.data_nics.split(",") if n.strip()]
    if len(args.data_nics) < 2:
        parser.error("At least 2 data NICs required for NIC failover testing")
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
        tag = label or command[:80]
        self.logger.log(f"{self.name}: RUN {tag}")
        try:
            _, stdout_ch, stderr_ch = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout_ch.read().decode("utf-8", errors="replace")
            stderr_text = stderr_ch.read().decode("utf-8", errors="replace")
            rc = stdout_ch.channel.recv_exit_status()
        except Exception as exc:
            self.logger.log(f"{self.name}: SSH error for {tag}: {exc}")
            self.close()
            raise RemoteCommandError(f"{self.name}: SSH error: {exc}")
        self.logger.block(f"{self.name}: STDOUT for {tag}", stdout_text)
        self.logger.block(f"{self.name}: STDERR for {tag}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={rc}: {tag}")
        return rc, stdout_text, stderr_text

    def _run_via_ssh_cli(self, command, timeout=600, check=True, label=None):
        tag = label or command[:80]
        self.logger.log(f"{self.name}: RUN (cli) {tag}")
        ssh_cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=15",
            "-i", self.key_path,
            f"{self.user}@{self.hostname}",
            command,
        ]
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise RemoteCommandError(f"{self.name}: timeout ({timeout}s): {tag}")
        self.logger.block(f"{self.name}: STDOUT for {tag}", result.stdout)
        self.logger.block(f"{self.name}: STDERR for {tag}", result.stderr)
        if check and result.returncode != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={result.returncode}: {tag}")
        return result.returncode, result.stdout, result.stderr

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None


class LocalHost:
    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def run(self, command, timeout=600, check=True, label=None):
        tag = label or command[:80]
        self.logger.log(f"{self.name}: RUN {tag}")
        result = subprocess.run(
            ["bash", "-lc", command], capture_output=True, text=True, timeout=timeout,
        )
        self.logger.block(f"{self.name}: STDOUT for {tag}", result.stdout)
        self.logger.block(f"{self.name}: STDERR for {tag}", result.stderr)
        if check and result.returncode != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={result.returncode}: {tag}")
        return result.returncode, result.stdout, result.stderr

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


class NicFailoverSoak:
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
        self.node_hosts = {}
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
        ip_map = {}
        for sn in self.metadata["storage_nodes"]:
            ip_map[sn["private_ip"]] = sn["public_ip"]
        return ip_map

    def _node_host(self, node_id):
        if node_id not in self.node_hosts:
            mgmt_ip = self._get_node_mgmt_ip(node_id)
            pub_ip = self.node_ip_map.get(mgmt_ip, mgmt_ip)
            self.node_hosts[node_id] = RemoteHost(
                pub_ip, self.user, self.key_path, self.logger, f"sn[{mgmt_ip}]"
            )
        return self.node_hosts[node_id]

    def _get_node_mgmt_ip(self, node_id):
        nodes = self._get_sn_list()
        for n in nodes:
            if n["uuid"] == node_id:
                return n["mgmt_ip"]
        raise TestRunError(f"Node {node_id} not found in sn list")

    # ---- sbctl helpers -------------------------------------------------------

    def sbctl(self, subcmd, timeout=120):
        _, stdout, _ = self.mgmt.run(
            f"sbctl {subcmd}",
            timeout=timeout,
            label=f"sbctl {subcmd}",
        )
        return stdout

    def sbctl_json(self, subcmd, timeout=120):
        _, stdout, _ = self.mgmt.run(
            f"sbctl {subcmd} --json",
            timeout=timeout,
            check=False,
            label=f"sbctl {subcmd} --json",
        )
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            return None

    def _get_sn_list(self):
        data = self.sbctl_json("sn list")
        if not data:
            return []
        return [
            {
                "uuid": n["UUID"],
                "status": n["Status"].lower(),
                "health": n["Health"],
                "mgmt_ip": n["Management IP"],
            }
            for n in data
        ]

    def ensure_expected_nodes(self):
        nodes = self._get_sn_list()
        if len(nodes) != self.args.expected_node_count:
            raise TestRunError(
                f"Expected {self.args.expected_node_count} nodes, found {len(nodes)}"
            )
        return nodes

    def wait_for_cluster_stable(self):
        started = time.time()
        while time.time() - started < 300:
            cluster_list = self.sbctl_json("cluster list")
            if not cluster_list:
                time.sleep(self.args.poll_interval)
                continue
            cluster_info = cluster_list[0] if isinstance(cluster_list, list) else cluster_list
            status = cluster_info.get("Status", "").lower().strip()
            rebalancing = "rebalancing" in status
            nodes = self._get_sn_list()
            if "active" in status and not rebalancing and all(
                n["status"] == "online" for n in nodes
            ):
                self.logger.log("Cluster stable: ACTIVE, online, not rebalancing")
                return
            self.logger.log(
                f"Waiting for cluster stable: status={status}, "
                f"nodes={'|'.join(n['status'] for n in nodes)}"
            )
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for cluster to stabilize")

    # ---- client / volume setup -----------------------------------------------

    def prepare_client(self):
        mount_root = f"/mnt/soak_{self.run_id}"
        self.client.run(
            "if command -v dnf >/dev/null; then sudo dnf install -y nvme-cli fio xfsprogs; "
            "else sudo apt-get update && sudo apt-get install -y nvme-cli fio xfsprogs; fi",
            timeout=120,
            label="install client packages",
        )
        self.client.run("sudo modprobe nvme_tcp", timeout=30, label="load nvme_tcp")
        return mount_root

    def create_volumes(self, nodes):
        self.logger.log(
            f"Creating {len(nodes)} volumes of size {self.args.volume_size}, one per storage node"
        )
        volumes = []
        for idx, node in enumerate(nodes, 1):
            self.wait_for_cluster_stable()
            vol_name = f"nic_soak_{self.run_id}_v{idx}"
            stdout = self.sbctl(
                f"lvol add {vol_name} {self.args.volume_size} {self.args.pool} "
                f"--host-id {node['uuid']}",
                timeout=120,
            )
            vol_uuid_match = UUID_RE.search(stdout)
            if not vol_uuid_match:
                raise TestRunError(f"Failed to extract volume UUID from: {stdout}")
            vol_id = vol_uuid_match.group()
            self.created_volume_ids.append(vol_id)
            self.logger.log(
                f"Created volume {vol_name} ({vol_id}) on node {node['uuid']}"
            )
            volumes.append({
                "volume_id": vol_id,
                "volume_name": vol_name,
                "node_uuid": node["uuid"],
                "index": idx,
            })
        return volumes

    def connect_and_mount_volumes(self, volumes, mount_root):
        self.logger.log("Connecting volumes to client and preparing filesystems")
        for volume in volumes:
            connect_out = self.sbctl(f"lvol connect {volume['volume_id']}", timeout=120)
            connect_cmds = [
                line.strip() for line in connect_out.splitlines()
                if line.strip().startswith("sudo nvme connect")
            ]
            for cmd in connect_cmds:
                self.client.run(cmd, timeout=60, check=False,
                                label=f"connect {volume['volume_id']}")

            mount_point = posixpath.join(mount_root, volume["volume_id"])
            self.client.run(
                f"sudo mkdir -p {shlex.quote(mount_point)} && "
                f"sleep 2 && "
                f"DEV=$(lsblk -dpno NAME,SERIAL | grep {volume['volume_id'][:20]} | awk '{{print $1}}' | head -1) && "
                f"sudo mkfs.xfs -f $DEV && "
                f"sudo mount $DEV {shlex.quote(mount_point)}",
                timeout=120,
                label=f"format and mount {volume['volume_id']}",
            )
            volume["mount_point"] = mount_point
            volume["fio_log"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.log")
            volume["rc_file"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.rc")

    def start_fio(self, volumes):
        self.logger.log("Starting fio on all mounted volumes in parallel")
        fio_jobs = []
        for volume in volumes:
            fio_name = f"nic_soak_{volume['index']}"
            start_script = (
                "set -euo pipefail\n"
                f"rm -f {shlex.quote(volume['rc_file'])}\n"
                "nohup bash -lc "
                + shlex.quote(
                    f"cd {shlex.quote(volume['mount_point'])} && "
                    f"fio --name={fio_name} --directory={shlex.quote(volume['mount_point'])} "
                    "--direct=1 --rw=randrw --bs=4K --group_reporting --time_based "
                    f"--numjobs=4 --iodepth=4 --size=4G --runtime={self.args.runtime} "
                    "--ioengine=aiolib --max_latency=10s "
                    "--verify=crc32c --verify_fatal=1 --verify_backlog=1024 "
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
        self.ensure_fio_running()

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
            raise TestRunError("fio completed before NIC failover loop started")

    # ---- NIC outage ----------------------------------------------------------

    def _nic_down_on_node(self, node_id, nic, duration):
        """Take a single NIC down on a storage node for *duration* seconds.
        Fire-and-forget via nohup so SSH doesn't block."""
        host = self._node_host(node_id)
        cmd = (
            f"sudo nohup bash -c '"
            f"ip link set {nic} down; sleep {duration}; ip link set {nic} up"
            f"' >/dev/null 2>&1 &"
        )
        try:
            host.run(
                f"bash -lc {shlex.quote(cmd)}",
                timeout=30,
                check=False,
                label=f"nic_down {node_id[:12]} {nic} {duration}s",
            )
        except RemoteCommandError as exc:
            self.logger.log(f"NIC down command failed on {node_id[:12]}: {exc}")

    def run_nic_failover_iteration(self, iteration, nic, node_uuids):
        """Take one NIC down on ALL nodes simultaneously, wait, verify fio."""
        duration = self.args.nic_down_duration

        self.logger.log(
            f"Iteration {iteration}: taking {nic} down on ALL {len(node_uuids)} nodes "
            f"for {duration}s"
        )

        # Fire NIC-down on all nodes (fire-and-forget, near-simultaneous)
        for uuid in node_uuids:
            self._nic_down_on_node(uuid, nic, duration)

        # Wait for NIC outage duration + settle time
        total_wait = duration + self.args.settle_time
        self.logger.log(
            f"Iteration {iteration}: waiting {total_wait}s "
            f"({duration}s outage + {self.args.settle_time}s settle)"
        )
        time.sleep(total_wait)

        # Check fio is still running and no verification errors
        self.logger.log(f"Iteration {iteration}: checking fio status")
        finished = self.check_fio()
        if finished:
            self.logger.log(f"Iteration {iteration}: fio completed successfully")
            return True

        self.logger.log(f"Iteration {iteration}: fio still running, all healthy")
        return False

    # ---- main loop -----------------------------------------------------------

    def run(self):
        self.logger.log("=== NIC Failover Soak Test ===")
        self.logger.log(f"Data NICs: {self.args.data_nics}")
        self.logger.log(f"NIC down duration: {self.args.nic_down_duration}s")
        self.logger.log(f"Settle time: {self.args.settle_time}s")
        self.logger.log(f"Iteration gap: {self.args.iteration_gap}s")

        nodes = self.ensure_expected_nodes()
        self.wait_for_cluster_stable()
        mount_root = self.prepare_client()
        volumes = self.create_volumes(nodes)
        self.connect_and_mount_volumes(volumes, mount_root)
        self.start_fio(volumes)

        iteration = 0
        while True:
            iteration += 1

            # Verify cluster is healthy before each iteration
            current_nodes = self.ensure_expected_nodes()
            node_uuids = [n["uuid"] for n in current_nodes]
            if any(n["status"] != "online" for n in current_nodes):
                raise TestRunError(
                    "Cluster not healthy before NIC failover iteration: "
                    + ", ".join(
                        f"{n['uuid'][:12]}:{n['status']}" for n in current_nodes
                    )
                )

            # Pick one NIC — same NIC on all nodes
            nic = random.choice(self.args.data_nics)

            done = self.run_nic_failover_iteration(iteration, nic, node_uuids)
            if done:
                self.logger.log(
                    f"Test completed successfully after {iteration} NIC failover iterations"
                )
                return

            # Wait between iterations
            if self.args.iteration_gap > 0:
                self.logger.log(
                    f"Waiting {self.args.iteration_gap}s before next iteration"
                )
                time.sleep(self.args.iteration_gap)


def main():
    args = parse_args()
    logger = Logger(args.log_file)
    logger.log(f"Logging to {args.log_file}")
    metadata = load_metadata(args.metadata)
    if not metadata.get("clients"):
        raise SystemExit("Metadata file does not contain a client host")

    runner = NicFailoverSoak(args, metadata, logger)
    try:
        runner.run()
    except (RemoteCommandError, TestRunError, ValueError) as exc:
        logger.log(f"ERROR: {exc}")
        sys.exit(1)
    finally:
        runner.close()


if __name__ == "__main__":
    main()
