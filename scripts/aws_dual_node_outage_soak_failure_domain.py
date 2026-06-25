#!/usr/bin/env python3
import argparse
import itertools
import json
import logging
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

# Silence paramiko's Transport-thread "Socket exception: Connection
# reset by peer (104)" prints. They fire whenever an open SSH
# connection to a storage node gets RST'd by a planned event —
# host_reboot outage tearing down sshd, NIC down/up flapping, etc.
# The retry/reconnect logic handles it cleanly; the stack-trace-less
# stderr lines just clutter the soak output.
logging.getLogger("paramiko").setLevel(logging.CRITICAL)
logging.getLogger("paramiko.transport").setLevel(logging.CRITICAL)


UUID_RE = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
# `sbctl lvol connect` emits `sudo nvme connect ... --nqn=<NQN> ...`
# (long form with `=`, see lvol_controller.py:1737). Tolerate the legacy
# short form `-n <NQN>` as well so older sbctl deployments still parse.
NQN_RE = re.compile(r"(?:--nqn[=\s]+|-n\s+)(\S+)")


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

# Node statuses that are TRANSIENT rather than a fault: the control plane's
# StorageNodeMonitor can autonomously kick off a node_restart (hublvol
# reconnect / ANA failback / leader role) and an auto-recover method can
# still be settling. At inter-iteration sync points (no deliberate outage in
# flight) these must be waited out, not treated as an FTT-budget breach.
# Strings mirror StorageNode: STATUS_RESTARTING / STATUS_IN_SHUTDOWN.
TRANSIENT_NODE_STATUSES = ("in_restart", "in_shutdown")
# Unaffected-node statuses that are tolerated (warned + waited out, not aborted)
# at inter-iteration sync points where tolerate_transient is set. A node can
# briefly report 'down' / 'unreachable' while the CP re-establishes its health
# check or during a transient blip; that is not an FTT-budget breach as long as
# it recovers within the wait window (the poll loop keeps waiting for it).
TOLERATED_UNAFFECTED_STATUSES = TRANSIENT_NODE_STATUSES + ("down", "unreachable")

# Scenario enumeration:
#   3 role categories × P(M,2) ordered distinct-method pairs
#   = 3 × M·(M-1) scenarios per cycle.
# Examples:
#   M=5 → 3 × 20 = 60
#   M=6 → 3 × 30 = 90
# Role categories (relative ring-distance preserved; the actual node pair
# is re-rolled randomly per scenario at execution time so the soak hits
# many different concrete pairs while keeping the topological distance
# fixed for each category).
# Order matters: the soak walks the full method permutation for one
# category before moving on. "unrelated" runs first so the outage with
# the widest blast-radius coverage (two nodes from different LVS rings)
# exercises the cluster before the within-ring categories.
#   - unrelated         : pair sharing no LVS in any role — ring-distance
#                         ≥ 3 (≥ 2 nodes between).
#   - primary_tertiary  : primary + tertiary of same LVS — ring-distance
#                         2 (exactly one node between); no replication
#                         edge connects them (jumps over the secondary).
#   - primary_secondary : primary + secondary of same LVS — ring-distance
#                         1 (direct successor). Represents both (P,S) and
#                         (S,T): two adjacent replicas of the same LVS
#                         going down is structurally symmetric regardless
#                         of which end.
# Same-method pairs (graceful,graceful etc.) are not enumerated — the
# user-agreed count 30 for 6 methods equals 6·5, not 6².
ROLE_CATEGORIES = ("unrelated", "primary_tertiary", "primary_secondary")


def parse_args():
    default_metadata = Path(__file__).with_name("cluster_metadata_failure_domain.json")
    default_log_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description=(
            "Run a long fio soak against a FAILURE-DOMAIN AWS cluster while "
            "cycling failure-domain-aware host-reboot outages. Two scenario "
            "types only: (a) reboot every host in one failure domain, then wait "
            "for cluster rebalancing to complete; (b) reboot one random node in "
            "each failure domain, then settle 60s. fio is kept running across "
            "outages; in parallel, a background thread randomly churns one "
            "volume at a time (stop fio -> unmount -> disconnect -> delete -> "
            "recreate -> connect -> format -> mount -> restart fio)."
        )
    )
    parser.add_argument("--metadata", default=str(default_metadata), help="Path to cluster metadata JSON.")
    parser.add_argument("--pool", default="pool01", help="Pool name for volume creation.")
    parser.add_argument("--expected-node-count", type=int, default=None,
                        help="Required storage node count. Default: the number of "
                             "storage_nodes in the cluster metadata.")
    parser.add_argument("--volume-size", default="200G", help="Volume size to create per storage node.")
    parser.add_argument("--runtime", type=int, default=72000, help="fio runtime in seconds.")
    parser.add_argument("--restart-timeout", type=int, default=900, help="Seconds to wait for restarted nodes.")
    parser.add_argument("--rebalance-timeout", type=int, default=7200, help="Seconds to wait for rebalancing.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Poll interval for health checks.")
    parser.add_argument(
        "--wait-for-rebalance",
        action="store_true",
        help=(
            "Between iterations, poll until cluster rebalancing / data migration "
            "fully drains (bounded by --rebalance-timeout). Default: sleep a fixed "
            "--inter-iteration-window instead (rebalance wait disabled)."
        ),
    )
    parser.add_argument(
        "--inter-iteration-window",
        type=int,
        default=60,
        help=(
            "Seconds to settle between iterations when --wait-for-rebalance is NOT "
            "set (default: 60). Nodes are still brought back online first; only the "
            "rebalance / data-migration completion wait is replaced by this fixed sleep."
        ),
    )
    parser.add_argument(
        "--shutdown-gap",
        type=int,
        default=0,
        help=(
            "Legacy fixed delay between the two outages within an iteration. "
            "When > 0 it overrides --outage-gap-min/--outage-gap-max with a "
            "constant. Default 0 = use the random range below."
        ),
    )
    parser.add_argument(
        "--outage-gap-min",
        type=int,
        default=15,
        help=(
            "Minimum seconds between applying outage 1 and outage 2 of an "
            "iteration. The actual gap is drawn uniformly from "
            "[--outage-gap-min, --outage-gap-max] and then capped per "
            "method 1 so the requested --min-outage-overlap is guaranteed."
        ),
    )
    parser.add_argument(
        "--outage-gap-max",
        type=int,
        default=180,
        help=(
            "Maximum seconds between applying outage 1 and outage 2 of an "
            "iteration. Default 3 min."
        ),
    )
    parser.add_argument(
        "--min-outage-overlap",
        type=int,
        default=10,
        help=(
            "Minimum seconds both outage targets must be simultaneously "
            "not-online inside an iteration. Used to cap the inter-outage "
            "gap when method 1's recovery window is short (e.g. "
            "network_outage_20)."
        ),
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
        default=30,
        help=(
            "Seconds to hold a data NIC down between iterations (multipath only). "
            "Per cycle, ONE of the two data NICs is picked at random and dropped on "
            "ALL storage nodes simultaneously — never a mix where some nodes drop "
            "eth1 while others drop eth2."
        ),
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
    parser.add_argument(
        "--cycles",
        type=int,
        default=None,
        help=(
            "Number of passes through the failure-domain scenario list "
            "(same_fd per domain + one_per_fd [+ fd_plus_one when >=4 domains]). "
            "0 means loop forever. Default: 1 normally, or 0 (endless) with "
            "--combo-only."
        ),
    )
    parser.add_argument(
        "--combo-only",
        action="store_true",
        help=(
            "Run ONLY the 'fd_plus_one' scenario: each iteration takes down one "
            "whole (random) failure domain PLUS one extra node in another domain, "
            "re-rolled randomly every iteration. Requires >=4 failure domains. "
            "Defaults to endless (--cycles 0) unless --cycles is given. Each "
            "downed node's outage method is drawn at random from --combo-methods."
        ),
    )
    parser.add_argument(
        "--combo-methods",
        default="container_kill,graceful,host_reboot",
        help=(
            "Comma-separated outage methods to randomly assign per node in "
            "--combo-only mode. Choices: container_kill,graceful,forced,host_reboot "
            "(network_outage_* not allowed — combo takes whole hosts down). "
            "At most one 'graceful' is used per iteration (the CP concurrent-"
            "shutdown guard rejects overlapping graceful shutdowns); extra "
            "graceful picks are demoted to an auto-recover method."
        ),
    )
    parser.add_argument(
        "--shuffle-scenarios",
        action="store_true",
        help=(
            "Shuffle scenario order per cycle (seeded deterministically off "
            "the cycle index). Useful when a full cycle is too long to finish "
            "and you want even coverage across early/mid/late pairs."
        ),
    )
    parser.add_argument(
        "--start-at",
        type=int,
        default=1,
        help=(
            "Start the first cycle at scenario N (1-indexed). Scenarios "
            "1..N-1 are skipped in the first cycle only; subsequent cycles "
            "run from scenario 1 as normal. Use to resume after a failure — "
            "e.g. --start-at 60 if scenario 60 is the one that failed."
        ),
    )
    parser.add_argument(
        "--churn-min-seconds",
        type=int,
        default=180,
        help="Minimum delay between volume churn cycles (seconds). Default 180 (3 min).",
    )
    parser.add_argument(
        "--churn-max-seconds",
        type=int,
        default=1200,
        help="Maximum delay between volume churn cycles (seconds). Default 1200 (20 min).",
    )
    parser.add_argument(
        "--no-churn",
        action="store_true",
        help="Disable the background volume churn thread (run plain mixed soak with fio kept running across outages).",
    )
    args = parser.parse_args()
    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    bad = [m for m in methods if m not in OUTAGE_METHODS]
    if bad:
        parser.error(f"Unknown outage method(s): {bad}. Choices: {list(OUTAGE_METHODS)}")
    if not methods:
        parser.error("At least one outage method must be enabled")
    args.methods = methods

    # --combo-only: resolve the random per-node method pool and default to
    # endless looping unless the user pinned --cycles explicitly.
    combo_methods = [m.strip() for m in args.combo_methods.split(",") if m.strip()]
    bad_combo = [m for m in combo_methods if m not in OUTAGE_METHODS]
    if bad_combo:
        parser.error(f"Unknown --combo-methods: {bad_combo}. Choices: {list(OUTAGE_METHODS)}")
    net_combo = [m for m in combo_methods if m.startswith("network_outage_")]
    if net_combo:
        parser.error(
            f"network_outage_* methods are not allowed in --combo-methods "
            f"({net_combo}); combo mode takes whole hosts down"
        )
    if not combo_methods:
        parser.error("--combo-methods must list at least one method")
    args.combo_methods = combo_methods
    if args.cycles is None:
        args.cycles = 0 if args.combo_only else 1

    if args.churn_min_seconds < 1:
        parser.error("--churn-min-seconds must be >= 1")
    if args.churn_max_seconds < args.churn_min_seconds:
        parser.error("--churn-max-seconds must be >= --churn-min-seconds")
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


# Number of fio worker processes per --name. Must match --numjobs in
# start_fio(). --group_reporting aggregates all workers into one report,
# so a single fio summary + per-run stderr stream is sufficient to
# diagnose any fio fault.
FIO_NUMJOBS = 4


@dataclass
class FioJob:
    volume_id: str
    volume_name: str
    mount_point: str
    fio_log: str       # fio's --output summary file (written on exit)
    fio_stderr: str    # captured stdout+stderr during the run (progress,
                       # errors, "max_latency exceeded" messages). This is
                       # the primary source of ground truth for fio faults.
    rc_file: str
    pid: int
    fio_name: str      # matches --name=<fio_name> in the fio command line


class TestRunError(RuntimeError):
    pass


class SoakRunner:
    def __init__(self, args, metadata, logger):
        self.args = args
        self.metadata = metadata
        self.logger = logger
        # Default the required node count to however many storage nodes the
        # metadata records (6 for --fd 2, 8 for --fd 4), so the soak adapts to
        # the deployed layout without needing --expected-node-count.
        if self.args.expected_node_count is None:
            self.args.expected_node_count = len(metadata.get("storage_nodes", []))
        self.user = metadata["user"]
        self.key_path = resolve_key_path(args.ssh_key or metadata["key_path"])
        self.run_id = time.strftime("%Y%m%d_%H%M%S")
        if args.run_on_mgmt:
            self.mgmt = LocalHost(logger, "mgmt")
        else:
            self.mgmt = RemoteHost(metadata["mgmt"]["public_ip"], self.user, self.key_path, logger, "mgmt")
        client_entry = metadata["clients"][0]
        if args.run_on_mgmt:
            client_addr = client_entry.get("private_ip") or client_entry["public_ip"]
        else:
            client_addr = client_entry["public_ip"]
        self.client = RemoteHost(client_addr, self.user, self.key_path, logger, "client")
        self.cluster_id = metadata.get("cluster_uuid") or ""
        self.fio_jobs = []
        # Stored so the churn cycle can pick a random volume to rebuild.
        self.volumes = []
        self.created_volume_ids = []
        # Mixed-outage state
        self.methods = list(args.methods)
        # Per-node random method pool for --combo-only iterations (whole-domain
        # + one extra node). None when combo mode is off (then FD outages use
        # plain host_reboot, as the deterministic scenarios always have).
        self.combo_methods = list(args.combo_methods) if args.combo_only else None
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
        # {failure_domain_id: [node_uuid, ...]} — populated in run() from the
        # mgmt DB; drives the failure-domain outage scenarios.
        self.failure_domains = {}
        # Serializes outage iterations and churn cycles. Both mutate cluster
        # state (outage takes nodes down; churn deletes/recreates volumes), so
        # they must not overlap. Held during run_outage_pair / check_fio /
        # post-outage settle waits / _inter_iteration_nic_chaos / entire
        # churn cycle. Churn must NOT run during NIC chaos either, since
        # nvme disconnect/connect during a data-NIC-down window can race
        # the kernel's multipath failover and leave the client in a
        # partially-attached state. The only unlocked region in the main
        # loop is the final per-iteration wait_for_cluster_stable /
        # wait_for_data_migration_complete, which is a pure poller.
        self.serial_lock = threading.RLock()
        self.churn_thread = None
        self.churn_stop_event = threading.Event()
        self.churn_error = None
        self.churn_counter = 0
        self.mount_root = None

    def close(self):
        # Stop the churn thread first so it doesn't try to use SSH clients
        # we're about to close.
        try:
            self.stop_churn_thread()
        except Exception:
            pass
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

    def wait_for_all_online(self, target_nodes=None, timeout=None,
                            tolerate_transient=False):
        """Poll until every node is 'online' (and membership == expected).

        Fails fast if any node NOT in ``target_nodes`` is not online — during
        a deliberate outage that is an FTT-budget breach. ``tolerate_transient``
        relaxes this for inter-iteration sync points (no outage in flight):
        an unaffected node that is merely TRANSIENT_NODE_STATUSES (the CP is
        autonomously restarting / shutting it down) is waited out instead of
        aborting the soak, mirroring wait_for_outage_ready. A genuinely
        offline/unknown unaffected node still raises immediately.
        """
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
            if tolerate_transient:
                # A transient / briefly-down unaffected node is waited out, not
                # treated as an FTT-budget breach: log a warning and keep
                # polling (the poll loop / outer timeout still catches a node
                # that never recovers). Only a hard-unexpected status aborts.
                tolerated = [
                    uuid for uuid in unaffected_bad
                    if statuses[uuid] in TOLERATED_UNAFFECTED_STATUSES
                ]
                if tolerated:
                    self.logger.log(
                        "WARN: tolerating transient unaffected node state "
                        "(waiting for recovery): "
                        + ", ".join(f"{uuid}:{statuses[uuid]}" for uuid in tolerated)
                    )
                unaffected_bad = [
                    uuid for uuid in unaffected_bad
                    if statuses[uuid] not in TOLERATED_UNAFFECTED_STATUSES
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

    def wait_for_outage_ready(self, timeout=None, poll=None):
        """Block until the cluster is safe to apply a fresh outage pair.

        Stronger precondition than wait_for_all_online: besides every node
        reporting 'online', NO node may be transiently in_restart /
        in_shutdown. The control plane's StorageNodeMonitor autonomously
        kicks off a node_restart during the quiet inter-iteration window,
        and an auto-recover method (container_kill / host_reboot) in the
        PREVIOUS pair can still be settling. The next pair's first
        non-force `sn shutdown` (the graceful method) is then rejected with
        rc=1 ("Node ... is restarting in this cluster, cannot shutdown ...
        concurrently") whenever any peer is STATUS_RESTARTING /
        STATUS_IN_SHUTDOWN (storage_node_ops.py:shutdown_storage_node) --
        aborting the whole soak (observed 2026-06-04). Poll here so an
        in-flight restart is waited out instead of raced, and so we never
        stack a deliberate dual outage on top of a node the CP is already
        restarting (which would also risk the FTT budget).

        Unlike wait_for_all_online, a transient non-online state is NOT a
        failure: we keep polling until it clears or the timeout expires.
        Status strings mirror StorageNode: STATUS_RESTARTING='in_restart',
        STATUS_IN_SHUTDOWN='in_shutdown', STATUS_ONLINE='online'.
        """
        timeout = timeout or self.args.restart_timeout
        poll = poll or self.args.poll_interval
        started = time.time()
        while True:
            self.assert_cluster_not_suspended()
            nodes = self.ensure_expected_nodes()
            statuses = {node["uuid"]: node["status"] for node in nodes}
            not_ready = {
                uuid: status for uuid, status in statuses.items()
                if status != "online"
            }
            if not not_ready:
                return nodes
            if time.time() - started >= timeout:
                raise TestRunError(
                    "Timed out waiting for cluster to be outage-ready; "
                    "nodes not online: "
                    + ", ".join(f"{u}:{s}" for u, s in not_ready.items())
                )
            self.logger.log(
                "Waiting for cluster to be outage-ready "
                "(CP may be auto-healing a node): "
                + ", ".join(f"{u}:{s}" for u, s in not_ready.items())
            )
            time.sleep(poll)

    def wait_for_cluster_stable(self, require_no_rebalance=True):
        cluster_id = self.get_cluster_id()
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            cluster_list = self.sbctl("cluster list --json", json_output=True)
            status = str(cluster_list[0].get("Status", "")).lower()
            # Cluster Status is a COMPOSITE string: while a background rebalance
            # / data-migration is draining it reads "active - rebalancing", not
            # plain "active". The base state is still active. Split off any
            # " - <substate>" suffix so we judge "is the cluster active"
            # independently of "is it rebalancing" — the rebalance question is
            # gated separately by rebalance_ok / is_re_balancing below. Without
            # this split, `status == "active"` never matches during a rebalance
            # and the soak silently blocks on rebalance completion even when
            # rebalance gating is DISABLED (require_no_rebalance=False, the
            # default) — the regression that made the 200G soak hang for
            # minutes per iteration.
            base_status = status.split(" - ", 1)[0].strip()
            if base_status == "suspended":
                raise TestRunError("Cluster entered suspended state")
            cluster_info = self.sbctl(f"cluster get {cluster_id}", json_output=True)
            rebalancing = bool(cluster_info.get("is_re_balancing", False))
            nodes = self.ensure_expected_nodes()
            node_statuses = {node["uuid"]: node["status"] for node in nodes}
            rebalance_ok = (not rebalancing) or (not require_no_rebalance)
            if base_status == "active" and rebalance_ok and all(
                state == "online" for state in node_statuses.values()
            ):
                self.logger.log(
                    "Cluster stable: ACTIVE, online"
                    + ("" if require_no_rebalance else f" (rebalancing={rebalancing}, not gated)")
                )
                return
            self.logger.log(
                "Waiting for cluster stability: "
                f"status={status}, rebalancing={rebalancing}"
                + ("" if require_no_rebalance else " (rebalance not gated)")
                + ", "
                + ", ".join(f"{uuid}:{state}" for uuid, state in node_statuses.items())
            )
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for cluster to become stable")

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

    def settle_between_iterations(self, reason):
        """Inter-iteration settle gate.

        With --wait-for-rebalance, poll until the cluster is stable and all
        rebalance / data-migration tasks have drained (bounded by
        --rebalance-timeout). Without it (the default), bring the nodes back
        online and then sleep a fixed --inter-iteration-window instead of
        waiting for rebalancing to finish — appropriate when device-migration
        runners are disabled or rebalancing is not the thing under test.
        """
        if self.args.wait_for_rebalance:
            self.wait_for_cluster_stable()
            self.wait_for_data_migration_complete(reason)
            return
        # Default: nodes must be back online, then a fixed settle window.
        # Tolerate a peer the CP is autonomously restarting/shutting down
        # (no deliberate outage is in flight here) instead of aborting.
        self.wait_for_all_online(timeout=self.args.restart_timeout,
                                 tolerate_transient=True)
        self.logger.log(
            f"Fixed inter-iteration window before {reason}: "
            f"{self.args.inter_iteration_window}s "
            "(rebalance / data-migration wait disabled; pass "
            "--wait-for-rebalance to gate on completion instead)"
        )
        time.sleep(self.args.inter_iteration_window)

    def sbctl_allow_failure(self, args, timeout=600):
        command = "sudo /usr/local/bin/sbctl -d " + args
        rc, stdout_text, stderr_text = self.mgmt.run(
            command,
            timeout=timeout,
            check=False,
            label=f"sbctl {args}",
        )
        return rc, stdout_text, stderr_text

    def _graceful_shutdown(self, node_id):
        """Single-shot `sbctl sn shutdown`. Raises on failure.

        Previously this method looped on output markers like 'migration'
        / 'rebalanc' / 'active task' and slept 15s between retries — the
        soak would sit and wait for CP-side rebalancing to drain before
        the outage could proceed. That retry was needed because
        shutdown_storage_node used to call _check_ftt_allows_node_removal
        (added in commit fbdffea3, 2026-03-28) which refused the
        shutdown during rebalance with a stdout containing 'rebalanc'.
        That call has been removed from the CP — shutdown is supposed to
        proceed under the cluster's FTT contract, regardless of in-flight
        rebalance, so we no longer need to wait here either.

        Still resilient to the CP's concurrent-shutdown guard: if a peer is
        transiently STATUS_RESTARTING / STATUS_IN_SHUTDOWN — e.g. an
        autonomous StorageNodeMonitor heal that landed in the narrow window
        between wait_for_outage_ready and this call — `sn shutdown` returns
        rc=1 with "... cannot shutdown ... concurrently". That is transient,
        so retry with backoff (bounded by --restart-timeout) until the peer
        settles, mirroring the restart-retry loop in run_outage_pair,
        instead of aborting the whole soak.
        """
        deadline = time.time() + self.args.restart_timeout
        while True:
            rc, stdout_text, stderr_text = self.sbctl_allow_failure(
                f"sn shutdown {node_id}",
                timeout=300,
            )
            if rc == 0:
                return
            guard_hit = "concurrently" in (stdout_text or "")
            if guard_hit and time.time() < deadline:
                last = (stdout_text or "").strip().splitlines()
                self.logger.log(
                    f"Graceful shutdown of {node_id} rejected by CP "
                    f"concurrent-shutdown guard (a peer is restarting / "
                    f"shutting down); retrying in 15s: "
                    f"{last[-1] if last else ''}"
                )
                time.sleep(15)
                continue
            raise RemoteCommandError(
                f"mgmt: command failed with rc={rc}: sbctl sn shutdown {node_id}"
                f" | stdout={stdout_text.strip()} | stderr={stderr_text.strip()}"
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

    def _create_one_volume(self, volume_name, node_uuid, index):
        """Create one lvol bound to ``node_uuid`` and return its volume dict.

        Retries inside the rebalance window if the LVStore is being recreated
        or while a rebalance / data migration is in flight, matching the
        behaviour of the bulk ``create_volumes`` path.
        """
        volume_id = None
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            self.wait_for_all_online(timeout=self.args.restart_timeout)
            self.wait_for_cluster_stable(require_no_rebalance=self.args.wait_for_rebalance)
            output = self.sbctl(
                f"lvol add {volume_name} {self.args.volume_size} {self.args.pool} --host-id {node_uuid}"
            )
            if "ERROR:" in output or "LVStore is being recreated" in output:
                self.logger.log(f"Volume create for {volume_name} deferred: {output.strip()}")
                time.sleep(self.args.poll_interval)
                continue
            volume_id = self.extract_uuid(output)
            break
        if volume_id is None:
            raise TestRunError(f"Timed out creating volume {volume_name} on node {node_uuid}")
        self.created_volume_ids.append(volume_id)
        self.logger.log(f"Created volume {volume_name} ({volume_id}) on node {node_uuid}")
        return {
            "index": index,
            "volume_name": volume_name,
            "volume_id": volume_id,
            "node_uuid": node_uuid,
        }

    def create_volumes(self, nodes):
        self.logger.log(
            f"Creating {len(nodes)} volumes of size {self.args.volume_size}, one per storage node"
        )
        volumes = []
        for index, node in enumerate(nodes, start=1):
            volume_name = f"aws_dual_soak_{self.run_id}_v{index}"
            volumes.append(self._create_one_volume(volume_name, node["uuid"], index))
        return volumes

    def connect_and_mount_volumes(self, volumes, mount_root):
        self.logger.log("Connecting volumes to client and preparing filesystems")
        for volume in volumes:
            self._connect_and_mount_one(volume, mount_root)

    def _connect_and_mount_one(self, volume, mount_root):
        """Connect, mkfs, mount a single volume. Mutates ``volume`` to add
        mount_point / fio_log / fio_stderr / rc_file / nqn keys.

        Saving ``nqn`` lets the churn cycle disconnect via ``nvme disconnect
        -n <nqn>`` without having to re-derive it from the device path.
        """
        connect_output = self.sbctl(f"lvol connect {volume['volume_id']}")
        connect_commands = []
        for line in connect_output.splitlines():
            stripped = line.strip()
            if stripped.startswith("sudo nvme connect"):
                connect_commands.append(stripped)
        if not connect_commands:
            raise TestRunError(f"No nvme connect command returned for {volume['volume_id']}")
        nqn = None
        for cmd in connect_commands:
            m = NQN_RE.search(cmd)
            if m:
                nqn = m.group(1)
                break
        if nqn is None:
            raise TestRunError(
                f"Failed to parse NQN from lvol connect output for {volume['volume_id']}"
            )
        volume["nqn"] = nqn
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
        # Unique mount point per (re)build — never reuse a path. ``volume_name``
        # carries the churn counter (``..._v{index}_c{churn}``), so a churn
        # rebuild always mounts at a FRESH directory. A previous mount that
        # could not be cleanly unmounted (stuck target I/O) therefore can never
        # collide with or block the new mount.
        volume["mount_point"] = posixpath.join(mount_root, f"mnt_{volume['volume_name']}")
        volume["fio_log"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.log")
        volume["fio_stderr"] = posixpath.join(mount_root, f"fio_vol{volume['index']}.stderr")
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

    def _build_fio_name(self, index, churn_id):
        # Names embed both the volume index and the churn counter so the name
        # is unique even after a churn replaces a volume — avoids prefix
        # collisions when pkill -f matches by --name=<name>.
        return f"aws_dual_soak_v{index}_c{churn_id}"

    def _start_fio_for_volume(self, volume, fio_name):
        # Capture fio's stdout+stderr to a dedicated file. --output only
        # writes the aggregate summary on exit; progress lines and error
        # messages ("fio: max_latency exceeded", IO error details, etc.)
        # go to stderr during the run. That stream is the authoritative
        # source for "what went wrong" — surface it on every fault.
        start_script = (
            "set -euo pipefail\n"
            f"rm -f {shlex.quote(volume['rc_file'])} {shlex.quote(volume['fio_stderr'])}\n"
            "nohup bash -lc "
            + shlex.quote(
                f"cd {shlex.quote(volume['mount_point'])} && "
                f"fio --name={fio_name} --directory={shlex.quote(volume['mount_point'])} "
                "--direct=1 --rw=randrw --bs=4K --group_reporting --time_based "
                f"--numjobs={FIO_NUMJOBS} --iodepth=4 --size=40G --runtime={self.args.runtime} "
                "--ioengine=aiolib --max_latency=20s --exitall_on_error=1 "
                f"--output={shlex.quote(volume['fio_log'])}; "
                "rc=$?; "
                f"echo $rc > {shlex.quote(volume['rc_file'])}"
            )
            + f" > {shlex.quote(volume['fio_stderr'])} 2>&1 & echo $!"
        )
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(start_script)}",
            timeout=60,
            label=f"start fio {volume['volume_id']}",
        )
        pid_text = stdout_text.strip().splitlines()[-1]
        pid = int(pid_text)
        job = FioJob(
            volume_id=volume["volume_id"],
            volume_name=volume["volume_name"],
            mount_point=volume["mount_point"],
            fio_log=volume["fio_log"],
            fio_stderr=volume["fio_stderr"],
            rc_file=volume["rc_file"],
            pid=pid,
            fio_name=fio_name,
        )
        self.logger.log(f"Started fio for {volume['volume_name']} with pid {pid} (name={fio_name})")
        return job

    def start_fio(self, volumes):
        self.logger.log("Starting fio on all mounted volumes in parallel")
        fio_jobs = []
        for volume in volumes:
            fio_name = self._build_fio_name(volume["index"], 0)
            fio_jobs.append(self._start_fio_for_volume(volume, fio_name))
        self.fio_jobs = fio_jobs
        # Give fio a few seconds to begin; don't block on worker fork — the
        # authoritative "fio is in trouble" signal is rc_file / stderr, not
        # process counts. If fio never issues IO the pre-stop check and
        # outage cluster-health checks will surface that.
        time.sleep(5)

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

    # ----- fio fault detection --------------------------------------------

    # Any line in fio_stderr matching one of these (case-sensitive, fixed
    # string) is treated as a fio fault — even if fio is still running.
    # ``--max_latency`` violations in particular log "fio: latency of …
    # exceeds specified max" and do NOT always terminate fio when run
    # with --group_reporting + --numjobs>1, so a process-still-alive
    # check alone misses them.
    FIO_STDERR_ERROR_MARKERS = (
        "fio: latency of",     # --max_latency violation
        "fio: io_u error",     # io_u submission/completion error
        "fio: pid=",           # generic fio per-job error dump
        "io_u error on file",  # alternate io_u error format
        "verify failed",       # data verification fault
        "fio: verify",         # alternate verify error
        "fio: error",          # generic fio error
        "Killed",              # bash reports fio got SIGKILL
        "Terminated",          # bash reports fio got SIGTERM (no churn here)
    )

    def _read_rc_file(self, job):
        """Return the rc string if fio's wrapping bash wrote rc_file, else None.

        ``rc_file`` is one of three independent fault signals; see
        ``_check_fio_fault``.
        """
        probe = (
            f"if [ -f {shlex.quote(job.rc_file)} ]; then "
            f"cat {shlex.quote(job.rc_file)}; fi"
        )
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(probe)}",
            timeout=15,
            check=False,
            label=f"check rc_file {job.volume_name}",
        )
        rc = (stdout_text or "").strip()
        return rc or None

    def _wrapper_alive(self, job):
        """Return True iff the wrapping bash that runs fio is still alive.

        ``job.pid`` is the pid printed by ``echo $!`` at start_fio time
        (the nohup'd bash, parent of fio). If that pid is gone AND no
        rc_file was written, fio was signalled away and bash never got
        to record an exit code — that case is a fault, not "still running".
        """
        probe = (
            f"if kill -0 {int(job.pid)} 2>/dev/null; then echo alive; fi"
        )
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(probe)}",
            timeout=15,
            check=False,
            label=f"check wrapper pid {job.volume_name}",
        )
        return stdout_text.strip() == "alive"

    def _scan_fio_stderr_for_errors(self, job):
        """Return matching error lines from fio_stderr (up to 20), or "".

        See FIO_STDERR_ERROR_MARKERS for the list. ``--max_latency``
        violations in particular are reported here even while fio
        continues running, so this catches faults the rc_file / pid
        checks would miss.
        """
        if not self.FIO_STDERR_ERROR_MARKERS:
            return ""
        grep_args = " ".join(
            f"-e {shlex.quote(p)}" for p in self.FIO_STDERR_ERROR_MARKERS
        )
        grep_cmd = (
            f"grep -F -m 20 {grep_args} "
            f"{shlex.quote(job.fio_stderr)} 2>/dev/null || true"
        )
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(grep_cmd)}",
            timeout=15,
            check=False,
            label=f"scan stderr {job.volume_name}",
        )
        return stdout_text.strip()

    def _check_fio_fault(self, job):
        """Detect any fio fault for ``job``. Returns ``(kind, detail)`` or None.

        Three independent signals — ANY one is a fault:
          * ``exited``: fio's wrapping bash wrote rc_file (any rc, including 0,
            is a fault mid-run because fio's --runtime is orders of magnitude
            longer than an outage iteration).
          * ``missing``: the wrapping bash pid is gone and no rc_file was
            written — fio was signalled away (or its wrapper died) without
            recording an exit code.
          * ``stderr_error``: fio_stderr contains a known fio error marker
            (max_latency violation, io_u error, verify failure, etc.) — fio
            may still be running but is degraded; treat it as a fault.

        ``detail`` is a human-readable one-liner. The full stderr/output
        is dumped via ``_dump_fio_streams`` by the callers.
        """
        rc = self._read_rc_file(job)
        if rc is not None:
            return ("exited", f"fio exited rc={rc}")

        if not self._wrapper_alive(job):
            return (
                "missing",
                f"fio wrapper pid {job.pid} is gone and no rc_file was written",
            )

        err = self._scan_fio_stderr_for_errors(job)
        if err:
            first_line = err.splitlines()[0][:240]
            return ("stderr_error", f"stderr error marker: {first_line}")

        return None

    def _dump_fio_streams(self, job, context):
        """Write fio's captured stderr and --output summary into the soak
        log so the actual fio error text (max_latency violations, IO
        errors, "fio: pid=…, err=…, func=…" lines) is visible next to
        the outage scenario that triggered it."""
        for label, path, lines in [
            ("fio stderr",  job.fio_stderr, 200),
            ("fio summary", job.fio_log,     60),
        ]:
            _, body, _ = self.client.run(
                f"bash -lc {shlex.quote(f'tail -{lines} {shlex.quote(path)} 2>/dev/null || true')}",
                timeout=30,
                check=False,
                label=f"dump {label} {job.volume_name}",
            )
            if body.strip():
                self.logger.block(
                    f"[{context}] {job.volume_name} {label} ({path}):",
                    body,
                )
            else:
                self.logger.log(
                    f"[{context}] {job.volume_name} {label} ({path}): (empty)"
                )

    def check_fio(self):
        """Raise if any tracked fio shows a fault.

        Three independent signals are evaluated per ``_check_fio_fault``:
        rc_file written (fio exited), wrapper pid gone with no rc_file
        (signalled away), or a fio error marker in stderr (max_latency
        violation, io_u/verify error, etc.). ANY of these is a fault —
        fio's ``--runtime`` is orders of magnitude longer than a single
        outage iteration, and a degraded-but-running fio is just as
        invalid a result as a dead one.

        On fault, every faulting job's captured stderr and --output
        summary are dumped into the soak log so the exact fio error
        lines are visible next to the iteration that triggered them.
        """
        faulted = []
        for job in self.fio_jobs:
            fault = self._check_fio_fault(job)
            if fault is not None:
                faulted.append((job, fault))
        if not faulted:
            return
        for job, (kind, detail) in faulted:
            self._dump_fio_streams(job, context=f"fio fault [{kind}] {detail}")
        details = ", ".join(
            f"{j.volume_name}={kind}:{detail}" for j, (kind, detail) in faulted
        )
        raise TestRunError(f"fio fault detected: {details}")

    def ensure_fio_running(self):
        self.check_fio()

    def stop_fio(self):
        """Stop every fio process launched by this soak on the client host.

        Called between outage iterations so rebalancing runs unloaded.
        Before killing, calls ``check_fio`` — any fio that wrote its
        rc_file is a mid-run exit, which is a fault. Dumps the captured
        fio stderr/summary into the soak log so the actual fio error
        text is side-by-side with the outage scenario that triggered it.
        After the check passes we SIGTERM (short grace window) then
        SIGKILL; matching by ``fio --name=aws_dual_soak_*`` catches both
        the bash wrapper and any fio workers.
        """
        if not self.fio_jobs:
            return

        # Pre-kill verification: any fio having exited is a fault.
        self.check_fio()

        self.logger.log("All fio still running; stopping them between iterations")
        kill_script = (
            "set +e\n"
            "sudo pkill -TERM -f 'fio --name=aws_dual_soak_' 2>/dev/null || true\n"
            "for i in $(seq 1 15); do\n"
            "  if ! pgrep -f 'fio --name=aws_dual_soak_' >/dev/null; then\n"
            "    exit 0\n"
            "  fi\n"
            "  sleep 2\n"
            "done\n"
            "sudo pkill -KILL -f 'fio --name=aws_dual_soak_' 2>/dev/null || true\n"
        )
        self.client.run(
            f"bash -lc {shlex.quote(kill_script)}",
            timeout=90,
            check=False,
            label="stop fio",
        )
        # Drop the job list; start_fio will rebuild it from self.volumes.
        self.fio_jobs = []

    # ----- single-volume fio + churn ---------------------------------------

    def _check_one_fio(self, job, context):
        """Pre-churn fio fault check for one job. Same contract as
        ``check_fio`` (rc_file / wrapper-gone / stderr error is a fault)
        but scoped to a single job so a healthy churn doesn't get
        blocked by an unrelated already-faulted job that the post-outage
        check would surface separately.
        """
        fault = self._check_fio_fault(job)
        if fault is None:
            return
        kind, detail = fault
        self._dump_fio_streams(job, context=f"{context} fio fault [{kind}] {detail}")
        raise TestRunError(
            f"fio for {job.volume_name} fault [{kind}]: {detail} ({context})"
        )

    def _stop_fio_for_job(self, job):
        """Kill the single fio matching this job's exact ``--name=`` arg.

        Names embed the volume index AND a churn counter (e.g.
        ``aws_dual_soak_v3_c0``), so an exact ``fio --name=<name> ``
        match never collides with sibling jobs.
        """
        # The space after <name> guarantees we don't accidentally kill a
        # later-churn-cycle job whose name shares the same prefix (e.g.
        # ``..._c0`` vs ``..._c10`` — without the space this could match).
        pattern = f"fio --name={job.fio_name} "
        kill_script = (
            "set +e\n"
            f"sudo pkill -TERM -f {shlex.quote(pattern)} 2>/dev/null || true\n"
            "for i in $(seq 1 15); do\n"
            f"  if ! pgrep -f {shlex.quote(pattern)} >/dev/null; then\n"
            "    exit 0\n"
            "  fi\n"
            "  sleep 2\n"
            "done\n"
            f"sudo pkill -KILL -f {shlex.quote(pattern)} 2>/dev/null || true\n"
        )
        self.client.run(
            f"bash -lc {shlex.quote(kill_script)}",
            timeout=90,
            check=False,
            label=f"stop fio {job.fio_name}",
        )

    def _disconnect_one_volume(self, volume):
        nqn = volume.get("nqn")
        if not nqn:
            self.logger.log(
                f"WARNING: no NQN saved for {volume['volume_name']}; skipping nvme disconnect"
            )
            return
        # ``nvme disconnect -n <nqn>`` tears down every controller (path) for
        # that subsystem in one call, so multipath connections are handled
        # without a per-path teardown loop.
        self.client.run(
            f"sudo nvme disconnect -n {shlex.quote(nqn)}",
            timeout=60,
            check=False,
            label=f"nvme disconnect {volume['volume_name']}",
        )

    def _unmount_one_volume(self, volume):
        """Best-effort unmount before disconnect/delete. NEVER blocks the churn
        and NEVER raises.

        A stuck target I/O can wedge a plain ``umount``/``sync``/``fuser`` in
        uninterruptible D-state (the client runs with ``nvme_core.io_timeout``
        effectively infinite, so such an I/O never times out on its own). That
        previously hung the SSH command for the full timeout and then aborted
        the entire churn cycle with a transport/timeout error — which surfaced
        much later as a bogus "fio missing" fault and killed the soak (run
        20260623_125401: vol5, 13:07 stop → umount hang → 13:11 churn timeout →
        13:14 missing-fio abort).

        We no longer care about a stuck unmount. Every (re)build mounts at a
        FRESH path (see ``_connect_and_mount_one``), so a lingering old mount
        cannot collide with or block the rebuild. Try one quick clean umount
        (flushes the XFS journal on the happy path, avoiding dmesg noise), then
        a lazy detach (``umount -l`` returns immediately even with I/O stuck in
        D-state), bound the whole thing hard, and swallow any hang/error so the
        churn proceeds.
        """
        mount_point = volume.get("mount_point")
        if not mount_point:
            return
        umount_script = (
            "set +e\n"
            'mp="$1"\n'
            'mountpoint -q "$mp" || exit 0\n'
            # one quick clean attempt (XFS journal flush on the happy path)
            'sudo umount "$mp" 2>/dev/null && exit 0\n'
            # stuck: evict holders and lazy-detach. `umount -l` removes the
            # mount from the namespace immediately and reaps it if/when the
            # stuck I/O ever completes — it does not block on D-state I/O.
            'sudo fuser -km "$mp" 2>/dev/null\n'
            'sudo umount -l "$mp" 2>/dev/null\n'
            "exit 0\n"
        )
        try:
            self.client.run(
                f"bash -lc {shlex.quote(umount_script)} _ {shlex.quote(mount_point)}",
                timeout=30,
                check=False,
                label=f"umount {volume['volume_name']}",
            )
        except Exception as exc:  # noqa: BLE001 — a stuck umount must never abort churn
            self.logger.log(
                f"WARNING: umount of {volume['volume_name']} ({mount_point}) hung "
                f"or failed ({exc!r}); ignoring and continuing. The rebuild uses a "
                f"fresh mount point, so the stuck mount is harmless."
            )

    def _delete_one_lvol(self, volume):
        rc, stdout_text, stderr_text = self.sbctl_allow_failure(
            f"lvol delete {volume['volume_id']} --force",
            timeout=600,
        )
        if rc != 0:
            raise TestRunError(
                f"lvol delete failed for {volume['volume_name']} ({volume['volume_id']}): "
                f"{stdout_text.strip()} | {stderr_text.strip()}"
            )
        if volume["volume_id"] in self.created_volume_ids:
            self.created_volume_ids.remove(volume["volume_id"])

    def _churn_one_volume(self):
        """Churn a single random volume end-to-end.

        Holds ``serial_lock`` for the whole cycle so it can't overlap an
        outage iteration (which mutates cluster state in conflicting ways:
        an in-progress lvol create on an offline node will fail).
        """
        with self.serial_lock:
            if not self.fio_jobs or not self.volumes:
                return
            idx = random.randrange(len(self.volumes))
            old_volume = self.volumes[idx]
            job = next(
                (j for j in self.fio_jobs if j.volume_id == old_volume["volume_id"]),
                None,
            )
            if job is None:
                self.logger.log(
                    f"churn: no fio job for {old_volume['volume_name']}; skipping"
                )
                return

            # Pre-churn fio check: same contract as the post-outage check —
            # any fio rc (clean or not) mid-run is a fault that must abort
            # the soak.
            self._check_one_fio(job, context="churn pre-check")

            self.churn_counter += 1
            churn_id = self.churn_counter
            new_name = f"aws_dual_soak_{self.run_id}_v{old_volume['index']}_c{churn_id}"
            self.logger.log(
                f"churn {churn_id}: rebuilding {old_volume['volume_name']} "
                f"({old_volume['volume_id']}) on node {old_volume['node_uuid']} "
                f"-> {new_name}"
            )

            # 1. stop fio for this volume only (others keep running)
            self._stop_fio_for_job(job)
            # 2. unmount (clean; warns + lazy-detaches if a stuck target I/O
            #    pins the mount). The return flag is informational — we still
            #    proceed to disconnect/delete since the lvol is going away, but
            #    a False has already been logged as a loud WARNING above.
            self._unmount_one_volume(old_volume)
            # 3. nvme disconnect (paths torn down only after the umount above)
            self._disconnect_one_volume(old_volume)
            # 4. delete lvol
            self._delete_one_lvol(old_volume)

            # 5. recreate lvol on the SAME storage node so the topology used
            # by the outage scenario list (which is pinned at startup off
            # role-representative pairs) doesn't drift.
            new_volume = self._create_one_volume(
                new_name,
                old_volume["node_uuid"],
                old_volume["index"],
            )
            # 6. connect, mkfs, mount — populates mount_point/nqn/etc.
            self._connect_and_mount_one(new_volume, self.mount_root)
            # 7. restart fio with a fresh, churn-counter-tagged name
            new_fio_name = self._build_fio_name(new_volume["index"], churn_id)
            new_job = self._start_fio_for_volume(new_volume, new_fio_name)

            # 8. swap in the new volume + job atomically under the lock
            self.volumes[idx] = new_volume
            self.fio_jobs = [
                j for j in self.fio_jobs if j.volume_id != old_volume["volume_id"]
            ] + [new_job]
            self.logger.log(
                f"churn {churn_id}: complete; {new_name} ({new_volume['volume_id']}) "
                f"running fio name={new_fio_name}"
            )

    def _churn_loop(self):
        """Background loop: sleep random(churn_min, churn_max), then churn one
        volume. Exits cleanly when ``churn_stop_event`` is set.

        Any exception is captured into ``self.churn_error`` and the event is
        set so the main thread re-raises it on its next sync point — the
        soak is meant to halt on any fio fault or churn failure.
        """
        rng = random.Random()
        while not self.churn_stop_event.is_set():
            wait = rng.uniform(self.args.churn_min_seconds, self.args.churn_max_seconds)
            self.logger.log(f"churn loop: next churn in {wait:.0f}s")
            if self.churn_stop_event.wait(wait):
                return
            try:
                self._churn_one_volume()
            except (TestRunError, RemoteCommandError, ValueError) as exc:
                self.logger.log(f"ERROR in churn cycle: {exc}")
                self.churn_error = exc
                self.churn_stop_event.set()
                return
            except Exception as exc:  # noqa: BLE001 — catch-all is intentional
                self.logger.log(f"UNEXPECTED ERROR in churn cycle: {exc!r}")
                self.churn_error = exc
                self.churn_stop_event.set()
                return

    def start_churn_thread(self):
        if self.args.no_churn:
            self.logger.log("churn thread disabled (--no-churn)")
            return
        self.churn_stop_event.clear()
        self.churn_error = None
        self.churn_thread = threading.Thread(
            target=self._churn_loop,
            name="churn-loop",
            daemon=True,
        )
        self.churn_thread.start()
        self.logger.log(
            f"churn thread started (interval {self.args.churn_min_seconds}-"
            f"{self.args.churn_max_seconds}s)"
        )

    def stop_churn_thread(self):
        if self.churn_thread is None:
            return
        self.churn_stop_event.set()
        self.churn_thread.join(timeout=120)
        if self.churn_thread.is_alive():
            self.logger.log("WARNING: churn thread did not exit within 120s")
        self.churn_thread = None

    def reraise_churn_error(self):
        """Re-raise any error captured by the churn thread on the main thread.

        Called at outage-iteration sync points so churn faults surface in
        the same exit path as outage / fio faults.
        """
        if self.churn_error is not None:
            err = self.churn_error
            self.churn_error = None
            raise err

    # ----- outage methods ---------------------------------------------------

    def _forced_shutdown(self, node_id):
        """Single-shot `sbctl sn shutdown --force`. Raises on failure.

        --force bypasses every shutdown guard inside the CP, so the
        retry-on-migration-markers loop that used to live here never
        actually fired — sbctl --force does not return 'migration' /
        'rebalanc' / 'active task' strings. Removed for clarity.
        """
        rc, stdout_text, stderr_text = self.sbctl_allow_failure(
            f"sn shutdown {node_id} --force",
            timeout=300,
        )
        if rc != 0:
            raise RemoteCommandError(
                f"mgmt: command failed with rc={rc}: sbctl sn shutdown {node_id} --force"
                f" | stdout={stdout_text.strip()} | stderr={stderr_text.strip()}"
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
        """Drop one data NIC on every storage node for nic_chaos_duration s,
        then bring it back up. Active only between outage iterations on a
        multipath cluster with at least two data NICs.

        Invariant: a single NIC name (eth1 OR eth2) is chosen once via
        random.choice and applied uniformly across all storage nodes — we
        NEVER concurrently drop eth1 on some nodes and eth2 on others, so
        each node always retains a live data path through the surviving
        NIC. _ensure_all_data_nics_up at the start of every outage
        iteration plus the try/finally re-enable here together guarantee
        no NIC stays down across the iteration boundary.
        """
        if self.args.no_nic_chaos or not self._is_multipath():
            return
        data_nics = self._get_data_nics()
        if len(data_nics) < 2:
            return
        duration = max(0, self.args.nic_chaos_duration)
        nic = random.choice(data_nics)
        self.logger.log(
            f"Inter-iteration NIC chaos: dropping {nic} on all nodes for {duration}s "
            f"(other data NICs stay up; eth1/eth2 are never dropped concurrently)"
        )
        try:
            self._disable_nic_on_all_nodes(nic)
            time.sleep(duration)
        finally:
            self._enable_nic_on_all_nodes(nic)
        # Tolerate an autonomous CP restart/shutdown that overlapped the chaos
        # window; only a hard-offline peer aborts here.
        self.wait_for_all_online(timeout=self.args.restart_timeout,
                                 tolerate_transient=True)
        self.wait_for_cluster_stable(require_no_rebalance=self.args.wait_for_rebalance)

    def _network_outage(self, node_id, duration):
        """Drop data NICs on one storage node; schedule the NIC bring-up
        ``duration`` seconds later on a background daemon thread, then
        return.

        Previously this method blocked for the full ``duration`` (the
        sleep ran inline before bringing NICs back up). That made it
        impossible to overlap a network_outage_N outage with a second
        outage applied within the same iteration — by the time
        run_outage_pair called _apply_outage for node 2, node 1's NICs
        were already up and the CP was already healing it. Decoupling
        the bring-up from the call site lets the second outage land
        while the first node is still partitioned.

        The bring-up thread is daemonized so the soak's exit (atexit /
        unhandled exception) does not block on it. We do NOT join the
        thread anywhere in the iteration: the only thing that depends
        on the NICs being back up is the next iteration's
        wait_for_all_online, which polls anyway.
        """
        host = self._node_host(node_id)
        nics = self._get_data_nics() or ["eth1"]
        self.logger.log(
            f"network_outage on {node_id}: dropping {nics} for {duration}s "
            "(async bring-up)"
        )
        for nic in nics:
            try:
                host.run(f"sudo ip link set {nic} down", timeout=10, check=False,
                         label=f"netout down {nic} on {node_id}")
            except Exception as e:
                self.logger.log(f"WARNING: failed to down {nic} on {node_id}: {e}")

        def _bring_up_later():
            try:
                time.sleep(duration)
            finally:
                for nic in nics:
                    try:
                        host.run(f"sudo ip link set {nic} up", timeout=10, check=False,
                                 label=f"netout up {nic} on {node_id}")
                    except Exception as e:
                        self.logger.log(f"WARNING: failed to up {nic} on {node_id}: {e}")

        t = threading.Thread(target=_bring_up_later, daemon=True,
                             name=f"netout-bringup-{node_id[:8]}")
        t.start()

    def _apply_outage(self, node_id, method):
        self.logger.log(f"Applying outage '{method}' on {node_id}")
        if method == "graceful":
            self._graceful_shutdown(node_id)
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

    # Conservative lower bound on how long node stays not-online for each
    # outage method. Used to cap the inter-outage gap so that the
    # configured --min-outage-overlap is guaranteed (the gap can never
    # eat the entire recovery window of outage 1). Real recovery is
    # usually longer; underestimating keeps the overlap invariant safe.
    #
    # graceful / forced: the node stays in OFFLINE until run_outage_pair
    # issues `sn restart` later in the iteration — so the unavailability
    # window is effectively unbounded from the gap's perspective. Use a
    # very large sentinel.
    _METHOD_MIN_UNAVAIL_S = {
        "graceful": 10_000,
        "forced": 10_000,
        # CP detection + auto-restart takes at least this long in practice.
        "container_kill": 30,
        # Reboot itself, BIOS, boot, SPDK start. Floor is generous.
        "host_reboot": 90,
        # network_outage_N handled by name parsing below.
    }

    def _expected_min_unavail_seconds(self, method):
        if method.startswith("network_outage_"):
            try:
                return int(method.rsplit("_", 1)[-1])
            except ValueError:
                return 30
        return self._METHOD_MIN_UNAVAIL_S.get(method, 30)

    def _pick_outage_gap(self, method1):
        """Random gap in [outage_gap_min, outage_gap_max], capped per
        method1 so --min-outage-overlap is guaranteed.

        --shutdown-gap > 0 overrides everything with a fixed constant
        (legacy behaviour; emit a warning if the constant would violate
        the overlap invariant for method1).
        """
        overlap = max(0, self.args.min_outage_overlap)
        unavail = self._expected_min_unavail_seconds(method1)
        # Hard upper bound: gap + overlap <= unavail  =>  gap <= unavail - overlap
        cap = max(1, unavail - overlap)

        if self.args.shutdown_gap and self.args.shutdown_gap > 0:
            gap = self.args.shutdown_gap
            if gap > cap:
                self.logger.log(
                    f"WARNING: --shutdown-gap={gap}s exceeds method1={method1}'s "
                    f"safe cap {cap}s; overlap of {overlap}s is NOT guaranteed"
                )
            return gap

        lo = max(1, self.args.outage_gap_min)
        hi = max(lo, self.args.outage_gap_max)
        # Clamp the upper bound to the cap; clamp the lower bound to
        # respect the cap too (otherwise random.randint would raise).
        hi = min(hi, cap)
        lo = min(lo, hi)
        gap = random.randint(lo, hi)
        self.logger.log(
            f"Outage gap chosen: {gap}s "
            f"(range=[{lo},{hi}], cap={cap}s for method1={method1}, "
            f"min-overlap={overlap}s)"
        )
        return gap

    def run_outage_pair(self, node1, node2, method1, method2):
        self.logger.log(
            f"Outage pair: {node1}={method1} and {node2}={method2}"
        )
        # Apply first outage, then a method1-aware gap, then second outage.
        # The gap is bounded so node 1's recovery window is guaranteed to
        # span at least --min-outage-overlap seconds after node 2 goes
        # down — i.e., both nodes are simultaneously not-online for the
        # configured minimum.
        # The only outage that consults the CP's concurrent-shutdown guard is
        # a non-force `sn shutdown` (the "graceful" method): "forced" passes
        # --force and bypasses it, and container_kill/host_reboot/network_*
        # aren't sbctl operations at all. If graceful is applied *second*, the
        # first node may already be RESTARTING (auto-recover method) or
        # IN_SHUTDOWN (forced), and the guard rejects it -- observed
        # 2026-06-04 iter 11: peer container_kill left 4d36a4a7 RESTARTING,
        # graceful on 6c9be7f7 returned "cannot shutdown ... concurrently"
        # (rc=1) and the soak aborted. Apply the graceful outage first so its
        # shutdown lands while the peer is still online; the partner method is
        # always safe to apply second. Methods in a pair are always distinct,
        # so there is at most one graceful. The node<->method assignment is
        # unchanged -- only the application order flips, and overlap still
        # holds because graceful stays down until the manual restart below,
        # well past the 2nd outage.
        first_node, first_method = node1, method1
        second_node, second_method = node2, method2
        if method2 == "graceful" and method1 != "graceful":
            first_node, first_method = node2, method2
            second_node, second_method = node1, method1

        gap = self._pick_outage_gap(first_method)
        t_outage1 = time.time()
        self._apply_outage(first_node, first_method)
        time.sleep(gap)
        t_outage2 = time.time()
        self._apply_outage(second_node, second_method)
        self.logger.log(
            f"Outage pair applied: {first_node[:8]}={first_method} at t=0, "
            f"{second_node[:8]}={second_method} at "
            f"t={t_outage2 - t_outage1:.1f}s (gap={gap}s)"
        )

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
                    # Emit a RESTART header with the wall-clock timestamp,
                    # then dump the raw sbctl -d restart stdout below it
                    # (without per-line timestamp prefix) so the CP trace
                    # produced by -d lines up with a single moment in time.
                    self.logger.log(
                        f"RESTART: {time.strftime('%Y-%m-%d %H:%M:%S')} {node_id}"
                    )
                    stdout_text = self.sbctl(f"sn restart {node_id}", timeout=300)
                    with self.logger.lock:
                        print(stdout_text, flush=True, end=""
                              if stdout_text.endswith("\n") else "\n")
                        with open(self.logger.path, "a", encoding="utf-8") as handle:
                            handle.write(stdout_text)
                            if not stdout_text.endswith("\n"):
                                handle.write("\n")
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
        # Intentionally no check_fio / wait_for_cluster_stable here: the
        # outer loop calls check_fio under serial_lock right after this
        # returns, then waits for cluster stability — and the churn thread
        # is also serialized via that lock, so any natural fio completion
        # (e.g. runtime expired) is surfaced consistently in one place.

    # ----- topology & scenario enumeration ---------------------------------

    def discover_topology(self):
        """Return {lvs_name: {'primary': uuid, 'secondary': uuid, 'tertiary': uuid}}.

        Queried once at soak startup to identify the 4 role-representative
        node pairs. Leader takeover mid-soak may shift role assignments;
        the scenario list is pinned at startup so the 4 chosen pairs stay
        fixed across retries even if the CP has re-promoted since.
        """
        script = (
            "import json; "
            "from simplyblock_core import db_controller; "
            "db = db_controller.DBController(); "
            "nodes = db.get_storage_nodes(); "
            "out = {n.lvstore: {"
            "'primary': n.get_id(), "
            "'secondary': getattr(n, 'secondary_node_id', '') or '', "
            "'tertiary': getattr(n, 'tertiary_node_id', '') or ''"
            "} for n in nodes "
            "if getattr(n, 'lvstore', '') "
            "and not getattr(n, 'is_secondary_node', False)}; "
            "print(json.dumps(out))"
        )
        _, stdout_text, _ = self.mgmt.run(
            f"sudo python3 -c {shlex.quote(script)}",
            timeout=60,
            label="discover topology",
        )
        for line in reversed((stdout_text or "").strip().splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        raise TestRunError(
            f"Failed to parse topology JSON from mgmt; stdout was:\n{stdout_text}"
        )

    def _validate_topology_for_categories(self):
        """Verify the pinned topology can supply at least one pair per category.

        Raises TestRunError if:
          * the pinned topology has no LVS (empty cluster)
          * no LVS has both primary and secondary (primary_secondary unservable)
          * no LVS has both primary and tertiary (primary_tertiary unservable)
          * no unrelated pair exists — in a dense FT=2 ring with N ≤ 4 this
            is possible; raise so the coverage gap is explicit.
        """
        if not self.topology:
            raise TestRunError("Empty topology; cannot pick representative pairs")

        if not self._candidate_pairs_for_role("secondary"):
            raise TestRunError(
                "No LVS in topology has both primary and secondary; "
                "primary_secondary category is unservable"
            )
        if not self._candidate_pairs_for_role("tertiary"):
            raise TestRunError(
                "No LVS in topology has both primary and tertiary; "
                "primary_tertiary category is unservable"
            )

        all_nodes, lvs_members = self._lvs_membership()
        if not self._unrelated_pairs(all_nodes, lvs_members):
            raise TestRunError(
                "No unrelated node pair found in topology "
                f"({len(all_nodes)} nodes across {len(lvs_members)} LVSs)"
            )

    def _candidate_pairs_for_role(self, role_b):
        """All (primary, role_b) pairs across the pinned topology."""
        pairs = []
        for roles in self.topology.values():
            a = roles.get("primary")
            b = roles.get(role_b)
            if a and b:
                pairs.append((a, b))
        return pairs

    def _lvs_membership(self):
        """Return (all_nodes, lvs_members) derived from the pinned topology."""
        all_nodes = set()
        lvs_members = []
        for r in self.topology.values():
            members = {v for v in r.values() if v}
            lvs_members.append(members)
            all_nodes.update(members)
        return all_nodes, lvs_members

    def _unrelated_pairs(self, all_nodes, lvs_members):
        """All node pairs that share no LVS in any role."""
        pairs = []
        for a, b in itertools.combinations(sorted(all_nodes), 2):
            if not any(a in m and b in m for m in lvs_members):
                pairs.append((a, b))
        return pairs

    def pick_pair_for_category(self, category):
        """Randomly pick a (node_a, node_b) pair for the given role category.

        Distance preserved per category (so each scenario in a "group" hits
        the same topological relationship, just on different concrete nodes):
          - primary_secondary: ring-distance 1 (direct successor)
          - primary_tertiary:  ring-distance 2 (exactly one node between)
          - unrelated:         ring-distance ≥ 3 (≥ 2 nodes between)
        """
        if category == "unrelated":
            all_nodes, lvs_members = self._lvs_membership()
            candidates = self._unrelated_pairs(all_nodes, lvs_members)
        elif category == "primary_secondary":
            candidates = self._candidate_pairs_for_role("secondary")
        elif category == "primary_tertiary":
            candidates = self._candidate_pairs_for_role("tertiary")
        else:
            raise TestRunError(f"Unknown role category: {category}")

        if not candidates:
            raise TestRunError(
                f"No candidate pairs available for category {category}"
            )
        return random.choice(candidates)

    # ----- failure-domain discovery & outage ------------------------------

    def discover_failure_domains(self):
        """Return {failure_domain_id: [node_uuid, ...]} from the mgmt DB.

        Only physical storage nodes tagged with a failure domain (>= 0) are
        included — i.e. the nodes added with ``--failure-domain`` against a
        cluster created with ``--enable-failure-domain``. Queried once at
        startup; node UUIDs are stable across the soak.
        """
        script = (
            "import json; "
            "from simplyblock_core import db_controller; "
            "db = db_controller.DBController(); "
            "nodes = db.get_storage_nodes(); "
            "out = {}; "
            "[out.setdefault(getattr(n, 'failure_domain', -1), []).append(n.get_id()) "
            "for n in nodes "
            "if getattr(n, 'failure_domain', -1) >= 0 "
            "and not getattr(n, 'is_secondary_node', False)]; "
            "print(json.dumps(out))"
        )
        _, stdout_text, _ = self.mgmt.run(
            f"sudo python3 -c {shlex.quote(script)}",
            timeout=60,
            label="discover failure domains",
        )
        for line in reversed((stdout_text or "").strip().splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # JSON object keys are strings; coerce failure-domain ids to int.
                return {int(k): v for k, v in raw.items()}
        raise TestRunError(
            f"Failed to parse failure-domain JSON from mgmt; stdout was:\n{stdout_text}"
        )

    def _validate_failure_domains(self):
        """The two scenario types need >= 2 non-empty failure domains."""
        if not self.failure_domains:
            raise TestRunError(
                "No failure domains discovered. The cluster must be created with "
                "--enable-failure-domain and each node added with --failure-domain "
                "<id> (see setup_perf_test_failure_domain.py)."
            )
        if len(self.failure_domains) < 2:
            raise TestRunError(
                "Need >= 2 failure domains for the one-per-FD scenario; found "
                f"{sorted(self.failure_domains)}"
            )
        for fd, uuids in self.failure_domains.items():
            if not uuids:
                raise TestRunError(f"Failure domain {fd} has no nodes")

    def build_scenarios(self, nodes):
        """Build the failure-domain outage scenario list.

        Scenario *types*, per the test design:
          (a) ``same_fd``     — reboot every host in one failure domain. One
              such scenario per failure domain (so each domain gets exercised).
              Followed by a full rebalance-completion wait.
          (b) ``one_per_fd``  — reboot one random node in each failure domain.
              Followed by a fixed 60s settle (no rebalance wait needed).
          (c) ``fd_plus_one`` — ONLY when there are >= 4 failure domains:
              reboot a whole (random) failure domain AND one extra node in one
              of the remaining domains. With >= 4 domains the data/parity
              chunks are spread widely enough that this combined loss stays
              within the fault-tolerance budget, so it must survive and is
              followed by a full rebalance-completion wait (like same_fd).

        The concrete node(s) for ``one_per_fd`` and ``fd_plus_one`` are rolled
        randomly at iteration time in ``_select_outage_targets``.
        """
        _ = nodes  # node set comes from the discovered failure-domain map
        fds = sorted(self.failure_domains)
        if self.combo_methods is not None:
            # --combo-only: just the whole-domain-plus-one-extra-node scenario,
            # re-rolled randomly every iteration. Needs >= 4 domains so the
            # combined loss stays within the fault-tolerance budget.
            if len(fds) < 4:
                raise TestRunError(
                    f"--combo-only requires >= 4 failure domains; found {len(fds)} "
                    f"({fds}). Redeploy with setup_perf_test_failure_domain.py --fd 4."
                )
            self.logger.log(
                f"Built 1 combo scenario (fd_plus_one) over domains {fds}; "
                f"per-node methods drawn from {self.combo_methods}"
            )
            return [{"type": "fd_plus_one"}]
        scenarios = [{"type": "same_fd", "fd": fd} for fd in fds]
        scenarios.append({"type": "one_per_fd"})
        extra = ""
        if len(fds) >= 4:
            scenarios.append({"type": "fd_plus_one"})
            extra = " + 1 whole-FD-plus-one-extra-node reboot"
        self.logger.log(
            f"Built {len(scenarios)} failure-domain scenarios: "
            f"{len(fds)} same-FD whole-domain reboots (domains {fds}) "
            f"+ 1 one-per-FD reboot{extra}"
        )
        return scenarios

    def _select_outage_targets(self, scenario):
        """Resolve a scenario to the concrete list of node UUIDs to reboot."""
        fd_map = self.failure_domains
        if scenario["type"] == "same_fd":
            return list(fd_map.get(scenario["fd"], []))
        if scenario["type"] == "one_per_fd":
            targets = []
            for _fd, uuids in sorted(fd_map.items()):
                if uuids:
                    targets.append(random.choice(uuids))
            return targets
        if scenario["type"] == "fd_plus_one":
            # Take down a whole (random) failure domain PLUS one extra node in
            # one of the remaining domains. Only built with >= 4 domains, but
            # guard defensively: need at least 2 populated domains.
            populated = [fd for fd, uuids in fd_map.items() if uuids]
            if len(populated) < 2:
                return []
            down_fd = random.choice(populated)
            targets = list(fd_map[down_fd])
            other_fds = [fd for fd in populated if fd != down_fd]
            extra_fd = random.choice(other_fds)
            targets.append(random.choice(fd_map[extra_fd]))
            self.logger.log(
                f"fd_plus_one: whole domain {down_fd} "
                f"({[u[:8] for u in fd_map[down_fd]]}) + one node from domain "
                f"{extra_fd} ({targets[-1][:8]})"
            )
            return targets
        raise TestRunError(f"Unknown scenario type: {scenario.get('type')}")

    def _select_outage_methods(self, targets):
        """Assign a random outage method to each target node from the
        --combo-methods pool.

        Constraint: at most ONE node may use 'graceful'. A non-force
        `sn shutdown` consults the CP concurrent-shutdown guard, which rejects
        a graceful shutdown while any peer is already in_shutdown/restarting,
        so two simultaneous gracefuls can't both land. Extra graceful picks
        are demoted to a random auto-recover method (so the whole group still
        goes down together)."""
        pool = list(self.combo_methods)
        methods = {n: random.choice(pool) for n in targets}
        graceful_nodes = [n for n, m in methods.items() if m == "graceful"]
        auto = [m for m in pool if m in AUTO_RECOVER_METHODS] or ["host_reboot"]
        for node_id in graceful_nodes[1:]:
            methods[node_id] = random.choice(auto)
        return methods

    def _restart_node_blocking(self, node_id):
        """Issue `sn restart` for a node the CP won't auto-recover (graceful /
        forced), retrying with backoff while a peer is still recovering (the
        per-cluster guard rejects concurrent restarts)."""
        deadline = time.time() + self.args.restart_timeout
        while True:
            try:
                # Emit a RESTART header with wall-clock time, then the raw
                # sbctl -d stdout below it (no per-line prefix) so the CP
                # trace lines up with a single moment.
                self.logger.log(
                    f"RESTART: {time.strftime('%Y-%m-%d %H:%M:%S')} {node_id}"
                )
                stdout_text = self.sbctl(f"sn restart {node_id}", timeout=300)
                with self.logger.lock:
                    print(stdout_text, flush=True,
                          end="" if stdout_text.endswith("\n") else "\n")
                    with open(self.logger.path, "a", encoding="utf-8") as handle:
                        handle.write(stdout_text)
                        if not stdout_text.endswith("\n"):
                            handle.write("\n")
                return
            except Exception as e:
                if time.time() >= deadline:
                    raise
                self.logger.log(
                    f"Restart of {node_id} failed ({e}), "
                    f"retrying in 15s (peer may still be recovering)")
                time.sleep(15)

    def run_fd_outage(self, scenario, targets):
        """Take every target host down, then bring the cluster back.

        Default (deterministic scenarios): every target is host_reboot'd
        (auto-recover, no sbctl restart). In --combo-only mode each target gets
        a method drawn at random from --combo-methods; methods that leave the
        node in a non-auto-recovering 'shutdown' state (graceful / forced) are
        manually restarted via `sn restart` afterwards.

        The post-outage stabilization (rebalance-wait vs 60s settle) is the
        caller's responsibility — it differs per scenario type."""
        stype = scenario["type"]
        if self.combo_methods is not None:
            methods = self._select_outage_methods(targets)
        else:
            methods = {n: "host_reboot" for n in targets}

        self.logger.log(
            f"FD outage [{stype}]: taking down {len(targets)} host(s): "
            + ", ".join(f"{t[:8]}={methods[t]}" for t in targets)
        )

        # Apply graceful first (its shutdown must land while peers are still
        # online, before any auto-recover method flips one to RESTARTING and
        # trips the concurrent-shutdown guard); the rest follow back-to-back so
        # the hosts go down together.
        ordered = sorted(
            targets, key=lambda n: 0 if methods[n] == "graceful" else 1
        )
        for node_id in ordered:
            self._apply_outage(node_id, methods[node_id])

        # Manually restart nodes the CP won't auto-recover (graceful / forced).
        for node_id in ordered:
            if self._needs_manual_restart(methods[node_id]):
                self._restart_node_blocking(node_id)

        # Make sure the CP actually observed each auto-recover node leaving
        # 'online' before we wait for it to come back — otherwise a stale
        # sn-list poll can report it still online and we return too early.
        for node_id in ordered:
            if methods[node_id] not in AUTO_RECOVER_METHODS:
                continue
            observed = self.wait_node_leaves_online(node_id, timeout=90)
            if not observed:
                self.logger.log(
                    f"WARN: CP never observed {node_id[:8]} offline after "
                    f"{methods[node_id]} within 90s; sn-list may be stale"
                )

        wait_timeout = max(self.args.restart_timeout, self.args.auto_recover_wait)
        self.wait_for_all_online(target_nodes=set(targets), timeout=wait_timeout)

    def run(self):
        self.ensure_prerequisites()
        nodes = self.ensure_expected_nodes()
        self.wait_for_all_online(timeout=self.args.restart_timeout)
        # Wait for the cluster to be ACTIVE with all nodes online before
        # starting iterations. The rebalance / data-migration completion
        # wait is gated on --wait-for-rebalance (default off): when off we
        # only require ACTIVE+online and do not block on rebalancing.
        self.wait_for_cluster_stable(require_no_rebalance=self.args.wait_for_rebalance)
        if self.args.wait_for_rebalance:
            self.wait_for_data_migration_complete("test start")
        mount_root = self.prepare_client()
        # Saved so the churn cycle can mount its newly-created volume back
        # into the same workspace tree.
        self.mount_root = mount_root
        volumes = self.create_volumes(nodes)
        # Stored so the churn cycle can drive per-volume teardown/rebuild
        # without re-creating / re-mounting the underlying soak workspace.
        self.volumes = volumes
        self.connect_and_mount_volumes(volumes, mount_root)

        # Start fio once, at the top of the soak. Unlike the base mixed
        # soak (which stops fio between iterations so rebalancing runs
        # unloaded), this variant keeps fio running across every outage —
        # the realism goal is "outages happen while live IO is in flight".
        # Per-volume churn rebuilds individual fio jobs without ever
        # tearing down the rest.
        self.start_fio(self.volumes)

        # Pin the topology once (informational logging) and discover the
        # failure-domain layout, which drives the outage scenarios. Node UUIDs
        # are stable across the soak even if leader takeover shifts roles.
        self.topology = self.discover_topology()
        self.logger.log(f"Pinned topology: {json.dumps(self.topology, sort_keys=True)}")
        self.failure_domains = self.discover_failure_domains()
        self.logger.log(
            "Failure domains: "
            + json.dumps(
                {fd: [u[:8] for u in us] for fd, us in sorted(self.failure_domains.items())},
                sort_keys=True,
            )
        )
        self._validate_failure_domains()
        self.scenarios = self.build_scenarios(nodes)
        if not self.scenarios:
            raise TestRunError("No outage scenarios built; failure-domain map empty")

        start_at = max(1, self.args.start_at)
        if start_at > len(self.scenarios):
            raise TestRunError(
                f"--start-at {start_at} exceeds scenario count "
                f"{len(self.scenarios)}; nothing to run"
            )

        # Background churn thread: random(churn_min, churn_max) seconds
        # between rebuilds of one randomly-selected volume. Started after
        # start_fio so self.fio_jobs is populated; stopped in the outer
        # main() finally via close().
        self.start_churn_thread()

        # iteration counter is aligned to scenario_idx: when --start-at N is
        # used, the first executed scenario logs as iteration=N so post-hoc
        # grep for "iteration 60" finds the resumed scenario and its prior
        # failure side by side.
        iteration = start_at - 1
        cycle = 0
        while True:
            cycle += 1
            if self.args.cycles and cycle > self.args.cycles:
                self.logger.log(
                    f"Completed {cycle - 1} full cycle(s) of {len(self.scenarios)} "
                    f"scenarios; exiting"
                )
                return

            cycle_scenarios = list(self.scenarios)
            if self.args.shuffle_scenarios:
                # Seed off cycle number so two soaks with the same --cycles
                # walk identical sequences, but successive cycles rotate
                # through different orderings.
                random.Random(cycle).shuffle(cycle_scenarios)

            cycle_start_at = start_at if cycle == 1 else 1
            self.logger.log(
                f"Starting cycle {cycle} ({len(cycle_scenarios)} scenarios"
                f"{', shuffled' if self.args.shuffle_scenarios else ''}"
                f"{f', starting at scenario {cycle_start_at}' if cycle_start_at > 1 else ''})"
            )

            for scenario_idx, scenario in enumerate(cycle_scenarios, 1):
                if scenario_idx < cycle_start_at:
                    continue
                iteration += 1
                # Surface any churn-thread fault before doing anything that
                # might mask it (a churn-broken volume's fio rc_file would
                # otherwise be dumped under the wrong context here).
                self.reraise_churn_error()
                # No pre-iteration waiting for rebalance / data migration:
                # the post-iteration grace below (wait_for_all_online +
                # 60 s pause) is the only inter-iteration quiet window.
                # Background reconciliation continues across iterations.

                # Safety: all data NICs must be up during an outage iteration.
                # NIC chaos runs only in the quiet window between iterations.
                self._ensure_all_data_nics_up()

                targets = self._select_outage_targets(scenario)
                if scenario["type"] == "same_fd":
                    scen_desc = f"same_fd(fd={scenario['fd']})"
                else:
                    scen_desc = scenario["type"]  # one_per_fd / fd_plus_one

                self.logger.log(
                    f"Starting outage iteration {iteration} "
                    f"(cycle {cycle} scenario {scenario_idx}/{len(cycle_scenarios)}): "
                    f"type={scen_desc} "
                    f"reboot_targets={[t[:8] for t in targets]}"
                )

                if not targets:
                    self.logger.log(
                        f"Scenario {iteration} skipped: no targets resolved for {scen_desc}"
                    )
                    continue

                # Skip scenarios whose nodes are not currently in the
                # expected-node set (e.g. one has been removed from the
                # cluster mid-soak). Better to log-and-skip than to try to
                # reboot a ghost.
                current_uuids = {n["uuid"] for n in self.ensure_expected_nodes()}
                missing = [uid for uid in targets if uid not in current_uuids]
                if missing:
                    self.logger.log(
                        f"Scenario {iteration} skipped: nodes {missing} not in "
                        f"current cluster set {sorted(current_uuids)}"
                    )
                    continue

                # Serialize the outage AND the inter-iteration NIC chaos
                # with the churn thread: while we hold the lock, no churn
                # cycle can start (and any in-progress churn finishes
                # first). Churn does lvol delete/create + nvme
                # disconnect/connect, which must not overlap a NIC-down
                # window — losing a data path mid-disconnect/connect
                # masks failures, races the kernel's path-failover, and
                # can leave the client in a partially-attached state.
                with self.serial_lock:
                    # Final readiness gate: the previous iteration's settle may
                    # have ended while the CP was autonomously restarting a
                    # node. The count-only ensure_expected_nodes check above
                    # does not catch a node in in_restart/in_shutdown; wait it
                    # out so we start from a fully-online cluster.
                    self.wait_for_outage_ready()
                    self.run_fd_outage(scenario, targets)
                    # Post-outage fio check: any fio that exited during this
                    # iteration is a fault. fio is NOT stopped — it keeps
                    # running into the next iteration and across churn cycles.
                    self.check_fio()
                    # Confirm all rebooted hosts are back online, then run NIC
                    # chaos under the same lock (its nvme-path perturbation must
                    # not overlap a churn disconnect/connect). Tolerate a peer
                    # the CP is autonomously restarting rather than aborting.
                    self.wait_for_all_online(timeout=self.args.restart_timeout,
                                             tolerate_transient=True)
                    self._inter_iteration_nic_chaos()

                # Re-check for any churn fault that may have fired
                # before we acquired the lock; exit at the next sync point.
                self.reraise_churn_error()

                # Scenario-specific stabilization:
                #   (a) same_fd / fd_plus_one — a whole failure domain (plus, for
                #       fd_plus_one, one extra node) was rebooted, so the cluster
                #       must re-replicate/rebalance the data that lived there.
                #       Gate on cluster ACTIVE + rebalance / data-migration
                #       completion before the next iteration (bounded by
                #       --rebalance-timeout), regardless of --wait-for-rebalance.
                #   (b) one_per_fd — only one node per domain bounced; the other
                #       copy in each domain stayed up, so no full rebalance is
                #       expected. A fixed 60s settle is enough.
                if scenario["type"] in ("same_fd", "fd_plus_one"):
                    self.logger.log(
                        f"{scen_desc} outage: waiting for cluster rebalance to "
                        f"complete (iteration {iteration})"
                    )
                    self.wait_for_cluster_stable(require_no_rebalance=True)
                    self.wait_for_data_migration_complete(
                        f"iteration {iteration} {scenario['type']} rebalance"
                    )
                else:
                    self.logger.log(
                        f"one_per_fd outage: settling 60s (iteration {iteration})"
                    )
                    time.sleep(60)


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
