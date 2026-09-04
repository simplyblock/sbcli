#!/usr/bin/env python3
"""
aws_dual_node_outage_soak_multipath.py — multipath soak for an AWS FTT2 cluster.

WHAT THIS TESTS
===============

Steady state: one NVMe-oF volume per storage node, all connected to a single
client over every advertised path, each formatted (xfs) and mounted, each
carrying an independent fio job — read/write mix, iodepth 128, 4 jobs in
parallel, 100 GB working set, 10 hours.

On top of that, each iteration runs two scenarios in sequence:

  Phase 1 — all-node single-NIC outage.
      ONE data NIC (eth1 or eth2, alternating) is taken down on EVERY storage
      node simultaneously, held 30 s, then restored by a timer on each host.
      30 s settle, then verify.

      PASS CRITERION: fio must not be interrupted at all. Every node keeps
      its surviving data NIC, so client IO, remote-device IO and hublvol IO
      all have a live path throughout. Any fio fault here — including a
      max_latency violation — fails the run, because a single-NIC outage on
      a multipath cluster must be transparent.

  Phase 2 — overlapping dual-node outage pair.
      Two nodes are taken out with two independently chosen methods. The
      second outage is initiated at a random 1-60 s offset from the first, so
      the two outage windows overlap. Each node is held down for 30 s before
      its recovery is initiated (an explicit ``sbctl sn restart`` for the
      manual-shutdown methods; a host-side timer or the node's own
      supervisor/boot for the rest).

      Methods combined per pair: network_outage (all data NICs down 30 s),
      shutdown, container_kill, host_reboot.

      IO interruption is a finding here too, but latency spikes are expected
      during leadership promotion, so max_latency violations are counted and
      reported rather than fatal. Hard IO errors and verify failures are
      always fatal.

      Between pairs the soak waits a fixed 90 s (--iteration-settle) and does
      NOT wait for rebalancing or migration to drain. After a two-node outage
      an FTT2 cluster re-replicates for a long time; blocking on that would
      fit only a handful of pairs into a 10-hour run, and outages landing
      while recovery is still in flight is realistic load, not a confound.
      The gate before each pair is "every node online", not "cluster quiesced".

The pair is chosen so the two nodes are never the primary and secondary of
the same lvstore — tearing down both ends of one LVS's path pair is not an
allowed multipath failure scenario. The role topology is read live from the
control plane (``secondary_node_id`` / ``tertiary_node_id`` back-refs), not
from the metadata file, which the multipath deployer does not populate with
topology.

WHAT IT VERIFIES BEYOND fio
===========================

After every phase, on every online node, inside the SPDK container:

  * every remote NVMe controller is ``enabled`` with the expected path count
    (2 for remote device/JM controllers — one per data NIC; 2 or 4 for
    hublvol controllers, which additionally carry the failover node's paths)
  * hublvol bdevs report ``mp_policy=active_active`` — active/active across
    the leader's two NICs, with the failover node held passive by ANA. A
    hublvol found at ``active_passive`` means the control plane failed to
    assert the policy and one NIC is carrying all hub IO.
  * every non-discovery NVMf subsystem has 2 listeners (one per data NIC)

Usage (from the mgmt node, which is the only host with a route to the
data-plane-isolated storage nodes and client):

    python3 aws_dual_node_outage_soak_multipath.py --run-on-mgmt \
        --metadata cluster_metadata_mp.json
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
from dataclasses import dataclass, field
from pathlib import Path

try:
    import paramiko
except ImportError:
    paramiko = None


UUID_RE = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")

#: Outage methods selectable per pair member. The first four are the default
#: set; the rest are available for targeted runs.
OUTAGE_METHODS = (
    "network_outage",     # all data NICs down for --outage-hold, host timer restores
    "shutdown",           # sbctl sn shutdown, then sbctl sn restart after the hold
    "container_kill",      # docker kill the SPDK container; supervisor brings it back
    "host_reboot",         # reboot -f; node returns on its own
    "forced_shutdown",    # sbctl sn shutdown --force, then restart after the hold
    "mgmt_nic_outage",    # mgmt NIC only; data plane stays up
    "all_nics_outage",    # mgmt + data NICs
)
DEFAULT_METHODS = (
    "network_outage", "shutdown", "container_kill", "host_reboot",
)

#: Recovery is initiated by us, with an explicit restart, after the hold.
MANUAL_RECOVERY_METHODS = frozenset({"shutdown", "forced_shutdown"})
#: The outage is undone by a timer running on the node itself, so the hold
#: duration is baked into the command we fire and there is nothing to issue.
HOST_TIMER_METHODS = frozenset({
    "network_outage", "mgmt_nic_outage", "all_nics_outage",
})
#: The node comes back by itself (container supervisor, or boot).
SELF_RECOVER_METHODS = frozenset({"container_kill", "host_reboot"})

#: fio stderr markers that mean "IO was interrupted" — always fatal.
FIO_HARD_ERROR_MARKERS = (
    "fio: io_u error",
    "io_u error on file",
    "verify failed",
    "fio: verify",
    "fio: error",
    "fio: pid=",
    "Killed",
    "Terminated",
    # fio prints verify failures WITHOUT the "fio: " prefix -- the line reads
    # "verify: bad magic header a8a4, wanted acca at file ...". Neither
    # "fio: verify" nor "verify failed" matches that, so run 20260825_155730
    # corrupted vol6 at 18:53 and vol2 at 19:43 and the soak kept applying
    # outages for another two hours, reporting PASS each time, until vol4's
    # fio *process* died at 20:49 and the rc-file branch finally caught it.
    # A data verification failure is the single most important thing this
    # harness can find; it must never again depend on fio also crashing.
    "verify: bad",
    "bad magic header",
    "bad header offset",
    "verify: got",
)
#: fio stderr markers for a --max_latency violation. Fatal in phase 1 (a
#: single-NIC outage must be transparent), counted in phase 2 (promotion
#: windows legitimately stall IO for seconds).
FIO_LATENCY_MARKERS = (
    "fio: latency of",
)


def parse_size_to_bytes(text):
    """Parse ``100G`` / ``4096M`` / ``1T`` / a bare byte count into bytes."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)i?[Bb]?\s*", str(text))
    if not match:
        raise argparse.ArgumentTypeError(f"Cannot parse size {text!r}")
    value = float(match.group(1))
    shift = {"": 0, "K": 10, "M": 20, "G": 30, "T": 40, "P": 50}[match.group(2).upper()]
    return int(value * (1 << shift))


def parse_args():
    default_metadata = Path(__file__).with_name("cluster_metadata_mp.json")
    default_log_dir = Path(__file__).parent

    parser = argparse.ArgumentParser(
        description=(
            "Multipath soak: one fio-loaded volume per storage node, cycling "
            "an all-node single-NIC outage followed by an overlapping "
            "dual-node outage pair with mixed outage methods."
        )
    )
    parser.add_argument("--metadata", default=str(default_metadata),
                        help="Path to cluster metadata JSON (deployer writes cluster_metadata_mp.json).")
    parser.add_argument("--pool", default="pool01", help="Pool name for volume creation.")
    parser.add_argument("--expected-node-count", type=int, default=6,
                        help="Required storage node count.")
    parser.add_argument("--volume-size", default="120G",
                        help="Volume size per storage node. Must exceed the fio working set.")
    parser.add_argument("--run-on-mgmt", action="store_true",
                        help="Run management-node commands locally instead of over SSH. "
                             "Required for a multipath cluster, whose nodes and client "
                             "have no public IPs.")
    parser.add_argument("--ssh-key", default="",
                        help="Optional SSH private key path override.")
    parser.add_argument("--log-file",
                        default=str(default_log_dir / f"soak_mp_{time.strftime('%Y%m%d_%H%M%S')}.log"),
                        help="Single log file for script and CLI output.")

    fio = parser.add_argument_group("fio workload")
    fio.add_argument("--runtime", type=int, default=36000,
                     help="fio runtime in seconds (default 36000 = 10 hours).")
    fio.add_argument("--fio-total-size", default="100G",
                     help="Working set per volume, split across the jobs (default 100G).")
    fio.add_argument("--fio-numjobs", type=int, default=4,
                     help="Parallel fio jobs per volume (default 4).")
    fio.add_argument("--fio-iodepth", type=int, default=128,
                     help="Queue depth per job (default 128).")
    fio.add_argument("--fio-bs", default="4K", help="Block size (default 4K).")
    fio.add_argument("--fio-rw", default="randrw", help="fio rw mode (default randrw).")
    fio.add_argument("--fio-ioengine", default="libaio", help="fio ioengine (default libaio).")
    fio.add_argument("--fio-max-latency", type=int, default=20,
                     help="--max_latency in seconds; 0 omits the flag (default 20).")
    fio.add_argument("--fio-verify", default="crc32c",
                     help="fio verify algorithm; empty string disables verification.")
    fio.add_argument("--fio-completion-grace", type=int, default=180,
                     help="Seconds before the projected fio end time at which a clean "
                          "rc=0 exit counts as completion rather than a mid-run fault.")

    nic = parser.add_argument_group("phase 1: all-node single-NIC outage")
    nic.add_argument("--data-nics", default="eth1,eth2",
                     help="Data NIC names on storage nodes (default eth1,eth2).")
    nic.add_argument("--mgmt-nic", default="eth0",
                     help="Management NIC name on storage nodes (default eth0).")
    nic.add_argument("--nic-phase-hold", type=int, default=30,
                     help="Seconds the NIC stays down on all nodes (default 30).")
    nic.add_argument("--nic-phase-settle", type=int, default=30,
                     help="Seconds to wait after NIC restore before verifying (default 30).")
    nic.add_argument("--no-nic-phase", action="store_true",
                     help="Disable phase 1 (the all-nodes single-NIC outage) "
                          "entirely. REQUIRED on a single-data-NIC, non-multipath "
                          "cluster: with one data NIC per node, phase 1 takes that "
                          "NIC down on every node at once and isolates the whole "
                          "cluster instead of exercising path redundancy. Note that "
                          "--nic-phase-every 0 does NOT disable phase 1 -- it means "
                          "'once, on iteration 1'.")
    nic.add_argument("--nic-phase-every", type=int, default=1,
                     help="Run the NIC phase every N iterations. 0 = once, before the "
                          "first pair only (default 1).")

    pair = parser.add_argument_group("phase 2: overlapping dual-node outage pair")
    pair.add_argument("--methods", default=",".join(DEFAULT_METHODS),
                      help=f"Comma-separated methods to pick from. Choices: {','.join(OUTAGE_METHODS)}")
    pair.add_argument("--outage-hold", type=int, default=30,
                      help="Seconds a node stays down before its recovery is initiated (default 30).")
    pair.add_argument("--pair-delay-min", type=int, default=1,
                      help="Minimum offset between the two outages (default 1).")
    pair.add_argument("--pair-delay-max", type=int, default=60,
                      help="Maximum offset between the two outages (default 60).")
    pair.add_argument("--force-overlap", action="store_true",
                      help="Clamp the offset below --outage-hold so the two outage "
                           "windows always overlap. Without this, an offset above the "
                           "hold produces back-to-back rather than overlapping outages "
                           "(logged either way).")
    pair.add_argument("--exclude-primary-secondary", action="store_true",
                      help="Exclude pairs that are the primary and secondary of one "
                           "lvstore. In the standard FTT2 rotation a node's secondary "
                           "is its ring neighbour, so this also excludes every "
                           "ring-distance-1 ('subsequent nodes') pair. Off by default: "
                           "an FTT2 cluster is meant to survive any two-node loss, and "
                           "P+S is the pair most likely to expose a cascade.")
    pair.add_argument("--forbid-any-shared-lvs", action="store_true",
                      help="Exclude pairs sharing ANY role of one lvstore (P+S, P+T, "
                           "S+T). Implies --exclude-primary-secondary and leaves only "
                           "pairs with no lvstore in common.")

    waits = parser.add_argument_group("timeouts")
    waits.add_argument("--restart-timeout", type=int, default=900,
                       help="Seconds to wait for restarted nodes.")
    waits.add_argument("--auto-recover-wait", type=int, default=900,
                       help="Seconds to wait for a self-recovering node (container "
                            "supervisor / host boot).")
    waits.add_argument("--rebalance-timeout", type=int, default=7200,
                       help="Seconds to wait for rebalancing / migration to finish.")
    waits.add_argument("--poll-interval", type=int, default=10,
                       help="Poll interval for health checks.")
    waits.add_argument("--survivor-down-grace", type=int, default=120,
                       help="Seconds a node outside the outage set may report non-online "
                            "before it is treated as collateral damage. Survivors "
                            "transiently flap DOWN for 10-30 s and self-heal.")
    waits.add_argument("--iteration-settle", type=int, default=90,
                       help="Fixed wait between outage pairs, in seconds (default 90). "
                            "The soak deliberately does NOT wait for rebalancing or "
                            "migration to drain between pairs — on a loaded cluster "
                            "that never fully settles and the soak would starve. "
                            "Nodes are still required to be back online first.")
    waits.add_argument("--wait-for-migration", action="store_true",
                       help="Additionally block on data-migration tasks draining "
                            "before each iteration. Off by default, for the reason "
                            "above.")

    verify = parser.add_argument_group("SPDK verification")
    verify.add_argument("--skip-spdk-verify", action="store_true",
                        help="Skip the in-container SPDK path/policy verification.")
    verify.add_argument("--path-heal-timeout", type=int, default=900,
                        help="Seconds to wait for ALL SPDK paths / policies / "
                             "listeners to heal after an outage before the next "
                             "iteration may start (default 900). Redundant-path "
                             "re-add runs on the health-check/reconcile cadence "
                             "and legitimately takes minutes; the wait is a gate, "
                             "not a check — only exceeding the timeout fails the "
                             "run. Healing time is logged per phase as a "
                             "measurement.")
    verify.add_argument("--path-heal-poll", type=int, default=30,
                        help="Seconds between heal-gate polls (default 30).")
    verify.add_argument("--policy-sample", type=int, default=2,
                        help="Remote device bdevs per node to sample for multipath "
                             "policy. Hublvol bdevs are always all checked. Keep small: "
                             "unfiltered bdev dumps have wedged app threads before.")
    verify.add_argument("--placement-dumps", action="store_true",
                        help="Take a placement-map dump (RPC "
                             "distr_debug_placement_map_dump) from every "
                             "distrib on every reachable node IMMEDIATELY "
                             "before and after each outage, gzip it, and store "
                             "it on the node under ~/placement_dumps/<run>/. "
                             "Off by default: a dump pair per outage phase "
                             "adds RPC load and disk, so it is opt-in for "
                             "placement investigations.")
    verify.add_argument("--start-iteration", type=int, default=1,
                        help="Number the first outage pair with this instead of "
                             "1. Pair distance rotation and NIC-phase "
                             "scheduling both key off the iteration number, so "
                             "resuming an interrupted run at the iteration it "
                             "died on continues the same sequence rather than "
                             "repeating the pairs already covered. The loop "
                             "still ends at --iterations, so "
                             "--start-iteration 21 --iterations 75 runs 55 "
                             "pairs.")
    verify.add_argument("--iterations", type=int, default=75,
                        help="Number of outage pairs to run (default 75). 0 = run "
                             "until fio's runtime expires. --runtime must outlast the "
                             "loop or fio ends the run early with fewer pairs.")

    args = parser.parse_args()

    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    bad = [m for m in methods if m not in OUTAGE_METHODS]
    if bad:
        parser.error(f"Unknown outage method(s): {bad}. Choices: {list(OUTAGE_METHODS)}")
    if not methods:
        parser.error("At least one outage method must be enabled")
    args.methods = methods

    args.data_nics = [n.strip() for n in args.data_nics.split(",") if n.strip()]
    if len(args.data_nics) < 2:
        parser.error("At least 2 data NICs are required for a multipath soak")
    if args.pair_delay_min < 0 or args.pair_delay_max < args.pair_delay_min:
        parser.error("--pair-delay-min/--pair-delay-max must satisfy 0 <= min <= max")

    total = parse_size_to_bytes(args.fio_total_size)
    volume = parse_size_to_bytes(args.volume_size)
    if total >= volume:
        parser.error(
            f"--fio-total-size ({args.fio_total_size}) must be smaller than "
            f"--volume-size ({args.volume_size}); the working set plus "
            f"filesystem overhead has to fit on the volume")
    per_job = total // args.fio_numjobs
    if per_job <= 0:
        parser.error("--fio-total-size is too small for --fio-numjobs")
    # fio gets an explicit per-job size in MiB so numjobs * size == total.
    args.fio_size_per_job = f"{per_job // (1 << 20)}M"
    args.fio_total_bytes = total
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


class TestRunError(RuntimeError):
    pass


class RemoteHost:
    def __init__(self, hostname, user, key_path, logger, name, quiet=False):
        self.hostname = hostname
        self.user = user
        self.key_path = key_path
        self.logger = logger
        self.name = name
        self.quiet = quiet
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
                    f"{self.name}: SSH attempt {attempt}/15 failed to {self.hostname}: {exc}")
                time.sleep(5)
        raise RemoteCommandError(
            f"{self.name}: failed to connect to {self.hostname}: {last_error}")

    def run(self, command, timeout=600, check=True, label=None):
        if paramiko is None:
            return self._run_via_ssh_cli(command, timeout=timeout, check=check, label=label)
        if self.client is None:
            self.connect()
        label = label or command
        if not self.quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        try:
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            self.logger.log(
                f"{self.name}: command transport failure for {label}: {exc}; reconnecting once")
            self.connect()
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        if not self.quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed with rc={rc}: {label}")
        return rc, stdout_text, stderr_text

    def _run_via_ssh_cli(self, command, timeout=600, check=True, label=None):
        label = label or command
        if not self.quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        ssh_cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-i", self.key_path, f"{self.user}@{self.hostname}", command,
        ]
        try:
            completed = subprocess.run(
                ssh_cmd, capture_output=True, text=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        if not self.quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and completed.returncode != 0:
            raise RemoteCommandError(
                f"{self.name}: command failed with rc={completed.returncode}: {label}")
        return completed.returncode, stdout_text, stderr_text

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None


class LocalHost:
    def __init__(self, logger, name, quiet=False):
        self.logger = logger
        self.name = name
        self.quiet = quiet

    def run(self, command, timeout=600, check=True, label=None):
        label = label or command
        if not self.quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        try:
            completed = subprocess.run(
                ["/bin/bash", "-lc", command],
                capture_output=True, text=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        if not self.quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and completed.returncode != 0:
            raise RemoteCommandError(
                f"{self.name}: command failed with rc={completed.returncode}: {label}")
        return completed.returncode, stdout_text, stderr_text

    def close(self):
        return


@dataclass
class FioJob:
    volume_id: str
    volume_name: str
    mount_point: str
    fio_log: str
    fio_stderr: str
    rc_file: str
    pid: int
    #: Count of --max_latency violation lines already reported, so each check
    #: reports only the new ones.
    latency_reported: int = 0


@dataclass
class OutageRecord:
    node_id: str
    method: str
    planned_offset: float
    applied_at: float = 0.0
    recovery_at: float = 0.0
    error: str = ""
    events: list = field(default_factory=list)


class SoakRunner:
    def __init__(self, args, metadata, logger):
        self.args = args
        self.metadata = metadata
        self.logger = logger
        self.user = metadata["user"]
        self.key_path = resolve_key_path(args.ssh_key or metadata["key_path"])
        self.run_id = time.strftime("%Y%m%d_%H%M%S")

        self.mgmt = self._new_mgmt_host("mgmt")

        client_entry = metadata["clients"][0]
        client_addr = (client_entry.get("private_ip") or client_entry.get("public_ip")
                       if args.run_on_mgmt
                       else client_entry.get("public_ip") or client_entry.get("private_ip"))
        if not client_addr:
            raise TestRunError(
                "No reachable client address in metadata. A multipath cluster's "
                "client has no public IP — run this from the mgmt node with "
                "--run-on-mgmt.")
        self.client = RemoteHost(client_addr, self.user, self.key_path, logger, "client")

        self.cluster_id = metadata.get("cluster_uuid") or ""
        self.fio_jobs = []
        self.fio_started_at = 0.0
        self.created_volume_ids = []
        self.methods = list(args.methods)

        self._node_hosts = {}
        self._node_hosts_lock = threading.Lock()
        self.topology = []
        self.node_ip_map = {}
        self._forbidden_pairs = set()
        #: Cumulative count of tolerated (phase-2) max_latency violations.
        self.latency_violations = 0
        #: volume_id -> path count observed at baseline (see _verify_client_paths)
        self._baseline_client_paths = {}

    # ----- hosts / plumbing -------------------------------------------------

    def _new_mgmt_host(self, name, quiet=False):
        """A dedicated mgmt connection. Outage threads each take their own so
        two concurrent sbctl calls never share one paramiko client (a
        reconnect on one would yank the channel out from under the other)."""
        if self.args.run_on_mgmt:
            return LocalHost(self.logger, name, quiet=quiet)
        return RemoteHost(self.metadata["mgmt"]["public_ip"], self.user,
                          self.key_path, self.logger, name, quiet=quiet)

    def close(self):
        for host in [self.client, self.mgmt]:
            try:
                host.close()
            except Exception:
                pass
        with self._node_hosts_lock:
            hosts = list(self._node_hosts.values())
            self._node_hosts.clear()
        for host in hosts:
            try:
                host.close()
            except Exception:
                pass

    def _node_host(self, uuid):
        with self._node_hosts_lock:
            host = self._node_hosts.get(uuid)
            if host is not None:
                return host
        ip = self.node_ip_map.get(uuid)
        if not ip:
            raise TestRunError(f"Cannot resolve storage-node IP for UUID {uuid}")
        host = RemoteHost(ip, self.user, self.key_path, self.logger, f"sn[{ip}]")
        with self._node_hosts_lock:
            existing = self._node_hosts.get(uuid)
            if existing is not None:
                host.close()
                return existing
            self._node_hosts[uuid] = host
            return host

    def _drop_node_host(self, uuid):
        with self._node_hosts_lock:
            host = self._node_hosts.pop(uuid, None)
        if host is not None:
            try:
                host.close()
            except Exception:
                pass

    def prewarm_node_hosts(self, uuids):
        """Establish SSH to every node up front. The NIC phase needs its
        fan-out to be near-simultaneous, and a cold paramiko handshake inside
        the fan-out would smear the outage start by seconds."""
        for uuid in uuids:
            try:
                self._node_host(uuid)
            except Exception as exc:
                self.logger.log(f"prewarm: cannot reach {uuid[:12]}: {exc}")

    @staticmethod
    def _fan_out(fn, items, label, join_timeout=120):
        """Run ``fn(item)`` for every item in its own thread, started as
        tightly as possible. Returns [(item, exception)] for failures."""
        errors = []
        errors_lock = threading.Lock()

        def _wrap(item):
            try:
                fn(item)
            except Exception as exc:
                with errors_lock:
                    errors.append((item, exc))

        threads = [
            threading.Thread(target=_wrap, args=(item,), daemon=True,
                             name=f"{label}-{str(item)[:12]}")
            for item in items
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=join_timeout)
        return errors

    # ----- sbctl ------------------------------------------------------------

    def sbctl(self, args, timeout=600, json_output=False, host=None):
        host = host or self.mgmt
        command = "sudo /usr/local/bin/sbctl -d " + args
        _, stdout_text, stderr_text = host.run(
            command, timeout=timeout, check=True, label=f"sbctl {args}")
        if not json_output:
            return stdout_text
        return self._parse_json(stdout_text, stderr_text, f"sbctl {args}")

    def sbctl_allow_failure(self, args, timeout=600, host=None):
        host = host or self.mgmt
        command = "sudo /usr/local/bin/sbctl -d " + args
        return host.run(command, timeout=timeout, check=False, label=f"sbctl {args}")

    @staticmethod
    def _parse_json(stdout_text, stderr_text, what):
        for candidate in (stdout_text, stderr_text, stdout_text + "\n" + stderr_text):
            candidate = candidate.strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
            decoder = json.JSONDecoder()
            final, lists, dicts = [], [], []
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
                    final.append(obj)
                elif isinstance(obj, list):
                    lists.append(obj)
                else:
                    dicts.append(obj)
            for bucket in (final, lists, dicts):
                if bucket:
                    return bucket[-1]
        raise TestRunError(f"Failed to parse JSON from {what}")

    def mgmt_python(self, script, label, timeout=120, host=None):
        host = host or self.mgmt
        _, stdout_text, stderr_text = host.run(
            f"sudo python3 -c {shlex.quote(script)}", timeout=timeout,
            check=True, label=label)
        for line in reversed((stdout_text or "").splitlines()):
            line = line.strip()
            if line.startswith("[") or line.startswith("{"):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        raise TestRunError(f"{label}: no JSON payload in output:\n{stdout_text}\n{stderr_text}")

    # ----- topology ---------------------------------------------------------

    def get_cluster_id(self):
        if self.cluster_id:
            return self.cluster_id
        clusters = self.sbctl("cluster list --json", json_output=True)
        if not clusters:
            raise TestRunError("No clusters returned by sbctl cluster list")
        self.cluster_id = clusters[0]["UUID"]
        return self.cluster_id

    def refresh_topology(self):
        """Read node roles and mgmt IPs straight from the control-plane DB.

        The multipath deployer does not write a ``topology`` key into the
        metadata file, so the previous metadata-driven role lookup silently
        produced an empty forbidden-pair set and the soak would happily take
        down both ends of one lvstore's path pair. Roles come from the
        authoritative back-refs: a node's own ``lvstore`` makes it that LVS's
        primary, and its ``secondary_node_id`` / ``tertiary_node_id`` name the
        other two role holders.
        """
        cluster_id = self.get_cluster_id()
        script = (
            "import json\n"
            "from simplyblock_core import db_controller\n"
            "db = db_controller.DBController()\n"
            f"nodes = db.get_storage_nodes_by_cluster_id({cluster_id!r})\n"
            "out = []\n"
            "for n in nodes:\n"
            "    out.append({\n"
            "        'uuid': n.get_id(),\n"
            "        'status': n.status,\n"
            "        'hostname': getattr(n, 'hostname', ''),\n"
            "        'mgmt_ip': n.mgmt_ip,\n"
            "        'lvstore': n.lvstore or '',\n"
            "        'secondary': n.secondary_node_id or '',\n"
            "        'tertiary': n.tertiary_node_id or '',\n"
            "    })\n"
            "print(json.dumps(out))\n"
        )
        nodes = self.mgmt_python(script, "read node topology")
        self.topology = nodes
        self.node_ip_map = {n["uuid"]: n["mgmt_ip"] for n in nodes if n.get("mgmt_ip")}
        self._forbidden_pairs = self._build_forbidden_pairs(nodes)
        self.logger.log(
            f"Topology: {len(nodes)} nodes, "
            f"{len(self._forbidden_pairs)} forbidden pair(s) "
            f"({'any shared LVS' if self.args.forbid_any_shared_lvs else 'primary+secondary'})")
        for node in nodes:
            if node["lvstore"]:
                self.logger.log(
                    f"  {node['uuid'][:12]} primary of {node['lvstore']}"
                    f" sec={node['secondary'][:12] or '-'}"
                    f" tert={node['tertiary'][:12] or '-'}")
        if not self._forbidden_pairs:
            if (self.args.exclude_primary_secondary
                    or self.args.forbid_any_shared_lvs):
                self.logger.log(
                    "WARNING: role-pair exclusion was requested but no forbidden "
                    "pairs were derived — check that nodes report an lvstore, or "
                    "the exclusion is silently doing nothing.")
            else:
                self.logger.log(
                    "Every node pair is eligible (no role-pair exclusion): an "
                    "FTT2 cluster must survive any two-node loss. Pass "
                    "--exclude-primary-secondary to skip both ends of one "
                    "lvstore's path pair.")
        return nodes

    def _build_forbidden_pairs(self, nodes):
        """Pairs that must never be outaged together.

        Empty by default: an FTT2 (2+2) cluster is meant to survive the loss
        of any two nodes, so every pair is a legitimate scenario and the
        role-adjacent ones are exactly where cascades have shown up.

        --exclude-primary-secondary restores the old behaviour of skipping
        both ends of one lvstore's path pair. Note that in the standard
        rotation a node's secondary is its ring neighbour, so that also
        removes every ring-distance-1 pair. --forbid-any-shared-lvs is the
        strictest setting: no two role holders of one lvstore together.
        """
        exclude_ps = self.args.exclude_primary_secondary or self.args.forbid_any_shared_lvs
        forbidden = set()
        if not exclude_ps:
            return forbidden
        for node in nodes:
            if not node["lvstore"]:
                continue
            primary = node["uuid"]
            secondary = node["secondary"]
            tertiary = node["tertiary"]
            if secondary and secondary != primary:
                forbidden.add(frozenset((primary, secondary)))
            if self.args.forbid_any_shared_lvs:
                if tertiary and tertiary != primary:
                    forbidden.add(frozenset((primary, tertiary)))
                if secondary and tertiary and secondary != tertiary:
                    forbidden.add(frozenset((secondary, tertiary)))
        return forbidden

    def _ring_order(self):
        """Node UUIDs in ring order, following the secondary back-refs.

        The FTT2 rotation wires each node's secondary to its ring successor,
        so following ``secondary`` from any node walks the ring. Ring position
        is what makes "subsequent nodes" versus "one or two nodes in between"
        a meaningful distinction for pair selection.
        """
        successor = {
            n["uuid"]: n["secondary"] for n in self.topology
            if n["lvstore"] and n["secondary"]
        }
        all_uuids = [n["uuid"] for n in self.topology]
        if len(successor) == len(all_uuids):
            start = all_uuids[0]
            order = [start]
            current = successor.get(start)
            while current and current != start and len(order) < len(all_uuids):
                order.append(current)
                current = successor.get(current)
            if len(order) == len(all_uuids) and current == start:
                return order
        self.logger.log(
            "Topology does not form a single secondary-ring; falling back to "
            "metadata order for ring-distance computation")
        return all_uuids

    def _pairs_by_distance(self, uuids):
        """{ring_distance: [(a, b), ...]} over eligible pairs.

        Distance 1 is adjacent ("subsequent") nodes, 2 has one node in
        between, 3 has two, and so on up to n//2 (the far side of the ring).
        """
        order = [u for u in self._ring_order() if u in set(uuids)]
        count = len(order)
        position = {uuid: index for index, uuid in enumerate(order)}
        buckets = {}
        for index, node_a in enumerate(order):
            for node_b in order[index + 1:]:
                raw = abs(position[node_a] - position[node_b])
                distance = min(raw, count - raw)
                if self._is_forbidden_pair(node_a, node_b):
                    continue
                buckets.setdefault(distance, []).append((node_a, node_b))
        return buckets

    def _is_forbidden_pair(self, uuid_a, uuid_b):
        return frozenset((uuid_a, uuid_b)) in self._forbidden_pairs

    def _describe_pair(self, uuid_a, uuid_b):
        """Human-readable role relationship, for the iteration log line."""
        relations = []
        for node in self.topology:
            lvs = node["lvstore"]
            if not lvs:
                continue
            roles = {node["uuid"]: "primary"}
            if node["secondary"]:
                roles[node["secondary"]] = "secondary"
            if node["tertiary"]:
                roles[node["tertiary"]] = "tertiary"
            if uuid_a in roles and uuid_b in roles:
                relations.append(f"{lvs}:{roles[uuid_a]}+{roles[uuid_b]}")
        return ",".join(relations) if relations else "no shared lvstore"

    def get_nodes(self):
        nodes = self.sbctl("sn list --json", json_output=True)
        return [
            {
                "uuid": node["UUID"],
                "status": str(node.get("Status", "")).lower(),
                "mgmt_ip": node.get("Management IP") or "",
                "hostname": node.get("Hostname") or "",
            }
            for node in nodes
        ]

    def ensure_expected_nodes(self):
        nodes = self.get_nodes()
        if len(nodes) != self.args.expected_node_count:
            raise TestRunError(
                f"Expected {self.args.expected_node_count} storage nodes, found "
                f"{len(nodes)}. Update metadata or pass --expected-node-count.")
        return nodes

    def assert_cluster_not_suspended(self):
        clusters = self.sbctl("cluster list --json", json_output=True)
        if not clusters:
            raise TestRunError("Cluster list returned no rows")
        status = str(clusters[0].get("Status", "")).lower()
        if status == "suspended":
            raise TestRunError(
                "Cluster is SUSPENDED — a two-node outage must not suspend the "
                "cluster. Stopping so the state is preserved for log collection.")
        return status

    # ----- health waits -----------------------------------------------------

    def wait_for_all_online(self, target_nodes=None, timeout=None):
        """Wait for every node online.

        Nodes outside ``target_nodes`` are survivors. A survivor that reports
        non-online is only fatal once it has stayed that way for
        --survivor-down-grace: survivors legitimately flap DOWN for 10-30 s
        during a peer's outage and self-heal, and failing instantly on that
        aborted earlier runs for no reason.
        """
        timeout = timeout or self.args.restart_timeout
        target_nodes = set(target_nodes or ())
        first_bad = {}
        started = time.time()
        while time.time() - started < timeout:
            self.assert_cluster_not_suspended()
            nodes = self.ensure_expected_nodes()
            statuses = {n["uuid"]: n["status"] for n in nodes}
            now = time.time()

            for uuid, status in statuses.items():
                if uuid in target_nodes:
                    continue
                if status != "online":
                    first_bad.setdefault(uuid, now)
                else:
                    first_bad.pop(uuid, None)

            overdue = {
                uuid: now - since for uuid, since in first_bad.items()
                if now - since > self.args.survivor_down_grace
            }
            if overdue:
                raise TestRunError(
                    "Survivor node(s) went and stayed non-online, which is "
                    "collateral damage from the outage: "
                    + ", ".join(
                        f"{uuid[:12]}:{statuses.get(uuid)} for {age:.0f}s"
                        for uuid, age in overdue.items()))
            if first_bad:
                self.logger.log(
                    "Survivor(s) transiently non-online (within grace): "
                    + ", ".join(
                        f"{uuid[:12]}:{statuses.get(uuid)} {now - since:.0f}s"
                        for uuid, since in first_bad.items()))

            offline = [uuid for uuid, status in statuses.items() if status != "online"]
            if not offline:
                return nodes
            self.logger.log(
                "Waiting for all nodes online: "
                + ", ".join(f"{uuid[:12]}:{status}" for uuid, status in statuses.items()))
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for nodes to return online")

    def wait_for_cluster_stable(self):
        cluster_id = self.get_cluster_id()
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            status = self.assert_cluster_not_suspended()
            cluster_info = self.sbctl(f"cluster get {cluster_id}", json_output=True)
            rebalancing = bool(cluster_info.get("is_re_balancing", False))
            nodes = self.ensure_expected_nodes()
            node_statuses = {n["uuid"]: n["status"] for n in nodes}
            if (status == "active" and not rebalancing
                    and all(s == "online" for s in node_statuses.values())):
                self.logger.log("Cluster stable: ACTIVE, all online, not rebalancing")
                return
            self.logger.log(
                f"Waiting for cluster stability: status={status}, "
                f"rebalancing={rebalancing}, "
                + ", ".join(f"{u[:12]}:{s}" for u, s in node_statuses.items()))
            time.sleep(self.args.poll_interval)
        raise TestRunError("Timed out waiting for cluster to stabilise")

    def get_active_tasks(self):
        cluster_id = self.get_cluster_id()
        script = (
            "import json\n"
            "from simplyblock_core import db_controller\n"
            "from simplyblock_core.models.job_schedule import JobSchedule\n"
            "db = db_controller.DBController()\n"
            f"tasks = db.get_job_tasks({cluster_id!r}, reverse=False)\n"
            "out = [t.get_clean_dict() for t in tasks "
            "if t.status != JobSchedule.STATUS_DONE and not getattr(t, 'canceled', False)]\n"
            "print(json.dumps(out))\n"
        )
        return self.mgmt_python(script, "list active tasks")

    @staticmethod
    def _is_data_migration_task(task):
        haystack = " ".join([
            str(task.get("function_name", "")),
            str(task.get("task_name", "")),
            str(task.get("task_type", "")),
        ]).lower()
        return any(marker in haystack for marker in ("migration", "rebalanc", "sync"))

    def wait_for_data_migration_complete(self, reason):
        started = time.time()
        while time.time() - started < self.args.rebalance_timeout:
            self.assert_cluster_not_suspended()
            migrating = [t for t in self.get_active_tasks() if self._is_data_migration_task(t)]
            if not migrating:
                return
            self.logger.log(
                f"Waiting before {reason}; data migration tasks: "
                + ", ".join(
                    f"{t.get('function_name')}:{t.get('status')}:"
                    f"{t.get('node_id') or t.get('device_id')}" for t in migrating))
            time.sleep(self.args.poll_interval)
        raise TestRunError(f"Timed out waiting for data migration before {reason}")

    # ----- client / volumes -------------------------------------------------

    def ensure_prerequisites(self):
        self.logger.log(f"Using SSH key {self.key_path}")
        self.client.run(
            "if command -v dnf >/dev/null 2>&1; then "
            "sudo dnf install -y nvme-cli fio xfsprogs; "
            "else sudo apt-get update && sudo apt-get install -y nvme-cli fio xfsprogs; fi",
            timeout=1800, label="install client packages")
        self.client.run("sudo modprobe nvme_tcp", timeout=60, label="load nvme_tcp")
        # Native NVMe multipath is what makes the client's two paths one
        # namespace. Without it each path shows up as its own device and the
        # whole client-side premise of this soak is void.
        _, mp, _ = self.client.run(
            "cat /sys/module/nvme_core/parameters/multipath", timeout=30,
            check=False, label="check nvme_core multipath")
        if mp.strip() != "Y":
            raise TestRunError(
                f"Client has nvme_core.multipath={mp.strip()!r}; native NVMe "
                f"multipath must be enabled (Y) or the client sees one device "
                f"per path instead of one multipath namespace.")
        _, iopolicy, _ = self.client.run(
            "cat /sys/module/nvme_core/parameters/iopolicy 2>/dev/null || echo unknown",
            timeout=30, check=False, label="check nvme_core iopolicy")
        self.logger.log(
            f"Client nvme_core: multipath=Y iopolicy={iopolicy.strip()} "
            f"(iopolicy 'numa' pins one optimized path per node; the surviving "
            f"path is still used on failure, which is what phase 1 tests)")

    def prepare_client(self):
        mount_root = posixpath.join("/home", self.user, f"soak_mp_{self.run_id}")
        command = (
            "sudo pkill -f '[f]io --name=soak_mp_' || true\n"
            f"sudo mkdir -p {shlex.quote(mount_root)}\n"
            f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} {shlex.quote(mount_root)}\n"
        )
        self.client.run(f"bash -lc {shlex.quote(command)}", timeout=120,
                        label="prepare client workspace")
        return mount_root

    def extract_uuid(self, text):
        for line in reversed(text.splitlines()):
            stripped = line.strip()
            if UUID_RE.fullmatch(stripped):
                return stripped
        raise TestRunError(f"Failed to extract standalone UUID from output: {text}")

    def create_volumes(self, nodes):
        self.logger.log(
            f"Creating {len(nodes)} volumes of {self.args.volume_size}, one per storage node")
        volumes = []
        for index, node in enumerate(nodes, start=1):
            volume_name = f"soak_mp_{self.run_id}_v{index}"
            volume_id = None
            started = time.time()
            while time.time() - started < self.args.rebalance_timeout:
                self.wait_for_cluster_stable()
                output = self.sbctl(
                    f"lvol add {volume_name} {self.args.volume_size} {self.args.pool} "
                    f"--host-id {node['uuid']}")
                if "ERROR:" in output or "LVStore is being recreated" in output:
                    self.logger.log(f"Volume create for {volume_name} deferred: {output.strip()}")
                    time.sleep(self.args.poll_interval)
                    continue
                volume_id = self.extract_uuid(output)
                break
            if volume_id is None:
                raise TestRunError(
                    f"Timed out creating volume {volume_name} on node {node['uuid']}")
            self.created_volume_ids.append(volume_id)
            volumes.append({
                "index": index,
                "volume_name": volume_name,
                "volume_id": volume_id,
                "node_uuid": node["uuid"],
            })
            self.logger.log(f"Created volume {volume_name} ({volume_id}) on {node['uuid'][:12]}")
        return volumes

    def connect_and_mount_volumes(self, volumes, mount_root):
        self.logger.log("Connecting volumes to client and preparing filesystems")
        for volume in volumes:
            connect_output = self.sbctl(f"lvol connect {volume['volume_id']}")
            connect_commands = [
                line.strip() for line in connect_output.splitlines()
                if line.strip().startswith("sudo nvme connect")
            ]
            if not connect_commands:
                raise TestRunError(
                    f"No nvme connect command returned for {volume['volume_id']}")
            connected, failed = 0, []
            for connect_cmd in connect_commands:
                try:
                    self.client.run(connect_cmd, timeout=120,
                                    label=f"connect {volume['volume_id']}")
                    connected += 1
                except RemoteCommandError as exc:
                    failed.append(str(exc))
                    self.logger.log(f"Path connect failed for {volume['volume_id']}: {exc}")
            if connected == 0:
                raise TestRunError(
                    f"No nvme paths connected for {volume['volume_id']}: {'; '.join(failed)}")
            # A multipath volume that came up single-pathed invalidates phase 1
            # before it starts: there is no second path to fail over to.
            if connected < len(connect_commands):
                raise TestRunError(
                    f"Only {connected}/{len(connect_commands)} paths connected for "
                    f"{volume['volume_id']}; a multipath soak needs every path. "
                    f"Failures: {'; '.join(failed)}")
            self.logger.log(
                f"Connected {connected}/{len(connect_commands)} paths for "
                f"{volume['volume_name']}")

            volume["mount_point"] = posixpath.join(mount_root, f"vol{volume['index']}")
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
                "echo \"device: $dev\"\n"
                f"sudo mkfs.xfs -f \"$dev\"\n"
                f"sudo mkdir -p {shlex.quote(volume['mount_point'])}\n"
                f"sudo mount \"$dev\" {shlex.quote(volume['mount_point'])}\n"
                f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} "
                f"{shlex.quote(volume['mount_point'])}\n"
            )
            self.client.run(f"bash -lc {shlex.quote(find_and_mount)}", timeout=900,
                            label=f"format and mount {volume['volume_id']}")

    # ----- fio --------------------------------------------------------------

    def start_fio(self, volumes):
        args = self.args
        self.logger.log(
            f"Starting fio on {len(volumes)} volumes: rw={args.fio_rw} bs={args.fio_bs} "
            f"numjobs={args.fio_numjobs} iodepth={args.fio_iodepth} "
            f"size={args.fio_size_per_job}/job "
            f"({args.fio_total_size} total) runtime={args.runtime}s "
            f"ioengine={args.fio_ioengine}")
        fio_jobs = []
        for volume in volumes:
            fio_name = f"soak_mp_{volume['index']}"
            fio_cmd = (
                f"fio --name={fio_name} "
                f"--directory={shlex.quote(volume['mount_point'])} "
                f"--direct=1 --rw={args.fio_rw} --bs={args.fio_bs} "
                f"--numjobs={args.fio_numjobs} --iodepth={args.fio_iodepth} "
                f"--size={args.fio_size_per_job} "
                f"--ioengine={args.fio_ioengine} "
                f"--time_based --runtime={args.runtime} --group_reporting "
            )
            if args.fio_max_latency > 0:
                fio_cmd += f"--max_latency={args.fio_max_latency}s "
            if args.fio_verify:
                # verify_dump writes <file>.<offset>.received / .expected on a
                # mismatch. Without it a verify failure gives only fio's one
                # line, and every corruption so far has died with the returned
                # bytes unidentified -- we could not tell stale data from
                # parity noise from a neighbouring block, and the volumes are
                # on instance store so they vanish when the fleet is stopped.
                # The dumps are 4 KiB each and only appear on failure.
                fio_cmd += (f"--verify={args.fio_verify} --verify_fatal=1 "
                            f"--verify_backlog=1024 --verify_dump=1 ")
            fio_cmd += f"--output={shlex.quote(volume['fio_log'])}"

            start_script = (
                "set -euo pipefail\n"
                f"rm -f {shlex.quote(volume['rc_file'])} {shlex.quote(volume['fio_stderr'])}\n"
                "nohup bash -lc "
                + shlex.quote(
                    f"cd {shlex.quote(volume['mount_point'])} && {fio_cmd}; "
                    f"rc=$?; echo $rc > {shlex.quote(volume['rc_file'])}")
                + f" >{shlex.quote(volume['fio_stderr'])} 2>&1 & echo $!"
            )
            _, stdout_text, _ = self.client.run(
                f"bash -lc {shlex.quote(start_script)}", timeout=60,
                label=f"start fio {volume['volume_id']}")
            pid = int(stdout_text.strip().splitlines()[-1])
            fio_jobs.append(FioJob(
                volume_id=volume["volume_id"],
                volume_name=volume["volume_name"],
                mount_point=volume["mount_point"],
                fio_log=volume["fio_log"],
                fio_stderr=volume["fio_stderr"],
                rc_file=volume["rc_file"],
                pid=pid,
            ))
            self.logger.log(f"Started fio for {volume['volume_name']} pid {pid}")
        self.fio_jobs = fio_jobs
        self.fio_started_at = time.time()
        time.sleep(10)
        if self.check_fio(strict_latency=True):
            raise TestRunError("fio finished before the outage loop started")
        self.logger.log("fio running on all volumes")

    def _read_rc_file(self, job):
        probe = f"if [ -f {shlex.quote(job.rc_file)} ]; then cat {shlex.quote(job.rc_file)}; fi"
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(probe)}", timeout=30, check=False,
            label=f"rc {job.volume_name}")
        return (stdout_text or "").strip() or None

    def _wrapper_alive(self, job):
        probe = f"if kill -0 {int(job.pid)} 2>/dev/null; then echo alive; fi"
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(probe)}", timeout=30, check=False,
            label=f"pid {job.volume_name}")
        return stdout_text.strip() == "alive"

    def _count_markers(self, job, markers):
        grep_args = " ".join(f"-e {shlex.quote(m)}" for m in markers)
        cmd = (f"grep -F -c {grep_args} {shlex.quote(job.fio_stderr)} 2>/dev/null "
               f"|| true")
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(cmd)}", timeout=30, check=False,
            label=f"scan {job.volume_name}")
        text = (stdout_text or "").strip().splitlines()
        try:
            return int(text[-1]) if text else 0
        except ValueError:
            return 0

    def _grep_markers(self, job, markers, limit=10):
        grep_args = " ".join(f"-e {shlex.quote(m)}" for m in markers)
        cmd = (f"grep -F -m {int(limit)} {grep_args} {shlex.quote(job.fio_stderr)} "
               f"2>/dev/null || true")
        _, stdout_text, _ = self.client.run(
            f"bash -lc {shlex.quote(cmd)}", timeout=30, check=False,
            label=f"grep {job.volume_name}")
        return (stdout_text or "").strip()

    def _dump_fio_streams(self, job, context):
        for label, path, lines in [
            ("fio stderr", job.fio_stderr, 200),
            ("fio summary", job.fio_log, 60),
        ]:
            _, body, _ = self.client.run(
                f"bash -lc {shlex.quote(f'tail -{lines} {shlex.quote(path)} 2>/dev/null || true')}",
                timeout=60, check=False, label=f"dump {label} {job.volume_name}")
            if body.strip():
                self.logger.block(f"[{context}] {job.volume_name} {label} ({path}):", body)
            else:
                self.logger.log(f"[{context}] {job.volume_name} {label} ({path}): (empty)")

    def _fio_completion_due(self):
        """True once fio's own runtime has essentially elapsed, so a clean
        rc=0 exit is completion rather than a mid-run fault."""
        if not self.fio_started_at:
            return False
        elapsed = time.time() - self.fio_started_at
        return elapsed >= self.args.runtime - self.args.fio_completion_grace

    def check_fio(self, strict_latency, context="check"):
        """Raise on any fio fault; return True iff every job finished cleanly.

        Four independent signals, evaluated per job:
          * rc_file written with rc=0 after the runtime elapsed -> completion
          * rc_file written at any other time, or with rc!=0 -> fault
          * wrapper pid gone with no rc_file -> signalled away -> fault
          * a hard-error marker in stderr -> fault even while fio still runs
          * a --max_latency marker -> fault when strict_latency (phase 1),
            otherwise counted and reported
        """
        faults, completed = [], 0
        for job in self.fio_jobs:
            rc = self._read_rc_file(job)
            if rc is not None:
                if rc == "0" and self._fio_completion_due():
                    completed += 1
                    continue
                faults.append((job, "exited", f"fio exited rc={rc}"))
                continue
            if not self._wrapper_alive(job):
                faults.append((job, "missing",
                               f"fio wrapper pid {job.pid} gone with no rc file"))
                continue
            if self._count_markers(job, FIO_HARD_ERROR_MARKERS):
                detail = self._grep_markers(job, FIO_HARD_ERROR_MARKERS).splitlines()
                faults.append((job, "io_error",
                               detail[0][:240] if detail else "hard error marker"))
                continue
            latency_hits = self._count_markers(job, FIO_LATENCY_MARKERS)
            if latency_hits > job.latency_reported:
                new = latency_hits - job.latency_reported
                job.latency_reported = latency_hits
                if strict_latency:
                    faults.append((
                        job, "max_latency",
                        f"{new} new max_latency violation(s) — a single-NIC "
                        f"outage must not stall IO past "
                        f"{self.args.fio_max_latency}s"))
                    continue
                self.latency_violations += new
                self.logger.log(
                    f"[{context}] {job.volume_name}: {new} new max_latency "
                    f"violation(s) (tolerated in this phase; "
                    f"{self.latency_violations} total)")
                self.logger.block(
                    f"[{context}] {job.volume_name} latency lines:",
                    self._grep_markers(job, FIO_LATENCY_MARKERS, limit=5))

        if faults:
            for job, kind, detail in faults:
                self._dump_fio_streams(job, context=f"fio fault [{kind}] {detail}")
            raise TestRunError(
                f"[{context}] fio fault: "
                + ", ".join(f"{j.volume_name}={k}:{d}" for j, k, d in faults))
        return completed == len(self.fio_jobs) and completed > 0

    # ----- SPDK verification ------------------------------------------------

    @staticmethod
    def _classify_controller(name):
        if "hublvol" in name:
            return "hublvol"
        if name.startswith("remote_"):
            return "remote"
        return "other"

    def verify_spdk_state(self, label, strict=True, timeout=None, poll=None):
        """Heal gate: block until path counts, multipath policies and
        listeners are correct on every online node, then report how long
        healing took.

        Redundant-path re-add after an outage runs on the health-check /
        reconcile cadence and legitimately takes minutes (observed: hublvol
        paths at 1/2 or 3/4 for several minutes after a 30 s NIC outage,
        then fully healed). The next iteration must not start against a
        cluster that is still repairing — a new outage on top of degraded
        redundancy tests a different, unplanned scenario. So this waits for
        full convergence, bounded by --path-heal-timeout, and only the
        timeout is a failure. Healing duration is logged as a measurement.

        Only targeted ``bdev_get_bdevs -b <name>`` calls are used, never a
        full dump: unfiltered bdev dumps on a loaded cluster have wedged SPDK
        app threads badly enough to trip keep-alive evictions.
        """
        if self.args.skip_spdk_verify:
            return
        timeout = timeout if timeout is not None else self.args.path_heal_timeout
        poll = poll if poll is not None else self.args.path_heal_poll
        self.logger.log(f"{label}: waiting for all SPDK paths to heal "
                        f"(timeout {timeout}s)")
        started = time.time()
        attempt = 0
        problems = []
        while True:
            attempt += 1
            problems = []
            for node in self.get_nodes():
                uuid = node["uuid"]
                if node["status"] != "online":
                    problems.append(f"{uuid[:12]}: node status={node['status']}")
                    continue
                try:
                    problems.extend(self._verify_node_spdk(uuid))
                except Exception as exc:
                    problems.append(f"{uuid[:12]}: verification error: {exc}")
            try:
                problems.extend(self._verify_client_paths())
            except Exception as exc:
                problems.append(f"client: path verification error: {exc}")
            elapsed = time.time() - started
            if not problems:
                if attempt == 1:
                    self.logger.log(
                        f"{label}: SPDK multipath state OK (already healed)")
                else:
                    self.logger.log(
                        f"{label}: all paths healed after {elapsed:.0f}s "
                        f"({attempt} checks)")
                return
            if elapsed + poll > timeout:
                break
            self.logger.log(
                f"{label}: {len(problems)} unhealed path problem(s) after "
                f"{elapsed:.0f}s, polling again in {poll}s")
            for problem in problems:
                self.logger.log(f"    healing? {problem}")
            time.sleep(poll)

        for problem in problems:
            self.logger.log(f"  UNHEALED {problem}")
        if strict:
            raise TestRunError(
                f"{label}: {len(problems)} path problem(s) still unhealed after "
                f"{timeout}s — repair is stuck, not merely slow")
        self.logger.log(
            f"{label}: {len(problems)} problem(s) unhealed after {timeout}s, "
            f"continuing (non-strict)")

    def _verify_client_paths(self):
        """Verify the CLIENT's view of every volume's paths.

        Target-side state being perfect is not sufficient, and assuming it was
        cost a whole run: soak 2026-08-12 iteration 7 lost all IO on a volume
        whose paths were TCP-live and whose target had subsystem, namespace and
        both listeners verified present, while the client's multipath head
        reported "no usable path" and EIO'd the application.

        Scope note: this checks what the client actually exposes on this kernel
        — per-subsystem path count and per-path State from ``nvme list-subsys``,
        plus the head namespace block device. It deliberately does NOT try to
        read per-path namespace nodes or ANA state: with nvme_core multipath
        this kernel publishes only the head namespace (``nvme1n1``) and the
        controllers, no ``nvmeXcYnZ`` per-path nodes, and this nvme-cli reports
        no ANAState field. An earlier version of this check globbed for those
        and reported 5-of-6 paths dead on every healthy volume.

        The expected path count per volume is learned at baseline rather than
        hardcoded, so a path that vanishes entirely is caught as well as one
        that goes non-live.
        """
        _, stdout_text, _ = self.client.run(
            "sudo nvme list-subsys -o json", timeout=120, check=False,
            label="verify client paths")
        text = (stdout_text or "").strip()
        if not text:
            return ["client: nvme list-subsys returned nothing"]
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as exc:
            return [f"client: cannot parse nvme list-subsys output: {exc}"]

        subsystems = []
        for entry in (doc if isinstance(doc, list) else [doc]):
            if isinstance(entry, dict):
                subsystems.extend(entry.get("Subsystems") or [])
        problems = []
        seen = {}
        for subsystem in subsystems:
            nqn = subsystem.get("NQN", "")
            if ":lvol:" not in nqn:
                continue
            volume = nqn.split(":lvol:")[-1]
            paths = subsystem.get("Paths") or []
            not_live = [
                f"{p.get('Name')}={p.get('State')}" for p in paths
                if p.get("State") != "live"
            ]
            # ANAState is absent on this nvme-cli; honour it when present.
            bad_ana = [
                f"{p.get('Name')}:ana={p.get('ANAState')}" for p in paths
                if p.get("ANAState") not in (None, "optimized", "non-optimized",
                                             "non_optimized")
            ]
            seen[volume] = len(paths)
            expected = self._baseline_client_paths.get(volume)
            if expected is None:
                self._baseline_client_paths[volume] = len(paths)
            elif len(paths) < expected:
                problems.append(
                    f"client: {volume[:12]} has {len(paths)} path(s), "
                    f"expected {expected} — a path disappeared from the "
                    f"multipath head")
            if not_live:
                problems.append(
                    f"client: {volume[:12]} path(s) not live: "
                    f"{', '.join(not_live)}")
            if bad_ana:
                problems.append(
                    f"client: {volume[:12]} unusable ANA state: "
                    f"{', '.join(bad_ana)}")

        tracked = {job.volume_id for job in self.fio_jobs}
        for volume_id in tracked - set(seen):
            problems.append(
                f"client: {volume_id[:12]} has no nvme-subsystem entry at all")
        return problems

    def _verify_node_spdk(self, uuid):
        host = self._node_host(uuid)
        short = uuid[:12]
        problems = []

        _, containers, _ = host.run(
            "sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' || true",
            timeout=30, check=False, label=f"find spdk container {short}")
        names = [n for n in containers.strip().splitlines() if n.strip()]
        if not names:
            return [f"{short}: no SPDK container running"]
        container = names[0]
        rpc = f"python3 /root/spdk/scripts/rpc.py -s /mnt/ramdisk/{container}/spdk.sock"

        def _rpc_json(subcmd, what):
            _, out, err = host.run(
                f"sudo docker exec -u root {container} "
                f"bash -c {shlex.quote(rpc + ' ' + subcmd)}",
                timeout=60, check=False, label=f"{what} {short}")
            out = (out or "").strip()
            if not out:
                raise TestRunError(f"{what} returned nothing ({err.strip()[:200]})")
            return json.loads(out)

        controllers = _rpc_json("bdev_nvme_get_controllers", "get controllers")
        hublvol_bdevs, remote_bdevs = [], []
        for controller in controllers:
            name = controller.get("name", "")
            kind = self._classify_controller(name)
            if kind == "other":
                continue
            # SPDK reports ONE ``ctrlrs`` entry per trid — i.e. one per data
            # NIC — each with an empty ``alternate_trids``. The path count is
            # therefore the number of entries (plus any alternates they do
            # carry), not ``1 + alternate_trids`` of a single entry. Counting
            # per entry makes every healthy 2-path controller look 1-pathed.
            entries = controller.get("ctrlrs", [])
            total_paths = sum(1 + len(e.get("alternate_trids", [])) for e in entries)
            addresses = ",".join(
                e.get("trid", {}).get("traddr", "?") for e in entries)
            not_enabled = [
                f"{e.get('trid', {}).get('traddr', '?')}={e.get('state', '?')}"
                for e in entries if e.get("state") != "enabled"
            ]
            if not_enabled:
                problems.append(
                    f"{short}: {kind} {name[:48]} path(s) not enabled: "
                    f"{', '.join(not_enabled)}")
            # A remote device/JM controller spans exactly the target node's
            # data NICs. A hublvol controller additionally carries the failover
            # node's NICs, so 2 (secondary, no failover peer) or 4 (tertiary,
            # primary + failover).
            if kind == "remote" and total_paths != len(self.args.data_nics):
                problems.append(
                    f"{short}: remote {name[:48]} has {total_paths} path(s), "
                    f"expected {len(self.args.data_nics)} ({addresses})")
            elif kind == "hublvol" and (
                    total_paths == 0
                    or total_paths % len(self.args.data_nics) != 0):
                problems.append(
                    f"{short}: hublvol {name[:48]} has {total_paths} path(s), "
                    f"expected a non-zero multiple of {len(self.args.data_nics)} "
                    f"({addresses})")
            if kind == "hublvol":
                hublvol_bdevs.append(f"{name}n1")
            else:
                remote_bdevs.append(f"{name}n1")

        # Multipath policy. Hublvols are the point of this check: active_active
        # round-robins the LVS leader's two NICs while ANA keeps the failover
        # node passive. active_passive means one NIC carries all hub IO.
        to_check = [(b, "hublvol") for b in hublvol_bdevs]
        to_check += [(b, "remote") for b in remote_bdevs[:max(0, self.args.policy_sample)]]
        # Only meaningful with more than one path: on a single-data-NIC
        # (non-multipath) cluster a bdev legitimately reports active_passive,
        # and asserting active_active there would make every bdev a "problem",
        # so the heal gate could never converge.
        if len(self.args.data_nics) < 2:
            to_check = []
        for bdev_name, kind in to_check:
            try:
                bdevs = _rpc_json(f"bdev_get_bdevs -b {shlex.quote(bdev_name)}",
                                  f"get bdev {bdev_name}")
            except Exception as exc:
                problems.append(f"{short}: cannot read bdev {bdev_name}: {exc}")
                continue
            for bdev in bdevs:
                driver = bdev.get("driver_specific") or {}
                policy = driver.get("mp_policy")
                if policy != "active_active":
                    problems.append(
                        f"{short}: {kind} bdev {bdev_name} mp_policy={policy!r}, "
                        f"expected 'active_active' — one NIC is carrying all its IO")
                elif driver.get("selector") == "round_robin" and driver.get("rr_min_io") != 1:
                    problems.append(
                        f"{short}: {kind} bdev {bdev_name} rr_min_io="
                        f"{driver.get('rr_min_io')}, expected 1 (paths would only "
                        f"alternate every rr_min_io IOs)")

        subsystems = _rpc_json("nvmf_get_subsystems", "get subsystems")
        expected_listeners = len(self.args.data_nics)
        for subsystem in subsystems:
            nqn = subsystem.get("nqn", "")
            if "discovery" in nqn:
                continue
            listeners = subsystem.get("listen_addresses", [])
            if len(listeners) != expected_listeners:
                problems.append(
                    f"{short}: subsystem {nqn.split(':')[-1][:48]} has "
                    f"{len(listeners)} listener(s), expected {expected_listeners}")
        return problems

    # ----- outage primitives ------------------------------------------------

    def _nic_outage(self, node_id, nics, duration, label):
        """Take NICs down on a node for ``duration`` seconds.

        Fire-and-forget through nohup so the SSH channel can die with the NIC:
        the restore is driven by a timer on the node itself, which is also
        what makes it safe if this script dies mid-outage.
        """
        host = self._node_host(node_id)
        down = "; ".join(f"ip link set {n} down" for n in nics)
        up = "; ".join(f"ip link set {n} up" for n in nics)
        cmd = f"sudo nohup bash -c '{down}; sleep {int(duration)}; {up}' >/dev/null 2>&1 &"
        try:
            host.run(f"bash -lc {shlex.quote(cmd)}", timeout=30, check=False,
                     label=f"{label} {node_id[:12]} nics={nics} {duration}s")
        except RemoteCommandError as exc:
            self.logger.log(f"{label} {node_id[:12]}: SSH dropped (expected): {exc}")
        if self.args.mgmt_nic in nics:
            self._drop_node_host(node_id)

    def _container_kill(self, node_id):
        host = self._node_host(node_id)
        cmd = (
            "set -euo pipefail; "
            "cns=$(sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' || true); "
            "if [ -z \"$cns\" ]; then echo 'no spdk_* container found' >&2; exit 0; fi; "
            "for cn in $cns; do echo \"killing $cn\"; sudo docker kill \"$cn\" || true; done"
        )
        host.run(f"bash -lc {shlex.quote(cmd)}", timeout=120, check=False,
                 label=f"container_kill {node_id[:12]}")

    def _host_reboot(self, node_id):
        host = self._node_host(node_id)
        cmd = "sudo nohup bash -c 'sleep 2; reboot -f' >/dev/null 2>&1 &"
        try:
            host.run(f"bash -lc {shlex.quote(cmd)}", timeout=30, check=False,
                     label=f"host_reboot {node_id[:12]}")
        except RemoteCommandError as exc:
            self.logger.log(f"host_reboot {node_id[:12]}: SSH terminated as expected: {exc}")
        self._drop_node_host(node_id)

    #: Refusals meaning "another node in this cluster is mid-transition". The CP
    #: allows one graceful transition at a time (see
    #: check_node_shutdown_preconditions), with --force as its documented escape
    #: hatch. This case exists to overlap two outages deliberately, so such a
    #: refusal is escalated to --force rather than waited out: waiting lets the
    #: partner finish recovering and removes the overlap that is the whole point.
    #: Iteration 21 of the 2026-08-20 run died on exactly this -- a container_kill
    #: partner entered restart 8s before the shutdown was issued. "is restart" is
    #: how "is restarting in this cluster" reaches us after truncation.
    PEER_TRANSITION_MARKERS = (
        "is restarting in this cluster",
        "is already shutting down in this cluster",
        "is restart",
    )

    def _shutdown(self, node_id, force, host, deadline):
        """sbctl sn shutdown, retrying while migration/tasks block it."""
        flag = " --force" if force else ""
        escalated = bool(force)
        while True:
            rc, stdout_text, stderr_text = self.sbctl_allow_failure(
                f"sn shutdown {node_id}{flag}", timeout=300, host=host)
            if rc == 0:
                return
            output = f"{stdout_text}\n{stderr_text}".lower()
            if not escalated and any(
                    m in output for m in self.PEER_TRANSITION_MARKERS):
                self.logger.log(
                    f"Shutdown of {node_id[:12]} refused because a peer is "
                    f"mid-transition; escalating to --force (the overlap is "
                    f"the point of this case)")
                flag = " --force"
                escalated = True
                continue
            blocked = any(marker in output for marker in (
                "migration", "migrat", "rebalanc", "active task", "running task",
                "in_progress", "in progress"))
            if blocked and time.time() < deadline:
                self.logger.log(
                    f"Shutdown of {node_id[:12]} blocked by migration/task; retry in 15s")
                time.sleep(15)
                continue
            raise RemoteCommandError(
                f"sbctl sn shutdown {node_id}{flag} failed rc={rc}: "
                f"{stdout_text.strip()[:200]} {stderr_text.strip()[:200]}")

    def _restart(self, node_id, host, deadline):
        """sbctl sn restart, retrying while the per-cluster guard rejects it.

        When the pair's other node used a self-recovering method it may still
        be in_restart, and the guard refuses concurrent restarts.
        """
        while True:
            rc, stdout_text, stderr_text = self.sbctl_allow_failure(
                f"sn restart {node_id}", timeout=600, host=host)
            if rc == 0:
                return
            if time.time() >= deadline:
                raise RemoteCommandError(
                    f"sbctl sn restart {node_id} failed rc={rc}: "
                    f"{stdout_text.strip()[:200]} {stderr_text.strip()[:200]}")
            self.logger.log(
                f"Restart of {node_id[:12]} rejected (peer likely still "
                f"recovering); retry in 15s")
            time.sleep(15)

    def _apply_outage(self, node_id, method, hold):
        """Start the outage. Returns when the node is down (or the command
        that takes it down has been fired)."""
        if method == "network_outage":
            self._nic_outage(node_id, self.args.data_nics, hold, "network_outage")
        elif method == "mgmt_nic_outage":
            self._nic_outage(node_id, [self.args.mgmt_nic], hold, "mgmt_nic_outage")
        elif method == "all_nics_outage":
            self._nic_outage(node_id, [self.args.mgmt_nic] + list(self.args.data_nics),
                             hold, "all_nics_outage")
        elif method == "container_kill":
            self._container_kill(node_id)
        elif method == "host_reboot":
            self._host_reboot(node_id)
        else:
            raise TestRunError(f"_apply_outage does not handle {method}")

    # ----- phase 1: all-node single-NIC outage -------------------------------

    #: In-container helper for the placement dump. rpc.py has no subcommand
    #: for the custom distr_* RPCs, so this goes through the generic
    #: JSONRPCClient: list bdevs, keep the distribs, ask each to dump its
    #: in-memory placement map (the RPC writes a file in the container and
    #: returns its path).
    _PLACEMENT_DUMP_PY = """
import json, sys
sys.path.insert(0, '/root/spdk/python')
from spdk.rpc.client import JSONRPCClient
client = JSONRPCClient('/mnt/ramdisk/{container}/spdk.sock', timeout=60.0)
out = {{}}
for bdev in client.call('bdev_get_bdevs'):
    name = bdev.get('name', '')
    product = str(bdev.get('product_name', '')).lower()
    if 'distrib' not in product and not name.startswith('distrib'):
        continue
    try:
        out[name] = client.call('distr_debug_placement_map_dump', {{'name': name}})
    except Exception as exc:
        out[name] = 'ERROR: %s' % exc
print(json.dumps(out))
"""

    def take_placement_dumps(self, tag, node_uuids=None):
        """Dump every distrib's in-memory placement map on every reachable
        node, gzip the dump files, and stash them on the node.

        Non-fatal by design: this brackets outages, so some nodes are expected
        to be dead for the post-outage dump -- they are logged and skipped,
        never failed on. Files land on each storage node under
        ~/placement_dumps/<run_id>/<tag>_<node>_<bdev>.txt.gz so the run's
        dumps can be fetched with one scp -r per node afterwards.
        """
        if not self.args.placement_dumps:
            return
        uuids = list(node_uuids) if node_uuids else list(self.node_ip_map)
        results = {}
        results_lock = threading.Lock()

        def _dump_one(uuid):
            short = uuid[:12]
            host = self._node_host(uuid)
            _, containers, _ = host.run(
                "sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' || true",
                timeout=30, check=False, label=f"pd find container {short}")
            names = [n for n in containers.strip().splitlines() if n.strip()]
            if not names:
                raise TestRunError("no SPDK container")
            container = names[0]
            script = self._PLACEMENT_DUMP_PY.format(container=container)
            _, out, err = host.run(
                f"sudo docker exec -u root {container} python3 -c {shlex.quote(script)}",
                timeout=120, check=False, label=f"pd dump {short}")
            try:
                dumped = json.loads((out or "").strip())
            except (ValueError, TypeError):
                raise TestRunError(f"dump RPC returned no JSON ({(err or '')[:150]})")
            dest = f"/home/{self.user}/placement_dumps/{self.run_id}"
            host.run(f"mkdir -p {dest}", timeout=30, check=False,
                     label=f"pd mkdir {short}")
            saved = 0
            for bdev, path in dumped.items():
                if not isinstance(path, str) or path.startswith("ERROR"):
                    self.logger.log(
                        f"placement dump {tag} {short}/{bdev}: {path}")
                    continue
                path = path.strip().strip('"')
                target = f"{dest}/{tag}_{short}_{bdev}.txt.gz"
                # gzip in-container, copy out via docker cp, drop the original
                # so repeated dumps cannot fill the container tmpfs.
                rc, _, gerr = host.run(
                    f"sudo docker exec -u root {container} gzip -f {shlex.quote(path)} && "
                    f"sudo docker cp {container}:{shlex.quote(path)}.gz {shlex.quote(target)} && "
                    f"sudo docker exec -u root {container} rm -f {shlex.quote(path)}.gz",
                    timeout=120, check=False, label=f"pd save {short}/{bdev}")
                if rc == 0:
                    saved += 1
                else:
                    self.logger.log(
                        f"placement dump {tag} {short}/{bdev}: save failed "
                        f"({(gerr or '')[:120]})")
            with results_lock:
                results[short] = saved

        errors = self._fan_out(_dump_one, uuids, f"placement-dump {tag}",
                               join_timeout=240)
        for uuid, exc in errors:
            self.logger.log(
                f"placement dump {tag}: {uuid[:12]} skipped ({exc})")
        if results:
            self.logger.log(
                f"placement dumps [{tag}]: "
                + ", ".join(f"{k}={v}" for k, v in sorted(results.items()))
                + f" file(s) -> ~/placement_dumps/{self.run_id}/ on each node")

    def run_nic_phase(self, iteration, nic, node_uuids):
        hold = self.args.nic_phase_hold
        settle = self.args.nic_phase_settle
        label = f"iter {iteration} phase1"
        self.logger.log(
            f"=== {label}: taking {nic} down on ALL {len(node_uuids)} nodes for "
            f"{hold}s (fio must not be interrupted) ===")

        self.prewarm_node_hosts(node_uuids)
        self.take_placement_dumps(f"iter{iteration}_p1_pre", node_uuids)
        started = time.time()
        errors = self._fan_out(
            lambda uuid: self._nic_outage(uuid, [nic], hold, "nic_phase"),
            node_uuids, "nic-down")
        spread = time.time() - started
        if errors:
            raise TestRunError(
                f"{label}: could not take {nic} down everywhere: "
                + ", ".join(f"{u[:12]}:{e}" for u, e in errors))
        self.logger.log(
            f"{label}: {nic} down on all {len(node_uuids)} nodes "
            f"(fan-out spread {spread:.1f}s); restore by host timer in {hold}s")

        # Probe fio mid-outage: an interruption should surface while the NIC
        # is still down, not only after it comes back. Strict here too — a
        # lenient probe would consume the latency counters and the strict
        # post-outage check would then see nothing new and pass.
        time.sleep(min(hold, max(5, hold // 2)))
        self.check_fio(strict_latency=True, context=f"{label} mid-outage")

        remaining = max(0, hold - (time.time() - started))
        time.sleep(remaining)
        # The host timers have restored the NIC at this point: dump the
        # placement maps IMMEDIATELY after the outage, before the settle.
        self.take_placement_dumps(f"iter{iteration}_p1_post", node_uuids)
        time.sleep(settle)
        self.logger.log(f"{label}: {nic} restored + {settle}s settle elapsed")

        self.verify_spdk_state(label, strict=True)

        # Strict: on a multipath cluster a single-NIC outage must be fully
        # transparent, latency included.
        done = self.check_fio(strict_latency=True, context=f"{label} post-outage")
        self.logger.log(f"{label}: PASS — fio uninterrupted through {nic} outage")
        return done

    # ----- phase 2: overlapping dual-node outage pair ------------------------

    def _outage_worker(self, record, hold, deadline):
        """One node's full outage lifecycle, on its own thread and its own
        mgmt connection so the two members of a pair overlap freely."""
        host = None
        try:
            # Establish the connection BEFORE the offset sleep: an SSH
            # handshake inside the timed window would smear the planned
            # offset by however long the handshake takes.
            if record.method in MANUAL_RECOVERY_METHODS:
                host = self._new_mgmt_host(f"mgmt[{record.node_id[:8]}]", quiet=False)
            else:
                self._node_host(record.node_id)
            if record.planned_offset > 0:
                time.sleep(record.planned_offset)
            if record.method in MANUAL_RECOVERY_METHODS:
                self._shutdown(record.node_id, record.method == "forced_shutdown",
                               host, deadline)
            else:
                self._apply_outage(record.node_id, record.method, hold)
            record.applied_at = time.time()
            record.events.append(f"outage applied at +{record.applied_at:.1f}")
            self.logger.log(
                f"  {record.node_id[:12]} {record.method}: down, holding {hold}s")

            time.sleep(hold)

            if record.method in MANUAL_RECOVERY_METHODS:
                self.logger.log(
                    f"  {record.node_id[:12]} {record.method}: hold elapsed, "
                    f"initiating restart")
                self._restart(record.node_id, host, deadline)
                record.recovery_at = time.time()
                record.events.append("restart issued")
            elif record.method in HOST_TIMER_METHODS:
                record.recovery_at = time.time()
                record.events.append("host timer restores NICs")
                self.logger.log(
                    f"  {record.node_id[:12]} {record.method}: NICs restored by "
                    f"host timer")
            else:
                record.recovery_at = time.time()
                record.events.append("self-recovering, no restart issued")
                self.logger.log(
                    f"  {record.node_id[:12]} {record.method}: self-recovering "
                    f"(no restart issued)")
        except Exception as exc:
            record.error = f"{type(exc).__name__}: {exc}"
            self.logger.log(f"  {record.node_id[:12]} {record.method}: ERROR {record.error}")
        finally:
            if host is not None:
                try:
                    host.close()
                except Exception:
                    pass

    def run_outage_pair(self, iteration, node_a, node_b, method_a, method_b, delay,
                        distance):
        hold = self.args.outage_hold
        label = f"iter {iteration} phase2"
        overlap = delay < hold
        between = distance - 1
        self.logger.log(
            f"=== {label}: pair {node_a[:12]}={method_a} + {node_b[:12]}={method_b}, "
            f"ring distance {distance} "
            f"({'subsequent nodes' if between == 0 else f'{between} node(s) in between'}), "
            f"second outage at +{delay}s, hold {hold}s each "
            f"({self._describe_pair(node_a, node_b)}) ===")
        if not overlap:
            self.logger.log(
                f"{label}: offset {delay}s >= hold {hold}s, so the two 30s hold "
                f"windows do not themselves overlap. The outages still overlap "
                f"whenever recovery outlasts the hold (shutdown/restart and "
                f"host_reboot always do). Use --force-overlap to force the holds "
                f"to overlap as well.")

        records = [
            OutageRecord(node_a, method_a, 0.0),
            OutageRecord(node_b, method_b, float(delay)),
        ]
        self.take_placement_dumps(f"iter{iteration}_p2_pre")
        deadline = time.time() + self.args.restart_timeout
        threads = [
            threading.Thread(target=self._outage_worker, args=(record, hold, deadline),
                             daemon=True, name=f"outage-{record.node_id[:8]}")
            for record in records
        ]
        t0 = time.time()
        for thread in threads:
            thread.start()
        join_budget = delay + hold + self.args.restart_timeout + 120
        for thread in threads:
            thread.join(timeout=join_budget)

        applied = [r.applied_at for r in records if r.applied_at]
        if len(applied) == 2:
            achieved = abs(applied[1] - applied[0])
            self.logger.log(
                f"{label}: achieved offset {achieved:.1f}s (planned {delay}s); "
                f"outage windows {'overlapped' if achieved < hold else 'did not overlap'}")
            if abs(achieved - delay) > 10:
                self.logger.log(
                    f"{label}: NOTE offset drifted {achieved - delay:+.1f}s from plan "
                    f"(a blocked shutdown retries until migration clears)")
        errored = [r for r in records if r.error]
        if errored:
            raise TestRunError(
                f"{label}: outage worker failure: "
                + ", ".join(f"{r.node_id[:12]}({r.method}): {r.error}" for r in errored))

        wait_timeout = self.args.restart_timeout
        if any(r.method in SELF_RECOVER_METHODS for r in records):
            wait_timeout = max(wait_timeout, self.args.auto_recover_wait)
        if any(r.method in HOST_TIMER_METHODS for r in records):
            wait_timeout = max(wait_timeout, hold + 300)
        self.logger.log(
            f"{label}: outages issued in {time.time() - t0:.0f}s; waiting up to "
            f"{wait_timeout}s for both nodes online")
        # IMMEDIATELY after the outage windows: the two target nodes are
        # typically still down/rebooting here -- the dump helper logs and
        # skips unreachable nodes, capturing the survivors' view of
        # placement as the outage ends.
        self.take_placement_dumps(f"iter{iteration}_p2_post")

        self.wait_for_all_online(target_nodes={node_a, node_b}, timeout=wait_timeout)
        done = self.check_fio(strict_latency=False, context=f"{label} post-pair")
        if done:
            return True

        # Fixed settle, deliberately NOT a rebalance wait: after a two-node
        # outage the cluster re-replicates for a long time, and blocking on
        # that would let the soak run only a handful of pairs in 10 hours.
        # Nodes being back online (asserted above) is the gate that matters.
        self.logger.log(
            f"{label}: settling {self.args.iteration_settle}s before verification "
            f"(fixed wait; rebalancing is allowed to continue in the background)")
        time.sleep(self.args.iteration_settle)

        self.verify_spdk_state(label, strict=True)
        self.check_fio(strict_latency=False, context=f"{label} post-settle")
        self.logger.log(f"{label}: PASS — both nodes back, paths verified, fio alive")
        return False

    # ----- main loop --------------------------------------------------------

    def pick_pair(self, uuids, iteration):
        """Pick a pair, rotating the ring distance so the run covers adjacent
        nodes, nodes one apart, and nodes two apart evenly rather than letting
        uniform random sampling over-weight the commonest distance."""
        buckets = self._pairs_by_distance(uuids)
        if not buckets:
            raise TestRunError(
                "No eligible node pair: every combination is excluded by the "
                "role-pair filters")
        distances = sorted(buckets)
        distance = distances[(iteration - 1) % len(distances)]
        node_a, node_b = random.choice(buckets[distance])
        return node_a, node_b, distance

    def pick_methods(self):
        if len(self.methods) >= 2:
            return random.sample(self.methods, 2)
        return [self.methods[0], self.methods[0]]

    def pick_delay(self):
        delay = random.randint(self.args.pair_delay_min, self.args.pair_delay_max)
        if self.args.force_overlap and delay >= self.args.outage_hold:
            delay = max(self.args.pair_delay_min, self.args.outage_hold - 1)
        return delay

    def run(self):
        args = self.args
        self.logger.log("=== multipath dual-node outage soak ===")
        self.logger.log(
            f"fio: {args.fio_rw} bs={args.fio_bs} numjobs={args.fio_numjobs} "
            f"iodepth={args.fio_iodepth} total={args.fio_total_size} "
            f"runtime={args.runtime}s ({args.runtime / 3600:.1f}h)")
        self.logger.log(
            f"phase 1: {args.data_nics} single-NIC on all nodes, "
            f"{args.nic_phase_hold}s down + {args.nic_phase_settle}s settle, "
            f"every {args.nic_phase_every or 'once'} iteration(s)")
        self.logger.log(
            f"phase 2: methods={args.methods}, hold={args.outage_hold}s, "
            f"offset {args.pair_delay_min}-{args.pair_delay_max}s"
            f"{' (clamped to overlap)' if args.force_overlap else ''}, "
            f"then a fixed {args.iteration_settle}s wait — no rebalance wait")

        if args.iterations:
            # Rough per-iteration cost: the NIC phase (hold + settle + checks)
            # plus the pair (offset + hold + node recovery + settle + verify).
            # Recovery dominates and is method-dependent; 240 s is a mid
            # estimate for a shutdown+restart or a host reboot.
            nic_cost = (args.nic_phase_hold + args.nic_phase_settle + 90
                        if args.nic_phase_every else 0)
            pair_cost = ((args.pair_delay_min + args.pair_delay_max) / 2
                         + args.outage_hold + 240 + args.iteration_settle + 60)
            remaining = max(0, args.iterations - max(1, args.start_iteration) + 1)
            estimate = remaining * (nic_cost + pair_cost)
            self.logger.log(
                f"loop: {remaining} outage pairs "
                f"({max(1, args.start_iteration)}..{args.iterations}), rough estimate "
                f"{estimate / 3600:.1f}h ({(nic_cost + pair_cost) / 60:.1f} min/pair)")
            if estimate > args.runtime:
                self.logger.log(
                    f"WARNING: fio --runtime {args.runtime}s "
                    f"({args.runtime / 3600:.1f}h) is shorter than the estimated "
                    f"{estimate / 3600:.1f}h needed for {args.iterations} pairs. fio "
                    f"will end the run first and fewer pairs will have executed. "
                    f"Pass --runtime {int(estimate * 1.15)} to cover the loop.")

        self.ensure_prerequisites()
        self.refresh_topology()
        nodes = self.ensure_expected_nodes()
        self.wait_for_all_online(timeout=args.restart_timeout)
        self.wait_for_cluster_stable()
        self.verify_spdk_state("baseline", strict=True)

        mount_root = self.prepare_client()
        volumes = self.create_volumes(nodes)
        self.connect_and_mount_volumes(volumes, mount_root)
        self.start_fio(volumes)
        self.refresh_topology()

        iteration = max(1, args.start_iteration) - 1
        if iteration:
            self.logger.log(
                f"resuming the outage loop at iteration {iteration + 1} "
                f"(pair rotation and NIC-phase schedule continue from there)")
        while True:
            iteration += 1
            if args.iterations and iteration > args.iterations:
                self.logger.log(f"Reached --iterations {args.iterations}; stopping")
                return
            self.logger.log(f"########## iteration {iteration} ##########")

            # No rebalance wait here by design — see --iteration-settle. The
            # gate is "every node online", not "cluster quiesced".
            self.assert_cluster_not_suspended()
            if args.wait_for_migration:
                self.wait_for_data_migration_complete(f"iteration {iteration}")
            self.refresh_topology()
            current = self.ensure_expected_nodes()
            if any(n["status"] != "online" for n in current):
                raise TestRunError(
                    "Cluster not healthy at iteration start: "
                    + ", ".join(f"{n['uuid'][:12]}:{n['status']}" for n in current))
            uuids = [n["uuid"] for n in current]

            # --nic-phase-every 0 means "once, on iteration 1"; only
            # --no-nic-phase disables phase 1 outright, which is required on a
            # single-data-NIC cluster where taking "one" NIC down on every node
            # isolates the whole cluster instead of testing path redundancy.
            nic_due = ((not args.no_nic_phase)
                       and (iteration == 1 if args.nic_phase_every == 0
                            else (iteration - 1) % args.nic_phase_every == 0))
            if nic_due:
                nic = args.data_nics[(iteration - 1) % len(args.data_nics)]
                if self.run_nic_phase(iteration, nic, uuids):
                    self.logger.log("fio completed during phase 1")
                    return

            node_a, node_b, distance = self.pick_pair(uuids, iteration)
            method_a, method_b = self.pick_methods()
            delay = self.pick_delay()
            if self.run_outage_pair(iteration, node_a, node_b, method_a, method_b,
                                    delay, distance):
                self.logger.log(
                    f"fio completed successfully after {iteration} iteration(s); "
                    f"tolerated max_latency violations: {self.latency_violations}")
                return

    def teardown(self):
        """Best-effort client cleanup. Only called after a clean finish — on
        failure everything is left mounted and connected so the failed state
        can be inspected."""
        self.logger.log("Cleaning up client")
        script = (
            "sudo pkill -f '[f]io --name=soak_mp_' || true\n"
            "sleep 5\n"
            f"for mp in /home/{self.user}/soak_mp_{self.run_id}/vol*; do "
            "  sudo umount \"$mp\" 2>/dev/null || true; done\n"
            "sudo nvme disconnect-all || true\n"
        )
        try:
            self.client.run(f"bash -lc {shlex.quote(script)}", timeout=600,
                            check=False, label="client cleanup")
        except Exception as exc:
            self.logger.log(f"Cleanup error (ignored): {exc}")
        for volume_id in self.created_volume_ids:
            rc, _, _ = self.sbctl_allow_failure(f"lvol delete {volume_id} --force", timeout=300)
            self.logger.log(f"lvol delete {volume_id}: rc={rc}")


def main():
    args = parse_args()
    logger = Logger(args.log_file)
    logger.log(f"Logging to {args.log_file}")
    metadata = load_metadata(args.metadata)
    if not metadata.get("clients"):
        raise SystemExit("Metadata file does not contain a client host")
    if not metadata.get("multipath"):
        logger.log(
            "WARNING: metadata does not declare multipath=true; this soak assumes "
            "a cluster deployed with --data-nics (two data NICs per node)")

    runner = SoakRunner(args, metadata, logger)
    clean = False
    try:
        runner.run()
        clean = True
    except (RemoteCommandError, TestRunError, ValueError) as exc:
        logger.log(f"ERROR: {exc}")
        logger.log(
            "Leaving the cluster and client as-is for inspection (no cleanup on failure)")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.log("Interrupted; leaving state as-is")
        sys.exit(130)
    finally:
        if clean:
            try:
                runner.teardown()
            except Exception as exc:
                logger.log(f"Teardown error: {exc}")
        runner.close()


if __name__ == "__main__":
    main()
