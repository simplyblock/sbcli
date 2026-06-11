#!/usr/bin/env python3
"""
aws_capacity_namespace_churn_soak.py — heavy capacity / namespace churn soak.

This soak ramps a simplyblock cluster up to its volume and capacity limits
with a continuous heavy churn of full volume lifecycles, while injecting
node outages, and runs until it is stopped.

Per-volume lifecycle (the "churn"):
    create lvol  ->  connect (NVMe/TCP)  ->  mkfs.xfs  ->  mount
        ->  fio fill (rw=write, bs=32k, numjobs=4, iodepth=8; single pass
            writing up to --fill-fraction of the filesystem's free space)
        ->  snapshot  ->  clone
        ->  (held until reaped)  ->  unmount  ->  disconnect  ->  delete

Namespaces / subsystems:
    Volumes are created with ``--namespaced`` so the control plane packs them
    into shared NVMe-oF subsystems (up to --ns-per-subsys namespaces each).
    With 50 namespaces/subsystem and ~50 subsystems that is the 2500-volume
    ceiling. Because many lvols share one subsystem NQN, the client side
    ref-counts connections per (client, NQN): the first volume on an NQN runs
    ``nvme connect``; later ones just wait for their namespace device to
    appear; the controller is only ``nvme disconnect``-ed when its last local
    volume is torn down.

Ramp / steady-state control:
    - Two hard caps: --max-volumes (namespaces, counting originals AND clones)
      and --max-total-size (effective capacity, counting ORIGINALS only —
      clones and snapshots do not count toward capacity).
    - New volume sizes are drawn in [--min-size, --max-size] but *steered* so
      the running average converges to --avg-size (~5G). Early in the ramp
      (< --converge-fraction of capacity used) sizes vary widely; later they
      tighten around the steering centre.
    - While below the caps the soak only creates (the population grows — the
      ramp). Once a cap is approached, the reaper starts deleting the oldest
      filled volumes to make room, so creation continues without ever
      exceeding the maxima (steady-state churn).

Fault injection:
    A background thread takes up to --outage-nodes storage nodes offline with
    a graceful ``sn shutdown``, keeps them down for a random interval in
    [--outage-min, --outage-max] (default 15-30 min), then ``sn restart``s
    them and waits for the cluster to come back. Repeats until stopped.

The workload is spread across every client host in the metadata, each with
its own worker pool. The whole run continues until SIGINT/SIGTERM (or the
optional --runtime elapses), then everything is torn down.
"""

import argparse
import json
import logging
import os
import posixpath
import queue
import random
import re
import shlex
import signal
import subprocess
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

try:
    import paramiko
except ImportError:  # pragma: no cover - paramiko is present in the lab image
    paramiko = None

# Silence paramiko's Transport-thread "Socket exception: Connection reset by
# peer" prints — they fire whenever an SSH connection to a node we just shut
# down gets RST'd. The reconnect logic handles it; the noise just clutters.
logging.getLogger("paramiko").setLevel(logging.CRITICAL)
logging.getLogger("paramiko.transport").setLevel(logging.CRITICAL)


UUID_RE = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
# `sbctl lvol connect` emits `sudo nvme connect ... --nqn=<NQN> ...` (long
# form). Tolerate the legacy short form `-n <NQN>` for older sbctl too.
NQN_RE = re.compile(r"(?:--nqn[=\s]+|-n\s+)(\S+)")

# fio fill parameters (per the test spec).
FIO_BS = "32k"
FIO_NUMJOBS = 4
FIO_IODEPTH = 8

_SIZE_UNITS = {"": 1, "B": 1, "K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40, "P": 2**50}


def parse_size(text):
    """Parse a size string like '14T', '100G', '512M', '1024' into bytes.

    Binary (1024-based) units, matching how simplyblock sizes are written.
    Accepts an int/float (treated as bytes) too.
    """
    if isinstance(text, (int, float)):
        return int(text)
    s = str(text).strip().upper().replace("IB", "").rstrip("B")
    if not s:
        raise ValueError("empty size")
    unit = ""
    if s[-1] in _SIZE_UNITS:
        unit = s[-1]
        s = s[:-1]
    return int(float(s) * _SIZE_UNITS[unit])


def human_bytes(num):
    num = float(num)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(num) < 1024.0 or unit == "P":
            return f"{num:.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}P"


def extract_uuid(text):
    """Return the last standalone-UUID line in ``text``.

    sbctl -d floods stdout with debug lines; the newly-created object's UUID
    is emitted as a bare line, so scanning bottom-up for a line that *is* a
    UUID is the reliable way to pick it out (see project notes on sbctl -d).
    """
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if UUID_RE.fullmatch(stripped):
            return stripped
    return None


# --------------------------------------------------------------------------
# SSH / local command execution
# --------------------------------------------------------------------------


class RemoteCommandError(RuntimeError):
    pass


class TestRunError(RuntimeError):
    pass


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


class RemoteHost:
    """One paramiko SSH connection (with a CLI ssh fallback)."""

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
            except Exception as exc:  # noqa: BLE001 - retry any connect error
                last_error = exc
                self.logger.log(f"{self.name}: SSH attempt {attempt}/15 failed to {self.hostname}: {exc}")
                time.sleep(5)
        raise RemoteCommandError(f"{self.name}: failed to connect to {self.hostname}: {last_error}")

    def run(self, command, timeout=600, check=True, label=None, quiet=False):
        if paramiko is None:
            return self._run_via_ssh_cli(command, timeout=timeout, check=check, label=label, quiet=quiet)
        if self.client is None:
            self.connect()
        label = label or command
        if not quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        try:
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as exc:  # noqa: BLE001 - reconnect once on transport failure
            self.logger.log(f"{self.name}: transport failure for {label}: {exc}; reconnecting once")
            self.connect()
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            stdout_text = stdout.read().decode("utf-8", errors="replace")
            stderr_text = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        if not quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", stdout_text)
        if stderr_text and (not quiet or rc != 0):
            self.logger.block(f"{self.name}: STDERR for {label}", stderr_text)
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed rc={rc}: {label}")
        return rc, stdout_text, stderr_text

    def _run_via_ssh_cli(self, command, timeout=600, check=True, label=None, quiet=False):
        label = label or command
        if not quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        ssh_cmd = [
            "ssh", "-o", "StrictHostKeyChecking=no", "-i", self.key_path,
            f"{self.user}@{self.hostname}", command,
        ]
        try:
            completed = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        rc = completed.returncode
        if not quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", completed.stdout or "")
        if (completed.stderr or "") and (not quiet or rc != 0):
            self.logger.block(f"{self.name}: STDERR for {label}", completed.stderr or "")
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed rc={rc}: {label}")
        return rc, completed.stdout or "", completed.stderr or ""

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:  # noqa: BLE001
                pass
            self.client = None


class LocalHost:
    """Runs commands locally (for --run-on-mgmt). subprocess is thread-safe."""

    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def run(self, command, timeout=600, check=True, label=None, quiet=False):
        label = label or command
        if not quiet:
            self.logger.log(f"{self.name}: RUN {label}")
        try:
            completed = subprocess.run(["/bin/bash", "-lc", command], capture_output=True,
                                       text=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            raise RemoteCommandError(f"{self.name}: command timed out: {label}") from exc
        rc = completed.returncode
        if not quiet:
            self.logger.block(f"{self.name}: STDOUT for {label}", completed.stdout or "")
        if (completed.stderr or "") and (not quiet or rc != 0):
            self.logger.block(f"{self.name}: STDERR for {label}", completed.stderr or "")
        if check and rc != 0:
            raise RemoteCommandError(f"{self.name}: command failed rc={rc}: {label}")
        return rc, completed.stdout or "", completed.stderr or ""

    def close(self):
        return


class HostPool:
    """A lease-able pool of connections to one host.

    paramiko's SSHClient is not safe for concurrent exec from many threads,
    so each worker leases its own connection. LocalHost is stateless, so a
    local pool just hands back the same object.
    """

    def __init__(self, factory, size):
        self._q = queue.Queue()
        self._all = []
        for _ in range(max(1, size)):
            host = factory()
            self._all.append(host)
            self._q.put(host)

    @contextmanager
    def lease(self, timeout=None):
        host = self._q.get(timeout=timeout)
        try:
            yield host
        finally:
            self._q.put(host)

    def close(self):
        for host in self._all:
            host.close()


# --------------------------------------------------------------------------
# Volume state
# --------------------------------------------------------------------------

# Lifecycle stages.
ST_CREATING = "creating"   # in the create->...->clone pipeline
ST_FILLED = "filled"       # fio done, snapshot+clone made; held, eligible to reap
ST_REAPING = "reaping"     # teardown in progress


@dataclass
class VolumeState:
    seq: int
    name: str
    size: int                       # provisioned bytes (counts toward capacity)
    client_name: str
    volume_id: str = ""
    nqn: str = ""
    dev: str = ""
    mount_point: str = ""
    snapshot_id: str = ""
    snapshot_name: str = ""
    clone_id: str = ""
    clone_name: str = ""
    stage: str = ST_CREATING
    created_at: float = field(default_factory=time.time)
    filled_at: float = 0.0


# --------------------------------------------------------------------------
# Client context (per client host)
# --------------------------------------------------------------------------


class ClientCtx:
    def __init__(self, name, host_pool, executor, mount_root):
        self.name = name
        self.host_pool = host_pool
        self.executor = executor
        self.mount_root = mount_root
        # Ref-count subsystem connections: nqn -> set(volume_id) connected here.
        self.nqn_lock = threading.Lock()
        self.nqn_refs = defaultdict(set)


# --------------------------------------------------------------------------
# Soak runner
# --------------------------------------------------------------------------


class SoakRunner:
    def __init__(self, args, metadata, logger):
        self.args = args
        self.metadata = metadata
        self.logger = logger
        self.user = metadata["user"]
        self.key_path = resolve_key_path(args.ssh_key or metadata["key_path"])
        self.run_id = time.strftime("%Y%m%d_%H%M%S")
        self.cluster_id = metadata.get("cluster_uuid") or ""

        # Caps (parsed once).
        self.max_volumes = args.max_volumes
        self.max_total_bytes = parse_size(args.max_total_size)
        self.avg_target_bytes = parse_size(args.avg_size)
        self.min_size_bytes = parse_size(args.min_size)
        self.max_size_bytes = parse_size(args.max_size)

        # mgmt connection pool (control-plane / sbctl commands).
        if args.run_on_mgmt:
            single = LocalHost(logger, "mgmt")
            self.mgmt_pool = HostPool(lambda: single, 1)
        else:
            mgmt_ip = metadata["mgmt"]["public_ip"]
            self.mgmt_pool = HostPool(
                lambda: RemoteHost(mgmt_ip, self.user, self.key_path, logger, "mgmt"),
                args.mgmt_connections,
            )

        # Per-client contexts (worker pool + connection pool + mount root).
        self.clients = []
        client_entries = metadata.get("clients") or []
        if not client_entries:
            raise TestRunError("Metadata has no client hosts")
        for idx, entry in enumerate(client_entries):
            if args.run_on_mgmt:
                addr = entry.get("private_ip") or entry["public_ip"]
            else:
                addr = entry["public_ip"]
            name = f"client{idx}"
            pool = HostPool(
                lambda a=addr, n=name: RemoteHost(a, self.user, self.key_path, logger, n),
                args.workers,
            )
            executor = ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix=name)
            mount_root = posixpath.join("/home", self.user, f"capsoak_{self.run_id}")
            self.clients.append(ClientCtx(name, pool, executor, mount_root))
        self.total_worker_slots = args.workers * len(self.clients)

        # Shared accounting (state_lock).
        self.state_lock = threading.RLock()
        self.volumes = {}            # volume_id -> VolumeState (originals)
        self.ns_used = 0             # namespaces reserved (originals + clones)
        self.cap_used = 0            # bytes reserved (originals only)
        self.creating_count = 0      # volumes in the create pipeline (not yet filled/failed)
        self.seq_counter = 0
        self.client_rr = 0           # round-robin index for client assignment

        # Stats.
        self.created_total = 0
        self.reaped_total = 0
        self.failed_total = 0
        self.recent_failures = 0     # consecutive lifecycle failures (reset on success)

        # Node-IP resolution cache + per-node SSH (for nothing here; outages
        # use sbctl). Kept for completeness / future host-level methods.
        self.node_ip_map = {}

        # Outage bookkeeping.
        self.down_nodes = set()
        self.down_nodes_lock = threading.Lock()

        # Control.
        self.stop_event = threading.Event()
        self.threads = []

    # ----- control-plane helpers -------------------------------------------

    def sbctl(self, args, timeout=600, json_output=False, check=True):
        command = "sudo /usr/local/bin/sbctl -d " + args
        with self.mgmt_pool.lease() as mgmt:
            rc, stdout_text, stderr_text = mgmt.run(
                command, timeout=timeout, check=check, label=f"sbctl {args}", quiet=True,
            )
        if not json_output:
            return rc, stdout_text
        parsed = self._parse_json(stdout_text, stderr_text)
        if parsed is None:
            raise TestRunError(f"Failed to parse JSON from sbctl {args}")
        return parsed

    @staticmethod
    def _parse_json(stdout_text, stderr_text):
        for candidate in (stdout_text, stderr_text, (stdout_text or "") + "\n" + (stderr_text or "")):
            candidate = (candidate or "").strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
            decoder = json.JSONDecoder()
            final_payloads, list_payloads, dict_payloads = [], [], []
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
        return None

    def get_nodes(self):
        nodes = self.sbctl("sn list --json", json_output=True)
        parsed = []
        for node in nodes:
            parsed.append({
                "uuid": node["UUID"],
                "status": str(node.get("Status", "")).lower(),
                "mgmt_ip": node.get("Management IP") or node.get("Mgmt IP") or node.get("mgmt_ip") or "",
                "hostname": node.get("Hostname") or "",
            })
        return parsed

    def cluster_status(self):
        clusters = self.sbctl("cluster list --json", json_output=True)
        if not clusters:
            raise TestRunError("cluster list returned no rows")
        return str(clusters[0].get("Status", "")).lower()

    def wait_for_all_online(self, timeout=None, ignore=None, respect_stop=True):
        """Poll until every storage node reports online. ``ignore`` is a set of
        node UUIDs we deliberately took down (don't require those).
        ``respect_stop=False`` keeps waiting through a stop (used in cleanup,
        where we still want nodes back before tearing volumes down)."""
        timeout = timeout or self.args.restart_timeout
        ignore = set(ignore or [])
        started = time.time()
        while time.time() - started < timeout:
            if respect_stop and self.stop_event.is_set():
                return False
            try:
                nodes = self.get_nodes()
            except (RemoteCommandError, TestRunError) as exc:
                self.logger.log(f"wait_for_all_online: node list error: {exc}")
                time.sleep(self.args.poll_interval)
                continue
            bad = [n["uuid"] for n in nodes if n["status"] != "online" and n["uuid"] not in ignore]
            if not bad:
                return True
            time.sleep(self.args.poll_interval)
        self.logger.log(f"wait_for_all_online: timed out after {timeout}s")
        return False

    # ----- prerequisites ----------------------------------------------------

    def ensure_prerequisites(self):
        self.logger.log(f"Using SSH key {self.key_path}")
        for ctx in self.clients:
            with ctx.host_pool.lease() as host:
                host.run(
                    "if command -v dnf >/dev/null 2>&1; then "
                    "sudo dnf install -y nvme-cli fio xfsprogs; "
                    "else sudo apt-get update && sudo apt-get install -y nvme-cli fio xfsprogs; fi",
                    timeout=1800, label=f"install packages on {ctx.name}",
                )
                host.run("sudo modprobe nvme_tcp", timeout=60, label="load nvme_tcp")
                host.run(
                    f"sudo mkdir -p {shlex.quote(ctx.mount_root)} && "
                    f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} {shlex.quote(ctx.mount_root)}",
                    timeout=120, label=f"prepare workspace on {ctx.name}",
                )

    # ----- size steering ----------------------------------------------------

    def _pick_volume_size(self):
        """Draw a size in [min,max] steered so the running average of live
        originals converges to --avg-size. Wide spread early in the ramp,
        tightening once --converge-fraction of capacity is used."""
        with self.state_lock:
            sizes = [v.size for v in self.volumes.values()]
            cap_used = self.cap_used
        target = self.avg_target_bytes
        cur_avg = (sum(sizes) / len(sizes)) if sizes else target
        # Reflect the current average about the target: if we've been running
        # high, centre low (and vice-versa) so the mean is pulled to target.
        centre = max(self.min_size_bytes, min(self.max_size_bytes, 2 * target - cur_avg))
        # ramp: 0 while empty -> 1 once converge-fraction of capacity is used.
        denom = max(1, self.max_total_bytes * self.args.converge_fraction)
        ramp = min(1.0, cap_used / denom)
        # Band around the centre widens to the full [min,max] early, narrows
        # to a tight window late. triangular(low, high, mode=centre).
        low = self.min_size_bytes + (max(self.min_size_bytes, centre * 0.6) - self.min_size_bytes) * ramp
        high = self.max_size_bytes - (self.max_size_bytes - min(self.max_size_bytes, centre * 1.6)) * ramp
        low = max(self.min_size_bytes, min(low, centre))
        high = min(self.max_size_bytes, max(high, centre))
        if high <= low:
            size = int(centre)
        else:
            size = int(random.triangular(low, high, min(max(centre, low), high)))
        size = max(self.min_size_bytes, min(self.max_size_bytes, size))
        # Round to whole MiB for tidy sizes.
        size = max(self.min_size_bytes, (size // (2**20)) * (2**20))
        return size

    # ----- reservation gating ----------------------------------------------

    def _try_reserve(self):
        """Atomically reserve a slot for one new volume lifecycle. Reserves
        room for the eventual clone too (so we never overshoot --max-volumes).
        Returns a VolumeState on success, else None."""
        size = self._pick_volume_size()
        with self.state_lock:
            if self.stop_event.is_set():
                return None
            if self.creating_count >= self.total_worker_slots:
                return None  # don't queue beyond the worker capacity
            if self.ns_used + 2 > self.max_volumes:
                return None  # +2 = this original and its future clone
            if self.cap_used + size > self.max_total_bytes:
                return None
            self.seq_counter += 1
            seq = self.seq_counter
            ctx = self.clients[self.client_rr % len(self.clients)]
            self.client_rr += 1
            vol = VolumeState(
                seq=seq,
                name=f"capsoak_{self.run_id}_v{seq}",
                size=size,
                client_name=ctx.name,
            )
            self.ns_used += 1            # the original namespace
            self.cap_used += size
            self.creating_count += 1
        return vol, ctx

    def _release(self, vol):
        """Release a volume's reservation (called once on teardown/failure)."""
        with self.state_lock:
            self.ns_used -= 1 + (1 if vol.clone_id else 0)
            self.cap_used -= vol.size
            self.volumes.pop(vol.volume_id, None)

    # ----- volume lifecycle -------------------------------------------------

    def _ctx_by_name(self, name):
        for ctx in self.clients:
            if ctx.name == name:
                return ctx
        raise KeyError(name)

    def _create_lvol(self, vol):
        namespaced = "True" if self.args.ns_per_subsys > 1 else "False"
        cmd = (
            f"lvol add {vol.name} {vol.size} {self.args.pool} "
            f"--namespaced {namespaced} --max-namespace-per-subsys {self.args.ns_per_subsys}"
        )
        if self.args.host_id:
            cmd += f" --host-id {self.args.host_id}"
        rc, out = self.sbctl(cmd, timeout=self.args.op_timeout, check=False)
        if rc != 0:
            raise TestRunError(f"lvol add failed for {vol.name}: rc={rc}")
        vid = extract_uuid(out)
        if not vid:
            raise TestRunError(f"could not parse volume UUID for {vol.name}")
        vol.volume_id = vid
        with self.state_lock:
            self.volumes[vid] = vol

    def _connect(self, vol, ctx):
        """Resolve/establish the NVMe connection for this volume's subsystem,
        ref-counting shared subsystems, and wait for the namespace device."""
        rc, out = self.sbctl(f"lvol connect {vol.volume_id}", timeout=self.args.op_timeout, check=False)
        if rc != 0:
            raise TestRunError(f"lvol connect failed for {vol.volume_id}")
        connect_cmds = [ln.strip() for ln in out.splitlines() if ln.strip().startswith("sudo nvme connect")]
        if not connect_cmds:
            raise TestRunError(f"no nvme connect command for {vol.volume_id}")
        nqn = None
        for cmd in connect_cmds:
            m = NQN_RE.search(cmd)
            if m:
                nqn = m.group(1)
                break
        if not nqn:
            raise TestRunError(f"could not parse NQN for {vol.volume_id}")
        vol.nqn = nqn

        with ctx.nqn_lock:
            first = len(ctx.nqn_refs[nqn]) == 0
            ctx.nqn_refs[nqn].add(vol.volume_id)
            if first:
                # First volume on this subsystem from this client: connect now,
                # under the lock so two racing volumes don't double-connect.
                with ctx.host_pool.lease() as host:
                    connected = 0
                    for cmd in connect_cmds:
                        try:
                            host.run(cmd, timeout=120, label=f"connect {nqn}", quiet=True)
                            connected += 1
                        except RemoteCommandError as exc:
                            self.logger.log(f"path connect failed for {nqn}: {exc}")
                    if connected == 0:
                        ctx.nqn_refs[nqn].discard(vol.volume_id)
                        raise TestRunError(f"no nvme paths connected for {nqn}")

        # Wait for this volume's namespace block device to appear (the kernel
        # may need a moment / a rescan when the namespace was added to an
        # already-connected controller).
        self._wait_for_device(vol, ctx)

    def _wait_for_device(self, vol, ctx):
        deadline = time.time() + self.args.device_timeout
        rescanned = False
        find = f"readlink -f /dev/disk/by-id/*{vol.volume_id}* 2>/dev/null | head -n1"
        while time.time() < deadline:
            if self.stop_event.is_set():
                raise TestRunError("stopping")
            with ctx.host_pool.lease() as host:
                _, out, _ = host.run(f"bash -lc {shlex.quote(find)}", timeout=30, check=False, quiet=True)
            dev = out.strip()
            if dev:
                vol.dev = dev
                return dev
            if not rescanned and time.time() > deadline - self.args.device_timeout / 2:
                # Half-way: force a namespace rescan on every nvme controller.
                with ctx.host_pool.lease() as host:
                    host.run(
                        "bash -lc 'for c in /dev/nvme[0-9]*; do "
                        "[ -c \"$c\" ] && sudo nvme ns-rescan \"$c\" 2>/dev/null || true; done'",
                        timeout=60, check=False, quiet=True,
                    )
                rescanned = True
            time.sleep(2)
        raise TestRunError(f"namespace device for {vol.volume_id} did not appear")

    def _format_and_mount(self, vol, ctx):
        vol.mount_point = posixpath.join(ctx.mount_root, f"vol{vol.seq}")
        script = (
            "set -euo pipefail\n"
            f"dev=$(readlink -f /dev/disk/by-id/*{vol.volume_id}* | head -n1)\n"
            "if [ -z \"$dev\" ]; then echo 'device not found' >&2; exit 1; fi\n"
            f"sudo mkfs.xfs -f \"$dev\"\n"
            f"sudo mkdir -p {shlex.quote(vol.mount_point)}\n"
            f"sudo mount \"$dev\" {shlex.quote(vol.mount_point)}\n"
            f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} {shlex.quote(vol.mount_point)}\n"
        )
        with ctx.host_pool.lease() as host:
            host.run(f"bash -lc {shlex.quote(script)}", timeout=600,
                     label=f"format+mount {vol.volume_id}", quiet=True)

    def _fio_fill(self, vol, ctx):
        """Single-pass sequential write filling up to --fill-fraction of the
        filesystem's free space. Blocks until fio completes."""
        with ctx.host_pool.lease() as host:
            _, out, _ = host.run(
                f"df -B1 --output=avail {shlex.quote(vol.mount_point)} | tail -1 | tr -d ' '",
                timeout=60, check=False, quiet=True,
            )
        try:
            avail = int(out.strip())
        except ValueError:
            raise TestRunError(f"could not read free space for {vol.mount_point}")
        per_job = int(avail * self.args.fill_fraction / FIO_NUMJOBS)
        if per_job < 2**20:  # < 1 MiB per job: nothing meaningful to write
            self.logger.log(f"vol {vol.seq}: too little space to fill ({human_bytes(avail)}), skipping fio")
            return
        log = posixpath.join(ctx.mount_root, f"fio_v{vol.seq}.log")
        fio_cmd = (
            f"cd {shlex.quote(vol.mount_point)} && fio --name=fill_v{vol.seq} "
            f"--directory={shlex.quote(vol.mount_point)} --rw=write --bs={FIO_BS} "
            f"--numjobs={FIO_NUMJOBS} --iodepth={FIO_IODEPTH} --ioengine={self.args.fio_ioengine} "
            f"--direct=1 --size={per_job} --group_reporting "
            f"--output={shlex.quote(log)}"
        )
        with ctx.host_pool.lease() as host:
            rc, _, stderr_text = host.run(f"bash -lc {shlex.quote(fio_cmd)}",
                                          timeout=self.args.fio_timeout, check=False,
                                          label=f"fio fill v{vol.seq} ({human_bytes(per_job * FIO_NUMJOBS)})")
            if rc != 0:
                _, tail, _ = host.run(f"tail -n 40 {shlex.quote(log)}", timeout=30, check=False, quiet=True)
                self.logger.block(f"fio fill FAILED v{vol.seq} (rc={rc})", (stderr_text or "") + "\n" + (tail or ""))
                raise TestRunError(f"fio fill failed for v{vol.seq} rc={rc}")

    def _snapshot_and_clone(self, vol):
        vol.snapshot_name = f"{vol.name}_snap"
        rc, out = self.sbctl(f"snapshot add {vol.volume_id} {vol.snapshot_name}",
                             timeout=self.args.op_timeout, check=False)
        if rc != 0:
            raise TestRunError(f"snapshot add failed for {vol.volume_id} rc={rc}")
        snap_id = extract_uuid(out)
        if not snap_id:
            raise TestRunError(f"could not parse snapshot UUID for {vol.volume_id}")
        vol.snapshot_id = snap_id

        vol.clone_name = f"{vol.name}_clone"
        namespaced = "True" if self.args.ns_per_subsys > 1 else "False"
        rc, out = self.sbctl(
            f"snapshot clone {vol.snapshot_id} {vol.clone_name} --namespaced {namespaced}",
            timeout=self.args.op_timeout, check=False,
        )
        if rc != 0:
            raise TestRunError(f"snapshot clone failed for {vol.snapshot_id} rc={rc}")
        clone_id = extract_uuid(out)
        if not clone_id:
            raise TestRunError(f"could not parse clone UUID for {vol.snapshot_id}")
        vol.clone_id = clone_id
        with self.state_lock:
            self.ns_used += 1  # the clone's namespace (counts toward volume count)

    def _lifecycle(self, vol, ctx):
        """Full create->fill->snapshot->clone pipeline. Marks the volume FILLED
        on success. On failure, best-effort teardown + release."""
        try:
            self._create_lvol(vol)
            self._connect(vol, ctx)
            self._format_and_mount(vol, ctx)
            self._fio_fill(vol, ctx)
            self._snapshot_and_clone(vol)
            with self.state_lock:
                vol.stage = ST_FILLED
                vol.filled_at = time.time()
                self.creating_count -= 1
                self.created_total += 1
                self.recent_failures = 0
        except Exception as exc:  # noqa: BLE001 - a soak tolerates per-volume failure
            self.logger.log(f"LIFECYCLE FAILED v{vol.seq} ({vol.volume_id or 'no-id'}): {exc}")
            with self.state_lock:
                self.creating_count -= 1
                self.failed_total += 1
                self.recent_failures += 1
                failures = self.recent_failures
            self._teardown(vol, ctx, best_effort=True)
            self._release(vol)
            if failures >= self.args.max_consecutive_failures:
                self.logger.log(
                    f"ABORT: {failures} consecutive lifecycle failures "
                    f">= --max-consecutive-failures ({self.args.max_consecutive_failures})"
                )
                self.stop_event.set()

    def _teardown(self, vol, ctx, best_effort=False):
        """unmount -> disconnect (if last on subsystem) -> delete clone ->
        delete snapshot -> delete original. Best-effort tolerates missing
        pieces (used on the failure path)."""

        def attempt(fn, what):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                if not best_effort:
                    raise
                self.logger.log(f"teardown v{vol.seq}: {what} failed (ignored): {exc}")

        # unmount
        if vol.mount_point:
            def _umount():
                script = (
                    f"sudo umount {shlex.quote(vol.mount_point)} 2>/dev/null || "
                    f"sudo umount -f {shlex.quote(vol.mount_point)} 2>/dev/null || "
                    f"sudo umount -l {shlex.quote(vol.mount_point)} 2>/dev/null || true"
                )
                with ctx.host_pool.lease() as host:
                    host.run(f"bash -lc {shlex.quote(script)}", timeout=120, check=False, quiet=True)
            attempt(_umount, "unmount")

        # disconnect subsystem only when this was the last local volume on it
        if vol.nqn:
            def _disconnect():
                last = False
                with ctx.nqn_lock:
                    refs = ctx.nqn_refs.get(vol.nqn)
                    if refs is not None:
                        refs.discard(vol.volume_id)
                        last = len(refs) == 0
                        if last:
                            ctx.nqn_refs.pop(vol.nqn, None)
                if last:
                    with ctx.host_pool.lease() as host:
                        host.run(f"sudo nvme disconnect -n {shlex.quote(vol.nqn)}",
                                 timeout=60, check=False, quiet=True, label=f"disconnect {vol.nqn}")
            attempt(_disconnect, "disconnect")

        # delete clone, then snapshot, then original
        if vol.clone_id:
            attempt(lambda: self._sbctl_delete(f"lvol delete {vol.clone_id} --force"), "delete clone")
        if vol.snapshot_id:
            attempt(lambda: self._sbctl_delete(f"snapshot delete {vol.snapshot_id} --force"), "delete snapshot")
        if vol.volume_id:
            attempt(lambda: self._sbctl_delete(f"lvol delete {vol.volume_id} --force"), "delete lvol")

    def _sbctl_delete(self, args):
        rc, _ = self.sbctl(args, timeout=self.args.op_timeout, check=False)
        if rc != 0:
            raise TestRunError(f"{args} -> rc={rc}")

    def _reap(self, vol):
        ctx = self._ctx_by_name(vol.client_name)
        try:
            self._teardown(vol, ctx, best_effort=True)
        finally:
            self._release(vol)
            with self.state_lock:
                self.reaped_total += 1

    # ----- control loops ----------------------------------------------------

    def _creator_loop(self):
        while not self.stop_event.is_set():
            reserved = self._try_reserve()
            if reserved is None:
                time.sleep(self.args.create_interval)
                continue
            vol, ctx = reserved
            ctx.executor.submit(self._lifecycle, vol, ctx)
            # Pace submissions so the ramp is gradual rather than a thundering
            # herd; the worker pools bound true concurrency regardless.
            time.sleep(self.args.create_interval)

    def _reaper_loop(self):
        while not self.stop_event.is_set():
            target = None
            with self.state_lock:
                near_count = self.ns_used + 2 > self.max_volumes
                near_cap = self.cap_used + self.avg_target_bytes > self.max_total_bytes
                if near_count or near_cap:
                    filled = [v for v in self.volumes.values() if v.stage == ST_FILLED]
                    if filled:
                        filled.sort(key=lambda v: v.filled_at)
                        target = filled[0]
                        target.stage = ST_REAPING
            if target is not None:
                ctx = self._ctx_by_name(target.client_name)
                ctx.executor.submit(self._reap, target)
            else:
                time.sleep(self.args.reaper_interval)

    def _pick_outage_nodes(self):
        try:
            nodes = self.get_nodes()
        except (RemoteCommandError, TestRunError) as exc:
            self.logger.log(f"outage: cannot list nodes: {exc}")
            return []
        online = [n["uuid"] for n in nodes if n["status"] == "online"]
        with self.down_nodes_lock:
            online = [u for u in online if u not in self.down_nodes]
        if len(online) <= self.args.outage_keep_min:
            return []  # never take the cluster below a safety floor
        k = random.randint(1, min(self.args.outage_nodes, len(online) - self.args.outage_keep_min))
        return random.sample(online, k)

    def _graceful_shutdown(self, node_id):
        deadline = time.time() + self.args.restart_timeout
        while not self.stop_event.is_set():
            rc, out = self.sbctl(f"sn shutdown {node_id}", timeout=300, check=False)
            if rc == 0:
                return True
            # Retry on the concurrent-shutdown guard (a peer is in_restart /
            # in_shutdown) until restart_timeout.
            if "concurrent" in (out or "").lower() and time.time() < deadline:
                self.logger.log(f"shutdown {node_id}: concurrent guard, retrying in 15s")
                time.sleep(15)
                continue
            self.logger.log(f"shutdown {node_id} failed rc={rc}")
            return False
        return False

    def _restart_node(self, node_id):
        # Deliberately does NOT short-circuit on stop_event: cleanup relies on
        # this to bring back nodes it took down. Bounded by restart_timeout.
        deadline = time.time() + self.args.restart_timeout
        while True:
            rc, _ = self.sbctl(f"sn restart {node_id}", timeout=600, check=False)
            if rc == 0:
                return True
            if time.time() >= deadline:
                self.logger.log(f"restart {node_id} failed rc={rc}, giving up")
                return False
            self.logger.log(f"restart {node_id} failed rc={rc}, retrying in 15s")
            time.sleep(15)

    def _outage_loop(self):
        # Initial settle before the first outage.
        if self.stop_event.wait(self.args.outage_initial_delay):
            return
        while not self.stop_event.is_set():
            targets = self._pick_outage_nodes()
            if not targets:
                if self.stop_event.wait(self.args.outage_gap_min):
                    return
                continue
            self.logger.log(f"OUTAGE: shutting down {targets}")
            actually_down = []
            for node_id in targets:
                if self._graceful_shutdown(node_id):
                    actually_down.append(node_id)
                    with self.down_nodes_lock:
                        self.down_nodes.add(node_id)
            if not actually_down:
                if self.stop_event.wait(self.args.outage_gap_min):
                    return
                continue
            down_for = random.uniform(self.args.outage_min, self.args.outage_max)
            self.logger.log(f"OUTAGE: {actually_down} down for {down_for/60:.1f} min")
            if self.stop_event.wait(down_for):
                # Stopping — restart what we took down so cleanup is sane.
                for node_id in actually_down:
                    self._restart_node(node_id)
                    with self.down_nodes_lock:
                        self.down_nodes.discard(node_id)
                return
            for node_id in actually_down:
                self.logger.log(f"OUTAGE: restarting {node_id}")
                self._restart_node(node_id)
                with self.down_nodes_lock:
                    self.down_nodes.discard(node_id)
            self.wait_for_all_online()
            gap = random.uniform(self.args.outage_gap_min, self.args.outage_gap_max)
            self.logger.log(f"OUTAGE: cycle done, next in {gap/60:.1f} min")
            if self.stop_event.wait(gap):
                return

    def _heartbeat_loop(self):
        while not self.stop_event.wait(self.args.heartbeat_interval):
            with self.state_lock:
                originals = len(self.volumes)
                filling = sum(1 for v in self.volumes.values() if v.stage == ST_CREATING)
                filled = sum(1 for v in self.volumes.values() if v.stage == ST_FILLED)
                clones = sum(1 for v in self.volumes.values() if v.clone_id)
                ns_used = self.ns_used
                cap_used = self.cap_used
                created, reaped, failed = self.created_total, self.reaped_total, self.failed_total
            with self.down_nodes_lock:
                down = sorted(self.down_nodes)
            self.logger.log(
                f"STATS: ns={ns_used}/{self.max_volumes} "
                f"(orig={originals}, clones={clones}, filling={filling}, filled={filled}) "
                f"cap={human_bytes(cap_used)}/{human_bytes(self.max_total_bytes)} "
                f"avg={human_bytes(cap_used / originals) if originals else '0'} "
                f"created={created} reaped={reaped} failed={failed} down={down}"
            )

    # ----- run / cleanup -----------------------------------------------------

    def run(self):
        self.logger.log(
            f"Starting capacity/namespace churn soak run_id={self.run_id} "
            f"clients={[c.name for c in self.clients]} workers/client={self.args.workers} "
            f"max_volumes={self.max_volumes} max_total={human_bytes(self.max_total_bytes)} "
            f"avg={human_bytes(self.avg_target_bytes)} size=[{human_bytes(self.min_size_bytes)},"
            f"{human_bytes(self.max_size_bytes)}] ns_per_subsys={self.args.ns_per_subsys}"
        )
        self.ensure_prerequisites()
        if not self.wait_for_all_online():
            raise TestRunError("cluster not fully online at start")

        loops = [
            ("creator", self._creator_loop),
            ("reaper", self._reaper_loop),
            ("heartbeat", self._heartbeat_loop),
        ]
        if not self.args.no_outage:
            loops.append(("outage", self._outage_loop))
        for name, target in loops:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self.threads.append(t)

        # Wait until stopped (signal) or optional runtime elapses.
        if self.args.runtime > 0:
            self.stop_event.wait(self.args.runtime)
            self.stop_event.set()
        else:
            while not self.stop_event.wait(1):
                pass
        self.logger.log("Stop requested; tearing down")

    def cleanup(self):
        self.stop_event.set()
        # Let control loops notice the stop.
        for t in self.threads:
            t.join(timeout=30)
        # Restart any nodes still down.
        with self.down_nodes_lock:
            still_down = sorted(self.down_nodes)
        for node_id in still_down:
            self.logger.log(f"cleanup: restarting {node_id}")
            self._restart_node(node_id)
        self.wait_for_all_online(timeout=self.args.restart_timeout, respect_stop=False)

        # Tear down every known volume across the client executors.
        with self.state_lock:
            vols = list(self.volumes.values())
        self.logger.log(f"cleanup: tearing down {len(vols)} volumes")
        futures = []
        for vol in vols:
            try:
                ctx = self._ctx_by_name(vol.client_name)
            except KeyError:
                continue
            futures.append(ctx.executor.submit(self._teardown, vol, ctx, True))
        for f in futures:
            try:
                f.result(timeout=self.args.op_timeout)
            except Exception as exc:  # noqa: BLE001
                self.logger.log(f"cleanup teardown error: {exc}")

        # Best-effort sweep for anything left bearing our run_id.
        self._sweep_leftovers()

        for ctx in self.clients:
            ctx.executor.shutdown(wait=False, cancel_futures=True)
            ctx.host_pool.close()
        self.mgmt_pool.close()
        self.logger.log(
            f"Done. created={self.created_total} reaped={self.reaped_total} failed={self.failed_total}"
        )

    def _sweep_leftovers(self):
        prefix = f"capsoak_{self.run_id}_"
        try:
            lvols = self.sbctl("lvol list --json", json_output=True)
        except (RemoteCommandError, TestRunError):
            lvols = []
        for lv in lvols or []:
            name = lv.get("Name") or lv.get("name") or ""
            lid = lv.get("UUID") or lv.get("id") or ""
            if name.startswith(prefix) and lid:
                self.logger.log(f"sweep: deleting leftover lvol {name} ({lid})")
                self.sbctl(f"lvol delete {lid} --force", check=False)
        try:
            snaps = self.sbctl("snapshot list --json", json_output=True)
        except (RemoteCommandError, TestRunError):
            snaps = []
        for sn in snaps or []:
            name = sn.get("Snapshot Name") or sn.get("name") or sn.get("Name") or ""
            sid = sn.get("UUID") or sn.get("id") or ""
            if name.startswith(prefix) and sid:
                self.logger.log(f"sweep: deleting leftover snapshot {name} ({sid})")
                self.sbctl(f"snapshot delete {sid} --force", check=False)


# --------------------------------------------------------------------------
# metadata / key resolution
# --------------------------------------------------------------------------


def load_metadata(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def candidate_key_paths(raw_path):
    expanded = os.path.expanduser(raw_path)
    base = os.path.basename(raw_path.replace("\\", "/"))
    home = Path.home()
    candidates = [Path(expanded), home / ".ssh" / base, home / base]
    seen, unique = set(), []
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
        f"Unable to resolve SSH key from {raw_path!r}. Tried: "
        f"{', '.join(str(p) for p in candidate_key_paths(raw_path))}"
    )


# --------------------------------------------------------------------------
# args
# --------------------------------------------------------------------------


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Heavy capacity/namespace churn soak with node outages.")
    here = os.path.dirname(os.path.abspath(__file__))
    p.add_argument("--metadata", default=os.path.join(here, "cluster_metadata.json"),
                   help="Path to cluster metadata JSON.")
    p.add_argument("--pool", default="pool01", help="Storage pool id or name.")
    p.add_argument("--ssh-key", default="", help="Override SSH key path from metadata.")
    p.add_argument("--run-on-mgmt", action="store_true",
                   help="Run mgmt commands locally and reach clients on private IPs.")
    p.add_argument("--host-id", default="", help="Pin all volumes to one node (default: let the CP place).")

    # Caps / sizing.
    p.add_argument("--max-volumes", type=int, default=2500,
                   help="Max namespaces (originals + clones). Default 2500 (50 subsys x 50 ns).")
    p.add_argument("--max-total-size", default="14T",
                   help="Max effective capacity (originals only). Default 14T.")
    p.add_argument("--avg-size", default="5G", help="Target running-average volume size. Default 5G.")
    p.add_argument("--min-size", default="1G", help="Minimum volume size. Default 1G.")
    p.add_argument("--max-size", default="100G", help="Maximum volume size. Default 100G.")
    p.add_argument("--ns-per-subsys", type=int, default=50,
                   help="Max namespaces per subsystem (--max-namespace-per-subsys). Default 50.")
    p.add_argument("--converge-fraction", type=float, default=0.25,
                   help="Capacity fraction by which size variance narrows to the avg. Default 0.25.")
    p.add_argument("--fill-fraction", type=float, default=0.80,
                   help="Fraction of each volume's free space fio fills. Default 0.80.")

    # Concurrency.
    p.add_argument("--workers", type=int, default=16, help="Worker threads per client. Default 16.")
    p.add_argument("--mgmt-connections", type=int, default=8,
                   help="Concurrent mgmt/sbctl SSH connections. Default 8.")
    p.add_argument("--create-interval", type=float, default=0.5,
                   help="Seconds between volume-create submissions (paces the ramp). Default 0.5.")
    p.add_argument("--reaper-interval", type=float, default=2.0,
                   help="Seconds between reaper checks. Default 2.0.")

    # fio.
    p.add_argument("--fio-ioengine", default="libaio", help="fio ioengine. Default libaio.")
    p.add_argument("--fio-timeout", type=int, default=7200, help="Per-volume fio fill timeout (s). Default 7200.")

    # Node outages.
    p.add_argument("--no-outage", action="store_true", help="Disable node-outage injection.")
    p.add_argument("--outage-nodes", type=int, default=2, help="Max nodes down at once. Default 2.")
    p.add_argument("--outage-keep-min", type=int, default=1,
                   help="Never take the cluster below this many online nodes via outage. Default 1.")
    p.add_argument("--outage-min", type=float, default=900, help="Min outage duration (s). Default 900 (15m).")
    p.add_argument("--outage-max", type=float, default=1800, help="Max outage duration (s). Default 1800 (30m).")
    p.add_argument("--outage-gap-min", type=float, default=120, help="Min gap between outages (s). Default 120.")
    p.add_argument("--outage-gap-max", type=float, default=600, help="Max gap between outages (s). Default 600.")
    p.add_argument("--outage-initial-delay", type=float, default=300,
                   help="Delay before the first outage (s). Default 300.")

    # Timeouts / pacing.
    p.add_argument("--op-timeout", type=int, default=600, help="Per control-plane op timeout (s). Default 600.")
    p.add_argument("--device-timeout", type=int, default=120,
                   help="How long to wait for a namespace device to appear (s). Default 120.")
    p.add_argument("--restart-timeout", type=int, default=900, help="Wait-for-online timeout (s). Default 900.")
    p.add_argument("--poll-interval", type=float, default=10, help="Node-status poll interval (s). Default 10.")
    p.add_argument("--heartbeat-interval", type=float, default=30, help="Stats log interval (s). Default 30.")
    p.add_argument("--max-consecutive-failures", type=int, default=50,
                   help="Abort after this many consecutive lifecycle failures. Default 50.")

    # Lifecycle.
    p.add_argument("--runtime", type=int, default=0,
                   help="Optional max runtime (s); 0 = run until stopped. Default 0.")
    p.add_argument("--log-file", default="", help="Log file path. Default timestamped.")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if not args.log_file:
        args.log_file = f"aws_capacity_namespace_churn_soak_{time.strftime('%Y%m%d_%H%M%S')}.log"
    logger = Logger(args.log_file)
    logger.log(f"Logging to {args.log_file}")
    metadata = load_metadata(args.metadata)
    runner = SoakRunner(args, metadata, logger)

    def _signal(signum, _frame):
        logger.log(f"Received signal {signum}; stopping")
        runner.stop_event.set()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)

    try:
        runner.run()
    except (RemoteCommandError, TestRunError, ValueError) as exc:
        logger.log(f"ERROR: {exc}")
        runner.stop_event.set()
    finally:
        runner.cleanup()


if __name__ == "__main__":
    main()
