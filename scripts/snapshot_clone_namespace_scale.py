#!/usr/bin/env python3
"""Scale test for namespaced snapshot-clones.

Workflow (drives the cluster via ``sbctl`` over SSH to the mgmt host and
``nvme-cli`` on the test client):

  1. Create one parent lvol, connect/mkfs.xfs/mount on the client.
  2. Fill it with --fill-size of /dev/urandom data via dd. sha256 a
     leading window (--verify-window bytes) for later comparison.
  3. Detach the parent and take one snapshot.
  4. Sequentially create --clones namespaced clones from that snapshot,
     timing each create wall-clock and recording the elapsed sample.
  5. Randomly pick --mount-fraction of the clones; for each:
        connect → mount → sha256 verify-window vs parent →
        write --modify-bytes of /dev/urandom at a random offset (forces
        COW from the clone) → sync → umount → disconnect.
  6. Sequentially delete every clone, timing each delete wall-clock.
  7. Best-effort delete the snapshot and the parent lvol.
  8. Emit summary.json + a human-readable per-bucket table for both
     phases. Buckets:  [0..100), [500..600), [1000..1100),
     [1500..1600), [2000..2100), [N-100..N) — also overall mean.

The script does NOT run any outage / churn / fio — it only measures
clone-create / clone-delete scaling and validates COW correctness.

Infrastructure (Logger, RemoteHost, LocalHost, sbctl JSON parser,
metadata loader, key-path resolver) is cribbed from
``aws_dual_node_outage_soak_mixed_churn.py``; trimmed because we don't
need outage methods, fio orchestration, or churn threads.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import random
import re
import shlex
import statistics
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


NQN_RE = re.compile(r"--nqn[ =]([A-Za-z0-9._:\-]+)")
UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
SHA256_RE = re.compile(r"\b([0-9a-f]{64})\b")


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def parse_args():
    default_metadata = Path(__file__).with_name("cluster_metadata.json")
    parser = argparse.ArgumentParser(
        description=("Scale test for namespaced snapshot-clones: 1 parent → "
                     "1 snapshot → N clones, timed in buckets, with a random "
                     "subset verified and COW-touched."))
    parser.add_argument("--metadata", default=str(default_metadata),
                        help="Path to cluster_metadata.json.")
    parser.add_argument("--ssh-key", help="Override SSH key path from metadata.")
    parser.add_argument("--pool", default="pool01",
                        help="Pool name for parent lvol create.")
    parser.add_argument("--lvol-size", default="100G",
                        help="Parent lvol size (sbctl size string).")
    parser.add_argument("--fill-size", default="20G",
                        help="Bytes of /dev/urandom to write into parent. "
                             "Suffixes K/M/G accepted.")
    parser.add_argument("--verify-window", default="256M",
                        help="Bytes at offset 0 to sha256 for parent/clone "
                             "verification. Default 256M (full-file sha is "
                             "slow over NVMe-TCP at 20G).")
    parser.add_argument("--clones", type=int, default=2500,
                        help="Number of clones to create sequentially.")
    parser.add_argument("--mount-fraction", type=float, default=0.10,
                        help="Random fraction of clones to mount+verify+COW.")
    parser.add_argument("--modify-bytes", default="4M",
                        help="Bytes of /dev/urandom to write into each "
                             "sampled clone (forces COW from the parent).")
    parser.add_argument("--mount-root", default="/mnt/snapclone_scale",
                        help="Client mount root.")
    parser.add_argument("--output-dir", default=None,
                        help="Where to write summary.json and run log. "
                             "Defaults to ./snapclone_scale_<runid>/.")
    parser.add_argument("--run-on-mgmt", action="store_true",
                        help="Run sbctl/client commands on the mgmt host "
                             "(local execution; skips SSH to mgmt).")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for the mount-fraction sampling "
                             "and COW offsets. Default: time-based.")
    parser.add_argument("--clone-name-prefix", default=None,
                        help="Prefix for clone names. Default: CLN_<runid>.")
    parser.add_argument("--snapshot-name", default=None,
                        help="Snapshot name. Default: SNAP_<runid>.")
    parser.add_argument("--parent-name", default=None,
                        help="Parent lvol name. Default: PARENT_<runid>.")
    return parser.parse_args()


def parse_size(value):
    """Parse 10K / 10M / 10G / 1024 → integer bytes."""
    if isinstance(value, int):
        return value
    s = str(value).strip()
    mul = 1
    if s and s[-1] in "KkMmGgTt":
        suf = s[-1].lower()
        s = s[:-1]
        mul = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}[suf]
    return int(float(s) * mul)


# --------------------------------------------------------------------------- #
# Metadata + logging                                                          #
# --------------------------------------------------------------------------- #

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
    ]
    seen = set()
    out = []
    for c in candidates:
        t = str(c)
        if t not in seen:
            seen.add(t)
            out.append(c)
    return out


def resolve_key_path(raw_path):
    for c in candidate_key_paths(raw_path):
        if c.exists():
            return str(c)
    raise FileNotFoundError(
        f"Unable to resolve SSH key from metadata path {raw_path!r}. "
        f"Tried: {', '.join(str(p) for p in candidate_key_paths(raw_path))}")


class Logger:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    def log(self, message):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        with self.lock:
            print(line, flush=True)
            with open(self.path, "a", encoding="utf-8") as h:
                h.write(line + "\n")


# --------------------------------------------------------------------------- #
# Remote / local command execution                                            #
# --------------------------------------------------------------------------- #

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
        last = None
        for attempt in range(1, 16):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(
                    hostname=self.hostname, username=self.user,
                    key_filename=self.key_path, timeout=15,
                    banner_timeout=15, auth_timeout=15,
                    allow_agent=False, look_for_keys=False)
                t = client.get_transport()
                if t is not None:
                    t.set_keepalive(30)
                self.client = client
                return
            except Exception as exc:
                last = exc
                self.logger.log(
                    f"{self.name}: SSH attempt {attempt}/15 failed to "
                    f"{self.hostname}: {exc}")
                time.sleep(5)
        raise RemoteCommandError(
            f"{self.name}: failed to connect to {self.hostname}: {last}")

    def run(self, command, timeout=600, check=True, label=None):
        if self.client is None:
            self.connect()
        label = label or command
        self.logger.log(f"{self.name}: RUN {label}")
        try:
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            raise RemoteCommandError(
                f"{self.name}: command failed: {label}: {exc}") from exc
        if check and rc != 0:
            raise RemoteCommandError(
                f"{self.name}: rc={rc} for {label}\nstdout: {out}\nstderr: {err}")
        return rc, out, err

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
        label = label or command
        self.logger.log(f"{self.name}: RUN {label}")
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=timeout)
        if check and proc.returncode != 0:
            raise RemoteCommandError(
                f"{self.name}: rc={proc.returncode} for {label}\n"
                f"stdout: {proc.stdout}\nstderr: {proc.stderr}")
        return proc.returncode, proc.stdout, proc.stderr

    def close(self):
        pass


# --------------------------------------------------------------------------- #
# Run state                                                                   #
# --------------------------------------------------------------------------- #

@dataclass
class CloneRecord:
    index: int
    name: str
    uuid: str = ""
    create_elapsed: float = 0.0
    create_error: str = ""
    delete_elapsed: float = 0.0
    delete_error: str = ""


@dataclass
class MountRecord:
    index: int
    clone_name: str
    sha_matched: bool = False
    sha_observed: str = ""
    cow_bytes_written: int = 0
    error: str = ""


@dataclass
class RunState:
    parent_id: str = ""
    parent_nqn: str = ""
    snapshot_id: str = ""
    parent_sha: str = ""
    clones: list = field(default_factory=list)  # list[CloneRecord]
    mounts: list = field(default_factory=list)  # list[MountRecord]


# --------------------------------------------------------------------------- #
# Bucket stats                                                                #
# --------------------------------------------------------------------------- #

BUCKETS = [
    ("first_100",  0, 100),
    ("500_600",    500, 600),
    ("1000_1100",  1000, 1100),
    ("1500_1600",  1500, 1600),
    ("2000_2100",  2000, 2100),
]


def _bucket_for_last(total):
    return ("last_100", max(0, total - 100), total)


def _summarize(samples):
    """samples: list[float]. Returns dict of count/mean/p50/p95/max."""
    if not samples:
        return {"count": 0}
    samples_sorted = sorted(samples)

    def pct(p):
        if not samples_sorted:
            return 0.0
        k = int(round((p / 100.0) * (len(samples_sorted) - 1)))
        return samples_sorted[k]

    return {
        "count": len(samples),
        "mean_s": round(statistics.mean(samples), 4),
        "p50_s":  round(pct(50), 4),
        "p95_s":  round(pct(95), 4),
        "max_s":  round(max(samples), 4),
    }


def bucket_stats(times, total):
    """times: list of (index, elapsed). Returns dict label → summary."""
    buckets = list(BUCKETS) + [_bucket_for_last(total)]
    by_label = {}
    for label, lo, hi in buckets:
        window = [el for (i, el) in times if lo <= i < hi]
        by_label[label] = _summarize(window)
    by_label["overall"] = _summarize([el for (_, el) in times])
    return by_label


# --------------------------------------------------------------------------- #
# Runner                                                                      #
# --------------------------------------------------------------------------- #

class SnapshotCloneScaleRun:
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
            self.mgmt = RemoteHost(metadata["mgmt"]["public_ip"], self.user,
                                   self.key_path, logger, "mgmt")
        client_entry = metadata["clients"][0]
        client_addr = (client_entry.get("private_ip") if args.run_on_mgmt
                       else client_entry["public_ip"])
        if not client_addr:
            client_addr = client_entry["public_ip"]
        self.client = RemoteHost(client_addr, self.user, self.key_path,
                                 logger, "client")

        self.fill_bytes = parse_size(args.fill_size)
        self.verify_bytes = parse_size(args.verify_window)
        self.modify_bytes = parse_size(args.modify_bytes)
        self.parent_name = args.parent_name or f"PARENT_{self.run_id}"
        self.snapshot_name = args.snapshot_name or f"SNAP_{self.run_id}"
        self.clone_prefix = args.clone_name_prefix or f"CLN_{self.run_id}"

        if args.seed is not None:
            random.seed(args.seed)
        self.state = RunState()

    def close(self):
        try:
            self.mgmt.close()
        except Exception:
            pass
        try:
            self.client.close()
        except Exception:
            pass

    # ------- sbctl wrapper --------------------------------------------------

    def sbctl(self, args, timeout=600, json_output=False, check=True):
        cmd = "sudo /usr/local/bin/sbctl -d " + args
        rc, out, err = self.mgmt.run(cmd, timeout=timeout, check=False,
                                     label=f"sbctl {args}")
        if check and rc != 0:
            raise RemoteCommandError(
                f"sbctl {args} rc={rc}\nstdout: {out}\nstderr: {err}")
        if not json_output:
            return rc, out, err
        for candidate in (out, err, out + "\n" + err):
            candidate = candidate.strip()
            if not candidate:
                continue
            try:
                return rc, json.loads(candidate), err
            except json.JSONDecodeError:
                pass
            decoder = json.JSONDecoder()
            final, lst, dct = [], [], []
            for start, ch in enumerate(candidate):
                if ch not in "[{":
                    continue
                try:
                    obj, end = decoder.raw_decode(candidate[start:])
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, (list, dict)):
                    continue
                if not candidate[start + end:].strip():
                    final.append(obj)
                elif isinstance(obj, list):
                    lst.append(obj)
                else:
                    dct.append(obj)
            if final:
                return rc, final[-1], err
            if lst:
                return rc, lst[-1], err
            if dct:
                return rc, dct[-1], err
        raise RemoteCommandError(f"Failed to parse JSON from sbctl {args}")

    # ------- prerequisites + node pick --------------------------------------

    def ensure_prerequisites(self):
        self.logger.log(f"Using SSH key {self.key_path}")
        self.client.run(
            "if command -v dnf >/dev/null 2>&1; then "
            "sudo dnf install -y nvme-cli xfsprogs coreutils; "
            "else sudo apt-get update && "
            "sudo apt-get install -y nvme-cli xfsprogs coreutils; fi",
            timeout=1800, label="install client packages")
        self.client.run("sudo modprobe nvme_tcp", timeout=60,
                        label="load nvme_tcp")
        self.client.run(f"sudo mkdir -p {shlex.quote(self.args.mount_root)} && "
                        f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} "
                        f"{shlex.quote(self.args.mount_root)}",
                        label="prepare mount root")

    def pick_storage_node(self):
        _, nodes, _ = self.sbctl("sn list --json", json_output=True)
        for node in nodes:
            status = str(node.get("Status", "")).lower()
            if status == "online":
                return node["UUID"]
        raise RemoteCommandError("No ONLINE storage node found")

    # ------- parent volume lifecycle ----------------------------------------

    @staticmethod
    def _extract_uuid(text):
        # sbctl `lvol add` / `snapshot add` / `snapshot clone` print the new
        # object's UUID as a standalone line; other UUIDs (the --host-id node,
        # log/INFO lines) only ever appear embedded in surrounding text. Scan
        # from the end for a line that is EXACTLY a UUID so we never capture
        # one of those (a bare search() grabbed the --host-id node UUID and
        # then connect_lvol failed with 'LVol <id> not found').
        for line in reversed((text or "").splitlines()):
            stripped = line.strip()
            if UUID_RE.fullmatch(stripped):
                return stripped
        raise RemoteCommandError(f"No standalone UUID in sbctl output: {text!r}")

    def create_parent(self):
        node_uuid = self.pick_storage_node()
        self.logger.log(f"Creating parent lvol {self.parent_name} on node "
                        f"{node_uuid}")
        _, out, _ = self.sbctl(
            f"lvol add {self.parent_name} {self.args.lvol_size} "
            f"{self.args.pool} --host-id {node_uuid}")
        self.state.parent_id = self._extract_uuid(out)
        self.logger.log(f"parent {self.parent_name} → {self.state.parent_id}")

    def _connect_lvol(self, lvol_id):
        """Run ``sbctl lvol connect`` and return (nqn, [connect_cmds])."""
        _, out, _ = self.sbctl(f"lvol connect {lvol_id}")
        cmds = [ln.strip() for ln in out.splitlines()
                if ln.strip().startswith("sudo nvme connect")]
        if not cmds:
            raise RemoteCommandError(
                f"No nvme connect command for {lvol_id}: {out!r}")
        nqn = None
        for c in cmds:
            m = NQN_RE.search(c)
            if m:
                nqn = m.group(1)
                break
        if nqn is None:
            raise RemoteCommandError(
                f"No NQN parsed from connect for {lvol_id}: {cmds!r}")
        connected = 0
        for cmd in cmds:
            try:
                self.client.run(cmd, timeout=120,
                                label=f"connect {lvol_id}")
                connected += 1
            except RemoteCommandError as exc:
                self.logger.log(f"connect failed for {lvol_id}: {exc}")
        if connected == 0:
            raise RemoteCommandError(f"No nvme path connected for {lvol_id}")
        # Give udev a moment to settle by-id symlinks.
        time.sleep(2)
        return nqn

    def _mount_lvol(self, lvol_id, mount_point, format_xfs):
        """Locate the block device, optionally mkfs.xfs, mount."""
        script_parts = [
            "set -euo pipefail",
            f"dev=$(readlink -f /dev/disk/by-id/*{lvol_id}* | head -n 1)",
            "if [ -z \"$dev\" ]; then",
            f"  echo 'no NVMe device for {lvol_id}' >&2",
            "  exit 1",
            "fi",
        ]
        if format_xfs:
            script_parts.append('sudo mkfs.xfs -f "$dev"')
        script_parts.extend([
            f"sudo mkdir -p {shlex.quote(mount_point)}",
            f'sudo mount "$dev" {shlex.quote(mount_point)}',
            f"sudo chown {shlex.quote(self.user)}:{shlex.quote(self.user)} "
            f"{shlex.quote(mount_point)}",
        ])
        script = "\n".join(script_parts) + "\n"
        self.client.run(f"bash -lc {shlex.quote(script)}", timeout=600,
                        label=f"mount {lvol_id}")

    def _umount_and_disconnect(self, mount_point, nqn):
        try:
            self.client.run(f"sync && sudo umount {shlex.quote(mount_point)}",
                            timeout=120, label=f"umount {mount_point}")
        except RemoteCommandError as exc:
            self.logger.log(f"umount {mount_point} failed (continuing): {exc}")
        try:
            self.client.run(f"sudo nvme disconnect -n {shlex.quote(nqn)}",
                            timeout=120, label=f"disconnect {nqn}",
                            check=False)
        except RemoteCommandError as exc:
            self.logger.log(f"disconnect {nqn} failed (continuing): {exc}")

    def fill_parent(self):
        mount = posixpath.join(self.args.mount_root, "parent")
        self.state.parent_nqn = self._connect_lvol(self.state.parent_id)
        self._mount_lvol(self.state.parent_id, mount, format_xfs=True)

        fill_mb = max(1, self.fill_bytes // (1024 ** 2))
        self.logger.log(f"Filling parent with {fill_mb} MiB of urandom")
        self.client.run(
            f"sudo dd if=/dev/urandom of={shlex.quote(mount)}/data "
            f"bs=1M count={fill_mb} oflag=direct status=none && sync",
            timeout=3600, label="dd fill parent")

        verify_mb = max(1, self.verify_bytes // (1024 ** 2))
        self.logger.log(f"sha256 of first {verify_mb} MiB for verification")
        _, out, _ = self.client.run(
            f"sudo dd if={shlex.quote(mount)}/data bs=1M count={verify_mb} "
            f"iflag=direct status=none | sha256sum",
            timeout=600, label="sha256 parent")
        m = SHA256_RE.search(out)
        if not m:
            raise RemoteCommandError(f"No sha256 in: {out!r}")
        self.state.parent_sha = m.group(1)
        self.logger.log(f"parent sha256[:{verify_mb}MiB]={self.state.parent_sha}")

        self._umount_and_disconnect(mount, self.state.parent_nqn)

    def snapshot_parent(self):
        self.logger.log(f"Taking snapshot {self.snapshot_name} of parent")
        _, out, _ = self.sbctl(
            f"snapshot add {self.state.parent_id} {self.snapshot_name}")
        self.state.snapshot_id = self._extract_uuid(out)
        self.logger.log(f"snapshot → {self.state.snapshot_id}")

    # ------- clone-create loop ----------------------------------------------

    def create_clones(self):
        self.logger.log(f"Creating {self.args.clones} clones sequentially "
                        f"(namespaced=true)")
        wall_start = time.monotonic()
        for i in range(self.args.clones):
            name = f"{self.clone_prefix}_{i:05d}"
            rec = CloneRecord(index=i, name=name)
            t0 = time.monotonic()
            try:
                rc, out, err = self.sbctl(
                    f"snapshot clone {self.state.snapshot_id} {name} "
                    f"--namespaced true", check=False)
                elapsed = time.monotonic() - t0
                if rc != 0:
                    rec.create_error = (err or out).strip()[:400]
                else:
                    try:
                        rec.uuid = self._extract_uuid(out)
                    except RemoteCommandError as exc:
                        rec.create_error = f"no uuid parsed: {exc}"
            except RemoteCommandError as exc:
                elapsed = time.monotonic() - t0
                rec.create_error = str(exc)[:400]
            rec.create_elapsed = elapsed
            self.state.clones.append(rec)
            if (i + 1) % 100 == 0 or rec.create_error:
                self.logger.log(
                    f"clone {i+1}/{self.args.clones} {name} "
                    f"elapsed={elapsed:.3f}s "
                    f"err={'Y' if rec.create_error else 'n'}")
        total = time.monotonic() - wall_start
        ok = sum(1 for r in self.state.clones if not r.create_error)
        self.logger.log(
            f"clone-create phase done: {ok}/{self.args.clones} ok, "
            f"wall={total:.1f}s")

    # ------- random mount + verify + COW ------------------------------------

    def mount_verify_cow(self):
        if not self.state.clones:
            return
        ok_clones = [r for r in self.state.clones if r.uuid]
        if not ok_clones:
            self.logger.log("no successful clones to mount-verify; skipping")
            return
        sample_n = max(1, int(len(ok_clones) * self.args.mount_fraction))
        sample = random.sample(ok_clones, min(sample_n, len(ok_clones)))
        sample.sort(key=lambda r: r.index)
        self.logger.log(f"Mount-verify-COW on {len(sample)} of "
                        f"{len(ok_clones)} clones")

        verify_mb = max(1, self.verify_bytes // (1024 ** 2))
        modify_b = self.modify_bytes
        for rec in sample:
            mrec = MountRecord(index=rec.index, clone_name=rec.name)
            mount = posixpath.join(self.args.mount_root, f"cl_{rec.index:05d}")
            nqn = None
            try:
                nqn = self._connect_lvol(rec.uuid)
                self._mount_lvol(rec.uuid, mount, format_xfs=False)
                _, out, _ = self.client.run(
                    f"sudo dd if={shlex.quote(mount)}/data bs=1M "
                    f"count={verify_mb} iflag=direct status=none | sha256sum",
                    timeout=600, label=f"sha256 {rec.name}")
                m = SHA256_RE.search(out)
                if not m:
                    raise RemoteCommandError(f"no sha256: {out!r}")
                mrec.sha_observed = m.group(1)
                mrec.sha_matched = (mrec.sha_observed == self.state.parent_sha)
                # COW write at a random offset inside the file (block-aligned).
                file_size_blocks = max(1, self.fill_bytes // (1024 ** 2))
                cow_blocks = max(1, modify_b // (1024 ** 2))
                max_offset_blocks = max(0, file_size_blocks - cow_blocks)
                offset_blocks = (random.randint(0, max_offset_blocks)
                                 if max_offset_blocks else 0)
                self.client.run(
                    f"sudo dd if=/dev/urandom of={shlex.quote(mount)}/data "
                    f"bs=1M count={cow_blocks} seek={offset_blocks} "
                    f"conv=notrunc oflag=direct status=none && sync",
                    timeout=600, label=f"COW {rec.name}")
                mrec.cow_bytes_written = cow_blocks * 1024 ** 2
            except RemoteCommandError as exc:
                mrec.error = str(exc)[:400]
                self.logger.log(f"mount/verify/COW failed for {rec.name}: {exc}")
            finally:
                if nqn is not None:
                    self._umount_and_disconnect(mount, nqn)
            self.state.mounts.append(mrec)

        matched = sum(1 for m in self.state.mounts if m.sha_matched)
        self.logger.log(f"verify: {matched}/{len(self.state.mounts)} clones "
                        f"matched parent sha256[:{verify_mb}MiB]")

    # ------- clone-delete loop ----------------------------------------------

    def delete_clones(self):
        self.logger.log(f"Deleting {len(self.state.clones)} clones sequentially")
        wall_start = time.monotonic()
        for i, rec in enumerate(self.state.clones):
            if not rec.uuid:
                continue  # nothing to delete
            t0 = time.monotonic()
            try:
                rc, out, err = self.sbctl(
                    f"lvol delete {rec.uuid} --force", check=False)
                rec.delete_elapsed = time.monotonic() - t0
                if rc != 0:
                    rec.delete_error = (err or out).strip()[:400]
            except RemoteCommandError as exc:
                rec.delete_elapsed = time.monotonic() - t0
                rec.delete_error = str(exc)[:400]
            if (i + 1) % 100 == 0 or rec.delete_error:
                self.logger.log(
                    f"delete {i+1}/{len(self.state.clones)} {rec.name} "
                    f"elapsed={rec.delete_elapsed:.3f}s "
                    f"err={'Y' if rec.delete_error else 'n'}")
        total = time.monotonic() - wall_start
        ok = sum(1 for r in self.state.clones if r.uuid and not r.delete_error)
        attempted = sum(1 for r in self.state.clones if r.uuid)
        self.logger.log(f"clone-delete phase done: {ok}/{attempted} ok, "
                        f"wall={total:.1f}s")

    # ------- cleanup --------------------------------------------------------

    def cleanup(self):
        if self.state.snapshot_id:
            try:
                self.sbctl(
                    f"snapshot delete {self.state.snapshot_id} --force",
                    check=False)
            except Exception as exc:
                self.logger.log(f"snapshot delete failed: {exc}")
        if self.state.parent_id:
            try:
                self.sbctl(f"lvol delete {self.state.parent_id} --force",
                           check=False)
            except Exception as exc:
                self.logger.log(f"parent delete failed: {exc}")

    # ------- summary --------------------------------------------------------

    def write_summary(self, output_dir):
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        n = len(self.state.clones)
        create_times = [(r.index, r.create_elapsed)
                        for r in self.state.clones if not r.create_error]
        delete_times = [(r.index, r.delete_elapsed)
                        for r in self.state.clones
                        if r.uuid and not r.delete_error]
        summary = {
            "run_id": self.run_id,
            "args": vars(self.args),
            "parent": {
                "id": self.state.parent_id,
                "name": self.parent_name,
                "sha256_verify_window": self.state.parent_sha,
                "verify_window_bytes": self.verify_bytes,
                "fill_bytes": self.fill_bytes,
            },
            "snapshot": {
                "id": self.state.snapshot_id,
                "name": self.snapshot_name,
            },
            "clones": {
                "attempted": n,
                "create_ok": sum(1 for r in self.state.clones
                                 if not r.create_error),
                "create_errors": sum(1 for r in self.state.clones
                                     if r.create_error),
                "delete_attempted": sum(1 for r in self.state.clones if r.uuid),
                "delete_ok": sum(1 for r in self.state.clones
                                 if r.uuid and not r.delete_error),
                "delete_errors": sum(1 for r in self.state.clones
                                     if r.uuid and r.delete_error),
            },
            "verify": {
                "sampled": len(self.state.mounts),
                "sha_matched": sum(1 for m in self.state.mounts if m.sha_matched),
                "sha_mismatched": sum(1 for m in self.state.mounts
                                      if m.sha_observed and not m.sha_matched),
                "errors": sum(1 for m in self.state.mounts if m.error),
                "cow_bytes_total": sum(m.cow_bytes_written
                                       for m in self.state.mounts),
            },
            "create_phase": bucket_stats(create_times, n),
            "delete_phase": bucket_stats(delete_times, n),
        }
        json_path = out_dir / "summary.json"
        with open(json_path, "w", encoding="utf-8") as h:
            json.dump(summary, h, indent=2, default=str)
        self.logger.log(f"summary → {json_path}")

        # Per-clone CSV for downstream plotting.
        csv_path = out_dir / "per_clone.csv"
        with open(csv_path, "w", encoding="utf-8") as h:
            h.write("index,name,uuid,create_elapsed_s,create_error,"
                    "delete_elapsed_s,delete_error\n")
            for r in self.state.clones:
                h.write(f"{r.index},{r.name},{r.uuid},{r.create_elapsed:.4f},"
                        f"{1 if r.create_error else 0},"
                        f"{r.delete_elapsed:.4f},"
                        f"{1 if r.delete_error else 0}\n")
        self.logger.log(f"per-clone CSV → {csv_path}")

        # Human-readable bucket table.
        self.logger.log("=" * 78)
        self.logger.log("CREATE phase per-bucket stats:")
        self._log_bucket_table(summary["create_phase"])
        self.logger.log("DELETE phase per-bucket stats:")
        self._log_bucket_table(summary["delete_phase"])
        self.logger.log("=" * 78)
        self.logger.log(
            f"verify: {summary['verify']['sha_matched']}/"
            f"{summary['verify']['sampled']} match, "
            f"{summary['verify']['sha_mismatched']} mismatch, "
            f"{summary['verify']['errors']} errors")

    def _log_bucket_table(self, by_label):
        header = f"  {'bucket':<14} {'count':>6} {'mean_s':>10} {'p50_s':>10} {'p95_s':>10} {'max_s':>10}"
        self.logger.log(header)
        order = [b[0] for b in BUCKETS] + ["last_100", "overall"]
        for label in order:
            row = by_label.get(label, {})
            count = row.get("count", 0)
            if count == 0:
                self.logger.log(
                    f"  {label:<14} {0:>6} {'—':>10} {'—':>10} {'—':>10} {'—':>10}")
                continue
            self.logger.log(
                f"  {label:<14} {count:>6} {row['mean_s']:>10.4f} "
                f"{row['p50_s']:>10.4f} {row['p95_s']:>10.4f} "
                f"{row['max_s']:>10.4f}")


# --------------------------------------------------------------------------- #
# main                                                                        #
# --------------------------------------------------------------------------- #

def main():
    args = parse_args()
    output_dir = args.output_dir or f"snapclone_scale_{time.strftime('%Y%m%d_%H%M%S')}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logger = Logger(str(Path(output_dir) / "run.log"))
    logger.log(f"args: {vars(args)}")

    metadata = load_metadata(args.metadata)
    run = SnapshotCloneScaleRun(args, metadata, logger)
    try:
        run.ensure_prerequisites()
        run.create_parent()
        run.fill_parent()
        run.snapshot_parent()
        run.create_clones()
        run.mount_verify_cow()
        run.delete_clones()
    finally:
        try:
            run.write_summary(output_dir)
        except Exception:
            logger.log("write_summary failed; will still attempt cleanup")
        try:
            run.cleanup()
        except Exception as exc:
            logger.log(f"cleanup error: {exc}")
        run.close()


if __name__ == "__main__":
    sys.exit(main())
