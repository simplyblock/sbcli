#!/usr/bin/env python3
"""
fio_migration_test.py — simplyblock live-migration I/O stress / correlation test.

What it does
------------
1. Creates two XFS StorageClasses cloned from the live pool StorageClass
   (`simplyblock-default-simplyblock-cluster-pool1`, provisioner csi.simplyblock.io)
   with `csi.storage.k8s.io/fstype: xfs`:
     * a single-namespace class (one NVMe subsystem per volume), and
     * a namespaced class with `max_namespace_per_subsys: 3` (--ns-per-subsys), whose
       volumes share a subsystem with up to two siblings.
   Volumes sharing a subsystem always migrate together, as one batch — so the test
   mixes both kinds and verifies whole-subsystem movement (see 3./5.).
2. Provisions N (default 10) single-namespace + M (default 6, --ns-pods) namespaced
   10Gi PVCs and one fio pod each.
   Each pod mounts the simplyblock volume (XFS) and runs fio for 10 minutes with:
       ioengine=libaio, direct=1, iodepth=1, numjobs=1 (override with
       --iodepth/--numjobs; see cmd/simplyblock-rebalancer/main.go::measure
       for the direct/libaio reference)
   fio emits per-second IOPS / latency / bandwidth logs plus a final JSON summary.
3. While the pods run, repeatedly picks a pod and migrates its volume to a
   *random storage node other than the one it currently lives on* by creating a
   `VolumeMigration` CR — one migration at a time. Picks alternate between the
   single-namespace and namespaced volumes so both kinds are exercised throughout the
   run, interleaved rather than in phases. The current storage node is
   resolved authoritatively before each pick by running `sbctl volume list --json`
   inside a webappapi pod (simplyblock namespace) and matching the PV's logical-volume
   UUID; the sbctl Hostname (e.g. vm19_4424) is mapped to a storage-node UUID via the
   `simplyblock-rebalancer-<nodeUUID>` benchmark volumes. Start and stop of every
   migration are timestamped and logged for later correlation. Before each migration,
   with a configurable probability (default 15%, --snapshot-chance) the volume is first
   snapshotted via a Kubernetes VolumeSnapshot CR, so the ensuing migration must carry
   the snapshot. The snapshot's backend id is validated via `sbctl snapshot list` both
   right after creation and again after the migration finishes (a migration that drops
   its snapshots is a failure). Snapshotting only happens pre-migration, and a volume
   already carrying a snapshot is snapshotted again regardless.
4. Continuously monitors pod health. After the run it pulls every pod's fio logs and
   correlates the per-second IOPS timeline against the migration windows.
5. Verifies every migration landed correctly. A subsystem moves as a unit, so for a
   namespaced volume the check covers *every* volume sharing its subsystem, not just
   the one named in the CR: after a Completed migration all members must sit on the
   target, after a Failed/Timeout one all must still sit on the source, and a
   half-moved subsystem (some members on the target, some not) is reported as its own,
   worst-case irregularity. Subsystem membership is re-read afterwards as well — a
   migration must not split a subsystem or move a volume into another one. The
   operator's own view is cross-checked too: `status.subsystemNQN` and
   `status.memberCount` on the CR must match the subsystem the test resolved via sbctl.
6. Verifies the *host* side of every migration, not only the backend placement. For the
   duration of each migration the NVMe state of the subsystem is sampled on every node
   that consumes it — controller address, controller state and each namespace's ANA
   state — read straight from that host's sysfs through its CSI node-plugin pod (which
   already mounts the host /sys). From those samples three things must hold:
     * the target must not start serving before cutover (a path that gains accessible
       namespaces mid-migration lets reads land on a target that has not got the data),
     * after a Completed migration the target must serve *every* namespace of the
       subsystem on every consuming node — the half-moved batch, seen from the host,
     * no namespace may be left without an accessible path for longer than
       --ana-stall-crit seconds (an I/O stall the fio timeline shows but cannot explain).
   The samples are written per migration to ana/<migration>.csv for correlation with the
   per-second fio time series.
7. Splits migrations by target discriminator (--target-policy): whether the node migrated
   *to* also runs a pod consuming that subsystem. Both cases are exercised by default
   (alternating), and the report accounts for each separately — a fault that only occurs
   when the target hosts a consumer otherwise reads as intermittent.

Failure criterion
------------------
Losing I/O is a TOTAL FAIL. Any of the following is treated as I/O loss and makes the
script exit non-zero, with the offending interval logged explicitly:
  * a per-second sample where total IOPS drops to 0 during the timed run,
  * a fio job ending in error (its JSON "error" errno; note this is an errno, not a count),
  * a fio pod leaving Running / restarting / failing before its planned completion.

Ranked above all of those is a DATA INTEGRITY failure — fio reading back a block whose
checksum does not match what it wrote. It is reported separately and never folded into the
I/O-error bucket: the read succeeded, so nothing surfaces as an error anywhere else in the
stack. It is detected two ways, either of which is sufficient: the "verify: bad magic
header" / md5 mismatch lines in the pod's log, and fio's own errno EILSEQ (84) on the job,
which survives even if the log was truncated.
Each I/O-loss event is annotated with whether it overlaps a migration window. Since a
namespaced migration moves sibling volumes too, an outage on a *sibling's* pod counts
the same as one on the migrated volume's pod — that is the point of the mixed run.

Requirements: python3, kubectl (current context pointed at the cluster), the
simplyblock operator running in `default`. fio is installed into the pods at runtime
(alpine `apk add fio`).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import random
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone

# ── configuration / constants ───────────────────────────────────────────────────

NAMESPACE = "default"
SIMPLYBLOCK_NAMESPACE = "simplyblock"  # where the webappapi pods run
WEBAPPAPI_MATCH = "webappapi"          # substring used to find a webappapi pod
SOURCE_STORAGECLASS = "simplyblock-default-simplyblock-cluster-pool1"
XFS_STORAGECLASS = SOURCE_STORAGECLASS + "-xfs"
# Namespaced variant: its volumes share one NVMe subsystem with up to
# --ns-per-subsys siblings and therefore migrate as a batch, all at once.
NS_STORAGECLASS = XFS_STORAGECLASS + "-ns"
PARAM_MAX_NS_PER_SUBSYS = "max_namespace_per_subsys"
NS_PER_SUBSYS = 3
STORAGENODE_CR = "simplyblock-node"  # StorageNodeSet CR in `default`
FIO_IMAGE = "alpine:3"

# fio knobs (the direct=1 / libaio pair mirrors rebalancer measure())
FIO_IOENGINE = "libaio"
FIO_DIRECT = 1
FIO_IODEPTH = 1
FIO_NUMJOBS = 1
FIO_BS = "4k"
FIO_RWMIXREAD = 70  # randrw read percentage
# fio buffers --write_*_log to memory and only writes them at job end, and with no TTY
# its default --eta=auto prints nothing — so without this fio is silent in `kubectl logs`
# for the whole run. --eta=always + --eta-newline forces a periodic status line to stdout
# (every N seconds) without corrupting the --output JSON report.
FIO_ETA_NEWLINE_SEC = 5

# fio reports the errno of a job's last error in its JSON "error" field. Only the ones
# this test can actually produce are named; anything else is reported as a bare errno.
#
# EILSEQ is the one that must never be read as a generic I/O failure: fio uses it for a
# data-integrity mismatch, i.e. the block it read back did not match the checksum it wrote.
# That is silent corruption — the read *succeeded* — and it ranks above every transport
# error, so it is classified separately everywhere below.
# How many individual mismatches to spell out per pod in the log. With verify_fatal=0 a
# corrupting migration can produce thousands; beyond a handful they add nothing a count and
# a per-migration aggregate do not.
CORRUPTION_DETAIL_LIMIT = 5

FIO_ERRNO_DATA_INTEGRITY = 84  # EILSEQ, as set by fio's verify path
FIO_ERRNO_MEANING = {
    5: "EIO — I/O error",
    28: "ENOSPC — out of space",
    30: "EROFS — read-only file system",
    FIO_ERRNO_DATA_INTEGRITY:
        "EILSEQ — DATA INTEGRITY: checksum mismatch on read-back (fio verify)",
}

API_GROUP = "storage.simplyblock.io/v1alpha1"

# CSI snapshotting: while migrating, randomly snapshot a volume *before* migrating it
# (via a Kubernetes VolumeSnapshot CR) so the ensuing migration must also carry the
# snapshot. See run_one_migration().
SNAPSHOT_APIVERSION = "snapshot.storage.k8s.io/v1"
SNAPSHOTCLASS_NAME = "simplyblock-fio-migration-snapclass"
SNAPSHOT_CHANCE = 0.15  # probability of snapshotting the volume before each migration

# ANA-state sampling: the host's own view of a migrating subsystem, read from sysfs on
# every node that consumes it. The CSI node plugin already mounts the host's /sys, so
# its pods are used as the window onto each node — no extra privileged pod needed.
CSI_NODE_MATCH = "csi-node"      # substring identifying the CSI node-plugin DaemonSet pods
CSI_NODE_CONTAINER = "csi-node"  # container within them that has the host's /sys mounted
# One line per controller of the subsystem: "<traddr>:<trsvcid>|<state>|<nsid>:<ana>,…".
# Controllers are the subsystem's directory entries that carry a `state` file; the ANA
# state lives per *path* (nvmeXcYnZ), which is why it is read from below the controller
# rather than from the multipath head device (whose ana_state file does not exist).
ANA_PROBE_SCRIPT = r'''
for s in /sys/class/nvme-subsystem/nvme-subsys*; do
  [ -r "$s/subsysnqn" ] || continue
  [ "$(cat "$s/subsysnqn")" = "$NQN" ] || continue
  for c in "$s"/nvme*; do
    [ -r "$c/state" ] || continue
    ad=$(cat "$c/address" 2>/dev/null)
    tr=$(echo "$ad" | sed -n 's/.*traddr=\([^,]*\).*/\1/p')
    sv=$(echo "$ad" | sed -n 's/.*trsvcid=\([^,]*\).*/\1/p')
    ana=""
    for p in "$c"/nvme*c*n*; do
      [ -r "$p/ana_state" ] || continue
      ana="$ana,$(cat "$p/nsid" 2>/dev/null):$(cat "$p/ana_state" 2>/dev/null)"
    done
    echo "$tr:$sv|$(cat "$c/state")|${ana#,}"
  done
done
'''
# ANA states the kernel will route I/O to. Everything else (inaccessible,
# persistent-loss, change) parks the path.
ANA_ACCESSIBLE = ("optimized", "non-optimized", "nonoptimized")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── logging ──────────────────────────────────────────────────────────────────────

class Logger:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._fh = open(path, "a", buffering=1)

    def log(self, level: str, msg: str) -> None:
        line = f"{iso(now_utc())} [{level:8}] {msg}"
        with self._lock:
            print(line, flush=True)
            self._fh.write(line + "\n")

    def info(self, m): self.log("INFO", m)
    def warn(self, m): self.log("WARN", m)
    def error(self, m): self.log("ERROR", m)
    def crit(self, m): self.log("CRITICAL", m)
    def event(self, m): self.log("EVENT", m)


# ── kubectl plumbing ──────────────────────────────────────────────────────────────

def kubectl(args: list[str], *, input_str: str | None = None, check: bool = True,
            timeout: int = 120) -> subprocess.CompletedProcess:
    cmd = ["kubectl", "-n", NAMESPACE] + args
    return subprocess.run(
        cmd, input=input_str, capture_output=True, text=True, check=check,
        timeout=timeout,
    )


def kubectl_apply(manifest: str) -> None:
    kubectl(["apply", "-f", "-"], input_str=manifest)


def kubectl_json(args: list[str], check: bool = True) -> dict:
    cp = kubectl(args + ["-o", "json"], check=check)
    if cp.returncode != 0:
        return {}
    return json.loads(cp.stdout)


# ── data structures ────────────────────────────────────────────────────────────────

@dataclass
class MigrationRecord:
    name: str
    pod: str
    pvc: str
    pv: str
    target: str
    vol: str = ""   # simplyblock logical-volume UUID (as referenced by webappapi errors)
    source: str = ""
    start: datetime = field(default_factory=now_utc)
    end: datetime | None = None
    phase: str = ""
    error: str = ""
    # The NQN the whole subsystem is migrated under, and the PVs sharing it — for a
    # namespaced volume the migration moves every one of them, so all are verified.
    nqn: str = ""
    group_pvs: list[str] = field(default_factory=list)
    pre_snapshot: str = ""       # VolumeSnapshot CR created on this volume right before migrating it
    pre_snapshot_id: str = ""    # backend snapshot UUID (from the CSI VolumeSnapshotContent handle)
    # snapshot must resolve via `sbctl snapshot list` both right after creation and again
    # after the migration finishes (a migration must carry its snapshots, not drop them).
    snapshot_created_ok: bool | None = None   # resolved right after creation
    snapshot_post_ok: bool | None = None      # still resolved after the migration
    snapshot_verify_msg: str = ""
    # post-migration verification (real primary node via sbctl vs expectation):
    #   Completed -> primary must be target; Failed/Timeout -> primary must stay source.
    # For a namespaced volume this covers every member of the subsystem: member_nodes
    # maps each member PV to the node it really sits on afterwards, and a subsystem
    # left half-moved (some members on the target, some on the source) is flagged as
    # its own failure mode via split_group.
    actual_node: str = ""        # real primary node UUID after the migration (sbctl)
    member_nodes: dict[str, str] = field(default_factory=dict)
    split_group: bool = False
    verify_ok: bool | None = None  # True=matches, False=irregularity, None=could not verify
    verify_msg: str = ""
    # Cross-check of the operator's own view against the subsystem resolved via sbctl:
    # status.subsystemNQN / status.memberCount on the CR.
    cr_nqn: str = ""
    cr_members: int | None = None
    # The backend migration id the CR was given. Empty means the migration was never
    # created (the submit itself failed), in which case the CR carries no subsystem
    # either — that is not a disagreement, it is a create failure.
    cr_migration_uuid: str = ""
    cr_match_ok: bool | None = None
    cr_match_msg: str = ""
    # Target-selection discriminator: whether the node this subsystem was migrated *to*
    # also runs a pod consuming it. That case needs the consuming host to join the
    # subsystem on a node that is simultaneously becoming its target, and is the one
    # this run separates out on purpose (--target-policy).
    target_has_consumer: bool | None = None
    target_consumers: list[str] = field(default_factory=list)
    target_policy: str = ""       # the policy in force for this pick
    target_policy_ok: bool = True  # False when no candidate node satisfied it
    # Host-side ANA sampling, taken on every consuming node for the duration of the
    # migration (see AnaSampler). The samples are the evidence for whether the target
    # started serving before cutover and whether it served everything afterwards.
    ana_samples: list["AnaSample"] = field(default_factory=list)
    ana_csv: str = ""
    ana_ok: bool | None = None    # True=paths behaved, False=violation, None=not sampled
    ana_msgs: list[str] = field(default_factory=list)
    ana_stall_s: float = 0.0      # longest window with no accessible path for some namespace
    # The cutover as the *hosts* saw it (per node): the first instant a target path was
    # optimized while no source path was. The CR reaching Completed trails this by tens of
    # seconds, so this — not rec.end — is the instant to correlate I/O against.
    ana_cutover: dict[str, str] = field(default_factory=dict)
    ana_cr_lag_s: float = 0.0     # how far the CR's Completed trailed the observed cutover
    # The CR phase as last polled, so each sample can be stamped with the phase in force
    # when it was taken (`phase` itself is only set once the migration goes terminal).
    live_phase: str = ""


@dataclass
class AnaSample:
    """One controller's state on one consuming node at one instant, as the host sees it."""
    ts: datetime
    node: str            # Kubernetes node the sample was taken on
    phase: str           # the migration CR's phase at that instant
    address: str         # "<traddr>:<trsvcid>"
    state: str           # controller state: live / connecting / resetting / …
    ana: dict[int, str] = field(default_factory=dict)  # nsid -> ANA state

    def accessible_nsids(self) -> set[int]:
        return {n for n, a in self.ana.items() if a in ANA_ACCESSIBLE}


@dataclass
class PodHealthEvent:
    ts: datetime
    pod: str
    detail: str


# ── the test runner ────────────────────────────────────────────────────────────────

class FioMigrationTest:
    def __init__(self, args, log: Logger, outdir: str):
        self.a = args
        self.log = log
        self.outdir = outdir
        self.run_id = f"fiomig-{int(time.time())}"
        self.pods: list[str] = []
        self.pvcs: list[str] = []
        self.pv_of: dict[str, str] = {}        # pod -> pv name
        self.pvc_of: dict[str, str] = {}       # pod -> pvc name
        self.kind_of: dict[str, str] = {}      # pod -> "single" | "namespaced" (StorageClass kind)
        self.volume_uuid_of: dict[str, str] = {}  # pv -> simplyblock logical-volume UUID
        self.placement: dict[str, str] = {}    # pv -> current storage node uuid
        # NVMe subsystem grouping, resolved from the backend (never assumed): which
        # subsystem each volume lives in and which volumes share it. A namespaced
        # volume's group has >1 member and migrates as a whole.
        self.nqn_of: dict[str, str] = {}          # pv -> subsystem NQN
        self.ns_id_of: dict[str, int] = {}        # pv -> namespace id within the subsystem
        self.subsystem_pvs: dict[str, list[str]] = {}  # NQN -> [pv, ...] (by ascending ns id)
        self.nodes: list[str] = []
        self.node_host: dict[str, str] = {}    # node uuid -> k8s hostname (StorageNode CR)
        self.sbctl_host_to_node: dict[str, str] = {}  # sbctl Hostname (vmNN_PORT) -> node uuid
        self.node_ip: dict[str, str] = {}      # node uuid -> management IP (== NVMe traddr)
        self.csi_node_pod_of: dict[str, str] = {}  # k8s node -> CSI node-plugin pod
        self._pod_node_of: dict[str, str] = {}    # fio pod -> k8s node
        self._ana_probe_errors: int = 0
        self._ana_stop: threading.Event = threading.Event()
        self._ana_thread: threading.Thread | None = None
        self._webappapi_pod: str = ""
        self._cluster_uuid: str = ""
        self.migrations: list[MigrationRecord] = []
        self.snapshots: list[str] = []          # VolumeSnapshot CR names created (for cleanup)
        self.snapshot_class: str = ""           # VolumeSnapshotClass bound to the simplyblock CSI driver
        self.health_events: list[PodHealthEvent] = []
        self._stop_monitor = threading.Event()
        self._monitor_thread: threading.Thread | None = None
        self._fio_finished = threading.Event()

    # ---- setup ------------------------------------------------------------------

    def discover_nodes(self) -> None:
        cr = kubectl_json(["get", "storagenodeset", STORAGENODE_CR])
        nodes = cr.get("status", {}).get("nodes", [])
        for n in nodes:
            uuid = n.get("uuid")
            if not uuid:
                continue
            healthy = n.get("health", False) and (n.get("status") == "online")
            if healthy:
                self.nodes.append(uuid)
                self.node_host[uuid] = n.get("hostname", "?")
        if len(self.nodes) < 2:
            raise SystemExit(
                f"need >=2 healthy storage nodes to migrate between, found {len(self.nodes)}"
            )
        self.log.info(f"healthy storage nodes ({len(self.nodes)}):")
        for u in self.nodes:
            self.log.info(f"    {u}  ({self.node_host[u]})")

    def ensure_storageclasses(self) -> None:
        """(Re)create both XFS StorageClasses from the live pool StorageClass: the
        single-namespace one and the namespaced one (max_namespace_per_subsys =
        --ns-per-subsys), whose volumes share a subsystem and migrate as a batch.

        Each SC is always deleted and recreated so a stale one left over from a
        previous run can never be reused, and its `cluster_id` is forced to the live
        cluster from `sbctl cluster list` — never copied blindly from the source SC,
        which may still carry a dead cluster id after a reinstall.
        """
        src = kubectl_json(["get", "sc", SOURCE_STORAGECLASS], check=False)
        if not src:
            raise SystemExit(
                f"source StorageClass {SOURCE_STORAGECLASS} not found — the operator has "
                "not created it yet. This usually means the Pool is not Active (e.g. its "
                "create is stuck retrying on the backend), so no StorageClass was provisioned. "
                "Check: kubectl -n default get pool pool1 -o jsonpath='{.status}'")

        cluster_uuid = self.sbctl_cluster_uuid()
        base = dict(src.get("parameters", {}))
        stale = base.get("cluster_id")
        if stale and stale != cluster_uuid:
            self.log.warn(f"source SC {SOURCE_STORAGECLASS} carries stale cluster_id "
                          f"{stale}; overriding with live cluster {cluster_uuid}")
        base["cluster_id"] = cluster_uuid
        base["csi.storage.k8s.io/fstype"] = "xfs"
        # A single-namespace class must say so explicitly rather than inherit whatever
        # the source SC (or the CSI default) carries — otherwise the "single" half of
        # the mix could silently be namespaced too and the test would compare like
        # with like.
        base.pop(PARAM_MAX_NS_PER_SUBSYS, None)

        self._apply_storageclass(src, XFS_STORAGECLASS, dict(base, **{PARAM_MAX_NS_PER_SUBSYS: "1"}))
        self.log.info(f"(re)created single-namespace StorageClass {XFS_STORAGECLASS} "
                      f"(fstype=xfs, {PARAM_MAX_NS_PER_SUBSYS}=1, cluster_id={cluster_uuid})")
        if self.a.ns_pods > 0:
            self._apply_storageclass(src, NS_STORAGECLASS,
                                     dict(base, **{PARAM_MAX_NS_PER_SUBSYS: str(self.a.ns_per_subsys)}))
            self.log.info(f"(re)created namespaced StorageClass {NS_STORAGECLASS} "
                          f"(fstype=xfs, {PARAM_MAX_NS_PER_SUBSYS}={self.a.ns_per_subsys}, "
                          f"cluster_id={cluster_uuid})")

    @staticmethod
    def _apply_storageclass(src: dict, name: str, params: dict) -> None:
        kubectl(["delete", "sc", name, "--ignore-not-found"], check=False)
        kubectl_apply(json.dumps({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": name,
                         "labels": {"app.kubernetes.io/created-by": "fio-migration-test"}},
            "provisioner": src.get("provisioner", "csi.simplyblock.io"),
            "parameters": params,
            "reclaimPolicy": src.get("reclaimPolicy", "Delete"),
            "volumeBindingMode": src.get("volumeBindingMode", "WaitForFirstConsumer"),
            "allowVolumeExpansion": src.get("allowVolumeExpansion", True),
        }))

    def ensure_snapshot_class(self) -> str:
        """Find (or create) a VolumeSnapshotClass bound to the simplyblock CSI driver and
        cache its name. Snapshots during the run are taken via VolumeSnapshot CRs, which
        need a VolumeSnapshotClass for the same driver as the pool StorageClass."""
        driver = "csi.simplyblock.io"
        src = kubectl_json(["get", "sc", SOURCE_STORAGECLASS], check=False)
        if src:
            driver = src.get("provisioner", driver)
        existing = kubectl_json(["get", "volumesnapshotclass"], check=False)
        for item in existing.get("items", []):
            if item.get("driver") == driver:
                self.snapshot_class = item["metadata"]["name"]
                self.log.info(f"using existing VolumeSnapshotClass {self.snapshot_class} "
                              f"(driver={driver})")
                return self.snapshot_class
        vsc = {
            "apiVersion": SNAPSHOT_APIVERSION,
            "kind": "VolumeSnapshotClass",
            "metadata": {"name": SNAPSHOTCLASS_NAME,
                         "labels": {"app.kubernetes.io/created-by": "fio-migration-test"}},
            "driver": driver,
            "deletionPolicy": "Delete",
        }
        kubectl_apply(json.dumps(vsc))
        self.snapshot_class = SNAPSHOTCLASS_NAME
        self.log.info(f"created VolumeSnapshotClass {SNAPSHOTCLASS_NAME} (driver={driver})")
        return self.snapshot_class

    def fio_command(self) -> str:
        # fio always pre-writes ("lays out") a file-backed target before random I/O, and
        # there's no way to skip it for a filesystem file (only a raw block volume would).
        # We don't need a large working set — just continuous I/O to detect loss — so keep
        # the file small (default 1 GiB) and the layout finishes in seconds, not minutes.
        # Bounded to leave headroom under the volume's xfs filesystem.
        file_gb = max(1, min(self.a.file_size_gb, self.a.volume_size_gb - 2))
        file_size = f"{file_gb}G"
        runtime = self.a.runtime
        # data file lives on the simplyblock XFS volume (/data); fio logs go to a
        # separate emptyDir (/logs) so log collection never depends on volume health.
        fio_args = [
            "fio",
            "--name=fiotest",
            "--filename=/data/fiotest",
            f"--size={file_size}",
            f"--ioengine={FIO_IOENGINE}",
            f"--direct={FIO_DIRECT}",
            "--rw=randrw",
            f"--rwmixread={FIO_RWMIXREAD}",
            f"--bs={FIO_BS}",
            f"--iodepth={self.a.iodepth}",
            f"--numjobs={self.a.numjobs}",
            "--group_reporting",
            "--time_based",
            f"--runtime={runtime}",
            "--continue_on_error=all",   # do not die on EIO: record errors instead
            "--percentile_list=50:95:99:99.9",
            "--write_iops_log=/logs/iops",
            "--write_lat_log=/logs/lat",
            "--write_bw_log=/logs/bw",
            "--log_avg_msec=1000",       # one sample per second
            # periodic live status to stdout (visible in `kubectl logs`) without
            # corrupting the JSON report written to --output
            "--eta=always",
            f"--eta-newline={FIO_ETA_NEWLINE_SEC}",
            "--output=/logs/result.json",
            "--output-format=json",
        ]
        # Data-integrity verification is only sound with a single in-flight writer:
        # with iodepth>1 or numjobs>1 multiple I/Os target overlapping blocks
        # concurrently, so verify races the writes and reports spurious md5 mismatches.
        # Only enable verify when both are 1; otherwise run a pure load test.
        if self.a.iodepth == 1 and self.a.numjobs == 1:
            fio_args += [
                # md5 header per block. verify_backlog re-verifies recently-written blocks
                # continuously during the run, so corruption is caught while the volume is
                # migrating (not only at job end), and verify_dump writes the mismatched
                # block to a file per offset so the content can be inspected afterwards
                # (a block that reverted to its pre-write content = a lost write).
                "--verify=md5",
                "--verify_backlog=4096",
                "--verify_backlog_batch=4096",
                "--verify_dump=1",
                # verify_fatal=0 (default here) keeps the job running past a mismatch, so
                # EVERY corrupted block of the run is reported instead of only the first.
                # That turns the mismatch count into a measurement: with a known write rate
                # it bounds how wide the interval was in which acknowledged writes were
                # lost. verify_fatal=1 stops at the first failure — cheaper on a corrupted
                # volume, but it also ends that verify batch, so the count becomes a lower
                # bound of 1 and says nothing about the size of the window.
                f"--verify_fatal={1 if self.a.verify_fatal else 0}",
            ]
        else:
            self.log.warn(
                f"data-integrity verification DISABLED: iodepth={self.a.iodepth} "
                f"numjobs={self.a.numjobs} (>1 races the verify and yields false "
                f"corruption); this run measures I/O only, not data integrity")
        fio = " ".join(fio_args)
        return (
            "set -u\n"
            'echo "[pod] $(date -u +%FT%TZ) installing fio"\n'
            "apk add --no-cache fio >/dev/null 2>&1 || "
            '{ echo "[pod] apk add fio FAILED"; exit 90; }\n'
            "mkdir -p /logs\n"
            'echo "[pod] $(date -u +%FT%TZ) starting fio"\n'
            f"{fio}\n"
            "rc=$?\n"
            'echo "$rc" > /logs/fio.rc\n'
            'echo "[pod] $(date -u +%FT%TZ) fio exited rc=$rc"\n'
            # keep the container alive so logs can be collected via kubectl cp
            "sleep 100000\n"
        )

    def worker_node_affinity(self) -> dict:
        """nodeAffinity that pins fio pods to the simplyblock storage worker nodes
        (vm17/vm18/vm19) and keeps them off control-plane nodes.

        The worker hostnames come from the StorageNode CR (self.node_host); the
        control-plane exclusion is belt-and-suspenders in case the hostname list is
        incomplete.
        """
        workers = sorted({h for h in self.node_host.values() if h})
        exprs = [{
            "key": "node-role.kubernetes.io/control-plane",
            "operator": "DoesNotExist",
        }]
        if workers:
            exprs.insert(0, {
                "key": "kubernetes.io/hostname",
                "operator": "In",
                "values": workers,
            })
            self.log.info(f"pods restricted to worker nodes: {', '.join(workers)}")
        else:
            self.log.warn("no worker hostnames known; only excluding control-plane nodes")
        return {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{"matchExpressions": exprs}],
                }
            }
        }

    def create_workload(self) -> None:
        affinity = self.worker_node_affinity()
        script = self.fio_command()
        docs = []
        # Both kinds run the same workload; only the StorageClass differs. The pods keep
        # one continuous index so the log-collection name filter ("<run_id>-fio") still
        # matches all of them.
        plan = ([("single", XFS_STORAGECLASS)] * self.a.pods
                + [("namespaced", NS_STORAGECLASS)] * self.a.ns_pods)
        for i, (kind, sc_name) in enumerate(plan):
            pvc = f"{self.run_id}-pvc-{i}"
            pod = f"{self.run_id}-fio-{i}"
            self.pvcs.append(pvc)
            self.pods.append(pod)
            self.pvc_of[pod] = pvc
            self.kind_of[pod] = kind
            docs.append({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": pvc,
                             "labels": {"test": self.run_id, "volume-kind": kind}},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": sc_name,
                    "resources": {"requests": {"storage": f"{self.a.volume_size_gb}Gi"}},
                },
            })
            docs.append({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod, "labels": {"test": self.run_id, "app": "fio",
                                                     "volume-kind": kind}},
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 5,
                    # Schedule only on the simplyblock storage worker nodes
                    # (vm17/vm18/vm19), never on control-plane nodes.
                    "affinity": affinity,
                    "containers": [{
                        "name": "fio",
                        "image": FIO_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sh", "-c", script],
                        "volumeMounts": [
                            {"name": "data", "mountPath": "/data"},
                            {"name": "logs", "mountPath": "/logs"},
                        ],
                        "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}},
                    }],
                    "volumes": [
                        {"name": "data", "persistentVolumeClaim": {"claimName": pvc}},
                        {"name": "logs", "emptyDir": {}},
                    ],
                },
            })
        manifest = "\n---\n".join(json.dumps(d) for d in docs)
        kubectl_apply(manifest)
        self.log.info(f"created {len(plan)} PVCs + fio pods (run id {self.run_id}): "
                      f"{self.a.pods} single-namespace ({XFS_STORAGECLASS}) + "
                      f"{self.a.ns_pods} namespaced ({NS_STORAGECLASS}, "
                      f"{PARAM_MAX_NS_PER_SUBSYS}={self.a.ns_per_subsys})")

    # ---- readiness --------------------------------------------------------------

    def wait_pods_running(self, timeout: int = 420) -> None:
        self.log.info("waiting for all fio pods to reach Running ...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            data = kubectl_json(["get", "pods", "-l", f"test={self.run_id}"])
            phases = {}
            for item in data.get("items", []):
                phases[item["metadata"]["name"]] = item.get("status", {}).get("phase", "?")
            running = [p for p, ph in phases.items() if ph == "Running"]
            bad = [f"{p}={ph}" for p, ph in phases.items() if ph in ("Failed", "Unknown")]
            if bad:
                raise SystemExit(f"pod(s) failed during startup: {', '.join(bad)}")
            if len(running) == len(self.pods) and running:
                self.log.info(f"all {len(running)} pods Running")
                return
            time.sleep(5)
        raise SystemExit("timed out waiting for pods to become Running")

    def resolve_pvs(self) -> None:
        for pod in self.pods:
            pvc = self.pvc_of[pod]
            data = kubectl_json(["get", "pvc", pvc])
            pv = data.get("spec", {}).get("volumeName", "")
            if not pv:
                raise SystemExit(f"PVC {pvc} has no bound PV yet")
            self.pv_of[pod] = pv
            # CSI volume handle: "<clusterUUID>:<poolUUID>:<volumeUUID>"
            pvdata = kubectl_json(["get", "pv", pv])
            handle = pvdata.get("spec", {}).get("csi", {}).get("volumeHandle", "")
            parts = handle.split(":")
            if len(parts) != 3 or not parts[2]:
                raise SystemExit(f"PV {pv} has unexpected CSI volume handle {handle!r}")
            self.volume_uuid_of[pv] = parts[2]
            self.placement[pv] = ""
        self.log.info("resolved PVCs -> PVs -> volume UUIDs:")
        for pod in self.pods:
            pv = self.pv_of[pod]
            self.log.info(f"    {pod}  {self.pvc_of[pod]}  ->  {pv}  "
                          f"(lvol {self.volume_uuid_of[pv]}, {self.kind_of.get(pod, '?')})")

    # ---- NVMe subsystem grouping -------------------------------------------------

    def sbctl_volume_get(self, lvol: str) -> dict:
        """`sbctl volume get <lvol> --json` — the full backend lvol record, which carries
        the fields `volume list` does not: `nqn` (the NVMe subsystem) and `ns_id` (the
        namespace within it)."""
        pod = self.webappapi_pod()
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "volume", "get", lvol, "--json"],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"sbctl volume get {lvol} failed: {cp.stderr.strip()}")
        data = json.loads(cp.stdout)
        return data if isinstance(data, dict) else {}

    def _subsystem_of(self, pv: str) -> "tuple[str, int]":
        """(NQN, ns_id) of a PV's volume, read from the backend. ('', 0) when unresolvable."""
        lvol = self.volume_uuid_of.get(pv, "")
        if not lvol:
            return "", 0
        try:
            data = self.sbctl_volume_get(lvol)
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cannot resolve subsystem of {pv} (lvol {lvol}): {e}")
            return "", 0
        nqn = data.get("nqn") or data.get("NQN") or ""
        try:
            ns_id = int(data.get("ns_id") or data.get("NS ID") or 0)
        except (TypeError, ValueError):
            ns_id = 0
        return nqn, ns_id

    def resolve_subsystems(self) -> None:
        """Group every volume by the NVMe subsystem it actually lives in.

        The grouping is *discovered*, never assumed: the control plane decides how it
        packs namespaced volumes into subsystems (up to --ns-per-subsys each), so the
        test reads each volume's NQN from the backend and derives the groups from that.
        Everything downstream — the source node of a migration, and the set of volumes
        a migration must move — follows from these groups.
        """
        self.nqn_of.clear()
        self.ns_id_of.clear()
        self._reread_subsystems([self.pv_of[pod] for pod in self.pods])

    def _reread_subsystems(self, pvs: list[str]) -> dict[str, str]:
        """Re-read the subsystem of each PV in pvs from the backend, update the grouping
        and return {pv: nqn} for just those PVs. PVs outside pvs keep their cached NQN, so
        the groups stay complete."""
        for pv in pvs:
            nqn, ns_id = self._subsystem_of(pv)
            if not nqn:
                self.nqn_of.pop(pv, None)
                continue
            self.nqn_of[pv] = nqn
            self.ns_id_of[pv] = ns_id
        groups: dict[str, list[str]] = {}
        for pv, nqn in self.nqn_of.items():
            groups.setdefault(nqn, []).append(pv)
        for nqn, members in groups.items():
            groups[nqn] = sorted(members, key=lambda p: self.ns_id_of.get(p, 0))
        self.subsystem_pvs = groups
        return {pv: self.nqn_of.get(pv, "") for pv in pvs}

    def log_subsystems(self) -> None:
        shared = {n: pvs for n, pvs in self.subsystem_pvs.items() if len(pvs) > 1}
        self.log.info(f"NVMe subsystems in use: {len(self.subsystem_pvs)} "
                      f"({len(shared)} shared by more than one volume)")
        for nqn, pvs in sorted(self.subsystem_pvs.items(), key=lambda kv: -len(kv[1])):
            members = ", ".join(
                f"{self.pod_of_pv(p) or p}(ns{self.ns_id_of.get(p, '?')})" for p in pvs)
            self.log.info(f"    {nqn}  [{len(pvs)} member(s)]  {members}")

    def warn_if_not_namespaced(self) -> None:
        """Flag up front when the namespaced volumes did not actually end up sharing
        subsystems — the multi-namespace half of the run would then be vacuous. Not fatal
        here (the run still tests single-namespace migration), but the analysis reports it
        as INCONCLUSIVE rather than letting it pass silently."""
        if self.a.ns_pods < 2:
            return
        ns_pvs = [self.pv_of[p] for p in self.pods if self.kind_of.get(p) == "namespaced"]
        shared = {self.nqn_of.get(pv) for pv in ns_pvs
                  if len(self.subsystem_pvs.get(self.nqn_of.get(pv, ""), [])) > 1}
        if not shared:
            self.log.crit(
                f"the {len(ns_pvs)} volume(s) from {NS_STORAGECLASS} "
                f"({PARAM_MAX_NS_PER_SUBSYS}={self.a.ns_per_subsys}) each got their own NVMe "
                "subsystem — no batch migration can be exercised. Check that the CSI driver "
                f"honours {PARAM_MAX_NS_PER_SUBSYS} and that the volumes landed on the same "
                "storage node (a subsystem is shared per node)")
        else:
            self.log.info(f"multi-namespace ready: {len(shared)} shared subsystem(s) across "
                          f"{len(ns_pvs)} namespaced volume(s)")

    def pod_of_pv(self, pv: str) -> str:
        for pod, p in self.pv_of.items():
            if p == pv:
                return pod
        return ""

    def group_of(self, pv: str) -> list[str]:
        """The PVs sharing pv's subsystem (including pv itself), i.e. everything a
        migration of pv moves. Falls back to [pv] when the subsystem is unknown."""
        nqn = self.nqn_of.get(pv, "")
        return list(self.subsystem_pvs.get(nqn, [pv])) if nqn else [pv]

    @staticmethod
    def _fio_in_timed_run(pod_stdout: str) -> bool:
        """True once fio is past file layout and in the timed run. During layout fio
        prints "Laying out IO file" / a [f(N)] status; only once the logged run starts
        does it emit a running status line — e.g.
        "Jobs: 4 (f=4): [m(4)][1.2%]...[r=...,w=... IOPS][eta 09m:54s]".
        We require an eta field together with a running-state token (m/r/w), which never
        appears during layout, so this distinguishes "real I/O" from "still laying out"."""
        for line in pod_stdout.splitlines():
            if "[eta " in line and ("[m(" in line or "[r(" in line or "[w(" in line):
                return True
        return False

    def wait_io_flowing(self, timeout: int = 420) -> None:
        """Block until every pod's fio has finished laying out its file and entered the
        timed run (real, logged per-second I/O).

        This gates the migration loop and the measurement window on fio *actually
        running* — not merely on "bytes are moving", which is also true during the file
        layout. Without it, a slow layout can consume the entire runtime and the test
        analyses empty logs (and would falsely report PASS)."""
        self.log.info("waiting for fio to finish layout and enter the timed run in every pod ...")
        deadline = time.time() + timeout
        pending = set(self.pods)
        while pending and time.time() < deadline:
            for pod in list(pending):
                cp = kubectl(["logs", pod, "--tail=8"], check=False, timeout=30)
                if cp.returncode == 0 and self._fio_in_timed_run(cp.stdout):
                    pending.discard(pod)
            if pending:
                time.sleep(5)
        if pending:
            self.log.warn(f"timed run not confirmed in: {', '.join(sorted(pending))} "
                          "(continuing anyway — collected data may be incomplete)")
        else:
            self.log.info("fio timed run active (real I/O) in all pods")

    # ---- health monitor ---------------------------------------------------------

    def start_health_monitor(self) -> None:
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop_health_monitor(self) -> None:
        self._stop_monitor.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)

    def _monitor_loop(self) -> None:
        last_restart: dict[str, int] = {}
        while not self._stop_monitor.is_set():
            try:
                data = kubectl_json(["get", "pods", "-l", f"test={self.run_id}"], check=False)
                for item in data.get("items", []):
                    name = item["metadata"]["name"]
                    st = item.get("status", {})
                    phase = st.get("phase", "?")
                    restarts = 0
                    terminated = None
                    for cs in st.get("containerStatuses", []):
                        restarts += cs.get("restartCount", 0)
                        term = cs.get("state", {}).get("terminated")
                        if term:
                            terminated = term
                    # fio is expected to keep the container Running (it sleeps after
                    # fio exits). Anything else before fio finished = I/O loss signal.
                    if not self._fio_finished.is_set():
                        if phase not in ("Running", "Pending"):
                            self._record_health(name,
                                f"pod phase={phase} BEFORE fio completion "
                                f"(terminated={terminated})")
                        if restarts > last_restart.get(name, 0):
                            self._record_health(name,
                                f"container restarted (count={restarts}) BEFORE fio completion")
                    last_restart[name] = restarts
            except Exception as e:  # noqa: BLE001 — monitor must never crash the test
                self.log.warn(f"health monitor poll error: {e}")
            self._stop_monitor.wait(self.a.health_poll)

    def _record_health(self, pod: str, detail: str) -> None:
        ev = PodHealthEvent(ts=now_utc(), pod=pod, detail=detail)
        self.health_events.append(ev)
        self.log.crit(f"POD HEALTH: {pod}: {detail}")

    # ---- current-node resolution via sbctl --------------------------------------

    def webappapi_pod(self) -> str:
        """Find (and cache) a running webappapi pod in the simplyblock namespace."""
        if self._webappapi_pod:
            return self._webappapi_pod
        # webappapi runs in the simplyblock namespace, not the default ns our
        # kubectl() helper targets — call kubectl directly here.
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "get", "pods", "-o", "json"],
            capture_output=True, text=True, check=True, timeout=60)
        data = json.loads(cp.stdout)
        for item in data.get("items", []):
            name = item["metadata"]["name"]
            if WEBAPPAPI_MATCH in name and item.get("status", {}).get("phase") == "Running":
                self._webappapi_pod = name
                self.log.info(f"using webappapi pod {name} for sbctl queries")
                return name
        raise SystemExit(f"no running '*{WEBAPPAPI_MATCH}*' pod found in "
                         f"namespace {SIMPLYBLOCK_NAMESPACE}")

    def sbctl_volume_list(self) -> list[dict]:
        """Run `sbctl volume list --json` inside a webappapi pod and parse it."""
        pod = self.webappapi_pod()
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "volume", "list", "--json"],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"sbctl volume list failed: {cp.stderr.strip()}")
        return json.loads(cp.stdout)

    def sbctl_snapshot_list(self) -> list[dict]:
        """Run `sbctl snapshot list --json` inside a webappapi pod and parse it."""
        pod = self.webappapi_pod()
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "snapshot", "list", "--json"],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"sbctl snapshot list failed: {cp.stderr.strip()}")
        return json.loads(cp.stdout)

    def _snapshot_resolves(self, snap_id: str) -> "bool | None":
        """Whether a backend snapshot UUID still appears in `sbctl snapshot list`.
        True=present, False=absent (lost), None=inconclusive (id unknown or listing failed)."""
        if not snap_id:
            return None
        try:
            snaps = self.sbctl_snapshot_list()
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"sbctl snapshot list failed: {e}")
            return None
        for s in snaps:
            if snap_id in (s.get("UUID"), s.get("Id"), s.get("SnapshotID"),
                           s.get("id"), s.get("uuid")):
                return True
        return False

    def sbctl_cluster_uuid(self) -> str:
        """Return the live cluster UUID from `sbctl cluster list --json`.

        This is the authoritative source after a reinstall: the StorageClass
        `parameters.cluster_id` (and the source pool SC it is cloned from) can still
        carry a dead cluster id from a previous installation, which would make every
        provisioned volume target a cluster that no longer exists.
        """
        if self._cluster_uuid:
            return self._cluster_uuid
        pod = self.webappapi_pod()
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "cluster", "list", "--json"],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            raise SystemExit(f"sbctl cluster list failed: {cp.stderr.strip()}")
        clusters = json.loads(cp.stdout)
        if not clusters:
            raise SystemExit("sbctl cluster list returned no clusters")
        active = [c for c in clusters if str(c.get("Status", "")).upper() == "ACTIVE"]
        chosen = active or clusters
        if len(chosen) != 1:
            desc = ", ".join(
                f"{c.get('Name')}={c.get('UUID')}({c.get('Status')})" for c in chosen)
            raise SystemExit(
                f"expected exactly one active cluster, found {len(chosen)}: {desc}")
        uuid = chosen[0].get("UUID", "")
        if not uuid:
            raise SystemExit("active cluster has no UUID in sbctl output")
        self.log.info(f"live cluster (sbctl): {chosen[0].get('Name')} = {uuid}")
        self._cluster_uuid = uuid
        return uuid

    def sbctl_storage_node_list(self) -> list[dict]:
        """Run `sbctl storage-node list --json` inside a webappapi pod and parse it."""
        pod = self.webappapi_pod()
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "storage-node", "list", "--json"],
            capture_output=True, text=True, timeout=60,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"sbctl storage-node list failed: {cp.stderr.strip()}")
        return json.loads(cp.stdout)

    def build_sbctl_host_map(self) -> None:
        """Map each sbctl Hostname (e.g. 'vm04_4424') to its storage-node UUID, straight
        from `sbctl storage-node list`: it reports both for every node in the cluster,
        whether or not that node currently holds a volume.

        The translation is needed because a volume's Hostname is the *storage node's*
        name (short host + rpc port), while the StorageNode CR — where the test learns
        the migration targets — carries the Kubernetes node name
        (vm04.simplyblock4.localdomain).
        """
        m = {}
        for n in self.sbctl_storage_node_list():
            host, uuid = n.get("Hostname", ""), n.get("UUID", "")
            if host and uuid:
                m[host] = uuid
            # The management IP is the NVMe transport address the node's subsystems listen
            # on, which is how a sampled path is attributed to source or target.
            if uuid and n.get("Management IP"):
                self.node_ip[uuid] = n["Management IP"]
        if not m:
            raise RuntimeError("sbctl storage-node list reported no Hostname/UUID pairs")
        self.sbctl_host_to_node = m
        self.log.info("sbctl hostname -> storage node map:")
        for h, u in sorted(m.items()):
            self.log.info(f"    {h}  ->  {u} ({self.node_host.get(u,'?')})")

    def resolve_current_nodes(self) -> None:
        """Authoritatively set placement[pv] = current storage node for every PV."""
        vols = self.sbctl_volume_list()
        self.build_sbctl_host_map()
        # The CSI volume handle's volume field is sbctl's "Id" (not "LVolUUID"), so
        # index by both to be robust.
        by_vol = {}
        for v in vols:
            for key in (v.get("Id"), v.get("LVolUUID")):
                if key:
                    by_vol[key] = v
        for pod in self.pods:
            pv = self.pv_of[pod]
            lvol = self.volume_uuid_of.get(pv)
            v = by_vol.get(lvol)
            if not v:
                self.log.warn(f"{pv}: lvol {lvol} not found in sbctl volume list")
                continue
            host = v.get("Hostname", "")
            node = self.sbctl_host_to_node.get(host, "")
            if not node:
                self.log.warn(f"{pv}: sbctl hostname {host!r} not mapped to a node")
                continue
            self.placement[pv] = node

    # ---- migrations -------------------------------------------------------------

    def _sbctl_nodes_of(self, pvs: list[str]) -> dict[str, str]:
        """Authoritative current primary storage-node UUID for several PVs at once, via a
        single `sbctl volume list`. PVs that cannot be resolved are absent from the result.

        Taking the whole set from one listing matters for a shared subsystem: its members
        must be compared against each other, and per-PV listings taken seconds apart could
        straddle a move and make a consistent subsystem look split."""
        try:
            vols = self.sbctl_volume_list()
            if not self.sbctl_host_to_node:
                self.build_sbctl_host_map()
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cannot resolve volume placement via sbctl: {e}")
            return {}
        by_lvol = {}
        for v in vols:
            for key in (v.get("Id"), v.get("LVolUUID")):
                if key:
                    by_lvol[key] = v

        def resolve(rebuilt: bool = False) -> dict[str, str]:
            out: dict[str, str] = {}
            missed = []
            for pv in pvs:
                v = by_lvol.get(self.volume_uuid_of.get(pv, ""))
                if not v:
                    continue
                host = v.get("Hostname", "")
                node = self.sbctl_host_to_node.get(host, "")
                if node:
                    out[pv] = node
                else:
                    missed.append(host)
            # A hostname the map does not know is not proof the volume is unplaceable:
            # a node may have joined since the map was built. Rebuild once and retry
            # before giving up, so a stale map cannot silently turn every verification
            # into "skipped".
            if missed and not rebuilt:
                self.log.warn("storage node hostname(s) missing from the host map: "
                              + ", ".join(sorted(set(missed))) + "; rebuilding it")
                try:
                    self.build_sbctl_host_map()
                except Exception as e:  # noqa: BLE001
                    self.log.warn(f"cannot rebuild the host map: {e}")
                    return out
                return resolve(rebuilt=True)
            return out

        return resolve()

    def _sbctl_node_of(self, pv: str) -> str:
        """Authoritative current primary storage-node UUID for a PV's volume, via sbctl.
        Returns '' when it can't be resolved. Used to re-sync the placement cache (which
        drifts because the auto-rebalancer also moves volumes) and to verify migrations."""
        return self._sbctl_nodes_of([pv]).get(pv, "")

    def _snapshot_backend_id(self, snap_name: str, timeout: int = 90) -> str:
        """Wait for a VolumeSnapshot to become readyToUse and return its backend snapshot
        UUID, read from the bound VolumeSnapshotContent's snapshotHandle. Returns '' if the
        snapshot never becomes ready or no handle is exposed."""
        deadline = time.time() + timeout
        content = ""
        while time.time() < deadline:
            vs = kubectl_json(["get", "volumesnapshot", snap_name], check=False)
            st = vs.get("status", {}) if vs else {}
            content = st.get("boundVolumeSnapshotContentName", "") or content
            if content and st.get("readyToUse"):
                break
            time.sleep(3)
        if not content:
            return ""
        # VolumeSnapshotContent is cluster-scoped (the -n flag is ignored for it).
        vsc = kubectl_json(["get", "volumesnapshotcontent", content], check=False)
        handle = vsc.get("status", {}).get("snapshotHandle", "") if vsc else ""
        # CSI handle mirrors the volume handle form "<cluster>:<pool>:<snapUUID>"; the
        # backend snapshot UUID is the last colon-separated field.
        return handle.split(":")[-1] if handle else ""

    def maybe_snapshot(self, rec: "MigrationRecord", idx: int) -> None:
        """With SNAPSHOT_CHANCE probability, snapshot this volume via a VolumeSnapshot CR
        *before* migrating it, then confirm the backend snapshot resolves via sbctl.

        Done pre-migration only: snapshotting a volume after it has been migrated adds
        nothing to this test. Whether the volume already carries a previous snapshot makes
        no difference — a fresh one is always added. The ensuing migration must then carry
        the snapshot; _verify_snapshot_after_migration re-checks it resolves afterwards."""
        if not self.snapshot_class or random.random() >= self.a.snapshot_chance:
            return
        snap = f"{self.run_id}-snap-{idx}"
        try:
            kubectl_apply(json.dumps({
                "apiVersion": SNAPSHOT_APIVERSION,
                "kind": "VolumeSnapshot",
                "metadata": {"name": snap, "labels": {"test": self.run_id}},
                "spec": {
                    "volumeSnapshotClassName": self.snapshot_class,
                    "source": {"persistentVolumeClaimName": rec.pvc},
                },
            }))
        except subprocess.CalledProcessError as e:
            self.log.error(f"snapshot {snap} create failed: {e.stderr or e}")
            return
        self.snapshots.append(snap)
        rec.pre_snapshot = snap
        rec.pre_snapshot_id = self._snapshot_backend_id(snap)
        self.log.event(
            f"SNAPSHOT CREATE  {snap}  pvc={rec.pvc}  pv={rec.pv}  vol={rec.vol or '?'}  "
            f"snap_id={rec.pre_snapshot_id or '?'} (pre-migration)")
        # validate the snapshot id resolves right after creation
        resolves = self._snapshot_resolves(rec.pre_snapshot_id)
        rec.snapshot_created_ok = resolves
        if resolves is True:
            self.log.event(f"SNAPSHOT VERIFY  {snap}  OK  snap_id={rec.pre_snapshot_id} "
                           "resolves via sbctl after creation")
        elif resolves is False:
            rec.snapshot_verify_msg = (f"snapshot {rec.pre_snapshot_id or snap} does NOT "
                                       "resolve via sbctl right after creation")
            self.log.crit(f"SNAPSHOT VERIFY FAIL  {snap}: {rec.snapshot_verify_msg}")
        else:
            rec.snapshot_verify_msg = ("could not resolve snapshot id after creation "
                                       "(no backend id or sbctl listing failed)")
            self.log.warn(f"SNAPSHOT VERIFY  {snap}: {rec.snapshot_verify_msg}")

    def _verify_snapshot_after_migration(self, rec: "MigrationRecord") -> None:
        """After the migration finishes, confirm the pre-migration snapshot still resolves
        via `sbctl snapshot list` — a migration must carry its snapshots, not drop them."""
        if not rec.pre_snapshot:
            return
        resolves = self._snapshot_resolves(rec.pre_snapshot_id)
        rec.snapshot_post_ok = resolves
        if resolves is True:
            self.log.event(f"SNAPSHOT VERIFY  {rec.pre_snapshot}  OK  "
                           f"snap_id={rec.pre_snapshot_id} still resolves after migration")
        elif resolves is False:
            msg = (f"snapshot {rec.pre_snapshot_id or rec.pre_snapshot} LOST — does not "
                   f"resolve via sbctl after migration {rec.name} (phase={rec.phase})")
            rec.snapshot_verify_msg = (rec.snapshot_verify_msg + "; " + msg
                                       if rec.snapshot_verify_msg else msg)
            self.log.crit(f"SNAPSHOT VERIFY FAIL  {rec.pre_snapshot}: {msg}")
        else:
            self.log.warn(f"SNAPSHOT VERIFY  {rec.pre_snapshot}: could not re-resolve "
                          "snapshot id after migration (inconclusive)")

    # ---- host-side ANA sampling (through the CSI node-plugin pods) ---------------

    def _refresh_csi_node_pods(self) -> None:
        """Map each Kubernetes node to its CSI node-plugin pod. That pod already has the
        host's /sys mounted, so it is the window onto the host's NVMe view — no extra
        privileged pod of our own is needed."""
        try:
            pods = self._list_pods(SIMPLYBLOCK_NAMESPACE, [CSI_NODE_MATCH])
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cannot list CSI node pods: {e}")
            return
        self.csi_node_pod_of = {p["node"]: p["name"] for p in pods if p["node"]}
        self.log.info("CSI node plugin pods (ANA sampling windows):")
        for node, pod in sorted(self.csi_node_pod_of.items()):
            self.log.info(f"    {node}  ->  {pod}")

    def csi_node_pod(self, k8s_node: str) -> str:
        if k8s_node not in self.csi_node_pod_of:
            self._refresh_csi_node_pods()
        return self.csi_node_pod_of.get(k8s_node, "")

    def pod_nodes(self) -> dict[str, str]:
        """Map each of the run's fio pods to the Kubernetes node it runs on.

        Resolved once and cached: the workload is plain Pods, so they never move for the
        lifetime of the run. A pod that is not scheduled yet is simply absent."""
        if self._pod_node_of:
            return self._pod_node_of
        try:
            pods = self._list_pods(NAMESPACE, [f"{self.run_id}-fio"])
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cannot resolve fio pod nodes: {e}")
            return {}
        self._pod_node_of = {p["name"]: p["node"] for p in pods if p["node"]}
        return self._pod_node_of

    def consumer_nodes(self, pvs: list[str]) -> dict[str, list[str]]:
        """Kubernetes nodes consuming any of these PVs, each with the pods doing so.

        This is the node set a shared subsystem is connected on, and therefore the set
        the migration has to establish new paths on — the same resolution the operator
        performs (via pods) to decide where to run its validation jobs."""
        by_node: dict[str, list[str]] = {}
        nodes = self.pod_nodes()
        for pv in pvs:
            pod = self.pod_of_pv(pv)
            node = nodes.get(pod or "", "")
            if node:
                by_node.setdefault(node, []).append(pod)
        return by_node

    def _sample_ana_on(self, k8s_node: str, nqn: str, phase: str) -> list[AnaSample]:
        """Read one host's view of the subsystem: every controller with its state and the
        ANA state of each namespace path. An empty result means the host holds no
        connection to that subsystem at all."""
        pod = self.csi_node_pod(k8s_node)
        if not pod:
            return []
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "-c", CSI_NODE_CONTAINER,
             "--", "env", f"NQN={nqn}", "sh", "-c", ANA_PROBE_SCRIPT],
            capture_output=True, text=True, check=False, timeout=30)
        if cp.returncode != 0:
            self._ana_probe_errors += 1
            if self._ana_probe_errors <= 3 or self._ana_probe_errors % 20 == 0:
                self.log.warn(f"ANA probe on {k8s_node} failed "
                              f"(#{self._ana_probe_errors}): {cp.stderr.strip()[:200]}")
            return []
        ts = now_utc()
        out = []
        for line in cp.stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) != 3:
                continue
            addr, state, ana = parts
            states = {}
            for item in ana.split(",") if ana else []:
                nsid, _, a = item.partition(":")
                if nsid.isdigit():
                    states[int(nsid)] = a
            out.append(AnaSample(ts=ts, node=k8s_node, phase=phase,
                                 address=addr, state=state, ana=states))
        return out

    def _sample_ana_round(self, rec: "MigrationRecord", nodes: list[str]) -> list[AnaSample]:
        """One sampling round across all consuming nodes, taken concurrently so the
        samples describe one instant rather than a staircase across nodes."""
        if not nodes:
            return []
        with ThreadPoolExecutor(max_workers=min(len(nodes), 8)) as pool:
            futures = [pool.submit(self._sample_ana_on, n, rec.nqn, rec.live_phase)
                       for n in nodes]
            out = []
            for f in futures:
                try:
                    out.extend(f.result())
                except Exception as e:  # noqa: BLE001
                    self.log.warn(f"ANA sampling round failed: {e}")
            return out

    def _start_ana_sampler(self, rec: "MigrationRecord", nodes: list[str]) -> None:
        """Sample the host view on every consuming node for the duration of the migration.

        The samples are what makes the verification state-aware rather than merely
        positional: they show *when* the target began serving relative to cutover, and
        whether every namespace was served afterwards."""
        if self.a.ana_interval <= 0 or not rec.nqn or not nodes:
            return
        stop = threading.Event()

        def loop() -> None:
            while True:
                rec.ana_samples.extend(self._sample_ana_round(rec, nodes))
                if stop.wait(self.a.ana_interval):
                    break
            # A last round after the CR went terminal: the post-cutover state is the half
            # of the evidence the in-flight rounds cannot capture.
            rec.live_phase = rec.phase or rec.live_phase
            rec.ana_samples.extend(self._sample_ana_round(rec, nodes))

        thread = threading.Thread(target=loop, name=f"ana-{rec.name}", daemon=True)
        self._ana_stop, self._ana_thread = stop, thread
        thread.start()
        self.log.info(f"ANA sampling started for {rec.name} on {len(nodes)} node(s) "
                      f"every {self.a.ana_interval}s: {', '.join(nodes)}")

    def _stop_ana_sampler(self, rec: "MigrationRecord") -> None:
        if not self._ana_thread:
            return
        self._ana_stop.set()
        self._ana_thread.join(timeout=90)
        self._ana_thread = None
        if rec.ana_samples:
            rec.ana_csv = self._write_ana_csv(rec)
            self.log.info(f"ANA samples for {rec.name}: {len(rec.ana_samples)} "
                          f"-> {rec.ana_csv}")

    def _write_ana_csv(self, rec: "MigrationRecord") -> str:
        """One row per (instant, node, controller, namespace) — the raw host view, kept
        alongside the fio time series so an I/O gap can be read against path state."""
        d = os.path.join(self.outdir, "ana")
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, f"{rec.name}.csv")
        src_ip = self.node_ip.get(rec.source, "")
        tgt_ip = self.node_ip.get(rec.target, "")
        with open(path, "w") as fh:
            fh.write("ts,node,phase,address,role,ctrl_state,nsid,ana_state\n")
            for s in sorted(rec.ana_samples, key=lambda x: (x.ts, x.node, x.address)):
                ip = s.address.rsplit(":", 1)[0]
                role = "target" if ip == tgt_ip else "source" if ip == src_ip else "other"
                if not s.ana:  # a controller with no namespace path at all is itself data
                    fh.write(f"{iso(s.ts)},{s.node},{s.phase},{s.address},{role},"
                             f"{s.state},,\n")
                for nsid, ana in sorted(s.ana.items()):
                    fh.write(f"{iso(s.ts)},{s.node},{s.phase},{s.address},{role},"
                             f"{s.state},{nsid},{ana}\n")
        return path

    def _verify_ana_states(self, rec: "MigrationRecord") -> None:
        """Verify the host-side path behaviour of one migration against its ANA samples.

        The cutover instant is taken from the samples themselves — the first moment a target
        path is optimized while no source path is — not from the CR reaching Completed. The
        CR trails the real cutover by tens of seconds (the lag is reported), so judging
        "before cutover" against it would flag every normal migration.

        Three things must hold, all of them invisible to the placement check:

        * Source and target must never serve at the same time. Two simultaneously
          optimized paths to two copies means reads can land on either, and a read served
          by the copy that is behind returns data that was never written there.
        * After a Completed migration a *live* target controller must serve every namespace
          of the subsystem on every consuming node. A batch migration that leaves one
          namespace unserved on the target is the half-moved case, seen from the host.
          Non-live controllers at the target address are reported but do not fail the
          check: the old instance's controllers are torn down at the same address while
          the new one takes over.
        * No namespace may be left with no accessible path anywhere for long. That is an
          I/O stall, which the fio timeline shows as a gap but cannot attribute.
        """
        if not rec.ana_samples:
            return
        tgt_ip = self.node_ip.get(rec.target, "")
        if not tgt_ip:
            self.log.warn(f"ANA VERIFY  {rec.name}: target {rec.target} has no known IP; "
                          "cannot tell target paths from source paths")
            return
        expected = {self.ns_id_of[pv] for pv in (rec.group_pvs or [rec.pv])
                    if pv in self.ns_id_of}

        src_ip = self.node_ip.get(rec.source, "")

        def ip_of(s: AnaSample) -> str:
            return s.address.rsplit(":", 1)[0]

        problems: list[str] = []
        by_node: dict[str, list[AnaSample]] = {}
        for s in rec.ana_samples:
            by_node.setdefault(s.node, []).append(s)

        for node, samples in sorted(by_node.items()):
            samples.sort(key=lambda x: x.ts)
            rounds: dict[datetime, list[AnaSample]] = {}
            for s in samples:
                rounds.setdefault(s.ts, []).append(s)
            times = sorted(rounds)

            # 1. the host-observed cutover, and any instant where both copies served.
            # "optimized" is what the kernel prefers; a target that is merely non-optimized
            # alongside an optimized source is the normal HA standby, not a second writer.
            overlaps = []
            cutover_at = None
            for t in times:
                src_opt = [s.address for s in rounds[t]
                           if ip_of(s) == src_ip and "optimized" in s.ana.values()]
                tgt_opt = [s.address for s in rounds[t]
                           if ip_of(s) == tgt_ip and "optimized" in s.ana.values()]
                if tgt_opt and not src_opt and cutover_at is None:
                    cutover_at = t
                    rec.ana_cutover[node] = iso(t)
                if src_opt and tgt_opt and src_ip != tgt_ip:
                    overlaps.append((t, src_opt, tgt_opt))
            if overlaps:
                t0, src_addrs, tgt_addrs = overlaps[0]
                problems.append(
                    f"{node}: source {', '.join(src_addrs)} and target "
                    f"{', '.join(tgt_addrs)} were BOTH optimized at {iso(t0)} "
                    f"({len(overlaps)} sample(s)) — reads could be served by either copy")

            # The CR's Completed is not the cutover; record how far it trailed, since every
            # correlation against the fio timeline has to use the observed instant instead.
            if rec.end and cutover_at:
                rec.ana_cr_lag_s = max(rec.ana_cr_lag_s,
                                       (rec.end - cutover_at).total_seconds())

            # 2. a live target controller serves everything afterwards
            if rec.phase == "Completed":
                final = [s for s in rounds[times[-1]] if ip_of(s) == tgt_ip]
                live = [s for s in final if s.state == "live"]
                served = {n for s in live for n in s.accessible_nsids()}
                stale = [f"{s.address}({s.state})" for s in final if s.state != "live"]
                if stale:
                    self.log.info(f"ANA VERIFY  {rec.name}  {node}: non-live controller(s) at "
                                  f"the target address: {', '.join(stale)} — the old instance "
                                  "being torn down")
                if not final:
                    problems.append(f"{node}: no controller for the target {tgt_ip} after "
                                    "a completed migration — the host never joined it")
                elif not live:
                    problems.append(f"{node}: no live controller for the target {tgt_ip} after "
                                    f"cutover (found {', '.join(stale)})")
                elif expected and expected - served:
                    problems.append(
                        f"{node}: live target controller(s) at {tgt_ip} serve namespace(s) "
                        f"{sorted(served) or '-'} but the subsystem has {sorted(expected)} — "
                        f"{sorted(expected - served)} unserved after cutover")

            # 3. stalls: some namespace with no accessible path anywhere on this node
            per_ts: dict[datetime, set[int]] = {}
            for s in samples:
                per_ts.setdefault(s.ts, set()).update(s.accessible_nsids())
            times = sorted(per_ts)
            want = expected or {n for acc in per_ts.values() for n in acc}
            stall_start = None
            for i, t in enumerate(times):
                missing = bool(want - per_ts[t])
                if missing and stall_start is None:
                    stall_start = t
                elif not missing and stall_start is not None:
                    rec.ana_stall_s = max(rec.ana_stall_s, (t - stall_start).total_seconds())
                    stall_start = None
            if stall_start is not None:
                rec.ana_stall_s = max(rec.ana_stall_s,
                                      (times[-1] - stall_start).total_seconds())

        if rec.ana_stall_s > self.a.ana_stall_crit:
            problems.append(f"some namespace had no accessible path for "
                            f"{rec.ana_stall_s:.0f}s (> {self.a.ana_stall_crit}s)")

        rec.ana_msgs = problems
        rec.ana_ok = not problems
        if problems:
            self.log.crit(f"ANA VERIFY FAIL  {rec.name}:")
            for p in problems:
                self.log.crit(f"    {p}")
        else:
            self.log.event(
                f"ANA VERIFY  {rec.name}  OK  nodes={len(by_node)}  "
                f"samples={len(rec.ana_samples)}  namespaces={sorted(expected) or '?'}  "
                f"max_stall={rec.ana_stall_s:.0f}s  "
                f"cutover={', '.join(f'{n}@{t}' for n, t in sorted(rec.ana_cutover.items())) or '-'}"
                + (f"  (CR reported Completed {rec.ana_cr_lag_s:.0f}s later)"
                   if rec.ana_cr_lag_s else ""))

    # ---- target selection --------------------------------------------------------

    def pick_target(self, pv: str, group: list[str], idx: int,
                    rec: "MigrationRecord | None" = None) -> str:
        """Pick the node to migrate to, honouring --target-policy.

        The policy is the discriminator: whether the target node also runs a pod consuming
        this subsystem. That case is materially different — the consuming host has to join
        a subsystem on the very node that is becoming its target — so which one a migration
        exercised is recorded rather than left to chance.

        placement[pv] is refreshed from sbctl before each pick (see run_one_migration), so
        it reflects the real current node even when the auto-rebalancer has moved the
        volume out from under us; the target is always some other node.
        """
        current = self.placement.get(pv, "")
        candidates = [n for n in self.nodes if n != current] if current else list(self.nodes)
        policy = self.a.target_policy
        if policy == "alternate":
            # First idx is 1, so odd migrations put the target on a consuming node — the
            # more demanding case, worth reaching first if a run is cut short.
            policy = "consumer" if idx % 2 == 1 else "no-consumer"
        consuming = self.consumer_nodes(group)  # k8s node -> consuming pods
        want = None if policy == "random" else (policy == "consumer")

        chosen_from = candidates
        if want is not None:
            matching = [n for n in candidates
                        if (self.node_host.get(n, "") in consuming) == want]
            if matching:
                chosen_from = matching
            elif rec is not None:
                rec.target_policy_ok = False
                self.log.warn(
                    f"target policy {policy!r}: no candidate node "
                    f"{'hosts' if want else 'is free of'} a consumer of this subsystem "
                    f"(consumers on {', '.join(sorted(consuming)) or 'none'}); "
                    "picking any other node")
        target = random.choice(chosen_from)
        if rec is not None:
            rec.target_policy = policy
            rec.target_consumers = sorted(consuming.get(self.node_host.get(target, ""), []))
            rec.target_has_consumer = bool(rec.target_consumers)
        return target

    def pick_pod(self, idx: int) -> str:
        """Pick the pod to migrate, alternating between single-namespace and namespaced
        volumes on successive migrations so both kinds are exercised throughout the run
        instead of clustering by chance. Falls back to whichever kind exists."""
        by_kind = {"single": [], "namespaced": []}
        for pod in self.pods:
            by_kind.setdefault(self.kind_of.get(pod, "single"), []).append(pod)
        # The loop's first idx is 1, so odd picks namespaced — the multi-namespace path is
        # the one worth reaching first when a run is cut short.
        want = "namespaced" if idx % 2 == 1 else "single"
        pool = by_kind.get(want) or by_kind.get(
            "single" if want == "namespaced" else "namespaced") or self.pods
        return random.choice(pool)

    def migration_manifest(self, name: str, pv: str, target: str) -> str:
        return json.dumps({
            "apiVersion": API_GROUP,
            "kind": "VolumeMigration",
            "metadata": {"name": name, "labels": {"test": self.run_id}},
            "spec": {"pvName": pv, "targetNodeUUID": target},
        })

    def run_one_migration(self, idx: int, hard_deadline: float) -> MigrationRecord | None:
        pod = self.pick_pod(idx)
        pv = self.pv_of[pod]
        pvc = self.pvc_of[pod]
        # The volume's whole subsystem migrates, so the group — not the single PV — is what
        # gets re-synced, targeted and later verified. Re-read it here rather than trusting
        # the initial grouping: a subsystem's membership is backend state.
        nqn, _ = self._subsystem_of(pv)
        if nqn and nqn != self.nqn_of.get(pv):
            self.log.warn(f"{pv} changed subsystem since the last look: "
                          f"{self.nqn_of.get(pv, '?')} -> {nqn}; regrouping")
            self.resolve_subsystems()
        group = self.group_of(pv)
        # Re-sync the real current node before picking a target: the auto-rebalancer may
        # have moved this volume since we last looked, so a cached source would make us
        # target the node it already lives on (HTTP 400 "already on node").
        nodes = self._sbctl_nodes_of(group)
        self.placement.update(nodes)
        name = f"{self.run_id}-mig-{idx}"
        rec = MigrationRecord(name=name, pod=pod, pvc=pvc, pv=pv, target="")
        rec.target = target = self.pick_target(pv, group, idx, rec)
        rec.vol = self.volume_uuid_of.get(pv, "")  # lvol UUID, as webappapi errors reference it
        rec.source = self.placement.get(pv, "")  # authoritative current node (sbctl)
        rec.nqn = self.nqn_of.get(pv, "")
        rec.group_pvs = group
        self.migrations.append(rec)

        # Members of one subsystem live on one node, since the subsystem itself is hosted
        # there. If they don't, the grouping or the placement is already wrong before this
        # migration starts — say so, because it changes how the verification reads.
        elsewhere = {p: n for p, n in nodes.items() if rec.source and n != rec.source}
        if elsewhere:
            self.log.warn(
                f"subsystem {rec.nqn or '?'} is not on one node before migrating: "
                + ", ".join(f"{self.pod_of_pv(p) or p}@{n}" for p, n in elsewhere.items()))

        # Randomly snapshot the volume *before* migrating it, so the migration must carry
        # the snapshot (validated again after it finishes).
        self.maybe_snapshot(rec, idx)

        # Sample the host view from before the CR exists: the pre-migration state is the
        # baseline every "started serving too early" judgement is made against.
        consuming = self.consumer_nodes(group)
        rec.live_phase = "Pending"
        self._start_ana_sampler(rec, sorted(consuming))

        kubectl_apply(self.migration_manifest(name, pv, target))
        siblings = [self.pod_of_pv(p) or p for p in group if p != pv]
        self.log.event(
            f"MIGRATION START  {name}  kind={self.kind_of.get(pod,'?')}  pod={pod}  pv={pv}  "
            f"vol={rec.vol or '?'}  subsystem={rec.nqn or '?'}  members={len(group)}"
            + (f" (moves along: {', '.join(siblings)})" if siblings else "")
            + f"  source={rec.source or '?'} ({self.node_host.get(rec.source,'?')})  "
            f"target={target} ({self.node_host.get(target,'?')})  "
            f"policy={rec.target_policy}"
            + (f"  target hosts consumer(s): {', '.join(rec.target_consumers)}"
               if rec.target_has_consumer else "  target hosts no consumer")
            + f"  consumers on {len(consuming)} node(s): "
            + ", ".join(f"{n}({len(p)})" for n, p in sorted(consuming.items())))

        # Bound the wait so a migration started late doesn't block long past fio's
        # end; hard_deadline already includes a grace window beyond the fio runtime.
        deadline = min(time.time() + self.a.migration_timeout, hard_deadline)
        terminal = {"Completed", "Failed", "Aborted"}
        while time.time() < deadline:
            cr = kubectl_json(["get", "volumemigration", name], check=False)
            status = cr.get("status", {}) if cr else {}
            phase = status.get("phase", "")
            rec.live_phase = phase or rec.live_phase  # stamped onto concurrent ANA samples
            if status.get("sourceNodeUUID"):
                rec.source = status["sourceNodeUUID"]
                if not self.placement.get(pv):
                    self.placement[pv] = rec.source  # learn current node
            rec.cr_nqn = status.get("subsystemNQN", "") or rec.cr_nqn
            rec.cr_migration_uuid = status.get("migrationUUID", "") or rec.cr_migration_uuid
            if status.get("memberCount") is not None:
                rec.cr_members = status["memberCount"]
            if phase in terminal:
                rec.phase = phase
                rec.error = status.get("errorMessage", "")
                rec.end = now_utc()
                break
            time.sleep(self.a.migration_poll)
        else:
            rec.phase = "TIMEOUT"
            rec.end = now_utc()

        dur = (rec.end - rec.start).total_seconds() if rec.end else -1
        src_h = self.node_host.get(rec.source, "?")
        tgt_h = self.node_host.get(rec.target, "?")
        if rec.phase == "Completed":
            self.log.event(
                f"MIGRATION STOP   {name}  phase=Completed  vol={rec.vol or '?'}  {rec.source}({src_h}) -> "
                f"{target}({tgt_h})  members={rec.cr_members if rec.cr_members is not None else len(group)}"
                f"  duration={dur:.0f}s")
        else:
            self.log.event(
                f"MIGRATION STOP   {name}  phase={rec.phase}  vol={rec.vol or '?'}  target={target}({tgt_h})  "
                f"source={rec.source or '?'}  duration={dur:.0f}s  error={rec.error!r}")

        # Stop sampling only after the CR is terminal, so the final round captures the
        # post-cutover state, then judge the collected path behaviour.
        self._stop_ana_sampler(rec)
        self._verify_cr_subsystem(rec)
        self._verify_migration(rec)
        self._verify_ana_states(rec)
        self._verify_snapshot_after_migration(rec)
        return rec

    def _verify_cr_subsystem(self, rec: "MigrationRecord") -> None:
        """Cross-check the operator's view of the subsystem against the test's own: the CR's
        status.subsystemNQN must be the subsystem the volume lives in, and
        status.memberCount the number of volumes sharing it.

        This catches the operator addressing (and so moving) a different set of volumes than
        the one the placement verification below then checks — the two would otherwise agree
        with each other while both being wrong.
        """
        problems = []
        if rec.cr_nqn and rec.nqn and rec.cr_nqn != rec.nqn:
            problems.append(f"CR subsystemNQN={rec.cr_nqn} but volume is in {rec.nqn}")
        # The test only sees its own volumes; a subsystem shared with volumes from outside
        # this run would legitimately report a higher member count, so only a *lower* one
        # is wrong.
        if rec.cr_members is not None and rec.group_pvs and rec.cr_members < len(rec.group_pvs):
            problems.append(f"CR memberCount={rec.cr_members} but {len(rec.group_pvs)} "
                            f"test volume(s) share the subsystem")
        # Only demand a subsystem once a migration actually exists. A migration whose
        # submit was rejected never gets one, and reporting that as a view mismatch would
        # bury the real error (the rejection) under a bogus one.
        if not rec.cr_nqn and rec.cr_migration_uuid:
            problems.append("CR reported a migration but no subsystemNQN")
        if problems:
            rec.cr_match_ok = False
            rec.cr_match_msg = "; ".join(problems)
            self.log.crit(f"MIGRATION CR CHECK FAIL  {rec.name}: {rec.cr_match_msg}")
        elif rec.cr_nqn:
            rec.cr_match_ok = True
            self.log.event(f"MIGRATION CR CHECK  {rec.name}  OK  subsystem={rec.cr_nqn}  "
                           f"memberCount={rec.cr_members}")

    def _verify_migration(self, rec: "MigrationRecord") -> None:
        """Confirm the real primary node (sbctl) of every volume the migration moved matches
        the outcome: Completed -> the target; Failed/Timeout -> still the source.

        A subsystem moves as a unit, so for a namespaced volume this checks all of its
        members, not only the PV named in the CR: leaving a sibling behind is exactly the
        bug this test exists to catch. A subsystem found half-moved (some members on the
        target, some on the source) is flagged as such — it is worse than a clean failure,
        since the subsystem is then split across two nodes.

        Any mismatch is an irregularity (recorded, reported, fails the run). The real
        positions are written back to the placement cache regardless, to re-sync it. The
        subsystem grouping is re-read afterwards as well: a migration must not change which
        volumes share a subsystem.
        """
        group = rec.group_pvs or [rec.pv]
        nodes = self._sbctl_nodes_of(group)
        rec.member_nodes = nodes
        rec.actual_node = nodes.get(rec.pv, "")
        self.placement.update(nodes)  # re-sync cache to reality

        expected = rec.target if rec.phase == "Completed" else rec.source
        kind = "target" if rec.phase == "Completed" else "source"

        def label(pv: str) -> str:
            node = nodes.get(pv, "")
            return (f"{self.pod_of_pv(pv) or pv}@"
                    f"{node or '?'}({self.node_host.get(node, '?')})")

        unresolved = [p for p in group if p not in nodes]
        wrong = [p for p, n in nodes.items() if n != expected]
        # A split subsystem: members on the target *and* members left on the source.
        on_target = {p for p, n in nodes.items() if n == rec.target}
        on_source = {p for p, n in nodes.items() if rec.source and n == rec.source}
        rec.split_group = bool(len(group) > 1 and on_target and on_source)

        if not nodes:
            rec.verify_ok = None
            rec.verify_msg = "could not resolve actual node(s) via sbctl"
            self.log.warn(f"MIGRATION VERIFY  {rec.name}: skipped — {rec.verify_msg}")
        elif not expected:
            rec.verify_ok = None
            rec.verify_msg = f"no expected {kind} node recorded; cannot verify"
            self.log.warn(f"MIGRATION VERIFY  {rec.name}: {rec.verify_msg}")
        elif not wrong and not unresolved:
            rec.verify_ok = True
            self.log.event(
                f"MIGRATION VERIFY  {rec.name}  OK  phase={rec.phase}  "
                f"all {len(group)} subsystem volume(s) on "
                f"{expected}({self.node_host.get(expected,'?')}) == expected {kind}")
        else:
            rec.verify_ok = False
            detail = ", ".join(label(p) for p in wrong + unresolved)
            rec.verify_msg = (
                f"phase={rec.phase}: {len(wrong) + len(unresolved)} of {len(group)} "
                f"subsystem volume(s) not on expected {kind} "
                f"{expected}({self.node_host.get(expected,'?')}): {detail}")
            if rec.split_group:
                rec.verify_msg = ("SUBSYSTEM SPLIT ACROSS NODES — " + rec.verify_msg
                                  + f"; on target: {', '.join(label(p) for p in sorted(on_target))}")
            self.log.crit(f"MIGRATION VERIFY FAIL  {rec.name}: {rec.verify_msg}")

        self._verify_group_intact(rec)

    def _verify_group_intact(self, rec: "MigrationRecord") -> None:
        """Confirm the migration did not change subsystem membership: the volumes that
        shared a subsystem before must still share one afterwards, under the same NQN
        (moving a subsystem between nodes does not re-identify it). Membership changes are
        folded into the migration's verification verdict.

        Only the affected volumes are re-read, not every volume in the run — this runs
        inside the migration loop, and one backend query per volume would cost more than
        the migration gap.
        """
        group = rec.group_pvs or [rec.pv]
        now = self._reread_subsystems(group)
        if len(group) < 2:
            return
        nqns = {n for n in now.values() if n}
        changed = len(nqns) > 1 or any(not n for n in now.values()) \
            or (rec.nqn and nqns and nqns != {rec.nqn})
        if changed:
            detail = ", ".join(f"{self.pod_of_pv(p) or p}->{n or '?'}" for p, n in now.items())
            msg = (f"subsystem membership changed by the migration: was one subsystem "
                   f"({rec.nqn or '?'}) with {len(group)} volume(s), now {detail}")
            rec.verify_ok = False
            rec.verify_msg = (rec.verify_msg + "; " + msg) if rec.verify_msg else msg
            self.log.crit(f"MIGRATION VERIFY FAIL  {rec.name}: {msg}")

    def migration_loop(self, stop_at: float) -> None:
        # Keep launching migrations across almost the whole fio runtime. A migration
        # may finish shortly after fio stops; its wait is bounded by hard_deadline.
        hard_deadline = stop_at + self.a.migration_grace
        idx = 0
        while time.time() < stop_at - self.a.migration_gap:
            idx += 1
            try:
                self.run_one_migration(idx, hard_deadline)
            except subprocess.CalledProcessError as e:
                self.log.error(f"migration {idx} kubectl error: {e.stderr or e}")
            except Exception as e:  # noqa: BLE001
                self.log.error(f"migration {idx} unexpected error: {e}")
            # gap between migrations (one at a time)
            sleep_for = min(self.a.migration_gap, max(0, stop_at - time.time()))
            if sleep_for > 0:
                time.sleep(sleep_for)
        self.log.info(f"migration loop done ({idx} migration(s) attempted); "
                      "letting fio run out its remaining time")

    # ---- log collection & analysis ----------------------------------------------

    def collect_logs(self) -> None:
        self.log.info("collecting fio logs from pods ...")
        for pod in self.pods:
            dest = os.path.join(self.outdir, pod)
            os.makedirs(dest, exist_ok=True)
            cp = subprocess.run(
                ["kubectl", "-n", NAMESPACE, "cp", f"{pod}:/logs", dest],
                capture_output=True, text=True, timeout=180,
            )
            if cp.returncode != 0:
                self.log.warn(f"kubectl cp from {pod} failed: {cp.stderr.strip()}")
            else:
                self.log.info(f"    pulled logs for {pod} -> {dest}")

    # ---- cluster (spdk / control-plane) log + dmesg collection -------------------

    def collect_cluster_logs(self) -> None:
        """Collect spdk / spdk-proxy / operator / webappapi / tasks container logs, the
        fio test pods' own container logs, and dmesg for the storage workers, into the
        artifact dir (fio pod logs go to their per-pod subfolder).

        Container logs are read straight from each host's /var/log/pods via a privileged
        grabber pod (hostPath mount) instead of `kubectl logs`, so kubelet log rotation
        cannot truncate them: rotated + current segments (incl. .gz) are concatenated
        oldest-first. Best-effort — failures are logged, never fatal."""
        self.log.info("collecting cluster logs (spdk/proxy/operator/webappapi/tasks/fio/dmesg) ...")
        try:
            snode = self._list_pods(NAMESPACE, ["snode-spdk"])
            cplane = self._list_pods(SIMPLYBLOCK_NAMESPACE, ["operator", "webappapi", "tasks"])
            # the fio test pods are named "<run_id>-fio-<N>"; this matches only those
            # (not the "loggrab-<vm>-<run_id>" grabbers, which end with the run id).
            fiopods = self._list_pods(NAMESPACE, [f"{self.run_id}-fio"])
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cluster log collection: cannot list pods: {e}")
            return

        grabbers: dict = {}
        try:
            for node in sorted({p["node"] for p in snode + cplane + fiopods if p["node"]}):
                name = self._start_loggrab(node)
                if name:
                    grabbers[node] = name
            ready = self._wait_loggrab(list(grabbers.values()))
            grabbers = {n: g for n, g in grabbers.items() if g in ready}

            # spdk + proxy per storage-node pod -> spdk-<port>.txt / spdk-<port>-proxy.txt
            for p in snode:
                grab = grabbers.get(p["node"])
                if not grab:
                    continue
                port = self._snode_port(p["name"])
                for container, suffix in (("spdk-container", ""), ("spdk-proxy-container", "-proxy")):
                    dest = os.path.join(self.outdir, f"spdk-{port}{suffix}.txt")
                    with open(dest, "wb") as fh:
                        self._grab_container_logs(grab, NAMESPACE, p["name"], container, fh)

            # control-plane: operator / webappapi / tasks (all containers, headered)
            for key in ("operator", "webappapi", "tasks"):
                pods = [p for p in cplane if key in p["name"]]
                if not pods:
                    continue
                with open(os.path.join(self.outdir, f"{key}.txt"), "wb") as fh:
                    for p in pods:
                        grab = grabbers.get(p["node"])
                        if not grab:
                            continue
                        fh.write(f"==================== POD {p['name']} "
                                 f"({self._short(p['node'])}) ====================\n".encode())
                        for c in p["containers"]:
                            fh.write(f"-------------------- container {c} "
                                     f"--------------------\n".encode())
                            self._grab_container_logs(grab, SIMPLYBLOCK_NAMESPACE, p["name"], c, fh)

            # fio test pods' container stdout (install/start markers + per-5s eta/IOPS
            # status lines) -> the pod's own subfolder, alongside its /logs artifacts.
            for p in fiopods:
                grab = grabbers.get(p["node"])
                if not grab:
                    continue
                sub = os.path.join(self.outdir, p["name"])
                os.makedirs(sub, exist_ok=True)
                with open(os.path.join(sub, "fio.log"), "wb") as fh:
                    self._grab_container_logs(grab, NAMESPACE, p["name"], "fio", fh)

            # simplyblock cluster event log (status changes, migrations, node events)
            # via `sbctl cluster get-logs` inside a webappapi pod -> cluster-events.json
            self._collect_cluster_events()

            # dmesg for the storage workers via the privileged spdk-container
            for p in snode:
                dest = os.path.join(self.outdir, f"dmesg-{self._short(p['node'])}.txt")
                cp = subprocess.run(
                    ["kubectl", "-n", NAMESPACE, "exec", p["name"], "-c", "spdk-container",
                     "--", "dmesg", "-T"],
                    capture_output=True, check=False, timeout=120)
                with open(dest, "wb") as fh:
                    fh.write(cp.stdout)

            self.log.info(f"cluster logs collected into {self.outdir}")
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cluster log collection error: {e}")
        finally:
            if grabbers:
                subprocess.run(["kubectl", "-n", NAMESPACE, "delete", "pod",
                                *grabbers.values(), "--ignore-not-found", "--wait=false"],
                               capture_output=True, text=True, check=False, timeout=60)

    @staticmethod
    def _short(node: str) -> str:
        return node.split(".")[0]

    @staticmethod
    def _snode_port(podname: str) -> str:
        # snode-spdk-pod-<port>-<hash>
        parts = podname.split("-")
        return parts[3] if len(parts) > 3 and parts[3].isdigit() else podname

    @staticmethod
    def _list_pods(namespace: str, name_substrings: list[str]) -> list[dict]:
        cp = subprocess.run(["kubectl", "-n", namespace, "get", "pods", "-o", "json"],
                            capture_output=True, text=True, timeout=60)
        if cp.returncode != 0:
            raise RuntimeError(cp.stderr.strip())
        out = []
        for it in json.loads(cp.stdout).get("items", []):
            name = it["metadata"]["name"]
            if not any(s in name for s in name_substrings):
                continue
            out.append({
                "name": name,
                "node": it.get("spec", {}).get("nodeName", ""),
                "containers": [c["name"] for c in it.get("spec", {}).get("containers", [])],
            })
        return out

    def _start_loggrab(self, node: str) -> str:
        name = f"loggrab-{self._short(node)}-{self.run_id}"
        manifest = json.dumps({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"test": self.run_id}},
            "spec": {
                "nodeName": node, "restartPolicy": "Never",
                "tolerations": [{"operator": "Exists"}],
                "containers": [{
                    "name": "grab", "image": FIO_IMAGE, "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", "sleep 1800"],
                    # privileged + runAsUser:0 is required to read the host's
                    # /var/log/pods on OpenShift: without it the container runs as
                    # container_t (SELinux) and gets EACCES even as root, so every
                    # grab silently produced empty files.
                    "securityContext": {"privileged": True, "runAsUser": 0},
                    "volumeMounts": [{"name": "pods", "mountPath": "/podlogs", "readOnly": True}],
                }],
                "volumes": [{"name": "pods", "hostPath": {"path": "/var/log/pods"}}],
            },
        })
        cp = subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest,
                            capture_output=True, text=True, timeout=60)
        if cp.returncode != 0:
            self.log.warn(f"could not start loggrab on {node}: {cp.stderr.strip()}")
            return ""
        return name

    def _wait_loggrab(self, names: list[str]) -> set:
        ready = set()
        if not names:
            return ready
        subprocess.run(["kubectl", "-n", NAMESPACE, "wait", "--for=condition=Ready",
                        *[f"pod/{n}" for n in names], "--timeout=120s"],
                       capture_output=True, text=True, check=False, timeout=140)
        for n in names:  # confirm individually (wait returns non-zero if any one isn't ready)
            cp = subprocess.run(["kubectl", "-n", NAMESPACE, "get", "pod", n,
                                 "-o", "jsonpath={.status.phase}"],
                                capture_output=True, text=True, check=False, timeout=30)
            if cp.returncode == 0 and cp.stdout.strip() == "Running":
                ready.add(n)
            else:
                self.log.warn(f"loggrab {n} not Running; logs from its node may be missing")
        return ready

    @staticmethod
    def _host_dump_script(namespace: str, pod: str, container: str) -> str:
        # cat all rotated + current CRI log files for one container, oldest-first, gz-aware
        return (f'd=$(ls -d /podlogs/{namespace}_{pod}_*/{container}/ 2>/dev/null) || exit 0; '
                f'for f in $(ls -1tr "$d" 2>/dev/null); do '
                f'case "$f" in *.gz) zcat "$d$f" 2>/dev/null;; *) cat "$d$f";; esac; done')

    def _grab_container_logs(self, grabber: str, namespace: str, pod: str, container: str, fh) -> None:
        cp = subprocess.run(
            ["kubectl", "-n", NAMESPACE, "exec", grabber, "--", "sh", "-c",
             self._host_dump_script(namespace, pod, container)],
            capture_output=True, check=False, timeout=300)
        fh.write(cp.stdout)
        # the host dump script swallows read errors (2>/dev/null, || exit 0), so an
        # empty grab is otherwise indistinguishable from "no logs" — surface it.
        if not cp.stdout:
            detail = cp.stderr.decode(errors="replace").strip() if cp.stderr else ""
            self.log.warn(f"empty log grab for {namespace}/{pod}/{container} "
                          f"(rc={cp.returncode}{'; ' + detail if detail else ''})")

    def _collect_cluster_events(self) -> None:
        """Dump the simplyblock cluster event log (`sbctl cluster get-logs`) — cluster
        status changes, migration/node events — into cluster-events.json. Best-effort."""
        try:
            cluster = self.sbctl_cluster_uuid()
            pod = self.webappapi_pod()
        except Exception as e:  # noqa: BLE001
            self.log.warn(f"cluster events: cannot resolve cluster/webappapi: {e}")
            return
        cp = subprocess.run(
            ["kubectl", "-n", SIMPLYBLOCK_NAMESPACE, "exec", pod, "--",
             "sbctl", "cluster", "get-logs", cluster, "--json", "--limit=50000"],
            capture_output=True, text=True, timeout=120)
        if cp.returncode != 0:
            self.log.warn(f"sbctl cluster get-logs failed: {cp.stderr.strip()}")
            return
        dest = os.path.join(self.outdir, "cluster-events.json")
        with open(dest, "w") as fh:
            fh.write(cp.stdout)
        try:
            n = len(json.loads(cp.stdout))
            self.log.info(f"    cluster event log: {n} entries -> {dest}")
        except Exception:  # noqa: BLE001
            self.log.info(f"    cluster event log -> {dest}")

    @staticmethod
    def _read_fio_log(paths: list[str]) -> dict[int, dict[int, list[float]]]:
        """Parse fio time-series logs into {second: {ddir: [values, ...]}}.

        fio log line: `time_ms, value, ddir, bs, offset`  (ddir 0=read, 1=write).
        Values are aggregated across the per-job files (one file per numjobs thread).
        """
        out: dict[int, dict[int, list[float]]] = {}
        for path in paths:
            try:
                with open(path) as fh:
                    for line in fh:
                        parts = [p.strip() for p in line.split(",")]
                        if len(parts) < 3:
                            continue
                        try:
                            t = int(round(int(parts[0]) / 1000.0))
                            val = float(parts[1])
                            ddir = int(parts[2])
                        except ValueError:
                            continue
                        out.setdefault(t, {}).setdefault(ddir, []).append(val)
            except OSError:
                continue
        return out

    def _parse_timeline(self, pod_dir: str) -> dict[int, dict]:
        """Build a per-second timeline of IOPS and completion latency for one pod.

        Returns {second: {read_iops, write_iops, total_iops,
                          read_clat_us, write_clat_us, avg_clat_us}}.
        IOPS are summed across jobs+directions; clat is averaged across jobs and
        IOPS-weighted across read/write.
        """
        iops = self._read_fio_log(
            glob.glob(os.path.join(pod_dir, "**", "iops_iops.*log"), recursive=True))
        # clat (completion latency, ns) mirrors measure()'s clat percentiles
        clat = self._read_fio_log(
            glob.glob(os.path.join(pod_dir, "**", "lat_clat.*log"), recursive=True))

        timeline: dict[int, dict] = {}
        for t in sorted(set(iops) | set(clat)):
            ri = sum(iops.get(t, {}).get(0, []))
            wi = sum(iops.get(t, {}).get(1, []))
            rc_vals = clat.get(t, {}).get(0, [])
            wc_vals = clat.get(t, {}).get(1, [])
            rc = (sum(rc_vals) / len(rc_vals) / 1000.0) if rc_vals else 0.0  # ns -> us
            wc = (sum(wc_vals) / len(wc_vals) / 1000.0) if wc_vals else 0.0
            total = ri + wi
            avg = ((ri * rc + wi * wc) / total) if total else 0.0
            timeline[t] = {
                "read_iops": round(ri, 1),
                "write_iops": round(wi, 1),
                "total_iops": round(total, 1),
                "read_clat_us": round(rc, 1),
                "write_clat_us": round(wc, 1),
                "avg_clat_us": round(avg, 1),
            }
        return timeline

    def _parse_result_json(self, pod_dir: str) -> dict:
        for path in glob.glob(os.path.join(pod_dir, "**", "result.json"), recursive=True):
            try:
                with open(path) as fh:
                    return json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
        return {}

    def _detect_outages(self, timeline: dict[int, dict]) -> list[dict]:
        """Find maximal contiguous runs of 'down' seconds (total_iops <= stall_threshold,
        or a missing sample). The first second is skipped (ramp-up). Each run is
        {start, end, duration, recovered}; recovered is False when the run extends to the
        last observed second — i.e. I/O never came back. Caller filters by min duration."""
        if not timeline:
            return []
        tmax = max(timeline)
        runs: list[dict] = []
        start = None
        for t in range(1, tmax + 1):
            down = timeline.get(t, {}).get("total_iops", 0.0) <= self.a.stall_threshold
            if down and start is None:
                start = t
            elif not down and start is not None:
                runs.append({"start": start, "end": t - 1,
                             "duration": t - start, "recovered": True})
                start = None
        if start is not None:
            runs.append({"start": start, "end": tmax,
                         "duration": tmax - start + 1, "recovered": False})
        return runs

    @staticmethod
    def _read_fio_rc(pod_dir: str) -> "str | None":
        """Read fio's recorded exit code (written to /logs/fio.rc by the pod script)."""
        for path in (glob.glob(os.path.join(pod_dir, "**", "fio.rc"), recursive=True)
                     or glob.glob(os.path.join(pod_dir, "fio.rc"))):
            try:
                with open(path) as fh:
                    return fh.read().strip()
            except OSError:
                continue
        return None

    @staticmethod
    def _scan_verify_failures(pod_dir: str) -> list[str]:
        """Scan the pod's fio stdout/stderr (fio.log) for md5 verification failures.

        A non-empty result means fio read back data that did not match the md5 header
        it had written — i.e. the migration lost or corrupted data. This is the single
        most important signal of the test; it is reported separately from (and ranks
        above) I/O outages. fio prints these to stderr, e.g.:
          "verify: bad magic header", "fio: verify type mismatch", "md5: verify failed".
        """
        hits: list[str] = []
        for path in (glob.glob(os.path.join(pod_dir, "**", "fio.log"), recursive=True)
                     or glob.glob(os.path.join(pod_dir, "fio.log"))):
            try:
                with open(path, errors="replace") as fh:
                    for line in fh:
                        low = line.lower()
                        if ("verify" in low and any(k in low for k in
                                ("fail", "bad", "mismatch", "corrupt"))) \
                                or "bad magic header" in low:
                            hits.append(line.strip())
            except OSError:
                continue
        return hits

    def _attribute_corruption(self, hits: list[str]) -> list[dict]:
        """Place each fio verify failure on the migration timeline.

        A corrupted read only says *when* it happened; what makes it diagnosable is where
        that instant falls relative to the migration that was in flight — and within it,
        relative to the cutover the hosts actually observed (which the CR reports tens of
        seconds late). "3s after cutover" and "during the copy" are different bugs.
        """
        out = []
        for line in hits:
            stamp = line.split(" ", 1)[0]
            when = None
            try:  # CRI log prefix: 2026-08-11T10:10:33.696991802Z stderr F <msg>
                when = datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(
                    tzinfo=timezone.utc)
            except ValueError:
                pass
            where = "outside every migration window"
            migration, into, after_cutover = "", None, None
            if when:
                for m in self.migrations:
                    if not m.end or not (m.start <= when <= m.end):
                        continue
                    cut = sorted(m.ana_cutover.values())
                    migration = m.name
                    into = round((when - m.start).total_seconds(), 1)
                    where = f"during {m.name} (phase={m.phase}, {into:.0f}s in)"
                    if cut:
                        cut_dt = datetime.strptime(cut[0], "%Y-%m-%dT%H:%M:%SZ").replace(
                            tzinfo=timezone.utc)
                        after_cutover = round((when - cut_dt).total_seconds(), 1)
                        where += (f", {abs(after_cutover):.0f}s "
                                  f"{'AFTER' if after_cutover >= 0 else 'before'} the "
                                  f"host-observed cutover at {cut[0]}")
                    break
            out.append({"ts": stamp if when else "", "where": where, "line": line,
                        "migration": migration, "seconds_into_migration": into,
                        "seconds_after_cutover": after_cutover})
        return out

    @staticmethod
    def _overlaps_migration(start_s: int, base: datetime,
                            migs: list[MigrationRecord]) -> MigrationRecord | None:
        ts = base.timestamp() + start_s
        for m in migs:
            if m.end is None:
                continue
            if m.start.timestamp() <= ts <= m.end.timestamp():
                return m
        return None

    def analyze(self) -> bool:
        """Returns True if I/O was continuous (PASS), False on any I/O loss (FAIL)."""
        self.log.info("=" * 78)
        self.log.info("ANALYSIS — correlating IOPS/latency with migration windows")
        self.log.info("=" * 78)

        io_lost = False
        corruption_pods: list[str] = []  # pods where fio md5 verify detected data corruption
        report: dict = {"run_id": self.run_id, "pods": {}, "migrations": [], "health_events": []}

        completed_migs = [m for m in self.migrations if m.end is not None]

        base = self._io_start_time

        for pod in self.pods:
            pod_dir = os.path.join(self.outdir, pod)
            result = self._parse_result_json(pod_dir)
            fio_rc = self._read_fio_rc(pod_dir)
            timeline = self._parse_timeline(pod_dir)

            # --- emit the per-second IOPS + latency time series as CSV ---
            csv_path = self._write_timeseries_csv(pod, pod_dir, timeline, base, completed_migs)

            pv = self.pv_of.get(pod, "")
            pod_report: dict = {"pv": pv, "jobs": [],
                                "volume_kind": self.kind_of.get(pod, ""),
                                "subsystem_nqn": self.nqn_of.get(pv, ""),
                                "subsystem_members": len(self.group_of(pv)) if pv else 0,
                                "timeseries_csv": os.path.relpath(csv_path, self.outdir)}

            # --- latency / IOPS summary from fio JSON ---
            total_iops = 0.0
            # fio's per-job "error" is the errno of the LAST error, not a count — summing it
            # produces a meaningless number that reads like a tally (a single md5 mismatch
            # shows up as "84", which is EILSEQ, fio's errno for a data-integrity failure).
            # Keep the errno and its meaning; the count of verify failures is scanned from
            # the log separately.
            failed_jobs = 0
            errnos: set[int] = set()
            for job in result.get("jobs", []):
                rd, wr = job.get("read", {}), job.get("write", {})
                err = job.get("error", 0)
                if err:
                    failed_jobs += 1
                    errnos.add(err)
                total_iops += rd.get("iops", 0.0) + wr.get("iops", 0.0)

                def clat_us(io):
                    c = io.get("clat_ns", {})
                    pct = c.get("percentile", {}) or {}
                    return {
                        "mean_us": round(c.get("mean", 0) / 1000.0, 1),
                        "p50_us": round(pct.get("50.000000", 0) / 1000.0, 1),
                        "p99_us": round(pct.get("99.000000", 0) / 1000.0, 1),
                        "p99_9_us": round(pct.get("99.900000", 0) / 1000.0, 1),
                    }

                pod_report["jobs"].append({
                    "name": job.get("jobname"),
                    "error": err,
                    "read": {"iops": round(rd.get("iops", 0), 1), **clat_us(rd)},
                    "write": {"iops": round(wr.get("iops", 0), 1), **clat_us(wr)},
                })

            pod_report["total_iops"] = round(total_iops, 1)
            pod_report["fio_failed_jobs"] = failed_jobs
            pod_report["fio_errnos"] = sorted(
                f"{e} ({FIO_ERRNO_MEANING.get(e, 'errno')})" for e in errnos)
            # A checksum mismatch is corruption whether or not the log line survived; the
            # errno is the authoritative record, since fio sets it on the job itself while
            # the message only lands on stderr (which can be rotated or truncated away).
            integrity_errno = FIO_ERRNO_DATA_INTEGRITY in errnos
            pod_report["fio_data_integrity_error"] = integrity_errno
            io_errnos = sorted(e for e in errnos if e != FIO_ERRNO_DATA_INTEGRITY)

            # --- I/O continuity ---
            # A loss is a SUSTAINED outage: a contiguous run of >= outage_seconds where
            # IOPS stays at/below stall_threshold (missing seconds count as down). A
            # single missed log entry or a few zero-IOPS seconds is transient noise — a
            # brief dip while I/O keeps flowing — NOT a loss.
            runs = self._detect_outages(timeline)
            outages = [r for r in runs if r["duration"] >= self.a.outage_seconds]
            transient = [r for r in runs if r["duration"] < self.a.outage_seconds]
            for r in outages:
                mig = self._overlaps_migration(r["start"], base, completed_migs) if base else None
                r["migration"] = mig.name if mig else None
            pod_report["outages"] = outages
            pod_report["transient_dips"] = len(transient)
            pod_report["samples_total"] = len(timeline)
            # full per-second timeline (also written to CSV); kept in JSON for tooling
            pod_report["timeseries"] = [
                {"second": t, **timeline[t]} for t in sorted(timeline)]

            # --- verdicts ---
            # Hard failures: fio reported I/O errors, fio exited non-zero (it "died"),
            # or a sustained I/O outage. Transient dips / missing samples are not losses.
            # Data integrity: fio md5 verify mismatches mean the migration corrupted or
            # lost data — the most severe failure, ranked above I/O outages.
            verify_hits = self._scan_verify_failures(pod_dir)
            pod_report["verify_failures"] = len(verify_hits)
            pod_report["corruption_events"] = self._attribute_corruption(verify_hits)
            if verify_hits or integrity_errno:
                corruption_pods.append(pod)
                evs = pod_report["corruption_events"]
                # With verify_fatal=0 a single run can report thousands of mismatches; the
                # count and its spread are the measurement, so only the first few are shown
                # in full. All of them stay in report.json and are aggregated per migration
                # in the summary below.
                for ev in evs[:CORRUPTION_DETAIL_LIMIT]:
                    self.log.crit(f"CORRUPTION {pod} at {ev['ts'] or '?'}: {ev['where']}")
                    self.log.crit(f"    {ev['line'][:200]}")
                if len(evs) > CORRUPTION_DETAIL_LIMIT:
                    self.log.crit(f"CORRUPTION {pod}: {len(evs)} mismatches total, "
                                  f"{len(evs) - CORRUPTION_DETAIL_LIMIT} not shown "
                                  "(all in report.json → pods[].corruption_events)")
                if integrity_errno and not verify_hits:
                    # No log line to place on the timeline, but the verdict stands.
                    self.log.crit(
                        f"CORRUPTION {pod}: fio job ended with errno "
                        f"{FIO_ERRNO_DATA_INTEGRITY} (EILSEQ) — checksum mismatch on "
                        "read-back, with no verify message in the log (truncated?)")

            errored = (failed_jobs > 0 or (fio_rc not in (None, "", "0"))
                       or bool(verify_hits) or integrity_errno)
            problems = []
            if verify_hits or integrity_errno:
                detail = []
                if verify_hits:
                    detail.append(f"{len(verify_hits)} md5 verify failure(s)")
                if integrity_errno:
                    detail.append(f"fio errno {FIO_ERRNO_DATA_INTEGRITY} (EILSEQ)")
                problems.append(
                    "DATA INTEGRITY — CHECKSUM MISMATCH on read-back: " + ", ".join(detail))
            if io_errnos:
                problems.append(
                    f"{failed_jobs} fio job(s) ended in error: "
                    + ", ".join(f"{e} ({FIO_ERRNO_MEANING.get(e, 'errno')})" for e in io_errnos))
            if fio_rc not in (None, "", "0"):
                problems.append(f"fio exited with rc={fio_rc}")
            if outages:
                perm = [o for o in outages if not o["recovered"]]
                problems.append(
                    f"{len(outages)} sustained I/O outage(s) (>= {self.a.outage_seconds}s)"
                    + (f", {len(perm)} never recovered" if perm else ""))

            if errored or outages:
                io_lost = True
                self.log.crit(f"POD {pod}: " + "; ".join(problems))
                for o in outages[:10]:
                    tag = f"during {o['migration']}" if o.get("migration") else "no migration active"
                    rec = (f"recovered after {o['duration']}s"
                           if o["recovered"] else "NEVER RECOVERED")
                    self.log.crit(
                        f"    I/O OUTAGE +{o['start']}s..+{o['end']}s "
                        f"({o['duration']}s, {rec})  ({tag})")
            elif not timeline:
                self.log.warn(f"POD {pod}: no per-second samples collected — I/O "
                              "continuity could not be verified (inconclusive)")
            else:
                msg = (f"POD {pod}: OK  total_iops={pod_report['total_iops']:.0f}  "
                       f"samples={len(timeline)}  errors=0  no sustained outages")
                if transient:
                    msg += (f"  ({len(transient)} transient <{self.a.outage_seconds}s "
                            "dip(s) ignored)")
                self.log.info(msg)

            report["pods"][pod] = pod_report

        # health events (pod death / restart) are hard I/O-loss signals
        for ev in self.health_events:
            io_lost = True
            mig = None
            for m in completed_migs:
                if m.end and m.start <= ev.ts <= m.end:
                    mig = m.name
            tag = f"during migration {mig}" if mig else "outside any migration window"
            self.log.crit(f"HEALTH EVENT: {iso(ev.ts)} {ev.pod}: {ev.detail} ({tag})")
            report["health_events"].append(
                {"ts": iso(ev.ts), "pod": ev.pod, "detail": ev.detail, "migration": mig})

        for m in completed_migs + [x for x in self.migrations if x.end is None]:
            report["migrations"].append({
                "name": m.name, "pod": m.pod, "pv": m.pv, "vol": m.vol,
                "kind": self.kind_of.get(m.pod, ""),
                "source": m.source, "target": m.target,
                "phase": m.phase, "error": m.error,
                "subsystem_nqn": m.nqn, "subsystem_members": len(m.group_pvs),
                "group_pvs": m.group_pvs, "member_nodes": m.member_nodes,
                "split_group": m.split_group,
                "cr_subsystem_nqn": m.cr_nqn, "cr_member_count": m.cr_members,
                "cr_migration_uuid": m.cr_migration_uuid,
                "cr_match_ok": m.cr_match_ok, "cr_match_msg": m.cr_match_msg,
                "start": iso(m.start), "end": iso(m.end) if m.end else None,
                "duration_s": round((m.end - m.start).total_seconds(), 0) if m.end else None,
                "actual_node": m.actual_node, "verify_ok": m.verify_ok, "verify_msg": m.verify_msg,
                "pre_snapshot": m.pre_snapshot, "pre_snapshot_id": m.pre_snapshot_id,
                "snapshot_created_ok": m.snapshot_created_ok, "snapshot_post_ok": m.snapshot_post_ok,
                "snapshot_verify_msg": m.snapshot_verify_msg,
                "target_policy": m.target_policy, "target_policy_ok": m.target_policy_ok,
                "target_has_consumer": m.target_has_consumer,
                "target_consumers": m.target_consumers,
                "ana_ok": m.ana_ok, "ana_msgs": m.ana_msgs,
                "ana_samples": len(m.ana_samples), "ana_csv": m.ana_csv,
                "ana_max_stall_s": round(m.ana_stall_s, 1),
                "ana_observed_cutover": m.ana_cutover,
                "ana_cr_completed_lag_s": round(m.ana_cr_lag_s, 1),
            })

        # The subsystem grouping the whole verification rests on, as last resolved.
        report["subsystems"] = [
            {"nqn": nqn, "members": len(pvs),
             "pvs": pvs, "pods": [self.pod_of_pv(p) for p in pvs],
             "ns_ids": [self.ns_id_of.get(p) for p in pvs]}
            for nqn, pvs in sorted(self.subsystem_pvs.items(), key=lambda kv: -len(kv[1]))]

        # Migration placement verification: a completed migration must land on its target,
        # a failed one must stay on its source — and for a shared subsystem that holds for
        # every volume in it. Any mismatch is an irregularity.
        verify_failures = [m for m in self.migrations if m.verify_ok is False]
        report["verification_failures"] = [
            {"name": m.name, "pv": m.pv, "phase": m.phase, "expected":
             (m.target if m.phase == "Completed" else m.source),
             "actual": m.actual_node, "member_nodes": m.member_nodes,
             "split_group": m.split_group, "msg": m.verify_msg}
            for m in verify_failures]

        # Operator/backend agreement on which subsystem was migrated (CR status vs sbctl).
        cr_failures = [m for m in self.migrations if m.cr_match_ok is False]
        report["cr_subsystem_failures"] = [
            {"name": m.name, "pv": m.pv, "phase": m.phase, "subsystem_nqn": m.nqn,
             "cr_subsystem_nqn": m.cr_nqn, "cr_member_count": m.cr_members,
             "msg": m.cr_match_msg}
            for m in cr_failures]

        # Host-side path behaviour (ANA samples taken on every consuming node): the target
        # must not serve before cutover, must serve every namespace after it, and no
        # namespace may be left without an accessible path for long.
        ana_failures = [m for m in self.migrations if m.ana_ok is False]
        report["ana_failures"] = [
            {"name": m.name, "pv": m.pv, "phase": m.phase, "subsystem_nqn": m.nqn,
             "target_has_consumer": m.target_has_consumer,
             "max_stall_s": round(m.ana_stall_s, 1), "csv": m.ana_csv, "msgs": m.ana_msgs}
            for m in ana_failures]

        # The target-selection discriminator: how the run split between migrating onto a
        # node that already consumes the subsystem and onto one that does not. Without this
        # split a failure that only occurs in one of the two cases reads as intermittent.
        by_discriminator: dict[str, dict] = {}
        for m in self.migrations:
            key = ("target_hosts_consumer" if m.target_has_consumer
                   else "target_hosts_no_consumer" if m.target_has_consumer is False
                   else "unknown")
            st = by_discriminator.setdefault(
                key, {"attempted": 0, "completed": 0, "ana_failed": 0, "corrupted_pods": []})
            st["attempted"] += 1
            if m.phase == "Completed":
                st["completed"] += 1
            if m.ana_ok is False:
                st["ana_failed"] += 1
        report["migrations_by_target_policy"] = {
            "policy": self.a.target_policy,
            "unsatisfied_picks": len([m for m in self.migrations if not m.target_policy_ok]),
            "split": by_discriminator,
        }

        # Every kind that was attempted must have migrated successfully at least once. A
        # kind that fails *every* time (e.g. the backend rejecting that shape of request)
        # otherwise slips through: each failed migration leaves its volume on the source,
        # which the placement check then happily confirms as correct-for-a-failure.
        kind_stats: dict[str, dict] = {}
        for m in self.migrations:
            kind = self.kind_of.get(m.pod, "?")
            st = kind_stats.setdefault(kind, {"attempted": 0, "completed": 0, "errors": []})
            st["attempted"] += 1
            if m.phase == "Completed":
                st["completed"] += 1
            elif m.error:
                st["errors"].append(f"{m.name}: {m.error}")
        report["migrations_by_kind"] = kind_stats
        never_completed = {k: st for k, st in kind_stats.items()
                           if st["attempted"] and not st["completed"]}

        # A run where no subsystem was ever shared did not test batch migration at all,
        # even if every single-namespace migration passed — report it rather than let a
        # vacuous PASS imply multi-namespace coverage.
        ns_migs = [m for m in self.migrations if len(m.group_pvs) > 1]
        shared_subsystems = [pvs for pvs in self.subsystem_pvs.values() if len(pvs) > 1]
        report["multi_namespace"] = {
            "ns_pods": self.a.ns_pods,
            "ns_per_subsys": self.a.ns_per_subsys,
            "shared_subsystems": len(shared_subsystems),
            "migrations_of_shared_subsystems": len(ns_migs),
        }
        ns_untested = (self.a.ns_pods > 1 and not self.a.no_migrations
                       and (not shared_subsystems or not ns_migs))
        if ns_untested:
            report["multi_namespace"]["problem"] = (
                f"{self.a.ns_pods} namespaced volume(s) were provisioned from "
                f"{NS_STORAGECLASS} ({PARAM_MAX_NS_PER_SUBSYS}={self.a.ns_per_subsys}) but "
                + ("no two of them ended up sharing a subsystem"
                   if not shared_subsystems
                   else "no migration of a shared subsystem completed")
                + " — multi-namespace migration was NOT exercised")

        # Snapshot verification: a snapshot taken before a migration must resolve via sbctl
        # both right after creation and after the migration finishes. Either False = failure.
        snapshot_failures = [m for m in self.migrations
                             if m.snapshot_created_ok is False or m.snapshot_post_ok is False]
        report["snapshot_failures"] = [
            {"name": m.name, "pv": m.pv, "snapshot": m.pre_snapshot,
             "snapshot_id": m.pre_snapshot_id, "created_ok": m.snapshot_created_ok,
             "post_migration_ok": m.snapshot_post_ok, "msg": m.snapshot_verify_msg}
            for m in snapshot_failures]

        # Guard against a false PASS on no data: if not a single pod produced per-second
        # samples, the run measured nothing (e.g. fio never left layout) and must not be
        # reported as continuous I/O.
        total_samples = sum(pr.get("samples_total", 0) for pr in report["pods"].values())
        no_data = total_samples == 0

        report["data_corruption_pods"] = corruption_pods

        # Per-migration corruption aggregate. With verify_fatal=0 every lost write that gets
        # re-read is reported, so the count per migration bounds how wide the interval was in
        # which acknowledged writes were lost — divide by the pod's write rate to get seconds.
        # The spread relative to the host-observed cutover says when the loss happened: a
        # tight cluster just after cutover is a cutover-instant loss, a wide spread means a
        # long unprotected interval.
        by_mig: dict[str, dict] = {}
        for pod, pr in report["pods"].items():
            for ev in pr.get("corruption_events", []):
                key = ev.get("migration") or "outside-any-migration"
                st = by_mig.setdefault(key, {"blocks": 0, "pods": {}, "after_cutover_s": []})
                st["blocks"] += 1
                st["pods"][pod] = st["pods"].get(pod, 0) + 1
                if ev.get("seconds_after_cutover") is not None:
                    st["after_cutover_s"].append(ev["seconds_after_cutover"])
        for key, st in by_mig.items():
            deltas = st.pop("after_cutover_s")
            st["vs_cutover_s"] = ({"first": min(deltas), "last": max(deltas),
                                   "n_timed": len(deltas)} if deltas else None)
            mig = next((m for m in self.migrations if m.name == key), None)
            # Writes per second summed over the affected pods, so blocks/rate ≈ the width of
            # the lossy interval if every write in it was lost.
            rate = sum(report["pods"].get(p, {}).get("jobs", [{}])[0]
                       .get("write", {}).get("iops", 0.0) for p in st["pods"])
            st["write_iops_affected_pods"] = round(rate, 1)
            st["implied_lossy_window_s"] = round(st["blocks"] / rate, 3) if rate else None
            st["subsystem_members"] = len(mig.group_pvs) if mig else None
            st["verify_fatal"] = bool(self.a.verify_fatal)
        report["corruption_by_migration"] = by_mig

        if corruption_pods:
            report["result"] = ("FAIL — DATA INTEGRITY: CHECKSUM MISMATCH on read-back on "
                                f"{len(corruption_pods)} pod(s)")
            ok = False
        elif io_lost:
            report["result"] = "FAIL — I/O LOSS DETECTED"
            ok = False
        elif verify_failures:
            split = [m for m in verify_failures if m.split_group]
            report["result"] = (f"FAIL — {len(verify_failures)} migration placement "
                                f"irregularity(ies)"
                                + (f", {len(split)} with a subsystem split across nodes"
                                   if split else ""))
            ok = False
        elif cr_failures:
            report["result"] = (f"FAIL — {len(cr_failures)} migration(s) where the operator's "
                                "subsystem view disagreed with the backend")
            ok = False
        elif ana_failures:
            early = [m for m in ana_failures
                     if any("BEFORE cutover" in x for x in m.ana_msgs)]
            report["result"] = (
                f"FAIL — {len(ana_failures)} migration(s) with misbehaving host paths"
                + (f", {len(early)} where the target served before cutover" if early else ""))
            ok = False
        elif never_completed:
            report["result"] = ("FAIL — no migration of "
                                + " or ".join(f"{k} volume(s) ({st['attempted']} attempted)"
                                              for k, st in never_completed.items())
                                + " ever completed")
            ok = False
        elif snapshot_failures:
            report["result"] = (f"FAIL — {len(snapshot_failures)} snapshot(s) did not resolve "
                                 "via sbctl after creation/migration")
            ok = False
        elif no_data:
            report["result"] = "INCONCLUSIVE — no fio samples collected (run produced no data)"
            ok = False
        elif ns_untested:
            report["result"] = ("INCONCLUSIVE — multi-namespace migration was not exercised "
                               "(no shared subsystem was migrated)")
            ok = False
        else:
            report["result"] = "PASS — I/O CONTINUOUS"
            ok = True

        report_path = os.path.join(self.outdir, "report.json")
        with open(report_path, "w") as fh:
            json.dump(report, fh, indent=2)
        self.log.info(f"wrote machine-readable report -> {report_path}")

        self._print_summary(report, io_lost)
        for key, st in sorted(report.get("corruption_by_migration", {}).items()):
            vs = st["vs_cutover_s"]
            self.log.crit(
                f"CORRUPTION SUMMARY  {key}: {st['blocks']} block(s) across "
                + ", ".join(f"{p.rsplit('-', 1)[-1]}={c}" for p, c in sorted(st["pods"].items()))
                + (f"  |  first {vs['first']:.0f}s / last {vs['last']:.0f}s "
                   "relative to the host-observed cutover" if vs else "")
                + (f"  |  {st['write_iops_affected_pods']:.0f} write IOPS over those pods "
                   f"=> lossy interval ~{st['implied_lossy_window_s']:.3f}s if every write "
                   "in it was lost" if st["implied_lossy_window_s"] else "")
                + ("  |  NOTE: --verify-fatal was set, so each pod stopped at its first "
                   "mismatch — these counts are lower bounds" if st["verify_fatal"] else ""))
        if corruption_pods:
            self.log.crit(
                "DATA INTEGRITY — CHECKSUM MISMATCH: fio read back blocks that did not match "
                f"the checksums it wrote, on pod(s): {', '.join(corruption_pods)}. The reads "
                "SUCCEEDED (no I/O error), so this is silent corruption: the migration served "
                "data that was never written at those offsets. Inspect each pod's fio.log for "
                "the mismatched offsets and the *.hdr_fail dumps.")
        if verify_failures:
            self.log.crit(f"MIGRATION VERIFICATION: {len(verify_failures)} irregularity(ies) — "
                          "volume(s) not on the expected node after migration:")
            for m in verify_failures:
                self.log.crit(f"    {m.name}: {m.verify_msg}")
        if cr_failures:
            self.log.crit(f"SUBSYSTEM VIEW: {len(cr_failures)} migration(s) where the CR's "
                          "subsystemNQN/memberCount disagreed with the backend — the operator "
                          "may have migrated a different set of volumes than it reported:")
            for m in cr_failures:
                self.log.crit(f"    {m.name}: {m.cr_match_msg}")
        if ana_failures:
            self.log.crit(f"HOST PATH VERIFICATION: {len(ana_failures)} migration(s) where the "
                          "consuming hosts' NVMe paths did not behave as a migration requires "
                          "(sampled ANA states per node):")
            for m in ana_failures:
                for msg in m.ana_msgs:
                    self.log.crit(f"    {m.name}: {msg}")
                if m.ana_csv:
                    self.log.crit(f"    {m.name}: samples -> {m.ana_csv}")
        if never_completed:
            for kind, st in never_completed.items():
                self.log.crit(
                    f"MIGRATION KIND {kind}: {st['attempted']} attempted, 0 completed — every "
                    f"migration of this volume kind failed:")
                for err in st["errors"][:5]:
                    self.log.crit(f"    {err}")
        if ns_untested:
            self.log.crit("MULTI-NAMESPACE: " + report["multi_namespace"]["problem"])
        if snapshot_failures:
            self.log.crit(f"SNAPSHOT VERIFICATION: {len(snapshot_failures)} snapshot(s) failed to "
                          "resolve via sbctl after creation and/or migration:")
            for m in snapshot_failures:
                self.log.crit(f"    {m.name} (snapshot {m.pre_snapshot}): {m.snapshot_verify_msg}")
        if no_data and not io_lost and not verify_failures and not cr_failures:
            self.log.crit("RESULT: INCONCLUSIVE — no per-second samples collected from any "
                          "pod; cannot confirm I/O continuity")
        return ok

    def _write_timeseries_csv(self, pod: str, pod_dir: str, timeline: dict[int, dict],
                              base: "datetime | None",
                              migs: list[MigrationRecord]) -> str:
        """Write the per-second IOPS + latency time series for one pod to CSV.

        Columns: second, wall_clock, total_iops, read_iops, write_iops,
                 read_clat_us, write_clat_us, avg_clat_us, active_migration.
        The active_migration column names the migration (if any) in flight that
        second, enabling direct correlation of IOPS/latency with migrations.
        """
        path = os.path.join(pod_dir, "timeseries.csv")
        header = ("second,wall_clock,total_iops,read_iops,write_iops,"
                  "read_clat_us,write_clat_us,avg_clat_us,active_migration\n")
        with open(path, "w") as fh:
            fh.write(header)
            for t in sorted(timeline):
                row = timeline[t]
                wall = iso(datetime.fromtimestamp(base.timestamp() + t, tz=timezone.utc)) \
                    if base else ""
                mig = self._overlaps_migration(t, base, migs) if base else None
                fh.write(
                    f"{t},{wall},{row['total_iops']},{row['read_iops']},{row['write_iops']},"
                    f"{row['read_clat_us']},{row['write_clat_us']},{row['avg_clat_us']},"
                    f"{mig.name if mig else ''}\n")
        return path

    def _print_summary(self, report: dict, io_lost: bool) -> None:
        line = "=" * 78
        self.log.info(line)
        self.log.info("SUMMARY")
        self.log.info(line)
        self.log.info(f"run id            : {self.run_id}")
        single_pods = len([p for p in self.pods if self.kind_of.get(p) == "single"])
        ns_pods = len(self.pods) - single_pods
        self.log.info(f"pods              : {len(self.pods)} "
                      f"({single_pods} single-namespace / {ns_pods} namespaced)")
        shared = [pvs for pvs in self.subsystem_pvs.values() if len(pvs) > 1]
        self.log.info(f"subsystems        : {len(self.subsystem_pvs)} total / {len(shared)} shared "
                      f"(sizes: {', '.join(str(len(p)) for p in sorted(shared, key=len, reverse=True)) or '-'})")
        self.log.info(f"migrations        : {len([m for m in self.migrations if m.end])} "
                      f"completed-window / {len(self.migrations)} attempted")
        completed = len([m for m in self.migrations if m.phase == 'Completed'])
        self.log.info(f"  of which phase=Completed: {completed}")
        batch = [m for m in self.migrations if len(m.group_pvs) > 1]
        solo = [m for m in self.migrations if len(m.group_pvs) <= 1]
        moved = sum(len(m.group_pvs) for m in self.migrations if m.phase == "Completed")
        self.log.info(f"  by subsystem size       : {len(solo)} single-namespace / "
                      f"{len(batch)} multi-namespace (batch)")
        self.log.info(f"  volumes moved (Completed): {moved}")
        for kind, st in sorted(report.get("migrations_by_kind", {}).items()):
            self.log.info(f"  {kind:>12} volumes   : {st['completed']} completed "
                          f"/ {st['attempted']} attempted")
        verified = len([m for m in self.migrations if m.verify_ok is True])
        vfail = len([m for m in self.migrations if m.verify_ok is False])
        split = len([m for m in self.migrations if m.split_group])
        self.log.info(f"placement verified: {verified} ok / {vfail} irregular "
                      f"/ {len(self.migrations)} total"
                      + (f"  ({split} with a SPLIT subsystem)" if split else ""))
        crok = len([m for m in self.migrations if m.cr_match_ok is True])
        crfail = len([m for m in self.migrations if m.cr_match_ok is False])
        self.log.info(f"subsystem view    : {crok} CR/backend agree / {crfail} disagree "
                      "(status.subsystemNQN + memberCount vs sbctl)")
        pol = report.get("migrations_by_target_policy", {})
        if pol:
            self.log.info(f"target policy     : {pol.get('policy')} "
                          f"({pol.get('unsatisfied_picks', 0)} pick(s) could not honour it)")
            for key, st in sorted(pol.get("split", {}).items()):
                self.log.info(f"  {key:>26}: {st['completed']} completed / "
                              f"{st['attempted']} attempted / "
                              f"{st['ana_failed']} with host-path violations")
        sampled = [m for m in self.migrations if m.ana_samples]
        if sampled:
            aok = len([m for m in sampled if m.ana_ok is True])
            afail = len([m for m in sampled if m.ana_ok is False])
            worst = max((m.ana_stall_s for m in sampled), default=0.0)
            self.log.info(f"host paths (ANA)  : {aok} ok / {afail} violated "
                          f"/ {len(sampled)} sampled  "
                          f"(worst namespace stall {worst:.0f}s; per-migration CSVs in ana/)")
        snapped = [m for m in self.migrations if m.pre_snapshot]
        if snapped:
            sok = len([m for m in snapped
                      if m.snapshot_created_ok is True and m.snapshot_post_ok is True])
            sfail = len([m for m in snapped
                        if m.snapshot_created_ok is False or m.snapshot_post_ok is False])
            self.log.info(f"snapshots         : {len(snapped)} taken / {sok} resolved ok "
                          f"(create+post-migration) / {sfail} failed")
        self.log.info(f"health events     : {len(self.health_events)}")
        self.log.info("")
        self.log.info("per-pod IOPS / latency (clat):")
        for pod, pr in report["pods"].items():
            rd = pr["jobs"][0]["read"] if pr["jobs"] else {}
            wr = pr["jobs"][0]["write"] if pr["jobs"] else {}
            members = pr.get("subsystem_members", 0)
            self.log.info(
                f"  {pod} [{pr.get('volume_kind','?')}, subsystem of {members}]: "
                f"total_iops={pr.get('total_iops',0):>9.0f} "
                f"failed_jobs={pr.get('fio_failed_jobs', 0)} "
                f"outages={len(pr.get('outages',[]))} "
                f"dips={pr.get('transient_dips',0)} | "
                f"read p99={rd.get('p99_us','-')}us write p99={wr.get('p99_us','-')}us")
        self.log.info("")
        self.log.info("per-second IOPS + latency time series (1s granularity):")
        for pod, pr in report["pods"].items():
            self.log.info(f"  {pod}: {pr.get('samples_total',0)} samples -> "
                          f"{pr.get('timeseries_csv','?')}")
        self.log.info("")
        self.log.info(f"artifacts         : {self.outdir}")
        self.log.info("  report.json           full machine-readable report (+ embedded timeseries)")
        self.log.info("  <pod>/timeseries.csv  per-second IOPS & clat latency, with active migration")
        self.log.info("  <pod>/result.json     fio final JSON summary")
        self.log.info("  <pod>/fio.log         fio pod container stdout (eta/IOPS status lines)")
        self.log.info("  test.log              full event log (migration start/stop, I/O-loss events)")
        self.log.info("  spdk-<port>[-proxy].txt / {operator,webappapi,tasks}.txt / dmesg-<vm>.txt")
        self.log.info("                        full host-sourced cluster logs (no rotation cut-off)")
        self.log.info(line)
        if io_lost:
            self.log.crit("RESULT: FAIL — I/O LOSS DETECTED (see CRITICAL lines above)")
        else:
            self.log.info("RESULT: PASS — I/O remained continuous across all migrations")
        self.log.info(line)

    # ---- cleanup ----------------------------------------------------------------

    def cleanup(self) -> None:
        if self.a.keep:
            self.log.info(f"--keep set; leaving resources (label test={self.run_id})")
            return
        self.log.info("cleaning up pods, PVCs, migration CRs and snapshots ...")
        kubectl(["delete", "volumemigration", "-l", f"test={self.run_id}",
                 "--ignore-not-found", "--wait=false"], check=False)
        kubectl(["delete", "volumesnapshot", "-l", f"test={self.run_id}",
                 "--ignore-not-found", "--wait=false"], check=False, timeout=180)
        kubectl(["delete", "pod", "-l", f"test={self.run_id}",
                 "--ignore-not-found", "--grace-period=5"], check=False, timeout=180)
        kubectl(["delete", "pvc", "-l", f"test={self.run_id}",
                 "--ignore-not-found"], check=False, timeout=180)
        self.log.info("cleanup done (both xfs StorageClasses kept for reuse; each run "
                      "recreates them)")

    # ---- orchestration ----------------------------------------------------------

    _io_start_time: datetime | None = None

    def run(self) -> int:
        self.log.info(f"=== fio migration test  run_id={self.run_id} ===")
        self.log.info(f"pods={self.a.pods} single-namespace + {self.a.ns_pods} namespaced "
                      f"({PARAM_MAX_NS_PER_SUBSYS}={self.a.ns_per_subsys})  "
                      f"volume={self.a.volume_size_gb}Gi xfs runtime={self.a.runtime}s "
                      f"fio(direct={FIO_DIRECT},ioengine={FIO_IOENGINE},"
                      f"iodepth={self.a.iodepth},numjobs={self.a.numjobs})")
        try:
            self.discover_nodes()
            self.ensure_storageclasses()
            self.create_workload()
            self.wait_pods_running()
            self.resolve_pvs()
            # Which volumes share an NVMe subsystem — the grouping every migration and
            # its verification is built on. Needed for the report even without
            # migrations, so resolve it unconditionally.
            self.resolve_subsystems()
            self.log_subsystems()
            self.warn_if_not_namespaced()
            if not self.a.no_migrations:
                # build the sbctl hostname->node map and log each volume's current node
                self.resolve_current_nodes()
                self.log.info("initial volume placement (authoritative, via sbctl):")
                for pod in self.pods:
                    pv = self.pv_of[pod]
                    node = self.placement.get(pv, "")
                    self.log.info(f"    {pod}  {pv}  on  {node or '?'} "
                                  f"({self.node_host.get(node, '?')})  "
                                  f"subsystem of {len(self.group_of(pv))}")
                if self.a.snapshot_chance > 0:
                    self.ensure_snapshot_class()
                    self.log.info(f"snapshots enabled: {self.a.snapshot_chance:.0%} chance "
                                  "to snapshot a volume before migrating it")
                # Which node each fio pod runs on drives both the target-selection policy
                # and the set of nodes the ANA sampler watches; the CSI node pods are the
                # windows it samples through.
                nodes_of_pods = self.pod_nodes()
                self.log.info("fio pod placement (consumers per node):")
                for node in sorted(set(nodes_of_pods.values())):
                    pods_here = sorted(p for p, n in nodes_of_pods.items() if n == node)
                    self.log.info(f"    {node}: {', '.join(pods_here)}")
                if self.a.ana_interval > 0:
                    self._refresh_csi_node_pods()
                    missing = sorted(set(nodes_of_pods.values()) - set(self.csi_node_pod_of))
                    if missing:
                        self.log.warn("no CSI node pod found on " + ", ".join(missing)
                                      + " — host paths on those nodes cannot be sampled")
                else:
                    self.log.info("ANA sampling disabled (--ana-interval 0)")
            self.wait_io_flowing()
            self._io_start_time = now_utc()

            self.start_health_monitor()
            # fio started roughly when I/O began flowing; stop migrating with enough
            # slack for the last migration to finish before fio exits.
            stop_at = self._io_start_time.timestamp() + self.a.runtime
            if self.a.no_migrations:
                # Pure-load mode: run fio for the full duration with NO migrations, to
                # isolate fio-load latency from migration-induced latency.
                self.log.info("--no-migrations set: running fio load only "
                              "(no VolumeMigration CRs created) ...")
            else:
                self.log.info("entering migration loop ...")
                self.migration_loop(stop_at)

            # wait out fio's remaining runtime
            remaining = stop_at - time.time()
            if remaining > 0:
                self.log.info(f"waiting {remaining:.0f}s for fio to finish ...")
                time.sleep(remaining + 15)
            self.wait_fio_exit()
        finally:
            self._fio_finished.set()
            self.stop_health_monitor()

        ok = False
        try:
            self.collect_logs()
            self.collect_cluster_logs()
            ok = self.analyze()
        finally:
            self.cleanup()
        return 0 if ok else 1

    def wait_fio_exit(self, timeout: int = 180) -> None:
        self.log.info("waiting for fio to exit in all pods ...")
        deadline = time.time() + timeout
        pending = set(self.pods)
        while pending and time.time() < deadline:
            for pod in list(pending):
                cp = kubectl(["exec", pod, "--", "sh", "-c", "cat /logs/fio.rc 2>/dev/null"],
                             check=False, timeout=30)
                if cp.returncode == 0 and cp.stdout.strip() != "":
                    rc = cp.stdout.strip()
                    if rc != "0":
                        self.log.warn(f"{pod}: fio exited with rc={rc}")
                    pending.discard(pod)
            if pending:
                time.sleep(5)
        self._fio_finished.set()
        if pending:
            self.log.warn(f"fio exit not confirmed for: {', '.join(sorted(pending))}")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pods", type=int, default=10,
                   help="number of fio pods on single-namespace volumes (default 10)")
    p.add_argument("--ns-pods", type=int, default=6,
                   help="number of fio pods on namespaced volumes — volumes that share an "
                        "NVMe subsystem with siblings and therefore migrate together as a "
                        "batch. Migrations alternate between the two kinds. 0 disables the "
                        "multi-namespace part of the test (default 6)")
    p.add_argument("--ns-per-subsys", type=int, default=NS_PER_SUBSYS,
                   help=f"{PARAM_MAX_NS_PER_SUBSYS} of the namespaced StorageClass, i.e. how "
                        f"many volumes may share one subsystem (default {NS_PER_SUBSYS}). The "
                        "control plane decides the actual packing; the test discovers it from "
                        "the backend")
    p.add_argument("--volume-size-gb", type=int, default=10, help="volume size in GiB (default 10)")
    p.add_argument("--file-size-gb", type=int, default=1,
                   help="fio data file size in GiB; kept small so the up-front layout is "
                        "near-instant (default 1, capped at volume-size-gb minus 2)")
    p.add_argument("--runtime", type=int, default=600, help="fio runtime seconds (default 600 = 10min)")
    p.add_argument("--iodepth", type=int, default=FIO_IODEPTH,
                   help=f"fio queue depth per job (default {FIO_IODEPTH})")
    p.add_argument("--numjobs", type=int, default=FIO_NUMJOBS,
                   help=f"fio parallel jobs per pod (default {FIO_NUMJOBS})")
    p.add_argument("--migration-gap", type=int, default=15,
                   help="seconds to wait between migrations (default 15)")
    p.add_argument("--migration-timeout", type=int, default=420,
                   help="max seconds to wait for one migration (default 420)")
    p.add_argument("--migration-grace", type=int, default=120,
                   help="extra seconds past fio runtime to let a late migration finish (default 120)")
    p.add_argument("--migration-poll", type=int, default=5,
                   help="migration status poll interval seconds (default 5)")
    p.add_argument("--verify-fatal", action="store_true",
                   help="stop each fio job at its FIRST checksum mismatch. Default is to keep "
                        "going, so every corrupted block is reported and the count measures how "
                        "many acknowledged writes were lost (see --ana-interval for the paired "
                        "path-state evidence)")
    p.add_argument("--target-policy", default="alternate",
                   choices=["alternate", "consumer", "no-consumer", "random"],
                   help="whether the migration target node may also run a pod consuming the "
                        "migrated subsystem: 'consumer' always picks such a node, "
                        "'no-consumer' never does, 'alternate' (default) switches per "
                        "migration, 'random' ignores the distinction")
    p.add_argument("--ana-interval", type=float, default=2.0,
                   help="seconds between host-side ANA samples taken on every consuming "
                        "node through the CSI node pods (0 disables sampling)")
    p.add_argument("--ana-stall-crit", type=float, default=60.0,
                   help="fail a migration if some namespace had no accessible path on a "
                        "consuming node for longer than this (seconds)")
    p.add_argument("--snapshot-chance", type=float, default=SNAPSHOT_CHANCE,
                   help="probability (0..1) of taking a VolumeSnapshot of a volume just "
                        "before migrating it, so the migration must carry the snapshot; the "
                        "snapshot id is validated via sbctl after creation and after the "
                        f"migration (default {SNAPSHOT_CHANCE}; 0 disables snapshots)")
    p.add_argument("--health-poll", type=int, default=3,
                   help="pod health poll interval seconds (default 3)")
    p.add_argument("--stall-threshold", type=float, default=0.0,
                   help="IOPS at/below this marks a 1s sample as 'down' (default 0)")
    p.add_argument("--outage-seconds", type=int, default=30,
                   help="minimum consecutive 'down' seconds to count as a real I/O outage; "
                        "shorter dips are transient noise, not a loss (default 30)")
    p.add_argument("--no-migrations", action="store_true",
                   help="run fio load only — skip the migration loop (no VolumeMigration CRs). "
                        "Use to isolate whether latency spikes come from fio load or from "
                        "the migrations themselves.")
    p.add_argument("--keep", action="store_true",
                   help="do not delete pods/PVCs/migrations after the run")
    p.add_argument("--outdir", default=None, help="artifact directory (default ./fio-mig-<ts>)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    outdir = args.outdir or os.path.abspath(f"fio-mig-{int(time.time())}")
    os.makedirs(outdir, exist_ok=True)
    log = Logger(os.path.join(outdir, "test.log"))
    test = FioMigrationTest(args, log, outdir)
    try:
        return test.run()
    except SystemExit as e:
        log.error(f"aborting: {e}")
        return 2
    except KeyboardInterrupt:
        log.warn("interrupted — attempting cleanup")
        test.cleanup()
        return 130


if __name__ == "__main__":
    sys.exit(main())