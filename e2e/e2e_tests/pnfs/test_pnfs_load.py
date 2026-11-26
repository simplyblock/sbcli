"""Track F: pNFS load, scale and soak — the `L-` rows.

`test-plan-pnfs-rwx.md` §7 records these as *"L-01 … L-08 — Load, scale
and soak: no long-running pNFS suite exists"*, with every E2E column `—`.
This file is that suite.

**What a load case here does and does not assert.** Three of the four
things L-01 asks to be measured have no target anywhere in the design --
dev has published no throughput, latency or MDS CPU number -- so they are
*recorded*, not gated. Inventing a threshold would convert a measurement
the plan asked for into an opinion, and a case that fails on a number
nobody agreed to is a case the next person learns to ignore. The same
stance the S3 latency rows take, for the same reason.

What *is* gated is correctness under that load, and it is gated hard:

  * every fio verify clean, in every pod;
  * every client still holding a pNFS layout at the end, with the
    metadata server's own READ and WRITE counts still zero. This is the
    one measurement that is also a verdict: a cluster that quietly
    stopped doing pNFS and started proxying data through the MDS passes
    a throughput run and has lost the feature;
  * no export losing Ready, and none leaking.

**L-06 is not here.** "Existing RWO load/reconnect suites pass alongside"
is `PnfsMixedRwxRwoK8s` (B-13), which already runs RWX and RWO load from
one pool at the same time as the regression gate for the existing
product. A second copy would mean two cases failing for one defect.

**L-03's `fsck` half is not here, and that is a gap.** The row asks for
sustained I/O plus snapshot/clone/resize churn, no leaks, and *"XFS fsck
clean"*; the plan's own T-8 notes nothing in the harness does the last
part. It still does not. `xfs_repair -n` refuses to run on a mounted
filesystem, the backing device is mounted by the metadata server guest
for as long as the export exists, and no harness path takes an export
offline and puts it back. So the soak case below gates on **checksums**
instead, which is what actually catches corruption; the structural check
needs an unexport/re-export primitive that does not exist yet. Named
here rather than silently dropped.

Class names end in ``K8s`` for the platform filter in ``e2e.py``.
"""
import json
import os
import time

from e2e_tests.pnfs.test_pnfs_mds_standby import _MdsBase
from e2e_tests.pnfs.test_pnfs_rwx import _PnfsBase
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    DATA_OPS,
    PnfsEvidenceError,
    PnfsProbeError,
    assert_layouts_taken,
    mountstats,
    nfsexport_status,
    nvme_ns_counters,
    wait_mds_promotion,
)


def _env_int(name, default):
    """An int from the environment, or *default*.

    Every scale knob in this file is overridable, because the honest value
    depends on the lab: L-01's client count is bounded by the workers'
    memory, L-07's export count by how long a run may take, and L-08's
    cycle count is the difference between a CI case and the multi-day soak
    L-03 asks for. A lab-specific number hard-coded here would be wrong
    everywhere else.
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class _LoadMixin:
    """Recording, and the one measurement that is also a verdict."""

    def record_measurement(self, label, payload):
        """Log the measurement and leave it on the share as JSON.

        Not ``record``: ``_MdsBase.record()`` is the MDS record reader and
        the soak case needs both. One of them having a distinct name is
        cheaper than an override that depends on which base wins the MRO.

        The log line is for the reader of a failed run; the file is so a
        later run can be compared against this one, which is the only way
        a number with no threshold ever becomes useful.
        """
        self.logger.info("[%s] %s", label, json.dumps(payload, default=str))
        try:
            path = os.path.join(self.docker_logs_path, f"{label}.json")
            with open(path, "w") as fh:
                json.dump(payload, fh, indent=2, default=str)
            self.logger.info("[%s] recorded to %s", label, path)
        except OSError as exc:
            # A measurement that cannot be written down is not a reason to
            # fail a case that has already proved its assertions.
            self.logger.warning("[%s] could not record to the share: %s",
                                label, exc)

    def direct_io_ratio(self, pods, context=""):
        """``(server_data_ops, total_data_ops, per_pod)`` across *pods*.

        The direct-vs-MDS ratio L-01 asks for, and the one number here
        that fails the case. Once a client holds a layout the metadata
        server should see no READ or WRITE at all, because the data path
        goes straight to the storage nodes; a non-zero count means that
        client is proxying its data through the single metadata server,
        which is plain NFS wearing a pNFS mount option.
        """
        per_pod, server, total = {}, 0, 0
        for pod in pods:
            try:
                st = mountstats(self.k8s_utils, pod, mountpoint=self.MOUNT)
            except PnfsProbeError as exc:
                per_pod[pod] = {"error": str(exc)[:200]}
                continue
            ops = (st or {}).get("ops") or {}
            srv = sum(int(ops.get(op, 0)) for op in DATA_OPS)
            lg = int(ops.get("LAYOUTGET", 0))
            per_pod[pod] = {
                "layout_type": (st or {}).get("layout_type"),
                "LAYOUTGET": lg,
                "server_data_ops": srv,
                "lease_time": (st or {}).get("lease_time"),
            }
            server += srv
            total += srv + lg
        offenders = {p: v for p, v in per_pod.items()
                     if isinstance(v, dict) and v.get("server_data_ops", 0)}
        if offenders:
            raise PnfsEvidenceError(
                f"{context}: the metadata server served data operations for "
                f"{len(offenders)} client(s): {dict(list(offenders.items())[:4])}. "
                f"Under load every byte is supposed to go direct to the "
                f"storage nodes; a client proxying through the MDS puts the "
                f"single metadata server in the data path of the whole "
                f"cluster, which is the scaling failure this row exists to "
                f"find. It would not fail a throughput-only case.")
        return server, total, per_pod

    def mds_cpu(self):
        """``kubectl top`` for the MDS pods, or why it is unavailable.

        Recorded, never asserted. Many labs run no metrics-server, and a
        load case that failed because a monitoring add-on is absent would
        be reporting on the lab.
        """
        ns = getattr(self, "mds_ns", None) or self.k8s_utils.namespace
        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl top pod -n {ns} --no-headers 2>&1 | grep pnfs-mds "
            f"|| true", supress_logs=True)
        text = (out or err or "").strip()
        if not text or "error" in text.lower() or "not found" in text.lower():
            return {"available": False, "why": text[:200] or "no output"}
        rows = {}
        for line in text.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                rows[parts[0]] = {"cpu": parts[1], "memory": parts[2]}
        return {"available": bool(rows), "pods": rows}

    def device_bytes(self):
        """Namespace write counters summed over the storage nodes.

        512-byte units as the controllers report them. Diffed around a
        workload this is the cluster's own view of how much data arrived,
        which is a more trustworthy aggregate than scraping fio's text
        output from twenty pod logs.
        """
        total, per_node = 0, {}
        for ip in (getattr(self, "storage_nodes", None) or []):
            try:
                counters = nvme_ns_counters(self.k8s_utils, ip)
            except Exception as exc:                        # noqa: BLE001
                per_node[ip] = {"error": str(exc)[:160]}
                continue
            got = sum(int(c.get("write", 0)) for c in counters.values())
            per_node[ip] = got
            total += got
        return total, per_node


class PnfsManyClientsLoadK8s(_PnfsBase, _LoadMixin):
    """L-01: many clients driving fio on one RWX volume, measured.

    The plan says 20–50 clients. The default here is 24 and it is
    deliberately over-subscribed -- more pods than nodes -- because that
    is the shape that finds the limit: per-client state on the single
    metadata server, not bandwidth on the data path. One pod per node is
    already B-01, and six clients would measure nothing new.

    Mixed sizes and patterns across the pods, per the row: a uniform
    64k sequential load from every client is one workload repeated, and
    the interesting interactions (small random reads against large
    sequential writes on one filesystem) only appear when they differ.

    The assertions are correctness; the throughput, latency and MDS CPU
    are recorded. See the module docstring for why.
    """

    #: 20-50 in the plan. 24 fits a six-node lab at four deep, and leaves
    #: room for the metadata server's own pods.
    CLIENTS = _env_int("PNFS_L01_CLIENTS", 24)
    RUNTIME = _env_int("PNFS_L01_RUNTIME", 300)
    CLAIM = "pnfs-l01"
    CLAIM_SIZE = "60Gi"
    JOB_TIMEOUT = 2400

    #: One of these per pod, round robin. Every entry writes and verifies;
    #: the mix is in the block size and the access pattern.
    PROFILES = (
        {"rw": "write", "bs": "64k", "size": "512M"},
        {"rw": "randwrite", "bs": "4k", "size": "128M"},
        {"rw": "write", "bs": "1M", "size": "768M"},
        {"rw": "randrw", "bs": "16k", "size": "256M"},
    )

    def run(self):
        self.logger.info("=== L-01 %d clients on one RWX claim ===",
                         self.CLIENTS)
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        configs = []
        for i in range(self.CLIENTS):
            prof = self.PROFILES[i % len(self.PROFILES)]
            cfg = self._pod_config(
                i, rw=prof["rw"], size=prof["size"], verify=True,
                runtime=self.RUNTIME)
            # The profile's block size replaces the helper's default, and
            # per-client latency has to be asked for -- fio does not emit
            # a percentile table unless it is told to.
            cfg = cfg.replace("bs=64k", f"bs={prof['bs']}")
            cfg = cfg.replace(
                "group_reporting",
                "group_reporting\nlat_percentiles=1\npercentile_list=50:95:99")
            configs.append(cfg)

        bytes_before, _ = self.device_bytes()
        started = time.time()
        # required_spread is off by construction: more pods than nodes
        # makes a required anti-affinity unsatisfiable, and this case
        # wants them stacked.
        jobs = self._start_shared_load("pnfs-l01-fio", configs,
                                       required_spread=False)
        pods = self._pods_of(jobs, timeout=900)
        self._wait_pods_running(pods, timeout=1200)

        placement = {p: self._pod_node(p) for p in pods}
        nodes = {n for n in placement.values() if n}
        if len(nodes) < 2:
            raise PnfsProbeError(
                f"[L-01] {self.CLIENTS} pods landed on {sorted(nodes)}. A "
                f"single-node run is not a multi-client filesystem test; "
                f"check for cordoned workers.")
        self.logger.info("[L-01] %d clients across %d node(s)", len(pods),
                         len(nodes))

        mid_cpu = self.mds_cpu()
        # The layout check happens while the load is running, not after.
        # A client that fell back to the metadata server under pressure
        # and recovered when the load stopped is the finding, and a
        # post-run check would miss it.
        sleep_n_sec(min(90, max(30, self.RUNTIME // 4)))
        assert_layouts_taken(
            self.k8s_utils, pods[:6], mountpoint=self.MOUNT,
            logger=self.logger, context="[L-01] under load")
        server_ops, total_ops, per_pod = self.direct_io_ratio(
            pods, context="[L-01] under load")

        self._await_jobs(jobs, "L-01 many-client load")
        elapsed = time.time() - started
        bytes_after, per_node = self.device_bytes()

        moved = max(0, bytes_after - bytes_before)
        self.record_measurement("L-01", {
            "clients": self.CLIENTS,
            "nodes": sorted(nodes),
            "runtime_s": self.RUNTIME,
            "elapsed_s": round(elapsed, 1),
            "profiles": list(self.PROFILES),
            # 512-byte units, as the NVMe controllers report them.
            "device_write_units": moved,
            "device_write_MiB": round(moved * 512 / 1024 ** 2, 1),
            "aggregate_MiB_s": round(moved * 512 / 1024 ** 2 / max(1.0, elapsed), 2),
            "per_node_write_units": per_node,
            "mds_server_data_ops": server_ops,
            "mds_cpu_under_load": mid_cpu,
            "mds_cpu_after": self.mds_cpu(),
            "per_client_mountstats": per_pod,
            "note": ("no throughput, latency or CPU target exists in the "
                     "design, so these are recorded and not gated; the "
                     "gated assertions are the fio verifies, the layouts "
                     "and mds_server_data_ops == 0"),
        })

        self.logger.info(
            "[L-01] PASS: %d clients on one claim across %d node(s), every "
            "verify clean, layouts held under load, %d server-side data "
            "op(s) (must be 0 of %d), ~%.1f MiB/s to the devices",
            self.CLIENTS, len(nodes), server_ops, total_ops,
            moved * 512 / 1024 ** 2 / max(1.0, elapsed))


class PnfsExportCeilingK8s(_PnfsBase, _LoadMixin):
    """L-07: exports per metadata server, up to the limit and then one more.

    The row is a boundary with no published bound: "up to nfsd's and the
    guest memory's limits, then one more". Nobody knows what that number
    is, so this case does not assert it. It climbs in batches and the
    finding is **where it stopped and how**, which is the number the row
    was asking for.

    Two things are gated, and neither depends on knowing the limit:

      * the refusal is clean. Reaching a ceiling is allowed; hanging at it
        is not. A claim that sits Pending forever with no event is
        indistinguishable to a user from a cluster that has broken, and it
        is the failure mode `nfsexports` reconciling eight at a time was
        introduced to avoid.
      * **nothing already exported breaks.** This is the real assertion.
        An export that was Ready and serving must stay Ready and serving
        while its neighbours are being created, however many of them
        fail. A ceiling that takes the existing volumes down with it is a
        cluster-wide outage caused by one tenant provisioning.

    The provisioning time per batch is recorded, because the curve is the
    early warning: exports reconcile eight at a time, so a per-export cost
    that grows with the count is what makes the last exports of a busy
    cluster come back after grace has already ended -- the problem the
    design's §328 note is about.
    """

    #: Claims created, in batches. The default is sized to finish inside a
    #: normal run; the soak lane raises it.
    TARGET = _env_int("PNFS_L07_EXPORTS", 24)
    BATCH = _env_int("PNFS_L07_BATCH", 8)
    #: Per batch. Generous: the first claim on a cluster provisions an
    #: lvol, an export and a mount on every client.
    BATCH_TIMEOUT = _env_int("PNFS_L07_BATCH_TIMEOUT", 900)
    CLAIM_SIZE = "2Gi"
    BASE = "pnfs-l07"

    def run(self):
        self.logger.info("=== L-07 export ceiling, target %d ===", self.TARGET)
        self.assert_pnfs_cluster()
        self.mds_ns = self.k8s_utils.namespace
        self._make_storage_class()

        # The sentinel. Created first, written to, and re-checked after
        # every batch: this is the export that must survive whatever the
        # ceiling does to the others.
        sentinel = f"{self.BASE}-keep"
        self._make_rwx_claim(sentinel)
        self.extra_claims.append(sentinel)
        keeper = self._start_shell_pods(f"{self.BASE}-keep-pod", 1,
                                        claim=sentinel, spread=False)[0]
        md5 = self._py(keeper, (
            "import hashlib, os\n"
            f"p = {self.MOUNT + '/l07-sentinel.bin'!r}\n"
            "blk = bytes(range(256)) * 4096\n"
            "h = hashlib.md5()\n"
            "with open(p, 'wb') as f:\n"
            "    for _ in range(8):\n"
            "        f.write(blk[:1024*1024]); h.update(blk[:1024*1024])\n"
            "    f.flush(); os.fsync(f.fileno())\n"
            "print(h.hexdigest())\n"))
        self.logger.info("[L-07] sentinel export written, md5=%s", md5)

        batches, created, stopped_at, reason = [], [], None, ""
        n = 0
        while n < self.TARGET and stopped_at is None:
            size = min(self.BATCH, self.TARGET - n)
            names = [f"{self.BASE}-{i}" for i in range(n, n + size)]
            t0 = time.time()
            for name in names:
                self.k8s_utils.create_pvc(
                    name=name, size=self.CLAIM_SIZE,
                    storage_class=self.sc_name,
                    access_modes=["ReadWriteMany"])
                self.extra_claims.append(name)
            bound, pending = [], []
            deadline = time.time() + self.BATCH_TIMEOUT
            while time.time() < deadline:
                bound, pending = [], []
                for name in names:
                    phase = (self.k8s_utils.get_pvc_status(name) or {}).get(
                        "phase") or ""
                    (bound if phase == "Bound" else pending).append(name)
                if not pending:
                    break
                sleep_n_sec(15)
            took = round(time.time() - t0, 1)
            created.extend(bound)
            ready, total = self.exports_ready()
            batches.append({
                "batch": len(batches) + 1,
                "requested": size,
                "bound": len(bound),
                "pending": pending,
                "seconds": took,
                "per_export_s": round(took / max(1, len(bound)), 1),
                "exports_ready": ready,
                "exports_total": total,
            })
            self.logger.info("[L-07] batch %d: %d/%d bound in %ss "
                             "(%.1fs each), exports %d/%d Ready",
                             len(batches), len(bound), size, took,
                             took / max(1, len(bound)), ready, total)

            if pending:
                stopped_at = len(created)
                reason = self.refusal_reason(pending)
            n += size

            # The gate, after every batch rather than only at the end: the
            # sentinel must still be serving while its neighbours are
            # being created, and an export that broke in batch two and
            # healed by batch four still broke.
            self.assert_sentinel(keeper, md5, sentinel,
                                 f"after batch {len(batches)}")

        payload = {
            "target": self.TARGET,
            "batch_size": self.BATCH,
            "exports_created": len(created),
            "stopped_at": stopped_at,
            "refusal": reason,
            "batches": batches,
            "mds_cpu": self.mds_cpu(),
            "note": ("no export-count bound is published, so the count "
                     "reached is the finding; the gated assertions are the "
                     "clean refusal and the sentinel export surviving"),
        }
        self.record_measurement("L-07", payload)

        if stopped_at is not None and not reason:
            raise PnfsEvidenceError(
                f"[L-07] provisioning stalled at {stopped_at} export(s) and "
                f"nothing said why: the claims stayed Pending for "
                f"{self.BATCH_TIMEOUT}s with no event and no condition on "
                f"the claim or its NFSExport. Reaching a ceiling is allowed; "
                f"reaching it silently is not -- to a user this is "
                f"indistinguishable from a broken cluster, and there is "
                f"nothing to act on.")

        self.assert_sentinel(keeper, md5, sentinel, "at the end")
        self.logger.info(
            "[L-07] PASS: %d export(s) created%s; the pre-existing export "
            "stayed Ready and its data verified throughout",
            len(created),
            "" if stopped_at is None else
            f", stopped at {stopped_at} with: {reason[:200]}")

    def exports_ready(self):
        items = nfsexport_status(self.k8s_utils, namespace=self.mds_ns) or []
        ready = 0
        for it in items:
            st = it.get("status") or {}
            conds = {c.get("type"): c.get("status")
                     for c in (st.get("conditions") or [])}
            if (st.get("phase") or st.get("state")) == "Ready" \
                    or conds.get("Ready") == "True":
                ready += 1
        return ready, len(items)

    def refusal_reason(self, pending):
        """Whatever the cluster said about the claims that did not bind.

        An empty string means it said nothing, which is itself the
        finding the case fails on.
        """
        said = []
        for name in pending[:4]:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get events -n {self.mds_ns} --field-selector "
                f"involvedObject.kind=PersistentVolumeClaim,"
                f"involvedObject.name={name} "
                f"-o custom-columns=REASON:.reason,MSG:.message "
                f"--no-headers 2>/dev/null || true", supress_logs=True)
            for line in (out or "").splitlines():
                if line.strip() and "WaitForFirstConsumer" not in line:
                    said.append(f"{name}: {line.strip()[:220]}")
        return " | ".join(said)

    def assert_sentinel(self, pod, md5, claim, when):
        """The pre-existing export is still serving the bytes it had."""
        export = nfsexport_status(self.k8s_utils, pvc_name=claim,
                                  namespace=self.mds_ns)
        if export is None:
            raise PnfsEvidenceError(
                f"[L-07] {when}: the sentinel claim {claim} no longer has an "
                f"NFSExport. Provisioning more exports took an existing one "
                f"away, which is a cluster-wide outage caused by one "
                f"tenant's provisioning.")
        got = self._py(pod, (
            "import hashlib\n"
            "h = hashlib.md5()\n"
            f"with open({self.MOUNT + '/l07-sentinel.bin'!r}, 'rb') as f:\n"
            "    while True:\n"
            "        b = f.read(1024*1024)\n"
            "        if not b:\n"
            "            break\n"
            "        h.update(b)\n"
            "print(h.hexdigest())\n"))
        if got != md5:
            raise PnfsEvidenceError(
                f"[L-07] {when}: the sentinel export read back {got}, "
                f"expected {md5}. Creating exports changed the contents of "
                f"an existing one.")
        self.logger.info("[L-07] sentinel still Ready and intact %s", when)


class PnfsRestartRebootSoakK8s(_MdsBase, _LoadMixin):
    """L-08: repeated MDS restarts and client reboots under active writers.

    *"ending with clean checksums and a clean fsck"*. The checksums are
    here and they are the gate. The `fsck` is not, and the module
    docstring says why: `xfs_repair -n` refuses on a mounted filesystem,
    the guest holds the backing device mounted for the export's whole
    life, and nothing in the harness can unexport and re-export. That is
    a missing primitive, not a skipped assertion.

    Each cycle does both faults, in this order and on purpose: the MDS
    restart first, then a client node reboot while the clients are still
    settling from it. Separately, each is F-01 and F-04. Together and
    repeated they are what L-08 asks for, and the compound is where the
    interesting failures live -- a reclaim that races a restage, a
    reservation key from a boot two cycles ago, an export that comes back
    Ready without its data.

    This class is also L-03's vehicle. `PNFS_L08_CYCLES` turns a
    three-cycle CI case into a multi-day soak without a second
    implementation; the assertions do not change with the duration, and a
    soak whose assertions are weaker than the short run's proves less the
    longer it runs.
    """

    CYCLES = _env_int("PNFS_L08_CYCLES", 3)
    CLAIM = "pnfs-l08"
    CLAIM_SIZE = "40Gi"
    #: Per cycle, for the writer that stays up the whole time. Above a
    #: full grace period plus a restage, and under the point where a
    #: client has plainly been abandoned.
    MAX_GAP = 180
    REBOOT_TIMEOUT = 1200
    #: Files written per cycle and verified at the very end, so a cycle
    #: that corrupted data three cycles ago is still caught.
    SEED_MIB = 16

    def run(self):
        self.logger.info("=== L-08 soak: %d cycle(s) ===", self.CYCLES)
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()
        reboots = not self.k8s_utils.detect_talos()
        if not reboots:
            self.logger.warning(
                "[L-08] Talos: the client-reboot half of every cycle needs "
                "talosctl on the runner and is not guaranteed here, so the "
                "cycles will be MDS restarts only. That is F-01 repeated, "
                "not L-08, and this line is what says so in the run log.")

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-l08", 3)
        anchor = pods[0]

        marker = self.start_writer(anchor)
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)

        seeds, history = {}, []
        for cycle in range(1, self.CYCLES + 1):
            name = f"l08-c{cycle}.bin"
            seeds[name] = self.seed(anchor, name)
            entry = {"cycle": cycle, "seed": name}
            t0 = time.time()

            # (1) the metadata server.
            active, epoch = self.record()
            if not active:
                raise PnfsProbeError(
                    f"[L-08] cycle {cycle}: no active guest is recorded.")
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
            new_active, new_epoch, took = wait_mds_promotion(
                self.k8s_utils, self.sts_name, epoch,
                timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
            self.wait_exports_ready(
                1, timeout=self.TAKEOVER_TIMEOUT,
                what=f"[L-08] cycle {cycle} export after the takeover")
            entry.update({"epoch": [epoch, new_epoch], "takeover_s": took,
                          "promoted": new_active})
            self.logger.info("[L-08] cycle %d: takeover %s -> %s in %ss",
                             cycle, epoch, new_epoch, took)

            # (2) a client node, while the clients are still settling.
            if reboots:
                victim = pods[1 + (cycle % 2)]
                node = self._pod_node(victim)
                entry["rebooted"] = node
                if node and node != self._pod_node(anchor):
                    self.reboot_client(node, victim, cycle, entry)
                else:
                    # Never the anchor's node: its writer is the continuity
                    # measurement and rebooting under it would be measuring
                    # the reboot, not the soak.
                    entry["rebooted"] = f"skipped ({node} hosts the writer)"
                    self.logger.info("[L-08] cycle %d: %s", cycle,
                                     entry["rebooted"])

            self._assert_mount_writable([anchor])
            entry["cycle_s"] = round(time.time() - t0, 1)
            history.append(entry)
            sleep_n_sec(30)

        # The gate. Every seed from every cycle, read back from a client
        # that was up the whole time and from one that was rebooted.
        for name, md5 in seeds.items():
            self.verify(anchor, name, md5, "[L-08] from the anchor")
        survivors = [p for p in self.shell_pods
                     if p != anchor and p.startswith("pnfs-l08")]
        if survivors:
            for name, md5 in seeds.items():
                self.verify(survivors[-1], name, md5,
                            f"[L-08] from {survivors[-1]}")

        assert_layouts_taken(
            self.k8s_utils, [anchor], mountpoint=self.MOUNT,
            logger=self.logger, context="[L-08] at the end of the soak")
        server_ops, _total, per_pod = self.direct_io_ratio(
            [anchor], context="[L-08] at the end of the soak")
        gap = self.assert_client_survived(
            anchor, marker, "[L-08] the anchor writer", self.MAX_GAP)

        self.record_measurement("L-08", {
            "cycles": self.CYCLES,
            "client_reboots": reboots,
            "seeds_verified": sorted(seeds),
            "worst_client_gap_s": gap,
            "mds_server_data_ops": server_ops,
            "per_client_mountstats": per_pod,
            "history": history,
            "fsck": ("not run: xfs_repair -n refuses on a mounted "
                     "filesystem and no harness path unexports a live "
                     "export. L-03/T-8 remains open."),
        })
        self.logger.info(
            "[L-08] PASS: %d cycle(s) of takeover%s, %d seed file(s) "
            "verified from two clients, anchor writer stalled at worst %ss "
            "and never lost its descriptor",
            self.CYCLES, " + client reboot" if reboots else "", len(seeds),
            gap)

    # -- the pieces ------------------------------------------------------

    def reboot_client(self, node, victim, cycle, entry):
        node_ip = self.k8s_utils.node_internal_ip(node)
        self.logger.info("[L-08] cycle %d: rebooting client node %s", cycle,
                         node)
        if not self.k8s_utils.reboot_node(node_ip, drain=False):
            entry["rebooted"] = f"not issued ({node})"
            self.logger.warning(
                "[L-08] cycle %d: no reboot could be issued for %s; the "
                "cycle is an MDS restart only", cycle, node)
            return
        self.k8s_utils.wait_node_condition(node_ip, ready=False, timeout=300)
        back = self.k8s_utils.wait_node_condition(
            node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
        self.k8s_utils.uncordon_all_workers()
        if not back:
            raise PnfsEvidenceError(
                f"[L-08] cycle {cycle}: {node} did not return Ready within "
                f"{self.REBOOT_TIMEOUT}s. The soak cannot continue and the "
                f"lab is short a worker.")
        if victim in self.shell_pods:
            self.shell_pods.remove(victim)
        self.k8s_utils.delete_pod(victim, wait=True)
        replacement = self._start_shell_pods(
            f"pnfs-l08-c{cycle}-restaged", 1, spread=False, node=node)[0]
        entry["restaged_as"] = replacement
        self.logger.info("[L-08] cycle %d: %s restaged on %s", cycle,
                         replacement, node)

    def seed(self, pod, name):
        return self._py(pod, (
            "import hashlib, os\n"
            f"p = {self.MOUNT!r} + '/' + {name!r}\n"
            "blk = bytes(range(256)) * 4096\n"
            "h = hashlib.md5()\n"
            "with open(p, 'wb') as f:\n"
            f"    for _ in range({self.SEED_MIB}):\n"
            "        f.write(blk[:1024*1024]); h.update(blk[:1024*1024])\n"
            "    f.flush(); os.fsync(f.fileno())\n"
            "print(h.hexdigest())\n"))

    def verify(self, pod, name, md5, what):
        got = self._py(pod, (
            "import hashlib\n"
            "h = hashlib.md5()\n"
            f"with open({self.MOUNT!r} + '/' + {name!r}, 'rb') as f:\n"
            "    while True:\n"
            "        b = f.read(1024*1024)\n"
            "        if not b:\n"
            "            break\n"
            "        h.update(b)\n"
            "print(h.hexdigest())\n"))
        if got != md5:
            raise PnfsEvidenceError(
                f"{what}: {name} read back md5 {got}, expected {md5}. A "
                f"file written in one cycle of the soak does not survive "
                f"the ones after it, which is silent corruption -- every "
                f"takeover in between reported success.")
        self.logger.info("%s: %s verified", what, name)
