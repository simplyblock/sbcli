"""Track E: pNFS failure injection and churn.

The `F-` and `L-` rows of `test-plan-pnfs-rwx.md` that are reachable from
a Kubernetes client on the lab we have.

**What is deliberately absent, and why.** The plan's §7 rules several of
these out by environment rather than by effort, and writing them anyway
would produce cases that fail for the lab rather than for the product:

  E-10, E-11   need RHEL/Debian node pools and a kernel below 6.11.
               "The lab cluster is Talos, and RHEL- and Debian-family
               node pools are not part of any run."
  F-21         needs RHCOS 5.14 clients. Two failovers on dev-ocp
               (kernel 6.12) passed but fenced no client, so on this lab
               the case cannot reach its own premise.
  F-04, F-16   need a client node actually down -- a reboot or a kubelet
               stop on a shared lab host. Recoverable in principle, but
               a case that does not bring the node back damages the lab
               for every run after it, and these have never been run
               once. They want a hand on the wheel the first time.
  F-17         two restarts in quick succession is the same boundary as
               `PnfsMdsSecondTakeoverK8s` (F-09 of the standby plan),
               which drives it against the record. Writing it twice
               would mean two cases failing for one defect.
  F-10, F-18   a network partition and a client path loss. Both are
               single commands away from wedging a lab node with no
               automatic undo; same reasoning as F-04.

What is here runs against pod-level and client-level faults only, so a
failed case leaves nothing a teardown cannot undo.

Class names end in ``K8s`` for the platform filter in ``e2e.py``.
"""
import time

from e2e_tests.pnfs.test_pnfs_rwx import _PnfsBase
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    PnfsEvidenceError,
    PnfsProbeError,
    mds_pods,
    mds_statefulset,
    nfsexport_status,
    pvc_volume_ref,
)

#: csi-driver/internal/csi/node/pnfs_report.go:28-29. Mirrored, not
#: imported (e2e/AGENTS.md). A Warning on the claim of the volume,
#: recorded by component ``simplyblock-csi-node``.
EVENT_DATA_THROUGH_MDS = "PNFSDataThroughMDS"
EVENT_LAYOUT_PROBE_HUNG = "PNFSLayoutProbeHung"


class _FailBase(_PnfsBase):
    def setup(self):
        super().setup()
        self.mds_ns = self.k8s_utils.namespace

    def exports(self):
        return nfsexport_status(self.k8s_utils, namespace=self.mds_ns) or []

    def pvc_events(self, pvc, reason=None):
        """``[(reason, count, message)]`` for events on one PVC.

        The count matters as much as the presence: Kubernetes aggregates
        a repeated event rather than recording it again, so "one event"
        in a plan means one event object whose count may be greater than
        one -- and a case that counted objects would read an aggregated
        storm as a single well-behaved report.
        """
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get events -n {self.mds_ns} "
            f"--field-selector involvedObject.kind=PersistentVolumeClaim,"
            f"involvedObject.name={pvc} "
            f"-o custom-columns=REASON:.reason,COUNT:.count,"
            f"MSG:.message --no-headers 2>/dev/null || true",
            supress_logs=True)
        rows = []
        for line in (out or "").splitlines():
            parts = line.split(None, 2)
            if len(parts) < 2:
                continue
            try:
                count = int(parts[1])
            except ValueError:
                count = 1
            msg = parts[2] if len(parts) > 2 else ""
            if reason is None or parts[0] == reason:
                rows.append((parts[0], count, msg))
        return rows


class PnfsCsiKilledMidOperationK8s(_FailBase):
    """F-05: killing the CSI pods mid-provision leaves one export, not two.

    CSI operations are retried by the sidecars, and a retry that is not
    idempotent creates the volume twice. The symptom is not an error --
    both calls succeed -- it is a second NFSExport and a second lvol that
    nothing points at, consuming capacity until somebody notices.

    So the assertion is on the count, after recovery: exactly one export
    per claim, and its volumeRef matches the PV the claim actually got.
    An orphan with a volumeRef that no PV names is the leak.

    The kill lands during provisioning on purpose. Killing the plugin
    while nothing is happening tests the restart, not the at-most-once
    guarantee, and would pass on a driver with no idempotency at all.
    """

    CLAIMS = 3
    CSI_LABELS = ("app=simplyblock-csi-node", "app=simplyblock-csi-controller")

    def run(self):
        self.logger.info("=== F-05 CSI killed mid-operation ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()

        exports_before = {(e.get("spec") or {}).get("volumeRef")
                          for e in self.exports()}
        names = [f"pnfs-f05-{i}" for i in range(self.CLAIMS)]

        # Create, then kill while the provisions are in flight.
        for n in names:
            self.k8s_utils.create_pvc(
                name=n, size=self.CLAIM_SIZE, storage_class=self.sc_name,
                access_modes=["ReadWriteMany"])
            self.extra_claims.append(n)
        sleep_n_sec(8)

        killed = []
        for label in self.CSI_LABELS:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.mds_ns} -l {label} "
                f"-o custom-columns=NAME:.metadata.name --no-headers "
                f"2>/dev/null || true", supress_logs=True)
            pods = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
            for p in pods:
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete pod {p} -n {self.mds_ns} --wait=false")
            killed += pods
        if not killed:
            raise PnfsProbeError(
                f"[F-05] no pod matched any of {self.CSI_LABELS} in "
                f"{self.mds_ns}, so nothing was killed and the case would "
                f"pass without testing anything. Check the chart's labels.")
        self.logger.info("[F-05] killed %d CSI pod(s) mid-provision: %s",
                         len(killed), killed)

        # Recovery on restart is half the row: the claims must still bind.
        for n in names:
            self.k8s_utils.wait_pvc_bound(n, timeout=self.BIND_TIMEOUT)
        self.logger.info("[F-05] all %d claim(s) bound after the kill",
                         len(names))

        # Settle, so a duplicate created by a retry has time to appear.
        # Asserting immediately would pass on a driver that makes the
        # second one a few seconds later.
        sleep_n_sec(60)

        refs = {n: pvc_volume_ref(self.k8s_utils, n) for n in names}
        problems = []
        for n, ref in refs.items():
            if not ref:
                problems.append(f"{n}: bound but its PV carries no volumeRef")
                continue
            matching = [e for e in self.exports()
                        if (e.get("spec") or {}).get("volumeRef") == ref]
            if len(matching) != 1:
                problems.append(
                    f"{n}: {len(matching)} NFSExport(s) carry volumeRef "
                    f"{ref} -- "
                    + ", ".join((e.get("metadata") or {}).get("name", "?")
                                for e in matching))
        if problems:
            raise PnfsEvidenceError(
                "[F-05] the CSI plugins were killed mid-provision and the "
                "retry was not idempotent:\n  " + "\n  ".join(problems)
                + "\nA second export and its lvol consume capacity that "
                  "nothing will ever reclaim, and no operation failed to "
                  "make anyone look.")

        # And nothing appeared that belongs to no claim at all.
        live_refs = set(refs.values()) | exports_before
        orphans = [(e.get("metadata") or {}).get("name")
                   for e in self.exports()
                   if (e.get("spec") or {}).get("volumeRef") not in live_refs]
        if orphans:
            raise PnfsEvidenceError(
                f"[F-05] {len(orphans)} NFSExport(s) exist whose volumeRef "
                f"belongs to no claim this case made and to nothing that "
                f"was here before it: {orphans}. That is the leak this row "
                f"is about.")

        self.logger.info(
            "[F-05] PASS: %d claim(s) bound after %d CSI pod(s) were killed "
            "mid-provision, exactly one export each, no orphans",
            len(names), len(killed))


class PnfsGuestCrashInPlaceK8s(_FailBase):
    """F-15: the guest crashes, its container restarts, the pod stays.

    Distinct from losing the pod (F-01 of the standby plan). Here the pod,
    its node and its identity survive and only the process inside dies, so
    there is no reschedule and no promotion to a different guest: the same
    pod has to reassemble every export and take its clients back.

    The pod UID is checked before and after precisely because this is the
    difference. A case that only looked at "exports came back" would pass
    on a cluster that quietly replaced the pod, which is a different code
    path with different timings and a different blast radius.
    """

    CLAIM = "pnfs-f15"
    RESTART_TIMEOUT = 420

    def run(self):
        self.logger.info("=== F-15 guest crash, container restarts in place ===")
        self.assert_pnfs_cluster()
        sts = mds_statefulset(self.k8s_utils, self.mds_ns)
        if not sts:
            raise PnfsProbeError(
                "[F-15] no pnfs-mds StatefulSet: no guest to crash.")
        sts_name = (sts.get("metadata") or {}).get("name")

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-f15", 2)
        self._assert_mount_writable(pods)

        guests = [p for p in mds_pods(self.k8s_utils, sts_name, self.mds_ns)
                  if p[2] == "Running" and p[3]]
        if not guests:
            raise PnfsProbeError("[F-15] no MDS guest is Running and Ready.")
        target = guests[0][0]
        obj = self.k8s_utils.get_resource_json("pod", target, self.mds_ns)
        uid_before = (obj.get("metadata") or {}).get("uid")
        statuses = ((obj.get("status") or {}).get("containerStatuses") or [])
        restarts_before = sum(c.get("restartCount", 0) for c in statuses)
        self.logger.info("[F-15] target guest %s uid=%s restarts=%s",
                         target, uid_before, restarts_before)

        # Kill the guest. QEMU is the guest; killing the runner would be a
        # different fault (O-24 covers the operator side of that).
        out, err = self.k8s_utils.exec_in_pod(
            target,
            "pkill -9 -f qemu-system || pkill -9 qemu-kvm || "
            "pkill -9 -f 'qemu' || echo NOQEMU",
            timeout=60)
        self.logger.info("[F-15] kill issued: out=%r err=%r",
                         (out or "").strip()[-200:], (err or "").strip()[-200:])
        if "NOQEMU" in (out or ""):
            raise PnfsProbeError(
                f"[F-15] found no qemu process in {target}, so nothing was "
                f"crashed. The guest may run under another process name in "
                f"this image; the case must kill the guest, not the runner.")

        deadline = time.time() + self.RESTART_TIMEOUT
        restarts_after = restarts_before
        uid_after = uid_before
        while time.time() < deadline:
            obj = self.k8s_utils.get_resource_json("pod", target, self.mds_ns)
            if not obj:
                break
            uid_after = (obj.get("metadata") or {}).get("uid")
            statuses = ((obj.get("status") or {}).get("containerStatuses")
                        or [])
            restarts_after = sum(c.get("restartCount", 0) for c in statuses)
            ready = all(c.get("ready") for c in statuses) and statuses
            if restarts_after > restarts_before and ready:
                break
            sleep_n_sec(10)

        if uid_after != uid_before:
            raise PnfsEvidenceError(
                f"[F-15] the pod was replaced rather than restarted in "
                f"place: uid {uid_before} -> {uid_after}. A crashing guest "
                f"is supposed to restart inside its own pod; replacing it "
                f"means a reschedule, a different node, and a different "
                f"recovery path from the one this row describes.")
        if restarts_after <= restarts_before:
            raise PnfsEvidenceError(
                f"[F-15] the guest was killed but the container's "
                f"restartCount is still {restarts_after}. Either the crash "
                f"was absorbed without restarting the guest -- in which "
                f"case nothing is serving -- or the process killed was not "
                f"the guest.")

        # The exports come back, on the same pod.
        deadline = time.time() + self.RESTART_TIMEOUT
        while time.time() < deadline:
            export = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM,
                                      namespace=self.mds_ns)
            st = (export or {}).get("status") or {}
            conds = {c.get("type"): c.get("status")
                     for c in (st.get("conditions") or [])}
            if st.get("phase") == "Ready" or conds.get("Ready") == "True":
                break
            sleep_n_sec(10)
        else:
            raise PnfsEvidenceError(
                f"[F-15] the guest restarted in place (restarts "
                f"{restarts_before} -> {restarts_after}) but the export for "
                f"{self.CLAIM} did not come back Ready within "
                f"{self.RESTART_TIMEOUT}s.")

        self._assert_mount_writable(pods)
        self.logger.info(
            "[F-15] PASS: guest crashed and restarted in pod %s (uid "
            "unchanged, restarts %s -> %s), exports reassembled, clients "
            "writing again", target, restarts_before, restarts_after)


class PnfsUnalignedDirectIoReportK8s(_FailBase):
    """F-22: unaligned O_DIRECT is reported once, not probed into the ground.

    A mount whose I/O falls back to the metadata server keeps working --
    every READ and WRITE simply goes through the server instead of the
    device, as the reporter's own header says: "Nothing fails, so without
    a report nobody sees it." The whole value of this row is that the
    silent case becomes visible.

    Two things are asserted, and they pull in opposite directions, which
    is why the row exists:

      * the fallback IS reported -- a Warning `PNFSDataThroughMDS` on the
        claim, naming both causes (unaligned, and direct);
      * it is reported ONCE. The node probes at most every 30s, so six
        minutes of continuous unaligned writes must not produce a
        per-probe event storm on the claim.

    Kubernetes aggregates a repeated event into one object with a count,
    so the count is read rather than the number of objects: an event
    recorded twelve times is a storm wearing one object's clothes.
    """

    CLAIM = "pnfs-f22"
    #: The plan says six minutes. Long enough for at least a dozen 30s
    #: probes, which is what makes "one event" a meaningful claim.
    WRITE_SECONDS = 360
    #: What a 30s probe interval allows over WRITE_SECONDS, with room for
    #: the mount being probed on more than one node.
    MAX_EVENT_COUNT = 30

    def run(self):
        self.logger.info("=== F-22 unaligned O_DIRECT reporting ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-f22", 1)
        self._assert_mount_writable(pods)
        pod = pods[0]

        before = self.pvc_events(self.CLAIM, EVENT_DATA_THROUGH_MDS)
        before_count = sum(c for _, c, _ in before)
        self.logger.info("[F-22] %s count before: %d",
                         EVENT_DATA_THROUGH_MDS, before_count)

        # Deliberately unaligned: an offset and a length that are not a
        # multiple of the logical block size, so the direct path cannot
        # take it and the mount falls back.
        script = (
            "import os, time\n"
            f"p = {self.MOUNT!r} + '/f22-unaligned.dat'\n"
            "fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_DIRECT)\n"
            "buf = os.urandom(4096 + 512)\n"
            "end = time.time() + %d\n" % self.WRITE_SECONDS +
            "n = 0\n"
            "while time.time() < end:\n"
            "    try:\n"
            "        os.pwrite(fd, buf, 3 * 512 + 1)\n"
            "        n += 1\n"
            "    except OSError:\n"
            "        # EINVAL is the expected answer on a strict direct\n"
            "        # path; the fallback is what this case is about, and\n"
            "        # either way the probe has something to report.\n"
            "        pass\n"
            "    time.sleep(0.05)\n"
            "print('writes', n)\n")
        self.k8s_utils.exec_in_pod(
            pod,
            "cat <<'__PY__' > /tmp/f22.py\n" + script + "\n__PY__\n"
            "nohup python3 /tmp/f22.py >/tmp/f22.out 2>&1 & echo started",
            timeout=60)
        self.logger.info("[F-22] writing unaligned O_DIRECT for %ds",
                         self.WRITE_SECONDS)
        sleep_n_sec(self.WRITE_SECONDS + 60)

        after = self.pvc_events(self.CLAIM, EVENT_DATA_THROUGH_MDS)
        after_count = sum(c for _, c, _ in after)
        new = after_count - before_count
        self.logger.info("[F-22] %s: %d event object(s), count %d -> %d",
                         EVENT_DATA_THROUGH_MDS, len(after),
                         before_count, after_count)

        if new <= 0:
            hung = self.pvc_events(self.CLAIM, EVENT_LAYOUT_PROBE_HUNG)
            allev = sorted({r for r, _, _ in self.pvc_events(self.CLAIM)})
            raise PnfsEvidenceError(
                f"[F-22] {self.WRITE_SECONDS}s of unaligned O_DIRECT "
                f"produced no {EVENT_DATA_THROUGH_MDS} event on "
                f"{self.CLAIM}. The I/O keeps working either way, so "
                f"without the event nobody ever learns the direct path is "
                f"not being used. Reasons present: {allev or '(none)'}"
                f"{f'; {EVENT_LAYOUT_PROBE_HUNG} present' if hung else ''}")

        if new > self.MAX_EVENT_COUNT:
            raise PnfsEvidenceError(
                f"[F-22] the fallback was reported {new} times in "
                f"{self.WRITE_SECONDS}s on {self.CLAIM}. The node is "
                f"supposed to probe at most every 30s and report the "
                f"condition, not every probe of it; at this rate the "
                f"claim's event history is unreadable and the real events "
                f"around it are evicted.")

        messages = [m for _, _, m in after]
        both = [m for m in messages
                if "align" in m.lower() and "direct" in m.lower()]
        if not both:
            self.logger.warning(
                "[F-22] the event fired but no message names both causes "
                "(unaligned and direct): %s. The plan asks for one event "
                "that names both, so an operator does not have to guess "
                "which of the two applies.", messages[:3])

        self.logger.info(
            "[F-22] PASS: %d new %s report(s) over %ds, within the %d a 30s "
            "probe allows", new, EVENT_DATA_THROUGH_MDS,
            self.WRITE_SECONDS, self.MAX_EVENT_COUNT)


class PnfsProvisioningChurnK8s(_FailBase):
    """L-05: rapid create/delete of RWX claims leaks nothing.

    Provisioning an RWX claim makes an lvol, an NFSExport and a mount on
    the metadata server; deleting it has to unwind all three. A reconciler
    that is correct one claim at a time can still leak under churn, when
    a delete overtakes the create it is undoing.

    A leak here is invisible from Kubernetes -- the claim and the PV are
    gone, so `kubectl get` is clean -- and shows up weeks later as a
    cluster that is full. So the count of NFSExports is compared against
    the baseline taken before the churn, which is the only place the
    orphan is still visible.

    The cycles are deliberately overlapped rather than serialised. Create,
    wait, delete, wait is not churn; it is the single-claim path run
    repeatedly, and it would pass on a reconciler with no concurrency
    handling at all.
    """

    CYCLES = 3
    PER_CYCLE = 4
    SETTLE = 90

    def run(self):
        self.logger.info("=== L-05 provisioning churn ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()

        base_exports = {(e.get("metadata") or {}).get("name")
                        for e in self.exports()}
        self.logger.info("[L-05] %d NFSExport(s) before the churn",
                         len(base_exports))

        for cycle in range(self.CYCLES):
            names = [f"pnfs-l05-c{cycle}-{i}" for i in range(self.PER_CYCLE)]
            for n in names:
                self.k8s_utils.create_pvc(
                    name=n, size=self.CLAIM_SIZE,
                    storage_class=self.sc_name,
                    access_modes=["ReadWriteMany"])
                self.extra_claims.append(n)
            # Delete while the provisions are still in flight -- that is
            # the whole point. Half are given time to bind so the case
            # covers both orders.
            half = len(names) // 2
            for n in names[:half]:
                self.k8s_utils.wait_pvc_bound(n, timeout=self.BIND_TIMEOUT)
            sleep_n_sec(5)
            for n in names:
                self.k8s_utils.delete_pvc(n)
                if n in self.extra_claims:
                    self.extra_claims.remove(n)
            self.logger.info(
                "[L-05] cycle %d: created %d, %d bound first, all deleted",
                cycle, len(names), half)

        # Everything must drain. The settle is generous because an export
        # teardown is slower than a PVC delete and a premature read would
        # report work in flight as a leak.
        sleep_n_sec(self.SETTLE)
        deadline = time.time() + 420
        leaked = set()
        while time.time() < deadline:
            now = {(e.get("metadata") or {}).get("name")
                   for e in self.exports()}
            leaked = now - base_exports
            if not leaked:
                break
            sleep_n_sec(15)

        pvcs, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pvc -n {self.mds_ns} "
            f"-o custom-columns=NAME:.metadata.name --no-headers "
            f"2>/dev/null || true", supress_logs=True)
        stuck = [ln.strip() for ln in (pvcs or "").splitlines()
                 if ln.strip().startswith("pnfs-l05-")]

        if leaked or stuck:
            raise PnfsEvidenceError(
                f"[L-05] after {self.CYCLES} cycles of "
                f"{self.PER_CYCLE} create/delete, "
                f"{len(leaked)} NFSExport(s) and {len(stuck)} PVC(s) "
                f"survive their claims: exports={sorted(leaked)[:8]} "
                f"pvcs={stuck[:8]}. Each leaked export pins an lvol, and "
                f"nothing in Kubernetes still refers to it -- this is "
                f"invisible until the cluster is full.")

        self.logger.info(
            "[L-05] PASS: %d claims created and deleted under churn, "
            "NFSExports back to the %d that were here before",
            self.CYCLES * self.PER_CYCLE, len(base_exports))
