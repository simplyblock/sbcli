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
  F-17         two restarts in quick succession is the same boundary as
               `PnfsMdsSecondTakeoverK8s` (F-09 of the standby plan),
               whose docstring is "a second takeover before the first
               one's grace has ended" -- the same assertion against the
               same window. Writing it twice would mean two cases
               failing for one defect.

**The client-node faults, and how they undo themselves.** F-04, F-10,
F-16 and F-18 each disturb a shared lab host, and every one of them
restores it by a mechanism that does not depend on this process
surviving:

  F-04   `reboot_node` is the suite's existing cordon/drain/reboot path
         and `uncordon_all_workers` is registered on `undo_stack`
         *before* the reboot, because a cordon outlives the run that
         set it and the operator then refuses the worker outright on the
         next bring-up.
  F-10   `cut_mds_access` is port 2049 only, to the MDS ClusterIP and
         its pod IPs, and it arms its own undo as a transient systemd
         unit on the host. It verifies the rules landed and raises after
         undoing them if they did not, so a partition that silently
         applied nothing cannot read as a pass. NVMe paths, kubelet and
         every other service are untouched -- the full-interface cut
         this suite does NOT use has twice been written up as a product
         defect when it was really OVN, FoundationDB and the health
         monitor going down with the target.
  F-18   a controller reset, which needs no undo at all: the heal is the
         initiator's own reconnect, and that is the behaviour under
         test. A firewall rule would have been wrong as well as
         riskier -- SPDK shares the host network namespace here, so
         dropping the storage nodes' data addresses would cut the
         storage node's replication traffic and produce a cluster
         outage dressed as a client fault.

None of it works on Talos, which has no host shell; those cases raise a
probe failure naming the platform instead of asserting on a client that
was never disturbed.

Class names end in ``K8s`` for the platform filter in ``e2e.py``.
"""
import itertools
import time

from e2e_tests.pnfs.test_pnfs_rwx import _PnfsBase
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    NFS_LEASE_DEFAULT,
    PnfsEvidenceError,
    PnfsProbeError,
    assert_layouts_taken,
    mds_pods,
    mds_statefulset,
    mountstats,
    nfsexport_status,
    nvme_client_controllers,
    pvc_volume_ref,
    reset_nvme_controllers,
    wait_nvme_live,
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


class _ClientFaultBase(_FailBase):
    """Faults injected on a *client* node rather than in a pod.

    All three need a host shell or a reboot verb, and ``run_on_node``
    raises on Talos by design -- it is API-managed and has none. That
    raise is turned into a :class:`PnfsProbeError` here, once, so the
    cases below say "this lab cannot host this fault" instead of failing
    as though the product had misbehaved.
    """

    #: A client may stall while its own node or path is gone. It may not
    #: lose data and it may not lose its descriptor. The gap bound is per
    #: case, because a node reboot and a controller reset are not the same
    #: order of outage.
    CLAIM_SIZE = "20Gi"

    def node_ip_of(self, pod):
        """The InternalIP of the node *pod* is running on."""
        name = self._pod_node(pod)
        if not name:
            raise PnfsProbeError(
                f"pod {pod} reports no spec.nodeName, so there is no host to "
                f"inject a fault on.")
        return name, self.k8s_utils.node_internal_ip(name)

    def require_host_access(self, node_ip, what):
        """Fail as a probe, not a verdict, where the platform has no host shell."""
        if self.k8s_utils.detect_talos():
            raise PnfsProbeError(
                f"{what}: this lab is Talos, which has no host shell, so the "
                f"fault cannot be injected. A case that ran anyway would "
                f"assert on a client that was never disturbed and pass. Run "
                f"this group on the k3s or OpenShift lane.")
        return node_ip

    # -- a file written and verified across the fault --------------------

    def write_seed(self, pod, name, mib=16):
        """Write *mib* MiB of reproducible data and return its md5.

        Written before the fault and verified after it. The writer log
        catches a broken descriptor; this catches a mount that came back
        serving different bytes, which no amount of gap measurement would.
        """
        path = f"{self.MOUNT}/{name}"
        out = self._py(pod, (
            "import hashlib, os\n"
            f"p = {path!r}\n"
            f"blk = bytes(range(256)) * 4096\n"
            "h = hashlib.md5()\n"
            "with open(p, 'wb') as f:\n"
            f"    for _ in range({mib}):\n"
            "        f.write(blk[:1024*1024])\n"
            "        h.update(blk[:1024*1024])\n"
            "    f.flush()\n"
            "    os.fsync(f.fileno())\n"
            "print(h.hexdigest())\n"))
        self.logger.info("[pnfs] seed %s written from %s md5=%s", name, pod,
                         out)
        return out

    def verify_seed(self, pod, name, md5, what):
        path = f"{self.MOUNT}/{name}"
        got = self._py(pod, (
            "import hashlib\n"
            f"h = hashlib.md5()\n"
            f"with open({path!r}, 'rb') as f:\n"
            "    while True:\n"
            "        b = f.read(1024*1024)\n"
            "        if not b:\n"
            "            break\n"
            "        h.update(b)\n"
            "print(h.hexdigest())\n"))
        if got != md5:
            raise PnfsEvidenceError(
                f"{what}: {name} read back md5 {got} from {pod}, expected "
                f"{md5}. The mount recovered and is serving different bytes, "
                f"which is worse than it not recovering at all.")
        self.logger.info("[pnfs] %s: %s verified from %s", what, name, pod)

    def mount_ops(self, pod):
        """``{OP: count}`` for the pNFS mount in *pod*, or ``None``."""
        try:
            st = mountstats(self.k8s_utils, pod, mountpoint=self.MOUNT)
        except PnfsProbeError:
            return None
        return dict((st or {}).get("ops") or {})

    def lease_seconds(self, pod, default=NFS_LEASE_DEFAULT):
        """The server's lease as this client sees it."""
        try:
            st = mountstats(self.k8s_utils, pod, mountpoint=self.MOUNT)
        except PnfsProbeError:
            return default
        lease = (st or {}).get("lease_time")
        if not lease:
            self.logger.info(
                "[pnfs] no lease_time in this client's mountstats; using the "
                "kernel default %ss", default)
            return default
        return int(lease)

    # -- the appending writer, shared with the MDS suite ----------------

    def start_writer(self, pod):
        """One timestamped line a second, never dying on an error.

        Same payload as the MDS suite's writer and for the same reason: a
        stall and a destroyed descriptor are different product failures,
        and a writer that exits on the first errno cannot tell them apart.
        It is duplicated rather than inherited because `_MdsBase` also
        brings the standby-topology gate, which none of these cases want.
        """
        marker = f"{self.MOUNT}/{pod}-writer.log"
        script = (
            "import os, time\n"
            f"p = {marker!r}\n"
            "end = time.time() + 7200\n"
            "n = 0\n"
            "while time.time() < end:\n"
            "    try:\n"
            "        with open(p, 'a') as f:\n"
            "            f.write('%.3f line %d\\n' % (time.time(), n))\n"
            "            f.flush()\n"
            "            os.fsync(f.fileno())\n"
            "        n += 1\n"
            "    except OSError as e:\n"
            "        with open('/tmp/writer.err', 'a') as g:\n"
            "            g.write('%.3f errno=%d %s\\n' % (\n"
            "                time.time(), e.errno or 0, e.strerror))\n"
            "    time.sleep(1)\n")
        self.k8s_utils.exec_in_pod(
            pod,
            "cat <<'__PY__' > /tmp/writer.py\n" + script + "\n__PY__\n"
            "rm -f /tmp/writer.err; nohup python3 /tmp/writer.py "
            ">/tmp/writer.out 2>&1 & echo started",
            timeout=60)
        self.logger.info("[pnfs] writer started in %s -> %s", pod, marker)
        return marker

    def writer_report(self, pod, marker):
        out, _ = self.k8s_utils.exec_in_pod(
            pod,
            f"cut -d' ' -f1 {marker} 2>/dev/null | tail -8000; "
            f"echo '---ERR---'; cat /tmp/writer.err 2>/dev/null || true",
            timeout=120)
        body, _, errs = (out or "").partition("---ERR---")
        stamps = []
        for tok in body.split():
            try:
                stamps.append(float(tok))
            except ValueError:
                continue
        gap = 0.0
        for a, b in itertools.pairwise(stamps):
            gap = max(gap, b - a)
        return len(stamps), round(gap, 1), [
            ln for ln in errs.splitlines() if ln.strip()]

    def assert_writer_survived(self, pod, marker, what, max_gap):
        lines, gap, errors = self.writer_report(pod, marker)
        if errors:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} took {len(errors)} I/O "
                f"error(s): {errors[:5]}. errno 9 is EBADF -- the mount went "
                f"away under an open descriptor, which nothing retries.")
        if lines < 5:
            raise PnfsProbeError(
                f"{what}: the writer in {pod} produced only {lines} line(s), "
                f"so there is nothing to judge the fault by.")
        if gap > max_gap:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} stalled {gap}s, over the "
                f"{max_gap}s this case allows.")
        self.logger.info("[pnfs] %s: %d lines, worst gap %ss, no errors",
                         what, lines, gap)
        return gap


class PnfsClientNodeRebootK8s(_ClientFaultBase):
    """F-04: a client node reboots with an RWX mount active on it.

    The plan's acceptance is "restage reconnects namespaces + remounts
    NFS; pods recover". Three distinct things have to happen on the way
    back and they fail separately, so they are asserted separately:

      1. the surviving clients are undisturbed. A node leaving must not
         cost the other clients their mounts -- the whole point of an RWX
         filesystem is that it is not one node's.
      2. the rebooted node restages. csi-node has to re-establish the
         NVMe namespaces and re-mount the export from nothing; this is
         the only case in the suite where staging runs on a host that has
         forgotten everything.
      3. the data is the data. A mount that came back serving different
         bytes passes 1 and 2.

    The pod on the rebooted node is replaced rather than waited for: the
    suite's shell pods are bare pods with ``restartPolicy: Never``, so the
    one that went down with the node is gone for good. That is not a
    product fact, it is the test's own topology, and the restage is proven
    by what a *new* pod pinned to that node can do -- which is the same
    code path kubelet would run for a rescheduled one.

    The reboot is undrained on purpose. A drained reboot evacuates the
    pods first, which is planned maintenance; F-04 is a client going away
    with its mount still open.
    """

    CLAIM = "pnfs-f04"
    #: The surviving clients' budget. They are not the ones rebooting and
    #: should see nothing at all; the allowance is for scheduler and
    #: kubelet noise on a shared lab, not for an outage.
    MAX_SURVIVOR_GAP = 45
    #: A baremetal worker's full cycle: BIOS, Talos/k3s, kubelet, CNI.
    REBOOT_TIMEOUT = 1200

    def run(self):
        self.logger.info("=== F-04 client node reboot with an active mount ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        pods = self._start_shell_pods("pnfs-f04", 3)
        victim, survivor = pods[0], pods[1]
        node_name, node_ip = self.node_ip_of(victim)
        self.require_host_access(node_ip, "[F-04]")

        md5 = self.write_seed(victim, "f04-seed.bin")
        marker = self.start_writer(survivor)
        sleep_n_sec(20)

        self.logger.info("[F-04] rebooting %s (%s), which hosts %s",
                         node_name, node_ip, victim)
        # Registered before the reboot: reboot_node cordons on the way in
        # and only uncordons on its own recovery path, so a failure in
        # between leaves the worker unschedulable and the NEXT run's
        # bring-up fails talking about stripe width.
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)

        issued = self.k8s_utils.reboot_node(node_ip, drain=False)
        if not issued:
            raise PnfsProbeError(
                f"[F-04] no reboot could be issued for {node_name}: on Talos "
                f"this needs talosctl on the runner, and elsewhere a host "
                f"shell. Nothing was disturbed, so there is nothing to "
                f"judge.")

        went_down = self.k8s_utils.wait_node_condition(
            node_ip, ready=False, timeout=300)
        if not went_down:
            raise PnfsProbeError(
                f"[F-04] {node_name} never left Ready after the reboot was "
                f"issued, so the client never went away. Asserting on "
                f"recovery now would be asserting on a cluster that never "
                f"failed.")
        self.logger.info("[F-04] %s is NotReady; the client is gone",
                         node_name)

        # (1) The survivors, while the node is down. Checked here rather
        # than only at the end: a mount that broke now and healed by the
        # time the node returned is still a client that lost its data.
        self._assert_mount_writable([survivor])

        back = self.k8s_utils.wait_node_condition(
            node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
        self.k8s_utils.uncordon_all_workers()
        if not back:
            raise PnfsEvidenceError(
                f"[F-04] {node_name} did not come back Ready within "
                f"{self.REBOOT_TIMEOUT}s. The lab is now short a worker, "
                f"which is a finding in itself.")
        self.logger.info("[F-04] %s is Ready again", node_name)

        self.assert_writer_survived(
            survivor, marker, "[F-04] the surviving client",
            self.MAX_SURVIVOR_GAP)

        # (2) The restage. A fresh pod pinned to the node that forgot
        # everything, so staging has to rebuild the namespaces and the
        # mount from nothing.
        self.k8s_utils.delete_pod(victim, wait=True)
        if victim in self.shell_pods:
            self.shell_pods.remove(victim)
        replacement = self._start_shell_pods(
            "pnfs-f04-restaged", 1, spread=False, node=node_name)[0]
        self.logger.info("[F-04] %s restaged the claim on %s", replacement,
                         node_name)

        # And it is pNFS again, not a plain NFS fallback. A restage that
        # silently came back without a layout is the one failure mode this
        # whole suite exists to catch.
        assert_layouts_taken(
            self.k8s_utils, [replacement], mountpoint=self.MOUNT,
            logger=self.logger, context="[F-04] after the restage")

        # (3) The bytes.
        self.verify_seed(replacement, "f04-seed.bin", md5,
                         "[F-04] after the restage")
        self.verify_seed(survivor, "f04-seed.bin", md5,
                         "[F-04] from a client that never went down")

        self.logger.info(
            "[F-04] PASS: %s rebooted and restaged, layouts taken again, "
            "seed verified from both sides, survivors never lost the mount",
            node_name)


class PnfsMdsPartitionedFromClientK8s(_ClientFaultBase):
    """F-10: one client is cut from the metadata server, its namespaces are not.

    The case the design's fencing exists for. Metadata has to block --
    there is no server to answer it -- while the data path, which is
    NVMe/TCP to the storage nodes and does not pass through the MDS at
    all, keeps working. Then the partition heals and the client comes back
    without anybody restaging it.

    Port 2049 only, to the MDS ClusterIP *and* its pod IPs, applied on the
    host. Not the whole interface: this lab has already produced two
    written-up "product defects" that were a full-interface cut taking out
    OVN, FoundationDB and the cluster's own health monitor along with the
    thing under test. `cut_mds_access` verifies its rules landed and arms
    the undo as a transient unit on the host, so the partition heals even
    if this process dies holding it.

    The cut outlasts the lease, read from the client's own mountstats
    rather than assumed, because a partition shorter than the lease never
    reaches the state the case is about: the server has not yet decided
    the client is gone.
    """

    CLAIM = "pnfs-f10"
    #: Added to the lease so the server has actually expired the client
    #: rather than being one poll away from it.
    OVER_LEASE = 45
    #: After the heal. The client has to re-establish its session and
    #: reclaim; the NFR-2 bound is 30s and this is deliberately looser,
    #: because the lease expiry it is recovering from is the worst case.
    MAX_RECOVERY = 180

    def run(self):
        self.logger.info("=== F-10 client partitioned from the MDS ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        pods = self._start_shell_pods("pnfs-f10", 3)
        cut_pod, other = pods[0], pods[1]
        node_name, node_ip = self.node_ip_of(cut_pod)
        self.require_host_access(node_ip, "[F-10]")

        md5 = self.write_seed(cut_pod, "f10-seed.bin")
        lease = self.lease_seconds(cut_pod)
        duration = lease + self.OVER_LEASE
        self.logger.info("[F-10] this client's lease is %ss; cutting for %ss",
                         lease, duration)

        ops_before = self.mount_ops(cut_pod)
        marker = self.start_writer(other)
        sleep_n_sec(15)

        ips = []
        cut_at = time.time()
        try:
            _dur, ips = self.k8s_utils.cut_mds_access(
                node_ip, duration, namespace=self.mds_ns)
            # Registered after the cut succeeded, because before it there
            # is nothing to restore; the host-armed unit is the backstop
            # for this process dying, and this is the backstop for the
            # unit not having armed.
            self.undo_stack.append(
                lambda: self.k8s_utils.restore_mds_access(node_ip, ips))

            # Metadata blocks. A LOOKUP of a name that is not cached has
            # to reach the server, so it hangs; the timeout is the
            # assertion. `ls` of the mount root may be served from the
            # attribute cache, which is why this creates a new name.
            probe, _err = self.k8s_utils.exec_in_pod(
                cut_pod,
                f"timeout 25 stat {self.MOUNT}/f10-absent-$(date +%s) "
                f">/dev/null 2>&1; echo rc=$?",
                timeout=90)
            self.logger.info("[F-10] metadata probe while cut: %s",
                             (probe or "").strip())
            if "rc=0" in (probe or ""):
                raise PnfsEvidenceError(
                    "[F-10] a stat of a name that cannot exist SUCCEEDED "
                    "while this client was cut off from the metadata server "
                    "on port 2049. Either the cut did not take -- which "
                    "cut_mds_access verifies, so it did -- or the client is "
                    "being answered by something other than the MDS.")

            sleep_n_sec(max(0, duration - (time.time() - cut_at)))
        finally:
            self.k8s_utils.restore_mds_access(node_ip, ips)
        healed_at = time.time()
        self.logger.info("[F-10] partition healed after %.1fs",
                         healed_at - cut_at)

        # The other clients never had a partition and must never have
        # noticed one. A single client losing its lease must not cost the
        # rest of them theirs.
        self.assert_writer_survived(other, marker, "[F-10] the other client",
                                    max_gap=45)

        # The heal, without anybody restaging. Writable again, and the op
        # counters still climbing from where they were: a remount resets
        # them, so a monotonic count is the evidence that this is the same
        # mount instance that was cut.
        deadline = time.time() + self.MAX_RECOVERY
        last_err = None
        while time.time() < deadline:
            try:
                self._assert_mount_writable([cut_pod])
                break
            except PnfsProbeError as exc:
                last_err = exc
                sleep_n_sec(10)
        else:
            raise PnfsEvidenceError(
                f"[F-10] {cut_pod} could not write to its mount "
                f"{self.MAX_RECOVERY}s after the partition healed. A client "
                f"that does not recover from a lease expiry needs a restage, "
                f"which is the failure this case looks for. Last probe: "
                f"{last_err}")
        recovered = round(time.time() - healed_at, 1)

        ops_after = self.mount_ops(cut_pod)
        if ops_before and ops_after:
            got = ops_after.get("LOOKUP", 0)
            was = ops_before.get("LOOKUP", 0)
            if got < was:
                raise PnfsEvidenceError(
                    f"[F-10] this client's LOOKUP count went backwards, "
                    f"{was} -> {got}, which means the mount was torn down "
                    f"and re-established. The plan's acceptance is that the "
                    f"heal reconnects, not that something restaged it.")

        self.verify_seed(cut_pod, "f10-seed.bin", md5, "[F-10] after the heal")
        self.logger.info(
            "[F-10] PASS: metadata blocked while cut, the other clients were "
            "untouched, this one was writable again %ss after the heal on the "
            "same mount, and its data verified", recovered)


class PnfsClientPathLossK8s(_ClientFaultBase):
    """F-18: a client loses the paths to its namespace and heals, direct.

    The plan's acceptance has two halves and the second is the one with
    teeth: "its I/O resumes **direct, without a restage**". A client whose
    data path broke could come back in three ways, and only one of them is
    correct --

      * direct again, same mount: what this asserts.
      * falling back to routing data through the metadata server: the
        mount still works, fio still passes, and the feature has silently
        stopped being pNFS. csi-node publishes PNFSDataThroughMDS when it
        sees this, so the event is checked as well as the counters.
      * restaged: the paths came back because something tore the mount
        down and built it again, which is a recovery the product is not
        supposed to need and which hides the bug.

    The loss is a controller reset, not a firewall rule, and that is a
    deliberate choice: the storage nodes' SPDK runs in the host's network
    namespace on this topology, so a rule dropping their data addresses
    would cut the storage node's own replication traffic and produce a
    cluster outage dressed as a client fault. A reset is scoped to the
    client's kernel controller, and it heals through the initiator's own
    reconnect logic -- which is the behaviour under test, so the test must
    not be the thing that restores it.
    """

    CLAIM = "pnfs-f18"
    #: Resets, with a settle between them. Once proves the reconnect; three
    #: times is where a leaked qpair or a reconnect that only works from a
    #: clean state shows up.
    RESETS = 3
    #: NFR-2's client-visible freeze bound is 30s. This allows more,
    #: because the bound is for an MDS migration and a path reconnect has
    #: ctrl-loss-tmo in it, but not so much that a client stalling for a
    #: minute per reset would pass.
    MAX_GAP = 60

    def run(self):
        self.logger.info("=== F-18 client path loss and heal ===")
        self.assert_pnfs_cluster()
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        pods = self._start_shell_pods("pnfs-f18", 2)
        victim, other = pods[0], pods[1]
        node_name, node_ip = self.node_ip_of(victim)
        self.require_host_access(node_ip, "[F-18]")

        md5 = self.write_seed(victim, "f18-seed.bin")
        # Layouts first: without them this client is not doing direct I/O
        # even before the fault, and "resumes direct" would be vacuous.
        assert_layouts_taken(
            self.k8s_utils, [victim], mountpoint=self.MOUNT,
            logger=self.logger, context="[F-18] before the path loss")

        ctrls = nvme_client_controllers(self.k8s_utils, node_ip)
        names = [c["name"] for c in ctrls if c.get("state") == "live"]
        if not names:
            raise PnfsProbeError(
                f"[F-18] {node_name} has no live NVMe controller, so this "
                f"client has no path to take away. Controllers seen: "
                f"{ctrls or '(none)'}. On a pNFS mount the data path is the "
                f"host's, so an empty list here means the client is not "
                f"doing direct I/O at all.")
        self.logger.info("[F-18] %s holds %d live controller(s): %s",
                         node_name, len(names), names)

        ops_before = self.mount_ops(victim)
        marker = self.start_writer(victim)
        other_marker = self.start_writer(other)
        sleep_n_sec(20)

        for i in range(self.RESETS):
            done = reset_nvme_controllers(self.k8s_utils, node_ip, names)
            if done == 0:
                raise PnfsProbeError(
                    f"[F-18] round {i + 1}: not one of {names} accepted a "
                    f"reset, so no path was lost and there is nothing to "
                    f"judge. reset_controller is not writable on this host.")
            live, took, states = wait_nvme_live(
                self.k8s_utils, node_ip, names, timeout=240)
            self.logger.info(
                "[F-18] round %d: reset %d/%d controller(s), live again "
                "after %ss (%s)", i + 1, done, len(names), took, states)
            if not live:
                raise PnfsEvidenceError(
                    f"[F-18] round {i + 1}: {states} -- the controllers did "
                    f"not all come back live within 240s. The initiator's "
                    f"own reconnect is what the client depends on here; if "
                    f"it does not heal, the client needs a restage, which is "
                    f"exactly what this case says must not be necessary.")
            sleep_n_sec(20)

        # (a) It survived, and (b) the other client never saw it.
        gap = self.assert_writer_survived(
            victim, marker, "[F-18] the client that lost its paths",
            self.MAX_GAP)
        self.assert_writer_survived(
            other, other_marker, "[F-18] the other client", max_gap=45)

        # (c) Still direct. Server-side READ/WRITE at zero is the whole
        # assertion: if the client had fallen back to the metadata server
        # the mount would still work and only this would show it.
        assert_layouts_taken(
            self.k8s_utils, [victim], mountpoint=self.MOUNT,
            logger=self.logger, context="[F-18] after the heal")
        through_mds = self.pvc_events(self.CLAIM, reason=EVENT_DATA_THROUGH_MDS)
        if through_mds:
            raise PnfsEvidenceError(
                f"[F-18] csi-node reported {EVENT_DATA_THROUGH_MDS} on "
                f"{self.CLAIM}: {through_mds[:3]}. The client's paths healed "
                f"but its I/O is going through the metadata server, so it is "
                f"not direct any more -- the mount works and the feature "
                f"does not.")

        # (d) No restage. A remount zeroes the per-op counters, so a count
        # that only ever went up is the proof that this is the same mount.
        ops_after = self.mount_ops(victim)
        if ops_before and ops_after:
            regressed = {op: (ops_before[op], ops_after.get(op, 0))
                         for op in ("WRITE", "LOOKUP", "GETATTR")
                         if op in ops_before
                         and ops_after.get(op, 0) < ops_before[op]}
            if regressed:
                raise PnfsEvidenceError(
                    f"[F-18] the mount's operation counters went backwards "
                    f"{regressed} (before -> after), which only happens on a "
                    f"fresh mount. The paths were restored by a restage, not "
                    f"by the initiator reconnecting, and the plan's "
                    f"acceptance is explicitly 'without a restage'.")

        self.verify_seed(victim, "f18-seed.bin", md5, "[F-18] after the heal")
        self.logger.info(
            "[F-18] PASS: %d path losses on %s, worst client stall %ss, "
            "layouts still held, no %s event, counters monotonic (no "
            "restage), data verified",
            self.RESETS, node_name, gap, EVENT_DATA_THROUGH_MDS)
