"""Track C: the warm-standby metadata server.

`test-plan-pnfs-mds-standby.md` scenario IDs. The plan's live-cluster
classes are `E-` (end-to-end, at least two KVM-capable nodes) and `F-`
(failure injection); `U-`, `O-` and `I-` are the operator's own and are
not ours to write.

Why this file exists at all, and first: at the time of writing every
defect we have filed against pNFS lives in this surface and none of it
had a regression guard --

  SFAM-2860   a flat 1m30s unhealthy deadline destroyed the single MDS
              in the middle of an in-tolerance storage outage, and the
              replacement could not re-attach the still-down node's
              volumes, so those clients' fds died with EBADF
  SFAM-2850   a metadata server failover fenced a client mid-write
  (unfiled)   replacing the MDS cost 93s of cluster-wide stall

Every one of those is a takeover going wrong, and a takeover is exactly
what this file drives on purpose.

**The record, not the pod, is the source of truth.** A guest always boots
as a standby and only `Promote` makes it active (design §4), the readiness
probe is SERVING for a healthy standby too (§5.5), and every guest shares
the StatefulSet's hostname so a client cannot tell them apart either. So
"did a takeover happen" is answered from the epoch on the StatefulSet
annotation and nowhere else. Asserting on which pod is Ready, or on pod
ordinals, would pass on a cluster that never promoted anything -- and a
pNFS suite that runs green while silently never failing over is the same
failure as the plain-NFS fallback `test_pnfs_rwx` gates against.

Class names end in ``K8s``: the platform filter in ``e2e.py`` keys on the
suffix, and none of this has a Docker variant.
"""
import itertools
import time

from e2e_tests.pnfs.test_pnfs_rwx import _PnfsBase
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    ANNO_MDS_EPOCH,
    EVENT_MDS_FENCE_FAILED,
    EVENT_MDS_PROMOTED,
    EVENT_MDS_PROMOTION_CONFLICT,
    EVENT_MDS_STANDBY_MISSING,
    MDS_TERMINATION_GRACE,
    NFS_LEASE_DEFAULT,
    PnfsEvidenceError,
    PnfsProbeError,
    mds_pods,
    mds_record,
    mds_statefulset,
    mountstats,
    nfsexport_status,
    wait_mds_promotion,
)


class _MdsBase(_PnfsBase):
    """Shared machinery for the takeover cases."""

    DRIVER = "simplyblock"

    #: How long a takeover is given. The grace period alone is 45s
    #: (design §5), and the promotion that follows connects and fences the
    #: state volume and every export namespace before nfsd starts, so a
    #: budget near the grace period would be timing the harness.
    TAKEOVER_TIMEOUT = 300

    #: How long exports are watched for a takeover that should NOT happen.
    #: Long enough for several operator reconciles; a takeover that arrives
    #: a minute late is still a takeover that should not have happened.
    QUIET_SECONDS = 150

    def setup(self):
        super().setup()
        self.sts_name = None
        self.mds_ns = self.k8s_utils.namespace

    # ── the gate ─────────────────────────────────────────────────────────

    def assert_mds_cluster(self, want_standbys=True):
        """Refuse the case unless there is a standby topology to test.

        Separate from ``assert_pnfs_cluster``, which only proves something
        can serve an export. These cases drive takeovers, and a takeover on
        a cluster with one guest and no standby is a different scenario
        (E-03) that happens to look similar: the active is lost, nothing is
        promoted, and the exports come back only when the pod is rescheduled.
        Running an F- case against that shape would report the product
        broken for doing exactly what `standbys: 0` asks of it.
        """
        sts = mds_statefulset(self.k8s_utils, self.mds_ns)
        if not sts:
            raise PnfsProbeError(
                "no pnfs-mds StatefulSet in "
                f"{self.mds_ns}: this cluster runs no metadata server, so "
                "there is nothing to fail over. The standby cases need an "
                "operator that reconciles spec.pnfs.mds.")
        self.sts_name = (sts.get("metadata") or {}).get("name")
        replicas = ((sts.get("spec") or {}).get("replicas")) or 0
        pods = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
        active, epoch = mds_record(sts)

        if want_standbys and replicas < 2:
            raise PnfsProbeError(
                f"StatefulSet {self.sts_name} runs {replicas} replica(s), so "
                f"there is no standby to promote. Set "
                f"spec.pnfs.mds.standbys >= 1 on driver {self.DRIVER}, and "
                f"note each standby needs its own kvm-capable node.")

        nodes = {n for _, n, _, _ in pods if n and n != "<none>"}
        if want_standbys and len(nodes) < len(pods):
            raise PnfsProbeError(
                f"two MDS guests share a node: {pods}. The design places "
                f"each on its own node, and a takeover onto the node that "
                f"just died proves nothing.")

        self.logger.info(
            "[mds] gate passed: sts=%s replicas=%s pods=%s active=%r epoch=%s",
            self.sts_name, replicas, [p[0] for p in pods], active, epoch)
        return sts

    # ── reading the record ───────────────────────────────────────────────

    def record(self):
        return mds_record(mds_statefulset(self.k8s_utils, self.mds_ns))

    def assert_no_promotion(self, from_epoch, seconds, what):
        """Watch for *seconds* and fail if the epoch moves.

        Used by the cases whose whole claim is that something does NOT
        cause a takeover. Sampled rather than checked once at the end: an
        epoch that advances and then advances again would otherwise look
        like one promotion, and a promotion that is undone looks like none.
        """
        deadline = time.time() + seconds
        while time.time() < deadline:
            active, epoch = self.record()
            if epoch != from_epoch:
                raise PnfsEvidenceError(
                    f"{what}: the epoch moved {from_epoch} -> {epoch} "
                    f"(active={active!r}). Nothing here should have caused a "
                    f"takeover, and each one costs every client of every "
                    f"export the grace period.")
            sleep_n_sec(10)
        self.logger.info("[mds] %s: epoch held at %s for %ds",
                         what, from_epoch, seconds)

    # ── events ───────────────────────────────────────────────────────────

    def event_reasons(self):
        """Event reasons in the namespace, driver-scoped first.

        The driver is where the design says the MDS events land, but a
        field selector that matches nothing returns nothing rather than
        erroring, which would read as "the operator said nothing". So an
        empty driver-scoped result falls back to every event in the
        namespace before anyone concludes the operator was silent.
        """
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get events -n {self.mds_ns} "
            f"--field-selector involvedObject.kind=SimplyblockDriver "
            f"-o custom-columns=REASON:.reason --no-headers "
            f"2>/dev/null || true", supress_logs=True)
        reasons = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        if not reasons:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get events -n {self.mds_ns} "
                f"-o custom-columns=REASON:.reason --no-headers "
                f"2>/dev/null || true", supress_logs=True)
            reasons = [ln.strip() for ln in (out or "").splitlines()
                       if ln.strip()]
        return reasons

    # ── exports ──────────────────────────────────────────────────────────

    def exports_ready(self):
        """``(ready, total)`` over every NFSExport in the namespace."""
        items = nfsexport_status(self.k8s_utils, namespace=self.mds_ns) or []
        ready = 0
        for it in items:
            st = (it.get("status") or {})
            phase = st.get("phase") or st.get("state") or ""
            conds = {c.get("type"): c.get("status")
                     for c in (st.get("conditions") or [])}
            if phase == "Ready" or conds.get("Ready") == "True":
                ready += 1
        return ready, len(items)

    def wait_exports_ready(self, total, timeout=300, what="exports"):
        deadline = time.time() + timeout
        ready = 0
        while time.time() < deadline:
            ready, seen = self.exports_ready()
            if ready >= total and seen >= total:
                self.logger.info("[mds] %s: %d/%d Ready", what, ready, total)
                return True
            sleep_n_sec(10)
        raise PnfsEvidenceError(
            f"{what}: only {ready} of {total} NFSExport(s) are Ready "
            f"{timeout}s later. An export that does not come back after a "
            f"takeover is a volume no client can reach.")

    # ── changing the standby count, reversibly ───────────────────────────

    def set_standbys(self, count):
        """Patch spec.pnfs.mds.standbys, registering the undo first.

        Registered before the mutation, and on ``undo_stack`` so teardown
        runs it even when run() raised. A lab left at standbys=0 silently
        turns every later F- case into E-03 -- they would not fail, they
        would pass while testing something else.
        """
        current, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get simplyblockdriver {self.DRIVER} -n {self.mds_ns} "
            f"-o jsonpath='{{.spec.pnfs.mds.standbys}}'", supress_logs=True)
        current = (current or "").strip().strip("'")
        if not hasattr(self, "_standbys_restored"):
            was = current if current != "" else "1"

            def restore():
                self.logger.info("[mds] restoring standbys=%s", was)
                self.k8s_utils._exec_kubectl(
                    f"kubectl patch simplyblockdriver {self.DRIVER} "
                    f"-n {self.mds_ns} --type=merge "
                    f"-p '{{\"spec\":{{\"pnfs\":{{\"mds\":"
                    f"{{\"standbys\":{was}}}}}}}}}'")
            self.undo_stack.append(restore)
            self._standbys_restored = True

        self.k8s_utils._exec_kubectl(
            f"kubectl patch simplyblockdriver {self.DRIVER} -n {self.mds_ns} "
            f"--type=merge -p '{{\"spec\":{{\"pnfs\":{{\"mds\":"
            f"{{\"standbys\":{count}}}}}}}}}'")
        self.logger.info("[mds] standbys set to %s (was %r)", count, current)

    def wait_replicas(self, want, timeout=420):
        deadline = time.time() + timeout
        seen = None
        while time.time() < deadline:
            sts = mds_statefulset(self.k8s_utils, self.mds_ns)
            seen = ((sts.get("spec") or {}).get("replicas"))
            if seen == want:
                return True
            sleep_n_sec(10)
        raise PnfsEvidenceError(
            f"the MDS StatefulSet still requests {seen} replica(s) {timeout}s "
            f"after standbys was set to produce {want}.")

    # ── a writer that survives, and says whether it did ──────────────────

    def start_writer(self, pod):
        """Start an appending writer in *pod* and return its marker path.

        Appends one timestamped line a second to its own file and never
        stops on error -- it records the error instead. A takeover that
        breaks a client shows up as a gap and an errno in this file, which
        is what distinguishes "the client paused for the grace period" from
        "the client's fd was destroyed". SFAM-2860 was exactly the second,
        and a writer that dies on the first error cannot tell them apart.
        """
        marker = f"{self.MOUNT}/{pod}-writer.log"
        script = (
            "import os, time, errno\n"
            f"p = {marker!r}\n"
            "end = time.time() + 3600\n"
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
        self.logger.info("[mds] writer started in %s -> %s", pod, marker)
        return marker

    def writer_report(self, pod, marker):
        """``(lines, max_gap_seconds, errors)`` for a writer started above.

        The gap is what a takeover costs a client that keeps its fd. The
        errors are what it costs one that does not: EBADF (errno 9) here
        means the export went away underneath an open descriptor, which no
        application retries, and is the fingerprint of SFAM-2860.
        """
        out, _ = self.k8s_utils.exec_in_pod(
            pod,
            f"cut -d' ' -f1 {marker} 2>/dev/null | tail -4000; "
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
        errors = [ln for ln in errs.splitlines() if ln.strip()]
        return len(stamps), round(gap, 1), errors

    def assert_client_survived(self, pod, marker, what, max_gap):
        lines, gap, errors = self.writer_report(pod, marker)
        if errors:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} took {len(errors)} I/O "
                f"error(s) across the takeover: {errors[:5]}. errno 9 is "
                f"EBADF -- the export went away under an open descriptor, "
                f"which nothing retries. A takeover is supposed to stall a "
                f"client, not break it.")
        if lines < 5:
            raise PnfsProbeError(
                f"{what}: the writer in {pod} produced only {lines} line(s), "
                f"so there is nothing to judge the takeover by.")
        if gap > max_gap:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} stalled {gap}s across the "
                f"takeover, over the {max_gap}s this case allows. Every "
                f"client of every export on this metadata server stalled "
                f"with it.")
        self.logger.info("[mds] %s: %d lines, worst gap %ss, no errors",
                         what, lines, gap)
        return gap


class PnfsMdsDefaultTopologyK8s(_MdsBase):
    """E-01: the default topology is an active and a standby, on two nodes.

    The shape every F- case assumes, asserted once so that a failure there
    is a failure of the takeover rather than of the deployment.

    Three separate claims, because they fail for different reasons: the
    replica count is the operator reading `standbys`; the node spread is
    the anti-affinity in the pod template; and exactly-one-active is the
    record, which is the only thing that distinguishes a cluster with a
    warm standby from one running two guests that both think they serve.
    """

    CLAIM = "pnfs-mds-e01"

    def run(self):
        self.logger.info("=== E-01 default MDS topology ===")
        self.assert_pnfs_cluster()
        sts = self.assert_mds_cluster()

        spec = sts.get("spec") or {}
        replicas = spec.get("replicas") or 0
        standbys, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get simplyblockdriver {self.DRIVER} -n {self.mds_ns} "
            f"-o jsonpath='{{.spec.pnfs.mds.standbys}}'", supress_logs=True)
        standbys = (standbys or "").strip().strip("'") or "1"
        want = int(standbys) + 1
        if replicas != want:
            raise PnfsEvidenceError(
                f"[E-01] spec.pnfs.mds.standbys is {standbys}, so the "
                f"StatefulSet should run 1+{standbys}={want} replicas; it "
                f"requests {replicas}.")

        pods = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
        running = [p for p in pods if p[2] == "Running"]
        if len(running) != want:
            raise PnfsEvidenceError(
                f"[E-01] {len(running)} of {want} MDS guest(s) are Running: "
                f"{pods}. A standby that is not running is not a standby.")

        nodes = [n for _, n, _, _ in running]
        if len(set(nodes)) != len(nodes):
            raise PnfsEvidenceError(
                f"[E-01] MDS guests share a node: {list(zip([p[0] for p in running], nodes))}. "
                f"Losing that node then loses the active and its standby "
                f"together, which is the one thing the standby exists to "
                f"prevent.")

        # Exactly one active, and it is one of the pods that exists.
        active, epoch = mds_record(sts)
        names = [p[0] for p in running]
        if not active:
            raise PnfsEvidenceError(
                f"[E-01] the StatefulSet carries no {ANNO_MDS_EPOCH} / "
                f"active-instance record, so no guest has been promoted and "
                f"no export can be served. Pods present: {names}")
        if active not in names:
            raise PnfsEvidenceError(
                f"[E-01] the record names {active!r} as active, but the pods "
                f"that exist are {names}. A record pointing at a pod that is "
                f"gone is how a takeover gets skipped.")
        if epoch < 1:
            raise PnfsEvidenceError(
                f"[E-01] the record's epoch is {epoch}; the design writes 1 "
                f"at the first promotion, so a served export with epoch 0 "
                f"means the fencing key was never taken.")

        # And the topology actually serves: a claim, mounted everywhere.
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods_ = self._start_shell_pods("pnfs-mds-e01", 2)
        self._assert_mount_writable(pods_)
        self.wait_exports_ready(1, what="[E-01] the claim's export")

        self.logger.info(
            "[E-01] PASS: %d guests on %d nodes, active=%s epoch=%s, export "
            "served", len(running), len(set(nodes)), active, epoch)


class PnfsMdsActiveDeletedK8s(_MdsBase):
    """F-01: delete the active guest; the standby takes over, clients hold.

    The headline case. A client with an open descriptor must stall for the
    takeover and then carry on with the same descriptor -- NFSv4.1 reclaim
    works precisely because every guest boots with the StatefulSet's
    hostname, so the client believes it is talking to a restarted server
    rather than a new one (design §5.6).

    The two halves are asserted separately because they fail separately:
    the record advancing says the operator noticed and promoted; the
    writer's gap and errno say whether that promotion was survivable. A
    promotion that completes while every client takes EBADF is the
    SFAM-2860 shape, and it would pass a case that only checked the epoch.
    """

    CLAIM = "pnfs-mds-f01"
    #: What a client may lose. Deliberately above the 45s grace period and
    #: well under the 93s a single-MDS replacement cost us, so this fails
    #: on the regression we have already seen rather than only on a total
    #: outage.
    MAX_CLIENT_GAP = 75

    def run(self):
        self.logger.info("=== F-01 active MDS deleted ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-f01", 2)
        self._assert_mount_writable(pods)

        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        active, epoch = self.record()
        self.logger.info("[F-01] before: active=%s epoch=%s", active, epoch)
        if not active:
            raise PnfsProbeError(
                "[F-01] no active guest is recorded, so there is nothing to "
                "delete and no takeover to observe.")

        killed_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        self.logger.info("[F-01] deleted the active guest %s", active)

        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[F-01] promoted to %s at epoch %s after %ss",
                         new_active, new_epoch, took)

        if new_active == active:
            # Not automatically wrong -- the deleted pod can be rescheduled
            # and promoted again -- but it means no standby took over, so
            # the standby was not what restored service.
            self.logger.warning(
                "[F-01] the promoted guest %s is the one that was deleted. "
                "Service came back by rescheduling, not by failover, which "
                "is standbys=0 behaviour on a cluster that has a standby.",
                new_active)

        self.wait_exports_ready(1, what="[F-01] the export after takeover")

        # And it was announced. An operator watching the cluster learns of a
        # takeover from the event, not from an annotation they would have to
        # know to read; design §7 emits MDSPromoted on the driver and on the
        # exports, carrying the cause.
        reasons = self.event_reasons()
        if EVENT_MDS_PROMOTED not in reasons:
            raise PnfsEvidenceError(
                f"[F-01] the epoch advanced {epoch} -> {new_epoch}, so a "
                f"takeover happened, but no {EVENT_MDS_PROMOTED} event says "
                f"so. Reasons present: {sorted(set(reasons)) or '(none)'}. "
                f"A failover nobody is told about is one nobody "
                f"investigates.")

        gap = self.assert_client_survived(
            pods[0], marker, "[F-01]", self.MAX_CLIENT_GAP)

        # The second client, which never wrote, must also still be able to.
        self._assert_mount_writable([pods[1]])

        self.logger.info(
            "[F-01] PASS: epoch %s -> %s in %ss, client stalled %ss and kept "
            "its descriptor, export Ready, elapsed %.1fs",
            epoch, new_epoch, took, gap, time.time() - killed_at)


class PnfsMdsLockAcrossTakeoverK8s(_MdsBase):
    """F-12: a byte-range lock is reclaimed across a takeover.

    The lock is the part a client cannot paper over. A write that stalls
    and resumes is invisible to an application; a lock silently lost is
    two writers in one byte range believing they are alone, which is the
    double-write the whole fencing design exists to prevent.

    The holder takes the lock before the takeover and still holds it
    after. The contender proves it is a real lock by failing to take it
    while the holder has it -- without that, a case that "reclaimed" a
    lock nobody could have taken anyway would pass on a server that lost
    every lock it had.
    """

    CLAIM = "pnfs-mds-f12"
    LOCKFILE = "lockme.dat"

    def run(self):
        self.logger.info("=== F-12 lock reclaim across a takeover ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        holder, contender = self._start_shell_pods("pnfs-mds-f12", 2)[:2]
        self._assert_mount_writable([holder, contender])

        path = f"{self.MOUNT}/{self.LOCKFILE}"
        # A lock held by a background process, so it outlives the exec.
        self.k8s_utils.exec_in_pod(
            holder,
            "cat <<'__PY__' > /tmp/hold.py\n"
            "import fcntl, time\n"
            f"f = open({path!r}, 'a+b')\n"
            "fcntl.lockf(f, fcntl.LOCK_EX, 100, 0, 0)\n"
            "open('/tmp/held', 'w').write('yes')\n"
            "time.sleep(3600)\n"
            "__PY__\n"
            "rm -f /tmp/held; nohup python3 /tmp/hold.py >/tmp/hold.out 2>&1 &"
            " sleep 5; cat /tmp/held 2>/dev/null || echo NOTHELD",
            timeout=120)
        out, _ = self.k8s_utils.exec_in_pod(
            holder, "cat /tmp/held 2>/dev/null || echo NOTHELD")
        if "yes" not in (out or ""):
            raise PnfsProbeError(
                f"[F-12] {holder} never acquired the lock on {path}, so "
                f"there is nothing to reclaim: {out!r}")

        def contend():
            return self._py(
                contender,
                "import fcntl\n"
                f"f = open({path!r}, 'a+b')\n"
                "try:\n"
                "    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 0, 0)\n"
                "    print('TOOK')\n"
                "except OSError as e:\n"
                "    print('BLOCKED', e.errno)\n")

        before = contend()
        if "BLOCKED" not in before:
            raise PnfsProbeError(
                f"[F-12] the contender took the lock while the holder had "
                f"it ({before!r}). The lock is not being enforced at all, so "
                f"the reclaim assertion below would be meaningless.")
        self.logger.info("[F-12] lock enforced before the takeover: %s", before)

        active, epoch = self.record()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[F-12] promoted %s -> %s (epoch %s) in %ss",
                         active, new_active, new_epoch, took)
        self.wait_exports_ready(1, what="[F-12] the export after takeover")

        # Grace has to be over before a contender is allowed to win or lose
        # meaningfully: during grace the server refuses new locks from
        # everyone, so a BLOCKED here would say nothing.
        sleep_n_sec(MDS_TERMINATION_GRACE + 45)

        after = contend()
        if "TOOK" in after:
            raise PnfsEvidenceError(
                f"[F-12] after the takeover the contender took a lock the "
                f"holder still believes it owns ({after!r}). The lock was "
                f"not reclaimed, and two clients now think they have "
                f"exclusive access to the same byte range.")
        held, _ = self.k8s_utils.exec_in_pod(
            holder, "pgrep -f hold.py >/dev/null && echo ALIVE || echo GONE")
        if "ALIVE" not in (held or ""):
            raise PnfsEvidenceError(
                f"[F-12] the lock holder's process died across the takeover "
                f"({held!r}), so the lock was released by its death rather "
                f"than reclaimed. A takeover must not kill a client.")

        self.logger.info(
            "[F-12] PASS: lock survived the takeover (epoch %s -> %s); "
            "contender still blocked: %s", epoch, new_epoch, after)


class PnfsMdsOperatorRestartNoTakeoverK8s(_MdsBase):
    """F-06: the operator is restarted while both guests are healthy.

    A takeover is expensive -- every client of every export on the cluster
    stalls for the grace period -- so it must be caused by losing the
    active, not by the thing that decides takeovers being restarted. The
    operator is the only writer of the record (design §4), which is exactly
    why a restart is the interesting moment: a controller that rebuilds its
    view from scratch and concludes "nobody is active" would promote.

    Asserted by the epoch holding still. A new epoch here is a stall the
    cluster did not need to take.
    """

    CLAIM = "pnfs-mds-f06"
    OPERATOR_SELECTOR = "app=simplyblock-operator"

    def run(self):
        self.logger.info("=== F-06 operator restart causes no takeover ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-f06", 2)
        self._assert_mount_writable(pods)
        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        active, epoch = self.record()
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.mds_ns} -l {self.OPERATOR_SELECTOR} "
            f"-o custom-columns=NAME:.metadata.name --no-headers "
            f"2>/dev/null || true", supress_logs=True)
        op_pods = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        if not op_pods:
            raise PnfsProbeError(
                f"[F-06] no pod matches {self.OPERATOR_SELECTOR} in "
                f"{self.mds_ns}; the case cannot restart what it cannot "
                f"name. (TC-S3-015 passed for a year on a kill that never "
                f"happened for exactly this reason.)")

        for p in op_pods:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod {p} -n {self.mds_ns} --wait=false")
        self.logger.info("[F-06] deleted operator pod(s) %s; active was %s "
                         "at epoch %s", op_pods, active, epoch)

        self.assert_no_promotion(epoch, self.QUIET_SECONDS,
                                 "[F-06] operator restarted")

        after_active, after_epoch = self.record()
        if after_active != active:
            raise PnfsEvidenceError(
                f"[F-06] the active instance changed from {active!r} to "
                f"{after_active!r} with the epoch unchanged at {after_epoch}. "
                f"The record is supposed to move only through a promotion.")

        self.assert_client_survived(pods[0], marker, "[F-06]", 30)
        self.wait_exports_ready(1, what="[F-06] the export")
        self.logger.info(
            "[F-06] PASS: operator restarted, active stayed %s at epoch %s, "
            "clients undisturbed", active, epoch)


class PnfsMdsNoStandbyRecoversK8s(_MdsBase):
    """E-03: at `standbys: 0`, one guest, and losing it recovers in place.

    The documented behaviour of `standbys: 0` is not "no recovery": the
    StatefulSet still replaces the pod, and the exports come back when it
    does. What is absent is the *warm* part -- the recovery takes a
    reschedule and a boot rather than a promotion, so it is slower, and the
    design says so outright.

    So this case asserts recovery happens, and records how long it took,
    rather than asserting a bound. A bound here would be an invented
    requirement: the design makes no promise about it.

    It leaves `standbys` where it found it. Every F- case in this file
    silently becomes this one if it does not.
    """

    CLAIM = "pnfs-mds-e03"

    def run(self):
        self.logger.info("=== E-03 standbys=0 recovers in place ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster(want_standbys=False)

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-e03", 2)
        self._assert_mount_writable(pods)

        self.set_standbys(0)
        self.wait_replicas(1)
        # The scale-down itself must not break the exports: the design
        # holds a scale-down that would need a switch-over first, with an
        # MDSScaleHeld event, rather than dropping the active.
        self.wait_exports_ready(1, what="[E-03] export after scale to 1")

        marker = self.start_writer(pods[0])
        sleep_n_sec(20)
        active, epoch = self.record()

        lost_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        self.logger.info("[E-03] deleted the only guest %s", active)

        deadline = time.time() + 600
        back = None
        while time.time() < deadline:
            rows = [p for p in mds_pods(self.k8s_utils, self.sts_name,
                                        self.mds_ns)
                    if p[2] == "Running" and p[3]]
            if rows:
                back = rows[0]
                break
            sleep_n_sec(10)
        if not back:
            raise PnfsEvidenceError(
                "[E-03] no MDS guest came back Ready within 600s of deleting "
                "the only one. At standbys=0 the StatefulSet is the whole "
                "recovery mechanism, and it did not run.")
        recovered = round(time.time() - lost_at, 1)

        self.wait_exports_ready(1, timeout=420,
                                what="[E-03] export after the guest returned")
        lines, gap, errors = self.writer_report(pods[0], marker)
        self.logger.info(
            "[E-03] PASS: guest %s back Ready after %ss, export served "
            "again; the client saw a %ss gap and %d error(s) -- recorded, "
            "not bounded, because standbys=0 promises no window",
            back[0], recovered, gap, len(errors))
        if errors:
            self.logger.warning(
                "[E-03] client errors across an unprotected restart: %s",
                errors[:5])


class PnfsMdsStandbyScaleK8s(_MdsBase):
    """E-04: `standbys` 1 -> 0 -> 1 with exports Ready throughout.

    Scaling the standby count is a routine operation and must not cost the
    cluster its exports. The interesting direction is down: removing a
    standby means removing its host from every volume's allowed hosts
    (design §5.8), and the design holds a scale-down that would otherwise
    have to drop the active, with an `MDSScaleHeld` event.

    Exports are sampled across both transitions rather than checked at the
    end, because a scale that breaks service and then restores it is still
    a scale that broke service.
    """

    CLAIM = "pnfs-mds-e04"

    def run(self):
        self.logger.info("=== E-04 standby scale down and up ===")
        self.assert_pnfs_cluster()
        sts = self.assert_mds_cluster()
        start_replicas = (sts.get("spec") or {}).get("replicas") or 2

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-e04", 2)
        self._assert_mount_writable(pods)
        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        active, epoch = self.record()
        samples = []

        def sample(tag):
            ready, total = self.exports_ready()
            a, e = self.record()
            samples.append({"at": tag, "exports": f"{ready}/{total}",
                            "active": a, "epoch": e})
            self.logger.info("[E-04] %s: exports=%s/%s active=%s epoch=%s",
                             tag, ready, total, a, e)
            if total and ready < total:
                raise PnfsEvidenceError(
                    f"[E-04] at {tag} only {ready} of {total} export(s) are "
                    f"Ready. Changing the standby count is routine and must "
                    f"not interrupt service.")

        sample("before")
        self.set_standbys(0)
        self.wait_replicas(1)
        sample("scaled to 1 guest")

        self.set_standbys(1)
        self.wait_replicas(start_replicas)
        deadline = time.time() + 420
        while time.time() < deadline:
            running = [p for p in mds_pods(self.k8s_utils, self.sts_name,
                                           self.mds_ns)
                       if p[2] == "Running" and p[3]]
            if len(running) >= start_replicas:
                break
            sleep_n_sec(10)
        sample("scaled back up")

        # The active must not have moved: nothing here lost the active, and
        # a scale that promotes is a scale that stalled every client.
        final_active, final_epoch = self.record()
        if final_epoch != epoch:
            raise PnfsEvidenceError(
                f"[E-04] the epoch moved {epoch} -> {final_epoch} across a "
                f"standby scale. Adding and removing a standby does not lose "
                f"the active, so nothing should have been promoted; every "
                f"client paid a grace period for it. Samples: {samples}")

        self.assert_client_survived(pods[0], marker, "[E-04]", 30)
        self.logger.info("[E-04] PASS: %s, active held at %s epoch %s",
                         samples, final_active, final_epoch)


class PnfsMdsManyExportsTakeoverK8s(_MdsBase):
    """F-10: a takeover with many exports brings every one of them back.

    One export recovering proves the promotion path works once. The risk
    this case covers is different: `Promote` connects and fences the state
    volume and then every export namespace in turn, so the cost and the
    failure modes scale with the count, and a single failure part-way
    leaves the rest unserved.

    The plan says 32. That is sized for a lab with more nodes than the k3s
    baremetal group has, and 32 RWX claims there is a twenty-minute
    provision before the case starts, so the count is an input with a
    smaller default. The assertion -- every export that existed before is
    Ready after -- does not depend on the number.
    """

    #: Override with PNFS_MDS_F10_EXPORTS on a lab that can take the plan's
    #: 32. The default is what fits the k3s baremetal group comfortably.
    import os as _os
    EXPORTS = int(_os.getenv("PNFS_MDS_F10_EXPORTS", "8"))
    del _os

    def run(self):
        self.logger.info("=== F-10 takeover with %d exports ===", self.EXPORTS)
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()

        self._make_storage_class()
        names = [f"pnfs-mds-f10-{i}" for i in range(self.EXPORTS)]
        self._make_rwx_claims(names)
        self.claim = names[0]

        self.wait_exports_ready(self.EXPORTS, timeout=600,
                                what="[F-10] exports before the takeover")
        before_ready, before_total = self.exports_ready()

        pods = self._start_shell_pods("pnfs-mds-f10", 1, claim=names[0])
        self._assert_mount_writable(pods)
        marker = self.start_writer(pods[0])
        sleep_n_sec(15)

        active, epoch = self.record()
        started = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)

        self.wait_exports_ready(
            before_total, timeout=600,
            what=f"[F-10] all {before_total} exports after the takeover")
        elapsed = round(time.time() - started, 1)

        self.assert_client_survived(pods[0], marker, "[F-10]", 120)
        self.logger.info(
            "[F-10] PASS: %d/%d exports back after a takeover to %s "
            "(epoch %s -> %s, promoted in %ss, all served by %ss)",
            before_total, before_total, new_active, epoch, new_epoch,
            took, elapsed)


class PnfsMdsStandbyMissingK8s(_MdsBase):
    """E-02: with nowhere to put a standby, say so and keep serving.

    A standby needs its own kvm-capable node. On a cluster that has only
    one, the right behaviour is not to fail and not to pretend: the active
    serves, the standby stays Pending, and `MDSStandbyMissing` says why.
    The failure this guards against is the quiet one -- a cluster that
    reports itself healthy while it has no standby at all, so the first
    takeover that matters never happens.

    Rather than needing a one-node lab, the case creates the condition by
    asking for more standbys than there are nodes to hold them. That is the
    same shape from the operator's point of view, and it is reversible.
    """

    def run(self):
        self.logger.info("=== E-02 no node for a standby ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster(want_standbys=False)

        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get nodes -l {self.KVM_LABEL} "
            f"-o custom-columns=NAME:.metadata.name --no-headers "
            f"--ignore-not-found", supress_logs=True)
        kvm = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        if not kvm:
            raise PnfsProbeError(
                f"[E-02] no node carries {self.KVM_LABEL}; the gate should "
                f"have caught this.")

        # One more guest than there are nodes to put one on.
        want_standbys = len(kvm)
        if want_standbys > 4:
            raise PnfsProbeError(
                f"[E-02] this lab has {len(kvm)} kvm-capable nodes, and the "
                f"CRD caps standbys at 4, so the condition cannot be created "
                f"by over-asking. Run this case on a smaller lab.")

        before_ready, before_total = self.exports_ready()
        active, epoch = self.record()
        self.set_standbys(want_standbys)

        deadline = time.time() + 300
        pending = []
        while time.time() < deadline:
            rows = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
            pending = [p for p in rows if p[2] == "Pending"]
            if pending:
                break
            sleep_n_sec(10)
        if not pending:
            rows = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
            raise PnfsEvidenceError(
                f"[E-02] asked for {want_standbys} standbys on {len(kvm)} "
                f"kvm-capable node(s), so one guest has nowhere to run, but "
                f"no pod is Pending: {rows}. Either two guests landed on one "
                f"node -- which defeats the standby -- or the replica count "
                f"was silently capped.")

        # The active keeps serving while a standby cannot be placed.
        if before_total:
            self.wait_exports_ready(
                before_total, timeout=180,
                what="[E-02] exports while a standby is unplaceable")
        self.assert_no_promotion(
            epoch, 60, "[E-02] a standby that cannot be placed")

        reasons = self.event_reasons()
        if EVENT_MDS_STANDBY_MISSING not in reasons:
            raise PnfsEvidenceError(
                f"[E-02] a standby is stuck Pending ({[p[0] for p in pending]}) "
                f"and the active is serving alone, but no "
                f"{EVENT_MDS_STANDBY_MISSING} event says so. The reasons "
                f"present are {sorted(set(reasons)) or '(none)'}. A cluster "
                f"silently without a standby looks healthy until the "
                f"takeover that needed one.")

        self.logger.info(
            "[E-02] PASS: %d pending guest(s), active %s still serving at "
            "epoch %s, %s raised",
            len(pending), active, epoch, EVENT_MDS_STANDBY_MISSING)



class PnfsMdsSecondTakeoverK8s(_MdsBase):
    """F-09: a second takeover before the first one's grace has ended.

    Takeovers do not queue politely in a real outage. The second promotion
    arrives while clients are still reclaiming from the first, and the
    risks are an epoch that does not advance -- which means the new guest
    is using a key an older one may still hold -- and a reclaim window that
    never completes, leaving locks lost without anything saying so.

    Needs two standbys to be meaningful: with one, the second takeover has
    nowhere to go and the case degrades into F-01 run twice.
    """

    CLAIM = "pnfs-mds-f09"

    def run(self):
        self.logger.info("=== F-09 second takeover inside grace ===")
        self.assert_pnfs_cluster()
        sts = self.assert_mds_cluster()
        replicas = (sts.get("spec") or {}).get("replicas") or 0
        if replicas < 3:
            raise PnfsProbeError(
                f"[F-09] this needs two standbys (3 guests); the cluster "
                f"runs {replicas}. With one standby the second takeover has "
                f"nowhere to go, and the case would quietly become F-01 "
                f"twice.")

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-f09", 2)
        self._assert_mount_writable(pods)
        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        first_active, epoch0 = self.record()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {first_active} -n {self.mds_ns} --wait=false")
        second_active, epoch1, took1 = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch0,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[F-09] first takeover: %s -> %s, epoch %s -> %s "
                         "in %ss", first_active, second_active, epoch0,
                         epoch1, took1)

        # Inside the grace period of the promotion that just happened.
        sleep_n_sec(max(5, MDS_TERMINATION_GRACE // 3))
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {second_active} -n {self.mds_ns} "
            f"--wait=false")
        third_active, epoch2, took2 = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch1,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[F-09] second takeover: %s -> %s, epoch %s -> %s "
                         "in %ss", second_active, third_active, epoch1,
                         epoch2, took2)

        if not epoch2 > epoch1 > epoch0:
            raise PnfsEvidenceError(
                f"[F-09] the epochs did not advance strictly across two "
                f"takeovers: {epoch0} -> {epoch1} -> {epoch2}. Each "
                f"promotion takes a reservation key derived from the epoch, "
                f"so a repeated epoch is a key an earlier guest may still "
                f"hold -- which is the double-writer the fencing exists to "
                f"stop.")

        self.wait_exports_ready(
            1, timeout=420, what="[F-09] the export after two takeovers")
        self.assert_client_survived(pods[0], marker, "[F-09]", 150)
        self.logger.info(
            "[F-09] PASS: %s -> %s -> %s, epochs %s -> %s -> %s",
            first_active, second_active, third_active, epoch0, epoch1, epoch2)


class PnfsMdsExportGrownBeforeTakeoverK8s(_MdsBase):
    """F-19: an export grown just before a takeover is served at its new size.

    The promotion re-assembles every export from the volume as it is now,
    not as the record remembered it. A resize immediately before a takeover
    is the window where those two disagree, and a client that looks the
    device up under the old size sees a filesystem smaller than the one it
    was told about -- or, worse, writes past what the new active mapped.

    The claim is grown, the growth is confirmed on a client, and only then
    is the active lost.
    """

    CLAIM = "pnfs-mds-f19"
    GROW_TO = "30Gi"

    def run(self):
        self.logger.info("=== F-19 resize immediately before a takeover ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mds-f19", 2)
        self._assert_mount_writable(pods)

        def size_on(pod):
            out, _ = self.k8s_utils.exec_in_pod(
                pod, f"df -B1 --output=size {self.MOUNT} | tail -1")
            try:
                return int((out or "0").strip())
            except ValueError:
                return 0

        before = size_on(pods[0])
        self.k8s_utils._exec_kubectl(
            f"kubectl patch pvc {self.CLAIM} -n {self.mds_ns} --type=merge "
            f"-p '{{\"spec\":{{\"resources\":{{\"requests\":"
            f"{{\"storage\":\"{self.GROW_TO}\"}}}}}}}}'")

        deadline = time.time() + 420
        grown = before
        while time.time() < deadline:
            grown = size_on(pods[0])
            if grown > before:
                break
            sleep_n_sec(10)
        if grown <= before:
            raise PnfsProbeError(
                f"[F-19] the claim did not grow on the client within 420s "
                f"({before} -> {grown} bytes), so there is no resize for the "
                f"takeover to race. Online resize is B-09's case; this one "
                f"needs it already working.")
        self.logger.info("[F-19] grew %d -> %d bytes on the client",
                         before, grown)

        marker = self.start_writer(pods[0])
        active, epoch = self.record()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.wait_exports_ready(1, what="[F-19] the export after takeover")

        after = size_on(pods[0])
        if after < grown:
            raise PnfsEvidenceError(
                f"[F-19] the mount reports {after} bytes after the takeover, "
                f"down from {grown} before it. The new active assembled the "
                f"export at the pre-resize size, so the client has been told "
                f"its filesystem shrank.")
        self._assert_mount_writable([pods[0], pods[1]])
        self.assert_client_survived(pods[0], marker, "[F-19]", 120)
        self.logger.info(
            "[F-19] PASS: %d bytes before, %d after the takeover to %s "
            "(epoch %s -> %s in %ss)",
            grown, after, new_active, epoch, new_epoch, took)


class PnfsMdsRestartWithClientDownK8s(_MdsBase):
    """F-16: a takeover while one client node is down, so grace runs long.

    The boundary the nfsd patches rest on. nfsd lifts its grace period
    early once every client it knows about has reclaimed; a client that is
    *gone* can never reclaim, so grace has to run to its full length
    instead. Two things can go wrong there and only one of them is the
    product's:

      * the clients that are still up do not recover, because the server
        spent the whole grace period waiting for one that never would.
        That is the failure this case exists to catch, and it is asserted
        hard.
      * grace ends early anyway. That is *recorded*, not failed. nfsd
        lifting grace once every **reachable** client has reclaimed is a
        defensible implementation, and inventing a minimum here would
        turn an observation into an opinion -- the same reason the S3
        latency cases carry no threshold. The log line names the lease so
        a reviewer can see which happened.

    The node is rebooted undrained, so its pod goes down with it and the
    server genuinely has a client it cannot reach. A drained reboot would
    unmount cleanly first, the server would have no unreclaimed client,
    and the case would silently become F-01.
    """

    CLAIM = "pnfs-mds-f16"
    #: The surviving clients' budget. Deliberately above a full grace
    #: period: with an unreachable client in the record, a stall for the
    #: whole lease is the expected cost, and anything beyond it is the
    #: server never having come back.
    MAX_SURVIVOR_GAP = 150
    REBOOT_TIMEOUT = 1200

    def run(self):
        self.logger.info("=== F-16 takeover with one client node down ===")
        self.assert_pnfs_cluster()
        self.assert_mds_cluster()
        if self.k8s_utils.detect_talos():
            raise PnfsProbeError(
                "[F-16] this needs a client node actually down. Talos has no "
                "host shell and this runner has no talosctl guarantee; a case "
                "that skipped the reboot would assert on a takeover with "
                "every client present, which is F-01.")

        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)

        pods = self._start_shell_pods("pnfs-mds-f16", 3)
        victim, survivor, bystander = pods[0], pods[1], pods[2]
        node_name = self._pod_node(victim)
        if not node_name:
            raise PnfsProbeError(
                f"[F-16] pod {victim} reports no node, so there is no client "
                f"host to take down.")
        node_ip = self.k8s_utils.node_internal_ip(node_name)

        # The lease, from a client rather than assumed, because it is what
        # the grace period is and the whole case is about its length.
        lease = NFS_LEASE_DEFAULT
        try:
            st = mountstats(self.k8s_utils, survivor, mountpoint=self.MOUNT)
            lease = int((st or {}).get("lease_time") or NFS_LEASE_DEFAULT)
        except PnfsProbeError:
            pass
        self.logger.info("[F-16] the clients report a %ss lease", lease)

        marker = self.start_writer(survivor)
        sleep_n_sec(20)

        self.logger.info("[F-16] taking client node %s (%s) down, undrained",
                         node_name, node_ip)
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)
        if not self.k8s_utils.reboot_node(node_ip, drain=False):
            raise PnfsProbeError(
                f"[F-16] no reboot could be issued for {node_name}, so no "
                f"client went away and the grace period would not be the one "
                f"this case is about.")
        if not self.k8s_utils.wait_node_condition(node_ip, ready=False,
                                                  timeout=300):
            raise PnfsProbeError(
                f"[F-16] {node_name} never left Ready, so the server still "
                f"has every client it started with.")
        self.logger.info("[F-16] %s is NotReady; one client is unreachable",
                         node_name)

        # Now the takeover, with that client still gone.
        active, epoch = self.record()
        if not active:
            raise PnfsProbeError(
                "[F-16] no active guest is recorded, so there is nothing to "
                "restart.")
        killed_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {active} -n {self.mds_ns} --wait=false")
        self.logger.info("[F-16] deleted the active guest %s", active)

        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.wait_exports_ready(
            1, timeout=self.TAKEOVER_TIMEOUT,
            what="[F-16] the export after a takeover with a client down")
        served_after = round(time.time() - killed_at, 1)

        # The observation, not an assertion. Which way it went is the
        # interesting part of this case and both ways are defensible.
        if served_after < lease:
            self.logger.info(
                "[F-16] grace was lifted EARLY: exports served %ss after the "
                "guest was deleted, inside the %ss lease, with a client that "
                "could not possibly have reclaimed. nfsd lifting grace once "
                "every reachable client has reclaimed is defensible, so this "
                "is recorded rather than failed -- but it means an "
                "unreachable client's locks were released before its lease "
                "expired, and the next case to care is F-12.",
                served_after, lease)
        else:
            self.logger.info(
                "[F-16] grace ran its full length: %ss to serve, against a "
                "%ss lease, as the plan expects with an unreclaimed client.",
                served_after, lease)

        # The assertion: the clients that stayed up recovered anyway.
        gap = self.assert_client_survived(
            survivor, marker, "[F-16] the surviving client",
            self.MAX_SURVIVOR_GAP)
        self._assert_mount_writable([survivor, bystander])

        # And the node comes back, because the lab is shared.
        back = self.k8s_utils.wait_node_condition(
            node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
        self.k8s_utils.uncordon_all_workers()
        if not back:
            raise PnfsEvidenceError(
                f"[F-16] {node_name} did not return Ready within "
                f"{self.REBOOT_TIMEOUT}s. The takeover assertions above "
                f"passed; the lab is now short a worker.")
        if victim in self.shell_pods:
            self.shell_pods.remove(victim)
        self.k8s_utils.delete_pod(victim, wait=True)
        rejoined = self._start_shell_pods(
            "pnfs-mds-f16-rejoin", 1, spread=False, node=node_name)[0]
        self.logger.info(
            "[F-16] PASS: epoch %s -> %s (%s) with %s down; exports served "
            "%ss against a %ss lease; survivor stalled %ss and kept its "
            "descriptor; %s remounted after the node rejoined",
            epoch, new_epoch, new_active, node_name, served_after, lease, gap,
            rejoined)


class _MdsNodeOutageBase(_MdsBase):
    """Outages of the node the active metadata server is running on.

    Every F- case above this takes the metadata server away by deleting
    its pod, which is a graceful stop: the runner gets SIGTERM, drains
    nfsd, powers the guest down over QMP and the operator promotes a
    standby with the cluster's full cooperation. None of that happens
    when the *node* goes.

    That difference is the whole point of these two cases. A node outage
    produces the one state the fencing design exists for -- a guest that
    the cluster believes is gone, running on a host the cluster cannot
    talk to, which may come back still believing it is active. If the
    promotion does not fence the state volume against it, two guests
    serve one filesystem and the clients cannot tell.

    Needs a host shell or a reboot verb, so Talos is a probe failure
    rather than a skip.
    """

    #: A node outage is not a pod delete. The promotion has to wait for
    #: the cluster to notice the node is gone -- node-monitor-grace-period
    #: plus the pod eviction timeout -- before it can even start, and that
    #: is minutes on a default cluster, not the 45s grace.
    TAKEOVER_TIMEOUT = 900
    REBOOT_TIMEOUT = 1200
    #: What a client may lose across it. Above the 93s a single-MDS
    #: replacement already cost us, because this outage is strictly worse
    #: than that one and a budget below it would fail on arithmetic rather
    #: than on a regression.
    MAX_CLIENT_GAP = 300

    def gate(self, tag):
        self.assert_pnfs_cluster()
        sts = self.assert_mds_cluster()
        if self.k8s_utils.detect_talos():
            raise PnfsProbeError(
                f"{tag} this lab is Talos: a node outage needs talosctl on "
                f"the runner for a reboot and a host shell for a drain, and "
                f"neither is guaranteed. A case that skipped the outage "
                f"would assert on a pod delete, which is F-01.")
        return sts

    def active_node(self, tag):
        """``(pod, node, epoch)`` for the active guest and its host."""
        active, epoch = self.record()
        if not active:
            raise PnfsProbeError(
                f"{tag} no active guest is recorded, so there is no node to "
                f"take an outage on.")
        node = None
        for name, host, _phase, _ready in mds_pods(
                self.k8s_utils, self.sts_name, self.mds_ns):
            if name == active:
                node = host
        if not node or node == "<none>":
            raise PnfsProbeError(
                f"{tag} the active guest {active} reports no node.")
        return active, node, epoch

    def assert_one_active(self, tag, want_epoch):
        """Exactly one active guest, at the epoch the promotion reached.

        The split-brain assertion. Read after the lost node is back,
        because that is when the old guest could reappear still believing
        it serves: the record is the only thing that arbitrates, every
        guest answers on the same StatefulSet hostname, and a standby's
        readiness probe reports SERVING too -- so a client cannot tell
        two actives apart and neither can a pod listing.
        """
        active, epoch = self.record()
        if epoch != want_epoch:
            raise PnfsEvidenceError(
                f"{tag} the record moved to epoch {epoch} after the lost "
                f"node rejoined; the promotion had reached {want_epoch}. "
                f"Either something promoted again for no reason, or the "
                f"returning guest asserted itself over the one that took "
                f"over from it.")
        reasons = self.event_reasons()
        if EVENT_MDS_PROMOTION_CONFLICT in reasons:
            raise PnfsEvidenceError(
                f"{tag} the operator published "
                f"{EVENT_MDS_PROMOTION_CONFLICT} across this outage. Two "
                f"guests contended for the active role, which is the state "
                f"the epoch-keyed reservation is supposed to make "
                f"impossible.")
        if EVENT_MDS_FENCE_FAILED in reasons:
            raise PnfsEvidenceError(
                f"{tag} {EVENT_MDS_FENCE_FAILED} was published. The promoted "
                f"guest could not fence the state volume against its "
                f"predecessor, so nothing is stopping the old guest writing "
                f"to it when its node comes back.")
        self.logger.info("%s one active (%s) at epoch %s, no conflict and no "
                         "fence failure", tag, active, epoch)
        return active, epoch


class PnfsMdsNodeRebootK8s(_MdsNodeOutageBase):
    """The active metadata server's node reboots, undrained.

    The unplanned outage. The guest is destroyed with its host and gets
    no chance to drain nfsd or release anything, which is the case the
    graceful pod delete can never produce:

      * the cluster has to *notice*. Nothing tells it the node is gone;
        it waits out node-monitor-grace-period and then the pod eviction
        timeout, which is why this case allows 900s for a takeover that
        F-01 does in 300.
      * the predecessor is not dead, it is unreachable. For the whole
        window between the reboot and the kubelet reporting in again,
        the old guest's state is unknown to the cluster -- and it is
        exactly then that a standby is promoted onto the same state
        volume and the same exports.
      * and then it comes back. A guest that rejoins still holding its
        old epoch's reservation, or still believing it is active, is a
        second writer on one filesystem. ``assert_one_active`` is read
        *after* the node is Ready again for that reason; checking before
        it rejoins would pass on a cluster with no fencing at all.
    """

    CLAIM = "pnfs-mds-nodereboot"

    def run(self):
        self.logger.info("=== MDS node reboot (unplanned) ===")
        self.gate("[mds-node-reboot]")
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mdsnr", 2)
        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        active, node, epoch = self.active_node("[mds-node-reboot]")
        node_ip = self.k8s_utils.node_internal_ip(node)
        self.logger.info(
            "[mds-node-reboot] the active guest %s runs on %s (%s) at epoch "
            "%s; rebooting that node undrained", active, node, node_ip, epoch)

        # Before the reboot: a cordon outlives the run that set it, and
        # the operator then refuses the worker outright on the next
        # bring-up, complaining about stripe width.
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)

        killed_at = time.time()
        if not self.k8s_utils.reboot_node(node_ip, drain=False):
            raise PnfsProbeError(
                f"[mds-node-reboot] no reboot could be issued for {node}, so "
                f"the metadata server never went away.")
        if not self.k8s_utils.wait_node_condition(node_ip, ready=False,
                                                  timeout=300):
            raise PnfsProbeError(
                f"[mds-node-reboot] {node} never left Ready, so the active "
                f"guest is still running and there is no takeover to judge.")
        self.logger.info("[mds-node-reboot] %s is NotReady after %.1fs",
                         node, time.time() - killed_at)

        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info(
            "[mds-node-reboot] promoted to %s at epoch %s, %ss after the "
            "record was read (%.1fs after the reboot was issued)",
            new_active, new_epoch, took, time.time() - killed_at)
        if new_active == active:
            raise PnfsEvidenceError(
                f"[mds-node-reboot] the epoch advanced to {new_epoch} but "
                f"the active guest is still {active}, which is on the node "
                f"that was rebooted. A promotion back onto the host that "
                f"just died is not a failover.")

        self.wait_exports_ready(
            1, timeout=self.TAKEOVER_TIMEOUT,
            what="[mds-node-reboot] the export after the node outage")
        reasons = self.event_reasons()
        if EVENT_MDS_PROMOTED not in reasons:
            raise PnfsEvidenceError(
                f"[mds-node-reboot] a takeover happened ({epoch} -> "
                f"{new_epoch}) and no {EVENT_MDS_PROMOTED} event says so. "
                f"Reasons present: {sorted(set(reasons)) or '(none)'}.")

        gap = self.assert_client_survived(
            pods[0], marker, "[mds-node-reboot]", self.MAX_CLIENT_GAP)
        self._assert_mount_writable([pods[1]])

        # Now the hard part: the predecessor returns.
        back = self.k8s_utils.wait_node_condition(
            node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
        self.k8s_utils.uncordon_all_workers()
        if not back:
            raise PnfsEvidenceError(
                f"[mds-node-reboot] {node} did not return Ready within "
                f"{self.REBOOT_TIMEOUT}s. The takeover assertions passed, "
                f"but the fencing half of this case cannot be judged and "
                f"the lab is short a worker.")
        self.logger.info("[mds-node-reboot] %s is Ready again; the old guest "
                         "can now reappear", node)
        # Long enough for the returning guest to be scheduled, started and
        # to have tried whatever it is going to try.
        sleep_n_sec(self.QUIET_SECONDS)
        self.assert_one_active("[mds-node-reboot]", new_epoch)
        self.wait_exports_ready(
            1, timeout=300,
            what="[mds-node-reboot] the export after the node rejoined")
        self._assert_mount_writable(pods)

        self.logger.info(
            "[mds-node-reboot] PASS: %s died with its node, %s took over at "
            "epoch %s in %ss, clients stalled %ss, and the returning guest "
            "did not take the role back", active, new_active, new_epoch,
            took, gap)


class PnfsMdsNodeDrainedK8s(_MdsNodeOutageBase):
    """The active metadata server's node is drained — planned maintenance.

    The same loss by the opposite route, and the interesting one because
    it is the route an administrator actually takes. A drain is supposed
    to be the *safe* way to move work off a node, so two things are worth
    asserting that the reboot case cannot:

      * **the drain completes.** A drain that hangs is the known shape
        here: replacing the metadata server has already been measured at
        93s of cluster-wide stall, 23s of it a guest power-down that can
        never succeed, and the drain reaches the guest at all only
        because it is not among the operator's eviction targets. A drain
        that never returns blocks the node's maintenance window and the
        administrator has no way to tell whether waiting longer would
        help.
      * **the client cost of the planned path is not worse than the
        unplanned one.** If a graceful drain stalls clients longer than
        pulling the power does, the graceful path is the one to stop
        recommending.

    Both are recorded with their timings whichever way they go; only the
    takeover itself and the clients keeping their descriptors are gated.
    `drain_node` is deliberately never fatal in the harness, so its
    return value is a finding here rather than an exception.
    """

    CLAIM = "pnfs-mds-nodedrain"
    DRAIN_MINUTES = 10

    def run(self):
        self.logger.info("=== MDS node drained (planned) ===")
        self.gate("[mds-node-drain]")
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)
        self.extra_claims.append(self.CLAIM)
        pods = self._start_shell_pods("pnfs-mdsnd", 2)
        marker = self.start_writer(pods[0])
        sleep_n_sec(20)

        active, node, epoch = self.active_node("[mds-node-drain]")
        node_ip = self.k8s_utils.node_internal_ip(node)
        self.logger.info(
            "[mds-node-drain] draining %s (%s), which hosts the active guest "
            "%s at epoch %s", node, node_ip, active, epoch)

        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)
        self.k8s_utils.cordon_node(node_ip, cordon=True)

        started = time.time()
        clean = self.k8s_utils.drain_node(node_ip,
                                         timeout_min=self.DRAIN_MINUTES)
        drain_s = round(time.time() - started, 1)
        self.logger.info("[mds-node-drain] drain returned %s after %ss",
                         "cleanly" if clean else "NOT cleanly", drain_s)

        new_active, new_epoch, took = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.wait_exports_ready(
            1, timeout=self.TAKEOVER_TIMEOUT,
            what="[mds-node-drain] the export after the drain")
        served_s = round(time.time() - started, 1)

        gap = self.assert_client_survived(
            pods[0], marker, "[mds-node-drain]", self.MAX_CLIENT_GAP)
        self._assert_mount_writable([pods[1]])

        # The guest must not have been left on the drained node. A drain
        # that reported success while the pod it could not evict is still
        # there has drained nothing that matters.
        still = [p for p, host, _ph, _r in mds_pods(
            self.k8s_utils, self.sts_name, self.mds_ns) if host == node]
        if still:
            self.logger.warning(
                "[mds-node-drain] MDS pod(s) %s are still on the drained "
                "node %s. The guest is not among the operator's eviction "
                "targets, so a drain that reports success can leave it in "
                "place -- and the administrator's maintenance window then "
                "takes down a running metadata server.", still, node)

        self.k8s_utils.uncordon_all_workers()
        self.logger.info("[mds-node-drain] %s uncordoned", node)
        self.wait_exports_ready(
            1, timeout=300, what="[mds-node-drain] the export after uncordon")

        if not clean:
            raise PnfsEvidenceError(
                f"[mds-node-drain] the drain of {node} did not complete "
                f"cleanly in {self.DRAIN_MINUTES}m ({drain_s}s elapsed). "
                f"Draining a node is the documented way to take it out of "
                f"service; a drain that hangs on the metadata server gives "
                f"an administrator no way to tell whether waiting longer "
                f"would help, and the takeover itself worked -- epoch "
                f"{epoch} -> {new_epoch} on {new_active}, clients stalled "
                f"{gap}s. The drain is the defect, not the failover.")

        self.logger.info(
            "[mds-node-drain] PASS: drain of %s completed in %ss, %s took "
            "over at epoch %s (%ss to serve from the start of the drain), "
            "clients stalled %ss and kept their descriptors",
            node, drain_s, new_active, new_epoch, served_s, gap)
