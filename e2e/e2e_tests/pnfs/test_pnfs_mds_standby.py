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
from exceptions.custom_exception import SkippedTestsException
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    ANNO_MDS_EPOCH,
    EVENT_MDS_FENCE_FAILED,
    EVENT_MDS_PROMOTED,
    EVENT_MDS_PROMOTION_CONFLICT,
    EVENT_MDS_STANDBY_MISSING,
    MDS_CRD,
    MDS_STANDBYS_FIELD,
    MDS_TERMINATION_GRACE,
    NFS_LEASE_DEFAULT,
    PnfsEvidenceError,
    PnfsProbeError,
    mds_active_pod,
    mds_pods,
    mds_record,
    mds_standby_supported,
    mds_statefulset,
    mountstats,
    nfsexport_status,
    wait_mds_promotion,
    writer_errno_note,
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

    #: Name of the throwaway claim used to summon a metadata server. Not a
    #: case's own claim: it exists for seconds and is deleted again.
    BOOTSTRAP_CLAIM = "pnfs-mds-bootstrap"

    #: How long the operator gets to build the StatefulSet once an export
    #: has asked for one. It took 8s on run 45906141173; this is slack.
    MDS_BOOTSTRAP_TIMEOUT = 300

    def setup(self):
        super().setup()
        self.sts_name = None
        self.mds_ns = self.k8s_utils.namespace

    # ── the gate ─────────────────────────────────────────────────────────

    def _ensure_mds_exists(self):
        """Make a metadata server exist, because asking is what creates one.

        The operator builds the MDS StatefulSet *lazily*, for the first
        NFSExport that needs one -- not when ``spec.pnfs`` is set, and not
        at cluster bring-up. The S3 run shows the order plainly::

            23:05:09  NFSExport nfsexp-4dbbc02d...  WaitingForActiveMDS:
                      "StatefulSet simplyblock-pnfs-mds-... does not exist yet"
            23:05:17  link: peer linked  mds/...-1
            23:05:22  MDSPromoted  ...-1 is the active at epoch 1, cause First

        Every case in this file calls its gate as the first statement of
        ``run()``, before the claim that would summon the server. So on a
        cluster where no pNFS volume had ever been made, all thirteen
        refused -- run 45945639150, on a cluster whose pNFS gate had just
        passed with 7 kvm-capable nodes and the mds-proxy 7/7 Ready::

            [pnfs] cluster gate passed: NFSExport CRD present, 7
                   kvm-capable node(s), mds-proxy Ready (7/7)
            PnfsProbeError: no pnfs-mds StatefulSet in simplyblock

        There were no NFSExports in that run at all. The gate was not
        wrong about what it saw; it was asking before anything had caused
        the thing it asks about.

        The claim is deleted once the StatefulSet is up. The MDS outlives
        its exports -- the S3 run kept one across eighteen tests that each
        deleted their buckets -- and leaving an export behind would hand
        every case one it did not create, which the cases that count
        exports would read as the product's.
        """
        if mds_statefulset(self.k8s_utils, self.mds_ns):
            return
        self.logger.info(
            "[mds] no metadata server on this cluster yet; creating claim "
            "%s to summon one, since the operator builds the StatefulSet "
            "for the first export that needs it", self.BOOTSTRAP_CLAIM)
        self._make_storage_class()
        self._make_rwx_claim(self.BOOTSTRAP_CLAIM)
        deadline = time.time() + self.MDS_BOOTSTRAP_TIMEOUT
        sts = {}
        while time.time() < deadline:
            sts = mds_statefulset(self.k8s_utils, self.mds_ns)
            if sts:
                break
            sleep_n_sec(5)
        try:
            self.k8s_utils.delete_pvc(self.BOOTSTRAP_CLAIM)
        except Exception as exc:
            self.logger.warning("[mds] could not delete %s: %s",
                                self.BOOTSTRAP_CLAIM, exc)
        if not sts:
            raise PnfsProbeError(
                f"a bound RWX pNFS claim ({self.BOOTSTRAP_CLAIM}) existed "
                f"for {self.MDS_BOOTSTRAP_TIMEOUT}s and the operator still "
                f"built no pnfs-mds StatefulSet in {self.mds_ns}. An export "
                f"that never gets a metadata server is a product failure, "
                f"not a missing prerequisite.")
        self.logger.info(
            "[mds] metadata server appeared: %s",
            (sts.get("metadata") or {}).get("name"))

    def assert_cluster_sound(self, tag="[mds]"):
        """Refuse to judge a takeover on a cluster that arrived broken.

        These cases take a node out and then ask what the client saw. That
        question has an answer only if the storage underneath was healthy
        to begin with -- otherwise whatever the client suffered gets
        attributed to the takeover this case performed, which is the wrong
        defect filed against the wrong team.

        Run 45960047836 is why. The reboot case took out vm204, which hosts
        both the metadata server and a storage node; its three surviving
        peers all raised ``node_unrecoverable_io_error`` and the cluster
        went ``degraded``. The drain case then ran on that same cluster
        thirteen minutes later, with those alerts still firing, and
        reported its client's stall as a finding about draining a metadata
        server's node.

        Checked at the gate of every case rather than in the suite, so it
        holds whichever subset is named on the command line.
        """
        try:
            details = self.sbcli_utils.get_cluster_details(self.cluster_id)
        except Exception as exc:
            self.logger.warning(
                "%s could not read the cluster before the takeover (%s), so "
                "this case cannot say whether it started sound", tag, exc)
            return
        if isinstance(details, list):
            details = details[0] if details else {}
        status = (details or {}).get("status")
        try:
            nodes = (self.sbcli_utils.get_storage_nodes()
                     or {}).get("results") or []
        except Exception as exc:
            self.logger.warning("%s could not list storage nodes (%s)",
                                tag, exc)
            nodes = []
        unwell = [
            (n.get("hostname") or n.get("mgmt_ip") or n.get("uuid"),
             n.get("status"))
            for n in nodes if (n.get("status") or "") != "online"]
        self.logger.info(
            "%s cluster before the takeover: status=%s, %d storage node(s), "
            "%d not online", tag, status, len(nodes), len(unwell))
        if status == "active" and not unwell:
            return
        raise PnfsProbeError(
            f"{tag} this cluster was already unhealthy before the case did "
            f"anything: cluster status={status!r}"
            + (f", storage node(s) not online: {unwell}" if unwell else "")
            + ". A takeover measured on top of that says nothing about the "
            "takeover -- whatever the client suffers here was already owed "
            "to the previous failure. Run the case on a sound cluster, or "
            "give the previous one time to recover.")

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
        self.assert_cluster_sound()
        self._ensure_mds_exists()
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
            # Two very different reasons to be here, and reporting them as
            # one is how an unbuilt feature arrives as eleven product
            # defects. The runner puts a PnfsProbeError in `errors` (it is
            # an AssertionError), so every takeover case would come back
            # red against an operator that never claimed to fail over.
            supported = mds_standby_supported(self.k8s_utils)
            if supported is False:
                raise SkippedTestsException(
                    f"this operator implements no warm standby: the "
                    f"{MDS_CRD} CRD has no spec.pnfs.mds.{MDS_STANDBYS_FIELD} "
                    f"field, and the MDS StatefulSet is one replica "
                    f"('One, and never more' in mds.go). A takeover cannot "
                    f"happen and is not supposed to, so this case is NOT "
                    f"APPLICABLE here rather than failing. The single-MDS "
                    f"recovery path is covered by PnfsMdsNodeRebootK8s and "
                    f"PnfsMdsNodeDrainedK8s, which do not need a standby. "
                    f"When the standby lands, the CRD gains the field and "
                    f"this case starts applying again with no change here.")
            if supported is None:
                raise PnfsProbeError(
                    f"StatefulSet {self.sts_name} runs {replicas} "
                    f"replica(s) and the {MDS_CRD} CRD could not be read, so "
                    f"whether this operator even implements a standby is "
                    f"unknown. That is not a verdict on the product.")
            raise PnfsProbeError(
                f"StatefulSet {self.sts_name} runs {replicas} replica(s), so "
                f"there is no standby to promote -- but this operator does "
                f"support one. Set spec.pnfs.mds.{MDS_STANDBYS_FIELD} >= 1 on "
                f"driver {self.DRIVER}, and note each standby needs its own "
                f"kvm-capable node. This is the deployment's configuration, "
                f"not a defect.")

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

    def active_pod(self, active, tag):
        """The pod running the record's active instance.

        Every case that acts on "the active" needs a pod name, and the
        record does not hold one -- it holds
        ``<instance-uuid>/containerd://<container-id>``. Going through a
        lookup is what keeps ``kubectl delete pod`` pointed at a pod that
        exists; see ``mds_active_pod`` for why the record is right to be
        shaped that way.
        """
        name, _node = mds_active_pod(self.k8s_utils, active,
                                     self.sts_name, self.mds_ns)
        if not name:
            pods = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
            raise PnfsProbeError(
                f"{tag} the record's active instance {active!r} resolves to "
                f"none of the metadata server pods {[p[0] for p in pods]} by "
                f"container id, pod uid or name. A record naming a guest "
                f"that no pod is running means no client can be served and "
                f"no takeover can be aimed at anything.")
        return name

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
        """``(lines, max_gap, errors, stalled, alive)`` for a writer above.

        The gap is what a takeover costs a client that keeps its fd. The
        errors are what it costs one that does not -- EBADF under an open
        descriptor, which no application retries, the fingerprint of
        SFAM-2860.

        ``stalled`` is the part that used to be missing. The gap was
        measured only *between* lines the writer managed to record, so a
        client blocked from its last line until the report was taken
        contributed **nothing** -- a write that hangs in the kernel does
        not raise, so there is no errno either, and the file simply stops
        growing. Run 45960047836 is the case: the writer in pnfs-mdsnd-0
        logged 2 lines of an expected ~74 at one a second, then blocked
        for the whole takeover, and the case reported "only 2 line(s), so
        there is nothing to judge the takeover by" -- a measurement it in
        fact had. Worse, the same blindness passes a client that stalls
        forever as long as it recorded five lines first.

        So the trailing interval is measured too, against the clock in the
        pod rather than the runner's, and ``alive`` separates "blocked in
        a write" from "the writer exited".
        """
        out, _ = self.k8s_utils.exec_in_pod(
            pod,
            f"cut -d' ' -f1 {marker} 2>/dev/null | tail -4000; "
            f"echo '---ERR---'; cat /tmp/writer.err 2>/dev/null || true; "
            f"echo '---NOW---'; date +%s; "
            f"echo '---ALIVE---'; pgrep -f writer.py | wc -l",
            timeout=120)
        body, _, rest = (out or "").partition("---ERR---")
        errs, _, rest = rest.partition("---NOW---")
        now_s, _, alive_s = rest.partition("---ALIVE---")
        stamps = []
        for tok in body.split():
            try:
                stamps.append(float(tok))
            except ValueError:
                continue
        gap = 0.0
        for a, b in itertools.pairwise(stamps):
            gap = max(gap, b - a)
        try:
            now = float(now_s.split()[0])
        except (IndexError, ValueError):
            now = 0.0
        # A clock we could not read must not quietly become "no stall".
        stalled = round(now - stamps[-1], 1) if (now and stamps) else None
        try:
            alive = int(alive_s.split()[0]) > 0
        except (IndexError, ValueError):
            alive = None
        errors = [ln for ln in errs.splitlines() if ln.strip()]
        return len(stamps), round(gap, 1), errors, stalled, alive

    def assert_client_survived(self, pod, marker, what, max_gap):
        lines, gap, errors, stalled, alive = self.writer_report(pod, marker)
        if errors:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} took {len(errors)} I/O "
                f"error(s) across the takeover: {errors[:5]}. "
                f"{writer_errno_note(errors)}. A takeover is supposed to "
                f"stall a client, not break it.")

        # A stall that has not ended is a gap -- a floor on one, in fact,
        # since nothing says when it would have finished. Folding it in
        # here is what makes two lines and seventy seconds of silence a
        # measurement rather than "too few lines to judge".
        open_stall = stalled is not None and stalled > 5
        worst = round(max(gap, stalled or 0.0), 1)

        if lines == 0 or (stalled is None and lines < 5):
            raise PnfsProbeError(
                f"{what}: the writer in {pod} recorded {lines} line(s) and "
                f"its clock could not be read, so nothing bounds what the "
                f"client saw and there is nothing to judge the takeover by. "
                f"The writer is {'alive' if alive else 'gone'}.")
        if worst > max_gap:
            raise PnfsEvidenceError(
                f"{what}: the writer in {pod} went {worst}s without a write "
                f"across the takeover, over the {max_gap}s this case allows "
                f"({lines} line(s) at one a second, last one {stalled}s ago, "
                f"writer {'still blocked' if alive else 'gone'}). "
                + ("The stall had not ended when this was read, so that is "
                   "a floor on what it cost, not the whole of it. "
                   if open_stall else "")
                + "Every client of every export on this metadata server "
                "stalled with it.")
        self.logger.info(
            "[mds] %s: %d lines, worst %ss without a write (between lines "
            "%ss, since the last line %ss, writer %s), no errors",
            what, lines, worst, gap, stalled,
            "alive" if alive else "gone")
        return worst


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
        # Resolved, not compared: the record holds an instance id, so
        # ``active in names`` is false on every healthy cluster and this
        # check would have reported the product broken for our own
        # misreading of a correct record.
        active_name = self.active_pod(active, "[E-01]")
        if active_name not in names:
            raise PnfsEvidenceError(
                f"[E-01] the record's active instance {active!r} runs in pod "
                f"{active_name}, which is not among the running pods "
                f"{names}. A record pointing at a pod that is gone is how a "
                f"takeover gets skipped.")
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

        victim = self.active_pod(active, "[F-01]")
        killed_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {victim} -n {self.mds_ns} --wait=false")
        self.logger.info("[F-01] deleted the active guest %s (instance %s)",
                         victim, active)

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
            f"kubectl delete pod {self.active_pod(active, '[F-12]')} "
            f"-n {self.mds_ns} --wait=false")
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

        victim = self.active_pod(active, "[E-03]")
        lost_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {victim} -n {self.mds_ns} --wait=false")
        self.logger.info("[E-03] deleted the only guest %s (instance %s)",
                         victim, active)

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
        lines, gap, errors, stalled, _alive = self.writer_report(
            pods[0], marker)
        self.logger.info(
            "[E-03] PASS: guest %s back Ready after %ss, export served "
            "again; the client saw a %ss gap, %ss since its last line and "
            "%d error(s) in %d line(s) -- recorded, not bounded, because "
            "standbys=0 promises no window",
            back[0], recovered, gap, stalled, len(errors), lines)
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
            f"kubectl delete pod {self.active_pod(active, '[F-10]')} "
            f"-n {self.mds_ns} --wait=false")
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
            f"kubectl delete pod {self.active_pod(first_active, '[F-09]')} "
            f"-n {self.mds_ns} --wait=false")
        second_active, epoch1, took1 = wait_mds_promotion(
            self.k8s_utils, self.sts_name, epoch0,
            timeout=self.TAKEOVER_TIMEOUT, namespace=self.mds_ns)
        self.logger.info("[F-09] first takeover: %s -> %s, epoch %s -> %s "
                         "in %ss", first_active, second_active, epoch0,
                         epoch1, took1)

        # Inside the grace period of the promotion that just happened.
        sleep_n_sec(max(5, MDS_TERMINATION_GRACE // 3))
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {self.active_pod(second_active, '[F-09]')} "
            f"-n {self.mds_ns} --wait=false")
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
            f"kubectl delete pod {self.active_pod(active, '[F-19]')} "
            f"-n {self.mds_ns} --wait=false")
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
        guest = self.active_pod(active, "[F-16]")
        killed_at = time.time()
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {guest} -n {self.mds_ns} --wait=false")
        self.logger.info("[F-16] deleted the active guest %s (instance %s)",
                         guest, active)

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
    """Outages of the node the metadata server is running on.

    Every other case in this file removes the metadata server by deleting
    its pod, which is a graceful stop: the runner gets SIGTERM, drains
    nfsd, powers the guest down over QMP, and the replacement starts with
    the cluster's full cooperation. None of that happens when the *node*
    goes, and that difference is the point of these two cases.

    **These two do not require a warm standby, and the rest of this file
    does.** On operator `main` there is none to require:

        operator/internal/controllers/driver/mds.go:370
            // One, and never more: two guests mounting one filesystem
            // lose data, and a StatefulSet does not start a second pod
            // while the first is terminating.
            Replicas: ptr.To(int32(1)),

    There is no `standbys` field on `DriverPNFSMDS`, and `standbys`,
    `mds-epoch`, `mds-active` and `MDSPromoted` do not appear anywhere in
    the operator repository. The warm standby is a design document and a
    test plan, not shipped code -- which is why every `F-` case above
    correctly refuses to run through `assert_mds_cluster()`.

    So recovery here is the single-MDS path, which is the one that
    actually runs on every cluster we have and is where every pNFS defect
    we have filed lives: the StatefulSet's one pod has to come back --
    rescheduled onto another kvm-capable node if there is one, otherwise
    on the same node once it returns -- reattach the state volume and
    every export's namespace, and serve again.

    The epoch assertions are kept and applied *conditionally*. When the
    standby feature lands, `mds_record` starts returning an epoch and
    these cases tighten themselves to assert a promotion and the absence
    of a split brain, without a second pair of classes to write. The log
    says which mode each run took, so a green run can never be mistaken
    for the stronger one.
    """

    #: A node outage is not a pod delete. Nothing tells the cluster the
    #: node is gone: it waits out node-monitor-grace-period and then the
    #: pod eviction timeout before the StatefulSet may even start a
    #: replacement, and with one replica it will not start one while the
    #: old pod is Terminating. Minutes, not the 45s shutdown grace.
    TAKEOVER_TIMEOUT = 900
    REBOOT_TIMEOUT = 1200
    #: What a client may lose. Above the 93s a single-MDS replacement has
    #: already been measured at, because this outage is strictly worse
    #: than that one -- a budget below it would fail on arithmetic rather
    #: than on a regression.
    MAX_CLIENT_GAP = 300

    def gate(self, tag):
        self.assert_pnfs_cluster()
        # want_standbys=False, for the reason in the class docstring. The
        # node outage is worth driving against a single MDS; it is the
        # topology every cluster we have actually runs.
        sts = self.assert_mds_cluster(want_standbys=False)
        if self.k8s_utils.detect_talos():
            raise PnfsProbeError(
                f"{tag} this lab is Talos: a node outage needs talosctl on "
                f"the runner for a reboot and a host shell for a drain, and "
                f"neither is guaranteed. A case that skipped the outage "
                f"would be asserting on a pod delete, which is F-01.")
        replicas = ((sts.get("spec") or {}).get("replicas")) or 0
        _active, epoch = mds_record(sts)
        self.standby_mode = replicas >= 2 and epoch is not None
        self.logger.info(
            "%s StatefulSet %s runs %s replica(s), record epoch=%r -> %s "
            "mode. %s", tag, self.sts_name, replicas, epoch,
            "standby" if self.standby_mode else "single-MDS",
            "The promotion and split-brain assertions apply."
            if self.standby_mode else
            "No warm standby exists on this cluster (operator main hard-codes "
            "one replica and publishes no epoch), so recovery is asserted as "
            "the one pod coming back and serving, not as a promotion.")
        self.kvm_nodes = self.kvm_capable()
        self.logger.info("%s kvm-capable node(s): %s", tag,
                         self.kvm_nodes or "(none labelled)")
        return sts

    def kvm_capable(self):
        """The nodes that can host the guest at all.

        Decides what recovery is even possible: with a second kvm-capable
        node the pod can be rescheduled while the first is down; with
        one, recovery cannot begin until that node returns, and a case
        that expected otherwise would report a product failure for a
        labelling decision.
        """
        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get nodes -l storage.simplyblock.io/kvm-capable=true "
            "-o custom-columns=NAME:.metadata.name --no-headers "
            "2>/dev/null || true", supress_logs=True)
        return sorted(n.strip() for n in (out or "").splitlines() if n.strip())

    def mds_host(self, tag):
        """``(pod, node, epoch)`` for the serving guest and its host.

        Falls back to the StatefulSet's only Ready pod when no record is
        published, which on operator main is always.
        """
        active, epoch = self.record()
        pods = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
        if not active:
            ready = [(n, h) for n, h, _p, r in pods if r]
            if not ready:
                raise PnfsProbeError(
                    f"{tag} no metadata server pod is Ready, so there is "
                    f"nothing serving and no outage to judge. Pods: {pods}")
            if len(ready) > 1:
                raise PnfsEvidenceError(
                    f"{tag} {len(ready)} metadata server pods are Ready "
                    f"before any outage: {ready}. The StatefulSet is "
                    f"declared one replica and never more, because two "
                    f"guests mounting one filesystem lose data.")
            pod, node = ready[0]
        else:
            # By lookup, because the record holds an instance id and not a
            # pod name. Comparing it to one found nothing, and the case
            # then blamed the product -- run 45952090637 failed both node
            # outage cases with "the serving guest
            # 20ed5865-.../containerd://b6bf... reports no node" on a
            # cluster whose two guests were both placed and running.
            pod, node = mds_active_pod(self.k8s_utils, active,
                                       self.sts_name, self.mds_ns)
            if not pod:
                raise PnfsProbeError(
                    f"{tag} the record's active instance {active!r} resolves "
                    f"to none of the metadata server pods "
                    f"{[p[0] for p in pods]}.")
        if not node or node == "<none>":
            raise PnfsProbeError(
                f"{tag} the serving guest {pod} (instance {active!r}) is on "
                f"no node, so there is no host to take out. Pods: {pods}")
        return pod, node, epoch

    def wait_mds_serving(self, tag, was_pod, was_epoch, timeout=None):
        """Wait until a metadata server is serving again. Returns seconds.

        In standby mode this is the promotion. In single-MDS mode it is
        the StatefulSet's pod being Ready again -- which is the honest
        assertion, because there is nothing else that could have taken
        over, and a case that waited for an epoch that no cluster
        publishes would simply time out and call the product broken.

        A **pod name** is returned in both modes, so that the callers'
        "is it still the guest that was there before" check compares two
        things of the same kind. Returning the record's instance id here
        and a pod name there is how that check becomes a no-op that never
        fires and never says it did not.
        """
        timeout = timeout or self.TAKEOVER_TIMEOUT
        start = time.time()
        if self.standby_mode:
            new_active, new_epoch, took = wait_mds_promotion(
                self.k8s_utils, self.sts_name, was_epoch, timeout=timeout,
                namespace=self.mds_ns)
            new_pod, new_node = None, None
            settle = time.time() + 60
            while time.time() < settle:
                new_pod, new_node = mds_active_pod(
                    self.k8s_utils, new_active, self.sts_name, self.mds_ns)
                if new_pod:
                    break
                sleep_n_sec(5)
            if not new_pod:
                self.logger.warning(
                    "%s promoted to instance %s at epoch %s after %ss, but "
                    "that instance matches no pod of %s, so this case cannot "
                    "check whether the guest that took over is the one that "
                    "was lost", tag, new_active, new_epoch, took,
                    self.sts_name)
                return new_active, new_epoch, took
            self.logger.info("%s promoted to %s on %s (instance %s) at epoch "
                             "%s after %ss", tag, new_pod, new_node,
                             new_active, new_epoch, took)
            return new_pod, new_epoch, took
        deadline = start + timeout
        seen = []
        while time.time() < deadline:
            seen = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
            ready = [(n, h) for n, h, _p, r in seen if r]
            if len(ready) == 1:
                name, host = ready[0]
                took = round(time.time() - start, 1)
                self.logger.info(
                    "%s a metadata server is serving again: %s on %s after "
                    "%ss (was %s)", tag, name, host, took, was_pod)
                return name, None, took
            if len(ready) > 1:
                raise PnfsEvidenceError(
                    f"{tag} {len(ready)} metadata server pods are Ready at "
                    f"once: {ready}. With one replica and no standby this "
                    f"is two guests on one filesystem, which the design "
                    f"forbids outright.")
            sleep_n_sec(10)
        raise PnfsEvidenceError(
            f"{tag} no metadata server pod became Ready within {timeout}s "
            f"after the outage. Pods: {seen}. With a single MDS and no "
            f"standby, every pNFS volume in the cluster is unserved for as "
            f"long as this lasts"
            + (f"; the only kvm-capable node(s) are {self.kvm_nodes}, so the "
               f"pod had nowhere else to go." if len(self.kvm_nodes) < 2
               else "."))

    def assert_no_split_brain(self, tag, want_epoch):
        """One guest serving, and nothing contended for the role.

        Read after the lost node rejoins, because that is when the old
        guest can reappear still believing it serves. Checking before it
        returns would pass on a cluster with no fencing at all -- which,
        on operator main, is the cluster we have.
        """
        pods = mds_pods(self.k8s_utils, self.sts_name, self.mds_ns)
        ready = [(n, h) for n, h, _p, r in pods if r]
        if len(ready) > 1:
            raise PnfsEvidenceError(
                f"{tag} {len(ready)} metadata server pods are Ready after "
                f"the lost node rejoined: {ready}. The returning guest came "
                f"back alongside the one that replaced it, so two guests "
                f"have the same filesystem mounted.")
        reasons = self.event_reasons()
        why = {
            EVENT_MDS_PROMOTION_CONFLICT: (
                "two guests contended for the active role, which the "
                "epoch-keyed reservation exists to make impossible"),
            EVENT_MDS_FENCE_FAILED: (
                "the serving guest could not fence the state volume against "
                "its predecessor, so nothing stops the old guest writing "
                "to it"),
        }
        for bad in (EVENT_MDS_PROMOTION_CONFLICT, EVENT_MDS_FENCE_FAILED):
            if bad in reasons:
                raise PnfsEvidenceError(
                    f"{tag} {bad} was published: {why[bad]}.")
        if self.standby_mode:
            _active, epoch = self.record()
            if epoch != want_epoch:
                raise PnfsEvidenceError(
                    f"{tag} the record moved to epoch {epoch} after the lost "
                    f"node rejoined; recovery had reached {want_epoch}. "
                    f"Either something promoted again for no reason, or the "
                    f"returning guest asserted itself over its replacement.")
        self.logger.info("%s one guest serving, no conflict and no fence "
                         "failure after the node rejoined", tag)
        return ready[0] if ready else (None, None)


class PnfsMdsNodeRebootK8s(_MdsNodeOutageBase):
    """The metadata server's node reboots, undrained — the unplanned outage.

    The guest is destroyed with its host and gets no chance to drain
    nfsd, release its reservations or power down, which is the case a pod
    delete can never produce. Three things follow that F-01 does not
    exercise:

      * the cluster has to *notice*. Nothing announces the node;
        node-monitor-grace-period and the pod eviction timeout have to
        expire, and with one replica the StatefulSet will not start a
        replacement while the old pod is Terminating. That is why this
        allows 900s where F-01 allows 300, and the elapsed time is the
        headline of the run: it is every pNFS volume in the cluster
        unserved.
      * the predecessor is not dead, it is unreachable. Its state is
        unknown to the cluster for the whole window in which a
        replacement attaches the same state volume and the same export
        namespaces.
      * and then it comes back. ``assert_no_split_brain`` is read *after*
        the node is Ready again, for that reason.
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

        was_pod, node, epoch = self.mds_host("[mds-node-reboot]")
        node_ip = self.k8s_utils.node_internal_ip(node)
        if len(self.kvm_nodes) < 2:
            self.logger.warning(
                "[mds-node-reboot] %s is the only kvm-capable node, so the "
                "guest has nowhere to be rescheduled and cannot recover "
                "until this node is back. The outage this measures is "
                "therefore the node's whole reboot, which is the honest "
                "cost of a single-MDS cluster on a single KVM host.", node)

        self.logger.info(
            "[mds-node-reboot] the metadata server %s runs on %s (%s); "
            "rebooting that node undrained", was_pod, node, node_ip)
        # Registered before the reboot: reboot_node cordons on the way in
        # and only uncordons on its own recovery path, so a failure in
        # between leaves the worker unschedulable and the NEXT run's
        # bring-up fails talking about stripe width.
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)

        killed_at = time.time()
        if not self.k8s_utils.reboot_node(node_ip, drain=False):
            raise PnfsProbeError(
                f"[mds-node-reboot] no reboot could be issued for {node}, so "
                f"the metadata server never went away.")
        if not self.k8s_utils.wait_node_condition(node_ip, ready=False,
                                                  timeout=300):
            raise PnfsProbeError(
                f"[mds-node-reboot] {node} never left Ready, so the guest is "
                f"still running and there is no recovery to judge.")
        self.logger.info("[mds-node-reboot] %s is NotReady after %.1fs",
                         node, time.time() - killed_at)

        # With one kvm-capable node the pod cannot start until the host
        # is back, so the node's return is part of the recovery rather
        # than a step after it.
        if len(self.kvm_nodes) < 2:
            back = self.k8s_utils.wait_node_condition(
                node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
            self.k8s_utils.uncordon_all_workers()
            if not back:
                raise PnfsEvidenceError(
                    f"[mds-node-reboot] {node} did not return Ready within "
                    f"{self.REBOOT_TIMEOUT}s, and it is the only node that "
                    f"can host the guest. The lab is short a worker.")

        new_pod, new_epoch, took = self.wait_mds_serving(
            "[mds-node-reboot]", was_pod, epoch)
        self.wait_exports_ready(
            1, timeout=self.TAKEOVER_TIMEOUT,
            what="[mds-node-reboot] the export after the node outage")
        unserved = round(time.time() - killed_at, 1)

        if self.standby_mode and new_pod == was_pod:
            raise PnfsEvidenceError(
                f"[mds-node-reboot] the epoch advanced to {new_epoch} but "
                f"the serving guest is still {was_pod}, on the node that was "
                f"rebooted. A promotion back onto the host that just died is "
                f"not a failover.")
        if self.standby_mode and EVENT_MDS_PROMOTED not in self.event_reasons():
            raise PnfsEvidenceError(
                f"[mds-node-reboot] a promotion happened ({epoch} -> "
                f"{new_epoch}) and no {EVENT_MDS_PROMOTED} event says so.")

        gap = self.assert_client_survived(
            pods[0], marker, "[mds-node-reboot]", self.MAX_CLIENT_GAP)
        self._assert_mount_writable([pods[1]])

        # The predecessor returns. Already waited for above when it was
        # the only KVM host.
        if len(self.kvm_nodes) >= 2:
            back = self.k8s_utils.wait_node_condition(
                node_ip, ready=True, timeout=self.REBOOT_TIMEOUT)
            self.k8s_utils.uncordon_all_workers()
            if not back:
                raise PnfsEvidenceError(
                    f"[mds-node-reboot] {node} did not return Ready within "
                    f"{self.REBOOT_TIMEOUT}s. Recovery passed, but the "
                    f"split-brain half cannot be judged and the lab is short "
                    f"a worker.")
        self.logger.info("[mds-node-reboot] %s is Ready again; the old guest "
                         "can now reappear", node)
        sleep_n_sec(self.QUIET_SECONDS)
        self.assert_no_split_brain("[mds-node-reboot]", new_epoch)
        self.wait_exports_ready(
            1, timeout=300,
            what="[mds-node-reboot] the export after the node rejoined")
        self._assert_mount_writable(pods)

        self.logger.info(
            "[mds-node-reboot] PASS (%s mode): %s died with %s, %s was "
            "serving %ss later, every pNFS volume was unserved for %ss, "
            "clients stalled %ss and kept their descriptors, and the "
            "returning guest did not come back alongside its replacement",
            "standby" if self.standby_mode else "single-MDS", was_pod, node,
            new_pod, took, unserved, gap)


class PnfsMdsNodeDrainedK8s(_MdsNodeOutageBase):
    """The metadata server's node is drained — planned maintenance.

    The same loss by the route an administrator actually takes, and the
    more interesting of the two because a drain is supposed to be the
    *safe* way to take a node out of service. Two things are worth
    asserting that the reboot case cannot:

      * **the drain completes.** This is the known sore spot: replacing
        the metadata server has already been measured at 93s of
        cluster-wide stall, 23s of it a guest power-down that cannot
        succeed, and a drain reaches the guest at all only because the
        operator's eviction targets omit it. A drain that never returns
        blocks the maintenance window with no way for the administrator
        to tell whether waiting longer would help, so it is gated as the
        defect even when the recovery itself worked.
      * **the planned path is not worse than the unplanned one.** If a
        graceful drain stalls clients longer than pulling the power
        does, the graceful path is the one to stop recommending. Both
        numbers are recorded for comparison against the reboot case.

    `drain_node` is never fatal in the harness by design, so its return
    value is a finding here rather than an exception.
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

        was_pod, node, epoch = self.mds_host("[mds-node-drain]")
        node_ip = self.k8s_utils.node_internal_ip(node)
        if len(self.kvm_nodes) < 2:
            # A cordon on the only KVM host means the guest cannot be
            # rescheduled anywhere for as long as the cordon stands. That
            # is a real finding about the topology, not a reason to skip:
            # it is what an administrator would hit.
            self.logger.warning(
                "[mds-node-drain] %s is the only kvm-capable node. Draining "
                "it cordons the only host the guest can run on, so the "
                "metadata server cannot come back until the node is "
                "uncordoned. The drain's own behaviour is still what this "
                "case is about.", node)

        self.logger.info(
            "[mds-node-drain] draining %s (%s), which hosts the metadata "
            "server %s", node, node_ip, was_pod)
        self.undo_stack.append(self.k8s_utils.uncordon_all_workers)
        self.k8s_utils.cordon_node(node_ip, cordon=True)

        started = time.time()
        clean = self.k8s_utils.drain_node(node_ip,
                                         timeout_min=self.DRAIN_MINUTES)
        drain_s = round(time.time() - started, 1)
        self.logger.info("[mds-node-drain] drain returned %s after %ss",
                         "cleanly" if clean else "NOT cleanly", drain_s)

        # The guest must not still be sitting on the drained node. It is
        # not among the operator's eviction targets, so a drain can report
        # success and leave it running -- and the administrator's
        # maintenance then takes down a live metadata server.
        left = [p for p, host, _ph, _r in mds_pods(
            self.k8s_utils, self.sts_name, self.mds_ns) if host == node]
        if left:
            self.logger.warning(
                "[mds-node-drain] MDS pod(s) %s are still on the drained "
                "node %s. The guest is not an eviction target, so a drain "
                "that reports success can leave it in place.", left, node)

        # Uncordon before waiting for recovery when this is the only host
        # that can run the guest: otherwise the wait is a wait on the
        # cordon, which the test placed.
        if len(self.kvm_nodes) < 2:
            self.k8s_utils.uncordon_all_workers()
            self.logger.info(
                "[mds-node-drain] %s uncordoned before the recovery wait: it "
                "is the only kvm-capable node, so leaving the cordon on "
                "would be timing the test's own cordon", node)

        new_pod, new_epoch, took = self.wait_mds_serving(
            "[mds-node-drain]", was_pod, epoch)
        self.wait_exports_ready(
            1, timeout=self.TAKEOVER_TIMEOUT,
            what="[mds-node-drain] the export after the drain")
        unserved = round(time.time() - started, 1)

        gap = self.assert_client_survived(
            pods[0], marker, "[mds-node-drain]", self.MAX_CLIENT_GAP)
        self._assert_mount_writable([pods[1]])

        self.k8s_utils.uncordon_all_workers()
        self.logger.info("[mds-node-drain] %s uncordoned", node)
        self.wait_exports_ready(
            1, timeout=300, what="[mds-node-drain] the export after uncordon")

        if not clean:
            raise PnfsEvidenceError(
                f"[mds-node-drain] the drain of {node} did not complete "
                f"cleanly within {self.DRAIN_MINUTES}m ({drain_s}s elapsed). "
                f"Draining is the documented way to take a node out of "
                f"service, and a drain that hangs on the metadata server "
                f"leaves an administrator no way to tell whether waiting "
                f"longer would help. The recovery itself worked -- {new_pod} "
                f"was serving {took}s later, clients stalled {gap}s -- so "
                f"the drain is the defect, not the failover.")

        self.logger.info(
            "[mds-node-drain] PASS (%s mode): drain of %s completed in %ss, "
            "%s was serving %ss later, pNFS unserved for %ss from the start "
            "of the drain, clients stalled %ss and kept their descriptors",
            "standby" if self.standby_mode else "single-MDS", node, drain_s,
            new_pod, took, unserved, gap)
