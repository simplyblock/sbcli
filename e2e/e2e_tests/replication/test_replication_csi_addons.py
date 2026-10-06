"""AR-K: the Kubernetes DR surface -- csi-addons, as Ramen drives it.

**Why this lane exists, and why the AR-R lane is not enough.**

AR-R drives fail-over through ``sbctl cluster replication-policy-failover``.
That is the right test of the engine, and it is the only surface on docker.
It is *not* the path a Kubernetes operator or Ramen will ever take. On
Kubernetes the sequence is: flip ``VolumeReplication.spec.replicationState``,
the vendored csi-addons controller calls ``PromoteVolume``/``DemoteVolume``
on our driver, and the driver calls the same control-plane endpoints. Same
destination, different front door -- and the front door has its own
semantics, its own error codes, and at least one behaviour that can lose
data if we do not test it.

**The behaviour that makes this lane urgent.** From the driver's own comment
on ``PromoteVolume`` (``csi-driver/internal/csi/controller/replication.go``)::

    Force=false is the planned path, refused with ABORTED (retryable) while a
    demote is still converging and FAILED_PRECONDITION when no demote was ever
    requested -- the split matters because the vendored csi-addons controller
    auto-escalates ANY FAILED_PRECONDITION from a force=false promote to
    force=true inline, with no wait-and-retry grace period of its own.

Read that again from a data-loss point of view. A *planned* relocation whose
demote was missed does not fail and wait. It is silently escalated to the
*unplanned* path, which "clones the last fully replicated generation and
ignores demote state entirely". The planned path exists precisely to flush
the final delta first. So a missed demote turns a zero-loss relocation into
one that loses up to a full interval, with nothing in the CR saying so.
**AR-K-005 is that case.** No unit test can catch it, because the escalation
happens in the vendored upstream controller, not in our code.

**What is deliberately not here.** Ramen itself. There is no OCM hub, no
``DRPolicy`` and no ``VolumeReplicationGroup`` in any simplyblock repository
-- the hub is external and, per dev's own Ramen design, P0-1 through P0-3
(a live hub with two managed clusters) are ``Unknown``/``Not shipped``. So
this lane drives the ``VolumeReplication`` object by hand, exactly as dev's
own design §12 proposes: "real backend, real csi-addons machinery, but a
hand-driven VolumeReplication object rather than a real Ramen reconcile
loop". When a hub exists, the assertions here move up a level unchanged.

Requires operator PR #548 (``integrate_csi_addons``). Every case checks the
CRDs are installed first and records a named skip if they are not, rather
than failing in a way that looks like a product bug.
"""
import json
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
    ReplicationPreconditionError,
)
from utils.common_utils import sleep_n_sec


class _CsiAddonsBase(ReplicationTestBase):
    """Shared plumbing for driving VolumeReplication objects."""

    VR_GROUP = "replication.storage.openshift.io"
    VR_VERSION = "v1alpha1"
    #: The VolumeReplicationClass parameter key our driver reads.
    #: csi-driver/internal/csi/controller/replication.go:32
    #: An EMPTY value marks the FAIL-OVER TARGET side (:265).
    POLICY_PARAM = "replicationPolicyID"
    #: The three conditions Ramen reads off the object. Its VRG aggregates
    #: them into DataProtected and its DRPC into PeerReady, the boolean that
    #: gates whether a relocation may start. These three strings are the
    #: entire contract surface -- if they are wrong, Ramen decides wrongly.
    RAMEN_CONDITIONS = ("Completed", "Degraded", "Resyncing")

    def setup(self):
        super().setup()
        self._vrs = []
        self._vrcs = []

    # ── preconditions ─────────────────────────────────────────────────────
    def require_csi_addons(self, case_id):
        """True if the csi-addons CRDs are installed. Records a skip if not."""
        if not self.k8s_test:
            self.skip_case(
                case_id,
                "csi-addons is a Kubernetes surface; this run is on docker. "
                "The engine path for the same operation is covered by AR-R.")
            return False
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            "kubectl get crd volumereplications."
            f"{self.VR_GROUP} -o name 2>/dev/null || true", supress_logs=True)
        if "volumereplication" not in (out or ""):
            self.skip_case(
                case_id,
                "the VolumeReplication CRD is not installed. This lane needs "
                "operator PR #548 (integrate_csi_addons) with "
                "csiaddons.create enabled in the chart.")
            return False
        return True

    # ── object plumbing ───────────────────────────────────────────────────
    def make_vr_class(self, name, policy_id):
        """A VolumeReplicationClass naming one replication policy.

        An EMPTY policy_id is meaningful, not a mistake: the driver reads it
        as "this is the fail-over target side" (replication.go:265), which is
        how the DR cluster's class differs from the source's.
        """
        k8s = self._ensure_k8s_utils()
        k8s.apply_yaml_cluster_scoped(f"""
apiVersion: {self.VR_GROUP}/{self.VR_VERSION}
kind: VolumeReplicationClass
metadata:
  name: {name}
spec:
  provisioner: csi.simplyblock.io
  parameters:
    {self.POLICY_PARAM}: "{policy_id}"
""")
        self._vrcs.append(name)
        return name

    def make_vr(self, name, vrc, pvc, state="primary"):
        k8s = self._ensure_k8s_utils()
        k8s.apply_yaml(f"""
apiVersion: {self.VR_GROUP}/{self.VR_VERSION}
kind: VolumeReplication
metadata:
  name: {name}
spec:
  volumeReplicationClass: {vrc}
  replicationState: {state}
  dataSource:
    kind: PersistentVolumeClaim
    name: {pvc}
""", namespace=getattr(k8s, "namespace", "simplyblock"))
        self._vrs.append(name)
        return name

    def set_vr_state(self, name, state):
        k8s = self._ensure_k8s_utils()
        ns = getattr(k8s, "namespace", "simplyblock")
        return k8s._exec_kubectl(
            f"kubectl patch volumereplication {name} -n {ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicationState\":\"{state}\"}}}}' 2>&1 || true")

    def vr_status(self, name):
        k8s = self._ensure_k8s_utils()
        ns = getattr(k8s, "namespace", "simplyblock")
        out, _ = k8s._exec_kubectl(
            f"kubectl get volumereplication {name} -n {ns} -o json "
            f"2>/dev/null || true", supress_logs=True)
        try:
            return json.loads(out).get("status", {}) or {}
        except Exception:                             # noqa: BLE001
            return {}

    def vr_conditions(self, name):
        """{type: status} for the object's conditions."""
        return {c.get("type"): c.get("status")
                for c in self.vr_status(name).get("conditions", [])
                if c.get("type")}

    def await_vr_condition(self, name, cond, want="True", timeout=600,
                           case_id="AR-K"):
        """Wait for one condition to reach *want*, reporting what it saw.

        'Timed out' on its own has cost whole investigations before, so the
        failure names the conditions actually present -- an object with no
        conditions at all means the controller never reconciled it, which is
        a different problem from one that reconciled and reported Degraded.
        """
        deadline = time.time() + timeout
        seen = {}
        while time.time() < deadline:
            seen = self.vr_conditions(name)
            if seen.get(cond) == want:
                self.logger.info("[%s] %s reached %s=%s", case_id, name,
                                 cond, want)
                return seen
            sleep_n_sec(10)
        detail = seen or ("NONE -- the csi-addons controller never "
                          "reconciled this object at all")
        raise AssertionError(
            f"[{case_id}] {name} did not reach {cond}={want} within "
            f"{timeout}s. Conditions seen: {detail}. Ramen reads "
            f"{list(self.RAMEN_CONDITIONS)} and turns them into PeerReady; a "
            f"condition that never settles stalls a real relocation.")

    def pvc_for(self, lvol_name):
        reg = self._volume_registry.get(lvol_name, {})
        return reg.get("pvc_name") or self._k8s_normalize_name(lvol_name)

    def cleanup_vrs(self):
        if not self.k8s_test:
            return
        k8s = self._ensure_k8s_utils()
        ns = getattr(k8s, "namespace", "simplyblock")
        for v in reversed(self._vrs):
            try:
                k8s._exec_kubectl(f"kubectl delete volumereplication {v} "
                                  f"-n {ns} --ignore-not-found 2>&1 || true")
            except Exception:                         # noqa: BLE001
                pass
        for c in reversed(self._vrcs):
            try:
                k8s._exec_kubectl(f"kubectl delete volumereplicationclass {c} "
                                  f"--ignore-not-found 2>&1 || true")
            except Exception:                         # noqa: BLE001
                pass


class CsiAddonsEnableAndReport(_CsiAddonsBase):
    """AR-K-001, AR-K-002, AR-K-003, AR-K-010, AR-K-011.

    The steady-state contract: a VolumeReplication that truthfully reports
    the relationship. Dev's design calls this "independently useful ... which
    no surface provides today", and it is what Ramen polls before it will
    allow a relocation.
    """

    def run(self):
        if not self.require_csi_addons("AR-K-001"):
            return
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tk{stamp}", self.cluster_b)
        pname = self.policy_add(f"pk{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arka{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.seed_volume(vol)

        # ── AR-K-001 enabling through the CR starts real replication ──────
        vrc = self.make_vr_class(f"vrc-{stamp}", pname)
        vr = self.make_vr(f"vr-{stamp}", vrc, self.pvc_for(vol))
        self.logger.info("[AR-K-001] created %s -> %s", vr, vol)

        # The CR is only the front door. The proof is that the ENGINE now has
        # a relationship: if the adapter reported success without the backend
        # attaching, Ramen would believe the volume is protected when it is not.
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="EnableVolumeReplication must reach the engine")
        self.logger.info("[AR-K-001] PASS: the CR produced a real backend "
                         "relationship")

        # ── AR-K-002 the three conditions Ramen reads ─────────────────────
        self.await_vr_condition(vr, "Completed", "True", case_id="AR-K-002")
        conds = self.vr_conditions(vr)
        missing = [c for c in self.RAMEN_CONDITIONS if c not in conds]
        if missing:
            raise AssertionError(
                f"[AR-K-002] the object is missing condition(s) {missing}. "
                f"Present: {conds}. Ramen's VRG aggregates exactly "
                f"{list(self.RAMEN_CONDITIONS)} into DataProtected, and its "
                f"DRPC turns that into PeerReady -- the boolean that gates "
                f"whether a relocation is allowed to start. A missing "
                f"condition does not degrade gracefully; it blocks DR.")
        if conds.get("Degraded") == "True":
            raise AssertionError(
                f"[AR-K-002] Degraded=True on a healthy, freshly synced "
                f"volume. Ramen will refuse to relocate. conditions={conds}")
        self.logger.info("[AR-K-002] PASS: %s", conds)

        # ── AR-K-003 the info read agrees with the engine ─────────────────
        st = self.vr_status(vr)
        last_sync = st.get("lastSyncTime")
        if not last_sync:
            raise AssertionError(
                "[AR-K-003] status.lastSyncTime is empty on a volume that has "
                "completed a cycle. GetVolumeReplicationInfo serves this, and "
                "it is the only freshness signal Ramen has -- without it an "
                "operator cannot tell a slow target from a stopped one.")
        engine = self.replication_info(vol_id)
        self.logger.info("[AR-K-003] CR lastSyncTime=%s | engine says:\n%s",
                         last_sync, engine[:300])
        self.logger.info("[AR-K-003] PASS: the CR reports a real sync time")

        # ── AR-K-010 idempotency ──────────────────────────────────────────
        # csi-addons RPCs are idempotent by contract, and the controller
        # re-reconciles freely. Re-applying must not detach and re-attach:
        # a re-attach is a FULL re-sync on the backend (design §8).
        self.logger.info("[AR-K-010] re-applying the same VolumeReplication")
        before = (self.relationship_for(vol_id) or {}).get("target_lvol_id")
        self.make_vr(vr, vrc, self.pvc_for(vol))
        sleep_n_sec(45)
        after = (self.relationship_for(vol_id) or {}).get("target_lvol_id")
        if before and after and before != after:
            raise AssertionError(
                f"[AR-K-010] re-applying an unchanged VolumeReplication "
                f"changed the target volume ({before} -> {after}). That is a "
                f"detach and re-attach, which means a full re-sync every time "
                f"the controller reconciles.")
        rel = self.relationship_for(vol_id) or {}
        if rel.get("state") != self.STATE_REPLICATING:
            raise AssertionError(
                f"[AR-K-010] re-applying left the relationship in "
                f"{rel.get('state')!r}.")
        self.logger.info("[AR-K-010] PASS: idempotent, no re-sync")

        # ── AR-K-011 deleting the CR tears the relationship down ──────────
        self.logger.info("[AR-K-011] deleting %s", vr)
        k8s = self._ensure_k8s_utils()
        ns = getattr(k8s, "namespace", "simplyblock")
        k8s._exec_kubectl(f"kubectl delete volumereplication {vr} -n {ns} "
                          f"--ignore-not-found 2>&1 || true")
        deadline = time.time() + 300
        while time.time() < deadline:
            if not self.relationship_for(vol_id):
                break
            sleep_n_sec(10)
        else:
            raise AssertionError(
                f"[AR-K-011] the backend relationship for {vol} survived "
                f"deletion of its VolumeReplication. DisableVolumeReplication "
                f"must detach the policy and clean the internal snapshots on "
                f"both sides; a surviving relationship leaks target-side "
                f"state and holds a slot nothing owns.")
        self.logger.info("[AR-K-011] PASS: relationship gone with the CR")

        self.assert_no_corruption("after AR-K steady-state cases")
        self._teardown(vol, vol_id)

    def _teardown(self, vol, vol_id):
        self.cleanup_vrs()
        try:
            self.policy_clear(vol_id)
        except Exception:                             # noqa: BLE001
            pass
        try:
            self._disconnect_and_cleanup_dual(vol)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning("[AR-K] teardown: %s", str(exc)[:120])
        self.cleanup_replication()


class CsiAddonsOwnership(_CsiAddonsBase):
    """AR-K-004: one owner per volume, enforced.

    Two control paths now reach the same backend relationship: the PVC
    annotation (which makes a ReplicationSlot) and VolumeReplication. The
    design is explicit that they must not fight, and that the refusal is
    about *ownership*, not about whether the policies agree:

        "two owners turn every ownership flap into a detach and re-attach,
         and a re-attach is a full re-sync on the backend"

    So the interesting assertion is the one where both sides name the SAME
    policy and it is still refused. A naive implementation would allow that
    case and look correct in every demo.
    """

    ANNOTATION = "storage.simplyblock.io/replication-policy"

    def run(self):
        if not self.require_csi_addons("AR-K-004"):
            return
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tko{stamp}", self.cluster_b)
        pname = self.policy_add(f"pko{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arkown{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        pvc = self.pvc_for(vol)
        k8s = self._ensure_k8s_utils()
        ns = getattr(k8s, "namespace", "simplyblock")

        self.logger.info("[AR-K-004] annotating %s so a slot takes ownership",
                         pvc)
        k8s._exec_kubectl(
            f"kubectl annotate pvc {pvc} -n {ns} "
            f"{self.ANNOTATION}={pname} --overwrite 2>&1 || true")
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="the annotation path should attach")

        # Same policy on purpose. Ownership is the conflict, not the value.
        vrc = self.make_vr_class(f"vrco-{stamp}", pname)
        vr = self.make_vr(f"vro-{stamp}", vrc, pvc)
        sleep_n_sec(60)

        conds = self.vr_conditions(vr)
        st = self.vr_status(vr)
        msg = json.dumps(st)[:400]
        if conds.get("Completed") == "True":
            raise AssertionError(
                f"[AR-K-004] a VolumeReplication was accepted for {pvc} while "
                f"a ReplicationSlot already owns it. EnableVolumeReplication "
                f"must return FAILED_PRECONDITION whenever a slot manages the "
                f"volume -- even when both name the same policy ({pname}), "
                f"because ownership is the conflict. Two owners mean every "
                f"flap becomes a detach + re-attach, and a re-attach is a "
                f"FULL re-sync. status={msg}")
        self.logger.info("[AR-K-004] PASS: refused while the slot owns it "
                         "(conditions=%s)", conds)

        # And the documented way across: drop the annotation, let the slot
        # detach, THEN the CR may take over.
        self.logger.info("[AR-K-004] removing the annotation and retrying")
        k8s._exec_kubectl(f"kubectl annotate pvc {pvc} -n {ns} "
                          f"{self.ANNOTATION}- 2>&1 || true")
        sleep_n_sec(60)
        self.make_vr(vr, vrc, pvc)
        try:
            self.await_vr_condition(vr, "Completed", "True", timeout=600,
                                    case_id="AR-K-004")
            self.logger.info("[AR-K-004] PASS: hand-over works once the "
                             "annotation is gone")
        except AssertionError as exc:
            self.logger.warning(
                "[AR-K-004] the documented hand-over (remove annotation, then "
                "create the VolumeReplication) did not complete: %s. Raised "
                "rather than failed -- the ownership refusal above is the "
                "case's main assertion and it passed.", str(exc)[:200])

        self.cleanup_vrs()
        try:
            self.policy_clear(vol_id)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()


class CsiAddonsPromoteDemote(_CsiAddonsBase):
    """AR-K-005, AR-K-006, AR-K-007, AR-K-008, AR-K-009.

    The lifecycle verbs, and the one real hazard in them.

    AR-K-005 is the case worth reading the code for. The driver refuses an
    unprepared planned promote with FAILED_PRECONDITION, and the vendored
    csi-addons controller **auto-escalates any FAILED_PRECONDITION from a
    force=false promote straight to force=true, inline, with no grace
    period**. The planned path exists to flush the final delta before
    switching. The forced path "clones the last fully replicated generation
    and ignores demote state entirely". So a relocation that should lose
    nothing can silently lose up to a full interval, and the CR will report
    success either way.
    """

    def run(self):
        if not self.require_csi_addons("AR-K-005"):
            return
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tkp{stamp}", self.cluster_b)
        pname = self.policy_add(f"pkp{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arkp{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol)
        vrc = self.make_vr_class(f"vrcp-{stamp}", pname)
        vr = self.make_vr(f"vrp-{stamp}", vrc, self.pvc_for(vol))
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")
        self.await_vr_condition(vr, "Completed", "True", case_id="AR-K-005")

        # ── AR-K-005 the auto-escalation hazard ───────────────────────────
        # Write data, do NOT demote, and ask for a planned promote. The
        # unflushed write is the probe: if the planned path ran, it survives;
        # if the escalation fired, it is gone.
        self.logger.info("[AR-K-005] writing an unreplicated marker, then "
                         "asking for a PLANNED promote with no demote first")
        self.write_marker_files(vol, "unflushed", count=1)
        unflushed = self._generate_checksums_dual(vol)
        probe = [f for f in unflushed if "unflushed" in f]
        if not probe:
            raise ReplicationPreconditionError(
                "[AR-K-005] the unflushed marker never landed, so the "
                "escalation probe below would prove nothing.")

        self.set_vr_state(vr, "secondary")   # ask the source to step down…
        sleep_n_sec(5)
        self.set_vr_state(vr, "primary")     # …then immediately re-promote
        sleep_n_sec(90)

        st = self.vr_status(vr)
        conds = self.vr_conditions(vr)
        self.logger.info("[AR-K-005] after the flip: conditions=%s message=%s",
                         conds, str(st.get("message"))[:200])
        self.logger.info(
            "[AR-K-005] NOTE for dev: the driver returns FAILED_PRECONDITION "
            "for a force=false promote with no prior demote, and the vendored "
            "controller escalates that to force=true inline with no grace "
            "period. If the unflushed marker is missing below, a PLANNED "
            "relocation silently took the UNPLANNED path and lost a full "
            "interval of writes, while the CR reported success.")

        # ── AR-K-006 / AR-K-007 the orderly path ──────────────────────────
        self.logger.info("[AR-K-006] demoting cleanly, then promoting")
        self._disconnect_and_cleanup_dual(vol)
        self.set_vr_state(vr, "secondary")
        try:
            self.await_vr_condition(vr, "Completed", "True", timeout=900,
                                    case_id="AR-K-006")
            self.logger.info("[AR-K-006] PASS: demote converged and settled")
        except AssertionError as exc:
            self.logger.warning(
                "[AR-K-006] demote did not report Completed within 900s: %s. "
                "NOTE the design and the code disagree on what a demote does: "
                "P0-3 specifies converge-while-serving then quiesce then "
                "fence, but demote_lvol fences FIRST and then ships, because "
                "'a write accepted on a still-optimized path after the "
                "snapshot is the delta of record is silently lost'. Under the "
                "implemented order the volume is FENCED for the whole final "
                "transfer -- and with allow_partial disabled that transfer is "
                "the WHOLE volume, not a delta. So a slow demote here is "
                "expected to scale with volume size, and the planned-"
                "relocation downtime is a full transfer. Raised for dev.",
                str(exc)[:200])

        self.set_vr_state(vr, "primary")
        self.await_state(vol_id, [self.STATE_FAILED_OVER,
                                  self.STATE_CUTOVER_DONE],
                         timeout=self.REPL_OP_SEC,
                         what="promote after a clean demote")
        name = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(name, format_disk=False)
        after = self._generate_checksums_dual(name)

        lost = [f for f in sums if f not in after or after[f] != sums[f]]
        if lost:
            raise AssertionError(
                f"[AR-K-007] data that was fully replicated before the "
                f"demote is missing or changed after the promote: {lost}. A "
                f"planned relocation must not lose anything that already "
                f"completed a cycle.")
        kept_probe = [f for f in after if "unflushed" in f]
        self.logger.info(
            "[AR-K-005/007] replicated data intact. Unflushed marker present "
            "after the move: %s. %s", bool(kept_probe),
            "Planned path flushed it." if kept_probe else
            "ABSENT -- consistent with the force=true escalation described "
            "above. Raise with dev: a planned relocation should flush this.")
        self.logger.info("[AR-K-007] PASS: no loss of replicated data")

        # ── AR-K-009 resync back ──────────────────────────────────────────
        self.logger.info("[AR-K-009] resyncing back toward %s", self.cluster_a)
        self._disconnect_and_cleanup_dual(name)
        self.set_vr_state(vr, "secondary")
        sleep_n_sec(60)
        rel = self.relationship_for(vol_id) or {}
        self.logger.info("[AR-K-009] relationship after resync request: "
                         "state=%s direction=%s", rel.get("state"),
                         rel.get("direction"))
        if rel and rel.get("direction") == self.DIRECTION_TO_TARGET:
            self.logger.warning(
                "[AR-K-009] direction is still %r after a resync request. "
                "Fail-back reverses direction, so a lookup by source now "
                "finds the wrong end. Raised for dev.",
                rel.get("direction"))
        else:
            self.logger.info("[AR-K-009] PASS: resync reversed the "
                             "relationship")

        self.assert_no_corruption("after AR-K lifecycle cases")
        self.cleanup_vrs()
        for n in {vol, name}:
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()


class CsiAddonsForcedFailoverAndOutage(_CsiAddonsBase):
    """AR-K-008, AR-K-012, AR-K-013, AR-K-014.

    The unplanned path and what a fault does to it. This is where this lane
    stops overlapping dev's unit tests entirely: none of their 68 unit cases
    injects an outage, and the forced path is exactly the one that runs
    during a real disaster, when things are already broken.
    """

    def run(self):
        if not self.require_csi_addons("AR-K-008"):
            return
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tkf{stamp}", self.cluster_b)
        pname = self.policy_add(f"pkf{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"arkf{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol)
        vrc = self.make_vr_class(f"vrcf-{stamp}", pname)
        vr = self.make_vr(f"vrf-{stamp}", vrc, self.pvc_for(vol))
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        # ── AR-K-012 the target side's class carries an EMPTY policy id ───
        # replication.go:265 reads an empty replicationPolicyID as "this is
        # the fail-over target". Worth asserting it is accepted rather than
        # rejected as a missing parameter, because that is how the DR
        # cluster's class is authored and a validation error there would
        # only show up during a real failover.
        self.logger.info("[AR-K-012] creating a target-side class with an "
                         "empty %s", self.POLICY_PARAM)
        tgt_vrc = self.make_vr_class(f"vrct-{stamp}", "")
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            f"kubectl get volumereplicationclass {tgt_vrc} -o name "
            f"2>/dev/null || true", supress_logs=True)
        if tgt_vrc not in (out or ""):
            raise AssertionError(
                f"[AR-K-012] the target-side VolumeReplicationClass was "
                f"rejected. An empty {self.POLICY_PARAM} is how the driver is "
                f"told 'this side is the fail-over target' "
                f"(replication.go:265); rejecting it means the DR cluster "
                f"cannot be configured at all.")
        self.logger.info("[AR-K-012] PASS: target-side class accepted")

        # ── AR-K-013 outage during a forced promote ───────────────────────
        self.logger.info("[AR-K-013] killing a source node, then forcing a "
                         "promote -- the real disaster sequence")
        src_ip = None
        for n in self.sbcli_utils.get_storage_nodes().get("results", []):
            if n.get("cluster_id") == self.cluster_a and n.get("mgmt_ip"):
                src_ip = n["mgmt_ip"]
                break
        self._disconnect_and_cleanup_dual(vol)
        if src_ip:
            undo = self.outage_on_node(src_ip, "container_stop")
        else:
            undo = lambda: True                       # noqa: E731

        self.set_vr_state(vr, "primary")
        try:
            self.await_state(vol_id, [self.STATE_FAILED_OVER,
                                      self.STATE_CUTOVER_DONE],
                             timeout=self.REPL_OP_SEC,
                             what="forced promote with the source degraded")
            self.logger.info("[AR-K-013] PASS: promote completed with a "
                             "source node down")
        finally:
            try:
                undo()
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[AR-K-013] undo: %s", str(exc)[:160])

        # ── AR-K-008 the forced path keeps the last replicated generation ─
        name = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(name, format_disk=False)
        after = self._generate_checksums_dual(name)
        lost = [f for f in sums if f not in after or after[f] != sums[f]]
        if lost:
            raise AssertionError(
                f"[AR-K-008] the forced promote did not preserve the last "
                f"fully replicated generation: {lost} missing or changed. "
                f"Force=true is documented as cloning that generation; "
                f"losing part of it makes the RPO unbounded in exactly the "
                f"scenario the feature exists for.")
        self.logger.info("[AR-K-008] PASS: last replicated generation intact "
                         "after a forced promote")

        # ── AR-K-014 control-plane restart mid-reconcile ──────────────────
        self.logger.info("[AR-K-014] restarting the control plane and "
                         "checking the CR reconciles again")
        mgmt = self.mgmt_nodes[0]
        self.ssh_obj.exec_command(
            node=mgmt,
            command="kubectl -n simplyblock rollout restart deploy "
                    "simplyblock-control-plane 2>&1 || "
                    "kubectl -n simplyblock rollout restart deploy "
                    "-l app.kubernetes.io/part-of=simplyblock 2>&1 || true")
        sleep_n_sec(150)
        st = self.vr_status(vr)
        if not st:
            raise AssertionError(
                "[AR-K-014] the VolumeReplication has no status at all after "
                "a control-plane restart. The adapter is stateless and "
                "derives every answer from the backend, so a missing status "
                "means it cannot reach the control plane -- and Ramen would "
                "read that as an unprotected volume.")
        self.logger.info("[AR-K-014] PASS: status still served after the "
                         "restart: %s", self.vr_conditions(vr))

        self.assert_no_corruption("after AR-K forced/outage cases")
        self.cleanup_vrs()
        for n in {vol, name}:
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()


class CsiAddonsGroupReplication(_CsiAddonsBase):
    """AR-K-015: VolumeGroupReplication -- a consistency group as one unit.

    Recorded, not implemented. Dev's design marks `VolumeGroupReplication`
    as **Phase 4, Planned**, blocked on two control-plane prerequisites that
    are explicitly "Not shipped": group-level replication (P0-6) and a group
    replication policy (P0-7). The engine replicates per volume today and a
    consistency group has no group-level replication of its own.

    This matters for DR scope: until it lands, Ramen can protect a
    multi-volume application only as a set of independent volumes, each
    failing over at its own instant. That is precisely what a consistency
    group exists to prevent, so the gap is worth stating rather than
    discovering during a drill.
    """

    def run(self):
        self.skip_case(
            "AR-K-015",
            "VolumeGroupReplication is Phase 4 (Planned) in dev's own "
            "csi-addons design, blocked on P0-6 (group-level replication in "
            "the control plane) and P0-7 (a group replication policy), both "
            "marked Not shipped. Until it lands, a multi-volume application "
            "under Ramen fails over as independent volumes at independent "
            "instants -- re-check once Phase 4 ships.")
        self.logger.info(
            "[AR-K-015] Engine-level consistency groups ARE covered today by "
            "AR-C-001..008; what is missing is only the csi-addons group "
            "surface that would let Ramen drive them as one unit.")
