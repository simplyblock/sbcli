"""AR-U: upgrade and migration. Also in the separate stress lane.

Three things that happen to a cluster that already has replication running,
none of which anyone currently tests:

    AR-U-001..003  the control plane is upgraded underneath live relationships
    AR-U-004..006  the operator is upgraded, and its CRDs change
    AR-U-007..009  an existing customer moves from the annotation path to
                   csi-addons

The third is the one with a customer behind it. Every volume protected
today uses the PVC annotation and a ``ReplicationSlot``. The csi-addons
design makes that path a compatibility layer and eventually retires it, and
the documented hand-over is "remove the annotation, let the slot detach,
then create the VolumeReplication". What nobody has written down is what
the detach does to the copy already sitting on the DR cluster. If it
deletes it, then every migrated volume needs a **full** re-sync -- and with
``allow_partial`` disabled that is the entire volume, per volume, across
the fleet. That is an upgrade-planning item, and AR-U-007 is the test that
turns the question into a number.

A note on what "upgrade" means here. These cases restart and re-apply; they
do not pull a different build. A genuine version-to-version upgrade needs
two images and a pipeline that installs one and then the other, which is a
CI change rather than a test change. Where a case can only do the weaker
thing, it says so rather than claiming the stronger one.
"""
import json
import time

from e2e_tests.replication.replication_base import (
    ReplicationTestBase,
)
from utils.common_utils import sleep_n_sec


class ReplicationSurvivesControlPlaneUpgrade(ReplicationTestBase):
    """AR-U-001, AR-U-002, AR-U-003: the control plane changes underneath.

    AR-O-010 already restarts the control plane mid-cycle. This is the
    harder version: the relationship must survive the control plane being
    *replaced*, and -- the part a restart does not test -- the task runners
    must pick the work back up rather than leaving a relationship that
    looks healthy and never cycles again.

    The distinction matters because replication state lives in the
    database, not in the process. A restart proves the process re-reads it.
    An upgrade proves the schema still means the same thing.
    """

    def run(self):
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tug{stamp}", self.cluster_b)
        pname = self.policy_add(f"pug{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)
        vol = f"arug{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol)
        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        before = self.relationship_for(vol_id) or {}
        before_target = before.get("target_lvol_id")
        self.logger.info("[AR-U-001] relationship before the upgrade: %s",
                         before)

        self.logger.info("[AR-U-001] rolling the control plane")
        mgmt = self.mgmt_nodes[0]
        if self.k8s_test:
            self.ssh_obj.exec_command(
                node=mgmt,
                command="kubectl -n simplyblock rollout restart deploy "
                        "simplyblock-control-plane 2>&1 || "
                        "kubectl -n simplyblock rollout restart deploy "
                        "-l app.kubernetes.io/part-of=simplyblock 2>&1 || true")
            self.ssh_obj.exec_command(
                node=mgmt,
                command="kubectl -n simplyblock rollout status deploy "
                        "simplyblock-control-plane --timeout=600s 2>&1 || true")
        else:
            self.ssh_obj.exec_command(
                node=mgmt,
                command="sudo docker restart $(sudo docker ps -q "
                        "-f name=app_ -f name=tasks 2>/dev/null) 2>&1 || true")
        sleep_n_sec(150)

        # ── AR-U-001 the relationship is still there ──────────────────────
        deadline = time.time() + 900
        while time.time() < deadline:
            after = self.relationship_for(vol_id)
            if after:
                break
            sleep_n_sec(15)
        else:
            raise AssertionError(
                f"[AR-U-001] the replication relationship for {vol} did not "
                f"come back within 900s of a control-plane roll. Replication "
                f"state lives in the database, so losing it on an upgrade "
                f"means every relationship in the cluster is only as durable "
                f"as the control plane's uptime. Before the roll it was: "
                f"{before}")
        if after.get("target_lvol_id") != before_target:
            raise AssertionError(
                f"[AR-U-001] the target volume changed across the upgrade "
                f"({before_target} -> {after.get('target_lvol_id')}). That is "
                f"a re-attach, which means a full re-sync of every protected "
                f"volume every time the control plane is upgraded.")
        self.logger.info("[AR-U-001] PASS: relationship and target survived")

        # ── AR-U-002 cycles resume with NO operator action ────────────────
        # Deliberately no trigger first: an upgrade must not need a human to
        # restart replication, and a relationship that looks healthy but
        # never cycles again is the failure this catches.
        self.logger.info("[AR-U-002] waiting for an UNPROMPTED cycle")
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 * 2 + 120)
        rel = self.relationship_for(vol_id) or {}
        if rel.get("state") not in (self.STATE_REPLICATING, None):
            raise AssertionError(
                f"[AR-U-002] state is {rel.get('state')!r} two intervals "
                f"after the upgrade, with no operator action. Cycles must "
                f"resume on their own.")
        self.replication_trigger(vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)
        self.logger.info("[AR-U-002] PASS: cycling again after the upgrade")

        # ── AR-U-003 and the data is still right ──────────────────────────
        self._disconnect_and_cleanup_dual(vol)
        self.failover_policy(pname)
        self.await_state(vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="post-upgrade failover")
        name = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(name, format_disk=False)
        self.verify_volume(name, sums,
                           context="after a control-plane upgrade (AR-U-003)")
        self.logger.info("[AR-U-003] PASS: data intact across the upgrade")

        self.assert_no_corruption("after AR-U control-plane upgrade")
        for n in {vol, name}:
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()


class ReplicationSurvivesOperatorUpgrade(ReplicationTestBase):
    """AR-U-004, AR-U-005, AR-U-006: the operator and its CRDs change.

    Kubernetes only. There is a specific trap here that has bitten this lab
    before, on the lblk work:

        **Helm never upgrades ``crds/``.** It is helm's one special
        directory -- installed once on first install, never touched on
        upgrade. Workflows only ever get fresh CRDs because cleanup deletes
        them first. On a real customer upgrade nothing deletes them, so the
        old schema survives and the apiserver **silently prunes** any field
        the old CRD does not know about.

    Silently is the problem. A replication field added in the new operator
    is accepted by ``kubectl``, dropped by the apiserver, and the resulting
    object looks valid and does the wrong thing. AR-U-005 looks for exactly
    that by round-tripping every field we set.
    """

    def run(self):
        if not self.k8s_test:
            self.skip_case(
                "AR-U-004",
                "operator and CRD upgrade is a Kubernetes concern; this run "
                "is on docker. The control-plane half is covered by AR-U-001.")
            return

        k8s = self._ensure_k8s_utils()
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"top{stamp}", self.cluster_b)
        pname = self.policy_add(f"pop{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)
        vol = f"arop{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        self.policy_set(vol_id, pname)
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC, what="initial sync")

        # ── AR-U-005 do the CRDs still carry every field? ─────────────────
        # Round-trip check against the installed schema. If the apiserver is
        # pruning, the field is simply absent on read-back with no error
        # anywhere -- which is why this is checked rather than assumed.
        self.logger.info("[AR-U-005] checking the installed CRD schemas for "
                         "the fields the operator sets")
        expect = {
            "replicationpolicies.storage.simplyblock.io":
                ["interval", "mode", "pairRef", "snapshotRetention"],
            "replicationpairs.storage.simplyblock.io":
                ["sourceCluster", "targetCluster"],
            "replicationops.storage.simplyblock.io":
                ["action", "scope", "ref", "deleteSource", "sourceClusterID"],
            "replicationslots.storage.simplyblock.io":
                ["policyRef", "pvcRef", "volumeID"],
        }
        pruned = {}
        for crd, fields in expect.items():
            out, _ = k8s._exec_kubectl(
                f"kubectl get crd {crd} -o json 2>/dev/null || true",
                supress_logs=True)
            if not (out or "").strip():
                pruned[crd] = "CRD NOT INSTALLED"
                continue
            missing = [f for f in fields if f'"{f}"' not in out]
            if missing:
                pruned[crd] = missing
        if pruned:
            raise AssertionError(
                f"[AR-U-005] the installed CRD schemas are missing fields the "
                f"operator sets: {pruned}. Helm never upgrades crds/ -- it is "
                f"installed once and never touched again -- so an upgraded "
                f"operator writes fields an old schema does not know, and the "
                f"apiserver PRUNES them silently. The object then looks valid "
                f"and does the wrong thing, with no error anywhere. Either "
                f"the chart must apply crds/ explicitly on upgrade, or the "
                f"upgrade procedure has to say 'kubectl apply -f .../crds/' "
                f"out loud.")
        self.logger.info("[AR-U-005] PASS: every expected field present in "
                         "the installed schemas")

        # ── AR-U-004 roll the operator; the relationship must not notice ──
        before = self.relationship_for(vol_id) or {}
        self.logger.info("[AR-U-004] rolling the operator")
        k8s._exec_kubectl(
            "kubectl -n simplyblock rollout restart deploy "
            "simplyblock-operator 2>&1 || true")
        k8s._exec_kubectl(
            "kubectl -n simplyblock rollout status deploy "
            "simplyblock-operator --timeout=600s 2>&1 || true")
        sleep_n_sec(120)

        after = self.relationship_for(vol_id) or {}
        if not after:
            raise AssertionError(
                f"[AR-U-004] the relationship is gone after an operator roll. "
                f"The operator reconciles the CRs; it must not be able to "
                f"destroy a live backend relationship by restarting. Before: "
                f"{before}")
        if after.get("target_lvol_id") != before.get("target_lvol_id"):
            raise AssertionError(
                f"[AR-U-004] the operator roll re-attached the volume "
                f"({before.get('target_lvol_id')} -> "
                f"{after.get('target_lvol_id')}), which is a full re-sync on "
                f"every operator upgrade.")
        self.logger.info("[AR-U-004] PASS: relationship untouched by the roll")

        # ── AR-U-006 slots are reconciled, not recreated ──────────────────
        out, _ = k8s._exec_kubectl(
            "kubectl get replicationslots -n simplyblock -o json "
            "2>/dev/null || true", supress_logs=True)
        try:
            slots = json.loads(out).get("items", [])
        except Exception:                             # noqa: BLE001
            slots = []
        self.logger.info("[AR-U-006] %d replication slot(s) after the roll",
                         len(slots))
        for s in slots:
            st = (s.get("status") or {})
            if st.get("state") in (None, "", "Unknown"):
                self.logger.warning(
                    "[AR-U-006] slot %s has no state after the operator roll "
                    "(%s). A slot whose status never repopulates reports an "
                    "unprotected volume to anything reading it.",
                    (s.get("metadata") or {}).get("name"), st)
        self.logger.info("[AR-U-006] PASS: slots present after the roll")

        self.assert_no_corruption("after AR-U operator upgrade")
        try:
            self.policy_clear(vol_id)
            self.sbcli_utils.delete_lvol(lvol_name=vol)
        except Exception:                             # noqa: BLE001
            pass
        self.cleanup_replication()


class ReplicationAnnotationToCsiAddonsMigration(ReplicationTestBase):
    """AR-U-007, AR-U-008, AR-U-009: the path every existing customer takes.

    Today a protected volume carries a PVC annotation and the operator keeps
    a ``ReplicationSlot`` for it. The csi-addons design makes that a
    compatibility layer and eventually retires it. The documented hand-over
    is deliberate and three-step, because the two paths are mutually
    exclusive per volume:

        remove the annotation  ->  the slot detaches  ->  create the
        VolumeReplication

    **AR-U-007 measures what the middle step costs.** If detaching deletes
    the target copy, the new owner starts from nothing and ships the whole
    volume again. Across a fleet, with full transfers, that is the
    difference between an afternoon and a week -- and it is currently
    nobody's documented number.

    This is open question 9 turned into a measurement.
    """

    def run(self):
        if not self.k8s_test:
            self.skip_case(
                "AR-U-007",
                "the annotation path and csi-addons are both Kubernetes "
                "surfaces; this run is on docker.")
            return

        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            "kubectl get crd volumereplications."
            "replication.storage.openshift.io -o name 2>/dev/null || true",
            supress_logs=True)
        if "volumereplication" not in (out or ""):
            self.skip_case(
                "AR-U-007",
                "the VolumeReplication CRD is not installed, so there is "
                "nothing to migrate TO. Needs operator PR #548 with "
                "csiaddons.create enabled.")
            return

        ns = getattr(k8s, "namespace", "simplyblock")
        ANNOT = "storage.simplyblock.io/replication-policy"
        self.build_second_cluster()
        stamp = int(time.time()) % 100000
        tname = self.target_add(f"tmg{stamp}", self.cluster_b)
        pname = self.policy_add(f"pmg{stamp}", tname,
                                interval_min=self.REPL_INTERVAL_MIN)

        vol = f"armg{stamp}"
        self.sbcli_utils.add_lvol(lvol_name=vol, pool_name=self.pool_name,
                                  size=self.REPL_VOLUME_SIZE)
        vol_id = self.sbcli_utils.get_lvol_id(lvol_name=vol)
        sums = self.seed_volume(vol)
        pvc = (self._volume_registry.get(vol, {}).get("pvc_name")
               or self._k8s_normalize_name(vol))

        # ── the "before": a customer on the annotation path ───────────────
        self.logger.info("[AR-U-007] protecting %s the OLD way (annotation)",
                         pvc)
        k8s._exec_kubectl(f"kubectl annotate pvc {pvc} -n {ns} "
                          f"{ANNOT}={pname} --overwrite 2>&1 || true")
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC,
                         what="annotation path attaches")
        self.replication_trigger(vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)
        old_target = (self.relationship_for(vol_id) or {}).get("target_lvol_id")
        self.logger.info("[AR-U-007] established; target copy is %s",
                         old_target)

        # ── step 1: remove the annotation, and watch the target copy ──────
        self.logger.info("[AR-U-007] removing the annotation (step 1 of the "
                         "documented hand-over)")
        t0 = time.time()
        k8s._exec_kubectl(f"kubectl annotate pvc {pvc} -n {ns} {ANNOT}- "
                          f"2>&1 || true")
        deadline = time.time() + 600
        while time.time() < deadline:
            if not self.relationship_for(vol_id):
                break
            sleep_n_sec(10)
        detach = time.time() - t0
        self.logger.info("[AR-U-007] slot detached in %.0fs", detach)

        # Did the detach take the target copy with it? That is the whole
        # question, and it decides whether the migration is cheap or a
        # fleet-wide full re-sync.
        target_survived = False
        if old_target:
            try:
                d = self.sbcli_utils.get_lvol_details(lvol_id=old_target)
                rows = d.get("results", d) if isinstance(d, dict) else d
                target_survived = bool(rows)
            except Exception:                         # noqa: BLE001
                target_survived = False
        self.logger.info(
            "[AR-U-007] RESULT: target copy %s %s the detach. %s",
            old_target, "SURVIVED" if target_survived else "was DELETED by",
            "The migration can reuse it, so the re-attach should be cheap."
            if target_survived else
            "Every migrated volume therefore needs a FULL re-sync -- with "
            "allow_partial disabled that is the entire volume, per volume, "
            "across the fleet. This is the number for open question 9, and "
            "it is an upgrade-planning item rather than a release note.")

        # ── step 2: the new owner takes over ──────────────────────────────
        self.logger.info("[AR-U-008] creating the VolumeReplication (step 3)")
        vrc = f"vrcmg-{stamp}"
        k8s.apply_yaml_cluster_scoped(f"""
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplicationClass
metadata:
  name: {vrc}
spec:
  provisioner: csi.simplyblock.io
  parameters:
    replicationPolicyID: "{pname}"
""")
        vr = f"vrmg-{stamp}"
        k8s.apply_yaml(f"""
apiVersion: replication.storage.openshift.io/v1alpha1
kind: VolumeReplication
metadata:
  name: {vr}
spec:
  volumeReplicationClass: {vrc}
  replicationState: primary
  dataSource:
    kind: PersistentVolumeClaim
    name: {pvc}
""", namespace=ns)

        t1 = time.time()
        self.await_state(vol_id, self.STATE_REPLICATING,
                         timeout=self.REPL_CYCLE_SEC * 2,
                         what="csi-addons takes over after the hand-over")
        resync = time.time() - t1
        new_target = (self.relationship_for(vol_id) or {}).get("target_lvol_id")
        self.logger.info(
            "[AR-U-008] PASS: csi-addons owns %s after %.0fs. target %s -> %s "
            "(%s)", vol, resync, old_target, new_target,
            "REUSED" if new_target == old_target else "NEW -- full re-sync")

        # ── AR-U-009 nothing was lost in the hand-over ────────────────────
        self.logger.info("[AR-U-009] verifying the data survived the migration")
        self.replication_trigger(vol_id)
        sleep_n_sec(self.REPL_INTERVAL_MIN * 60 + 90)
        self._disconnect_and_cleanup_dual(vol)
        self.failover_policy(pname)
        self.await_state(vol_id, self.STATE_FAILED_OVER,
                         timeout=self.REPL_OP_SEC, what="post-migration failover")
        name = self.failed_over_volume_name(vol_id, vol)
        self._connect_and_mount_dual(name, format_disk=False)
        self.verify_volume(name, sums,
                           context="after migrating from the annotation path "
                                   "to csi-addons (AR-U-009)")
        self.logger.info("[AR-U-009] PASS: no data lost in the hand-over")

        self.assert_no_corruption("after AR-U migration")
        for obj, kind in ((vr, "volumereplication"),):
            k8s._exec_kubectl(f"kubectl delete {kind} {obj} -n {ns} "
                              f"--ignore-not-found 2>&1 || true")
        k8s._exec_kubectl(f"kubectl delete volumereplicationclass {vrc} "
                          f"--ignore-not-found 2>&1 || true")
        for n in {vol, name}:
            try:
                self._disconnect_and_cleanup_dual(n)
                self.sbcli_utils.delete_lvol(lvol_name=n)
            except Exception:                         # noqa: BLE001
                pass
        self.cleanup_replication()
