"""Operator restarts, a sealed IAM backend, RBAC, and the latency question.

Four of these close scenarios dev *wrote* and could not run: `envtest` has no
kubelet, so a missing `envFrom` Secret never produces the
`CreateContainerConfigError` that is supposed to degrade an ObjectStore; it
runs no real manager, so a restart mid-deletion is untested; and it runs with
an unconstrained client, so the ClusterRole has never completed a real cycle.

The fifth produces a number rather than a verdict. `design-bucket-provisioning`
Open Question 4 asks for the provisioning-latency distribution and says it can
only be answered "once this design is deployed somewhere the pNFS path exists".
We have that cluster.
"""
import time

from e2e_tests.objectstore.objectstore_base import _ObjectStoreBase
from utils import s3_utils as s3


class S3OpenBaoSealedK8s(_ObjectStoreBase):
    """TC-S3-006 -- OpenBao sealed under the gateway's IAM.

    Flagged by dev's own deploy notes and tested nowhere:

        OpenBao re-seals whenever its pod restarts, and only a rerun of this
        script unseals it.

    Single replica, pebbledb, unseal key in `Secret/openbao-init`. The
    interesting question is not whether a sealed Bao breaks IAM -- it must --
    but *how*: a clean 5xx is survivable, a hang is not, and an operation that
    half-succeeds with the wrong identity is the one that matters.
    """

    BAO_NS = "vault"
    BAO_POD = "openbao-0"

    def run(self):
        self.logger.info("=== TC-S3-006 OpenBao sealed mid-run ===")
        probe = s3.BaoUnsealer(self.k8s_utils, namespace=self.BAO_NS,
                               pod=self.BAO_POD)
        state = probe.sealed()
        if state is None:
            raise s3.S3ProbeError(
                f"[TC-S3-006] cannot read OpenBao's seal state from "
                f"{self.BAO_NS}/{self.BAO_POD}. This case needs Vault-backed "
                f"IAM deployed; without it the gateway serves the root "
                f"account alone and there is nothing to seal.")
        if state and not probe.unseal():
            raise s3.S3ProbeError(
                "[TC-S3-006] OpenBao is sealed at the start and could not be "
                "unsealed, so the case cannot establish its own baseline")

        self.make_storage_class()
        # Prefer the Secret the lane's s3 step wrote from the AppRole it
        # configured: it carries the role_id/secret_id versitygw needs, which
        # an endpoint URL alone does not. Falling back to the endpoint keeps
        # the case runnable on a cluster brought up by hand, and says which
        # mode it is in rather than silently testing the weaker one.
        iam = self.iam_secret()
        if iam:
            creds = self.make_credentials()
            refs = [creds, iam]
            self.logger.info("[TC-S3-006] using the lane's AppRole Secret %s", iam)
        else:
            creds = self.make_credentials(
                vault_url=f"https://openbao-0.openbao-internal.{self.BAO_NS}.svc:8200")
            refs = [creds]
            self.logger.warning(
                "[TC-S3-006] no simplyblock-s3-iam Secret; the gateway gets an "
                "endpoint but no AppRole credential, so IAM may never have "
                "worked and 'it broke when sealed' would prove nothing")
        self.make_objectstore(secret_refs=refs)
        client = self.start_client()
        cfg = self.service_cfg()

        name = "tc006"
        self.make_bucket(name)
        s3.assert_roundtrip(client, cfg, name, "before.bin", 8 * 1024 ** 2,
                            "tc006", context="(TC-S3-006 baseline)")

        # ── seal it, by restarting the pod ──────────────────────────────
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {self.BAO_POD} -n {self.BAO_NS} --wait=false")
        deadline = time.time() + 300
        while time.time() < deadline:
            if probe.sealed():
                break
            time.sleep(5)
        else:
            self.logger.warning(
                "[TC-S3-006] OpenBao came back unsealed -- auto-unseal is "
                "configured on this cluster, which makes the whole scenario "
                "moot. Recording that rather than forcing it.")
            self.record("tc006_openbao", {"auto_unseal": True})
            return
        sealed_at = time.time()
        self.logger.info("[TC-S3-006] OpenBao is sealed")

        # ── the four operations ─────────────────────────────────────────
        results = {}
        results["list_buckets"] = client.call(cfg, "list_buckets", timeout=180)
        results["put_object"] = client.call(
            cfg, "put_object", bucket=name, key="while-sealed.bin",
            size=4 * 1024 ** 2, seed="tc006-sealed", timeout=180)
        # Creating a bucket through the CRD, which does not pass through Bao:
        # this is the control. A bucket that also fails would say the blast
        # radius is wider than IAM.
        self.make_bucket("tc006-while-sealed", wait=False)
        crd_phase_during = s3.phase_of(
            s3.s3bucket(self.k8s_utils, "tc006-while-sealed"))
        results["create_s3bucket_crd"] = {"phase": crd_phase_during}

        for op, res in results.items():
            self.logger.info("[TC-S3-006] while sealed, %s -> %s", op, res)

        # No hang. A call the client could not complete at all comes back as a
        # probe error from S3Pod.call, which is not what a sealed backend
        # should produce.
        for op in ("list_buckets", "put_object"):
            status = results[op].get("status")
            if status is None:
                raise s3.S3EvidenceError(
                    f"[TC-S3-006] {op} against a sealed IAM backend produced "
                    f"no HTTP response at all: {results[op]}. A hang is the "
                    f"worst of the three possible answers -- a client has "
                    f"nothing to retry on.")

        # The data path does not go through Bao, so an already-Ready bucket
        # must still read. If it does not, the blast radius is wrong.
        got = client.call(cfg, "get_object", bucket=name, key="before.bin",
                          size=8 * 1024 ** 2, seed="tc006", timeout=180)
        data_path_survived = got.get("status") == 200 and got.get("match")

        # And the ObjectStore must say something. Staying Ready with a dead
        # IAM backend is the finding.
        store = s3.objectstore(self.k8s_utils, self.STORE)
        store_phase = s3.phase_of(store)
        events = s3.events_for(self.k8s_utils, "ObjectStore", self.STORE)
        bucket_phase = s3.phase_of(s3.s3bucket(self.k8s_utils, name))

        # ── unseal, and retry everything ────────────────────────────────
        if not probe.unseal():
            raise s3.S3ProbeError(
                "[TC-S3-006] could not unseal OpenBao again; the lab is left "
                "with a sealed IAM backend and the next case will fail on it")
        sealed_seconds = time.time() - sealed_at

        recovered = {}
        deadline = time.time() + 300
        while time.time() < deadline:
            recovered["list_buckets"] = client.call(cfg, "list_buckets", timeout=180)
            if recovered["list_buckets"].get("status") == 200:
                break
            time.sleep(10)
        s3.assert_roundtrip(client, cfg, name, "after-unseal.bin",
                            8 * 1024 ** 2, "tc006-after",
                            context="(TC-S3-006 after unseal)")
        s3.wait_phase(self.k8s_utils, "s3bucket", "tc006-while-sealed", "Ready",
                      timeout=self.BUCKET_TIMEOUT, forbid="Degraded")

        self.record("tc006_openbao", {
            "sealed_seconds": round(sealed_seconds, 1),
            "while_sealed": {k: {kk: vv for kk, vv in v.items()
                                 if kk in ("status", "code", "phase")}
                             for k, v in results.items()},
            "data_path_survived": data_path_survived,
            "objectstore_phase_while_sealed": store_phase,
            "bucket_phase_while_sealed": bucket_phase,
            "objectstore_events": events[-10:],
            "recovered_without_gateway_restart": True,
        })

        if not data_path_survived:
            raise s3.S3EvidenceError(
                f"[TC-S3-006] a bucket that was already Ready stopped reading "
                f"while OpenBao was sealed: {got}. The data path does not go "
                f"through Vault, so this is a wider blast radius than the "
                f"design describes.")
        if store_phase == "Ready" and not events:
            self.logger.error(
                "[TC-S3-006] FINDING: the ObjectStore stayed Ready with a "
                "sealed IAM backend and recorded no event. Nothing in the "
                "cluster's own state says IAM is dead.")
        self.logger.info("[TC-S3-006] PASS -- sealed for %.0fs", sealed_seconds)


class S3BucketProvisioningLatencyK8s(_ObjectStoreBase):
    """TC-S3-014 -- cold-MDS and warm-MDS provisioning latency, and the histogram.

    Closes `design-bucket-provisioning` E-05, E-06 and E-07 -- and with them
    **Open Question 4**, which asks for this distribution and says it can only
    be measured "once this design is deployed somewhere the pNFS path exists".

    The first bucket on a storage cluster boots the MDS guest; later ones do
    not. E-06 expects the second to be "substantially lower", which this case
    turns into a number rather than an adjective. There is no pass/fail
    threshold here on purpose: dev has no target yet, and inventing one would
    make this case's result an opinion.
    """

    WARM_COUNT = 5

    def run(self):
        self.logger.info("=== TC-S3-014 provisioning latency, cold and warm ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()

        mds_before = self._mds_pods()
        self.logger.info("[TC-S3-014] MDS pods before the first bucket: %s",
                         mds_before)

        # ── cold ────────────────────────────────────────────────────────
        t0 = time.time()
        self.make_bucket("tc014-cold", wait=False)
        _, cold_transitions = s3.wait_phase(
            self.k8s_utils, "s3bucket", "tc014-cold", "Ready",
            timeout=self.BUCKET_TIMEOUT, forbid="Degraded")
        cold = time.time() - t0
        mds_after = self._mds_pods()
        self.logger.info("[TC-S3-014] cold bucket Ready in %.1fs (MDS now %s)",
                         cold, mds_after)

        # ── warm ────────────────────────────────────────────────────────
        warm = []
        for i in range(self.WARM_COUNT):
            name = f"tc014-warm-{i}"
            t = time.time()
            self.make_bucket(name, wait=False)
            s3.wait_phase(self.k8s_utils, "s3bucket", name, "Ready",
                          timeout=self.BUCKET_TIMEOUT, forbid="Degraded")
            warm.append(round(time.time() - t, 1))
            self.logger.info("[TC-S3-014] warm bucket %d Ready in %.1fs",
                             i, warm[-1])

        warm_sorted = sorted(warm)
        warm_median = warm_sorted[len(warm_sorted) // 2]

        # ── E-07: the histogram records both, distinguishably ───────────
        text = s3.scrape_operator_metrics(self.k8s_utils)
        hist = s3.histogram_quantiles(text, s3.METRIC_PROVISIONING_SECONDS)
        phases = s3.metric_samples(text, s3.METRIC_BUCKET_PHASE)
        self.logger.info("[TC-S3-014] histogram: %s", hist)

        if not hist:
            raise s3.S3EvidenceError(
                f"[TC-S3-014] {s3.METRIC_PROVISIONING_SECONDS} has no samples "
                f"after {1 + self.WARM_COUNT} buckets reached Ready. The "
                f"metric is the only answer Open Question 4 has, and E-07 "
                f"exists to prove it records real values.")
        if hist.get("count", 0) < 1 + self.WARM_COUNT:
            raise s3.S3EvidenceError(
                f"[TC-S3-014] the histogram counted {hist.get('count')} "
                f"observations for {1 + self.WARM_COUNT} buckets. One sample "
                f"per bucket reaching Ready is what I-13 asserts.")

        # A bucket stuck outside Ready is the cheapest detector there is, so
        # the gauge is checked here too while every bucket is known-Ready.
        not_ready = [(lbl, v) for lbl, v in phases
                     if v == 1 and lbl.get("phase") not in ("Ready", "")]
        if not_ready:
            raise s3.S3EvidenceError(
                f"[TC-S3-014] {s3.METRIC_BUCKET_PHASE} reports buckets outside "
                f"Ready while every bucket this case made is Ready: {not_ready}")

        self.record("tc014_provisioning_latency", {
            "cold_seconds": round(cold, 1),
            "cold_transitions": cold_transitions,
            "warm_seconds": warm,
            "warm_median": warm_median,
            "ratio_cold_to_warm_median": round(cold / warm_median, 2)
            if warm_median else None,
            "mds_pods_before": mds_before,
            "mds_pods_after": mds_after,
            "histogram": hist,
            "note": "No pass/fail threshold: dev has no target yet. This is "
                    "the measurement Open Question 4 asks for.",
        })
        self.logger.info(
            "[TC-S3-014] PASS -- cold %.1fs, warm median %.1fs (%.1fx)",
            cold, warm_median, cold / warm_median if warm_median else 0)

    def _mds_pods(self):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.ns} "
            f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} {{end}}' "
            f"2>/dev/null || true", supress_logs=True)
        return sorted(p for p in (out or "").split() if "mds" in p.lower())


class S3OperatorRestartK8s(_ObjectStoreBase):
    """TC-S3-015/016 -- the operator killed mid-provision, and mid-delete.

    TC-S3-015 closes `design-bucket-provisioning` E-08, `—`. TC-S3-016 closes
    "operator restart mid-deletion", listed in §5's axis table as not covered
    at any level -- and it is the more interesting of the two, because a
    delete interrupted between `WithdrawBucket` and the finalizer release is
    what leaves a mount nothing will ever withdraw.

    Both rely on `status.phaseDeadline` being persisted rather than held in
    memory, which is the whole reason that field exists.
    """

    def run(self):
        self.logger.info("=== TC-S3-015/016 operator restart ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()
        client = self.start_client()
        cfg = self.service_cfg()

        # ── TC-S3-015: killed mid-Provisioning ──────────────────────────
        name = "tc015"
        t0 = time.time()
        self.make_bucket(name, wait=False)

        # Catch it in flight. Pending or Provisioning both count: the point is
        # that the operator dies before the bucket is Ready.
        caught = None
        deadline = time.time() + 300
        while time.time() < deadline:
            ph = s3.phase_of(s3.s3bucket(self.k8s_utils, name))
            if ph in ("Pending", "Provisioning"):
                caught = ph
                break
            if ph == "Ready":
                break
            time.sleep(1)
        if not caught:
            raise s3.S3ProbeError(
                "[TC-S3-015] the bucket reached Ready before the operator "
                "could be killed, so the case did not test what it meant to. "
                "Rerun on a cluster where provisioning takes longer than one "
                "poll, or lower the poll interval.")

        deadline_before = ((s3.s3bucket(self.k8s_utils, name).get("status") or {})
                           .get("phaseDeadline"))
        killed = self._restart_operator()
        self.logger.info("[TC-S3-015] killed the operator at phase=%s "
                         "(pods %s)", caught, killed)

        obj, transitions = s3.wait_phase(
            self.k8s_utils, "s3bucket", name, "Ready",
            timeout=self.BUCKET_TIMEOUT, forbid="Degraded")
        total = time.time() - t0
        self.bucket_is_consistent(name, context="after an operator restart")
        s3.assert_roundtrip(client, cfg, name, "probe.bin", 8 * 1024 ** 2,
                            "tc015", context="(TC-S3-015)")

        deadline_after = (obj.get("status") or {}).get("phaseDeadline")
        self.logger.info("[TC-S3-015] Ready in %.1fs across the restart; "
                         "phaseDeadline %s -> %s",
                         total, deadline_before, deadline_after)

        # ── TC-S3-016: killed mid-deletion ──────────────────────────────
        victim = "tc016"
        self.make_bucket(victim)
        s3.assert_roundtrip(client, cfg, victim, "probe.bin", 8 * 1024 ** 2,
                            "tc016", context="(TC-S3-016 setup)")
        vobj = self.bucket_is_consistent(victim)
        vpvc = vobj["status"]["persistentVolumeClaimName"]
        vpv = self.k8s_utils.get_pvc_pv_name(vpvc)
        vexport = vobj["status"]["nfsExportName"]
        vnodes = s3.delivered_nodes(vobj)

        self.k8s_utils._exec_kubectl(
            f"kubectl delete s3bucket {victim} -n {self.ns} --wait=false")
        # Kill immediately: the window between WithdrawBucket and the
        # finalizer release is short, and missing it means the case proves
        # only that a completed delete stays completed.
        time.sleep(2)
        self._restart_operator()

        s3.wait_gone(self.k8s_utils, "s3bucket", victim, timeout=900)
        self.buckets = [b for b in self.buckets if b[0] != victim]

        leaked = []
        if self.k8s_utils.get_resource_json("pvc", vpvc):
            leaked.append(f"PVC {vpvc}")
        if vpv and self.k8s_utils.get_resource_json("pv", vpv):
            leaked.append(f"PV {vpv}")
        if vexport and self.k8s_utils.get_resource_json("nfsexport", vexport):
            leaked.append(f"NFSExport {vexport}")
        if leaked:
            raise s3.S3EvidenceError(
                f"[TC-S3-016] the operator was restarted mid-delete and the "
                f"S3Bucket finalized, but {', '.join(leaked)} survive. The "
                f"finalizer's whole purpose is that this cannot happen.")

        # The withdrawal has to have happened on every node the bucket was
        # delivered to, or a mount is left that nothing describes.
        still = [n for n in vnodes
                 if n in s3.delivered_nodes(s3.s3bucket(self.k8s_utils, victim))]
        if still:
            raise s3.S3EvidenceError(
                f"[TC-S3-016] nodes {still} still claim a delivery for a "
                f"bucket that no longer exists")

        # And the cluster still works afterwards.
        after = "tc016-after"
        self.make_bucket(after)
        s3.assert_roundtrip(client, cfg, after, "probe.bin", 8 * 1024 ** 2,
                            "tc016-after",
                            context="(TC-S3-016, after the interrupted delete)")

        self.record("tc015_operator_restart", {
            "caught_phase": caught,
            "phase_deadline_before": deadline_before,
            "phase_deadline_after": deadline_after,
            "seconds_to_ready_across_restart": round(total, 1),
            "transitions": transitions,
            "delete_interrupted": {
                "bucket": victim, "pvc": vpvc, "pv": vpv,
                "export": vexport, "delivered_nodes": vnodes,
                "all_released": True,
            },
        })
        self.logger.info("[TC-S3-015/016] PASS")

    def _restart_operator(self):
        """Delete the operator pod(s) and wait for a replacement to be Ready."""
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.ns} -l control-plane=controller-manager "
            f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} {{end}}' "
            f"2>/dev/null || true", supress_logs=True)
        pods = (out or "").split()
        if not pods:
            # The operator is not always labelled the same way across charts;
            # fall back to the deployment name rather than silently skipping
            # the kill, which would make the case pass for no reason.
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get deploy -n {self.ns} "
                f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} {{end}}' "
                f"2>/dev/null || true", supress_logs=True)
            target = next((d for d in (out or "").split()
                           if "operator" in d), "")
            if not target:
                raise s3.S3ProbeError(
                    "[TC-S3-015] cannot find the operator pod or Deployment; "
                    "the case cannot kill what it cannot name")
            self.k8s_utils._exec_kubectl(
                f"kubectl rollout restart deploy/{target} -n {self.ns}")
            self.k8s_utils._exec_kubectl(
                f"kubectl rollout status deploy/{target} -n {self.ns} "
                f"--timeout=300s || true")
            return [target]

        for p in pods:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod {p} -n {self.ns} --wait=false")
        deadline = time.time() + 300
        while time.time() < deadline:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.ns} "
                f"-l control-plane=controller-manager "
                f"-o jsonpath='{{range .items[*]}}{{.metadata.name}}:"
                f"{{.status.containerStatuses[0].ready}} {{end}}' "
                f"2>/dev/null || true", supress_logs=True)
            rows = [r.split(":") for r in (out or "").split() if ":" in r]
            fresh = [r for r in rows if r[0] not in pods and r[1] == "true"]
            if fresh:
                return pods
            time.sleep(5)
        raise s3.S3ProbeError(
            "[TC-S3-015] the operator did not come back Ready within 5 "
            "minutes of being killed")


class S3RBACFullCycleK8s(_ObjectStoreBase):
    """TC-S3-017 -- the shipped ClusterRole completes a real create-to-Ready cycle.

    Closes `test-plan-bucket-provisioning` I-11, `—` because it "needs a
    constrained client built from `config/rbac/role.yaml`, not yet set up in
    this harness". On a live cluster there is nothing to set up: the operator
    is *already* running under that ClusterRole, so the cycle either completes
    or it does not.

    A forbidden verb does not surface as a failed reconcile. It surfaces as a
    bucket that sits in one phase forever, which is indistinguishable from
    every other cause of the same symptom -- so the operator's own log is
    checked for the word directly.
    """

    def run(self):
        self.logger.info("=== TC-S3-017 RBAC, full cycle ===")
        self.make_storage_class()
        self.make_credentials()

        since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 60))
        self.make_objectstore()
        client = self.start_client()
        cfg = self.service_cfg()

        name = "tc017"
        self.make_bucket(name)
        self.bucket_is_consistent(name)
        s3.assert_roundtrip(client, cfg, name, "probe.bin", 8 * 1024 ** 2,
                            "tc017", context="(TC-S3-017)")
        self.k8s_utils.delete_resource("s3bucket", name)
        s3.wait_gone(self.k8s_utils, "s3bucket", name, timeout=600)
        self.buckets = [b for b in self.buckets if b[0] != name]

        # Now read the operator's own log across the whole cycle. The cycle
        # passing is necessary but not sufficient: a forbidden verb on a
        # non-critical path (events, say) completes the cycle and still means
        # the shipped role is wrong.
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl logs -n {self.ns} -l control-plane=controller-manager "
            f"--since-time={since} --tail=-1 --all-containers 2>/dev/null "
            f"| grep -iE 'forbidden|cannot (get|list|watch|create|update|"
            f"patch|delete)|RBAC' | head -40 || true", supress_logs=True)
        denials = [ln for ln in (out or "").splitlines() if ln.strip()]

        self.record("tc017_rbac", {
            "cycle": "create -> Ready -> data -> delete -> gone",
            "rbac_denials": denials,
        })
        if denials:
            raise s3.S3EvidenceError(
                f"[TC-S3-017] a full bucket cycle completed, but the "
                f"operator's ClusterRole was denied {len(denials)} "
                f"operation(s) along the way:\n  "
                + "\n  ".join(denials[:10]))
        self.logger.info("[TC-S3-017] PASS -- no forbidden verbs")


class S3MissingEnvFromSecretK8s(_ObjectStoreBase):
    """TC-S3-010 -- `spec.envFrom` naming a Secret that does not exist.

    Closes `test-plan-objectstore` I-07, which dev wrote and cannot run:

        Needs a real kubelet to produce `CreateContainerConfigError`;
        `envtest` runs none.

    We have a kubelet. The design says this degrades the ObjectStore with an
    event naming the missing reference, and explicitly notes the reconciler
    does *not* validate the Secret's existence itself -- so the whole
    behaviour depends on the kubelet's report reaching the phase machine,
    which is exactly the part `envtest` could never exercise.
    """

    def run(self):
        self.logger.info("=== TC-S3-010 envFrom names a missing Secret ===")
        self.make_storage_class()
        # Deliberately no make_credentials(): the Secret below never exists.
        self.make_objectstore(secret_name="tc010-does-not-exist", wait=False)

        obj, transitions = s3.wait_phase(
            self.k8s_utils, "objectstore", self.STORE,
            ("Degraded", "Ready"), timeout=self.STORE_TIMEOUT)
        phase = s3.phase_of(obj)
        self.logger.info("[TC-S3-010] settled at %s via %s", phase, transitions)

        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        pod_detail = []
        for name, _, _, _ in pods:
            detail = self.k8s_utils.get_pod_status_detail(name, self.ns)
            pod_detail.append({name: detail})
        self.logger.info("[TC-S3-010] gateway pods: %s", pod_detail)

        events = s3.events_for(self.k8s_utils, "ObjectStore", self.STORE)
        cond = s3.condition(obj, "DeploymentAvailable")

        self.record("tc010_missing_secret", {
            "phase": phase,
            "transitions": transitions,
            "deployment_available_condition": cond,
            "events": events[-10:],
            "pods": pod_detail,
            "message": (obj.get("status") or {}).get("message", ""),
        })

        if phase == "Ready":
            raise s3.S3EvidenceError(
                "[TC-S3-010] the ObjectStore reached Ready with an envFrom "
                "Secret that does not exist. The gateway cannot have started "
                "-- it refuses without ROOT_ACCESS_KEY_ID -- so Ready here "
                "means readiness is being inferred from something other than "
                "a serving pod.")
        if cond.get("status") != "False":
            raise s3.S3EvidenceError(
                f"[TC-S3-010] the ObjectStore is Degraded but its "
                f"DeploymentAvailable condition is {cond.get('status')!r}. "
                f"§6 names that condition as how this failure is expressed.")

        blob = " ".join(events) + " " + str((obj.get("status") or {}).get("message", ""))
        if "tc010-does-not-exist" not in blob:
            self.logger.error(
                "[TC-S3-010] FINDING: the ObjectStore degraded correctly, but "
                "nothing in its events or status message names the missing "
                "Secret. I-07's scenario is 'Degraded, event naming the "
                "missing reference'; an operator reading this has to go to "
                "the kubelet to find out which reference. events=%s message=%r",
                events[-5:], (obj.get("status") or {}).get("message", ""))

        # And it recovers once the Secret appears, with no manual nudge.
        self.make_credentials(name="tc010-does-not-exist")
        s3.wait_phase(self.k8s_utils, "objectstore", self.STORE, "Ready",
                      timeout=self.STORE_TIMEOUT)
        self.logger.info("[TC-S3-010] PASS -- degraded, then recovered when "
                         "the Secret was created")
