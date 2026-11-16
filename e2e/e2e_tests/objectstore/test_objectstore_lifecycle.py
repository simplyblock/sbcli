"""Bucket lifecycle, capacity and validation, driven through the CRDs.

Dev's S1 spec exercises the bucket lifecycle through the *S3 API*. Nobody
drives the `S3Bucket` object itself on a live cluster and watches the chain it
expands into -- a PVC, an lvol, an NFSExport, and a mount delivered to every
gateway node -- or watches that chain come apart again on delete. That is this
file.

The delete half is the part worth having. Dev's own `waitForBucketGone` cites
`issues/02` as the case where the backing volume is never released, and a
surviving PV is invisible from the S3 API: `ListBuckets` is perfectly happy
about it.
"""
import time

from e2e_tests.objectstore.objectstore_base import _ObjectStoreBase
from utils import s3_utils as s3


class S3BucketLifecycleK8s(_ObjectStoreBase):
    """TC-S3-001 -- the CRD lifecycle, and the volume actually released.

    Closes `design-bucket-provisioning` E-01 and E-03, both `—`.
    """

    def run(self):
        self.logger.info("=== TC-S3-001 bucket lifecycle through the CRD ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()
        pod = self.start_client()
        cfg = self.service_cfg()

        name = "tc001"
        t0 = time.time()
        self.make_bucket(name, wait=False)
        obj, transitions = s3.wait_phase(
            self.k8s_utils, "s3bucket", name, "Ready",
            timeout=self.BUCKET_TIMEOUT, forbid="Degraded")
        provisioned = time.time() - t0
        self.logger.info("[TC-S3-001] Ready in %.1fs via %s",
                         provisioned, transitions)

        # The phase machine. Pending and Provisioning are both allowed to be
        # missed -- a fast cluster can go Pending->Ready between two polls --
        # but Degraded must never appear, which wait_phase already enforced.
        seen = [p for _, p in transitions]
        if "Ready" not in seen:
            raise s3.S3EvidenceError(
                f"[TC-S3-001] never observed Ready in {seen}")
        for ph in seen:
            if ph not in s3.BUCKET_PHASES:
                raise s3.S3EvidenceError(
                    f"[TC-S3-001] phase {ph!r} is not one of {s3.BUCKET_PHASES}")

        # The chain it expanded into.
        self.bucket_is_consistent(name, context="right after Ready")
        pvc = (obj["status"] or {}).get("persistentVolumeClaimName")
        export = (obj["status"] or {}).get("nfsExportName")
        pvc_obj = self.k8s_utils.get_pvc_status(pvc)
        if (pvc_obj.get("status") or {}).get("phase") != "Bound":
            raise s3.S3EvidenceError(
                f"[TC-S3-001] PVC {pvc} is {(pvc_obj.get('status') or {}).get('phase')!r}, "
                f"not Bound, while the bucket says Ready")
        pv = self.k8s_utils.get_pvc_pv_name(pvc)

        # The export must reference the bound PV, not merely exist: a bucket
        # Ready against somebody else's export would read identically here.
        export_obj = self.k8s_utils.get_resource_json("nfsexport", export)
        handle = self.k8s_utils.get_pvc_volume_handle(pvc)
        ref = ((export_obj.get("spec") or {}).get("volumeRef")
               or (export_obj.get("spec") or {}).get("volumeID") or "")
        if handle and handle not in str(ref):
            raise s3.S3EvidenceError(
                f"[TC-S3-001] NFSExport/{export} references {ref!r}, which is "
                f"not the bucket's own volume handle {handle!r}")

        # The one condition set the design names.
        for ctype in s3.BUCKET_CONDITIONS:
            cond = s3.condition(obj, ctype)
            if cond.get("status") != "True":
                raise s3.S3EvidenceError(
                    f"[TC-S3-001] condition {ctype} is "
                    f"{cond.get('status')!r} on a Ready bucket "
                    f"(reason={cond.get('reason')!r}, "
                    f"message={cond.get('message')!r})")

        reasons = [ln.split()[0] for ln in s3.events_for(
            self.k8s_utils, "S3Bucket", name) if ln.split()]
        if s3.EVENT_BUCKET_READY not in reasons:
            raise s3.S3EvidenceError(
                f"[TC-S3-001] no {s3.EVENT_BUCKET_READY} event on the bucket; "
                f"saw {sorted(set(reasons))}")

        # The data path, 64 MiB, byte for byte.
        s3.assert_roundtrip(pod, cfg, name, "tc001/payload.bin",
                            64 * 1024 ** 2, "tc001-seed",
                            context="(TC-S3-001)")
        listed = pod.call(cfg, "list_objects", bucket=name)
        if "tc001/payload.bin" not in (listed.get("keys") or []):
            raise s3.S3EvidenceError(
                f"[TC-S3-001] the object is readable but ListObjectsV2 does "
                f"not show it: {listed.get('keys')}")

        # ── delete, and the release ──────────────────────────────────────
        self.k8s_utils.delete_resource("s3bucket", name)
        s3.wait_gone(self.k8s_utils, "s3bucket", name, timeout=600)
        self.buckets = [b for b in self.buckets if b[0] != name]

        leaked = []
        if self.k8s_utils.get_resource_json("pvc", pvc):
            leaked.append(f"PVC {pvc}")
        if pv and self.k8s_utils.get_resource_json("pv", pv):
            leaked.append(f"PV {pv}")
        if export and self.k8s_utils.get_resource_json("nfsexport", export):
            leaked.append(f"NFSExport {export}")
        if leaked:
            raise s3.S3EvidenceError(
                f"[TC-S3-001] the S3Bucket is gone but its backing objects "
                f"are not: {', '.join(leaked)}. This is the issues/02 "
                f"signature -- the volume is never released, and nothing in "
                f"the S3 API shows it.")

        self.record("tc001_lifecycle", {
            "provisioning_seconds": round(provisioned, 1),
            "transitions": transitions,
            "pvc": pvc, "pv": pv, "nfsexport": export,
            "delivered_nodes": s3.delivered_nodes(obj),
        })
        self.logger.info("[TC-S3-001] PASS")


class S3ObjectStoreDeletedWithBucketsK8s(_ObjectStoreBase):
    """TC-S3-002 -- `ObjectStore` deleted while buckets still reference it.

    Closes the live half of `test-plan-objectstore` I-04, which `envtest`
    cannot cover for want of a garbage collector, and the invariant each of
    dev's two plans assigns to the other.

    The design does not state which of two behaviours is correct, so this case
    *records* which one happened and fails only on the outcomes that are
    unambiguously wrong: a bucket left `Ready` with no gateway, or an orphaned
    PVC. If neither behaviour is specified, that is the finding.
    """

    def run(self):
        self.logger.info("=== TC-S3-002 ObjectStore deleted with live buckets ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()
        pod = self.start_client()
        cfg = self.service_cfg()

        names = ["tc002a", "tc002b"]
        for n in names:
            self.make_bucket(n)
            s3.assert_roundtrip(pod, cfg, n, "probe.bin", 4 * 1024 ** 2,
                                f"{n}-seed", context="(TC-S3-002 setup)")

        pvcs = {n: self.bucket_is_consistent(n)["status"]
                ["persistentVolumeClaimName"] for n in names}

        # What the ObjectStore owns, before.
        owned = {}
        for kind in ("deployment", "service", "certificate"):
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get {kind} -n {self.ns} "
                f"-o jsonpath='{{range .items[?(@.metadata.ownerReferences[0].name==\"{self.STORE}\")]}}"
                f"{{.metadata.name}} {{end}}' 2>/dev/null || true",
                supress_logs=True)
            owned[kind] = sorted((out or "").split())
        self.logger.info("[TC-S3-002] owned before delete: %s", owned)

        self.k8s_utils._exec_kubectl(
            f"kubectl delete objectstore {self.STORE} -n {self.ns} "
            f"--wait=false --ignore-not-found")

        # Five minutes, sampling, so the report can say what happened over the
        # window rather than only at its end.
        timeline = []
        deadline = time.time() + 300
        while time.time() < deadline:
            store = s3.objectstore(self.k8s_utils, self.STORE)
            row = {
                "t": round(time.time() - (deadline - 300)),
                "store_present": bool(store),
                "store_phase": s3.phase_of(store),
                "buckets": {n: s3.phase_of(s3.s3bucket(self.k8s_utils, n))
                            for n in names},
                "gateway_pods": len(s3.gateway_pods(self.k8s_utils, self.STORE)),
            }
            if not timeline or row != {**timeline[-1], "t": row["t"]}:
                timeline.append(row)
            time.sleep(15)

        store_gone = not s3.objectstore(self.k8s_utils, self.STORE)
        final = {n: s3.s3bucket(self.k8s_utils, n) for n in names}
        gateways = s3.gateway_pods(self.k8s_utils, self.STORE)

        if store_gone:
            outcome = "delete proceeded"
            # The serious failure: a bucket still claiming Ready with nothing
            # to serve it. Degraded is a defensible answer; Ready is not.
            for n, obj in final.items():
                if s3.phase_of(obj) == "Ready" and not gateways:
                    raise s3.S3EvidenceError(
                        f"[TC-S3-002] S3Bucket/{n} is still Ready after its "
                        f"ObjectStore was deleted and every gateway pod is "
                        f"gone. Nothing serves it, and nothing says so.")
            reasons = {n: [ln.split()[0] for ln in s3.events_for(
                self.k8s_utils, "S3Bucket", n) if ln.split()] for n in final}
            self.logger.info("[TC-S3-002] bucket events after delete: %s", reasons)
            # The live half of I-04: owned objects actually collected.
            still = {}
            for kind, items in owned.items():
                left = [i for i in items
                        if self.k8s_utils.get_resource_json(kind, i)]
                if left:
                    still[kind] = left
            if still:
                raise s3.S3EvidenceError(
                    f"[TC-S3-002] the ObjectStore is gone but objects it "
                    f"owned were not garbage-collected: {still}. "
                    f"ownerReferences are the only mechanism here, so this is "
                    f"the live half of I-04 failing.")
        else:
            outcome = "delete held"
            store = s3.objectstore(self.k8s_utils, self.STORE)
            finalizers = (store.get("metadata") or {}).get("finalizers") or []
            if not finalizers:
                raise s3.S3EvidenceError(
                    "[TC-S3-002] the ObjectStore delete neither completed nor "
                    "is held by a finalizer -- it is simply stuck, which is "
                    "neither of the two defensible behaviours.")
            self.logger.info("[TC-S3-002] held by finalizers %s", finalizers)

        # Whichever happened, no PVC may be orphaned.
        orphans = [p for p in pvcs.values()
                   if self.k8s_utils.get_resource_json("pvc", p)
                   and not s3.s3bucket(self.k8s_utils,
                                       next(n for n, q in pvcs.items() if q == p))]
        if orphans:
            raise s3.S3EvidenceError(
                f"[TC-S3-002] PVC(s) {orphans} survive with no S3Bucket "
                f"describing them")

        self.record("tc002_objectstore_delete", {
            "outcome": outcome, "owned_before": owned,
            "timeline": timeline,
            "final_phases": {n: s3.phase_of(o) for n, o in final.items()},
        })
        self.logger.info("[TC-S3-002] PASS -- %s", outcome)


class S3BucketCapacityExhaustionK8s(_ObjectStoreBase):
    """TC-S3-003 -- a bucket filled past its capacity.

    In no dev plan at any level. The part worth the case is the *second*
    bucket: every bucket on an ObjectStore shares one gateway process and one
    hostPath root, so one full bucket taking the gateway down would be a
    single-tenant failure mode in a multi-tenant component.
    """

    #: Deliberately small. The case is about the behaviour at the limit, and a
    #: 2 GiB bucket takes long enough to fill as it is.
    FILL_BYTES = 1024 ** 3

    def run(self):
        self.logger.info("=== TC-S3-003 capacity exhaustion ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()
        pod = self.start_client()
        cfg = self.service_cfg()

        victim, bystander = "tc003", "tc003-bystander"
        self.make_bucket(victim, capacity=self.FILL_BYTES)
        self.make_bucket(bystander, capacity=self.FILL_BYTES)

        # Objects written *before* the limit, which must survive it.
        pre = [("pre/%02d.bin" % i, 16 * 1024 ** 2) for i in range(4)]
        for key, size in pre:
            s3.assert_roundtrip(pod, cfg, victim, key, size, "tc003-pre",
                                context="(TC-S3-003 pre-fill)")

        # Fill. 64 MiB at a time, more objects than the bucket can hold.
        chunk = 64 * 1024 ** 2
        result = pod.call(cfg, "fill", bucket=victim, prefix="fill/",
                          count=int(self.FILL_BYTES / chunk) + 12,
                          size=chunk, seed="tc003-fill", timeout=3600)
        self.logger.info("[TC-S3-003] fill stopped after %d object(s): %s",
                         result.get("written_count"),
                         {k: result.get(k) for k in
                          ("status", "code", "stopped_at", "exception")})

        if "stopped_at" not in result:
            raise s3.S3EvidenceError(
                f"[TC-S3-003] wrote {result.get('written_count')} x 64 MiB "
                f"into a {self.FILL_BYTES} byte bucket without a single "
                f"refusal. capacityBytes is not bounding anything.")
        if result.get("exception"):
            raise s3.S3EvidenceError(
                f"[TC-S3-003] the over-capacity PutObject did not fail, it "
                f"broke the connection: {result['exception']}. A hang or a "
                f"reset is worse than any error code.")

        code = result.get("code") or ""
        status = result.get("status")
        # Record what actually came back. 507 InsufficientStorage or 503 are
        # sane; a 500 with no code is the finding, and is reported as one
        # rather than silently accepted.
        sane = status in (403, 503, 507) and code
        if not sane:
            self.logger.error(
                "[TC-S3-003] FINDING: over-capacity PutObject answered "
                "status=%s code=%r body=%r -- not a usable S3 error",
                status, code, (result.get("body") or "")[:300])

        # Everything written before the limit still reads back.
        for key, size in pre:
            got = pod.call(cfg, "get_object", bucket=victim, key=key,
                           size=size, seed="tc003-pre")
            if not got.get("match"):
                raise s3.S3EvidenceError(
                    f"[TC-S3-003] {victim}/{key} written before the limit no "
                    f"longer reads back: status={got.get('status')} "
                    f"match={got.get('match')} first_diff={got.get('first_diff')}")

        # The bystander. This is the assertion the case exists for.
        s3.assert_roundtrip(pod, cfg, bystander, "after.bin", 32 * 1024 ** 2,
                            "tc003-bystander",
                            context="(TC-S3-003, after the other bucket filled)")
        if s3.phase_of(s3.s3bucket(self.k8s_utils, bystander)) != "Ready":
            raise s3.S3EvidenceError(
                "[TC-S3-003] filling one bucket left the other one not Ready")

        self.record("tc003_capacity", {
            "capacity_bytes": self.FILL_BYTES,
            "chunk_bytes": chunk,
            "objects_written": result.get("written_count"),
            "refusal_status": status, "refusal_code": code,
            "refusal_body": (result.get("body") or "")[:600],
            "sane_s3_error": bool(sane),
        })
        if not sane:
            raise s3.S3EvidenceError(
                f"[TC-S3-003] the bucket bounded correctly and the bystander "
                f"survived, but the refusal was status={status} code={code!r}, "
                f"which no S3 client can act on. Expected InsufficientStorage "
                f"or ServiceUnavailable with a code.")
        self.logger.info("[TC-S3-003] PASS -- refused with %s/%s", status, code)


class S3BucketCapacityPatchK8s(_ObjectStoreBase):
    """TC-S3-004 -- `capacityBytes` grows, shrinks are refused, rest immutable.

    The CRD states both behaviours and neither is tested anywhere:

        "An increase reconciles to a PVC expansion. A decrease is rejected,
        the same restriction the PVC it wraps already carries."

    `objectStoreRef`, `bucketName` and `storageClassName` carry `+k8s:immutable`.
    That marker does not by itself generate a CEL rule, so whether it is
    *enforced* is exactly what this case establishes rather than assumes.
    """

    def run(self):
        self.logger.info("=== TC-S3-004 capacityBytes and the immutables ===")
        self.make_storage_class(expansion=True)
        self.make_credentials()
        self.make_objectstore()
        pod = self.start_client()
        cfg = self.service_cfg()

        name = "tc004"
        start = 1024 ** 3
        self.make_bucket(name, capacity=start)
        s3.assert_roundtrip(pod, cfg, name, "before-grow.bin", 32 * 1024 ** 2,
                            "tc004", context="(TC-S3-004 pre-grow)")
        pvc = self.bucket_is_consistent(name)["status"]["persistentVolumeClaimName"]

        # ── grow ────────────────────────────────────────────────────────
        grown = 3 * 1024 ** 3
        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl patch s3bucket {name} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"capacityBytes\":{grown}}}}}'")
        if "patched" not in (out or ""):
            raise s3.S3EvidenceError(
                f"[TC-S3-004] growing capacityBytes was refused: "
                f"{(err or out or '')[-400:]}")

        deadline = time.time() + 600
        reached = None
        while time.time() < deadline:
            req = ((self.k8s_utils.get_pvc_status(pvc).get("spec") or {})
                   .get("resources", {}).get("requests", {}).get("storage", ""))
            if req and self._bytes(req) >= grown:
                reached = req
                break
            time.sleep(10)
        if not reached:
            raise s3.S3EvidenceError(
                f"[TC-S3-004] 10 minutes after capacityBytes went to {grown}, "
                f"PVC {pvc} still requests "
                f"{(self.k8s_utils.get_pvc_status(pvc).get('spec') or {})}")
        self.logger.info("[TC-S3-004] PVC expanded to %s", reached)

        if s3.phase_of(s3.s3bucket(self.k8s_utils, name)) != "Ready":
            raise s3.S3EvidenceError(
                "[TC-S3-004] the bucket left Ready during its own expansion")
        got = pod.call(cfg, "get_object", bucket=name, key="before-grow.bin",
                       size=32 * 1024 ** 2, seed="tc004")
        if not got.get("match"):
            raise s3.S3EvidenceError(
                f"[TC-S3-004] data written before the expansion no longer "
                f"reads back: {got}")
        s3.assert_roundtrip(pod, cfg, name, "after-grow.bin", 64 * 1024 ** 2,
                            "tc004-grown", context="(TC-S3-004 post-grow)")

        # ── shrink ──────────────────────────────────────────────────────
        shrunk = 2 * 1024 ** 3
        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl patch s3bucket {name} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"capacityBytes\":{shrunk}}}}}' "
            f"2>&1 || true")
        blob = (out or "") + (err or "")
        refused_at_admission = "patched" not in blob
        if refused_at_admission:
            self.logger.info("[TC-S3-004] shrink refused at admission: %s",
                             blob.strip()[-300:])
        else:
            # Accepted by the API. The reconciler must then refuse it, by
            # degrading or by leaving the PVC alone and saying why. A silent
            # accept is the finding.
            time.sleep(60)
            obj = s3.s3bucket(self.k8s_utils, name)
            req = ((self.k8s_utils.get_pvc_status(pvc).get("spec") or {})
                   .get("resources", {}).get("requests", {}).get("storage", ""))
            degraded = s3.phase_of(obj) == "Degraded"
            pvc_held = self._bytes(req) >= grown
            if not degraded and not pvc_held:
                raise s3.S3EvidenceError(
                    f"[TC-S3-004] a capacityBytes DECREASE was accepted and "
                    f"acted on: PVC now requests {req!r}. The CRD says a "
                    f"decrease is rejected.")
            if not degraded:
                self.logger.warning(
                    "[TC-S3-004] the shrink was admitted and silently ignored "
                    "(PVC still %s, bucket still %s). The volume is safe, but "
                    "spec and reality now disagree with nothing saying so.",
                    req, s3.phase_of(obj))

        # ── the immutables ──────────────────────────────────────────────
        immutable = {}
        for field, value in (("objectStoreRef", "somewhere-else"),
                             ("bucketName", "tc004-renamed"),
                             ("storageClassName", "some-other-sc")):
            out, err = self.k8s_utils._exec_kubectl(
                f"kubectl patch s3bucket {name} -n {self.ns} --type=merge "
                f"-p '{{\"spec\":{{\"{field}\":\"{value}\"}}}}' 2>&1 || true")
            blob = (out or "") + (err or "")
            immutable[field] = "refused" if "patched" not in blob else "ACCEPTED"
            if immutable[field] == "ACCEPTED":
                # Put it back before anything else runs against this bucket.
                self.k8s_utils._exec_kubectl(
                    f"kubectl patch s3bucket {name} -n {self.ns} --type=merge "
                    f"-p '{{\"spec\":{{\"{field}\":\"{{}}\"}}}}' 2>&1 || true")
        self.logger.info("[TC-S3-004] immutability: %s", immutable)

        self.record("tc004_capacity_patch", {
            "start_bytes": start, "grown_bytes": grown,
            "pvc_after_grow": reached,
            "shrink_refused_at_admission": refused_at_admission,
            "immutable_fields": immutable,
        })

        accepted = [f for f, r in immutable.items() if r == "ACCEPTED"]
        if accepted:
            raise s3.S3EvidenceError(
                f"[TC-S3-004] field(s) {accepted} carry +k8s:immutable in the "
                f"CRD but a patch changing them was accepted. The marker is "
                f"not generating a validation rule.")
        self.logger.info("[TC-S3-004] PASS")

    @staticmethod
    def _bytes(quantity):
        """A Kubernetes quantity as bytes. Returns 0 on anything unparseable."""
        text = str(quantity or "").strip()
        units = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4,
                 "K": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9, "T": 10 ** 12}
        for suffix, mult in sorted(units.items(), key=lambda kv: -len(kv[0])):
            if text.endswith(suffix):
                try:
                    return int(float(text[:-len(suffix)]) * mult)
                except ValueError:
                    return 0
        try:
            return int(text)
        except ValueError:
            return 0


class S3BucketNameBoundaryK8s(_ObjectStoreBase):
    """TC-S3-023 -- bucket names at the edges of two disagreeing grammars.

    Dev covers the deterministic PVC naming as unit tests (U-17, U-18) with a
    fake client. Those prove the function; they cannot prove that the name it
    produces is one the API server accepts, that two such buckets coexist, or
    that the S3 API answers to the original name.

    The two grammars disagree on purpose: `bucketName` is validated against
    the S3 grammar (`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`), which allows dots,
    while a DNS label -- which the PVC name must be -- does not.
    """

    def run(self):
        self.logger.info("=== TC-S3-023 bucket-name boundaries ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()
        pod = self.start_client()
        cfg = self.service_cfg()

        # 63 characters exactly: the S3 maximum, and the DNS label maximum,
        # which is where the two grammars stop agreeing about length too.
        longest = "a" + ("b" * 61) + "c"
        if not s3.BUCKET_NAME_RE.match(longest) or len(longest) != 63:
            raise s3.S3ProbeError(
                f"[TC-S3-023] the harness built a {len(longest)}-char name "
                f"that does not match the mirrored pattern -- fix the test, "
                f"not the product")

        # Two names a DNS label cannot distinguish between, but S3 can.
        dotted = "tc023.alpha.beta"
        dashed = "tc023-alpha-beta"

        cases = {
            "longest": longest,
            "dotted": dotted,
            "dashed": dashed,
        }
        created = {}
        for label, bucket_name in cases.items():
            # The CR's own name must be a DNS label, so it cannot be the
            # bucket name in the dotted case. That is the whole point.
            cr = f"tc023-{label}"
            self.make_bucket(cr, bucket_name=bucket_name)
            created[label] = {
                "cr": cr, "bucketName": bucket_name,
                "pvc": self.bucket_is_consistent(cr)["status"]
                       ["persistentVolumeClaimName"],
            }

        pvcs = [c["pvc"] for c in created.values()]
        if len(set(pvcs)) != len(pvcs):
            raise s3.S3EvidenceError(
                f"[TC-S3-023] two buckets share a backing PVC: {created}. "
                f"This is U-18's collision, live.")

        # Each answers to its own name over S3, and holds its own bytes.
        for label, info in created.items():
            s3.assert_roundtrip(
                pod, cfg, info["bucketName"], "probe.bin", 8 * 1024 ** 2,
                f"tc023-{label}", context=f"(TC-S3-023 {label})")
        listed = pod.call(cfg, "list_buckets")
        missing = [i["bucketName"] for i in created.values()
                   if i["bucketName"] not in (listed.get("buckets") or [])]
        if missing:
            raise s3.S3EvidenceError(
                f"[TC-S3-023] ListBuckets does not show {missing}; it showed "
                f"{listed.get('buckets')}")

        # And a name the grammar forbids is refused, rather than accepted and
        # failing later somewhere less obvious.
        for bad in ("UPPERCASE", "has_underscore", "ab", "-leading-dash"):
            out, err = self.k8s_utils._exec_kubectl(
                "echo 'apiVersion: storage.simplyblock.io/v1alpha2\n"
                "kind: S3Bucket\nmetadata:\n  name: tc023-bad\nspec:\n"
                f"  objectStoreRef: {self.STORE}\n"
                f"  bucketName: \"{bad}\"\n"
                f"  capacityBytes: {self.BUCKET_BYTES}\n"
                f"  storageClassName: {self.SC_NAME}' "
                f"| kubectl apply -n {self.ns} -f - 2>&1 || true")
            blob = (out or "") + (err or "")
            if "created" in blob or "configured" in blob:
                self.k8s_utils.delete_resource("s3bucket", "tc023-bad")
                raise s3.S3EvidenceError(
                    f"[TC-S3-023] bucketName {bad!r} violates the CRD pattern "
                    f"{s3.BUCKET_NAME_RE.pattern} but was accepted")

        self.record("tc023_name_boundaries", created)
        self.logger.info("[TC-S3-023] PASS")


class S3ObjectStoreReplicasZeroK8s(_ObjectStoreBase):
    """TC-S3-019 -- `replicas: 0` is refused by validation.

    `test-plan-objectstore` §5 lists replica count 0 under "not covered",
    reasoning that it is "rejected by CRD validation, not a runtime scenario".
    That reasoning is only sound if the rejection actually happens, which no
    test anywhere checks. The CRD carries `+kubebuilder:validation:Minimum=1`.

    A gateway scaled to zero is not a harmless no-op: every bucket on it would
    have its mounts withdrawn from every node (U-14), which is a data-path
    outage produced by a field edit.
    """

    def run(self):
        self.logger.info("=== TC-S3-019 replicas: 0 ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore()

        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":0}}}}' 2>&1 || true")
        blob = (out or "") + (err or "")
        if "patched" in blob:
            # Put it back before asserting, so a failure here does not also
            # leave the lab with a dead gateway.
            self.k8s_utils._exec_kubectl(
                f"kubectl patch objectstore {self.STORE} -n {self.ns} "
                f"--type=merge -p '{{\"spec\":{{\"replicas\":1}}}}'")
            raise s3.S3EvidenceError(
                "[TC-S3-019] replicas: 0 was accepted. The CRD declares "
                "Minimum=1 and test-plan-objectstore §5 excludes this case on "
                "the grounds that validation rejects it -- it does not.")
        self.logger.info("[TC-S3-019] refused: %s", blob.strip()[-300:])

        # Negative numbers too, which the same bound covers.
        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":-1}}}}' 2>&1 || true")
        blob = (out or "") + (err or "")
        if "patched" in blob:
            self.k8s_utils._exec_kubectl(
                f"kubectl patch objectstore {self.STORE} -n {self.ns} "
                f"--type=merge -p '{{\"spec\":{{\"replicas\":1}}}}'")
            raise s3.S3EvidenceError(
                "[TC-S3-019] replicas: -1 was accepted")

        # And the port bounds, from the same validation block (U-13 proves the
        # builder handles 1 and 65535; nothing proves the CRD rejects 0 and
        # 65536).
        ports = {}
        for port in (0, 65536):
            out, err = self.k8s_utils._exec_kubectl(
                f"kubectl patch objectstore {self.STORE} -n {self.ns} "
                f"--type=merge -p '{{\"spec\":{{\"port\":{port}}}}}' 2>&1 || true")
            blob = (out or "") + (err or "")
            ports[port] = "refused" if "patched" not in blob else "ACCEPTED"
            if ports[port] == "ACCEPTED":
                self.k8s_utils._exec_kubectl(
                    f"kubectl patch objectstore {self.STORE} -n {self.ns} "
                    f"--type=merge -p '{{\"spec\":{{\"port\":{self.GATEWAY_PORT}}}}}'")

        self.record("tc019_validation", {"replicas_0": "refused",
                                         "replicas_-1": "refused",
                                         "ports": ports})
        bad = [p for p, r in ports.items() if r == "ACCEPTED"]
        if bad:
            raise s3.S3EvidenceError(
                f"[TC-S3-019] port(s) {bad} are outside the CRD's declared "
                f"1-65535 range but were accepted")
        self.logger.info("[TC-S3-019] PASS")
