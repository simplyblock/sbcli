"""The gateway: TLS, scaling, and bucket delivery to nodes.

Everything here is a live-cluster half of something dev proved against a fake.
`buildDeployment` is unit-tested thoroughly and `diffDeliveryNodes` has six
unit scenarios, but the claim the whole bucket-delivery design rests on --
that a bucket already `Ready` becomes visible on a node a gateway pod *later*
lands on, with no pod restart and no bucket reconcile -- has never run against
a real CSI node plugin. `test-plan-bucket-provisioning` E-01 to E-04 are all
`—`, and `test-plan-objectstore` E-01 to E-03 are all `—`.

The gateway is always addressed per pod, never through the Service. A
ClusterIP load-balances, and a load balancer in front of three replicas is
exactly what hides the one that cannot see a bucket.
"""
import threading
import time

from e2e_tests.objectstore.objectstore_base import _ObjectStoreBase
from utils import s3_utils as s3


class S3GatewayScaleOutK8s(_ObjectStoreBase):
    """TC-S3-005 -- gateway 1 -> 3, pre-existing buckets delivered to the new nodes.

    Closes the live half of `test-plan-objectstore` U-18 and the
    `s3bucket_delivery.go` claim that delivery runs on every pass "independent
    of the phase machine". Also covers `test-plan-bucket-provisioning` E-02.
    """

    BUCKETS = ("tc005a", "tc005b", "tc005c")

    def run(self):
        self.logger.info("=== TC-S3-005 gateway scale-out and delivery ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        if len(nodes) < 3:
            raise s3.S3ProbeError(
                f"[TC-S3-005] needs three schedulable nodes for a required "
                f"hostname anti-affinity to be satisfiable; this cluster has "
                f"{len(nodes)}: {nodes}")

        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=1)
        pod = self.start_client()

        for b in self.BUCKETS:
            self.make_bucket(b)
            s3.assert_roundtrip(pod, self.service_cfg(), b, "seed.bin",
                                16 * 1024 ** 2, f"{b}-seed",
                                context="(TC-S3-005 setup)")

        before_metrics = s3.scrape_operator_metrics(self.k8s_utils)
        before_deliveries = s3.metric_total(
            before_metrics, s3.METRIC_DELIVERY_CALLS,
            operation="DeliverBucket", outcome="success")
        start_nodes = s3.gateway_nodes(self.k8s_utils, self.STORE)
        self.logger.info("[TC-S3-005] starting on %s, %d successful deliveries "
                         "so far", start_nodes, before_deliveries)

        # ── scale out ───────────────────────────────────────────────────
        self.undo_stack.append(lambda: self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":1}}}}' || true"))
        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":3}}}}'")
        pods = s3.wait_gateway_pods(self.k8s_utils, self.STORE, 3, timeout=600)

        # U-18, live: a *required* hostname anti-affinity with no preferred
        # rule alongside it. Three pods on two nodes would still serve, and
        # would still pass every data assertion below -- so it is checked
        # directly rather than inferred.
        placed = [p[1] for p in pods]
        if len(set(placed)) != 3:
            raise s3.S3EvidenceError(
                f"[TC-S3-005] three gateway pods landed on "
                f"{len(set(placed))} node(s): {placed}. The pod template is "
                f"documented (U-18) to carry a REQUIRED PodAntiAffinity on "
                f"kubernetes.io/hostname.")

        # ── every bucket delivered to every node ────────────────────────
        want = set(placed)
        deadline = time.time() + 600
        pending = set(self.BUCKETS)
        while pending and time.time() < deadline:
            for b in list(pending):
                if set(s3.delivered_nodes(s3.s3bucket(self.k8s_utils, b))) >= want:
                    pending.discard(b)
            if pending:
                time.sleep(10)
        if pending:
            detail = {b: s3.delivered_nodes(s3.s3bucket(self.k8s_utils, b))
                      for b in pending}
            raise s3.S3EvidenceError(
                f"[TC-S3-005] buckets {sorted(pending)} were never delivered "
                f"to the new gateway nodes {sorted(want)}: {detail}. Delivery "
                f"is documented to follow pod placement independently of the "
                f"phase machine, so a Ready bucket is not exempt.")

        # ── the assertion the case exists for ───────────────────────────
        # Every bucket, through every pod, byte for byte. Through the Service
        # this would pass with one broken replica out of three.
        for name, node, ip, _ready in pods:
            cfg = self.pod_cfg(ip)
            for b in self.BUCKETS:
                got = pod.call(cfg, "get_object", bucket=b, key="seed.bin",
                               size=16 * 1024 ** 2, seed=f"{b}-seed")
                if got.get("status") != 200 or not got.get("match"):
                    raise s3.S3EvidenceError(
                        f"[TC-S3-005] gateway pod {name} on {node} cannot "
                        f"serve bucket {b}: status={got.get('status')} "
                        f"code={got.get('code')!r} match={got.get('match')}. "
                        f"status.mountDeliveredNodes claims it was delivered.")
            # And writable, not only readable: a delivered mount that is
            # read-only would pass every GET above.
            s3.assert_roundtrip(pod, cfg, self.BUCKETS[0], f"via-{node}.bin",
                                4 * 1024 ** 2, f"tc005-{node}",
                                context=f"(TC-S3-005 through {name})")

        after_metrics = s3.scrape_operator_metrics(self.k8s_utils)
        after_deliveries = s3.metric_total(
            after_metrics, s3.METRIC_DELIVERY_CALLS,
            operation="DeliverBucket", outcome="success")
        failures = s3.metric_total(after_metrics, s3.METRIC_DELIVERY_CALLS,
                                   outcome="failure")
        expected = len(self.BUCKETS) * len(want - set(start_nodes))
        self.logger.info(
            "[TC-S3-005] deliveries %s -> %s (expected +%d), failures %s",
            before_deliveries, after_deliveries, expected, failures)
        if failures:
            raise s3.S3EvidenceError(
                f"[TC-S3-005] {failures} mount-delivery call(s) reported "
                f"failure while every bucket still became readable. A silent "
                f"retry that eventually works still means the first attempt "
                f"did not.")

        # ── scale back, and the withdrawal ──────────────────────────────
        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":1}}}}'")
        remaining = s3.wait_gateway_pods(self.k8s_utils, self.STORE, 1,
                                         timeout=600)
        keep = {remaining[0][1]}
        deadline = time.time() + 600
        while time.time() < deadline:
            shrunk = {b: set(s3.delivered_nodes(s3.s3bucket(self.k8s_utils, b)))
                      for b in self.BUCKETS}
            if all(v == keep for v in shrunk.values()):
                break
            time.sleep(10)
        else:
            raise s3.S3EvidenceError(
                f"[TC-S3-005] after scaling back to one pod on {keep}, "
                f"mountDeliveredNodes still reads {shrunk}. WithdrawBucket is "
                f"documented to run for every removed node; a stale entry "
                f"makes status a record of where a mount used to be.")

        self.record("tc005_scale_out", {
            "start_nodes": start_nodes, "scaled_nodes": sorted(want),
            "deliveries_before": before_deliveries,
            "deliveries_after": after_deliveries,
            "expected_increase": expected,
            "delivery_failures": failures,
        })
        self.logger.info("[TC-S3-005] PASS")


class S3GatewayTLSK8s(_ObjectStoreBase):
    """TC-S3-007/008/009 -- a real certificate, really issued, really served.

    Closes `test-plan-objectstore` E-01, E-02 and E-03, all three `—`. The
    integration tests set `Certificate.status.conditions` by hand because
    `envtest` runs neither cert-manager nor the service-ca-operator, so the
    entire claim that a certificate is issued, lands in the Secret the pod
    mounts, and is the one served is unproven below this level.

    Which provider runs is the cluster's choice, not the case's: OpenShift
    relies on the service-ca-operator, which a plain Kubernetes cluster has
    nobody to run, and cert-manager needs a ClusterIssuer that may not exist.
    Both paths are asserted identically once the provider is settled.
    """

    def run(self):
        self.logger.info("=== TC-S3-007/008/009 gateway TLS ===")
        self.make_storage_class()
        self.make_credentials()

        # Resolved from the chart's CA bundle, not from detect_openshift():
        # being on OpenShift does not mean the chart was installed with
        # tls.provider=openshift, and asserting the wrong half of this branch
        # is how the case would pass for the wrong reason. See
        # ObjectStoreTestBase.resolve_tls_provider.
        provider = self.resolve_tls_provider()
        self.logger.info("[TC-S3-00x] cluster selects tlsProvider=%s", provider)
        self.make_objectstore(tls_provider=provider)

        # ── the provider did its half ───────────────────────────────────
        if provider == "cert-manager":
            # E-01: a Certificate object exists, is Ready, and names a Secret.
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get certificate -n {self.ns} "
                f"-o jsonpath='{{range .items[?(@.metadata.ownerReferences[0].name==\"{self.STORE}\")]}}"
                f"{{.metadata.name}} {{.spec.secretName}} "
                f"{{.status.conditions[?(@.type==\"Ready\")].status}}{{\"\\n\"}}{{end}}' "
                f"2>/dev/null || true", supress_logs=True)
            rows = [ln.split() for ln in (out or "").splitlines() if ln.split()]
            if not rows:
                raise s3.S3EvidenceError(
                    "[TC-S3-007] tlsProvider=cert-manager but the ObjectStore "
                    "owns no Certificate. U-15 proves the builder is called; "
                    "nothing proved the object reaches the cluster.")
            cert, secret_name = rows[0][0], rows[0][1]
            if len(rows[0]) < 3 or rows[0][2] != "True":
                raise s3.S3EvidenceError(
                    f"[TC-S3-007] Certificate/{cert} is not Ready "
                    f"({rows[0][2:] or 'no Ready condition'}). The gateway "
                    f"cannot serve TLS without it.")
        else:
            # E-02: the Service carries the serving-cert annotation and the
            # service-ca-operator populated the Secret it names.
            svc = self.k8s_utils.get_resource_json("service", self.STORE)
            anns = (svc.get("metadata") or {}).get("annotations") or {}
            key = next((k for k in anns
                        if "serving-cert-secret-name" in k), "")
            if not key:
                raise s3.S3EvidenceError(
                    f"[TC-S3-008] tlsProvider=OpenShift but Service/{self.STORE} "
                    f"carries no serving-cert annotation; it has {sorted(anns)}")
            secret_name = anns[key]

        secret = self.k8s_utils.get_resource_json("secret", secret_name)
        data = secret.get("data") or {}
        missing = [k for k in ("tls.crt", "tls.key") if k not in data]
        if missing:
            raise s3.S3EvidenceError(
                f"[TC-S3-00x] Secret/{secret_name} is missing {missing}; it "
                f"holds {sorted(data)}. The gateway pod mounts this Secret, "
                f"so an empty one means it is serving nothing or serving a "
                f"self-signed certificate of its own.")

        # ── the pod mounts that Secret ──────────────────────────────────
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        gw = self.k8s_utils.get_resource_json("pod", pods[0][0])
        mounted = {
            (v.get("secret") or {}).get("secretName")
            for v in ((gw.get("spec") or {}).get("volumes") or [])}
        if secret_name not in mounted:
            raise s3.S3EvidenceError(
                f"[TC-S3-00x] gateway pod {pods[0][0]} mounts {sorted(m for m in mounted if m)}, "
                f"not the issued Secret {secret_name!r}")

        # ── and actually serves it ──────────────────────────────────────
        pod = self.start_client()
        cfg = self.pod_cfg(pods[0][2])
        self.make_bucket("tc007")
        s3.assert_roundtrip(pod, cfg, "tc007", "probe.bin", 8 * 1024 ** 2,
                            "tc007", context="(TC-S3-00x over TLS, unverified)")

        # E-03: a client inside the cluster completes a handshake *verified*
        # against the cluster's own CA, addressed by the Service name the
        # certificate is issued for. The data path above only proves TLS was
        # spoken, not that the certificate is the right one -- an unverified
        # context accepts anything, including a self-signed certificate that
        # has nothing to do with the Secret just asserted on.
        ca_path = self._install_ca(pod, provider, secret_name)
        verified_cfg = self.service_cfg(ca=ca_path)
        listed = pod.call(verified_cfg, "list_buckets")
        if listed.get("status") != 200:
            raise s3.S3EvidenceError(
                f"[TC-S3-009] a verified TLS handshake against "
                f"{s3.service_host(self.STORE, self.ns)} failed: {listed}. "
                f"The gateway serves TLS, but not with a certificate this "
                f"cluster's own CA vouches for, or not for that name.")

        self.record("tc007_tls", {
            "provider": provider, "secret": secret_name,
            "verified_handshake": True,
            "service_host": s3.service_host(self.STORE, self.ns),
        })
        self.logger.info("[TC-S3-007/008/009] PASS on %s", provider)

    def _install_ca(self, pod, provider, secret_name):
        """Put the cluster's CA bundle in the client pod and return its path.

        Two sources, because the two providers publish their CA in different
        places: OpenShift's service-ca lands in every namespace as a
        ConfigMap, cert-manager's issuing CA is in the issued Secret itself.
        """
        if provider == "OpenShift":
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get configmap openshift-service-ca.crt -n {self.ns} "
                f"-o jsonpath='{{.data.service-ca\\.crt}}' 2>/dev/null || true",
                supress_logs=True)
            bundle = out or ""
        else:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get secret {secret_name} -n {self.ns} "
                f"-o jsonpath='{{.data.ca\\.crt}}' 2>/dev/null || true",
                supress_logs=True)
            bundle = ""
            if (out or "").strip():
                decoded, _ = self.k8s_utils._exec_kubectl(
                    f"echo {out.strip()} | base64 -d", supress_logs=True)
                bundle = decoded or ""
        if "BEGIN CERTIFICATE" not in bundle:
            raise s3.S3ProbeError(
                f"[TC-S3-009] could not obtain this cluster's CA bundle for "
                f"provider {provider}; without it the verified handshake "
                f"would only be testing the harness")
        path = "/tmp/cluster-ca.crt"
        out, err = self.k8s_utils.exec_in_pod(
            pod.name, f"cat > {path} <<'__CA__'\n{bundle}\n__CA__\n"
                      f"openssl x509 -in {path} -noout -subject && echo CA_OK",
            namespace=pod.namespace, timeout=60)
        if "CA_OK" not in (out or ""):
            raise s3.S3ProbeError(
                f"[TC-S3-009] the CA bundle did not land in the client pod: "
                f"{(err or out or '')[-400:]}")
        return path


class S3BucketDeliveredWithoutRestartK8s(_ObjectStoreBase):
    """TC-S3-011/012 -- a bucket created *after* the gateway is already running.

    Closes `design-bucket-provisioning` E-01 and E-03, both `—`. The claim is
    narrow and important: `DeliverBucket` is called on the running pod's node
    and the mount appears *inside the already-running pod*, with no restart.
    `HostToContainer` propagation is what makes that work, and M-01 verified it
    by hand once during the spike; nothing verifies it on every build.
    """

    def run(self):
        self.logger.info("=== TC-S3-011/012 delivery without a pod restart ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=1)
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        gw_name, gw_node, gw_ip, _ = pods[0]

        # The pod's identity before. A replacement pod would serve the bucket
        # too, and would make this case pass for the wrong reason.
        before = self.k8s_utils.get_resource_json("pod", gw_name)
        uid_before = (before.get("metadata") or {}).get("uid")
        restarts_before = sum(
            c.get("restartCount", 0)
            for c in ((before.get("status") or {}).get("containerStatuses") or []))
        started_before = (before.get("status") or {}).get("startTime")

        client = self.start_client()
        name = "tc011"
        self.make_bucket(name)

        obj = self.bucket_is_consistent(name, context="after delivery")
        if gw_node not in s3.delivered_nodes(obj):
            raise s3.S3EvidenceError(
                f"[TC-S3-011] the bucket is Ready but was not delivered to "
                f"{gw_node}, the only node running a gateway pod: "
                f"{s3.delivered_nodes(obj)}")

        after = self.k8s_utils.get_resource_json("pod", gw_name)
        uid_after = (after.get("metadata") or {}).get("uid")
        restarts_after = sum(
            c.get("restartCount", 0)
            for c in ((after.get("status") or {}).get("containerStatuses") or []))
        if uid_before != uid_after:
            raise s3.S3EvidenceError(
                f"[TC-S3-011] the gateway pod was replaced while the bucket "
                f"was delivered (uid {uid_before} -> {uid_after}). The design "
                f"claims delivery reaches a running pod through "
                f"HostToContainer propagation, with no restart.")
        if restarts_after != restarts_before:
            raise s3.S3EvidenceError(
                f"[TC-S3-011] the gateway container restarted during delivery "
                f"({restarts_before} -> {restarts_after})")

        # E-03: a write made through the delivered mount is readable through
        # the gateway's own propagated view. Written at the filesystem, read
        # over S3 -- the two halves of the same mount.
        cfg = self.pod_cfg(gw_ip)
        s3.assert_roundtrip(client, cfg, name, "via-s3.bin", 16 * 1024 ** 2,
                            "tc011", context="(TC-S3-012 S3 round trip)")

        # The same proof in the direction that always works, and one that
        # does not depend on guessing the on-disk layout: the object written
        # over S3 is listed back by the gateway that mounts the delivery.
        listed = client.call(cfg, "list_objects", bucket=name)
        if "via-s3.bin" not in (listed.get("keys") or []):
            raise s3.S3EvidenceError(
                f"[TC-S3-012] the object round-tripped but is not listed: "
                f"{listed.get('keys')}")

        self.record("tc011_delivery_no_restart", {
            "gateway_pod": gw_name, "node": gw_node,
            "uid_stable": uid_before == uid_after,
            "restarts": [restarts_before, restarts_after],
            "pod_start_time": started_before,
            "delivered_nodes": s3.delivered_nodes(obj),
        })
        self.logger.info("[TC-S3-011/012] PASS")


class S3GatewayRescheduledK8s(_ObjectStoreBase):
    """TC-S3-013 -- a gateway pod lands on a node with no delivery for its buckets.

    Closes `design-bucket-provisioning` E-04, `—`. The question E-04 asks is
    precise: is the mount there *before* the pod is considered ready to serve,
    or is there a window during which it is not? A window is an acceptable
    answer if it is defined and observable; an undefined one means a client
    can get an empty bucket back.
    """

    def run(self):
        self.logger.info("=== TC-S3-013 gateway rescheduled onto a fresh node ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        if len(nodes) < 2:
            raise s3.S3ProbeError(
                f"[TC-S3-013] needs two schedulable nodes; have {nodes}")

        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=1)
        client = self.start_client()

        names = ["tc013a", "tc013b"]
        for n in names:
            self.make_bucket(n)
            s3.assert_roundtrip(client, self.service_cfg(), n, "seed.bin",
                                8 * 1024 ** 2, f"{n}-seed",
                                context="(TC-S3-013 setup)")

        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        old_pod, old_node, _, _ = pods[0]
        target = next(n for n in nodes if n != old_node)
        self.logger.info("[TC-S3-013] moving the gateway off %s; %s has never "
                         "had these buckets delivered to it", old_node, target)

        # Cordon the current node and delete the pod: the Deployment must put
        # it somewhere else. Cordon rather than drain -- a drain evicts
        # everything on the node, which would also move the storage the case
        # is trying to keep still.
        self.k8s_utils._exec_kubectl(f"kubectl cordon {old_node}")
        self.undo_stack.append(
            lambda n=old_node: self.k8s_utils._exec_kubectl(f"kubectl uncordon {n}"))
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {old_pod} -n {self.ns} --wait=false")

        # Sample from the moment the new pod is Ready, so the answer to E-04
        # is a measurement rather than a yes/no.
        deadline = time.time() + 600
        new = None
        while time.time() < deadline:
            rows = [p for p in s3.gateway_pods(self.k8s_utils, self.STORE)
                    if p[0] != old_pod and p[3]]
            if rows:
                new = rows[0]
                break
            time.sleep(5)
        if not new:
            raise s3.S3EvidenceError(
                "[TC-S3-013] no replacement gateway pod became Ready in 10 "
                "minutes after the original was deleted")
        ready_at = time.time()
        new_pod, new_node, new_ip, _ = new
        self.logger.info("[TC-S3-013] %s Ready on %s", new_pod, new_node)
        if new_node == old_node:
            raise s3.S3ProbeError(
                f"[TC-S3-013] the replacement landed back on the cordoned "
                f"node {old_node}; the case did not test what it meant to")

        # The window, measured per bucket: Ready-to-serve against
        # first-successful-read.
        cfg = self.pod_cfg(new_ip)
        windows = {}
        for n in names:
            first_ok, attempts, codes = None, 0, []
            while time.time() - ready_at < 300:
                attempts += 1
                got = client.call(cfg, "get_object", bucket=n, key="seed.bin",
                                  size=8 * 1024 ** 2, seed=f"{n}-seed")
                if got.get("status") == 200 and got.get("match"):
                    first_ok = time.time() - ready_at
                    break
                codes.append((got.get("status"), got.get("code")))
                time.sleep(2)
            windows[n] = {"seconds_after_ready": first_ok,
                          "attempts": attempts, "codes": codes[:10]}
            if first_ok is None:
                raise s3.S3EvidenceError(
                    f"[TC-S3-013] five minutes after {new_pod} became Ready "
                    f"on {new_node}, bucket {n} is still not readable through "
                    f"it: {codes[:10]}. status.mountDeliveredNodes says "
                    f"{s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))}.")

        # The serious failure is not a window, it is an *empty* one: a pod
        # that answers 200 with nothing in the bucket. A client cannot tell
        # that from a bucket that is genuinely empty.
        for n in names:
            listed = client.call(cfg, "list_objects", bucket=n)
            if "seed.bin" not in (listed.get("keys") or []):
                raise s3.S3EvidenceError(
                    f"[TC-S3-013] {new_pod} lists bucket {n} as "
                    f"{listed.get('keys')} -- it is serving an undelivered "
                    f"bucket as an empty one, which no client can distinguish "
                    f"from the truth")

        for n in names:
            delivered = s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))
            if new_node not in delivered:
                raise s3.S3EvidenceError(
                    f"[TC-S3-013] {n} is readable through {new_node} but "
                    f"status.mountDeliveredNodes says {delivered}")
            if old_node in delivered:
                raise s3.S3EvidenceError(
                    f"[TC-S3-013] {n} still claims delivery to {old_node}, "
                    f"which runs no gateway pod any more: {delivered}")

        self.record("tc013_reschedule_window", {
            "from_node": old_node, "to_node": new_node,
            "windows": windows,
        })
        self.logger.info("[TC-S3-013] PASS -- windows %s", windows)


class S3GatewayNodeSelectorK8s(_ObjectStoreBase):
    """TC-S3-018 -- nodes that never run a gateway pod are never delivered to.

    `test-plan-bucket-provisioning` §5 lists "heterogeneous node pools (some
    nodes never run gateway pods)" as not covered. Delivery is driven off pod
    placement, so a bug that delivers to every node instead of the placed ones
    is invisible on a homogeneous cluster -- every assertion passes, because
    every node happens to be correct.

    A bucket delivered to a node with no gateway is a mount nothing uses and
    nothing withdraws.
    """

    LABEL = "simplyblock.io/s3-gateway"

    def run(self):
        self.logger.info("=== TC-S3-018 heterogeneous node pool ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        if len(nodes) < 3:
            raise s3.S3ProbeError(
                f"[TC-S3-018] needs at least three nodes so some can be "
                f"excluded; have {nodes}")

        chosen = sorted(nodes)[:2]
        excluded = sorted(set(nodes) - set(chosen))
        for n in chosen:
            self.k8s_utils._exec_kubectl(
                f"kubectl label node {n} {self.LABEL}=yes --overwrite")
            self.undo_stack.append(
                lambda node=n: self.k8s_utils._exec_kubectl(
                    f"kubectl label node {node} {self.LABEL}- || true"))
        self.logger.info("[TC-S3-018] gateway pool %s, excluded %s",
                         chosen, excluded)

        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(
            replicas=2,
            extra_spec=f"  nodeSelector:\n    {self.LABEL}: \"yes\"\n")

        client = self.start_client()
        name = "tc018"
        self.make_bucket(name)
        s3.assert_roundtrip(client, self.service_cfg(), name, "probe.bin",
                            8 * 1024 ** 2, "tc018", context="(TC-S3-018)")

        placed = set(s3.gateway_nodes(self.k8s_utils, self.STORE))
        if not placed <= set(chosen):
            raise s3.S3EvidenceError(
                f"[TC-S3-018] spec.nodeSelector names {self.LABEL}=yes, which "
                f"only {chosen} carry, but gateway pods are on {sorted(placed)}")

        delivered = set(s3.delivered_nodes(s3.s3bucket(self.k8s_utils, name)))
        stray = delivered - placed
        if stray:
            raise s3.S3EvidenceError(
                f"[TC-S3-018] the bucket was delivered to {sorted(stray)}, "
                f"which run no gateway pod. Each of those is a mount nothing "
                f"uses and nothing will withdraw.")
        if delivered != placed:
            raise s3.S3EvidenceError(
                f"[TC-S3-018] delivered to {sorted(delivered)} but gateway "
                f"pods are on {sorted(placed)}")

        self.record("tc018_node_pool", {
            "gateway_pool": chosen, "excluded": excluded,
            "placed": sorted(placed), "delivered": sorted(delivered),
        })
        self.logger.info("[TC-S3-018] PASS")


class S3TwoObjectStoresSameNameK8s(_ObjectStoreBase):
    """TC-S3-020 -- two ObjectStores, two namespaces, one shared hostPath root.

    Closes the live half of U-17 and U-19, both regression tests for bugs that
    were found on a cluster and fixed by scoping `GATEWAY_ROOT_DIR` and the
    hostPath per ObjectStore. U-19's own comment records the second one:

        live on cluster91, the gateway crashed on every start -- `chdir
        .../simplyblock/simplyblock-s3: no such file or directory`

    Both are unit tests against a builder. The failure they describe was a
    live one, and nothing on a live cluster would catch its return.
    """

    OTHER_NS = "s3-tc020"

    def run(self):
        self.logger.info("=== TC-S3-020 two ObjectStores sharing a node ===")
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=1)

        self.k8s_utils.apply_yaml_cluster_scoped(
            f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {self.OTHER_NS}\n")
        self.extra_namespaces.append(self.OTHER_NS)
        self.make_credentials(namespace=self.OTHER_NS)
        # The same ObjectStore name in a different namespace: the exact shape
        # U-17 is about. Both gateways run on the same nodes and mount the
        # same shared host root.
        self.make_objectstore(namespace=self.OTHER_NS, replicas=1)

        client = self.start_client()
        here, there = "tc020-here", "tc020-there"
        self.make_bucket(here, bucket_name=here)
        self.make_bucket(there, bucket_name=there, namespace=self.OTHER_NS)

        cfg_here = self.service_cfg()
        cfg_there = self.service_cfg(namespace=self.OTHER_NS)
        s3.assert_roundtrip(client, cfg_here, here, "a.bin", 8 * 1024 ** 2,
                            "tc020-here", context="(TC-S3-020 default ns)")
        s3.assert_roundtrip(client, cfg_there, there, "b.bin", 8 * 1024 ** 2,
                            "tc020-there", context=f"(TC-S3-020 {self.OTHER_NS})")

        # The isolation U-17 exists for: neither gateway may see the other's
        # bucket. A shared GATEWAY_ROOT_DIR shows up here and nowhere else.
        for cfg, mine, theirs, where in (
                (cfg_here, here, there, self.ns),
                (cfg_there, there, here, self.OTHER_NS)):
            listed = client.call(cfg, "list_buckets")
            buckets = listed.get("buckets") or []
            if mine not in buckets:
                raise s3.S3EvidenceError(
                    f"[TC-S3-020] the gateway in {where} does not list its own "
                    f"bucket {mine}: {buckets}")
            if theirs in buckets:
                raise s3.S3EvidenceError(
                    f"[TC-S3-020] the gateway in {where} lists {theirs}, which "
                    f"belongs to the other namespace's ObjectStore. "
                    f"GATEWAY_ROOT_DIR is not scoped per ObjectStore -- this "
                    f"is U-17 failing on a live cluster.")

        # And neither gateway crash-looped on start, which is U-19's bug.
        for ns in (self.ns, self.OTHER_NS):
            for pod_name, _, _, _ in s3.gateway_pods(self.k8s_utils, self.STORE, ns):
                obj = self.k8s_utils.get_resource_json("pod", pod_name, ns)
                restarts = sum(
                    c.get("restartCount", 0) for c in
                    ((obj.get("status") or {}).get("containerStatuses") or []))
                if restarts:
                    logs = self.k8s_utils.get_pod_logs(pod_name, ns, tail=40)
                    raise s3.S3EvidenceError(
                        f"[TC-S3-020] gateway {pod_name} in {ns} restarted "
                        f"{restarts} time(s). U-19's bug looked exactly like "
                        f"this: {logs[-600:]}")

        self.record("tc020_two_stores", {
            "namespaces": [self.ns, self.OTHER_NS],
            "isolated": True,
        })
        self.logger.info("[TC-S3-020] PASS")


class S3GatewayPodKilledK8s(_ObjectStoreBase):
    """TC-S3-021/022 -- a gateway pod killed, and a rolling update, under traffic.

    TC-S3-021 closes `test-plan-objectstore` M-01, written up as a manual
    scenario and never automated. Its claim: the replacement pod sees every
    bucket already delivered to that node, with no re-delivery call.

    TC-S3-022 covers §7's last `—`, "graceful shutdown / in-flight request
    draining during a rolling update". The design's Open Question 2 leaves it
    undecided whether that is the gateway binary's concern. This case does not
    decide it either -- it measures it, so the question can be closed with a
    number instead of an opinion.
    """

    def run(self):
        self.logger.info("=== TC-S3-021/022 pod kill and rolling update ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        replicas = 2 if len(nodes) >= 2 else 1
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=replicas)
        client = self.start_client()

        name = "tc021"
        self.make_bucket(name)
        cfg = self.service_cfg()
        s3.assert_roundtrip(client, cfg, name, "seed.bin", 16 * 1024 ** 2,
                            "tc021", context="(TC-S3-021 setup)")

        before = s3.scrape_operator_metrics(self.k8s_utils)
        deliveries_before = s3.metric_total(
            before, s3.METRIC_DELIVERY_CALLS, operation="DeliverBucket")

        # ── TC-S3-021: kill one pod ─────────────────────────────────────
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        existing = {p[0] for p in pods}
        victim, victim_node, _, _ = pods[0]
        delivered_before = s3.delivered_nodes(s3.s3bucket(self.k8s_utils, name))
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {victim} -n {self.ns} --wait=false")

        # Any new Ready pod is the replacement, wherever it landed. M-01 does
        # not ask for the same node, and the gateway Deployment could not
        # honor it: required pod anti-affinity on hostname and no
        # nodeSelector, so while the victim is terminating its node is the
        # one place the replacement may not go.
        deadline = time.time() + 600
        replacement = None
        while time.time() < deadline:
            rows = [p for p in s3.gateway_pods(self.k8s_utils, self.STORE)
                    if p[0] not in existing and p[3]]
            if rows:
                replacement = rows[0]
                break
            time.sleep(5)
        if not replacement:
            self._log_gateway_placement()
            raise s3.S3EvidenceError(
                f"[TC-S3-021] no replacement for gateway pod {victim} (on "
                f"{victim_node}) became Ready within 10 minutes")
        new_node = replacement[1]
        same_node = new_node in delivered_before
        self.logger.info(
            "[TC-S3-021] replacement %s on %s; bucket was delivered to %s",
            replacement[0], new_node, delivered_before)

        if not same_node:
            # A node the bucket never reached needs a delivery before the pod
            # can serve it. That delivery is the operator's job, so wait for
            # the bucket's own status to name the node.
            deadline = time.time() + 300
            while time.time() < deadline:
                if new_node in s3.delivered_nodes(
                        s3.s3bucket(self.k8s_utils, name)):
                    break
                time.sleep(5)
            else:
                self._log_gateway_placement()
                raise s3.S3EvidenceError(
                    f"[TC-S3-021] the replacement pod {replacement[0]} landed "
                    f"on {new_node}, which the bucket was not delivered to, "
                    f"and 5 minutes later status.mountDeliveredNodes still "
                    f"does not name it: "
                    f"{s3.delivered_nodes(s3.s3bucket(self.k8s_utils, name))}")

        got = client.call(self.pod_cfg(replacement[2]), "get_object",
                          bucket=name, key="seed.bin", size=16 * 1024 ** 2,
                          seed="tc021")
        if got.get("status") != 200 or not got.get("match"):
            self._log_gateway_placement()
            raise s3.S3EvidenceError(
                f"[TC-S3-021] the replacement pod {replacement[0]} on "
                f"{new_node} cannot serve a bucket "
                + ("that was already delivered to its node: "
                   if same_node else "the operator reports delivered there: ")
                + f"{got}. M-01's claim is that the delivery is targeted at "
                f"the node, not the pod.")

        after = s3.scrape_operator_metrics(self.k8s_utils)
        deliveries_after = s3.metric_total(
            after, s3.METRIC_DELIVERY_CALLS, operation="DeliverBucket")
        redelivered = deliveries_after - deliveries_before
        self.logger.info("[TC-S3-021] DeliverBucket calls across the kill: %s",
                         redelivered)
        # "No re-delivery" is M-01's claim only for a pod that comes back on a
        # node that already had the bucket. A new node needs one.
        if same_node and redelivered:
            self.logger.error(
                "[TC-S3-021] FINDING: the replacement came back on %s, which "
                "already had the bucket, and the operator still made %s "
                "DeliverBucket call(s). M-01 says the delivery is per node.",
                new_node, redelivered)

        # ── TC-S3-022: a rolling update, under traffic ──────────────────
        # Traffic during the roll, so the question is "what does a client
        # see", not "did the pods come back".
        errors, oks = [], 0
        #: One entry per read the driver *attempted*, monotonic. Counting
        #: outcomes is not enough: the window a read landed in is what says
        #: whether the roll was measured at all.
        attempts = []
        stop = threading.Event()

        def drive():
            nonlocal oks
            while not stop.is_set():
                started = time.monotonic()
                try:
                    res = client.call(cfg, "get_object", bucket=name,
                                      key="seed.bin", size=16 * 1024 ** 2,
                                      seed="tc021", timeout=120)
                except Exception as exc:
                    # A refused connection mid-roll is the very thing this
                    # case measures, and ``client.call`` *raises* it rather
                    # than returning a status -- so the ``else`` below never
                    # saw it. Run 45906141173: the roll started at 02:33:18,
                    # this thread died ~2s in with ConnectionRefusedError,
                    # and the case reported "4 successful reads, 0 failed"
                    # at 02:33:41 -- all four from the baseline before the
                    # roll. A counter that dies reads exactly like a clean
                    # run, so the loop has to outlive its own errors.
                    errors.append((time.strftime("%H:%M:%S"), "raised",
                                   str(exc).splitlines()[0][:160]))
                    attempts.append((started, False))
                    continue
                ok = res.get("status") == 200 and bool(res.get("match"))
                if ok:
                    oks += 1
                else:
                    errors.append((time.strftime("%H:%M:%S"),
                                   res.get("status"), res.get("code")))
                attempts.append((started, ok))

        driver = threading.Thread(target=drive, daemon=True)
        driver.start()
        time.sleep(10)
        roll_started = time.time()
        roll_started_mono = time.monotonic()
        self.k8s_utils._exec_kubectl(
            f"kubectl rollout restart deployment/{self.STORE} -n {self.ns} "
            f"2>/dev/null || kubectl delete pods -n {self.ns} "
            f"-l {s3.GATEWAY_LABEL}={self.STORE} --wait=false")
        self.k8s_utils._exec_kubectl(
            f"kubectl rollout status deployment/{self.STORE} -n {self.ns} "
            f"--timeout=600s 2>/dev/null || true")
        s3.wait_gateway_pods(self.k8s_utils, self.STORE, replicas, timeout=600)
        roll_seconds = time.time() - roll_started
        roll_ended_mono = time.monotonic()
        time.sleep(15)
        stop.set()
        driver.join(timeout=180)

        in_roll = [ok for at, ok in attempts
                   if roll_started_mono <= at <= roll_ended_mono]
        self.logger.info(
            "[TC-S3-022] roll took %.0fs, %d successful reads, %d failed: %s "
            "(%d read(s) landed inside the roll window, %d of them ok)",
            roll_seconds, oks, len(errors), errors[:10],
            len(in_roll), sum(in_roll))

        # The reads outside the window say nothing about a roll. If none
        # landed inside it, the measurement is vacuous and the case must not
        # report on it -- that is the shape of the false pass above, and it
        # stays impossible only while something checks for it.
        if not in_roll:
            raise s3.S3EvidenceError(
                f"[TC-S3-022] no read landed while the gateway rolled "
                f"({roll_seconds:.0f}s), so what a client sees during a roll "
                f"was never measured. {len(attempts)} read(s) were attempted "
                f"in total and the driver thread is "
                f"{'alive' if driver.is_alive() else 'finished'}.")

        # Data is the assertion. Availability during a roll of a
        # single-replica Deployment is not something to fail on -- it is
        # something to report, which is what Open Question 2 asked for.
        s3.assert_roundtrip(client, cfg, name, "after-roll.bin",
                            16 * 1024 ** 2, "tc021-post",
                            context="(TC-S3-022 after the roll)")
        got = client.call(cfg, "get_object", bucket=name, key="seed.bin",
                          size=16 * 1024 ** 2, seed="tc021")
        if not got.get("match"):
            raise s3.S3EvidenceError(
                f"[TC-S3-022] data written before the rolling update does not "
                f"read back after it: {got}")

        self.record("tc021_pod_kill_and_roll", {
            "replicas": replicas,
            "killed_pod": victim, "node": victim_node,
            "replacement_pod": replacement[0], "replacement_node": new_node,
            "replacement_on_delivered_node": same_node,
            "deliver_calls_across_kill": redelivered,
            "roll_seconds": round(roll_seconds, 1),
            "reads_ok": oks, "reads_failed": len(errors),
            "failures": errors[:50],
        })
        if replicas > 1 and errors:
            self.logger.error(
                "[TC-S3-022] FINDING for Open Question 2: with %d replicas, a "
                "rolling update still produced %d failed request(s): %s. "
                "A multi-replica roll should be invisible to a client.",
                replicas, len(errors), errors[:10])
        self.logger.info("[TC-S3-021/022] PASS")

    def _log_gateway_placement(self):
        """Log where every gateway pod is, so a placement failure is visible."""
        self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.ns} -l {s3.GATEWAY_LABEL}={self.STORE} "
            f"-o wide")
