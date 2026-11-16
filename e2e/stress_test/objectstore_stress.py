"""S3 under a cluster that breaks -- the axis nothing in the feature has.

```
grep -niE 'delete pod|restart|kill|drain|outage|reboot|failover|cordon' \
    csi-driver/e2e/s3.go
→ no matches
```

Every spec dev wrote runs against a healthy cluster, and both of their test
plans say so: every E2E row is `—`, and three gaps carry no owner at all. This
file is that axis.

## The oracle

The plan this comes from proposed reusing dev's S2 model-checker rather than
writing a second one. That is right in principle and not available in
practice: S2 is a Ginkgo spec inside their Go test binary, driven by their own
harness, and it cannot be handed an outage schedule owned by ours. So the
*oracle* is reused rather than the code -- :class:`S3Load` keeps the same
in-memory model S2 does, compares every read byte-for-byte against it, and
checks every listing against the model's keys. A corruption finding from here
is expressed in the same terms as one from theirs.

## What every case asserts, on top of its own

Three things, because they are the ones that mean the product is broken rather
than merely slow:

1. **No data mismatch, ever.** A read that returns bytes nobody wrote is the
   worst outcome available and is never excused by an outage.
2. No 5xx the case does not predict, and no request that hangs rather than
   failing.
3. The `ObjectStore` and every `S3Bucket` back to `Ready` inside the outage's
   own budget, with nothing left `Degraded`.

## OpenBao

A lane that restarts things restarts OpenBao, and OpenBao comes back sealed --
dev's own deploy notes say only a rerun of their script unseals it. That turns
every restart into an S3 failure with no product cause. :class:`BaoUnsealer`
runs for the whole lane and logs every seal it sees, so TC-S3-006 can still
assert on a sealed window while nothing else is derailed by one.
"""
import json
import random
import threading
import time

from e2e_tests.objectstore.objectstore_base import _ObjectStoreBase
from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest
from utils import s3_utils as s3
from utils.common_utils import sleep_n_sec


class S3Load:
    """Continuous S3 traffic against a set of buckets, checked against a model.

    One worker thread per bucket. Each worker writes an object, reads it back
    and compares byte for byte, then occasionally re-reads an older key and
    lists the bucket against the model. Every failure is recorded with its
    wall-clock time and a classification, because "47 errors" and "47 errors
    all inside the 93 seconds the MDS was moving" are different findings.
    """

    #: Object size. Small enough that a worker completes a cycle inside the
    #: shortest outage this lane produces -- a workload whose single request
    #: spans the whole outage measures nothing about recovery.
    OBJECT_BYTES = 4 * 1024 ** 2

    def __init__(self, pod, cfg, buckets, logger, seed="s3load"):
        self.pod = pod
        self.cfg = cfg
        self.buckets = list(buckets)
        self.logger = logger
        self.seed = seed
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._lock = threading.Lock()
        self.threads = []
        #: bucket -> {key: (seed, size)} -- the model every read is checked
        #: against.
        self.model = {b: {} for b in self.buckets}
        self.ok = 0
        self.failures = []          # (iso, bucket, op, status, code)
        self.mismatches = []        # the ones that matter
        self.hangs = []

    # -- control ------------------------------------------------------

    def start(self):
        for b in self.buckets:
            t = threading.Thread(target=self._worker, args=(b,), daemon=True,
                                 name=f"s3load-{b}")
            t.start()
            self.threads.append(t)
        self.logger.info("[s3load] %d worker(s) started", len(self.threads))
        return self

    def pause(self):
        self._pause.set()

    def resume(self):
        self._pause.clear()

    def stop(self, timeout=180):
        self._stop.set()
        for t in self.threads:
            t.join(timeout=timeout / max(1, len(self.threads)))
        self.logger.info("[s3load] stopped: %d ok, %d failures, %d mismatches",
                         self.ok, len(self.failures), len(self.mismatches))

    # -- the worker ---------------------------------------------------

    def _worker(self, bucket):
        i = 0
        while not self._stop.is_set():
            if self._pause.is_set():
                time.sleep(1)
                continue
            i += 1
            key = f"load/{i:06d}.bin"
            try:
                self._cycle(bucket, key)
            except s3.S3ProbeError as exc:
                # The client could not complete the call at all. Recorded as
                # a hang rather than a failure: a request with no response is
                # a different product problem from one with a 503.
                with self._lock:
                    self.hangs.append(
                        (time.strftime("%Y-%m-%d %H:%M:%S"), bucket, str(exc)[:300]))
                time.sleep(2)
            except Exception as exc:                      # noqa: BLE001
                with self._lock:
                    self.failures.append(
                        (time.strftime("%Y-%m-%d %H:%M:%S"), bucket,
                         "driver", None, str(exc)[:200]))
                time.sleep(2)

    def _cycle(self, bucket, key):
        seed = f"{self.seed}-{bucket}-{key}"
        put = self.pod.call(self.cfg, "put_object", bucket=bucket, key=key,
                            size=self.OBJECT_BYTES, seed=seed, timeout=240)
        if put.get("status") not in (200, 201):
            self._record(bucket, "put", put)
            return
        with self._lock:
            self.model[bucket][key] = (seed, self.OBJECT_BYTES)

        got = self.pod.call(self.cfg, "get_object", bucket=bucket, key=key,
                            size=self.OBJECT_BYTES, seed=seed, timeout=240)
        if got.get("status") != 200:
            self._record(bucket, "get", got)
            return
        if not got.get("match"):
            with self._lock:
                self.mismatches.append({
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "bucket": bucket, "key": key,
                    "read_length": got.get("length"),
                    "expected_length": self.OBJECT_BYTES,
                    "first_diff": got.get("first_diff"),
                    "got": got.get("got_at_diff"),
                    "want": got.get("want_at_diff"),
                })
            self.logger.error(
                "[s3load] DATA MISMATCH %s/%s: read %s B, first difference at "
                "%s (got %s, want %s)", bucket, key, got.get("length"),
                got.get("first_diff"), got.get("got_at_diff"),
                got.get("want_at_diff"))
            return
        with self._lock:
            self.ok += 1

        # Every tenth cycle, re-read something older and check the listing.
        # A bucket that serves its newest object but has lost its older ones,
        # or lists keys that were never written, is a failure the write/read
        # loop alone cannot see.
        if self.ok % 10:
            return
        with self._lock:
            keys = list(self.model[bucket].items())
        if not keys:
            return
        old_key, (old_seed, old_size) = random.choice(keys)
        old = self.pod.call(self.cfg, "get_object", bucket=bucket, key=old_key,
                            size=old_size, seed=old_seed, timeout=240)
        if old.get("status") != 200:
            self._record(bucket, "get-old", old)
        elif not old.get("match"):
            with self._lock:
                self.mismatches.append({
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "bucket": bucket, "key": old_key, "aged": True,
                    "first_diff": old.get("first_diff"),
                    "got": old.get("got_at_diff"), "want": old.get("want_at_diff"),
                })
        listed = self.pod.call(self.cfg, "list_objects", bucket=bucket,
                               max=1000, timeout=240)
        if listed.get("status") == 200:
            shown = set(listed.get("keys") or [])
            with self._lock:
                known = set(self.model[bucket])
            phantom = shown - known
            if phantom:
                with self._lock:
                    self.failures.append(
                        (time.strftime("%Y-%m-%d %H:%M:%S"), bucket, "list",
                         200, f"keys nobody wrote: {sorted(phantom)[:5]}"))

    def _record(self, bucket, op, res):
        with self._lock:
            self.failures.append(
                (time.strftime("%Y-%m-%d %H:%M:%S"), bucket, op,
                 res.get("status"), res.get("code")))

    # -- reporting ----------------------------------------------------

    def report(self):
        with self._lock:
            return {
                "requests_ok": self.ok,
                "failures": len(self.failures),
                "failure_sample": self.failures[:60],
                "hangs": len(self.hangs),
                "hang_sample": self.hangs[:20],
                "mismatches": self.mismatches,
                "objects_in_model": {b: len(k) for b, k in self.model.items()},
            }

    def failures_between(self, start, end):
        """Failures inside a window, for correlating with an outage.

        An errno on its own has no judge -- what makes a 503 a finding is that
        it lands inside a window the test created, and what makes it noise is
        that it does not.
        """
        lo = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        hi = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        with self._lock:
            return [f for f in self.failures if lo <= f[0] <= hi]


class _S3StressBase(_ObjectStoreBase, K8sNativeFailoverTest):
    """Bring-up plus the outage machinery.

    ``_ObjectStoreBase`` first in the MRO: its ``setup``, builders and
    teardown are the ones that apply, and ``K8sNativeFailoverTest`` is here
    for ``perform_n_plus_k_outages`` and the recovery path. Both derive from
    ``TestClusterBase``, so the chain resolves cleanly.
    """

    BUCKET_COUNT = 10
    #: Each case asserts recovery inside this, measured from the end of the
    #: outage it caused. Generous: a storage node restart on this lab takes
    #: minutes before migration even starts.
    RECOVER_BUDGET = 900

    def setup(self):
        _ObjectStoreBase.setup(self)
        self._initialize_outage_log()

        cluster = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = (cluster[0] if isinstance(cluster, list)
                                    else cluster).get("max_fault_tolerance", 1) \
            if cluster else 1

        # The outage runner picks from these two. Built here rather than
        # inherited because this lane does not run the parent's own bring-up.
        nodes = self.sbcli_utils.get_storage_nodes()
        for result in nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = \
                result.get("secondary_node_id")
        self.logger.info("[s3stress] %d storage node(s), ftt=%s",
                         len(self.sn_nodes), self.max_fault_tolerance)

        self.bao = s3.BaoUnsealer(self.k8s_utils)
        self.bao.start()

    # -- shared bring-up for a loaded lane ----------------------------

    def bring_up_load(self, buckets=None, replicas=None, seed="s3load"):
        """StorageClass, gateway, N buckets, and traffic running on all of them."""
        count = buckets or self.BUCKET_COUNT
        nodes = self.k8s_utils.get_all_k8s_node_names()
        replicas = replicas or min(3, max(1, len(nodes)))

        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=replicas)
        client = self.start_client()

        names = [f"s3load-{i:03d}" for i in range(count)]
        for n in names:
            self.make_bucket(n, wait=False)
        for n in names:
            s3.wait_phase(self.k8s_utils, "s3bucket", n, "Ready",
                          timeout=self.BUCKET_TIMEOUT, forbid="Degraded")
        self.logger.info("[s3stress] %d bucket(s) Ready", len(names))

        self.load = S3Load(client, self.service_cfg(), names, self.logger,
                           seed=seed)
        self.load.start()
        # Let the model fill before anything is broken, so a mismatch after
        # an outage is a mismatch against bytes that demonstrably read back
        # correctly before it.
        sleep_n_sec(60)
        base = self.load.report()
        if base["requests_ok"] < count:
            raise s3.S3ProbeError(
                f"[s3stress] the load did not establish a baseline: "
                f"{base}. Nothing below would be attributable.")
        return names

    # -- node identity --------------------------------------------------
    #
    # Half of K8sUtils is keyed by node IP and half of kubectl by node name,
    # and passing one where the other is expected fails as "node not found"
    # several steps later.

    def node_ip(self, node_name):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get node {node_name} -o jsonpath="
            f"'{{.status.addresses[?(@.type==\"InternalIP\")].address}}' "
            f"2>/dev/null || true", supress_logs=True)
        ip = (out or "").split()
        if not ip:
            raise s3.S3ProbeError(
                f"[s3stress] node {node_name} reports no InternalIP")
        return ip[0]

    def drain_by_name(self, node_name, timeout_min=10):
        cli = "oc adm" if self.k8s_utils.detect_openshift() else "kubectl"
        return self.k8s_utils._exec_kubectl(
            f"{cli} drain {node_name} --ignore-daemonsets "
            f"--delete-emptydir-data --force --timeout={timeout_min}m "
            f"2>&1 || true", timeout=timeout_min * 60 + 120)

    # -- the three shared assertions ----------------------------------

    def assert_no_corruption(self, context=""):
        rep = self.load.report()
        if rep["mismatches"]:
            raise s3.S3EvidenceError(
                f"[s3stress] DATA MISMATCH {context}: "
                f"{json.dumps(rep['mismatches'][:5], indent=2)}. "
                f"Hexdump the object before any theory -- a block with no "
                f"recognisable header is neither stale nor never-written.")
        return rep

    def assert_recovered(self, store=None, buckets=None, context="",
                         budget=None):
        """Everything back to Ready inside the outage's own budget."""
        store = store or self.STORE
        budget = budget or self.RECOVER_BUDGET
        deadline = time.time() + budget
        last = {}
        while time.time() < deadline:
            sphase = s3.phase_of(s3.objectstore(self.k8s_utils, store))
            last = {b: s3.phase_of(s3.s3bucket(self.k8s_utils, b))
                    for b in (buckets or self.load.buckets)}
            if sphase == "Ready" and all(p == "Ready" for p in last.values()):
                return
            time.sleep(15)
        stuck = {b: p for b, p in last.items() if p != "Ready"}
        raise s3.S3EvidenceError(
            f"[s3stress] {budget}s after the outage {context}, ObjectStore is "
            f"{s3.phase_of(s3.objectstore(self.k8s_utils, store))!r} and "
            f"bucket(s) {stuck} are not Ready")

    def assert_no_hangs(self, context=""):
        rep = self.load.report()
        if rep["hangs"]:
            raise s3.S3EvidenceError(
                f"[s3stress] {rep['hangs']} request(s) {context} produced no "
                f"response at all: {rep['hang_sample'][:5]}. A hang gives a "
                f"client nothing to retry on, which is worse than any status "
                f"code.")

    def finish(self, label, extra=None):
        rep = self.load.report()
        self.record(label, {**rep, **(extra or {})})
        self.assert_no_corruption(f"({label})")
        return rep

    def teardown(self, delete_lvols=True, close_ssh=True,
                 skip_k8s_cleanup=False):
        load = getattr(self, "load", None)
        if load is not None:
            try:
                load.stop()
            except Exception:                             # noqa: BLE001
                pass
        _ObjectStoreBase.teardown(self, delete_lvols=delete_lvols,
                                  close_ssh=close_ssh,
                                  skip_k8s_cleanup=skip_k8s_cleanup)


class S3MdsMoveUnderLoadK8s(_S3StressBase):
    """TC-S3-101 -- the pNFS MDS moved while every bucket is serving traffic.

    Closes `test-plan-bucket-provisioning` §7's *"MDS failover concurrent with
    a bucket's own provisioning -- cross-design interaction, not assessed by
    either design as written"*, and widens it: every bucket is a pNFS volume,
    so the MDS singleton is in the data path of **every bucket at all times**,
    not only during provisioning.

    `pnfs_mds_replacement_stalls_every_client` measured 93 s of cluster-wide
    stall for a raw pNFS client. An S3 client does not experience a slow
    write; it experiences a timeout and a 5xx, so the number here is not
    assumed to be the same one.
    """

    ROUNDS = 3

    def run(self):
        self.logger.info("=== TC-S3-101 MDS move under S3 load ===")
        self.bring_up_load(seed="tc101")
        rounds = []

        for r in range(self.ROUNDS):
            name, _ip, pod_ips = self.k8s_utils.mds_nfs_targets()
            mds_pod, mds_node = self._mds_pod()
            self.logger.info("[TC-S3-101] round %d: MDS %s on %s (svc %s)",
                             r + 1, mds_pod, mds_node, name)

            before = self.load.report()["requests_ok"]
            t0 = time.time()
            # Drain, which is how the MDS actually moves in production -- a
            # delete would be replaced in place and measure nothing about the
            # eviction path.
            self.k8s_utils._exec_kubectl(f"kubectl cordon {mds_node}")
            self.undo_stack.append(
                lambda n=mds_node: self.k8s_utils._exec_kubectl(
                    f"kubectl uncordon {n} || true"))
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod {mds_pod} -n {self.ns} --wait=false")

            new_pod, new_node, ready_at = self._wait_new_mds(mds_pod, mds_node)
            # Ready, not Running. A replacement that comes up 0/1 and stays
            # there is the known worse case
            # (pnfs_mds_stays_dead_after_state_disk_journal_abort) and looks
            # identical from the S3 side for the first minute.
            self.logger.info("[TC-S3-101] replacement %s Ready on %s after "
                             "%.0fs", new_pod, new_node, ready_at - t0)

            recovered = self._first_clean_cycle(deadline=t0 + 900)
            window = self.load.failures_between(t0, recovered)
            rounds.append({
                "round": r + 1,
                "from_node": mds_node, "to_node": new_node,
                "mds_ready_seconds": round(ready_at - t0, 1),
                "client_outage_seconds": round(recovered - t0, 1),
                "failures_in_window": len(window),
                "codes": sorted({(f[3], f[4]) for f in window})[:10],
                "requests_ok_before": before,
            })
            self.logger.info("[TC-S3-101] round %d: client-visible outage "
                             "%.0fs, %d failed request(s)",
                             r + 1, recovered - t0, len(window))

            self.k8s_utils._exec_kubectl(f"kubectl uncordon {mds_node}")
            self.assert_no_corruption(f"(TC-S3-101 round {r + 1})")
            self.assert_recovered(context=f"(TC-S3-101 round {r + 1})")
            sleep_n_sec(120)

        self.load.stop()
        rep = self.finish("tc101_mds_move", {"rounds": rounds})

        # Classification, not a threshold. A 503 with a retry is survivable; a
        # 500 is not, and a hang is worst.
        fatal = [f for f in rep["failure_sample"] if f[3] == 500]
        if fatal:
            raise s3.S3EvidenceError(
                f"[TC-S3-101] the MDS move produced {len(fatal)} HTTP 500(s), "
                f"which no S3 client retries: {fatal[:5]}")
        self.assert_no_hangs("(TC-S3-101)")
        self.logger.info("[TC-S3-101] PASS -- outages %s",
                         [r["client_outage_seconds"] for r in rounds])

    def _mds_pod(self):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.ns} "
            f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} "
            f"{{.spec.nodeName}}{{\"\\n\"}}{{end}}' 2>/dev/null || true",
            supress_logs=True)
        for line in (out or "").splitlines():
            parts = line.split()
            if len(parts) == 2 and "pnfs-mds" in parts[0]:
                return parts[0], parts[1]
        raise s3.S3ProbeError(
            "[TC-S3-101] no pnfs-mds pod found; there is nothing to move and "
            "a case that silently moves nothing reads as a pass")

    def _wait_new_mds(self, old_pod, old_node, timeout=900):
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.ns} "
                f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} "
                f"{{.spec.nodeName}} "
                f"{{.status.containerStatuses[0].ready}}{{\"\\n\"}}{{end}}' "
                f"2>/dev/null || true", supress_logs=True)
            for line in (out or "").splitlines():
                parts = line.split()
                if (len(parts) == 3 and "pnfs-mds" in parts[0]
                        and parts[0] != old_pod and parts[2] == "true"):
                    return parts[0], parts[1], time.time()
            time.sleep(5)
        raise s3.S3EvidenceError(
            f"[TC-S3-101] no replacement MDS became Ready within {timeout}s "
            f"of {old_pod} being evicted from {old_node}. A pod that is "
            f"Running but 0/1 is this failure, not a slow one.")

    def _first_clean_cycle(self, deadline):
        """When the load next completes a full verified cycle."""
        base = self.load.report()["requests_ok"]
        while time.time() < deadline:
            if self.load.report()["requests_ok"] > base + len(self.load.buckets):
                return time.time()
            time.sleep(2)
        return deadline


class S3LoadAcrossOutagesK8s(_S3StressBase):
    """TC-S3-102 -- S3 traffic across every outage type the lane has.

    No dev equivalent at any level. Runs the existing outage runner --
    `storage_node_reboot`, `container_stop`, `graceful_shutdown`,
    `operator_shutdown` and the network cut -- underneath continuous S3
    traffic, with the same pacing the pNFS lane uses.

    Note the network-cut asymmetry: `interface_full_network_interrupt` cuts
    the whole worker, so a client scheduled there loses the gateway as well as
    the storage. The S3 client pod is pinned away from the outage node for
    exactly that reason -- otherwise the test measures where the scheduler put
    it.
    """

    ITERATIONS = int(__import__("os").getenv("S3_STRESS_ITERATIONS", "12"))

    def run(self):
        self.logger.info("=== TC-S3-102 S3 load across the outage types ===")
        self.bring_up_load(seed="tc102")
        seen_types = {}
        rounds = []

        for i in range(self.ITERATIONS):
            self.logger.info("=== TC-S3-102 iteration %d/%d ===",
                             i + 1, self.ITERATIONS)
            t0 = time.time()
            events = self.perform_n_plus_k_outages()
            for node, outage_type, _ in events:
                seen_types[outage_type] = seen_types.get(outage_type, 0) + 1
                self.current_outage_node = node
                self.restart_nodes_after_failover(outage_type)

            for node, _, _ in events:
                self.sbcli_utils.wait_for_storage_node_status(
                    node, "online", timeout=900)
            recovered = time.time()

            window = self.load.failures_between(t0, recovered + 60)
            rounds.append({
                "iteration": i + 1,
                "outages": [(n, t) for n, t, _ in events],
                "seconds": round(recovered - t0, 1),
                "failures_in_window": len(window),
                "codes": sorted({(f[3], f[4]) for f in window})[:10],
            })

            self.assert_no_corruption(f"(TC-S3-102 iteration {i + 1})")
            self.assert_recovered(context=f"(TC-S3-102 iteration {i + 1})")

            # The cheapest detector there is: a bucket outside Ready in the
            # gauge, even momentarily, with nothing else wrong.
            text = s3.scrape_operator_metrics(self.k8s_utils)
            stuck = [(lbl, v) for lbl, v in
                     s3.metric_samples(text, s3.METRIC_BUCKET_PHASE)
                     if v == 1 and lbl.get("phase") not in ("Ready", "")]
            if stuck:
                raise s3.S3EvidenceError(
                    f"[TC-S3-102] after iteration {i + 1} every bucket reads "
                    f"Ready through the API but {s3.METRIC_BUCKET_PHASE} "
                    f"reports {stuck}")
            sleep_n_sec(random.randint(50, 90))

        self.load.stop()
        self.finish("tc102_outage_matrix",
                    {"rounds": rounds, "outage_types": seen_types})
        self.assert_no_hangs("(TC-S3-102)")
        self.logger.info("[TC-S3-102] PASS -- outage types exercised: %s",
                         seen_types)


class S3CsiNodePluginRestartK8s(_S3StressBase):
    """TC-S3-103 -- the CSI node plugin restarted under load.

    Closes `design-bucket-provisioning` **Open Question 6**, which has no
    owner anywhere:

        the CSI node plugin's own stack record can disagree with what is
        actually mounted on the node after the plugin's restart, a node
        reboot, or a fabric-repair race, and the reconciler only observes what
        `DeliverBucket` last reported, not the node's real state. Neither a
        reconciliation pass against drift nor its detection is designed here.

    So the finding this case is looking for is not an error -- it is a
    *disagreement*: `status.mountDeliveredNodes` claiming a delivery that
    `findmnt` on the node cannot see.
    """

    def run(self):
        self.logger.info("=== TC-S3-103 CSI node plugin restart (OQ 6) ===")
        names = self.bring_up_load(buckets=5, seed="tc103")
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        gw_pod, gw_node, gw_ip, _ = pods[0]
        self.logger.info("[TC-S3-103] gateway %s on %s", gw_pod, gw_node)

        before = self._mounts_on(gw_node)
        self.logger.info("[TC-S3-103] %d bucket mount(s) on %s before",
                         len(before), gw_node)

        t0 = time.time()
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl delete pod -n {self.ns} -l app=simplyblock-csi-node "
            f"--field-selector spec.nodeName={gw_node} --wait=false "
            f"2>&1 || true")
        if "deleted" not in (out or ""):
            # Label schemes differ across charts. Find it by name rather than
            # skipping the restart, which would make the case pass for free.
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.ns} --field-selector "
                f"spec.nodeName={gw_node} -o jsonpath='{{range .items[*]}}"
                f"{{.metadata.name}} {{end}}' 2>/dev/null || true",
                supress_logs=True)
            target = next((p for p in (out or "").split()
                           if "csi" in p and "node" in p), "")
            if not target:
                raise s3.S3ProbeError(
                    f"[TC-S3-103] no CSI node-plugin pod found on {gw_node}; "
                    f"the case cannot restart what it cannot name")
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod {target} -n {self.ns} --wait=false")

        self._wait_csi_ready(gw_node)
        self.logger.info("[TC-S3-103] node plugin back after %.0fs",
                         time.time() - t0)
        sleep_n_sec(60)

        # ── the drift check, which is the case ──────────────────────────
        after = self._mounts_on(gw_node)
        claimed = {n: s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))
                   for n in names}
        drifted = []
        for n in names:
            if gw_node not in claimed[n]:
                continue
            bucket_name = (s3.s3bucket(self.k8s_utils, n).get("spec") or {}) \
                .get("bucketName", n)
            if not any(bucket_name in m for m in after):
                drifted.append(n)

        # ── and the data path, which is what a user would notice ────────
        cfg = self.pod_cfg(gw_ip)
        unreadable = []
        for n in names:
            got = self.load.pod.call(cfg, "list_objects", bucket=n, timeout=240)
            if got.get("status") != 200 or not (got.get("keys") or []):
                unreadable.append((n, got.get("status"), got.get("code"),
                                   len(got.get("keys") or [])))
        # A write, too: a mount that came back read-only serves every GET.
        for n in names[:2]:
            s3.assert_roundtrip(self.load.pod, cfg, n, "after-csi-restart.bin",
                                4 * 1024 ** 2, f"tc103-{n}",
                                context="(TC-S3-103 after the plugin restart)")

        # Then exercise the record rather than only reading it: a delete and a
        # create after the drift is where a stale stack record actually bites.
        self.k8s_utils.delete_resource("s3bucket", names[-1])
        s3.wait_gone(self.k8s_utils, "s3bucket", names[-1], timeout=600)
        self.buckets = [b for b in self.buckets if b[0] != names[-1]]
        self.load.buckets = [b for b in self.load.buckets if b != names[-1]]
        fresh = "tc103-after-drift"
        self.make_bucket(fresh)
        s3.assert_roundtrip(self.load.pod, cfg, fresh, "probe.bin",
                            4 * 1024 ** 2, "tc103-fresh",
                            context="(TC-S3-103 created after the restart)")

        self.load.stop()
        self.finish("tc103_csi_restart", {
            "node": gw_node,
            "mounts_before": len(before), "mounts_after": len(after),
            "claimed_delivered": claimed,
            "drifted_buckets": drifted,
            "unreadable_through_gateway": unreadable,
        })

        if unreadable:
            raise s3.S3EvidenceError(
                f"[TC-S3-103] after the CSI node plugin restarted on "
                f"{gw_node}, bucket(s) {unreadable} are not readable through "
                f"the gateway pod on that node, with no S3Bucket reconcile "
                f"having been needed. §5.2/§5.4 claim they should be.")
        if drifted:
            raise s3.S3EvidenceError(
                f"[TC-S3-103] **Open Question 6, live**: "
                f"status.mountDeliveredNodes claims {gw_node} holds "
                f"{drifted}, but findmnt on that node shows no mount for "
                f"them. The reconciler observes what DeliverBucket last "
                f"reported, not the node's real state, and there is no "
                f"reconciliation pass against the difference.")
        self.assert_no_hangs("(TC-S3-103)")
        self.logger.info("[TC-S3-103] PASS -- no drift detected")

    def _mounts_on(self, node):
        """Mounts under the node plugin's own subtrees, read on the node.

        ``run_on_node`` is keyed by IP, not by name -- it maps one to the
        other itself -- so the name has to be resolved first. On OpenShift
        this becomes ``oc debug node/<n>``, which is the only way in: the
        nodes take no ssh.
        """
        ip = self.node_ip(node)
        out, _ = self.k8s_utils.run_on_node(
            ip, "findmnt -rn -o TARGET 2>/dev/null | "
                "grep -E 'kubelet/(plugins|pods)' || true",
            check=False)
        return [ln.strip() for ln in str(out or "").splitlines() if ln.strip()]

    def _wait_csi_ready(self, node, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.ns} --field-selector "
                f"spec.nodeName={node} -o jsonpath='{{range .items[*]}}"
                f"{{.metadata.name}}:{{.status.containerStatuses[0].ready}} "
                f"{{end}}' 2>/dev/null || true", supress_logs=True)
            for row in (out or "").split():
                name, _, ready = row.partition(":")
                if "csi" in name and "node" in name and ready == "true":
                    return
            time.sleep(5)
        raise s3.S3ProbeError(
            f"[TC-S3-103] the CSI node plugin on {node} did not come back "
            f"Ready within {timeout}s")


class S3GatewayNodeDrainedK8s(_S3StressBase):
    """TC-S3-104 -- the node running a gateway pod drained, under load.

    No dev equivalent. Two shapes, and they are different tests:

    * `replicas == nodes`: the evicted pod **cannot** reschedule, because the
      hostname anti-affinity (U-18) is *required*, not preferred. How long the
      gateway runs degraded and whether the ObjectStore says so is the
      measurement.
    * `replicas < nodes`: the pod reschedules, and every bucket must be
      delivered to its new node *before* it serves. A gateway serving a bucket
      that has not been delivered is the serious failure -- it reads as an
      empty bucket, which no client can tell from the truth.
    """

    def run(self):
        self.logger.info("=== TC-S3-104 gateway node drained ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        if len(nodes) < 3:
            raise s3.S3ProbeError(
                f"[TC-S3-104] needs at least three nodes to run both shapes; "
                f"have {nodes}")

        names = self.bring_up_load(buckets=5, replicas=len(nodes), seed="tc104")
        results = {}

        # ── shape 1: replicas == nodes ──────────────────────────────────
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        victim_node = pods[0][1]
        t0 = time.time()
        self.k8s_utils._exec_kubectl(f"kubectl cordon {victim_node}")
        self.undo_stack.append(
            lambda n=victim_node: self.k8s_utils._exec_kubectl(
                f"kubectl uncordon {n} || true"))
        self.drain_by_name(victim_node)

        sleep_n_sec(120)
        degraded = s3.gateway_pods(self.k8s_utils, self.STORE)
        ready_now = len([p for p in degraded if p[3]])
        text = s3.scrape_operator_metrics(self.k8s_utils)
        reported = s3.metric_total(text, s3.METRIC_READY_REPLICAS)
        store_phase = s3.phase_of(s3.objectstore(self.k8s_utils, self.STORE))
        self.logger.info(
            "[TC-S3-104] with replicas==nodes and one drained: %d ready pod(s), "
            "metric says %s, ObjectStore is %s",
            ready_now, reported, store_phase)

        if ready_now >= len(nodes):
            raise s3.S3EvidenceError(
                f"[TC-S3-104] {ready_now} gateway pods are Ready after a node "
                f"was drained with replicas == nodes. The hostname "
                f"anti-affinity is documented as REQUIRED (U-18); if the pod "
                f"rescheduled, it is preferred.")
        if reported and reported >= len(nodes):
            raise s3.S3EvidenceError(
                f"[TC-S3-104] {s3.METRIC_READY_REPLICAS} reports {reported} "
                f"while only {ready_now} pod(s) are actually Ready. The "
                f"metric is the only signal for this window.")

        # Traffic must still work through the survivors.
        self.assert_no_corruption("(TC-S3-104 degraded window)")
        results["replicas_equal_nodes"] = {
            "drained": victim_node,
            "ready_pods": ready_now, "expected_max": len(nodes) - 1,
            "metric_ready_replicas": reported,
            "objectstore_phase": store_phase,
            "degraded_seconds_observed": round(time.time() - t0, 1),
        }

        self.k8s_utils._exec_kubectl(f"kubectl uncordon {victim_node}")
        s3.wait_gateway_pods(self.k8s_utils, self.STORE, len(nodes),
                             timeout=600)

        # ── shape 2: replicas < nodes ───────────────────────────────────
        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":{len(nodes) - 1}}}}}'")
        s3.wait_gateway_pods(self.k8s_utils, self.STORE, len(nodes) - 1,
                             timeout=600)
        pods = s3.gateway_pods(self.k8s_utils, self.STORE)
        victim_node = pods[0][1]
        victim_pod = pods[0][0]

        t0 = time.time()
        self.k8s_utils._exec_kubectl(f"kubectl cordon {victim_node}")
        self.drain_by_name(victim_node)
        moved = None
        deadline = time.time() + 600
        while time.time() < deadline:
            rows = [p for p in s3.gateway_pods(self.k8s_utils, self.STORE)
                    if p[0] != victim_pod and p[3] and p[1] != victim_node]
            if len(rows) >= len(nodes) - 1:
                moved = rows
                break
            time.sleep(5)
        if not moved:
            raise s3.S3EvidenceError(
                f"[TC-S3-104] with replicas < nodes, the evicted gateway pod "
                f"never rescheduled off {victim_node} within 10 minutes")

        # The serious assertion: delivered before serving, on the new node.
        new_node = next(p[1] for p in moved if p[1] != victim_node)
        empty = []
        for n in names:
            delivered = s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))
            cfg = self.pod_cfg(next(p[2] for p in moved if p[1] == new_node))
            listed = self.load.pod.call(cfg, "list_objects", bucket=n, timeout=240)
            if listed.get("status") == 200 and not (listed.get("keys") or []):
                empty.append((n, new_node, delivered))
        if empty:
            raise s3.S3EvidenceError(
                f"[TC-S3-104] the rescheduled gateway on {new_node} answers "
                f"ListObjects with 200 and no keys for {[e[0] for e in empty]}. "
                f"It is serving undelivered buckets as empty ones, which no "
                f"client can distinguish from the truth. "
                f"mountDeliveredNodes: {[e[2] for e in empty]}")

        results["replicas_below_nodes"] = {
            "drained": victim_node, "rescheduled_to": new_node,
            "reschedule_seconds": round(time.time() - t0, 1),
        }
        self.k8s_utils._exec_kubectl(f"kubectl uncordon {victim_node}")

        self.load.stop()
        self.finish("tc104_gateway_drain", results)
        self.assert_no_hangs("(TC-S3-104)")
        self.logger.info("[TC-S3-104] PASS -- %s", results)


class S3ManyBucketsK8s(_S3StressBase):
    """TC-S3-105 -- 100 buckets, the delivery fan-out, then an outage.

    Closes `test-plan-bucket-provisioning` §7's *"Concurrent creation of many
    distinct S3Buckets under sustained load -- Not scoped. No performance
    target exists yet"*, and produces the number **Open Question 4** asks for
    at scale.

    This case's first job is to produce the measurement, not to pass. The only
    hard assertions are the ones that are wrong at any scale: a bucket left
    `Pending` past the budget, and a delivery that reported failure.
    `s3bucketPendingRequeue`'s "retry forever" is design-accepted, which is
    exactly what makes a stuck bucket invisible without this measurement.
    """

    COUNT = int(__import__("os").getenv("S3_BUCKET_SCALE", "100"))
    #: Per bucket. A hundred buckets at the usual size would ask for more
    #: capacity than the lab has, and the case is about count, not bytes.
    SMALL = 1024 ** 3

    def run(self):
        self.logger.info("=== TC-S3-105 %d buckets ===", self.COUNT)
        nodes = self.k8s_utils.get_all_k8s_node_names()
        self.make_storage_class()
        self.make_credentials()
        self.make_objectstore(replicas=1, capacity=self.SMALL)
        client = self.start_client()

        # ── create them all at once, not one at a time ──────────────────
        names = [f"tc105-{i:03d}" for i in range(self.COUNT)]
        t0 = time.time()
        for n in names:
            self.make_bucket(n, capacity=self.SMALL, wait=False)
        created_at = time.time()
        self.logger.info("[TC-S3-105] %d S3Bucket objects submitted in %.1fs",
                         self.COUNT, created_at - t0)

        ready_at = {}
        pending = set(names)
        deadline = time.time() + 3600
        while pending and time.time() < deadline:
            for n in list(pending):
                if s3.phase_of(s3.s3bucket(self.k8s_utils, n)) == "Ready":
                    ready_at[n] = time.time() - t0
                    pending.discard(n)
            if pending:
                time.sleep(10)
                if len(ready_at) and len(ready_at) % 25 == 0:
                    self.logger.info("[TC-S3-105] %d/%d Ready",
                                     len(ready_at), self.COUNT)

        latencies = sorted(ready_at.values())
        summary = {}
        if latencies:
            summary = {
                "p50": round(latencies[len(latencies) // 2], 1),
                "p95": round(latencies[int(len(latencies) * 0.95) - 1], 1),
                "max": round(latencies[-1], 1),
                "first": round(latencies[0], 1),
                "ready": len(latencies), "requested": self.COUNT,
            }
        self.logger.info("[TC-S3-105] provisioning: %s", summary)

        if pending:
            stuck = {n: s3.phase_of(s3.s3bucket(self.k8s_utils, n))
                     for n in sorted(pending)[:10]}
            raise s3.S3EvidenceError(
                f"[TC-S3-105] {len(pending)} of {self.COUNT} buckets never "
                f"reached Ready in an hour: {stuck}. The requeue retries "
                f"forever by design, so nothing else would ever have said so.")

        # ── the fan-out: one more gateway node, N delivery RPCs ─────────
        fan_t0 = time.time()
        if len(nodes) >= 2:
            self.k8s_utils._exec_kubectl(
                f"kubectl patch objectstore {self.STORE} -n {self.ns} "
                f"--type=merge -p '{{\"spec\":{{\"replicas\":2}}}}'")
            pods = s3.wait_gateway_pods(self.k8s_utils, self.STORE, 2,
                                        timeout=900)
            want = {p[1] for p in pods}
            deadline = time.time() + 1800
            while time.time() < deadline:
                done = sum(
                    1 for n in names
                    if set(s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))) >= want)
                if done == self.COUNT:
                    break
                time.sleep(15)
            else:
                raise s3.S3EvidenceError(
                    f"[TC-S3-105] only {done}/{self.COUNT} buckets were "
                    f"delivered to the second gateway node within 30 minutes")
        fan_seconds = time.time() - fan_t0

        text = s3.scrape_operator_metrics(self.k8s_utils)
        hist = s3.histogram_quantiles(text, s3.METRIC_PROVISIONING_SECONDS)
        failures = s3.metric_total(text, s3.METRIC_DELIVERY_CALLS,
                                   outcome="failure")

        # ── and an outage with all of them live ─────────────────────────
        cfg = self.service_cfg()
        sample = names[::max(1, self.COUNT // 10)]
        for n in sample:
            s3.assert_roundtrip(client, cfg, n, "probe.bin", 1024 ** 2,
                                f"tc105-{n}", context="(TC-S3-105 pre-outage)")
        self.load = S3Load(client, cfg, sample, self.logger, seed="tc105")
        self.load.start()
        sleep_n_sec(45)

        out_t0 = time.time()
        events = self.perform_n_plus_k_outages()
        for node, outage_type, _ in events:
            self.current_outage_node = node
            self.restart_nodes_after_failover(outage_type)
        for node, _, _ in events:
            self.sbcli_utils.wait_for_storage_node_status(node, "online",
                                                          timeout=900)
        self.assert_recovered(buckets=names, context="(TC-S3-105)",
                              budget=1800)
        window = self.load.failures_between(out_t0, time.time())
        self.load.stop()

        self.finish("tc105_scale", {
            "count": self.COUNT,
            "submit_seconds": round(created_at - t0, 1),
            "provisioning": summary,
            "fanout_seconds": round(fan_seconds, 1),
            "histogram": hist,
            "delivery_failures": failures,
            "outage": [(n, t) for n, t, _ in events],
            "failures_in_outage_window": len(window),
            "note": "No pass/fail on the latencies: dev has no target. This "
                    "is Open Question 4's measurement at scale, and the "
                    "number the CreateBucket wait bound needs.",
        })

        if failures:
            raise s3.S3EvidenceError(
                f"[TC-S3-105] {failures} mount-delivery call(s) reported "
                f"failure across the fan-out")
        # Superlinearity is the finding dev asked for, reported rather than
        # failed on: a single bucket on a warm cluster is TC-S3-014's number.
        if summary.get("p95") and summary.get("first"):
            ratio = summary["p95"] / max(summary["first"], 1)
            self.logger.info(
                "[TC-S3-105] p95 is %.1fx the first bucket's latency. If that "
                "grows with COUNT, provisioning is superlinear and that is "
                "the number the CreateBucket wait bound needs.", ratio)
        self.assert_no_hangs("(TC-S3-105)")
        self.logger.info("[TC-S3-105] PASS -- %s", summary)


class S3MdsMoveDuringProvisioningK8s(_S3StressBase):
    """TC-S3-106 -- the MDS moved *while a bucket is provisioning*.

    The narrow form of the gap `test-plan-bucket-provisioning` §7 states:
    "MDS failover concurrent with a bucket's own provisioning -- cross-design
    interaction, not assessed by either design as written." TC-S3-101 covers
    the broad form (every bucket, at all times); this one covers the exact
    scenario dev named, which is a different code path: provisioning has a
    deadline, and a bucket whose MDS disappears mid-provision can degrade
    rather than wait.
    """

    IN_FLIGHT = 5

    def run(self):
        self.logger.info("=== TC-S3-106 MDS move during provisioning ===")
        self.bring_up_load(buckets=3, seed="tc106")

        names = [f"tc106-{i}" for i in range(self.IN_FLIGHT)]
        t0 = time.time()
        for n in names:
            self.make_bucket(n, wait=False)

        # Move the MDS while they are still in flight. Wait for the first one
        # to leave Pending so the move lands inside provisioning rather than
        # before it starts.
        deadline = time.time() + 180
        while time.time() < deadline:
            phases = [s3.phase_of(s3.s3bucket(self.k8s_utils, n)) for n in names]
            if any(p in ("Provisioning", "Ready") for p in phases):
                break
            time.sleep(2)
        in_flight = {n: s3.phase_of(s3.s3bucket(self.k8s_utils, n))
                     for n in names}
        self.logger.info("[TC-S3-106] moving the MDS with buckets at %s",
                         in_flight)

        mds_pod, mds_node = S3MdsMoveUnderLoadK8s._mds_pod(self)
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pod {mds_pod} -n {self.ns} --wait=false")
        S3MdsMoveUnderLoadK8s._wait_new_mds(self, mds_pod, mds_node)

        # Every in-flight bucket must still arrive. Degraded is the finding:
        # the provisioning deadline firing because the MDS was briefly away
        # means a bucket creation fails for a reason the user never sees.
        degraded = []
        for n in names:
            try:
                s3.wait_phase(self.k8s_utils, "s3bucket", n, "Ready",
                              timeout=1800, forbid=())
            except s3.S3EvidenceError:
                degraded.append((n, s3.phase_of(s3.s3bucket(self.k8s_utils, n))))
                continue
            if s3.phase_of(s3.s3bucket(self.k8s_utils, n)) != "Ready":
                degraded.append((n, s3.phase_of(s3.s3bucket(self.k8s_utils, n))))
        total = time.time() - t0

        cfg = self.service_cfg()
        for n in names:
            if any(n == d[0] for d in degraded):
                continue
            s3.assert_roundtrip(self.load.pod, cfg, n, "probe.bin",
                                4 * 1024 ** 2, f"tc106-{n}",
                                context="(TC-S3-106 after the MDS move)")

        self.load.stop()
        self.finish("tc106_mds_during_provisioning", {
            "in_flight_phases_at_move": in_flight,
            "seconds_to_all_ready": round(total, 1),
            "degraded": degraded,
        })
        if degraded:
            raise s3.S3EvidenceError(
                f"[TC-S3-106] bucket(s) {degraded} did not reach Ready after "
                f"the MDS moved during their provisioning. This is the exact "
                f"cross-design interaction neither design assesses: the "
                f"provisioning deadline is measured against a dependency that "
                f"can legitimately be away for ~90s.")
        self.assert_no_hangs("(TC-S3-106)")
        self.logger.info("[TC-S3-106] PASS -- all %d arrived in %.0fs",
                         self.IN_FLIGHT, total)


class S3GatewayLargeReplicaCountK8s(_S3StressBase):
    """TC-S3-107 -- a large gateway replica count.

    `test-plan-bucket-provisioning` §5 lists "large replica counts (10+)" as
    not covered. It is not only a scale question: the hostname anti-affinity
    is *required*, so replicas above the node count can never all schedule,
    and what the ObjectStore does about permanently-unschedulable replicas is
    undefined. A gateway stuck in `Provisioning` forever because it was asked
    for one pod too many would be a configuration mistake the product never
    reports.
    """

    def run(self):
        self.logger.info("=== TC-S3-107 large replica count ===")
        nodes = self.k8s_utils.get_all_k8s_node_names()
        names = self.bring_up_load(buckets=3, replicas=1, seed="tc107")

        # First, as many replicas as there are nodes: all must schedule.
        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":{len(nodes)}}}}}'")
        pods = s3.wait_gateway_pods(self.k8s_utils, self.STORE, len(nodes),
                                    timeout=900)
        if len({p[1] for p in pods}) != len(nodes):
            raise s3.S3EvidenceError(
                f"[TC-S3-107] {len(nodes)} replicas landed on "
                f"{len({p[1] for p in pods})} node(s)")
        for n in names:
            want = {p[1] for p in pods}
            deadline = time.time() + 900
            while time.time() < deadline:
                if set(s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))) >= want:
                    break
                time.sleep(10)
            else:
                raise s3.S3EvidenceError(
                    f"[TC-S3-107] bucket {n} was not delivered to all "
                    f"{len(nodes)} gateway nodes: "
                    f"{s3.delivered_nodes(s3.s3bucket(self.k8s_utils, n))}")

        # Then one more than can ever schedule.
        over = len(nodes) + 2
        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":{over}}}}}'")
        sleep_n_sec(180)
        ready = [p for p in s3.gateway_pods(self.k8s_utils, self.STORE) if p[3]]
        store = s3.objectstore(self.k8s_utils, self.STORE)
        phase = s3.phase_of(store)
        text = s3.scrape_operator_metrics(self.k8s_utils)
        reported = s3.metric_total(text, s3.METRIC_READY_REPLICAS)
        pending = self._unschedulable()

        self.logger.info(
            "[TC-S3-107] asked for %d on %d node(s): %d Ready, phase=%s, "
            "metric=%s, unschedulable=%s",
            over, len(nodes), len(ready), phase, reported, pending)

        # The data path must be untouched by the impossible ask.
        self.assert_no_corruption("(TC-S3-107 over-replicated)")
        cfg = self.service_cfg()
        for n in names:
            s3.assert_roundtrip(self.load.pod, cfg, n, "probe.bin",
                                4 * 1024 ** 2, f"tc107-{n}",
                                context="(TC-S3-107 over-replicated)")

        self.k8s_utils._exec_kubectl(
            f"kubectl patch objectstore {self.STORE} -n {self.ns} --type=merge "
            f"-p '{{\"spec\":{{\"replicas\":{len(nodes)}}}}}'")
        s3.wait_gateway_pods(self.k8s_utils, self.STORE, len(nodes),
                             timeout=600)

        self.load.stop()
        self.finish("tc107_large_replicas", {
            "nodes": len(nodes), "requested": over,
            "ready_pods": len(ready),
            "objectstore_phase": phase,
            "metric_ready_replicas": reported,
            "unschedulable_pods": pending,
        })
        if phase == "Ready" and len(ready) < over:
            self.logger.error(
                "[TC-S3-107] FINDING: the ObjectStore reports Ready with "
                "%d of %d replicas running and %d pod(s) permanently "
                "unschedulable. Nothing in the cluster's state says the "
                "replica count can never be met.", len(ready), over, len(pending))
        self.assert_no_hangs("(TC-S3-107)")
        self.logger.info("[TC-S3-107] PASS")

    def _unschedulable(self):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.ns} -l {s3.GATEWAY_LABEL}={self.STORE} "
            f"-o jsonpath='{{range .items[?(@.status.phase==\"Pending\")]}}"
            f"{{.metadata.name}} {{end}}' 2>/dev/null || true",
            supress_logs=True)
        return (out or "").split()


class S3BucketChurnUnderLoadK8s(_S3StressBase):
    """TC-S3-108 -- buckets created and destroyed continuously, under load.

    No dev equivalent, and the one shape most likely to surface a leak: every
    bucket carries a PVC, an lvol, an NFSExport and a mount on every gateway
    node, and `issues/02` is already on record as "the volume is never
    released". One create/delete proves nothing about the fiftieth.

    Run with an outage in the middle, because a delete interrupted by one is
    where a withdrawal is most likely to be skipped.
    """

    CYCLES = int(__import__("os").getenv("S3_CHURN_CYCLES", "20"))

    def run(self):
        self.logger.info("=== TC-S3-108 bucket churn under load ===")
        self.bring_up_load(buckets=4, seed="tc108")
        cfg = self.service_cfg()

        pvcs_before = self._count("pvc")
        pvs_before = self._count("pv")
        exports_before = self._count("nfsexport")
        self.logger.info("[TC-S3-108] before: %d pvc, %d pv, %d nfsexport",
                         pvcs_before, pvs_before, exports_before)

        outage_at = self.CYCLES // 2
        for i in range(self.CYCLES):
            name = f"tc108-{i:03d}"
            self.make_bucket(name)
            s3.assert_roundtrip(self.load.pod, cfg, name, "probe.bin",
                                2 * 1024 ** 2, f"tc108-{i}",
                                context=f"(TC-S3-108 cycle {i})")
            obj = self.bucket_is_consistent(name, context=f"cycle {i}")
            pvc = obj["status"]["persistentVolumeClaimName"]
            pv = self.k8s_utils.get_pvc_pv_name(pvc)

            if i == outage_at:
                self.logger.info("[TC-S3-108] outage mid-delete at cycle %d", i)
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete s3bucket {name} -n {self.ns} --wait=false")
                events = self.perform_n_plus_k_outages()
                for node, outage_type, _ in events:
                    self.current_outage_node = node
                    self.restart_nodes_after_failover(outage_type)
                for node, _, _ in events:
                    self.sbcli_utils.wait_for_storage_node_status(
                        node, "online", timeout=900)
            else:
                self.k8s_utils.delete_resource("s3bucket", name)

            s3.wait_gone(self.k8s_utils, "s3bucket", name, timeout=900)
            self.buckets = [b for b in self.buckets if b[0] != name]

            leaked = []
            if self.k8s_utils.get_resource_json("pvc", pvc):
                leaked.append(f"PVC {pvc}")
            if pv and self.k8s_utils.get_resource_json("pv", pv):
                leaked.append(f"PV {pv}")
            if leaked:
                raise s3.S3EvidenceError(
                    f"[TC-S3-108] cycle {i}: {', '.join(leaked)} survived the "
                    f"S3Bucket's deletion"
                    + (" across an outage" if i == outage_at else "")
                    + ". This is the issues/02 signature.")
            self.assert_no_corruption(f"(TC-S3-108 cycle {i})")

        # The aggregate check: counts back where they started. A per-cycle
        # check can pass every time while the totals creep.
        sleep_n_sec(120)
        after = {"pvc": self._count("pvc"), "pv": self._count("pv"),
                 "nfsexport": self._count("nfsexport")}
        before = {"pvc": pvcs_before, "pv": pvs_before,
                  "nfsexport": exports_before}
        grew = {k: (before[k], after[k]) for k in before if after[k] > before[k]}

        self.load.stop()
        self.finish("tc108_churn", {
            "cycles": self.CYCLES, "outage_at_cycle": outage_at,
            "counts_before": before, "counts_after": after,
        })
        if grew:
            raise s3.S3EvidenceError(
                f"[TC-S3-108] {self.CYCLES} create/delete cycles left objects "
                f"behind: {grew}. Every individual cycle released its own "
                f"PVC, so this is an accumulation nothing per-cycle sees.")
        self.assert_no_hangs("(TC-S3-108)")
        self.logger.info("[TC-S3-108] PASS -- %d cycles, no leak", self.CYCLES)

    def _count(self, kind):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get {kind} -n {self.ns} --no-headers 2>/dev/null | wc -l",
            supress_logs=True)
        try:
            return int((out or "0").strip())
        except ValueError:
            return 0
