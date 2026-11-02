"""Track B: the shared filesystem on a healthy cluster.

The four cases here are the gate for everything else in the plan. B-01 proves
a ReadWriteMany claim can be provisioned and mounted by a pod on every worker
at once; B-03 proves those mounts are actually doing pNFS rather than plain
NFS; B-02 proves a read really crossed the wire instead of being served from
the writer's own page cache; B-04 proves the bytes reached the NVMe devices.

Until B-01 and B-03 pass, nothing downstream means anything -- a suite that
runs green for eight hours against a plain NFS fallback is the worst outcome
available, so the fallback is asserted against explicitly rather than
inferred from the filesystem behaving.

Class names end in ``K8s`` on purpose: pNFS has no Docker variant, and the
platform filter in ``e2e.py`` keys on that suffix, so ``--testname pnfs`` on
a Docker cluster reports these as skipped instead of failing obscurely.
"""
import json
import time

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.pnfs_utils import (
    PnfsEvidenceError,
    assert_devices_moved,
    assert_layouts_taken,
    nvme_ns_counters,
)


class _PnfsBase(TestClusterBase):
    """Shared machinery for the Track B cases."""

    #: One pod per worker. Resolved from the cluster at setup; this is only
    #: the ceiling, so a four-node lab runs four pods rather than failing.
    MAX_PODS = 6
    CLAIM_SIZE = "20Gi"
    FIO_RUNTIME = 120
    MOUNT = "/spdkvol"
    #: Generous: the first RWX claim on a cluster provisions an lvol, an
    #: NFSExport and a mount on every client, and the export is the slow part.
    BIND_TIMEOUT = 600
    JOB_TIMEOUT = 900

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.k8s_test = True
        self.sc_name = "sb-pnfs-sc"
        self.claim = None
        self.jobs = []
        self.workers = []

    # ── setup, and the gate ──────────────────────────────────────────────

    def setup(self):
        self.logger.info("Inside %s.setup()", type(self).__name__)
        retry = 30
        while retry > 0:
            try:
                self.mgmt_nodes, self.storage_nodes = \
                    self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as exc:                      # noqa: BLE001
                retry -= 1
                if retry == 0:
                    raise
                self.logger.info("Retrying base APIs (%s)", exc)
                sleep_n_sec(10)

        self.assert_pnfs_cluster()
        self.workers = self._worker_names()[:self.MAX_PODS]
        if len(self.workers) < 2:
            raise PnfsEvidenceError(
                f"a shared-filesystem test needs at least two schedulable "
                f"workers; this cluster offers {self.workers}")
        self.logger.info("[pnfs] %d worker(s) in play: %s",
                         len(self.workers), ", ".join(self.workers))

    def assert_pnfs_cluster(self):
        """F-8. Refuse the run unless the cluster can actually serve pNFS.

        Mirrors ``assert_lblk_cluster``. Three independent things, each of
        which fails silently rather than loudly if it is missing: the CRD
        has to exist, a metadata server has to be configured on the cluster,
        and the mds-proxy DaemonSet has to be Ready. Without the gate, a
        missing MDS gives a claim that provisions as ordinary NFS and a
        suite that passes while testing the wrong product.
        """
        problems = []

        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get crd nfsexports.storage.simplyblock.io "
            "--ignore-not-found -o name", supress_logs=True)
        if "nfsexports" not in (out or ""):
            problems.append(
                "the NFSExport CRD is not installed, so nothing will ever "
                "provision an export")

        ns = self.k8s_utils.namespace
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get storagecluster -n {ns} -o json", supress_logs=True)
        mds = None
        try:
            for item in json.loads(out or "{}").get("items", []):
                mds = ((item.get("spec") or {}).get("pnfs") or {}).get("mds")
                if mds:
                    break
        except ValueError:
            pass
        if not mds:
            problems.append(
                "no StorageCluster carries spec.pnfs.mds, so a pNFS claim "
                "would come back as plain NFS with no metadata server")

        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get daemonset -n {ns} "
            f"-o custom-columns=NAME:.metadata.name,"
            f"READY:.status.numberReady,WANT:.status.desiredNumberScheduled "
            f"--no-headers", supress_logs=True)
        mds_rows = [ln.split() for ln in (out or "").splitlines()
                    if "mds" in ln.split()[0]] if out else []
        if not mds_rows:
            problems.append("no mds-proxy DaemonSet is deployed")
        else:
            for row in mds_rows:
                if len(row) >= 3 and row[1] != row[2]:
                    problems.append(
                        f"DaemonSet {row[0]} is {row[1]}/{row[2]} Ready")

        if problems:
            raise PnfsEvidenceError(
                "this cluster is not set up for pNFS, and running anyway "
                "would test plain NFS without saying so:\n  "
                + "\n  ".join(problems))
        self.logger.info("[pnfs] cluster gate passed: CRD present, "
                         "spec.pnfs.mds=%s, mds-proxy Ready", mds)

    # ── cluster shape ────────────────────────────────────────────────────

    def _worker_names(self):
        """Schedulable worker node names, in a stable order."""
        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get nodes -l '!node-role.kubernetes.io/master' "
            "-o custom-columns=NAME:.metadata.name,"
            "TAINTS:.spec.taints[*].effect --no-headers", supress_logs=True)
        names = []
        for line in (out or "").splitlines():
            parts = line.split()
            if not parts:
                continue
            taints = parts[1] if len(parts) > 1 else "<none>"
            if "NoSchedule" in taints:
                continue
            names.append(parts[0])
        return sorted(names)

    def _pod_node(self, pod_name):
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pod {pod_name} -n {self.k8s_utils.namespace} "
            f"-o custom-columns=NODE:.spec.nodeName --no-headers",
            supress_logs=True)
        return (out or "").strip()

    # ── claim and load ───────────────────────────────────────────────────

    def _make_storage_class(self):
        pool = self.sbcli_utils.add_storage_pool(self.pool_name) \
            or self.pool_name
        self.k8s_utils.create_storage_class(
            name=self.sc_name, cluster_id=self.cluster_id, pool_name=pool,
            fs_type="pnfs")
        self.logger.info("[pnfs] StorageClass %s created with fstype=pnfs",
                         self.sc_name)

    def _make_rwx_claim(self, name):
        self.k8s_utils.create_pvc(
            name=name, size=self.CLAIM_SIZE, storage_class=self.sc_name,
            access_modes=["ReadWriteMany"])
        self.k8s_utils.wait_pvc_bound(name, timeout=self.BIND_TIMEOUT)
        self.claim = name
        self.logger.info("[pnfs] RWX claim %s bound", name)
        return name

    def _pod_config(self, idx, rw="write", verify=True, size="256M",
                    runtime=None, filename=None):
        """One FIO config for one pod.

        Every path carries the pod index. On a shared claim the pods see one
        filesystem, so a config keyed on the claim would have all of them
        writing the same file and md5 verification would stop meaning
        anything (F-4).
        """
        fn = filename or f"{self.MOUNT}/pod{idx}.dat"
        lines = [
            "[global]",
            f"name=pnfs-pod{idx}",
            f"filename={fn}",
            f"rw={rw}",
            "bs=64k",
            "iodepth=4",
            "direct=1",
            "ioengine=libaio",
            f"size={size}",
            "numjobs=1",
            "group_reporting",
            f"randseed={1000 + idx}",
        ]
        if verify:
            lines += ["verify=md5", "verify_fatal=1", "verify_dump=1"]
        if runtime:
            lines += ["time_based", f"runtime={runtime}"]
        lines += ["", f"[pod{idx}]"]
        return "\n".join(lines) + "\n"

    def _start_shared_load(self, base, configs, required_spread=True):
        self.jobs = self.k8s_utils.create_shared_fio_jobs(
            base_name=base, pvc_name=self.claim, configs=configs,
            required_spread=required_spread)
        return self.jobs

    def _pods_of(self, jobs, timeout=300):
        """Wait until every job has a pod, and return the pod names."""
        deadline = time.time() + timeout
        pods = {}
        while time.time() < deadline:
            pods = {j: self.k8s_utils.get_job_pod_name(j) for j in jobs}
            if all(pods.values()):
                return [pods[j] for j in jobs]
            sleep_n_sec(5)
        missing = [j for j, p in pods.items() if not p]
        raise PnfsEvidenceError(
            f"no pod appeared for job(s) {missing} within {timeout}s. On a "
            f"required anti-affinity spread this usually means there are "
            f"fewer schedulable workers than pods.")

    def _assert_one_pod_per_node(self, pods, context=""):
        placement = {p: self._pod_node(p) for p in pods}
        nodes = [n for n in placement.values() if n]
        if len(set(nodes)) != len(pods):
            raise PnfsEvidenceError(
                f"the pods did not land one per node {context}, so this is "
                f"not a distributed-filesystem test: {placement}")
        self.logger.info("[pnfs] placement %s: %s", context, placement)
        return placement

    def _await_jobs(self, jobs, what):
        bad = []
        for j in jobs:
            status = self.k8s_utils.wait_job_complete(
                j, timeout=self.JOB_TIMEOUT)
            if status != "succeeded":
                logs = ""
                pod = self.k8s_utils.get_job_pod_name(j)
                if pod:
                    logs = (self.k8s_utils.get_pod_logs(pod, tail=40) or "")
                bad.append(f"{j}: {status}\n{logs[-1200:]}")
        if bad:
            raise AssertionError(
                f"{what}: FIO did not succeed in every pod:\n"
                + "\n---\n".join(bad))
        self.logger.info("[pnfs] %s: all %d FIO job(s) succeeded",
                         what, len(jobs))

    # ── teardown ─────────────────────────────────────────────────────────

    def teardown(self):
        """F-10. Drain the pods before the claim.

        An RWX claim has N pods. Deleting the claim while any of them still
        holds it hangs on finalizers and leaks an NFSExport and an lvol into
        the next run, which then fails for a reason that has nothing to do
        with it.
        """
        self.logger.info("Inside %s.teardown()", type(self).__name__)
        for j in self.jobs:
            try:
                self.k8s_utils.delete_resource("job", j)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete job %s: %s",
                                    j, exc)
        if self.jobs:
            self._wait_pods_gone()
        if self.claim:
            try:
                self.k8s_utils.delete_pvc(self.claim)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete claim %s: %s",
                                    self.claim, exc)
        super().teardown()

    def _wait_pods_gone(self, timeout=300):
        label = self.k8s_utils.SHARED_CLAIM_LABEL
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {self.k8s_utils.namespace} "
                f"-l {label}={self.claim} --no-headers "
                f"--ignore-not-found", supress_logs=True)
            if not (out or "").strip():
                return
            sleep_n_sec(5)
        self.logger.warning(
            "[pnfs] pods still holding %s after %ds; the claim delete may "
            "hang on finalizers", self.claim, timeout)


# ── B-01 ─────────────────────────────────────────────────────────────────

class PnfsSharedWriteVerifyK8s(_PnfsBase):
    """B-01: one pod per worker on one RWX claim, each writes and verifies.

    The baseline. Everything else in Track B assumes this works.
    """

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b01")
        configs = [self._pod_config(i) for i in range(len(self.workers))]
        jobs = self._start_shared_load("pnfs-b01-fio", configs)
        pods = self._pods_of(jobs)
        self._assert_one_pod_per_node(pods, "(B-01)")
        self._await_jobs(jobs, "B-01")
        self.logger.info(
            "[B-01] PASS: %d pods on %d nodes wrote and verified their own "
            "file on one shared claim", len(pods), len(set(self.workers)))


# ── B-03 ─────────────────────────────────────────────────────────────────

class PnfsLayoutProofK8s(_PnfsBase):
    """B-03: every mount took a layout, and the server served no data.

    This is the gate. A pNFS mount and a plain NFS mount are
    indistinguishable from the filesystem's behaviour, so the proof has to
    come from ``/proc/self/mountstats``: a ``pnfs=`` layout type, a non-zero
    LAYOUTGET, and zero server-side READ and WRITE.
    """

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b03")
        # Long enough that a layout is certainly taken and used, and the pods
        # are still alive to be inspected while IO is in flight.
        configs = [self._pod_config(i, rw="randwrite", verify=False,
                                    runtime=self.FIO_RUNTIME)
                   for i in range(len(self.workers))]
        jobs = self._start_shared_load("pnfs-b03-fio", configs)
        pods = self._pods_of(jobs)
        self._assert_one_pod_per_node(pods, "(B-03)")

        sleep_n_sec(45)          # let the clients settle into the data path
        proven = assert_layouts_taken(
            self.k8s_utils, pods, mountpoint=self.MOUNT,
            logger=self.logger, context="(B-03)")

        self._await_jobs(jobs, "B-03")
        self.logger.info(
            "[B-03] PASS: %d/%d mounts hold a pNFS layout and drove no "
            "server-side READ/WRITE: %s",
            len(proven), len(pods),
            ", ".join(f"{p}={d['layout_type']}/{d['LAYOUTGET']}"
                      for p, d in proven.items()))


# ── B-02 ─────────────────────────────────────────────────────────────────

class PnfsCrossClientReadK8s(_PnfsBase):
    """B-02: pod i writes, pod i+1 reads and verifies, round robin.

    B-01 can be satisfied by a client that never talked to anyone -- each pod
    writing and verifying its own file is served perfectly well by its own
    page cache. Reading a neighbour's file is the first case that requires
    the data to have actually crossed the wire.
    """

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b02")
        n = len(self.workers)

        write_cfgs = [self._pod_config(i, rw="write") for i in range(n)]
        wjobs = self._start_shared_load("pnfs-b02-w", write_cfgs)
        wpods = self._pods_of(wjobs)
        self._assert_one_pod_per_node(wpods, "(B-02 writers)")
        self._await_jobs(wjobs, "B-02 write pass")

        for j in wjobs:
            self.k8s_utils.delete_resource("job", j)
        self._wait_pods_gone()

        # Pod i now reads pod i-1's file. verify=md5 with the neighbour's
        # randseed is what makes this an assertion and not just a read.
        read_cfgs = []
        for i in range(n):
            src = (i - 1) % n
            cfg = self._pod_config(src, rw="read",
                                   filename=f"{self.MOUNT}/pod{src}.dat")
            read_cfgs.append(cfg)
        rjobs = self._start_shared_load("pnfs-b02-r", read_cfgs)
        rpods = self._pods_of(rjobs)
        self._assert_one_pod_per_node(rpods, "(B-02 readers)")
        self._await_jobs(rjobs, "B-02 cross read")
        self.jobs = rjobs

        self.logger.info(
            "[B-02] PASS: %d pods each verified a neighbour's file, so the "
            "data crossed the wire rather than coming from a local cache", n)


# ── B-04 ─────────────────────────────────────────────────────────────────

class PnfsDeviceProofK8s(_PnfsBase):
    """B-04: the NVMe namespaces on each node moved while the run was on.

    The other end of the path from B-03. A layout says the client intends to
    talk to the storage nodes directly; the device counters say it did.
    """

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b04")

        before = {ip: nvme_ns_counters(self.k8s_utils, ip)
                  for ip in self.storage_nodes}
        self.logger.info("[B-04] sampled device counters on %d node(s)",
                         len(before))

        configs = [self._pod_config(i, rw="write", verify=False, size="512M")
                   for i in range(len(self.workers))]
        jobs = self._start_shared_load("pnfs-b04-fio", configs)
        pods = self._pods_of(jobs)
        self._assert_one_pod_per_node(pods, "(B-04)")
        self._await_jobs(jobs, "B-04")

        after = {ip: nvme_ns_counters(self.k8s_utils, ip)
                 for ip in self.storage_nodes}

        moved_nodes, flat = [], []
        for ip in self.storage_nodes:
            try:
                assert_devices_moved(before.get(ip), after.get(ip),
                                     logger=self.logger, context=f"on {ip}")
                moved_nodes.append(ip)
            except PnfsEvidenceError:
                flat.append(ip)
        if not moved_nodes:
            raise PnfsEvidenceError(
                "not one storage node's NVMe counters moved during the run. "
                "The write succeeded, so the data went somewhere -- but not "
                "to the devices this cluster is made of.")
        self.logger.info(
            "[B-04] PASS: counters moved on %d/%d node(s)%s",
            len(moved_nodes), len(self.storage_nodes),
            f"; flat on {flat}" if flat else "")
