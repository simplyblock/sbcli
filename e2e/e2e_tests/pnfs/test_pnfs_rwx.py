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
import os
import threading
import time
from datetime import UTC, datetime

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils
from utils.pnfs_utils import (
    PnfsEvidenceError,
    PnfsProbeError,
    assert_devices_moved,
    assert_layouts_taken,
    nfsexport_status,
    nvme_ns_counters,
)
from utils.ssh_utils import RunnerK8sLog


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
        self.k8s_utils = None
        self.sc_name = "sb-pnfs-sc"
        self.claim = None
        self.jobs = []
        self.workers = []
        # Everything below is cleaned by teardown even when run() raised
        # part-way. A Track B case that leaks a claim leaks an NFSExport and
        # an lvol with it, and the next case in the group fails for a reason
        # that has nothing to do with it (F-10).
        self.extra_claims = []
        self.shell_pods = []
        self.extra_namespaces = []
        #: Callables run in reverse order at teardown. B-16 mutates the
        #: driver CR, and a mutation with no guaranteed undo on a shared
        #: lab is how a cluster stays broken for everyone after one test.
        self.undo_stack = []

    # ── setup, and the gate ──────────────────────────────────────────────

    def setup(self):
        self.logger.info("Inside %s.setup()", type(self).__name__)

        # The run's log directory, and the file the workflow reads to learn
        # it. TestClusterBase.setup() does this, but no k8s-native test
        # calls it -- the base setup reaches for SSH to worker nodes that
        # a k8s-native run does not have. Skipping it without doing this
        # much by hand is why run 41428476500 reported
        #   Logs Path:
        #   E2E Run Dir: ``
        #   Graylog: /graylog_collected/
        # and left no run folder on the share at all: RUN_DIR_FILE was
        # never written, so RUN_BASE_DIR was empty everywhere downstream
        # and every per-test path in the summary came out blank.
        self.test_start_time_utc = datetime.now(UTC)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.docker_logs_path = os.path.join(
            self.nfs_log_base, f"{self.test_name}-{timestamp}")
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        os.makedirs(self.docker_logs_path, exist_ok=True)

        run_file = os.getenv("RUN_DIR_FILE", None)
        if run_file:
            with open(run_file, "w") as f:
                f.write(self.docker_logs_path)
        self.logger.info("Logs Path: %s", self.docker_logs_path)

        # cleanup_logs() in the base class iterates both of these. They
        # default to None, so leaving them unset turned B-01's result into
        # "TypeError: 'NoneType' object is not iterable" raised from the
        # cleanup path, which buried whatever the test had actually done.
        self.client_machines = []
        self.fio_node = []

        self.runner_k8s_log = RunnerK8sLog(
            log_dir=self.docker_logs_path, test_name=self.test_name)
        self.runner_k8s_log.start_logging()
        self.runner_k8s_log.monitor_pod_logs()

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

        # The base class does not build this; every k8s-native test makes
        # its own once the management node is known. Run 41422123643 failed
        # four cases on AttributeError here before the gate below could say
        # anything useful about the cluster.
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        self.k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
        self.logger.info("[pnfs] K8sUtils ready for mgmt_node=%r", mgmt_node)

        self.assert_pnfs_cluster()
        self.workers = self._worker_names()[:self.MAX_PODS]
        if len(self.workers) < 2:
            raise PnfsEvidenceError(
                f"a shared-filesystem test needs at least two schedulable "
                f"workers; this cluster offers {self.workers}")
        self.logger.info("[pnfs] %d worker(s) in play: %s",
                         len(self.workers), ", ".join(self.workers))

    #: The DaemonSet's own nodeSelector. A node without it runs no MDS, so
    #: a cluster with none of them cannot serve a layout however healthy the
    #: rest of it looks.
    KVM_LABEL = "storage.simplyblock.io/kvm-capable=true"

    def assert_pnfs_cluster(self):
        """F-8. Refuse the run unless the cluster can actually serve pNFS.

        Mirrors ``assert_lblk_cluster``: a suite that quietly runs as plain
        NFS is the worst outcome available, so the preconditions are asserted
        rather than discovered from a confusing failure three cases later.

        **This deliberately does not check ``spec.pnfs.mds``.** The plan said
        to, following the design document, and the operator deployed on this
        lab has no such field -- ``storageclusters.storage.simplyblock.io``
        carries no ``pnfs`` property in either v1alpha1 or v1alpha2, and
        ``ClusterDeploymentConfig`` has no NFS keys at all. The metadata
        server is not configured by us: ``NFSExport.status`` carries
        ``mdsNodeIP`` and ``mdsPodName``, so the operator selects one from
        the mds-proxy DaemonSet when it assembles the export. What we can
        assert is that there is something for it to select.
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
            f"kubectl get nodes -l {self.KVM_LABEL} "
            f"-o custom-columns=NAME:.metadata.name --no-headers "
            f"--ignore-not-found", supress_logs=True)
        kvm_nodes = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        if not kvm_nodes:
            problems.append(
                f"no node carries {self.KVM_LABEL}, so the mds-proxy "
                f"DaemonSet has nowhere to run and no metadata server exists "
                f"for the operator to put in NFSExport.status.mdsNodeIP")

        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get daemonset -n {ns} "
            f"-o custom-columns=NAME:.metadata.name,"
            f"READY:.status.numberReady,WANT:.status.desiredNumberScheduled "
            f"--no-headers", supress_logs=True)
        mds_rows = [ln.split() for ln in (out or "").splitlines()
                    if ln.split() and "mds" in ln.split()[0]]
        if not mds_rows:
            problems.append("no mds-proxy DaemonSet is deployed")
        else:
            for row in mds_rows:
                if len(row) >= 3 and (row[1] != row[2] or row[2] in ("0", "<none>")):
                    problems.append(
                        f"DaemonSet {row[0]} is {row[1]}/{row[2]} Ready")

        if problems:
            raise PnfsEvidenceError(
                "this cluster is not set up for pNFS, and running anyway "
                "would test plain NFS without saying so:\n  "
                + "\n  ".join(problems))
        self.logger.info(
            "[pnfs] cluster gate passed: NFSExport CRD present, %d "
            "kvm-capable node(s), mds-proxy Ready (%s)",
            len(kvm_nodes), ", ".join("/".join(r) for r in mds_rows))

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

    def _make_rwx_claims(self, names):
        """Create every claim first, then wait. Not one at a time.

        Provisioning an RWX claim makes an lvol, an NFSExport and a mount on
        the metadata server, which takes tens of seconds. B-08 and B-15 want
        twenty and thirty of them, so serialising create-then-wait turns a
        two-minute step into a twenty-minute one for no extra coverage.
        """
        for n in names:
            self.k8s_utils.create_pvc(
                name=n, size=self.CLAIM_SIZE, storage_class=self.sc_name,
                access_modes=["ReadWriteMany"])
            self.extra_claims.append(n)
        for n in names:
            self.k8s_utils.wait_pvc_bound(n, timeout=self.BIND_TIMEOUT)
        self.logger.info("[pnfs] %d RWX claim(s) bound: %s",
                         len(names), ", ".join(names))
        return names

    # ── a pod that stays up, for the cases fio cannot express ────────────
    #
    # fio writes bytes. B-05 needs fcntl byte-range locks, B-07 needs
    # create/rename/unlink/mkdir, B-06 needs a write on one node to be read
    # from another the moment it closes. None of those are a fio workload,
    # so they run as Python in a pod that stays alive and is driven with
    # kubectl exec.

    #: UBI, not Docker Hub, and deliberately. An anonymous Docker Hub pull
    #: has already cost this lab a run: the node could not pull the image,
    #: the pod sat in ImagePullBackOff and it read as an operator bug. The
    #: Red Hat registry needs no credential on an OpenShift node and ships
    #: python3 with fcntl, which is the whole requirement here.
    TOOLS_IMAGE = os.getenv(
        "PNFS_TOOLS_IMAGE",
        "registry.access.redhat.com/ubi9/python-311:latest")

    def _start_shell_pods(self, base, count, claim=None, spread=True):
        """*count* long-lived pods on the claim, one per node when *spread*."""
        claim = claim or self.claim
        names = []
        for i in range(count):
            name = f"{base}-{i}"
            affinity = ""
            if spread:
                affinity = (
                    "  affinity:\n"
                    "    podAntiAffinity:\n"
                    "      requiredDuringSchedulingIgnoredDuringExecution:\n"
                    "      - labelSelector:\n"
                    "          matchLabels:\n"
                    f"            {self.k8s_utils.SHARED_CLAIM_LABEL}: "
                    f"{claim}\n"
                    "        topologyKey: kubernetes.io/hostname\n")
            self.k8s_utils.apply_yaml(
                "apiVersion: v1\n"
                "kind: Pod\n"
                "metadata:\n"
                f"  name: {name}\n"
                "  labels:\n"
                f"    {self.k8s_utils.SHARED_CLAIM_LABEL}: {claim}\n"
                "spec:\n"
                f"{affinity}"
                "  restartPolicy: Never\n"
                "  containers:\n"
                "  - name: tools\n"
                f"    image: {self.TOOLS_IMAGE}\n"
                '    command: ["sleep", "infinity"]\n'
                "    volumeMounts:\n"
                "    - name: vol\n"
                f"      mountPath: {self.MOUNT}\n"
                "  volumes:\n"
                "  - name: vol\n"
                "    persistentVolumeClaim:\n"
                f"      claimName: {claim}\n")
            names.append(name)
            self.shell_pods.append(name)
        self._wait_pods_running(names)
        if spread:
            self._assert_one_pod_per_node(names, f"({base})")
        return names

    def _py(self, pod, script, timeout=300):
        """Run *script* with python3 in *pod* and return its stdout.

        The script goes in on stdin rather than as ``-c``, so it can be
        ordinary multi-line Python with no quoting games. Anything the
        payload prints to stderr is a harness failure, not a product
        verdict, so it is raised as PnfsProbeError.
        """
        heredoc = "cat <<'__PY__' | python3 -\n" + script + "\n__PY__"
        out, err = self.k8s_utils.exec_in_pod(pod, heredoc, timeout=timeout)
        if (err or "").strip():
            raise PnfsProbeError(
                f"{pod}: python payload failed: {err[-600:]}")
        return (out or "").strip()

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

    def _wait_pods_running(self, pods, timeout=600):
        """Block until every pod's container is actually up.

        ``get_job_pod_name`` returns as soon as the pod OBJECT exists, about
        a second after the Job is created and long before the image is
        pulled and the RWX volume is mounted. Run 41428476500 slept a fixed
        45s after that and exec'd into containers that did not exist yet --
        every probe failed, and the suite reported it as "not an NFS mount".
        """
        deadline = time.time() + timeout
        pending = list(pods)
        while pending and time.time() < deadline:
            still = []
            for pod in pending:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get pod {pod} -n {self.k8s_utils.namespace} "
                    f"-o custom-columns=PHASE:.status.phase,"
                    f"READY:.status.containerStatuses[*].ready "
                    f"--no-headers --ignore-not-found", supress_logs=True)
                parts = (out or "").split()
                phase = parts[0] if parts else ""
                ready = parts[1] if len(parts) > 1 else ""
                if phase == "Running" and "true" in ready.lower():
                    continue
                if phase == "Succeeded":
                    raise PnfsEvidenceError(
                        f"{pod} reached Succeeded before it could be probed; "
                        f"the workload is too short to observe. Raise the "
                        f"runtime, or probe from the node instead.")
                still.append(pod)
            pending = still
            if pending:
                sleep_n_sec(5)
        if pending:
            raise PnfsEvidenceError(
                f"pod(s) {pending} never reached Running within {timeout}s")
        self.logger.info("[pnfs] %d pod(s) running and ready", len(pods))

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

    def teardown(self, delete_lvols=True, close_ssh=True,
                 skip_k8s_cleanup=False):
        """F-10. Drain the pods before the claim.

        An RWX claim has N pods. Deleting the claim while any of them still
        holds it hangs on finalizers and leaks an NFSExport and an lvol into
        the next run, which then fails for a reason that has nothing to do
        with it.
        """
        self.logger.info("Inside %s.teardown()", type(self).__name__)
        if self.k8s_utils is None:
            # setup() failed before it got this far; there is nothing of ours
            # on the cluster to drain and the base teardown still has to run.
            super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh,
                             skip_k8s_cleanup=skip_k8s_cleanup)
            return

        # Undo first and unconditionally. B-16 takes the metadata server away
        # from the whole cluster; if that is not put back before anything
        # else, every later case in the group fails at its own bring-up gate
        # and the lab stays broken for whoever runs next.
        for undo in reversed(self.undo_stack):
            try:
                undo()
            except Exception as exc:                      # noqa: BLE001
                self.logger.error("[pnfs] UNDO FAILED, cluster may be left "
                                  "modified: %s", exc)
        self.undo_stack = []

        for j in self.jobs:
            try:
                self.k8s_utils.delete_resource("job", j)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete job %s: %s",
                                    j, exc)
        for pod in self.shell_pods:
            try:
                self.k8s_utils.delete_resource("pod", pod)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete pod %s: %s",
                                    pod, exc)
        if self.jobs or self.shell_pods:
            self._wait_pods_gone()
        for claim in [self.claim, *self.extra_claims]:
            if not claim:
                continue
            try:
                self.k8s_utils.delete_pvc(claim)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete claim %s: %s",
                                    claim, exc)
        for ns in self.extra_namespaces:
            try:
                self.k8s_utils.delete_resource("namespace", ns, namespace=None)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] could not delete namespace %s: %s",
                                    ns, exc)
        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh,
                         skip_k8s_cleanup=skip_k8s_cleanup)

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
        self._wait_pods_running(pods)

        sleep_n_sec(30)          # let the clients settle into the data path
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
            except PnfsProbeError:
                # Subclass of PnfsEvidenceError, so it has to be caught
                # first or a node we could not read becomes a node that
                # did no IO -- the harness failure reported as a product
                # one, which is the single worst outcome for this test.
                raise
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


# ── B-05 ─────────────────────────────────────────────────────────────────

class PnfsByteRangeLockK8s(_PnfsBase):
    """B-05: six pods increment one counter under fcntl byte-range locks.

    A shared filesystem that serves bytes correctly can still fail to
    serialise. The counter is the assertion: every pod does ROUNDS
    read-modify-writes of the same eight bytes under an exclusive
    ``fcntl`` lock, so the only correct final value is pods x ROUNDS.
    Any lost update means two pods held the range at once, which on pNFS
    means the metadata server granted overlapping locks or a client
    ignored one.

    Deliberately NOT ``flock``: that locks the whole open file and is a
    different kernel path. The specification and the upstream case are
    about byte ranges.
    """

    ROUNDS = 40

    WORKER = """
import fcntl, os, struct, sys, time
path = "{mount}/b05-counter.bin"
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
for _ in range({rounds}):
    for attempt in range(600):
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX, 8, 0, os.SEEK_SET)
            break
        except OSError:
            time.sleep(0.05)
    else:
        sys.stderr.write("could not take the lock in 30s")
        raise SystemExit(1)
    os.lseek(fd, 0, os.SEEK_SET)
    raw = os.read(fd, 8)
    val = struct.unpack("<Q", raw)[0] if len(raw) == 8 else 0
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, struct.pack("<Q", val + 1))
    os.fsync(fd)
    fcntl.lockf(fd, fcntl.LOCK_UN, 8, 0, os.SEEK_SET)
os.close(fd)
print("done")
"""

    READ = """
import os, struct
fd = os.open("{mount}/b05-counter.bin", os.O_RDONLY)
print(struct.unpack("<Q", os.read(fd, 8))[0])
"""

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b05")
        pods = self._start_shell_pods("pnfs-b05-lock", len(self.workers))

        # Create and zero the counter once, before anyone contends for it.
        self._py(pods[0], f'''
import os, struct
fd = os.open("{self.MOUNT}/b05-counter.bin", os.O_RDWR | os.O_CREAT, 0o644)
os.write(fd, struct.pack("<Q", 0))
os.fsync(fd); os.close(fd)
print("zeroed")
''')

        # Start every pod before waiting on any of them, or they take the
        # lock in sequence and the test proves nothing about contention.
        errors, done = [], []

        def drive(pod):
            try:
                self._py(pod, self.WORKER.format(mount=self.MOUNT,
                                                 rounds=self.ROUNDS),
                         timeout=self.JOB_TIMEOUT)
                done.append(pod)
            except Exception as exc:                      # noqa: BLE001
                errors.append(f"{pod}: {exc}")

        threads = [threading.Thread(target=drive, args=(p,)) for p in pods]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self.JOB_TIMEOUT)
        if errors:
            raise PnfsEvidenceError(
                "a locking worker failed:\n  " + "\n  ".join(errors))

        final = int(self._py(pods[0], self.READ.format(mount=self.MOUNT)))
        expected = len(pods) * self.ROUNDS
        if final != expected:
            raise PnfsEvidenceError(
                f"byte-range locks did not serialise (B-05): the counter "
                f"reads {final}, every one of {len(pods)} pods did "
                f"{self.ROUNDS} locked increments so it must read "
                f"{expected}. {expected - final} update(s) were lost, which "
                f"means two clients held the same range at once.")
        self.logger.info(
            "[B-05] PASS: %d pods x %d locked increments = %d, no lost "
            "update", len(pods), self.ROUNDS, final)


# ── B-06 ─────────────────────────────────────────────────────────────────

class PnfsCloseToOpenK8s(_PnfsBase):
    """B-06: close on one node, open on another, and never see stale data.

    NFS close-to-open says a reader that opens a file after the writer
    closed it sees the writer's data. B-02 proves bulk data crosses the
    wire; this proves the *coherence point* is right, including on a
    rewrite, which is where an attribute cache goes wrong.
    """

    GENERATIONS = 3

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b06")
        if len(self.workers) < 2:
            raise PnfsEvidenceError("B-06 needs two nodes")
        writer, reader = self._start_shell_pods("pnfs-b06", 2)

        path = f"{self.MOUNT}/b06-marker.txt"
        for gen in range(1, self.GENERATIONS + 1):
            payload = f"generation-{gen}-" + ("x" * 64 * gen)
            self._py(writer, f'''
import os
fd = os.open("{path}", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, {payload!r}.encode())
os.fsync(fd)
os.close(fd)
print("wrote")
''')
            # No sleep on purpose. A sleep here would let an attribute cache
            # expire and turn a coherence bug into a pass.
            got = self._py(reader, f'''
with open("{path}") as f:
    print(f.read(), end="")
''')
            if got != payload:
                raise PnfsEvidenceError(
                    f"close-to-open broke at generation {gen} (B-06): the "
                    f"reader on a different node saw "
                    f"{got[:40]!r}... where the writer had just closed "
                    f"{payload[:40]!r}.... A reader that opens after a close "
                    f"must see that close.")
            self.logger.info("[B-06] generation %d coherent across nodes",
                             gen)
        self.logger.info(
            "[B-06] PASS: %d write-close/open-read generations, reader on a "
            "different node than the writer, no stale read",
            self.GENERATIONS)


# ── B-07 ─────────────────────────────────────────────────────────────────

class PnfsMetadataStormK8s(_PnfsBase):
    """B-07: concurrent create, rename, unlink and mkdir from every node.

    Everything else in Track B is a data path test. The metadata server is
    a single kernel nfsd in a guest, and the namespace operations all land
    on it at once. This is the only case that loads that side.
    """

    OPS = 120

    WORKER = """
import os, sys
root = "{mount}/b07"
mine = os.path.join(root, "w{idx}")
os.makedirs(mine, exist_ok=True)
made = 0
for i in range({ops}):
    f = os.path.join(mine, "f%d" % i)
    with open(f, "w") as fh:
        fh.write("x" * 128)
    os.rename(f, f + ".renamed")
    d = os.path.join(mine, "d%d" % i)
    os.mkdir(d)
    os.rmdir(d)
    if i % 2 == 0:
        os.unlink(f + ".renamed")
    else:
        made += 1
# Also touch the shared directory every node writes into, which is where
# a namespace race would actually show up.
with open(os.path.join(root, "shared-w{idx}"), "w") as fh:
    fh.write("ok")
print(made)
"""

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b07")
        pods = self._start_shell_pods("pnfs-b07", len(self.workers))
        self._py(pods[0], f'''
import os
os.makedirs("{self.MOUNT}/b07", exist_ok=True)
print("root made")
''')

        errors, survivors = [], {}

        def drive(idx, pod):
            try:
                out = self._py(pod, self.WORKER.format(
                    mount=self.MOUNT, idx=idx, ops=self.OPS),
                    timeout=self.JOB_TIMEOUT)
                survivors[pod] = int(out.splitlines()[-1])
            except Exception as exc:                      # noqa: BLE001
                errors.append(f"{pod}: {exc}")

        threads = [threading.Thread(target=drive, args=(i, p))
                   for i, p in enumerate(pods)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self.JOB_TIMEOUT)
        if errors:
            raise PnfsEvidenceError(
                "a metadata worker failed (B-07):\n  " + "\n  ".join(errors))

        # Every pod kept the odd-numbered files. Count them from a different
        # pod than the one that made them, so the listing crosses the wire.
        listing = self._py(pods[-1], f'''
import os
root = "{self.MOUNT}/b07"
for d in sorted(os.listdir(root)):
    p = os.path.join(root, d)
    if os.path.isdir(p):
        print(d, len(os.listdir(p)))
''')
        counts = dict(line.split() for line in listing.splitlines())
        bad = []
        for i, pod in enumerate(pods):
            want = survivors[pod]
            got = int(counts.get(f"w{i}", -1))
            if got != want:
                bad.append(f"w{i}: directory holds {got}, worker kept {want}")
        if bad:
            raise PnfsEvidenceError(
                "the namespace does not agree with what the workers did "
                "(B-07):\n  " + "\n  ".join(bad))
        self.logger.info(
            "[B-07] PASS: %d pods x %d create/rename/mkdir/rmdir/unlink "
            "cycles, namespace consistent when read from another node",
            len(pods), self.OPS)


# ── B-08 ─────────────────────────────────────────────────────────────────

class PnfsFsidUniquenessK8s(_PnfsBase):
    """B-08: many RWX claims on one metadata server, no duplicate fsid.

    One nfsd exports all of them out of one kernel. Two exports sharing an
    fsid is a silent cross-wiring: a client can be handed the wrong
    filesystem for a handle. It cannot be seen from the data path, only
    from the NFSExport records, which is why this is its own case.
    """

    CLAIMS = 20

    def run(self):
        self._make_storage_class()
        names = [f"pnfs-b08-{i:02d}" for i in range(self.CLAIMS)]
        self._make_rwx_claims(names)

        exports = nfsexport_status(self.k8s_utils) or []
        mine = {}
        for item in exports:
            n = (item.get("metadata") or {}).get("name") or ""
            spec = item.get("spec") or {}
            status = item.get("status") or {}
            claim = (spec.get("claimRef") or {}).get("name") or ""
            match = next((c for c in names if c == claim or c in n), None)
            if not match:
                continue
            fsid = (status.get("fsid") or spec.get("fsid")
                    or status.get("Fsid") or "")
            mine[match] = (n, str(fsid))

        missing = [c for c in names if c not in mine]
        if missing:
            raise PnfsEvidenceError(
                f"{len(missing)} of {self.CLAIMS} bound claims have no "
                f"NFSExport (B-08): {missing[:5]}")

        blank = [c for c, (_n, f) in mine.items() if not f]
        if blank:
            raise PnfsProbeError(
                f"the NFSExport records carry no fsid field, so this says "
                f"nothing about uniqueness. Fix the probe, not the product: "
                f"{len(blank)} of {self.CLAIMS} blank, e.g. {blank[:3]}")

        seen = {}
        for claim, (_n, fsid) in sorted(mine.items()):
            seen.setdefault(fsid, []).append(claim)
        dupes = {f: c for f, c in seen.items() if len(c) > 1}
        if dupes:
            raise PnfsEvidenceError(
                f"two or more exports share an fsid on one metadata server "
                f"(B-08), which can hand a client the wrong filesystem: "
                f"{dupes}")
        self.logger.info("[B-08] PASS: %d claims, %d distinct fsids",
                         len(mine), len(seen))


# ── B-09 ─────────────────────────────────────────────────────────────────

class PnfsOnlineResizeK8s(_PnfsBase):
    """B-09: grow the claim under load; every pod sees it, nothing stalls.

    A shared filesystem grows once and every client has to notice. The
    interesting failure is not the grow, it is a client left on the old
    size, which then gets ENOSPC on a filesystem that has room.
    """

    START_SIZE = "20Gi"
    GROWN_SIZE = "30Gi"

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b09")
        pods = self._start_shell_pods("pnfs-b09", len(self.workers))

        before = self._statfs_everywhere(pods)
        self.logger.info("[B-09] size before: %s", before)

        self.k8s_utils.resize_pvc("pnfs-b09", self.GROWN_SIZE)

        deadline = time.time() + 600
        after = {}
        while time.time() < deadline:
            after = self._statfs_everywhere(pods)
            if all(after[p] > before[p] for p in pods):
                break
            sleep_n_sec(10)

        flat = [p for p in pods if after.get(p, 0) <= before[p]]
        if flat:
            raise PnfsEvidenceError(
                f"the filesystem grew but {len(flat)} of {len(pods)} client "
                f"(s) still report the old size (B-09): before={before}, "
                f"after={after}. Those clients will hit ENOSPC on a "
                f"filesystem with room.")

        # And it is still writable from every node afterwards.
        for i, pod in enumerate(pods):
            self._py(pod, f'''
with open("{self.MOUNT}/b09-after-{i}.dat", "wb") as f:
    f.write(b"y" * 1048576)
print("ok")
''')
        self.logger.info(
            "[B-09] PASS: grown %s -> %s, all %d clients see it and still "
            "write", self.START_SIZE, self.GROWN_SIZE, len(pods))

    def _statfs_everywhere(self, pods):
        out = {}
        for pod in pods:
            v = self._py(pod, f'''
import os
s = os.statvfs("{self.MOUNT}")
print(s.f_blocks * s.f_frsize)
''')
            out[pod] = int(v)
        return out


# ── B-10 ─────────────────────────────────────────────────────────────────

class PnfsSnapshotRestoreK8s(_PnfsBase):
    """B-10: snapshot an RWX claim, restore to a block claim, read the marker.

    Restoring a pNFS snapshot *into* another pNFS claim is refused by
    design, so this asserts the refusal rather than treating it as a bug,
    and proves the data is recoverable through the block path.
    """

    BLOCK_SC = "sb-pnfs-restore-sc"
    MARKER = "b10-marker-f0f0f0"

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b10")
        pods = self._start_shell_pods("pnfs-b10", min(2, len(self.workers)))
        self._py(pods[0], f'''
import os
with open("{self.MOUNT}/b10-marker.txt", "w") as f:
    f.write("{self.MARKER}")
    f.flush()
    os.fsync(f.fileno())
print("marker written")
''')

        self.k8s_utils.create_volume_snapshot_class()
        self.k8s_utils.create_volume_snapshot("pnfs-b10-snap", "pnfs-b10")
        self.logger.info("[B-10] snapshot taken under an open mount")

        # A clone back into pNFS must be refused, not silently provisioned.
        self.k8s_utils.create_clone_pvc(
            "pnfs-b10-clone-pnfs", self.CLAIM_SIZE, self.sc_name,
            "pnfs-b10-snap")
        self.extra_claims.append("pnfs-b10-clone-pnfs")
        refused = self._claim_stays_pending("pnfs-b10-clone-pnfs", 180)
        if not refused:
            raise PnfsEvidenceError(
                "a clone of a pNFS snapshot back into a pNFS StorageClass "
                "bound (B-10). The design refuses this; a silent bind means "
                "two filesystems share one fsid lineage.")
        self.logger.info("[B-10] clone into pNFS refused, as designed")

        # Into a block XFS class it must work, and carry the marker.
        pool = self.sbcli_utils.add_storage_pool(self.pool_name) \
            or self.pool_name
        self.k8s_utils.create_storage_class(
            name=self.BLOCK_SC, cluster_id=self.cluster_id, pool_name=pool,
            fs_type="xfs")
        self.k8s_utils.create_clone_pvc(
            "pnfs-b10-restore", self.CLAIM_SIZE, self.BLOCK_SC,
            "pnfs-b10-snap")
        self.extra_claims.append("pnfs-b10-restore")
        self.k8s_utils.wait_pvc_bound("pnfs-b10-restore",
                                      timeout=self.BIND_TIMEOUT)
        reader = self._start_shell_pods(
            "pnfs-b10-restored", 1, claim="pnfs-b10-restore", spread=False)[0]
        got = self._py(reader, f'''
with open("{self.MOUNT}/b10-marker.txt") as f:
    print(f.read(), end="")
''')
        if got != self.MARKER:
            raise PnfsEvidenceError(
                f"the restored block claim does not carry the marker "
                f"(B-10): read {got[:60]!r}, wrote {self.MARKER!r}")
        self.logger.info(
            "[B-10] PASS: snapshot under load, clone into pNFS refused, "
            "restore into XFS carries the marker")

    def _claim_stays_pending(self, name, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline:
            phase = (self.k8s_utils.get_pvc_status(name)
                     or {}).get("phase", "")
            if phase == "Bound":
                return False
            sleep_n_sec(10)
        return True


# ── B-11 ─────────────────────────────────────────────────────────────────

class PnfsDeleteLeakGateK8s(_PnfsBase):
    """B-11: deleting the claim takes the export and the lvol with it.

    The leak gate. Everything else in Track B creates; this is the only
    case that proves the cluster goes back to where it started, and a leak
    here is what makes the *next* run fail for an unrelated-looking reason.
    """

    def run(self):
        self._make_storage_class()
        claim = self._make_rwx_claim("pnfs-b11")

        pv = self.k8s_utils.get_pvc_volume_handle(claim)
        if not pv:
            raise PnfsProbeError(
                "could not read the bound PV name for the claim, so a leak "
                "check would be meaningless. Fix the probe.")
        jobs = self._start_shared_load(
            "pnfs-b11-fio", [self._pod_config(i, size="64M")
                             for i in range(len(self.workers))])
        self._await_jobs(jobs, "B-11 warm-up")

        export = nfsexport_status(self.k8s_utils, pvc_name=claim)
        if export is None:
            raise PnfsEvidenceError(
                f"no NFSExport exists for the bound claim {claim} (B-11), "
                f"so the delete path cannot be judged")

        for j in jobs:
            self.k8s_utils.delete_resource("job", j)
        self.jobs = []
        self._wait_pods_gone()
        self.k8s_utils.delete_pvc(claim)
        self.claim = None

        leaks, deadline = None, time.time() + 420
        while time.time() < deadline:
            leaks = []
            if nfsexport_status(self.k8s_utils, pvc_name=claim) is not None:
                leaks.append("the NFSExport still exists")
            if pv in (self.k8s_utils.list_lvols() or {}):
                leaks.append(f"the lvol named {pv} still exists")
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pv {pv} --no-headers --ignore-not-found",
                supress_logs=True)
            if (out or "").strip():
                leaks.append(f"the PV {pv} still exists")
            if not leaks:
                break
            sleep_n_sec(15)

        if leaks:
            raise PnfsEvidenceError(
                "deleting the claim left state behind (B-11), which is what "
                "makes the next run fail for an unrelated reason:\n  "
                + "\n  ".join(leaks))
        self.logger.info(
            "[B-11] PASS: claim, NFSExport, PV and lvol %s all gone", pv)


# ── B-12 ─────────────────────────────────────────────────────────────────

class PnfsScaleThirtyPodsK8s(_PnfsBase):
    """B-12: thirty pods on one claim, five deep on every node.

    B-01 is one pod per node. This is the same claim oversubscribed, which
    is where per-client state on the metadata server runs out rather than
    where the data path does.
    """

    PODS = 30

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b12")
        configs = [self._pod_config(i, size="64M") for i in range(self.PODS)]
        # required_spread would be unsatisfiable: thirty pods, six nodes.
        jobs = self._start_shared_load("pnfs-b12-fio", configs,
                                       required_spread=False)
        pods = self._pods_of(jobs)
        self._wait_pods_running(pods)
        placement = {p: self._pod_node(p) for p in pods}
        nodes = {n for n in placement.values() if n}
        if len(nodes) < len(self.workers):
            raise PnfsEvidenceError(
                f"thirty pods landed on only {len(nodes)} of "
                f"{len(self.workers)} nodes, so this is not the scale case: "
                f"{sorted(nodes)}")
        self._await_jobs(jobs, "B-12 scale")
        self.logger.info(
            "[B-12] PASS: %d pods across %d nodes on one claim, every "
            "verify clean", self.PODS, len(nodes))


# ── B-13 ─────────────────────────────────────────────────────────────────

class PnfsMixedRwxRwoK8s(_PnfsBase):
    """B-13: RWX and RWO from the same pool, under load, at the same time.

    The regression gate for the existing product. pNFS adds a metadata
    server and a second mount path to a cluster whose other volumes are
    plain NVMe block; this proves the new thing does not disturb them.
    """

    BLOCK_SC = "sb-pnfs-block-sc"

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim("pnfs-b13")

        pool = self.sbcli_utils.add_storage_pool(self.pool_name) \
            or self.pool_name
        self.k8s_utils.create_storage_class(
            name=self.BLOCK_SC, cluster_id=self.cluster_id, pool_name=pool,
            fs_type="xfs")
        self.k8s_utils.create_pvc(name="pnfs-b13-rwo", size="10Gi",
                                  storage_class=self.BLOCK_SC)
        self.extra_claims.append("pnfs-b13-rwo")
        self.k8s_utils.wait_pvc_bound("pnfs-b13-rwo",
                                      timeout=self.BIND_TIMEOUT)

        rwx_jobs = self._start_shared_load(
            "pnfs-b13-rwx", [self._pod_config(i, size="128M")
                             for i in range(len(self.workers))])
        rwo_jobs = self._start_shared_load(
            "pnfs-b13-rwo-fio", [self._pod_config(99, size="128M")],
            required_spread=False)
        # _start_shared_load records into self.jobs; keep both sets.
        self.jobs = list(rwx_jobs) + list(rwo_jobs)

        self._await_jobs(rwx_jobs, "B-13 RWX lane")
        self._await_jobs(rwo_jobs, "B-13 RWO lane")
        self.logger.info(
            "[B-13] PASS: %d RWX pods and the RWO block claim both finished "
            "clean while sharing the pool", len(rwx_jobs))


# ── B-14 ─────────────────────────────────────────────────────────────────

class PnfsTwoNamespacesK8s(_PnfsBase):
    """B-14: the same claim name in two namespaces is two filesystems.

    Export paths are derived from the claim, and a derivation that forgets
    the namespace gives two tenants one filesystem. The failure is silent
    and it is a data-exposure bug, not an availability one.
    """

    OTHER_NS = "pnfs-b14-second"
    CLAIM = "pnfs-b14"

    def run(self):
        self._make_storage_class()
        self._make_rwx_claim(self.CLAIM)

        self.k8s_utils.apply_yaml_cluster_scoped(
            f"apiVersion: v1\nkind: Namespace\nmetadata:\n"
            f"  name: {self.OTHER_NS}\n")
        self.extra_namespaces.append(self.OTHER_NS)
        self.k8s_utils.create_pvc(
            name=self.CLAIM, size=self.CLAIM_SIZE,
            storage_class=self.sc_name, namespace=self.OTHER_NS,
            access_modes=["ReadWriteMany"])
        self.k8s_utils.wait_pvc_bound(self.CLAIM, timeout=self.BIND_TIMEOUT,
                                      namespace=self.OTHER_NS)
        self.logger.info("[B-14] the same claim name bound in both namespaces")

        a = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM)
        b = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM,
                             namespace=self.OTHER_NS)
        if a is None or b is None:
            raise PnfsEvidenceError(
                f"one of the two namespaces has no NFSExport (B-14): "
                f"default={a is not None}, {self.OTHER_NS}={b is not None}")

        def ident(item):
            spec, status = item.get("spec") or {}, item.get("status") or {}
            return (str(status.get("path") or spec.get("path") or ""),
                    str(status.get("fsid") or spec.get("fsid") or ""),
                    (item.get("metadata") or {}).get("name") or "")

        ia, ib = ident(a), ident(b)
        if ia == ib:
            raise PnfsEvidenceError(
                f"the two namespaces resolved to the same export (B-14): "
                f"{ia}. Two tenants would be sharing one filesystem.")
        if ia[0] and ia[0] == ib[0]:
            raise PnfsEvidenceError(
                f"the two namespaces share an export path (B-14): {ia[0]}")
        if ia[1] and ia[1] == ib[1]:
            raise PnfsEvidenceError(
                f"the two namespaces share an fsid (B-14): {ia[1]}")
        self.logger.info(
            "[B-14] PASS: %r and %r are separate exports", ia, ib)

    def teardown(self, delete_lvols=True, close_ssh=True,
                 skip_k8s_cleanup=False):
        # The claim in the second namespace is not in extra_claims, which
        # the base only deletes from the default namespace.
        if self.k8s_utils is not None:
            try:
                self.k8s_utils.delete_pvc(self.CLAIM, namespace=self.OTHER_NS)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[pnfs] second-namespace claim: %s", exc)
        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh,
                         skip_k8s_cleanup=skip_k8s_cleanup)


# ── B-15 ─────────────────────────────────────────────────────────────────

class PnfsClaimCeilingK8s(_PnfsBase):
    """B-15: thirty claims on one metadata server, then one more.

    A boundary case, and the answer is allowed to be either. What is not
    allowed is the thirty-first binding and then not working, so the
    assertion is that whatever happens is *consistent*: it binds and
    serves, or it stays Pending and says why.
    """

    CLAIMS = 30

    def run(self):
        self._make_storage_class()
        names = [f"pnfs-b15-{i:02d}" for i in range(self.CLAIMS)]
        self._make_rwx_claims(names)
        self.logger.info("[B-15] %d claims bound on one metadata server",
                         self.CLAIMS)

        extra = f"pnfs-b15-{self.CLAIMS:02d}"
        self.k8s_utils.create_pvc(
            name=extra, size=self.CLAIM_SIZE, storage_class=self.sc_name,
            access_modes=["ReadWriteMany"])
        self.extra_claims.append(extra)

        bound, deadline = False, time.time() + 300
        while time.time() < deadline:
            if (self.k8s_utils.get_pvc_status(extra)
                    or {}).get("phase") == "Bound":
                bound = True
                break
            sleep_n_sec(10)

        if bound:
            pod = self._start_shell_pods("pnfs-b15-probe", 1, claim=extra,
                                         spread=False)[0]
            self._py(pod, f'''
with open("{self.MOUNT}/b15.dat", "wb") as f:
    f.write(b"z" * 1048576)
print("ok")
''')
            self.logger.info(
                "[B-15] PASS: claim %d bound and serves; no ceiling at %d",
                self.CLAIMS + 1, self.CLAIMS + 1)
            return

        export = nfsexport_status(self.k8s_utils, pvc_name=extra)
        reason = ""
        if export:
            status = export.get("status") or {}
            reason = str(status.get("phase") or status.get("reason") or "")
        if not reason:
            raise PnfsEvidenceError(
                f"claim {self.CLAIMS + 1} neither bound nor said why "
                f"(B-15). A ceiling is acceptable; a silent one is not: the "
                f"claim is Pending and its NFSExport reports {export!r}")
        self.logger.info(
            "[B-15] PASS: ceiling at %d claims, reported as %r rather than "
            "silently", self.CLAIMS, reason)


# ── B-16 ─────────────────────────────────────────────────────────────────

class PnfsNoMetadataServerK8s(_PnfsBase):
    """B-16: with no metadata server configured, say so; do not provision.

    The negative case. It takes ``spec.pnfs.mds`` off the driver for the
    length of the test, so it must run alone and it must put it back. The
    undo is registered before the mutation and runs from teardown even if
    run() raises, because a lab left with no metadata server fails every
    pNFS case after it at the bring-up gate.
    """

    DRIVER = "simplyblock"
    CLAIM = "pnfs-b16"

    def run(self):
        self._make_storage_class()

        current, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get simplyblockdriver {self.DRIVER} "
            f"-n {self.k8s_utils.namespace} "
            f"-o jsonpath='{{.spec.pnfs}}'", supress_logs=True)
        current = (current or "").strip().strip("'")
        if not current or "mds" not in current:
            raise PnfsProbeError(
                f"spec.pnfs.mds is not set on driver {self.DRIVER} to begin "
                f"with, so removing it proves nothing. Read: {current!r}")

        def restore():
            self.logger.info("[B-16] restoring spec.pnfs on the driver")
            self.k8s_utils._exec_kubectl(
                f"kubectl patch simplyblockdriver {self.DRIVER} "
                f"-n {self.k8s_utils.namespace} --type=merge "
                f"-p '{{\"spec\":{{\"pnfs\":{current}}}}}'")
        self.undo_stack.append(restore)

        self.k8s_utils._exec_kubectl(
            f"kubectl patch simplyblockdriver {self.DRIVER} "
            f"-n {self.k8s_utils.namespace} --type=json "
            f"-p '[{{\"op\":\"remove\",\"path\":\"/spec/pnfs/mds\"}}]'")
        self.logger.info("[B-16] spec.pnfs.mds removed")
        sleep_n_sec(30)

        self.k8s_utils.create_pvc(
            name=self.CLAIM, size=self.CLAIM_SIZE,
            storage_class=self.sc_name, access_modes=["ReadWriteMany"])
        self.extra_claims.append(self.CLAIM)

        deadline = time.time() + 240
        while time.time() < deadline:
            if (self.k8s_utils.get_pvc_status(self.CLAIM)
                    or {}).get("phase") == "Bound":
                raise PnfsEvidenceError(
                    "the claim bound with no metadata server configured "
                    "(B-16). It cannot be served, so a bind here is a "
                    "promise the cluster cannot keep.")
            sleep_n_sec(10)

        export = nfsexport_status(self.k8s_utils, pvc_name=self.CLAIM)
        said = ""
        if export:
            status = export.get("status") or {}
            said = " ".join(str(status.get(k) or "")
                            for k in ("phase", "reason", "message"))
        if "NoMetadataServer" not in said:
            raise PnfsEvidenceError(
                f"the claim correctly stayed Pending, but nothing said why "
                f"(B-16). Expected NoMetadataServer; the export reports "
                f"{said!r}. A operator who cannot see the cause has to guess.")
        self.logger.info(
            "[B-16] PASS: claim stayed Pending and the export reported %r",
            said.strip())
