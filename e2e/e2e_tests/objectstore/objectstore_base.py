"""Shared machinery for the ObjectStore cases.

Bring-up, the gate that refuses to run a case on a cluster that cannot serve
one, the CRD builders, and a teardown that puts the cluster back.

The gate matters more here than in most suites. An `S3Bucket` whose
StorageClass does not select the pnfs fstype never leaves `Pending`, and a
whole group failing on "never reached Ready" nine hundred seconds at a time
says nothing about the product. :meth:`_ObjectStoreBase.assert_s3_cluster`
fails once, early, naming which prerequisite is missing.
"""
import os
import time
from datetime import UTC, datetime

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils import s3_utils as s3
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils
from utils.ssh_utils import RunnerK8sLog


class _ObjectStoreBase(TestClusterBase):
    """Bring-up, builders and teardown shared by every ObjectStore case."""

    # ── knobs ────────────────────────────────────────────────────────────

    #: The gateway image. Overridable because the gateway is released from a
    #: different repository than the operator, so the pair under test is not
    #: always the pair the sample manifest names.
    GATEWAY_IMAGE = os.getenv(
        "S3_GATEWAY_IMAGE", "registry.simplyblock.ai/simplyblock/gateway:main")
    GATEWAY_PORT = int(os.getenv("S3_GATEWAY_PORT", "9000"))

    #: Root credentials. The gateway refuses to start without both, so the
    #: Secret is required rather than decorative (the CRD sample says so).
    ROOT_ACCESS = os.getenv("S3_ROOT_ACCESS_KEY", "simplyblock")
    ROOT_SECRET = os.getenv("S3_ROOT_SECRET_KEY", "simplyblockSecret123")

    #: Per-bucket backing volume. Small on purpose: these cases create and
    #: destroy a lot of them, and a 10 GiB default would spend the lab's
    #: capacity proving nothing.
    BUCKET_BYTES = 2 * 1024 ** 3
    SC_NAME = "sb-s3-pnfs-sc"
    STORE = "s3-fn"
    REGION = "us-east-1"

    #: The service-ca bundle the OpenShift TLS provider mounts into every
    #: gateway pod. Mirrors `certificateAuthorityConfigMap` in the operator's
    #: objectstore/objectstore_build.go, which projects it alongside the
    #: serving-cert Secret; the two must move together. It is rendered only by
    #: the operator chart's controlplane-cabundle.yaml, which is itself gated
    #: on `tls.enabled AND tls.provider == openshift` -- so its presence is
    #: the one honest answer to which provider this deployment can serve.
    CA_BUNDLE_CM = "simplyblock-certificate-authority"

    #: Provisioning a bucket makes a PVC, an lvol, an NFSExport and a mount
    #: on every gateway node, and on a cluster whose MDS guest has not booted
    #: yet the MDS boot dominates. Dev's own E-05 exists to measure exactly
    #: that, so the budget here is generous rather than tight.
    BUCKET_TIMEOUT = 900
    STORE_TIMEOUT = 600

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.k8s_test = True
        self.k8s_utils = None
        self.ns = None
        self.client_pod = None
        self.bao = None
        # Everything created by a case, cleaned in teardown even when run()
        # raised part way. A leaked S3Bucket leaks a PVC, an NFSExport and an
        # lvol with it, and the next case fails for a reason that is not its
        # own.
        self.buckets = []
        self.stores = []
        self.extra_pods = []
        self.extra_secrets = []
        self.extra_storage_classes = []
        self.extra_namespaces = []
        #: Callables run in reverse at teardown. Several cases mutate shared
        #: objects (the gateway's replica count, a node's schedulability), and
        #: a mutation with no guaranteed undo leaves the lab broken for whoever
        #: runs next.
        self.undo_stack = []

    # ── setup ────────────────────────────────────────────────────────────

    def setup(self):
        self.logger.info("Inside %s.setup()", type(self).__name__)

        # TestClusterBase.setup() reaches for SSH to worker nodes a k8s-native
        # run does not have, so it is skipped -- but RUN_DIR_FILE has to be
        # written by hand or RUN_BASE_DIR is empty everywhere downstream and
        # the run leaves no folder on the share at all.
        self.test_start_time_utc = datetime.now(UTC)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.docker_logs_path = os.path.join(
            self.nfs_log_base, f"{self.test_name}-{timestamp}")
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        os.makedirs(self.docker_logs_path, exist_ok=True)

        run_file = os.getenv("RUN_DIR_FILE", None)
        if run_file:
            with open(run_file, "w") as fh:
                fh.write(self.docker_logs_path)
        self.logger.info("Logs Path: %s", self.docker_logs_path)

        # cleanup_logs() in the base iterates both; they default to None and
        # a bare None turns a case's result into "'NoneType' is not iterable"
        # raised from the cleanup path, burying whatever the case found.
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

        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        self.k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
        self.ns = self.k8s_utils.namespace
        self.logger.info("[s3] K8sUtils ready, namespace=%s", self.ns)

        self.assert_s3_cluster()

    # ── the gate ─────────────────────────────────────────────────────────

    #: The MDS DaemonSet's own nodeSelector. A cluster with no node carrying
    #: it runs no metadata server, so no pnfs volume can ever be served
    #: however healthy everything else looks.
    KVM_LABEL = "storage.simplyblock.io/kvm-capable=true"

    def assert_s3_cluster(self):
        """Refuse to run on a cluster that cannot serve a bucket.

        Four prerequisites, each of which produces the identical symptom when
        missing -- a bucket that sits in ``Pending`` until the case's timeout
        -- so they are checked by name instead.
        """
        missing = []

        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get crd objectstores.storage.simplyblock.io "
            "s3buckets.storage.simplyblock.io "
            "-o jsonpath='{range .items[*]}{.metadata.name}{\"\\n\"}{end}' "
            "2>/dev/null || true", supress_logs=True)
        have_crds = {ln.strip() for ln in (out or "").splitlines() if ln.strip()}
        for crd in ("objectstores.storage.simplyblock.io",
                    "s3buckets.storage.simplyblock.io"):
            if crd not in have_crds:
                missing.append(
                    f"CRD {crd} is not installed -- this operator build predates "
                    f"the ObjectStore work, so nothing in this suite applies")

        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get nodes -l {self.KVM_LABEL} "
            f"-o jsonpath='{{range .items[*]}}{{.metadata.name}} {{end}}' "
            f"2>/dev/null || true", supress_logs=True)
        if not (out or "").strip():
            missing.append(
                f"no node carries {self.KVM_LABEL}, so no MDS guest can run "
                f"and every bucket's pnfs volume stays Pending")

        # -n is not optional here. SimplyblockDriver is namespaced, and the
        # two checks above are not: CRDs and nodes are cluster-scoped, so this
        # is the one query in the probe that needs a namespace and the one
        # that was missing it. Without it kubectl looks in the context's
        # default namespace, finds nothing, and `2>/dev/null || true` turns
        # that NotFound into an empty string -- which reads here exactly like
        # a driver that genuinely has no pnfs block.
        #
        # Run 44289765204 failed all eighteen cases that way, on a cluster
        # whose driver was carrying {"mds":{"resources":{},"stateSize":"1Gi"}}
        # the whole time, patched on by the workflow step four steps earlier.
        # So keep stderr and say what it said: an empty answer and a refused
        # question are different failures and must not read the same.
        out, err = self.k8s_utils._exec_kubectl(
            f"kubectl get simplyblockdriver simplyblock -n {self.ns} "
            "-o jsonpath='{.spec.pnfs}' || true", supress_logs=True)
        if not (out or "").strip():
            detail = next((ln.strip() for ln in reversed((err or "").splitlines())
                           if ln.strip()), "")
            missing.append(
                f"SimplyblockDriver/simplyblock in namespace {self.ns} has no "
                f"spec.pnfs -- the pnfs fstype is not enabled on this cluster"
                + (f" (kubectl said: {detail[:160]})" if detail else ""))

        if missing:
            raise s3.S3ProbeError(
                "this cluster cannot serve an S3 bucket, so nothing below "
                "would mean anything:\n  - " + "\n  - ".join(missing))

        self.workers = self.k8s_utils.get_all_k8s_node_names()
        self.logger.info("[s3] prerequisites satisfied; %d node(s)",
                         len(self.workers))

    # ── builders ─────────────────────────────────────────────────────────

    def make_storage_class(self, name=None, expansion=True):
        """The pnfs StorageClass every bucket's backing volume uses.

        ``volumeBindingMode: Immediate`` rather than the default: the backing
        volume has no consumer pod of its own -- the gateway reaches it through
        a delivered mount, not a PVC reference -- so ``WaitForFirstConsumer``
        would wait forever.
        """
        name = name or self.SC_NAME
        pool = self.sbcli_utils.add_storage_pool(self.pool_name) or self.pool_name
        self.k8s_utils.create_storage_class(
            name=name, cluster_id=self.cluster_id, pool_name=pool,
            fs_type="pnfs")
        # create_storage_class() does not expose either field, and both matter
        # here: without expansion TC-S3-004 cannot grow a bucket, and without
        # Immediate nothing binds at all.
        self.k8s_utils._exec_kubectl(
            f"kubectl patch storageclass {name} --type=merge "
            f"-p '{{\"allowVolumeExpansion\": {str(bool(expansion)).lower()}, "
            f"\"volumeBindingMode\": \"Immediate\"}}' 2>/dev/null || true")
        if name not in self.extra_storage_classes:
            self.extra_storage_classes.append(name)
        return name

    def make_credentials(self, name="s3-root-credentials", vault_url=None,
                         access=None, secret=None, namespace=None):
        """The Secret the gateway's ``envFrom`` points at.

        ``VGW_IAM_VAULT_ENDPOINT_URL`` is what opts the gateway into
        Vault-backed IAM; without it versitygw falls back to the single root
        account (``gateway/README.md``). Cases that do not exercise IAM leave
        it out on purpose, so they do not depend on OpenBao being unsealed.
        """
        ns = namespace or self.ns
        body = (f"    ROOT_ACCESS_KEY_ID: {access or self.ROOT_ACCESS}\n"
                f"    ROOT_SECRET_ACCESS_KEY: {secret or self.ROOT_SECRET}\n")
        if vault_url:
            body += f"    VGW_IAM_VAULT_ENDPOINT_URL: {vault_url}\n"
        self.k8s_utils.apply_yaml(
            "apiVersion: v1\n"
            "kind: Secret\n"
            "metadata:\n"
            f"  name: {name}\n"
            "type: Opaque\n"
            "stringData:\n" + body,
            namespace=ns)
        self.extra_secrets.append((name, ns))
        return name

    def iam_secret(self, namespace=None):
        """``simplyblock-s3-iam`` if the lane wrote one, else ``None``.

        The workflow's ``cluster_security: s3`` step creates it from the
        AppRole it configures in OpenBao. A case that needs Vault-backed IAM
        asks for it rather than assuming it: without
        ``VGW_IAM_VAULT_ENDPOINT_URL`` versitygw falls back to the single root
        account, which is a different product mode and not one to end up
        testing by accident.
        """
        name = "simplyblock-s3-iam"
        return name if self.k8s_utils.get_resource_json(
            "secret", name, namespace or self.ns) else None

    def resolve_tls_provider(self, namespace=None):
        """Pick the ObjectStore's tlsProvider from the chart, not the distro.

        Being on OpenShift does not mean the OpenShift provider will work. The
        provider decides what the gateway pod MOUNTS: with ``OpenShift`` the
        operator projects the service-ca bundle ConfigMap
        (:attr:`CA_BUNDLE_CM`) into the pod's ``tls`` volume, and the operator
        chart renders that ConfigMap only under ``tls.provider=openshift``. Our
        lanes install the chart on its defaults -- ``tls.enabled: true`` with
        ``tls.provider: cert-manager`` -- so on an OpenShift lab the two
        disagreed and every gateway pod hung:

            MountVolume.SetUp failed for volume "tls" :
            configmap "simplyblock-certificate-authority" not found

        A missing ConfigMap keeps the pod in ContainerCreating rather than
        failing it, so nothing crashes and nothing logs. The Deployment simply
        never goes Available, the ObjectStore lands in Degraded with
        ``DeploymentNotAvailable``, and the case reports a retry budget running
        out -- naming neither TLS nor a ConfigMap. That cost run 44690388182
        all 18 cases, 0 passed, on a cluster that was Online throughout.

        So ask for the ConfigMap. Its presence is exactly the condition the
        gateway's mount needs, which is a narrower and more honest question
        than ``detect_openshift()``: it is also right on an OpenShift cluster
        whose chart was installed with cert-manager, and on a plain Kubernetes
        cluster that somehow has the bundle.
        """
        ns = namespace or self.ns
        if self.k8s_utils.get_resource_json("configmap", self.CA_BUNDLE_CM, ns):
            self.logger.info(
                "[tls] %s is present; using the OpenShift provider",
                self.CA_BUNDLE_CM)
            return "OpenShift"
        self.logger.info(
            "[tls] no %s in %s, so the chart was not installed with "
            "tls.provider=openshift; using cert-manager", self.CA_BUNDLE_CM, ns)
        return "cert-manager"

    def make_objectstore(self, name=None, replicas=1, tls_provider=None,
                         secret_name="s3-root-credentials", sc_name=None,
                         namespace=None, extra_spec="", wait=True,
                         capacity=None, secret_refs=None):
        """Create an ObjectStore and, by default, wait for it to serve.

        *tls_provider* defaults to what this deployment can actually honour.
        See :meth:`resolve_tls_provider` for why that is not the same question
        as which distribution we are on.
        """
        name = name or self.STORE
        ns = namespace or self.ns
        sc_name = sc_name or self.SC_NAME
        if tls_provider is None:
            tls_provider = self.resolve_tls_provider(ns)
        self.k8s_utils.apply_yaml(
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: ObjectStore\n"
            "metadata:\n"
            f"  name: {name}\n"
            "spec:\n"
            f"  image: {self.GATEWAY_IMAGE}\n"
            f"  port: {self.GATEWAY_PORT}\n"
            f"  replicas: {replicas}\n"
            f"  region: {self.REGION}\n"
            f"  tlsProvider: {tls_provider}\n"
            "  envFrom:\n"
            + "".join(f"  - secretRef:\n      name: {ref}\n"
                      for ref in (secret_refs or [secret_name]))
            + "  buckets:\n"
            f"    storageClassName: {sc_name}\n"
            f"    capacityBytes: {capacity or self.BUCKET_BYTES}\n"
            + extra_spec,
            namespace=ns)
        if (name, ns) not in self.stores:
            self.stores.append((name, ns))
        if wait:
            s3.wait_phase(self.k8s_utils, "objectstore", name, "Ready",
                          timeout=self.STORE_TIMEOUT, namespace=ns,
                          forbid="Degraded")
            s3.wait_gateway_pods(self.k8s_utils, name, replicas,
                                 timeout=self.STORE_TIMEOUT, namespace=ns)
        return name

    def make_bucket(self, name, store=None, bucket_name=None, capacity=None,
                    sc_name=None, namespace=None, wait=True, timeout=None):
        store = store or self.STORE
        ns = namespace or self.ns
        self.k8s_utils.apply_yaml(
            "apiVersion: storage.simplyblock.io/v1alpha2\n"
            "kind: S3Bucket\n"
            "metadata:\n"
            f"  name: {name}\n"
            "spec:\n"
            f"  objectStoreRef: {store}\n"
            f"  bucketName: {bucket_name or name}\n"
            f"  capacityBytes: {capacity or self.BUCKET_BYTES}\n"
            f"  storageClassName: {sc_name or self.SC_NAME}\n",
            namespace=ns)
        if (name, ns) not in self.buckets:
            self.buckets.append((name, ns))
        if wait:
            s3.wait_phase(self.k8s_utils, "s3bucket", name, "Ready",
                          timeout=timeout or self.BUCKET_TIMEOUT, namespace=ns,
                          forbid="Degraded")
        return name

    # ── the S3 client ────────────────────────────────────────────────────

    def start_client(self, name=None, namespace=None):
        pod = s3.S3Pod(self.k8s_utils, name or f"s3-client-{int(time.time())}",
                       namespace or self.ns)
        pod.start()
        self.extra_pods.append((pod.name, pod.namespace))
        self.client_pod = pod
        return pod

    def service_cfg(self, store=None, namespace=None, **kwargs):
        """Talk to the gateway through its Service."""
        store = store or self.STORE
        ns = namespace or self.ns
        return s3.gateway_cfg(
            s3.service_host(store, ns), self.GATEWAY_PORT,
            self.ROOT_ACCESS, self.ROOT_SECRET, region=self.REGION, **kwargs)

    def pod_cfg(self, pod_ip, **kwargs):
        """Talk to one gateway pod directly.

        A ClusterIP Service load-balances, which is precisely what hides a
        single broken replica -- so every case that asserts "this pod serves
        this bucket" addresses the pod, as dev's own ``exposeGateway`` does.
        """
        return s3.gateway_cfg(
            pod_ip, self.GATEWAY_PORT, self.ROOT_ACCESS, self.ROOT_SECRET,
            region=self.REGION, **kwargs)

    # ── small shared steps ───────────────────────────────────────────────

    def bucket_is_consistent(self, name, namespace=None, context=""):
        """The three things a `Ready` bucket must have, checked together.

        Checked as a set rather than one at a time because the interesting
        failures are the inconsistent ones: a bucket `Ready` with no export,
        or delivered to a node that runs no gateway pod.
        """
        ns = namespace or self.ns
        obj = s3.s3bucket(self.k8s_utils, name, ns)
        status = obj.get("status") or {}
        problems = []
        if not status.get("persistentVolumeClaimName"):
            problems.append("status.persistentVolumeClaimName is empty")
        if not status.get("nfsExportName"):
            problems.append("status.nfsExportName is empty")
        store = (obj.get("spec") or {}).get("objectStoreRef", self.STORE)
        want = set(s3.gateway_nodes(self.k8s_utils, store, ns))
        got = set(s3.delivered_nodes(obj))
        if got != want:
            problems.append(
                f"status.mountDeliveredNodes={sorted(got)} but the gateway "
                f"pods of {store} are on {sorted(want)}")
        if problems:
            raise s3.S3EvidenceError(
                f"S3Bucket/{name} is {s3.phase_of(obj)!r} but inconsistent "
                f"{context}: " + "; ".join(problems))
        return obj

    def record(self, label, payload):
        """Write a measurement into the run folder, not only the log.

        Several of these cases exist to produce a number dev has asked for
        (Open Question 4 above all). A number that lives only in a 200 MB log
        is a number nobody reads.
        """
        path = os.path.join(self.docker_logs_path, f"s3_{label}.json")
        try:
            import json
            with open(path, "w") as fh:
                json.dump(payload, fh, indent=2, default=str)
            self.logger.info("[s3] wrote %s", path)
        except Exception as exc:                          # noqa: BLE001
            self.logger.warning("[s3] could not write %s: %s", path, exc)

    # ── teardown ─────────────────────────────────────────────────────────

    def teardown(self, delete_lvols=True, close_ssh=True,
                 skip_k8s_cleanup=False):
        """Buckets before stores, stores before classes, undos before all.

        The order is the deletion chain's own: an `S3Bucket` holds a finalizer
        until its PVC is released and its mounts withdrawn, and deleting the
        `ObjectStore` first takes away the gateway pods the withdrawal is
        addressed to.
        """
        self.logger.info("Inside %s.teardown()", type(self).__name__)
        if self.k8s_utils is None:
            super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh,
                             skip_k8s_cleanup=skip_k8s_cleanup)
            return

        if self.bao is not None:
            try:
                self.bao.stop()
                if self.bao.events:
                    self.record("openbao_events",
                                {"unseals": self.bao.unseal_count,
                                 "events": self.bao.events})
            except Exception:                             # noqa: BLE001
                pass

        for undo in reversed(self.undo_stack):
            try:
                undo()
            except Exception as exc:                      # noqa: BLE001
                self.logger.error(
                    "[s3] UNDO FAILED, the cluster may be left modified: %s", exc)
        self.undo_stack = []

        for pod, ns in self.extra_pods:
            self._quiet(lambda p=pod, n=ns:
                        self.k8s_utils.delete_resource("pod", p, n))

        for name, ns in self.buckets:
            self._quiet(lambda n=name, s=ns:
                        self.k8s_utils.delete_resource("s3bucket", n, s))
        # Wait for the finalizers rather than racing them: a surviving
        # S3Bucket pins a PVC, and a surviving PVC pins an lvol into the next
        # run's capacity.
        for name, ns in self.buckets:
            try:
                s3.wait_gone(self.k8s_utils, "s3bucket", name, timeout=300,
                             namespace=ns)
            except Exception as exc:                      # noqa: BLE001
                self.logger.warning("[s3] %s did not finalize: %s", name, exc)

        for name, ns in self.stores:
            self._quiet(lambda n=name, s=ns:
                        self.k8s_utils.delete_resource("objectstore", n, s))
        for name, ns in self.extra_secrets:
            self._quiet(lambda n=name, s=ns:
                        self.k8s_utils.delete_resource("secret", n, s))
        for sc in self.extra_storage_classes:
            self._quiet(lambda s=sc:
                        self.k8s_utils.delete_resource("storageclass", s))
        for ns in self.extra_namespaces:
            self._quiet(lambda n=ns:
                        self.k8s_utils.delete_resource("namespace", n))

        super().teardown(delete_lvols=delete_lvols, close_ssh=close_ssh,
                         skip_k8s_cleanup=skip_k8s_cleanup)

    def _quiet(self, fn):
        try:
            fn()
        except Exception as exc:                          # noqa: BLE001
            self.logger.warning("[s3] cleanup step failed: %s", exc)
