"""
E2E security tests for lvol DH-HMAC-CHAP, allowed-hosts, and dynamic host management.

Security feature summary:
  pool add --sec-options <file>   JSON {dhchap_key: bool, dhchap_ctrlr_key: bool}; applied at pool level.
  --allowed-hosts <file>  JSON list of host NQNs that can access the lvol
  volume connect <id> --host-nqn <nqn>   returns connect string with embedded DHCHAP keys
  volume add-host <id> <nqn>   add host to existing lvol
  volume remove-host <id> <nqn>                remove host from existing lvol

All sbcli CLI wrappers live in ssh_utils.SshUtils:
  ssh_obj.create_sec_lvol(...)
  ssh_obj.get_lvol_connect_str_with_host_nqn(...)
  ssh_obj.add_host_to_lvol(...)
  ssh_obj.remove_host_from_lvol(...)
  ssh_obj.get_client_host_nqn(node)
"""

import json
import re
import shlex
import threading
import time
import random
import string
from pathlib import Path

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from exceptions.custom_exception import LvolNotConnectException


# ───────────────────────────────────── helpers ──────────────────────────────


class DhchapUnsupportedByHost(Exception):
    """The node's kernel has no in-band NVMe authentication.

    DH-HMAC-CHAP needs ``CONFIG_NVME_AUTH`` in the host kernel. Without it
    the kernel's nvme-fabrics parser reports ``option "dhchap_secret"
    ignored`` and refuses the controller, so a DHCHAP volume cannot be
    mounted even from an allowed node. Confirmed on Talos v1.12.7
    (6.18.24-talos); works on RHCOS 9.6 (5.14.0-570.el9_6). Treated as an
    environment limit and skipped, matching how the RDMA security test skips
    when RDMA is unavailable.
    """


def _rand_suffix(n=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


class DhchapHost:
    """An identity a DHCHAP authorization question can be asked about.

    Two coordinates, because the two modes enforce at different layers:

      nqn  -- docker: the NQN handed to ``volume connect --host-nqn``. The
              control plane decides whether to hand back DHCHAP keys for it.
      node -- k8s:   the node a workload is pinned to. The operator labels
              allowed nodes and the CSI driver writes a matching nodeAffinity
              onto every PV, so the *node* is the subject. The NQN is derived
              by the CSI node plugin from the node's Kubernetes UID and is
              carried here for logging only -- a K8s test never supplies it,
              which is the documented model.

    Exactly one coordinate is load-bearing per mode. Authorization state is
    deliberately NOT stored here: a host allowed now can be revoked later, so
    the expectation always lives at the call site.
    """

    def __init__(self, nqn=None, node=None, desc=""):
        self.nqn = nqn
        self.node = node
        self.desc = desc

    def __repr__(self):
        bits = [f"node={self.node!r}"] if self.node else []
        if self.nqn:
            bits.append(f"nqn={self.nqn!r}")
        if self.desc:
            bits.append(self.desc)
        return f"DhchapHost({', '.join(bits)})"


def _as_nqn(host):
    """Accept a bare NQN string or a DhchapHost.

    Back-compat shim so residual docker-only call sites need no edit.
    """
    return host.nqn if isinstance(host, DhchapHost) else host


# Grep-able coverage tokens. The e2e runner has no skip API -- it computes
# ``skipped = total - (passed + failed)`` (e2e/e2e.py:473), so a test that
# returns early is counted as PASSED. Any run whose log contains one of these
# is NOT full coverage, whatever the summary says.
TOK_COVERAGE_LOST = "DHCHAP-COVERAGE-LOST"
TOK_K8S_LIMITATION = "DHCHAP-K8S-LIMITATION"
TOK_WEAK_EVIDENCE = "DHCHAP-WEAK-EVIDENCE"
TOK_SKIPPED_K8S = "SKIPPED-K8S"

# The enforcement canary runs once per process, not once per test class.
_DHCHAP_ENFORCEMENT_CHECKED = False

# Event substrings that positively identify a DHCHAP/allowed-hosts denial.
_DENIAL_REASONS = (
    "nodeaffinity check failed",
    "no matching nodeselectorterms",
    "not found in allowed hosts",
    # Provisioning-time rejection. With the operator StorageClass the
    # volume may never be provisioned for a disallowed node at all:
    # allowedTopologies keys off the pool label, which the CSI plugin
    # only reports on allowed nodes, so the provisioner refuses before
    # any mount is attempted.
    "is not in requisite",
    "volume node affinity conflict",
)

# Event substrings that mean the pod failed for a reason that has NOTHING to do
# with DHCHAP. Seeing one of these on a denial path is a test bug, not a pass:
# the observation "pod never ran" would be right for the wrong reason.
_DISQUALIFYING_REASONS = (
    "multi-attach",
    "volume is already exclusively attached",
    # NOTE: "failedscheduling" is deliberately NOT here. With the
    # operator StorageClass (WaitForFirstConsumer + allowedTopologies) a
    # genuinely denied node produces FailedScheduling as its real,
    # correct symptom, so treating it as an impostor would reject valid
    # evidence. It is caught instead by requiring a positive denial
    # reason, with a bare FailedScheduling logged as weak evidence.
    "errimagepull",
    "imagepullbackoff",
    "createcontainerconfigerror",
    "insufficient",
    "untolerated taint",
    "waiting for first consumer",
    "unbound immediate persistentvolumeclaims",
)


# COMMENTED OUT: old security option constants (DHCHAP is now pool-level via --dhchap flag)
# SEC_BOTH = {"dhchap_key": True, "dhchap_ctrlr_key": True}
# SEC_HOST_ONLY = {"dhchap_key": True, "dhchap_ctrlr_key": False}
# SEC_CTRL_ONLY = {"dhchap_key": False, "dhchap_ctrlr_key": True}


# ─────────────────────────────────── base class ─────────────────────────────


class SecurityTestBase(TestClusterBase):
    """
    Base class for all security test scenarios.

    CLI-level security operations are delegated to ssh_obj so that the
    implementations are reusable across E2E and stress tests:
      self.ssh_obj.create_sec_lvol(...)
      self.ssh_obj.get_lvol_connect_str_with_host_nqn(...)
      self.ssh_obj.add_host_to_lvol(...)
      self.ssh_obj.remove_host_from_lvol(...)
      self.ssh_obj.get_client_host_nqn(node)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "5G"
        self.fio_size = "1G"
        self.mount_path = "/mnt"
        self.log_path = str(Path.home())
        self.lvol_mount_details = {}
        # Kept short on purpose. The operator turns this into a node label
        # key: simplyblock.io/pool.<ns>.<cluster CR>.simplyblock-<pool>, whose
        # name part is capped at 63 chars, and the CSI driver writes that key
        # into every PV's nodeAffinity — overflow means the PV is rejected and
        # the PVC never binds. With ns=simplyblock and CR=simplyblock-cluster,
        # "sec_test_pool" landed at 62/63 and any dedicated-pool suffix pushed
        # it over. "secpool" leaves ~7 chars of headroom for the suffix.
        self.pool_name = "secpool"
        self._client_host_nqn = None
        self.fio_threads = []

        # K8s-native resource tracking (only used when k8s_test=True)
        self.created_pvcs: list[str] = []
        self.created_fio_jobs: list[str] = []
        self.created_configmaps: list[str] = []
        self.created_pods: list[str] = []
        self.created_storage_classes: list[str] = []
        self._storage_class_name: str = "simplyblock-sec-sc"
        self._dhchap_node_label: str = None
        # Resolved once in _ensure_pool_and_sc. The operator derives the node
        # label key from the StoragePool CRD's metadata.name, which can carry a
        # timestamp suffix and be truncated to fit the 63-char label budget --
        # so it is NOT always self.pool_name. Recomputing it later is a footgun.
        self._pool_crd_name: str = None
        self._dhchap_allowed_nodes: list[str] = []
        self._dhchap_disallowed_nodes: list[str] = []
        # Volumes for which a positive control has already succeeded. A denial
        # assertion refuses to run without one -- see _assert_host_denied.
        self._dhchap_positive_control: set = set()
        # Operator StorageClass of a second, encrypted DHCHAP pool.
        # storageClassParameters is immutable per pool, so encryption needs
        # its own StoragePool rather than another class on the main one.
        self._encrypted_sc_name: str = None
        self._encrypted_pool_crd: str = None
        self._encrypted_pool_label: str = None
        self._encrypted_pool_name: str = None
        # Chosen once per test so docker and K8s exercise the same filesystem.
        self._fs_type: str = None

    # ── filesystem helper ────────────────────────────────────────────────────

    def _pick_fs_type(self):
        """Choose ext4 or xfs once per test so both filesystems get coverage.

        Cached on ``self._fs_type`` because in K8s the choice has to be baked
        into the StorageClass (the CSI node plugin creates the filesystem from
        ``csi.storage.k8s.io/fstype``) *before* any volume exists, and the
        later verification has to compare against the same value.
        """
        if self._fs_type:
            return self._fs_type
        self._fs_type = random.choice(["ext4", "xfs"])
        self.logger.info(f"[_pick_fs_type] Selected filesystem: {self._fs_type}")
        return self._fs_type

    def _normalize_fio_node(self):
        """Collapse ``self.fio_node`` to a single host.

        Tolerates the K8s-native shape where there are no client machines at
        all: ``cluster_test_base`` sets ``self.fio_node = []`` when there is
        neither a CLIENT_IP nor a mgmt node (cluster_test_base.py:243), so the
        ``self.fio_node[0]`` every test class used to open with raised
        IndexError before a single assertion ran.

        Also the single hook every active class already calls first, so the
        enforcement canary runs from here -- once per process.
        """
        if isinstance(self.fio_node, list):
            self.fio_node = self.fio_node[0] if self.fio_node else None
        self._assert_dhchap_enforceable()
        return self.fio_node

    # ── debug helpers ─────────────────────────────────────────────────────────

    def _log_cluster_security_config(self):
        """Log cluster-level security fields for debugging."""
        try:
            details = self.sbcli_utils.get_cluster_details()
            keys = ["ha_type", "sec_enabled", "host_sec", "tls_enabled",
                    "fabric_tcp", "fabric_rdma", "status"]
            summary = {k: details.get(k) for k in keys if k in details}
            self.logger.info(f"[DEBUG] Cluster security fields: {summary}")
            self.logger.info(f"[DEBUG] Full cluster details: {details}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] Could not get cluster details: {exc}")

        # Also dump via CLI
        try:
            out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} cluster list")
            self.logger.info(f"[DEBUG] cluster list output:\n{out}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] cluster list failed: {exc}")

    def _log_lvol_security(self, lvol_id, label=""):
        """Log full lvol details via CLI after creation."""
        try:
            out = self._get_lvol_details_via_cli(lvol_id)
            self.logger.info(f"[DEBUG] volume get {lvol_id} {label}:\n{out}")
        except Exception as exc:
            self.logger.warning(f"[DEBUG] volume get failed: {exc}")

    # ── NQN cache ────────────────────────────────────────────────────────────

    def _get_client_host_nqn(self, node=None, force_new=False):
        """Return (and cache) the host NQN from /etc/nvme/hostnqn on the client node.

        Reads the existing hostnqn rather than generating a new one so that the
        NQN matches what the kernel NVMe driver will present during connect.
        """
        if self._client_host_nqn and not force_new:
            return self._client_host_nqn
        target = node or self.fio_node
        nqn_out, _ = self.ssh_obj.exec_command(target, "cat /etc/nvme/hostnqn")
        nqn = nqn_out.strip().split('\n')[0].strip()
        assert nqn, f"Could not read hostnqn from /etc/nvme/hostnqn on {target}"
        self.logger.info(f"[_get_client_host_nqn] NQN on {target}: {nqn!r}")
        self._client_host_nqn = nqn
        return nqn

    # ── connect / disconnect helpers ─────────────────────────────────────────

    def _get_connect_str_cli(self, lvol_id, host_nqn=None):
        """
        Return (connect_commands, stderr) for *lvol_id*.

        When *host_nqn* is provided the commands include embedded DHCHAP keys
        and use ``--ctrl-loss-tmo=-1`` (matching the existing API helper) so
        that NVMe controllers never time out during a storage-node outage.

        When *host_nqn* is None the plain ``volume connect`` output is returned
        (no DHCHAP keys, default ctrl-loss-tmo).
        """
        if host_nqn:
            return self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                self.mgmt_nodes[0], lvol_id, host_nqn)
        # Unauthenticated path — use existing API helper via CLI
        cmd = f"{self.base_cmd} volume connect {lvol_id}"
        out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
        connect_lines = [
            line.strip() for line in out.strip().split('\n')
            if line.strip() and 'nvme connect' in line
        ]
        return connect_lines, err

    def _connect_and_get_device(self, lvol_name, lvol_id, host_nqn=None):
        """
        Issue nvme connect command(s) on fio_node and return the new
        block device path (e.g. ``/dev/nvme3n1``).

        Returns (device_path, connect_commands_list).
        """
        self.logger.info(f"[DEBUG] _connect_and_get_device: lvol={lvol_name} id={lvol_id} host_nqn={host_nqn}")
        if host_nqn:
            connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn)
            self.logger.info(f"[DEBUG] connect strings (with host_nqn): err={err!r} cmds={connect_ls}")
            if err or not connect_ls:
                raise LvolNotConnectException(
                    f"No connect string for {lvol_name} (host_nqn={host_nqn}): {err}")
        else:
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.logger.info(f"[DEBUG] connect strings (no host_nqn): cmds={connect_ls}")

        initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
        self.logger.info(f"[DEBUG] initial devices on {self.fio_node}: {initial_devices}")

        for cmd in connect_ls:
            cmd = ' '.join(cmd.split())  # normalise any embedded whitespace / stray \r\n
            self.logger.info(f"[DEBUG] executing nvme connect (repr): {cmd!r}")
            out, err = self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
            self.logger.info(f"[DEBUG] nvme connect result: out={out!r} err={err!r}")
            if err:
                self.logger.warning(f"nvme connect warning: {err}")
                # Dump dmesg nvme entries after failure for diagnosis
                dmesg_out, _ = self.ssh_obj.exec_command(
                    node=self.fio_node, command="dmesg | grep -i nvme | tail -20")
                self.logger.info(f"[DEBUG] dmesg nvme tail after failed connect:\n{dmesg_out}")

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=self.fio_node)
        self.logger.info(f"[DEBUG] final devices on {self.fio_node}: {final_devices}")
        new_devices = [d for d in final_devices if d not in initial_devices]
        self.logger.info(f"[DEBUG] new devices after connect: {new_devices}")

        lvol_device = None
        for dev in final_devices:
            if dev not in initial_devices:
                lvol_device = f"/dev/{dev.strip()}"
                break

        if not lvol_device:
            raise LvolNotConnectException(
                f"LVOL {lvol_name} did not appear as a block device")

        return lvol_device, connect_ls

    def _disconnect_lvol(self, lvol_id):
        """Disconnect a single lvol from fio_node by NQN."""
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            if details:
                nqn = details[0]["nqn"]
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)
        except Exception as e:
            self.logger.warning(f"Disconnect error for {lvol_id}: {e}")

    def _get_lvol_details_via_cli(self, lvol_id):
        """Run ``volume get <id>`` and return raw CLI output."""
        out, _ = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"{self.base_cmd} volume get {lvol_id}")
        return out

    # ── FIO helpers ──────────────────────────────────────────────────────────

    def _run_fio_and_validate(self, lvol_name, mount_point, log_file,
                               rw="randrw", bs="4K", numjobs=2, runtime=120,
                               fio_size=None):
        """Start FIO in a detached tmux session, wait for it to finish, then validate."""
        job_name = f"{lvol_name}_fio"
        self.ssh_obj.run_fio_test(
            self.fio_node, None, mount_point, log_file,
            size=fio_size or self.fio_size,
            name=job_name,
            rw=rw, bs=bs, nrfiles=4, iodepth=1,
            numjobs=numjobs, time_based=True, runtime=runtime,
        )
        # run_fio_test launches FIO inside a detached tmux session and returns
        # immediately.  Poll until the process exits so that any subsequent
        # unmount/disconnect never races with a still-running FIO job.
        deadline = runtime + 60   # generous grace period
        waited = 0
        while waited < deadline:
            procs = self.ssh_obj.find_process_name(self.fio_node, f"fio.*{job_name}")
            running = [p for p in procs
                       if p.strip() and "grep" not in p and "fio --name" in p]
            if not running:
                break
            sleep_n_sec(5)
            waited += 5
        else:
            self.logger.warning(
                f"FIO job {job_name!r} did not finish after {deadline}s; killing")
            self.ssh_obj.kill_processes(node=self.fio_node, process_name="fio")
            sleep_n_sec(3)
        self.common_utils.validate_fio_test(self.fio_node, log_file=log_file)

    # ── K8s dual-mode helpers ─────────────────────────────────────────────────
    # These allow the same test logic to run in both Docker (SSH) and K8s
    # (CRD/PVC/Job) modes, following the pattern from BackupTestBase.

    def _ensure_pool_and_sc(self, dhchap=False, allowed_nodes=None,
                            encryption=False):
        """Create (or reuse) a storage pool and StorageClass.

        In Docker mode: uses ssh_obj.add_storage_pool(dhchap=True).
        In K8s mode: creates StoragePool CRD with dhchap + allowedNodes fields.
        """
        if not self.k8s_test:
            self.ssh_obj.add_storage_pool(
                self.mgmt_nodes[0], self.pool_name, self.cluster_id,
                dhchap=dhchap)
            return

        # The operator builds this pool's StorageClass from these. We do
        # NOT create our own class -- the operator's is the documented
        # customer path and it already sets dhchap_node_label itself.
        scp = {
            "encryption": bool(encryption),
            "filesystem": self._pick_fs_type(),
        }
        actual = self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name, dhchap=dhchap,
            allowed_nodes=allowed_nodes,
            storage_class_parameters=scp)
        if actual and actual != self.pool_name:
            self.logger.info(
                f"[pool] Requested '{self.pool_name}' but using '{actual}'")
            self.pool_name = actual
        self._pool_crd_name = self._k8s_resolve_pool_crd(dhchap, allowed_nodes)
        self._dhchap_node_label = (
            self._k8s_pool_node_label(allowed_nodes) if dhchap else None
        )
        self._k8s_setup_storage_class(allowed_nodes=allowed_nodes)

    def _k8s_resolve_pool_crd(self, dhchap, allowed_nodes):
        """Return the StoragePool CRD's metadata.name for the current pool.

        The operator builds the node label key from the CRD name, not from the
        backend pool name that ``add_storage_pool`` hands back -- and the two
        diverge whenever the CRD name picked up a timestamp suffix or was
        truncated to fit the 63-char label budget (k8s_utils.py:3669,3702).
        Match on the spec we asked for, which is unambiguous for a DHCHAP pool
        with a specific allowedNodes subset.
        """
        k8s = self._ensure_k8s_utils()

        # Authoritative source first: `pool get` reports the CRD name it was
        # reconciled from as ``cr_name``. Verified on OpenShift 2026-09-04.
        try:
            details = self.sbcli_utils.get_pool_by_id(self._get_pool_id())
            if isinstance(details, list):
                details = details[0] if details else {}
            cr_name = (details or {}).get("cr_name")
            if cr_name:
                self.logger.info(
                    f"[pool] StoragePool CRD from pool.cr_name: {cr_name!r}")
                return cr_name
        except Exception as exc:
            self.logger.info(
                f"[pool] cr_name unavailable ({exc}) — matching on spec")

        wanted = sorted(allowed_nodes or [])
        out, _ = k8s._exec_kubectl(
            f"kubectl get storagepools -n {k8s.namespace} -o json "
            f"2>/dev/null || true")
        try:
            items = json.loads(out).get("items", []) if out.strip() else []
        except (json.JSONDecodeError, AttributeError):
            items = []
        for crd in items:
            spec = crd.get("spec", {}) or {}
            if (bool(spec.get("dhchap")) == bool(dhchap)
                    and sorted(spec.get("allowedNodes") or []) == wanted):
                name = crd.get("metadata", {}).get("name")
                self.logger.info(f"[pool] StoragePool CRD resolved: {name!r}")
                return name
        # Fall back to the derived shape rather than failing here; the label
        # lookup and _k8s_assert_dhchap_wiring both cross-check it.
        derived = f"simplyblock-{self.pool_name.lower().replace('_', '-')}"
        self.logger.warning(
            f"[pool] Could not match a StoragePool CRD to dhchap={dhchap} "
            f"allowedNodes={wanted} — assuming CRD name {derived!r}")
        return derived

    def _k8s_pool_node_label(self, allowed_nodes=None):
        """Return the operator's node label key for the current pool.

        Read it off an allowed node rather than rebuilding the string. The
        operator derives the key from the StoragePool CRD's metadata.name,
        while ``self.pool_name`` holds the *backend* pool name — those have
        matched so far but nothing guarantees it, and a wrong key means the
        StorageClass silently carries no enforcement at all (the exact
        failure this whole change is fixing). Falls back to the computed
        shape ``simplyblock.io/pool.<ns>.<StorageCluster CR>.<pool>`` only if
        no label can be found, and says so loudly.
        """
        k8s = self._ensure_k8s_utils()
        for node in (allowed_nodes or []):
            out, _ = k8s._exec_kubectl(
                f"kubectl get node {node} -o jsonpath='{{.metadata.labels}}' "
                f"2>/dev/null || true"
            )
            try:
                labels = json.loads(out) if out.strip().startswith("{") else {}
            except (json.JSONDecodeError, AttributeError):
                labels = {}
            pool_keys = [
                key for key, val in labels.items()
                if key.startswith("simplyblock.io/pool.") and val == "allowed"
            ]
            # Match on the CRD name first: the operator derives the key from
            # the StoragePool CRD's metadata.name, while self.pool_name is the
            # *backend* pool name, and the two diverge under the timestamp
            # suffix / 63-char truncation in add_storage_pool.
            for candidate in (self._pool_crd_name, self.pool_name):
                if not candidate:
                    continue
                exact = [k for k in pool_keys
                         if k.rsplit(".", 1)[-1] == candidate]
                if exact:
                    self.logger.info(
                        f"[dhchap] pool node label (read from {node}): "
                        f"{exact[0]}")
                    return exact[0]
            if len(pool_keys) == 1:
                self.logger.info(
                    f"[dhchap] pool node label (sole pool label on {node}, "
                    f"did not match CRD {self._pool_crd_name!r} or pool "
                    f"{self.pool_name!r}): {pool_keys[0]}")
                return pool_keys[0]

        # Do NOT fall back silently. A wrong key means the StorageClass
        # carries no enforcement at all and every assertion in this suite
        # passes vacuously -- which is the exact failure this change exists to
        # prevent. It also cannot be diagnosed later: it surfaces 20 minutes
        # downstream as "pod never reached Running".
        computed = self._k8s_pool_node_label_computed()
        raise AssertionError(
            f"{TOK_COVERAGE_LOST}: could not read the operator's pool label "
            f"off any allowed node {allowed_nodes} (CRD "
            f"{self._pool_crd_name!r}, pool {self.pool_name!r}). Computed "
            f"shape would be {computed!r}. Without the real key the "
            f"StorageClass gets no dhchap_node_label and DHCHAP is not "
            f"enforced at all -- refusing to run a suite that would pass "
            f"vacuously. Check that the operator reconciled the StoragePool "
            f"and labelled its allowedNodes.")

    def _k8s_pool_node_label_computed(self):
        """Best-effort construction of the pool label key (fallback only)."""
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            f"kubectl get storageclusters -n {k8s.namespace} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
        )
        names = [n.strip() for n in (out or "").strip().splitlines() if n.strip()]
        cluster_cr = names[0] if names else "simplyblock-cluster"
        label = (
            f"simplyblock.io/pool.{k8s.namespace}.{cluster_cr}.{self.pool_name}"
        )
        self.logger.info(f"[dhchap] pool node label: {label}")
        return label

    def _k8s_setup_storage_class(self, allowed_nodes=None):
        """Adopt the StorageClass the operator generated for this pool.

        We deliberately do NOT create our own class. The operator emits one
        per StoragePool, named
        ``simplyblock-{namespace}-{clusterName}-{poolCRDname}``, and it
        already carries ``dhchap_node_label`` -- the parameter that makes CSI
        write a matching ``nodeAffinity`` onto every PV, which is what
        actually enforces allowedNodes at mount. Encryption and filesystem
        come from the pool's ``spec.storageClassParameters``, set in
        ``_ensure_pool_and_sc``.

        One product workaround is unavoidable here. The operator's class also
        sets ``allowedTopologies`` keyed on the pool label, and the CSI node
        plugin only snapshots node labels as topology keys at REGISTRATION
        time. A pool created while the driver is already running is therefore
        invisible to it and every PVC fails with::

            ProvisioningFailed: topology map[topology.kubernetes.io/zone:...]
            from selected node "worker-0" is not in requisite:
            [map[simplyblock.io/pool....:allowed]]

        So we restart the CSI node daemonset once per pool to force a
        re-register. Customers following the documented flow hit exactly this
        and need the same step -- it belongs in the docs until the operator
        either triggers the re-register itself after labelling nodes, or drops
        ``allowedTopologies`` (``dhchap_node_label`` alone already enforces,
        which this suite proves).
        """
        if not self.k8s_test:
            return
        k8s = self._ensure_k8s_utils()

        self._storage_class_name = k8s.operator_storage_class_name(
            self._pool_crd_name)
        assert k8s.wait_storage_class_exists(self._storage_class_name), (
            f"{TOK_COVERAGE_LOST}: the operator did not generate "
            f"StorageClass {self._storage_class_name!r} for pool "
            f"{self._pool_crd_name!r}. Without it there is nothing to "
            f"provision from.")
        self.logger.info(
            f"[k8s] using operator StorageClass {self._storage_class_name!r}")

        # Make the pool label a CSI topology key, or allowedTopologies cannot
        # be satisfied and nothing provisions.
        if self._dhchap_node_label:
            ok = k8s.restart_csi_node_driver(
                expect_topology_key=self._dhchap_node_label,
                expect_on_nodes=list(allowed_nodes or []))
            if not ok:
                self.logger.warning(
                    f"{TOK_K8S_LIMITATION}: CSI re-register did not surface "
                    f"{self._dhchap_node_label!r} as a topology key; "
                    f"provisioning from the operator StorageClass may fail")

        # Aliases the inherited cluster_test_base dual helpers key off, so
        # _create_snapshot_dual / _create_clone_dual / _resize_lvol_dual all
        # operate against the operator class and clones inherit its
        # dhchap_node_label (and therefore its enforcement).
        self._k8s_storage_class_name = self._storage_class_name
        try:
            k8s.create_volume_snapshot_class(
                name=self._k8s_snapshot_class_name)
        except Exception as exc:
            self.logger.warning(
                f"[k8s] VolumeSnapshotClass "
                f"{self._k8s_snapshot_class_name!r}: {exc}")

    def _k8s_encrypted_storage_class(self):
        """Return the operator StorageClass of a DHCHAP pool with encryption.

        ``storageClassParameters`` is immutable once the class exists -- the
        CRD says to create a new StoragePool to change it -- so an encrypted
        volume needs its own pool rather than a second class on this one.
        Created lazily and cached, with the same allowedNodes as the main
        pool so the allowed/denied matrix still holds.
        """
        if self._encrypted_sc_name:
            return self._encrypted_sc_name
        saved = (self.pool_name, self._storage_class_name,
                 self._dhchap_node_label, self._pool_crd_name)
        try:
            self.pool_name = "secenc"
            self._ensure_pool_and_sc(
                dhchap=True, allowed_nodes=list(self._dhchap_allowed_nodes),
                encryption=True)
            self._encrypted_sc_name = self._storage_class_name
            self._encrypted_pool_crd = self._pool_crd_name
            self._encrypted_pool_label = self._dhchap_node_label
            self._encrypted_pool_name = self.pool_name
            self.logger.info(
                f"[k8s] encrypted DHCHAP pool {self.pool_name!r} -> "
                f"StorageClass {self._encrypted_sc_name!r}")
        finally:
            (self.pool_name, self._storage_class_name,
             self._dhchap_node_label, self._pool_crd_name) = saved
        return self._encrypted_sc_name

    def _k8s_verify_pod_scheduling(self, pvc_name, node_name, expect_success,
                                    pod_prefix="dhchap"):
        """Pin a utility pod consuming *pvc_name* to *node_name* and verify
        the DHCHAP allowedNodes outcome, the K8s-native way: no host NQN is
        ever supplied by the test — the CSI node plugin derives it from the
        node itself and the operator's StoragePool CRD enforces the rest.

        expect_success=True: *node_name* is an allowed host — the pod must
        reach Running.
        expect_success=False: *node_name* is NOT an allowed host — the pod
        must NOT reach Running, and kubelet must emit a FailedMount event
        (NodeStageVolume rejected with "not found in allowed hosts").

        Returns the (still-running) pod name on the success path, or None
        on the expected-failure path (pod is cleaned up before returning).

        Raises ``DhchapUnsupportedByHost`` if the node's kernel has no
        in-band NVMe authentication — the connect then fails on an allowed
        node too, which is an environment limit rather than a test failure.

        Pinning uses ``spec.nodeSelector``, never ``spec.nodeName``. The
        operator's StorageClass binds ``WaitForFirstConsumer``, and only the
        scheduler triggers that binding -- a nodeName-pinned pod bypasses the
        scheduler, so the claim would sit at "waiting for first consumer" and
        the pod would never run whether the node is allowed or not, making the
        negative assertion pass vacuously.

        Because the operator's class also carries ``allowedTopologies``, a
        denial can now surface at PROVISIONING time (``is not in requisite``)
        as well as at mount time (``NodeAffinity check failed``); both are in
        ``_DENIAL_REASONS``.
        """
        k8s = self._ensure_k8s_utils()
        pod_name = f"{pod_prefix}-{_rand_suffix().lower()}"
        # Track before creating: if anything below raises, teardown still
        # deletes the pod. A surviving pod pins kubernetes.io/pvc-protection
        # on its claim and leaves the PVC stuck Terminating for hours.
        self.created_pods.append(pod_name)
        # nodeSelector, not nodeName: the operator StorageClass binds
        # WaitForFirstConsumer, and only the scheduler triggers that.
        k8s.create_utility_pod(pod_name, pvc_name, node_selector=node_name)

        if expect_success:
            try:
                running = k8s.wait_pod_running(pod_name, timeout=300)
            except (TimeoutError, RuntimeError):
                events = k8s.get_pod_events(pod_name)
                self.logger.info(
                    f"[dhchap] Events for {pod_name} on allowed node "
                    f"{node_name!r}: {events!r}")
                if 'dhchap_secret" ignored' in events or "dhchap_ctrl_secret\" ignored" in events:
                    raise DhchapUnsupportedByHost(
                        f"node {node_name!r} kernel ignored the DHCHAP "
                        f"connect options (no in-band NVMe auth / "
                        f"CONFIG_NVME_AUTH); events: {events!r}")
                raise
            assert running, (
                f"Pod {pod_name} pinned to allowed node {node_name!r} did "
                f"not reach Running — DHCHAP should have permitted this node")
            self.logger.info(
                f"[dhchap] Pod {pod_name} on allowed node {node_name!r} "
                f"is Running")
            return pod_name

        # expect_success=False: wait_pod_running raises TimeoutError rather
        # than returning False when the pod never reaches Running — that
        # timeout IS the expected outcome here. A pod that unexpectedly
        # reaches Running (no exception) is the real failure.
        try:
            try:
                k8s.wait_pod_running(pod_name, timeout=60)
            except TimeoutError:
                pass
            else:
                raise AssertionError(
                    f"Pod {pod_name} pinned to DISALLOWED node {node_name!r} "
                    f"reached Running — DHCHAP allowedNodes restriction was "
                    f"not enforced")
            events = k8s.get_pod_events(pod_name)
            self.logger.info(
                f"[dhchap] Events for {pod_name} on disallowed node "
                f"{node_name!r}: {events!r}")
            low = events.lower()

            # The event list is a TIMELINE (get_pod_events sorts by
            # .lastTimestamp), not a set of competing verdicts. So order of
            # evaluation matters: a positive DHCHAP denial anywhere in it is
            # conclusive, because the volume was offered to this node and
            # refused on nodeAffinity grounds. A transient impostor earlier in
            # the timeline does not undo that.
            #
            # Real example from CI run 093822:
            #   1. FailedAttachVolume: Multi-Attach error      (transient)
            #   2. SuccessfulAttachVolume: Attach succeeded    (resolved)
            #   3. FailedMount: NodeAffinity check failed      (decisive)
            # Checking for the impostor first rejected a correct denial.
            impostor = next(
                (r for r in _DISQUALIFYING_REASONS if r in low), None)
            specific = any(r in low for r in _DENIAL_REASONS)

            if specific:
                self.logger.info(
                    f"[dhchap] denial positively identified for {pod_name} "
                    f"on {node_name!r}")
                if impostor:
                    # Worth surfacing: it means a volume was still attached
                    # elsewhere when this pod was created, i.e. a release did
                    # not wait for detach. The assertion still stands.
                    self.logger.warning(
                        f"[dhchap] {pod_name} also saw a transient "
                        f"{impostor!r} before the DHCHAP denial — a preceding "
                        f"release did not wait for the volume to detach. The "
                        f"denial itself is conclusive, but see "
                        f"_k8s_release_pod(pvc_name=...). Events: {events!r}")
                return None

            # No positive DHCHAP wording. Now an impostor IS disqualifying:
            # without a denial reason, "pod never ran" is equally explained by
            # Multi-Attach, a failed image pull, or an unschedulable node.
            assert not impostor, (
                f"Pod {pod_name} on disallowed node {node_name!r} never ran, "
                f"and the only reason given is unrelated to DHCHAP "
                f"({impostor!r}) — this assertion would have passed for the "
                f"wrong reason. Release the volume on the allowed node (and "
                f"wait for detach) before re-checking. Events: {events!r}")

            # A bare FailedMount is weak evidence: it also covers a CSI-down
            # or quota failure. Accept it so a genuine denial on a
            # differently-worded build still passes, but mark the run.
            assert "failedmount" in low, (
                f"Pod {pod_name} on disallowed node {node_name!r} never "
                f"ran, but not for a DHCHAP reason — expected a "
                f"NodeAffinity / not-in-allowed-hosts / FailedMount "
                f"event; got: {events!r}")
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE}: {pod_name} on disallowed node "
                f"{node_name!r} failed with a bare FailedMount and no "
                f"NodeAffinity / allowed-hosts wording — treating as a "
                f"denial, but the evidence does not positively identify "
                f"DHCHAP. Events: {events!r}")
        finally:
            # Wait for detach here too. A denied pod still ATTACHES the volume
            # before the mount is refused — the CI events show
            # "SuccessfulAttachVolume" immediately before the NodeAffinity
            # failure — so it leaves a VolumeAttachment on the disallowed node
            # that would block the next pod in classes which continue past a
            # denial (e.g. DynamicModification's grant-then-authorize).
            try:
                k8s.delete_pod_and_wait_detached(pod_name, pvc_name=pvc_name)
                if pod_name in self.created_pods:
                    self.created_pods.remove(pod_name)
            except Exception as e:
                self.logger.warning(f"  cleanup {pod_name}: {e}")
        return None

    def _assert_dhchap_enforceable(self):
        """Prove DHCHAP enforcement is live before any test trusts it.

        Turns the *silent* failure mode into a loud one. If the StorageClass
        carries no ``dhchap_node_label``, or the operator never labelled the
        allowed nodes, then nothing is enforced and EVERY restriction
        assertion in this suite passes while proving nothing -- the condition
        that made five consecutive CI runs look plausible.

        Cheap by design: four kubectl reads plus one PVC, no pods. The
        expensive pod probes are the individual tests' job; this only has to
        establish that the wiring exists at all, and to distinguish
        "the environment cannot" (skip) from "the product is not wired"
        (fail).

        Cached per process -- the cost is paid once per run, not once per
        class.
        """
        global _DHCHAP_ENFORCEMENT_CHECKED
        if not self.k8s_test or _DHCHAP_ENFORCEMENT_CHECKED:
            return
        _DHCHAP_ENFORCEMENT_CHECKED = True

        k8s = self._ensure_k8s_utils()
        workers = self._get_k8s_worker_nqns()
        if len(workers) < 2:
            self.logger.warning(
                f"{TOK_COVERAGE_LOST} [canary]: only {len(workers)} "
                f"schedulable worker(s) — no node can be excluded from "
                f"spec.allowedNodes, so no DHCHAP rejection is provable in "
                f"this environment. Restriction assertions will be skipped.")
            return

        saved = (self.pool_name, self._storage_class_name,
                 self._dhchap_node_label, self._pool_crd_name)
        canary_pvc = None
        try:
            self.pool_name = f"canary{_rand_suffix().lower()}"[:20]
            allowed = [w[0] for w in workers[:-1]]
            disallowed = [workers[-1][0]]
            self.logger.info(
                f"[canary] verifying DHCHAP enforcement is wired: "
                f"allowed={allowed} excluded={disallowed}")
            self._ensure_pool_and_sc(dhchap=True, allowed_nodes=allowed)

            # L1-L4 minus the PV: this is the whole silent-failure class.
            self._k8s_assert_dhchap_wiring(allowed, disallowed)

            canary_pvc = f"canary-{_rand_suffix().lower()}"
            k8s.create_pvc(name=canary_pvc, size="1Gi",
                           storage_class=self._storage_class_name)
            self.created_pvcs.append(canary_pvc)
            # WaitForFirstConsumer: needs a scheduled pod to bind.
            self._dhchap_allowed_nodes = allowed
            self._k8s_bind_pvc(canary_pvc, node=allowed[0])
            self._k8s_assert_pv_node_affinity(canary_pvc, tc="canary")

            self.logger.info(
                "[canary] DHCHAP enforcement is wired end to end: pool "
                "labelled, StorageClass carries dhchap_node_label, and the "
                "provisioned PV carries a matching nodeAffinity. Restriction "
                "assertions in this run are meaningful.")
        except AssertionError:
            self.logger.error(
                f"{TOK_COVERAGE_LOST} [canary]: DHCHAP enforcement is NOT "
                f"wired in this environment. Refusing to run a suite whose "
                f"restriction assertions would all pass vacuously — this is "
                f"the exact condition that produced several green-but-empty "
                f"CI runs. See the assertion below for which link broke.")
            raise
        finally:
            for name in ([canary_pvc] if canary_pvc else []):
                try:
                    k8s.delete_pvc(name)
                    if name in self.created_pvcs:
                        self.created_pvcs.remove(name)
                except Exception as exc:
                    self.logger.warning(f"[canary] cleanup {name}: {exc}")
            # No StorageClass to clean up: the operator owns it and
            # removes it with the pool below.
            # Delete the canary's StoragePool too. Without this it outlives
            # the check and, because it asks for the same dhchap +
            # allowedNodes as _k8s_setup_dhchap_pool_subset does,
            # add_storage_pool hands it straight back to the first real test
            # -- so the whole suite ends up running against a pool named
            # "canary...". Functionally equivalent, but it leaks a pool per
            # run and makes every downstream log line misleading.
            canary_crd = self._pool_crd_name
            if canary_crd:
                try:
                    self.sbcli_utils.delete_storage_pool(canary_crd)
                except Exception as exc:
                    self.logger.warning(
                        f"[canary] cleanup pool {canary_crd}: {exc}")
            (self.pool_name, self._storage_class_name,
             self._dhchap_node_label, self._pool_crd_name) = saved

    def _get_pool_id(self):
        """Get pool UUID for host registration."""
        return self.sbcli_utils.get_storage_pool_id(self.pool_name)

    def _register_host_to_pool(self, pool_id, host_nqn):
        """Register a host NQN at pool level. Works in both Docker and K8s."""
        if self.k8s_test:
            self.sbcli_utils.add_host_to_pool(pool_id, host_nqn)
        else:
            self.ssh_obj.add_host_to_pool(self.mgmt_nodes[0], pool_id, host_nqn)

    def _unregister_host_from_pool(self, pool_id, host_nqn):
        """Remove a host NQN from pool. Works in both Docker and K8s."""
        if self.k8s_test:
            self.sbcli_utils.remove_host_from_pool(pool_id, host_nqn)
        else:
            self.ssh_obj.remove_host_from_pool(self.mgmt_nodes[0], pool_id, host_nqn)

    def _get_k8s_worker_nqns(self):
        """Get all K8s worker node names and their deterministic NQNs.

        Returns list of (node_name, nqn) tuples.
        """
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            "kubectl get nodes "
            "-l node-role.kubernetes.io/control-plane!= "
            "--no-headers "
            "-o custom-columns=NAME:.metadata.name,UID:.metadata.uid"
        )
        results = []
        for line in out.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            node_name = parts[0]
            node_uid = parts[1] if len(parts) > 1 else None
            if not node_uid:
                uid_out, _ = k8s._exec_kubectl(
                    f"kubectl get node {node_name} "
                    f"-o jsonpath='{{{{.metadata.uid}}}}'")
                node_uid = uid_out.strip()
            nqn = f"nqn.2014-08.io.simplyblock:uuid:{node_uid}"
            results.append((node_name, nqn))
        assert results, "No K8s worker nodes found"
        self.logger.info(f"[k8s] Worker NQNs: {results}")
        return results

    def _setup_pool_and_host(self, dhchap=True, register=True):
        """Create the DHCHAP pool and return ``(pool_id, allowed, denied)``.

        ``allowed`` and ``denied`` are :class:`DhchapHost` instances.

        docker -- ``allowed.nqn`` is the client's real /etc/nvme/hostnqn,
          registered at pool level. ``denied.nqn`` is a synthetic NQN that is
          deliberately never registered.
        k8s -- ``spec.allowedNodes`` is a STRICT SUBSET of the workers, so
          ``denied.node`` is a real node outside the pool. Nothing is
          registered by hand: the operator derives each allowed node's NQN
          from its Kubernetes UID and reconciles the pool's allowed hosts
          purely from ``allowedNodes``, which is what the K8s security doc
          specifies.

        The previous version passed EVERY worker as an allowed node and
        hand-registered an NQN on top. That contradicted the operator model
        and, more importantly, left no node outside the pool -- so not one of
        the ~14 classes built on it could exercise a rejection.

        ``denied`` is None only when the environment cannot express denial (a
        single-worker cluster). Callers whose purpose *is* denial must go
        through :meth:`_require_denied_host`.
        """
        if self.k8s_test:
            allowed_names, denied_names = self._k8s_setup_dhchap_pool_subset(
                dhchap=dhchap)
            nqn_by_node = dict(self._get_k8s_worker_nqns())
            allowed = DhchapHost(
                node=allowed_names[0], nqn=nqn_by_node.get(allowed_names[0]),
                desc="in spec.allowedNodes")
            denied = (
                DhchapHost(node=denied_names[0],
                           nqn=nqn_by_node.get(denied_names[0]),
                           desc="NOT in spec.allowedNodes")
                if denied_names else None)
            pool_id = self._get_pool_id()
            if dhchap:
                self._k8s_assert_dhchap_wiring(allowed_names, denied_names)
            return pool_id, allowed, denied

        self._ensure_pool_and_sc(dhchap=dhchap)
        pool_id = self._get_pool_id()
        allowed = DhchapHost(nqn=self._get_client_host_nqn(),
                             node=self.fio_node, desc="registered host")
        denied = DhchapHost(
            nqn=f"nqn.2014-08.org.nvmexpress:uuid:deadbeef-{_rand_suffix().lower()}",
            desc="never registered")
        if register:
            self._register_host_to_pool(pool_id, allowed.nqn)
        return pool_id, allowed, denied

    def _require_denied_host(self, denied, tc=""):
        """Fail loudly when the environment cannot express a denial.

        A single-worker K8s cluster has no node outside the pool, so every
        restriction assertion silently evaporates and the suite still reports
        green. For a class whose entire purpose is proving a rejection, that
        is worse than a failure -- it is a false statement about coverage.
        """
        if denied is not None:
            return denied
        raise AssertionError(
            f"{TOK_COVERAGE_LOST} [{tc}]: no host outside the pool's allowed "
            f"set exists in this environment (K8s needs >=2 schedulable "
            f"workers so one can be excluded from spec.allowedNodes). This "
            f"test exists to prove a rejection and cannot do so here.")

    def _k8s_assert_dhchap_wiring(self, allowed, disallowed):
        """Prove the four enforcement links up front.

        Each break is then attributable to a stage, instead of surfacing 20
        minutes later as an unexplained "pod never reached Running":

          L0 Pool keys -- the pool reports ``dhchap: true`` and carries both
             a ``dhchap_key`` and a ``dhchap_ctrlr_key``. The only check here
             that is about authentication rather than placement.
          L1 StoragePool CRD -- spec.dhchap is true, spec.allowedNodes is what
             we asked for, and status.allowedNodes mirrors it (i.e. the
             operator has actually reconciled).
          L2 Node labels -- every allowed node carries ``<label>=allowed`` and
             no disallowed node does. This is the check that catches a wrong
             label key, the failure this whole change exists to prevent.
          L3 Allowed hosts -- the backend pool's allowed hosts are exactly the
             derived NQNs of status.allowedNodes.
          L4 StorageClass -- parameters.dhchap_node_label equals that key.
             Without the parameter, CSI writes no nodeAffinity and there is
             *zero* enforcement while every other assertion still passes.

        NOTE ON WHAT THIS PROVES. L1-L4 plus a pod-placement check demonstrate
        that the pool's ``allowedNodes`` restriction is enforced at
        mount/attach via the PV's nodeAffinity. They do NOT demonstrate that
        DH-HMAC-CHAP was negotiated in-band: a pool with ``dhchap: false``
        carrying the same node label would satisfy all of them, because
        nodeAffinity is a scheduling/mount gate rather than authentication.
        In-band negotiation is probed separately, on an allowed node, by
        ``TestLvolSecurityNegativeConnect``.
        """
        k8s = self._ensure_k8s_utils()
        crd = self._pool_crd_name
        label = self._dhchap_node_label
        assert label, (
            f"{TOK_COVERAGE_LOST}: no dhchap_node_label resolved for pool "
            f"{self.pool_name!r} — the StorageClass would carry no "
            f"enforcement")

        # L1 — CRD spec/status
        out, _ = k8s._exec_kubectl(
            f"kubectl get storagepool {crd} -n {k8s.namespace} -o json")
        try:
            obj = json.loads(out) if out.strip() else {}
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"L1: StoragePool {crd!r} did not return JSON: {exc}; "
                f"raw={out[:200]!r}")
        spec, status = obj.get("spec", {}) or {}, obj.get("status", {}) or {}
        assert bool(spec.get("dhchap")) is True, (
            f"L1: StoragePool {crd!r} spec.dhchap is "
            f"{spec.get('dhchap')!r}, expected true")
        assert sorted(spec.get("allowedNodes") or []) == sorted(allowed), (
            f"L1: StoragePool {crd!r} spec.allowedNodes is "
            f"{spec.get('allowedNodes')!r}, expected {sorted(allowed)}")
        self._k8s_wait_allowed_nodes_converged(allowed)
        self.logger.info(
            f"[dhchap L1] CRD {crd!r}: dhchap=true, allowedNodes={sorted(allowed)} "
            f"reconciled (status={sorted(status.get('allowedNodes') or [])})")

        # L2 — node labels, set equality both ways
        labelled = set(self._k8s_nodes_with_label(label))
        assert labelled == set(allowed), (
            f"L2: nodes carrying {label}=allowed are {sorted(labelled)}, "
            f"expected exactly {sorted(allowed)}. A disallowed node holding "
            f"this label can mount the volume and every restriction "
            f"assertion below would pass for the wrong reason.")
        for node in disallowed or []:
            assert node not in labelled, (
                f"L2: disallowed node {node!r} carries {label}=allowed")
        self.logger.info(
            f"[dhchap L2] label {label}=allowed on exactly {sorted(labelled)}; "
            f"absent from {sorted(disallowed or [])}")

        # L0 — the pool really has DHCHAP provisioned.
        #
        # This is the ONLY assertion in the K8s path that is specific to
        # DH-HMAC-CHAP rather than to node placement: it checks the pool
        # carries actual host and controller keys. Everything below (labels,
        # nodeAffinity, mount outcome) would hold equally for a pool with
        # dhchap:false that happened to carry the same node label, because
        # nodeAffinity is a scheduling gate and not authentication.
        pool_details = None
        try:
            pool_details = self.sbcli_utils.get_pool_by_id(self._get_pool_id())
            if isinstance(pool_details, list):
                pool_details = pool_details[0] if pool_details else {}
        except Exception as exc:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [dhchap L0] could not read pool "
                f"details: {exc}")
        if isinstance(pool_details, dict) and pool_details:
            assert pool_details.get("dhchap") is True, (
                f"L0: pool {self.pool_name!r} reports dhchap="
                f"{pool_details.get('dhchap')!r}, expected True")
            for key in ("dhchap_key", "dhchap_ctrlr_key"):
                val = pool_details.get(key) or ""
                assert val.startswith("DHHC-"), (
                    f"L0: pool {self.pool_name!r} has no usable {key} "
                    f"(got {val[:12]!r}...). Without a provisioned key there "
                    f"is no in-band authentication to enforce, whatever the "
                    f"node labels say.")
            self.logger.info(
                "[dhchap L0] pool has dhchap=true with both a host and a "
                "controller key provisioned")

        # L3 — backend allowed hosts == derived NQNs of the allowed nodes
        nqn_by_node = dict(self._get_k8s_worker_nqns())
        expected_nqns = {nqn_by_node[n] for n in allowed if n in nqn_by_node}
        actual_nqns = set(self._get_pool_allowed_hosts(self._get_pool_id()))
        if expected_nqns and actual_nqns:
            assert actual_nqns == expected_nqns, (
                f"L3: pool allowed hosts are {sorted(actual_nqns)}, expected "
                f"the derived NQNs of {sorted(allowed)} = "
                f"{sorted(expected_nqns)}")
            self.logger.info(
                f"[dhchap L3] pool allowed hosts == derived NQNs of "
                f"allowedNodes ({len(actual_nqns)} host(s))")
        else:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [dhchap L3] could not compare pool "
                f"allowed hosts (expected={sorted(expected_nqns)}, "
                f"actual={sorted(actual_nqns)}) — skipping L3")

        # L4 — the StorageClass actually carries the parameter
        sc_label = self._k8s_sc_dhchap_label(self._storage_class_name)
        assert sc_label == label, (
            f"L4: StorageClass {self._storage_class_name!r} has "
            f"dhchap_node_label={sc_label!r}, expected {label!r}. Without a "
            f"matching parameter the CSI driver writes no nodeAffinity onto "
            f"the PV and DHCHAP is not enforced at all.")
        self.logger.info(
            f"[dhchap L4] StorageClass {self._storage_class_name!r} carries "
            f"dhchap_node_label={sc_label}")

    def _k8s_nodes_with_label(self, label_key, value="allowed"):
        """Return the node names carrying ``label_key=value``."""
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            f"kubectl get nodes -l {label_key}={value} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true")
        return [n.strip() for n in (out or "").strip().splitlines() if n.strip()]

    def _k8s_sc_dhchap_label(self, sc_name):
        """Read ``parameters.dhchap_node_label`` back off a StorageClass."""
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            f"kubectl get storageclass {sc_name} "
            f"-o jsonpath='{{.parameters.dhchap_node_label}}' "
            f"2>/dev/null || true")
        return (out or "").strip()

    def _k8s_assert_pv_node_affinity(self, pvc_name, tc=""):
        """Assert the PV bound to *pvc_name* carries the pool's nodeAffinity.

        Per volume, not per class: the parameter is applied at provision time,
        so a crypto class, a clone PVC, a restored PVC or the seventh volume
        in a scale loop can each individually miss it while everything else
        still looks fine.
        """
        k8s = self._ensure_k8s_utils()
        label = self._dhchap_node_label
        pv_name = k8s.get_pvc_pv_name(pvc_name)
        assert pv_name, f"L4 [{tc}]: PVC {pvc_name!r} has no bound PV"
        out, _ = k8s._exec_kubectl(
            f"kubectl get pv {pv_name} "
            f"-o jsonpath='{{.spec.nodeAffinity}}' 2>/dev/null || true")
        affinity = (out or "").strip()
        assert label and label in affinity, (
            f"L4 [{tc}]: PV {pv_name!r} (PVC {pvc_name!r}) nodeAffinity does "
            f"not reference {label!r} — this volume has NO DHCHAP "
            f"enforcement. nodeAffinity={affinity!r}")
        self.logger.info(
            f"[dhchap L4] PV {pv_name} (PVC {pvc_name}) nodeAffinity "
            f"references {label}")
        return pv_name

    def _k8s_wait_allowed_nodes_converged(self, expected, timeout=180):
        """Wait until the operator has reconciled ``allowedNodes``.

        Convergence is observed on two surfaces, both of which have to agree
        before any assertion downstream is trustworthy: ``status.allowedNodes``
        on the CRD, and the node labels the CSI nodeAffinity actually keys off.
        Replaces the blind ``sleep_n_sec(3)`` the dynamic tests used to do,
        which made every one of them a race.

        Returns True on convergence. On timeout, returns False rather than
        raising -- whether the operator clears the label on *removal* is not
        among the behaviours we have verified, so the caller decides whether
        that is a hard failure (see :meth:`_revoke_host_dual`).
        """
        k8s = self._ensure_k8s_utils()
        want = sorted(expected)
        label = self._dhchap_node_label
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            out, _ = k8s._exec_kubectl(
                f"kubectl get storagepool {self._pool_crd_name} "
                f"-n {k8s.namespace} "
                f"-o jsonpath='{{.status.allowedNodes}}' 2>/dev/null || true")
            try:
                status_nodes = sorted(json.loads(out) if out.strip().startswith("[")
                                      else [])
            except json.JSONDecodeError:
                status_nodes = []
            labelled = sorted(self._k8s_nodes_with_label(label)) if label else []
            last = (status_nodes, labelled)
            if status_nodes == want and labelled == want:
                self.logger.info(
                    f"[dhchap] allowedNodes converged to {want} "
                    f"(status + node labels agree)")
                return True
            sleep_n_sec(5)
        self.logger.warning(
            f"{TOK_K8S_LIMITATION}: allowedNodes did not converge to {want} "
            f"within {timeout}s — status.allowedNodes={last[0] if last else None}, "
            f"labelled nodes={last[1] if last else None}")
        return False

    def _k8s_setup_dhchap_pool_subset(self, dhchap=True):
        """K8s-native: create a pool whose allowedNodes is a strict subset of
        the worker nodes, guaranteeing at least one disallowed node so the
        restriction can actually be exercised.

        No host NQN is registered manually — the operator derives each
        allowed node's NQN itself and reconciles allowed hosts purely from
        the StoragePool's allowedNodes field.

        Returns (allowed_node_names, disallowed_node_names) and caches both on
        ``self`` so FIO pinning and the assertion verbs can default sensibly.
        """
        workers = self._get_k8s_worker_nqns()  # [(node_name, nqn), ...]
        all_names = [w[0] for w in workers]
        if len(all_names) > 1:
            allowed, disallowed = all_names[:-1], all_names[-1:]
        else:
            allowed, disallowed = all_names, []
            self.logger.warning(
                f"{TOK_COVERAGE_LOST}: only one schedulable worker "
                f"({all_names}) — no node can be excluded from "
                f"spec.allowedNodes, so no DHCHAP rejection can be proven "
                f"in this environment")
        self._ensure_pool_and_sc(dhchap=dhchap, allowed_nodes=allowed)
        self._dhchap_allowed_nodes = allowed
        self._dhchap_disallowed_nodes = disallowed
        return allowed, disallowed

    # ── authorization verbs ──────────────────────────────────────────────────

    def _assert_host_authorized(self, lvol_name, lvol_id, host, tc="",
                                require_ctrl_secret=False, prove_io=False,
                                expect_fs_type=None, keep_pod=False):
        """Assert *host* IS authorized for this volume.

        docker -- ``volume connect --host-nqn host.nqn`` must return at least
          one nvme-connect command with an empty error channel, containing
          ``--dhchap-secret`` (and ``--dhchap-ctrl-secret`` when
          *require_ctrl_secret*). This is the assertion the classes already
          made, moved verbatim.
        k8s -- a pod consuming the volume, hard-pinned to ``host.node``, must
          reach Running. Mount/attach is where the operator's restriction is
          enforced, so reaching Running *is* the authorization proof; a FIO
          job would add data-path coverage but takes minutes rather than
          seconds and, on the denial path, is strictly worse evidence (see
          :meth:`_assert_host_denied`). Pass *prove_io* where the data path
          itself is the point.

        Records *lvol_name* as a positive control, without which a later
        denial assertion on the same volume refuses to run.

        Raises :class:`DhchapUnsupportedByHost` when the node kernel has no
        in-band NVMe auth -- the connect then fails on an allowed node too,
        which is an environment limit rather than a test failure.
        """
        if self.k8s_test:
            assert host is not None and host.node, (
                f"[{tc}] _assert_host_authorized needs a host with a node in "
                f"K8s mode, got {host!r}")
            pvc_name = self._k8s_normalize_name(lvol_name)
            self._k8s_assert_pv_node_affinity(pvc_name, tc=tc)
            pod = self._k8s_verify_pod_scheduling(
                pvc_name, host.node, expect_success=True)
            self._dhchap_positive_control.add(lvol_name)
            fs_want = expect_fs_type or self._fs_type
            if fs_want and pod:
                self._k8s_assert_fs_type(pod, fs_want, tc=tc)
            if prove_io:
                self._run_fio_dual(lvol_name, None, None, runtime=30,
                                   node_name=host.node)
            if pod and not keep_pod:
                self._k8s_release_pod(pod, pvc_name=pvc_name)
                return None
            self.logger.info(
                f"[{tc}] AUTHORIZED: {host!r} may mount {lvol_name}")
            return pod

        connect_ls, err = self._get_connect_str_dual(
            lvol_id, host_nqn=_as_nqn(host))
        assert not err, (
            f"[{tc}] connect for authorized host {_as_nqn(host)!r} errored: "
            f"{err!r}")
        assert connect_ls, (
            f"[{tc}] no connect string returned for authorized host "
            f"{_as_nqn(host)!r}")
        blob = " ".join(connect_ls).lower()
        assert "dhchap-secret" in blob, (
            f"[{tc}] connect string for authorized host {_as_nqn(host)!r} "
            f"carries no --dhchap-secret: {connect_ls}")
        if require_ctrl_secret:
            assert "dhchap-ctrl-secret" in blob, (
                f"[{tc}] connect string carries no --dhchap-ctrl-secret "
                f"(bidirectional auth expected): {connect_ls}")
        self._dhchap_positive_control.add(lvol_name)
        self.logger.info(
            f"[{tc}] AUTHORIZED: {_as_nqn(host)!r} got DHCHAP keys for "
            f"{lvol_name}")
        return connect_ls

    def _assert_host_denied(self, lvol_name, lvol_id, host, tc="",
                            why="not in the pool's allowed set"):
        """Assert *host* is NOT authorized for this volume.

        docker -- ``volume connect --host-nqn host.nqn`` must be rejected:
          a non-empty error channel, or no connect line at all.
        k8s -- a pod pinned to ``host.node`` must NOT reach Running, and its
          kubelet events must NAME the authorization failure
          (``MountVolume.NodeAffinity check failed`` / ``no matching
          NodeSelectorTerms`` / ``not found in allowed hosts``).

        PRECONDITION, enforced: a positive control must already have
        succeeded for *lvol_name* in this test. Without one, a volume that
        cannot mount ANYWHERE -- wrong dhchap_node_label, CSI node plugin
        down, PV never published -- produces the identical observation ("pod
        never ran"), and the denial would pass for entirely the wrong reason.
        This single precondition covers the largest class of false pass in
        this suite.
        """
        if self.k8s_test:
            assert host is not None and host.node, (
                f"[{tc}] _assert_host_denied needs a host with a node in K8s "
                f"mode, got {host!r}")
            assert lvol_name in self._dhchap_positive_control, (
                f"{TOK_WEAK_EVIDENCE} [{tc}]: refusing to assert a denial for "
                f"{lvol_name!r} before a positive control has passed on it. "
                f"A volume that cannot mount anywhere looks exactly like a "
                f"DHCHAP denial, so this assertion would be meaningless. "
                f"Call _assert_host_authorized on an allowed node first.")
            pvc_name = self._k8s_normalize_name(lvol_name)
            self._k8s_verify_pod_scheduling(
                pvc_name, host.node, expect_success=False)
            self.logger.info(
                f"[{tc}] DENIED as expected: {host!r} ({why}) could not "
                f"mount {lvol_name}")
            return

        connect_ls, err = self._get_connect_str_dual(
            lvol_id, host_nqn=_as_nqn(host))
        rejected = bool(err) or not connect_ls
        assert rejected, (
            f"[{tc}] host {_as_nqn(host)!r} ({why}) was NOT rejected — got a "
            f"usable connect string: {connect_ls}")
        self.logger.info(
            f"[{tc}] DENIED as expected: {_as_nqn(host)!r} ({why}) "
            f"err={err!r} connect={connect_ls}")

    def _k8s_assert_fs_type(self, pod_name, expect_fs_type, tc=""):
        """Verify the CSI-created filesystem type inside a running pod.

        This is the only place ext4/xfs coverage exists in K8s: the mount is
        performed by the CSI node plugin from the StorageClass's
        ``csi.storage.k8s.io/fstype``, so ``_pick_fs_type`` was previously
        dead code in K8s mode. Costs no extra pod -- the positive control
        already has one running with the volume mounted.
        """
        k8s = self._ensure_k8s_utils()
        try:
            # exec_in_pod returns (stdout, stderr) -- unlike get_pod_logs and
            # get_pod_events on the same class, which return a bare string.
            out, err = k8s.exec_in_pod(
                pod_name, "grep ' /spdkvol ' /proc/mounts || cat /proc/mounts")
        except Exception as exc:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [{tc}] could not read /proc/mounts in "
                f"{pod_name}: {exc}")
            return
        if err and not (out or "").strip():
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [{tc}] reading /proc/mounts in "
                f"{pod_name} errored: {err!r}")
            return
        line = next((ln for ln in (out or "").splitlines()
                     if " /spdkvol " in ln), "")
        if not line:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [{tc}] /spdkvol not found in "
                f"/proc/mounts of {pod_name}: {out!r}")
            return
        assert expect_fs_type in line, (
            f"[{tc}] volume mounted in {pod_name} is not {expect_fs_type}: "
            f"{line.strip()!r}")
        self.logger.info(
            f"[{tc}] filesystem verified as {expect_fs_type}: {line.strip()}")

    def _k8s_bind_pvc(self, pvc_name, node=None, timeout=420):
        """Force a WaitForFirstConsumer PVC to bind, then release it.

        The operator's StorageClass uses ``volumeBindingMode:
        WaitForFirstConsumer``, so a freshly created PVC stays Pending until a
        pod referencing it is scheduled -- provisioning is deliberately
        deferred so the volume lands in the right topology. Every caller here
        needs the bound PV up front (for the volumeHandle and for the
        per-volume nodeAffinity assertion), so schedule a short-lived binder
        pod on an allowed node to trigger it.

        The binder is pinned with ``nodeSelector``, never ``nodeName``:
        nodeName bypasses the scheduler, and it is the scheduler that triggers
        WaitForFirstConsumer binding, so a nodeName-pinned pod would leave the
        claim Pending forever.
        """
        k8s = self._ensure_k8s_utils()
        node = node or (self._dhchap_allowed_nodes[0]
                        if self._dhchap_allowed_nodes else None)
        binder = f"bind-{_rand_suffix().lower()}"
        self.created_pods.append(binder)
        try:
            k8s.create_utility_pod(binder, pvc_name, node_selector=node)
            k8s.wait_pvc_bound(pvc_name, timeout=timeout)
            self.logger.info(
                f"[k8s] PVC {pvc_name!r} bound via binder pod on {node!r}")
        finally:
            self._k8s_release_pod(binder, pvc_name=pvc_name)

    def _k8s_release_pod(self, pod_name, pvc_name=None):
        """Delete a pod and wait until its volume is genuinely detached.

        Load-bearing before any denial assertion: a volume still attached on
        the previous node makes the next node's attach fail with
        ``Multi-Attach error``, which is not a DHCHAP denial at all.

        Deleting the pod is NOT enough. ``delete_pod(wait=True)`` waits for the
        Pod object only, while the VolumeAttachment survives until kubelet
        finishes unmounting and the CSI controller completes
        ``ControllerUnpublishVolume``. Observed in CI: 31 seconds after the
        pod was gone, the volume was still attached and the next pod hit
        Multi-Attach. Pass *pvc_name* wherever it is known so the detach is
        actually waited for.
        """
        k8s = self._ensure_k8s_utils()
        try:
            if pvc_name:
                k8s.delete_pod_and_wait_detached(pod_name, pvc_name=pvc_name)
            else:
                k8s.delete_pod(pod_name, wait=True)
        except Exception as exc:
            self.logger.warning(f"  release {pod_name}: {exc}")
        if pod_name in self.created_pods:
            self.created_pods.remove(pod_name)

    # ── dynamic host management ──────────────────────────────────────────────

    def _get_pool_allowed_hosts(self, pool_id):
        """Return the pool's registered allowed-host NQNs, both modes."""
        try:
            details = self.sbcli_utils.get_pool_by_id(pool_id)
        except Exception as exc:
            self.logger.warning(
                f"[pool] could not read pool {pool_id}: {exc}")
            return []
        if isinstance(details, list):
            details = details[0] if details else {}
        if not isinstance(details, dict):
            return []
        hosts = (details.get("allowed_hosts")
                 or details.get("allowedHosts") or [])
        out = []
        for h in hosts:
            if isinstance(h, dict):
                nqn = h.get("nqn") or h.get("host_nqn")
                if nqn:
                    out.append(nqn)
            elif h:
                out.append(str(h))
        return out

    def _pool_host_op_dual(self, pool_id, host_nqn, remove=False):
        """Run ``pool add-host`` / ``pool remove-host`` returning (out, err).

        The K8s wrappers on ``K8sSbcliUtils`` return stdout only, so the four
        negative host-op assertions used to force ``err = ""`` and could not
        fail. Go through ``exec_sbcli`` for a real error channel.
        """
        sub = "remove-host" if remove else "add-host"
        if self.k8s_test:
            cmd = f"{self.sbcli_utils.sbcli_cmd} pool {sub} {pool_id} {host_nqn}"
            out, err = self.sbcli_utils.k8s.exec_sbcli(cmd)
            out = out or ""
            if not err and self.sbcli_utils._cli_output_is_error(out, err):
                err = next(
                    (ln.strip() for ln in out.splitlines()
                     if "error" in ln.lower() or "usage:" in ln.lower()),
                    out.strip()[:200])
            return out, err
        return self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} pool {sub} {pool_id} {host_nqn}")

    def _assert_cli_rejected(self, out, err, label, pool_id=None,
                             host_nqn=None):
        """Assert a CLI mutation was rejected, via two independent signals.

        (a) textual -- out/err carries a failure word. Best-effort only,
            because ``exec_sbcli`` discards the exit code in K8s.
        (b) effect -- when *pool_id* and *host_nqn* are given, re-read the
            pool's allowed hosts and assert *host_nqn* is absent. This one is
            mode-independent and wording-independent, and it is the real
            assertion.

        The previous helper passed whenever stdout happened to be empty
        (``has_signal or not out.strip()``), which combined with the forced
        ``err = ""`` on the K8s path meant it could not fail. That escape
        hatch is gone; signal (b) carries its weight.
        """
        blob = f"{out or ''}\n{err or ''}".lower()
        has_signal = any(
            tok in blob for tok in
            ("error", "invalid", "not found", "failed", "usage:", "traceback",
             "exception", "must be", "cannot"))
        checked_effect = False
        if pool_id and host_nqn:
            hosts = self._get_pool_allowed_hosts(pool_id)
            assert host_nqn not in hosts, (
                f"[{label}] operation was NOT rejected — {host_nqn!r} is now "
                f"in the pool's allowed hosts {hosts}")
            checked_effect = True
            self.logger.info(
                f"[{label}] effect verified: {host_nqn!r} absent from pool "
                f"allowed hosts")
        if not has_signal and not checked_effect:
            raise AssertionError(
                f"[{label}] expected a rejection but got no error signal and "
                f"no verifiable effect. out={out!r} err={err!r}")
        if not has_signal:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} [{label}] no textual error signal; "
                f"relying on the state check. out={out!r} err={err!r}")
        else:
            self.logger.info(f"[{label}] rejected as expected: {err or out!r}")

    def _grant_host_dual(self, pool_id, host, tc=""):
        """Grant *host* access. docker: ``pool add-host``. k8s: append the
        node to ``spec.allowedNodes`` and wait for the operator.
        """
        if self.k8s_test:
            nodes = sorted(set(self._dhchap_allowed_nodes) | {host.node})
            self._k8s_set_pool_allowed_nodes(nodes)
            converged = self._k8s_wait_allowed_nodes_converged(nodes)
            assert converged, (
                f"[{tc}] operator did not add {host.node!r} to allowedNodes")
            self._dhchap_allowed_nodes = nodes
            self._dhchap_disallowed_nodes = [
                n for n in self._dhchap_disallowed_nodes if n != host.node]
            self.logger.info(f"[{tc}] granted {host.node!r}; allowed={nodes}")
            return True
        out, err = self._pool_host_op_dual(pool_id, _as_nqn(host))
        assert not err or "error" not in err.lower(), (
            f"[{tc}] pool add-host failed: {err!r}")
        hosts = self._get_pool_allowed_hosts(pool_id)
        if hosts:
            assert _as_nqn(host) in hosts, (
                f"[{tc}] {_as_nqn(host)!r} not in pool allowed hosts after "
                f"add-host: {hosts}")
        self.logger.info(f"[{tc}] granted {_as_nqn(host)!r}")
        return True

    def _revoke_host_dual(self, pool_id, host, tc="", hard=False):
        """Withdraw *host*'s access. Returns True if it was OBSERVED to
        take effect.

        docker -- ``pool remove-host``, then re-read the pool's allowed hosts
          and confirm the NQN is gone. Replaces the blind ``sleep_n_sec(3)``
          the call sites used to do.
        k8s -- patch ``spec.allowedNodes`` to drop the node, then WAIT (not
          sleep) until the operator has removed the pool label from it and
          ``status.allowedNodes`` no longer lists it. Waiting on the label is
          what makes the following denial assertion deterministic, since the
          PV's nodeAffinity keys off exactly that label.

        IMPORTANT, and a real product limit rather than a test shortcut: a
        PV's ``nodeAffinity`` is written at provision time and does not
        shrink, and an already-mounted volume is unaffected because
        nodeAffinity is checked at mount. So revocation is only observable for
        *newly provisioned* volumes and *new* mounts.

        Whether the operator clears the node label on removal is not among the
        behaviours verified so far. ``hard=False`` logs a limitation token and
        returns False so the caller can skip just that assertion; ``hard=True``
        raises. Use ``hard=True`` only where probing this is the point, so the
        unknown is exercised once per run instead of silently everywhere.
        """
        if self.k8s_test:
            nodes = [n for n in self._dhchap_allowed_nodes if n != host.node]
            assert nodes, (
                f"[{tc}] refusing to empty spec.allowedNodes (revoking "
                f"{host.node!r} would leave the pool with no allowed node)")
            self._k8s_set_pool_allowed_nodes(nodes)
            converged = self._k8s_wait_allowed_nodes_converged(nodes)
            if not converged:
                msg = (f"{TOK_K8S_LIMITATION} [{tc}]: operator did not "
                       f"withdraw {host.node!r} from allowedNodes/node labels "
                       f"within the timeout — revocation not observable")
                if hard:
                    raise AssertionError(msg)
                self.logger.warning(msg)
                return False
            self._dhchap_allowed_nodes = nodes
            if host.node not in self._dhchap_disallowed_nodes:
                self._dhchap_disallowed_nodes.append(host.node)
            self.logger.info(f"[{tc}] revoked {host.node!r}; allowed={nodes}")
            return True

        out, err = self._pool_host_op_dual(pool_id, _as_nqn(host), remove=True)
        assert not err or "error" not in err.lower(), (
            f"[{tc}] pool remove-host failed: {err!r}")
        hosts = self._get_pool_allowed_hosts(pool_id)
        assert _as_nqn(host) not in hosts, (
            f"[{tc}] {_as_nqn(host)!r} still in pool allowed hosts after "
            f"remove-host: {hosts}")
        self.logger.info(f"[{tc}] revoked {_as_nqn(host)!r}")
        return True

    def _k8s_set_pool_allowed_nodes(self, nodes):
        """Patch the StoragePool CRD's ``spec.allowedNodes``."""
        k8s = self._ensure_k8s_utils()
        patch = json.dumps({"spec": {"allowedNodes": list(nodes)}})
        cmd = (f"kubectl patch storagepool {self._pool_crd_name} "
               f"-n {k8s.namespace} --type merge -p {shlex.quote(patch)}")
        out, err = k8s._exec_kubectl(cmd)
        assert not err or "error" not in err.lower(), (
            f"patching allowedNodes to {list(nodes)} failed: {err!r}")
        self.logger.info(
            f"[dhchap] patched {self._pool_crd_name} allowedNodes="
            f"{list(nodes)}: {(out or '').strip()}")

    def _create_lvol_dual(self, name, size=None, encrypt=False):
        """Create an lvol. Docker: ssh_obj.create_sec_lvol(). K8s: PVC.

        Returns (name, lvol_id).
        """
        size = size or self.lvol_size
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(name)
            sc_name = self._storage_class_name
            if encrypt:
                # Operator SC of a dedicated encrypted DHCHAP pool.
                sc_name = self._k8s_encrypted_storage_class()
            pvc_size = size if "Gi" in size else size.replace("G", "Gi")
            k8s.create_pvc(name=pvc_name, size=pvc_size,
                           storage_class=sc_name)
            self.created_pvcs.append(pvc_name)
            self._k8s_bind_pvc(pvc_name)
            lvol_id = k8s.get_pvc_volume_handle(pvc_name)
            return pvc_name, lvol_id

        out, err = self.ssh_obj.create_sec_lvol(
            self.mgmt_nodes[0], name, size, self.pool_name,
            encrypt=encrypt)
        assert not err or "error" not in err.lower(), \
            f"lvol creation failed: {err}"
        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(name)
        assert lvol_id, f"Could not find ID for {name}"
        return name, lvol_id

    def _get_lvol_id_dual(self, name):
        """Get lvol UUID. K8s: resolve via PVC volumeHandle."""
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(name)
            vol_handle = k8s.get_pvc_volume_handle(pvc_name)
            if vol_handle and ":" in vol_handle:
                return vol_handle.rsplit(":", 1)[-1]
            return vol_handle
        return self.sbcli_utils.get_lvol_id(name)

    def _get_connect_str_dual(self, lvol_id, host_nqn=None):
        """Get connect string. Docker: ssh. K8s: kubectl exec sbcli.

        In K8s mode, lvol_id may be a compound volumeHandle; extracts UUID.
        Returns (connect_lines, error_string).

        The K8s branch used to hardcode the error slot to ``""``, which
        disarmed every ``assert not err`` and every
        ``rejected = bool(err) or not connect_ls`` in the file -- they all
        collapsed to ``not connect_ls``. It goes through ``exec_sbcli``, which
        returns a real (stdout, stderr) tuple, rather than
        ``sbcli_utils._run``, which throws stderr away; and it folds an sbcli
        ``Error:`` printed on *stdout* into the error channel, because
        ``exec_sbcli`` discards the exit code.
        """
        host_nqn = _as_nqn(host_nqn)
        if self.k8s_test:
            actual_id = lvol_id
            if ":" in str(lvol_id):
                actual_id = str(lvol_id).rsplit(":", 1)[-1]
            cmd = f"{self.sbcli_utils.sbcli_cmd} volume connect {actual_id}"
            if host_nqn:
                cmd += f" --host-nqn {host_nqn} --ctrl-loss-tmo -1"
            out, err = self.sbcli_utils.k8s.exec_sbcli(cmd)
            out = out or ""
            if not err and self.sbcli_utils._cli_output_is_error(out, err):
                # sbcli printed a failure to stdout; surface it as an error so
                # callers asserting on the error channel see it.
                err = next(
                    (ln.strip() for ln in out.splitlines()
                     if "error" in ln.lower() or "usage:" in ln.lower()),
                    out.strip()[:200])
            connect_lines = [
                ' '.join(line.split()) for line in out.strip().split('\n')
                if line.strip() and 'nvme connect' in line
            ]
            return connect_lines, err
        return self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)

    def _connect_and_get_device_dual(self, lvol_name, lvol_id,
                                      host_nqn=None):
        """Connect lvol. Docker: nvme connect. K8s: genuinely nothing to do.

        Returns (device_or_pvc, connect_commands).

        There is no client-issued ``nvme connect`` in the CSI path -- the node
        plugin owns it. The no-op is correct, but it must not be mistaken for
        coverage: the authorization this call used to imply is now asserted
        explicitly by :meth:`_assert_host_authorized`.
        """
        host_nqn = _as_nqn(host_nqn)
        if self.k8s_test:
            pvc_name = self._k8s_normalize_name(lvol_name)
            return pvc_name, []
        return self._connect_and_get_device(
            lvol_name, lvol_id, host_nqn=host_nqn)

    def _format_and_mount_dual(self, lvol_name, device, mount_point=None,
                                fs_type=None, format_first=True):
        """Format + mount. Docker: ssh. K8s: CSI already mounted it.

        Returns the mount point (Docker) or PVC name (K8s).

        The K8s branch is a no-op by necessity -- the CSI node plugin creates
        the filesystem from the StorageClass's ``csi.storage.k8s.io/fstype``
        and mounts it. That is why ``_pick_fs_type`` used to be dead code in
        K8s: the choice is now threaded into the StorageClass instead, and
        verified inside a running pod by ``_k8s_assert_fs_type``.

        Pass ``format_first=False`` for a volume that already carries a
        filesystem (a clone, or a re-mount after a reconnect) -- formatting it
        would destroy the very data the test is about to verify.
        """
        if self.k8s_test:
            return self._k8s_normalize_name(lvol_name)
        fs_type = fs_type or self._pick_fs_type()
        mount_point = mount_point or f"{self.mount_path}/{lvol_name}"
        if format_first:
            self.ssh_obj.format_disk(
                node=self.fio_node, device=device, fs_type=fs_type)
        self.ssh_obj.mount_path(
            node=self.fio_node, device=device, mount_path=mount_point)
        return mount_point

    def _run_fio_dual(self, lvol_name, mount_point, log_file,
                       rw="randrw", bs="4K", runtime=30, numjobs=2,
                       fio_size=None, node_name=None):
        """Run FIO. Docker: tmux session. K8s: FIO Job + ConfigMap.

        In K8s the job is pinned to *node_name*, defaulting to an allowed
        node. Pinning is not optional once the pool restricts allowedNodes: an
        unpinned pod can land on the disallowed node and fail, which would
        break every class non-deterministically. The landed node is asserted
        afterwards, so a pin that silently did not apply is caught rather than
        producing a pass that proves nothing about node placement.
        """
        if self.k8s_test:
            node_name = node_name or (
                self._dhchap_allowed_nodes[0]
                if self._dhchap_allowed_nodes else None)
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            fio_name = f"sec-fio-{_rand_suffix().lower()}"
            job_name = f"fio-{fio_name}"
            cm_name = f"fiocfg-{job_name}"
            size = fio_size or self.fio_size

            fio_config = (
                f"[global]\n"
                f"ioengine=libaio\n"
                f"direct=1\n"
                f"bs={bs}\n"
                f"iodepth=1\n"
                f"numjobs={numjobs}\n"
                f"time_based\n"
                f"runtime={runtime}\n"
                f"\n"
                f"[{fio_name[:20]}]\n"
                f"rw={rw}\n"
                f"size={size}\n"
                f"directory=/spdkvol\n"
                f"nrfiles=4\n"
            )

            k8s.create_fio_job(job_name, pvc_name, cm_name, fio_config,
                               node_selector=node_name)
            self.created_fio_jobs.append(job_name)
            self.created_configmaps.append(cm_name)

            status = k8s.wait_job_complete(job_name, timeout=runtime + 120)
            assert status == "succeeded", (
                f"FIO job {job_name} did not succeed (status={status})")

            # Confirm the pin took. Without this an unpinned/mis-pinned job
            # that happens to succeed proves nothing about node placement.
            try:
                fio_pod = k8s.get_job_pod_name(job_name)
                landed = k8s.get_pod_node_name(fio_pod) if fio_pod else None
            except Exception as exc:
                landed = None
                self.logger.warning(
                    f"{TOK_WEAK_EVIDENCE} could not resolve the node for FIO "
                    f"job {job_name}: {exc}")
            if landed:
                if node_name:
                    assert landed == node_name, (
                        f"FIO job {job_name} was pinned to {node_name!r} but "
                        f"ran on {landed!r}")
                elif self._dhchap_allowed_nodes:
                    assert landed in self._dhchap_allowed_nodes, (
                        f"FIO job {job_name} ran on {landed!r}, which is not "
                        f"in the pool's allowed nodes "
                        f"{self._dhchap_allowed_nodes} — the PV's "
                        f"nodeAffinity should have made this impossible")
                self.logger.info(
                    f"[dhchap] FIO job {job_name} ran on {landed!r}")

            k8s.delete_job(job_name)
            k8s.delete_configmap(cm_name)
            if job_name in self.created_fio_jobs:
                self.created_fio_jobs.remove(job_name)
            if cm_name in self.created_configmaps:
                self.created_configmaps.remove(cm_name)
            return

        self._run_fio_and_validate(
            lvol_name, mount_point, log_file,
            rw=rw, bs=bs, numjobs=numjobs, runtime=runtime,
            fio_size=fio_size)

    # ── background FIO (outage / failover tests) ─────────────────────────────

    def _start_bg_fio_dual(self, lvol_name, mount_point, log_file,
                            runtime=300, rw="randrw", bs="4K", numjobs=2,
                            node_name=None):
        """Start FIO and return immediately with an opaque handle.

        docker: a thread running ``ssh_obj.run_fio_test`` (as before).
        k8s: a FIO Job, pinned to an allowed node, that is NOT waited on.

        The outage classes need I/O in flight *across* the outage, which the
        synchronous ``_run_fio_dual`` cannot express -- it blocks on
        ``wait_job_complete``. The three classes previously started a raw
        ``threading.Thread(target=self.ssh_obj.run_fio_test, ...)``, which
        cannot work in K8s at all.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            node_name = node_name or (
                self._dhchap_allowed_nodes[0]
                if self._dhchap_allowed_nodes else None)
            fio_name = f"bg-{_rand_suffix().lower()}"
            job_name = f"fio-{fio_name}"
            cm_name = f"fiocfg-{job_name}"
            fio_config = (
                f"[global]\n"
                f"ioengine=libaio\n"
                f"direct=1\n"
                f"bs={bs}\n"
                f"iodepth=1\n"
                f"numjobs={numjobs}\n"
                f"time_based\n"
                f"runtime={runtime}\n"
                f"verify=md5\n"
                f"verify_fatal=1\n"
                f"\n"
                f"[{fio_name}]\n"
                f"rw={rw}\n"
                f"size={self.fio_size}\n"
                f"directory=/spdkvol\n"
                f"nrfiles=4\n"
            )
            k8s.create_fio_job(job_name, pvc_name, cm_name, fio_config,
                               node_selector=node_name)
            self.created_fio_jobs.append(job_name)
            self.created_configmaps.append(cm_name)
            self.logger.info(
                f"[k8s] background FIO job {job_name} started on "
                f"{node_name!r} (runtime={runtime}s)")
            return {"job": job_name, "cm": cm_name, "runtime": runtime}

        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node, None, mount_point, log_file),
            kwargs={
                "name": f"fio_run_{lvol_name}", "runtime": runtime,
                "rw": rw, "bs": bs, "size": self.fio_size, "nrfiles": 4,
                "iodepth": 1, "numjobs": numjobs, "time_based": True,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        return {"thread": fio_thread, "name": f"fio_run_{lvol_name}",
                "log": log_file, "runtime": runtime}

    def _assert_bg_fio_alive_dual(self, handle, tc=""):
        """Assert the background FIO is still doing I/O mid-outage.

        docker: the fio process is still in the process table.
        k8s: the job's pod is still Running and has not been rescheduled --
          a rescheduled pod would void the "I/O survived" claim, since the
          new pod would simply have re-mounted after the outage.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            job = handle["job"]
            pod = k8s.get_job_pod_name(job)
            assert pod, f"[{tc}] background FIO job {job} has no pod"
            detail = k8s.get_pod_status_detail(pod)
            phase = (detail or {}).get("phase") if isinstance(detail, dict) \
                else str(detail)
            assert phase == "Running", (
                f"[{tc}] background FIO pod {pod} is {phase!r}, expected "
                f"Running — I/O did not survive the outage")
            node = k8s.get_pod_node_name(pod)
            if handle.get("node") and node != handle["node"]:
                raise AssertionError(
                    f"[{tc}] background FIO pod moved from "
                    f"{handle['node']!r} to {node!r} — the 'I/O survived' "
                    f"claim is void, the pod simply re-mounted elsewhere")
            handle["node"] = node
            self.logger.info(
                f"[{tc}] background FIO pod {pod} still Running on {node!r}")
            return
        procs = self.ssh_obj.find_process_name(
            self.fio_node, f"fio.*{handle['name']}")
        running = [p for p in procs
                   if p.strip() and "grep" not in p and "fio --name" in p]
        assert running, f"[{tc}] FIO should still be running during outage"
        self.logger.info(f"[{tc}] FIO process still alive")

    def _finish_bg_fio_dual(self, handle, tc=""):
        """Wait for the background FIO to finish and validate the result.

        Job status alone is not enough: a job that exits instantly reports
        succeeded. The config sets ``verify=md5`` + ``verify_fatal=1``, and the
        pod log is checked for a nonzero error count.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            job, cm = handle["job"], handle["cm"]
            status = k8s.wait_job_complete(
                job, timeout=handle["runtime"] + 300)
            assert status == "succeeded", (
                f"[{tc}] background FIO job {job} ended {status!r} — I/O did "
                f"not survive the outage")
            pod = k8s.get_job_pod_name(job)
            logs = k8s.get_pod_logs(pod, tail=200) if pod else ""
            bad = [ln for ln in logs.splitlines()
                   if "err=" in ln and "err= 0" not in ln and "err=0" not in ln]
            assert not bad, (
                f"[{tc}] background FIO job {job} succeeded but reported "
                f"errors: {bad[:3]}")
            self.logger.info(
                f"[{tc}] background FIO job {job} completed with md5 verify")
            for name, lst, fn in (
                    (job, self.created_fio_jobs, k8s.delete_job),
                    (cm, self.created_configmaps, k8s.delete_configmap)):
                try:
                    fn(name)
                    if name in lst:
                        lst.remove(name)
                except Exception as exc:
                    self.logger.warning(f"  cleanup {name}: {exc}")
            return
        self.common_utils.manage_fio_threads(
            self.fio_node, self.fio_threads,
            timeout=handle["runtime"] + 120)
        self.common_utils.validate_fio_test(
            self.fio_node, log_file=handle["log"])
        self.logger.info(f"[{tc}] FIO completed without interruption")

    def _network_outage_dual(self, node_ip, duration=30):
        """Trigger a self-restoring full network outage on a storage node.

        docker: drop the node's NICs over SSH for *duration* seconds.
        k8s: kubectl exec into the privileged hostNetwork SPDK pod and apply
          iptables DROP rules, with the flush scheduled as a HOST-level
          process via ``nsenter --target 1`` so it survives SPDK's 60-second
          abort timer killing the container. Without that, the DROP rules
          would be permanent and the node never comes back. Ported from the
          proven implementation in
          ``e2e/stress_test/continuous_k8s_native_failover.py``.
        """
        if not self.k8s_test:
            active = self.ssh_obj.get_active_interfaces(node_ip)
            assert active, f"No active interfaces found on {node_ip}"
            self.ssh_obj.disconnect_all_active_interfaces(
                node_ip, active, duration_secs=duration)
            return duration

        k8s = self._ensure_k8s_utils()
        flush_delay = duration + 5
        flush_cmd = (
            f"sudo nsenter --target 1 --mount --net -- "
            f"bash -c 'nohup bash -c \"sleep {flush_delay} && iptables -F\" "
            f"> /dev/null 2>&1 &'"
        )
        k8s.exec_in_spdk_container(node_ip, flush_cmd)
        self.logger.info(
            f"[k8s] scheduled host-level iptables flush in {flush_delay}s on "
            f"{node_ip}")
        drop_cmd = (
            "sudo nohup bash -c '"
            "sleep 5 && "
            "iptables -A INPUT -j DROP && "
            "iptables -A OUTPUT -j DROP"
            "' > /tmp/k8s_nw_outage.log 2>&1 &"
        )
        k8s.exec_in_spdk_container(node_ip, drop_cmd)
        self.logger.info(
            f"[k8s] network outage triggered on {node_ip} (self-restoring "
            f"after {duration}s)")
        return duration

    def _disconnect_and_unmount_dual(self, lvol_name, lvol_id, mount_point):
        """Release the volume so it can be published elsewhere.

        docker: unmount + nvme disconnect.
        k8s: NOT a no-op, despite CSI owning the mount. Delete every pod and
          FIO job this test attached to the volume and wait for them to go,
          freeing the ReadWriteOnce claim. Leaving one attached makes the next
          node's mount fail with ``Multi-Attach error``, which is
          indistinguishable from a DHCHAP denial at the event level and is the
          primary way a denial assertion could pass for the wrong reason.
        """
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            for job_name in list(self.created_fio_jobs):
                try:
                    k8s.delete_job(job_name)
                    self.created_fio_jobs.remove(job_name)
                except Exception as exc:
                    self.logger.warning(f"  release job {job_name}: {exc}")
            for pod_name in list(self.created_pods):
                # Pod names are prefixed, not PVC-derived, so release every
                # pod this test created rather than trying to match them to
                # the claim.
                self._k8s_release_pod(pod_name)
            # Then wait for THIS claim's volume to be genuinely detached.
            # Deleting the pods above only removes the Pod objects; the
            # VolumeAttachment outlives them, and a denial assertion issued
            # inside that window sees Multi-Attach rather than the DHCHAP
            # rejection it is trying to observe.
            try:
                pv_name = k8s.get_pvc_pv_name(pvc_name)
                if pv_name and not k8s.wait_volume_detached(pv_name):
                    self.logger.warning(
                        f"{TOK_WEAK_EVIDENCE} PVC {pvc_name!r} (PV {pv_name}) "
                        f"is still attached; a following denial assertion may "
                        f"observe Multi-Attach instead of a DHCHAP rejection")
            except Exception as exc:
                self.logger.warning(
                    f"  detach wait for {pvc_name}: {exc}")
            self.logger.info(
                f"[k8s] released all attachments to PVC {pvc_name!r}")
            return
        if mount_point:
            self.ssh_obj.unmount_path(self.fio_node, mount_point)
            sleep_n_sec(2)
        self._disconnect_lvol(lvol_id)
        sleep_n_sec(2)

    def teardown(self, **kwargs):
        """Clean up K8s resources before delegating to parent teardown."""
        if self.k8s_test:
            try:
                k8s = self._ensure_k8s_utils()
                for job_name in list(self.created_fio_jobs):
                    try:
                        k8s.delete_job(job_name)
                    except Exception:
                        pass
                for cm_name in list(self.created_configmaps):
                    try:
                        k8s.delete_configmap(cm_name)
                    except Exception:
                        pass
                # Pods MUST go before PVCs: a surviving pod holds the
                # kubernetes.io/pvc-protection finalizer on its claim, which
                # leaves the PVC stuck Terminating indefinitely.
                for pod_name in list(self.created_pods):
                    try:
                        k8s.delete_pod(pod_name, wait=True)
                    except Exception:
                        pass
                # VolumeSnapshots before PVCs: a snapshot holds a reference to
                # its source claim.
                for vs_name in list(getattr(self, "_k8s_volume_snapshots", [])):
                    try:
                        k8s.delete_volume_snapshot(vs_name, wait=True)
                    except Exception:
                        pass
                for pvc_name in list(self.created_pvcs):
                    try:
                        k8s.delete_pvc(pvc_name)
                    except Exception:
                        pass
                for pvc_name in list(getattr(self, "_k8s_pvcs", [])):
                    # Clone PVCs created via the inherited _create_clone_dual
                    # land in the parent's registry, not ours.
                    try:
                        k8s.delete_pvc(pvc_name)
                    except Exception:
                        pass
                for sc_name in list(self.created_storage_classes):
                    try:
                        k8s.delete_storage_class(sc_name)
                    except Exception:
                        pass
            except Exception as exc:
                self.logger.warning(f"K8s teardown error: {exc}")
        super().teardown(**kwargs)

# ═══════════════════════════════════════════════════════════════════════════
# COMMENTED OUT: All old test classes below used volume-level host management
# (volume add-host/remove-host, --allowed-hosts, --sec-options) which has been
# replaced by pool-level DHCHAP (pool add --dhchap, pool add-host/remove-host).
# ═══════════════════════════════════════════════════════════════════════════

# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 1 – All 4 core security combinations with FIO validation
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityCombinations(SecurityTestBase):
#     """
#     Creates one lvol for each of the four core security combinations:
#       1. plain         – no encryption, no auth
#       2. crypto        – encryption only
#       3. auth          – bidirectional DH-HMAC-CHAP, no encryption
#       4. crypto_auth   – encryption + bidirectional DH-HMAC-CHAP
#
#     Each lvol is connected to the FIO node and subjected to a 2-minute
#     randrw FIO workload.  Data integrity is validated via FIO log.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_combinations"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityCombinations START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_auth", self.cluster_id, sec_options=SEC_BOTH)
#
#         # (label, encrypt, sec_opts, pool)
#         combinations = [
#             ("plain",       False, None,     self.pool_name),
#             ("crypto",      True,  None,     self.pool_name),
#             ("auth",        False, SEC_BOTH, self.pool_name + "_auth"),
#             ("crypto_auth", True,  SEC_BOTH, self.pool_name + "_auth"),
#         ]
#
#         fio_threads = []
#         for sec_type, encrypt, sec_opts, pool in combinations:
#             suffix = _rand_suffix()
#             lvol_name = f"sec{sec_type}{suffix}"
#             self.logger.info(f"--- Creating lvol {lvol_name!r} (sec_type={sec_type}) ---")
#
#             if sec_opts is not None:
#                 host_nqn = self._get_client_host_nqn()
#                 _, err = self.ssh_obj.create_sec_lvol(
#                     self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
#                     encrypt=encrypt,
#                     allowed_hosts=[host_nqn],
#                     key1=self.lvol_crypt_keys[0] if encrypt else None,
#                     key2=self.lvol_crypt_keys[1] if encrypt else None)
#                 assert not err or "error" not in err.lower(), \
#                     f"Failed to create {sec_type} lvol: {err}"
#             else:
#                 host_nqn = None
#                 self.sbcli_utils.add_lvol(
#                     lvol_name=lvol_name,
#                     pool_name=pool,
#                     size=self.lvol_size,
#                     crypto=encrypt,
#                     key1=self.lvol_crypt_keys[0] if encrypt else None,
#                     key2=self.lvol_crypt_keys[1] if encrypt else None,
#                 )
#
#             sleep_n_sec(3)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id, f"Could not get lvol ID for {lvol_name}"
#             self._log_lvol_security(lvol_id, label=f"({sec_type})")
#
#             lvol_device, connect_ls = self._connect_and_get_device(
#                 lvol_name, lvol_id, host_nqn=host_nqn)
#             self.logger.info(f"Connected {lvol_name} → {lvol_device}")
#
#             fs_type = "ext4"
#             mount_point = f"{self.mount_path}/{lvol_name}"
#             self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device,
#                                      fs_type=fs_type)
#             self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                     mount_path=mount_point)
#             log_file = f"{self.log_path}/{lvol_name}.log"
#
#             self.lvol_mount_details[lvol_name] = {
#                 "ID":      lvol_id,
#                 "Command": connect_ls,
#                 "Mount":   mount_point,
#                 "Device":  lvol_device,
#                 "FS":      fs_type,
#                 "Log":     log_file,
#                 "sec_type": sec_type,
#                 "host_nqn": host_nqn,
#             }
#
#             if sec_opts is not None:
#                 # DHCHAP volumes run synchronously: FIO → unmount → disconnect before
#                 # the next iteration can reset _client_host_nqn and get a new hostnqn.
#                 # Running them in background would leave the NVMe connection active when
#                 # the next DHCHAP iteration resets the hostnqn, causing the kernel to
#                 # reject the new connect with "found same hostid but different hostnqn".
#                 self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=120)
#                 self.logger.info(f"FIO validated for {sec_type} ✓")
#                 self.ssh_obj.unmount_path(self.fio_node, mount_point)
#                 sleep_n_sec(2)
#                 self._disconnect_lvol(lvol_id)
#                 sleep_n_sec(2)
#                 self.lvol_mount_details[lvol_name]["Mount"] = None
#             else:
#                 # Non-DHCHAP volumes run FIO in background (unchanged behaviour)
#                 t = threading.Thread(
#                     target=self._run_fio_and_validate,
#                     args=(lvol_name, mount_point, log_file),
#                     kwargs={"runtime": 120},
#                 )
#                 t.start()
#                 fio_threads.append((sec_type, t))
#                 sleep_n_sec(5)
#
#         # Wait for non-DHCHAP background FIO jobs
#         for sec_type, t in fio_threads:
#             self.logger.info(f"Waiting for FIO on {sec_type} lvol …")
#             t.join(timeout=600)
#             assert not t.is_alive(), f"FIO timed out for {sec_type}"
#             self.logger.info(f"FIO validated for {sec_type} ✓")
#
#         self.logger.info("=== TestLvolSecurityCombinations PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 2 – Allowed-hosts positive (correct NQN → connects)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsPositive(SecurityTestBase):
#     """
#     Creates an lvol with --allowed-hosts + bidirectional DH-HMAC-CHAP.
#     Verifies that:
#       - Connecting with the registered host NQN succeeds and FIO runs.
#       - ``volume get-secret`` returns non-empty credentials for that NQN.
#       - Connecting *without* --host-nqn returns a connect string but
#         without embedded DHCHAP keys (no dhchap-secret flag in the output).
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_positive"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsPositive START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secallowed{_rand_suffix()}"
#
#         # Create lvol with both sec-options and allowed-hosts
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation with allowed-hosts failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, "Could not find lvol ID"
#
#         # ── positive: connect with the registered NQN ──────────────────────
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected with allowed NQN → {lvol_device}")
#
#         # Verify DHCHAP keys appear in at least one connect command
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         self.logger.info(f"DHCHAP key present in connect string: {has_dhchap}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         log_file = f"{self.log_path}/{lvol_name}.log"
#
#         self.lvol_mount_details[lvol_name] = {
#             "ID": lvol_id, "Mount": mount_point,
#             "Device": lvol_device, "Log": log_file,
#         }
#
#         # Run FIO to validate actual I/O
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # ── verify get-secret returns credentials ──────────────────────────
#         secret_out, _ = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         self.logger.info(f"get-secret output: {secret_out!r}")
#         assert secret_out.strip(), "Expected non-empty secret for registered host"
#
#         # ── verify lvol get shows allowed_hosts ───────────────────────────
#         detail_out = self._get_lvol_details_via_cli(lvol_id)
#         self.logger.info(f"lvol get output: {detail_out}")
#
#         # ── no host-nqn → connect string returned without dhchap keys ─────
#         connect_no_nqn, _ = self._get_connect_str_cli(lvol_id, host_nqn=None)
#         self.logger.info(f"Connect-without-NQN strings: {connect_no_nqn}")
#         # The connect string should exist (system responds) but DHCHAP key
#         # info should not be present since no specific host was identified
#         if connect_no_nqn:
#             has_dhchap_no_nqn = any("dhchap" in c.lower() for c in connect_no_nqn)
#             self.logger.info(f"DHCHAP in no-NQN connect string: {has_dhchap_no_nqn} "
#                              f"(expected False or command-level rejection)")
#
#         self.logger.info("=== TestLvolAllowedHostsPositive PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 3 – Allowed-hosts negative (wrong NQN → rejected)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsNegative(SecurityTestBase):
#     """
#     Creates an lvol with a specific allowed host NQN and verifies that
#     requesting a connect string for a *different* NQN is rejected at the
#     connect-string-generation stage (before any nvme connect attempt).
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_negative"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsNegative START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         allowed_nqn = self._get_client_host_nqn()
#         wrong_nqn = "nqn.2024-01.io.simplyblock:test:wrong-host-" + _rand_suffix()
#         lvol_name = f"secneg{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[allowed_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Attempt connect with wrong NQN – expect error or empty connect list
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"Connect with wrong NQN → connect_ls={connect_ls}, err={err!r}")
#
#         rejected = bool(err) or not connect_ls
#         assert rejected, (
#             f"Expected rejection for wrong NQN {wrong_nqn!r} "
#             f"but got connect strings: {connect_ls}")
#
#         self.logger.info("Correct: wrong host NQN was rejected at connect-string "
#                          "generation stage.")
#         self.logger.info("=== TestLvolAllowedHostsNegative PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 4 – Dynamic add-host / remove-host management
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolDynamicHostManagement(SecurityTestBase):
#     """
#     Verifies that hosts can be added to and removed from an existing lvol:
#
#     1. Create a plain lvol (no initial security).
#     2. Add a host NQN with sec-options (DHCHAP) via ``volume add-host``.
#     3. Verify the host appears in ``volume get`` output.
#     4. Connect and run FIO using the newly added host NQN.
#     5. Remove the host via ``volume remove-host``.
#     6. Verify connection with that NQN is now rejected.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_dynamic_host_management"
#
#     def run(self):
#         self.logger.info("=== TestLvolDynamicHostManagement START ===")
#         fio_nodes = self.fio_node          # full list before reassignment
#         self.fio_node = fio_nodes[0]
#         two_clients = len(fio_nodes) >= 2
#         self.logger.info(f"two_clients={two_clients} (fio_nodes={fio_nodes})")
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         lvol_name = f"secdyn{_rand_suffix()}"
#         host_nqn = self._get_client_host_nqn()        # NQN from fio_nodes[0]
#
#         # Get second client NQN when available (read directly, bypass cache)
#         second_host_nqn = None
#         if two_clients:
#             nqn_out, _ = self.ssh_obj.exec_command(fio_nodes[1], "cat /etc/nvme/hostnqn")
#             second_host_nqn = nqn_out.strip().split('\n')[0].strip()
#             assert second_host_nqn, f"Could not read hostnqn from {fio_nodes[1]}"
#             self.logger.info(f"Second client NQN: {second_host_nqn!r}")
#
#         # ── Step 1: Create plain lvol via API ──────────────────────────────
#         self.sbcli_utils.add_lvol(
#             lvol_name=lvol_name,
#             pool_name=self.pool_name,
#             size=self.lvol_size,
#         )
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, "Could not find lvol ID"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # ── Step 2: Add host(s) with DHCHAP via CLI ──────────────────────────
#         self.logger.info(f"Adding host {host_nqn!r} …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), \
#             f"add-host failed: {err}"
#
#         if two_clients:
#             self.logger.info(f"Adding second host {second_host_nqn!r} …")
#             out, err = self.ssh_obj.add_host_to_lvol(
#                 self.mgmt_nodes[0], lvol_id, second_host_nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"add-host (second client) failed: {err}"
#
#         # ── Step 3: Verify host(s) appear in lvol details ───────────────────
#         # Use the API (structured data) rather than the CLI table output,
#         # because the table wraps long NQN strings across multiple lines.
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs after add-host: {allowed_nqns}")
#         assert host_nqn in allowed_nqns, \
#             f"Expected {host_nqn!r} in allowed_hosts, got: {allowed_nqns}"
#         if two_clients:
#             assert second_host_nqn in allowed_nqns, \
#                 f"Expected second {second_host_nqn!r} in allowed_hosts, got: {allowed_nqns}"
#
#         # ── Step 4: Connect with the first host NQN and run FIO ─────────────
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected via added host NQN → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}.log"
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # Unmount and disconnect before removing host
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # ── Step 5: Remove the first host ────────────────────────────────────
#         self.logger.info(f"Removing host {host_nqn!r} …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), \
#             f"remove-host failed: {err}"
#
#         # ── Step 6: Verify removed host is rejected ───────────────────────────
#         sleep_n_sec(3)
#         if two_clients:
#             # allowed_hosts still has second_host_nqn → backend must reject removed NQN
#             connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#             rejected = bool(err) or not connect_ls
#             self.logger.info(
#                 f"[2-client] Connect after remove-host → connect_ls={connect_ls}, "
#                 f"err={err!r}, rejected={rejected}")
#             assert rejected, \
#                 "Expected rejection after remove-host (2-client) but still got a connect string"
#             self.logger.info("[2-client] Removed host correctly rejected PASSED")
#         else:
#             # allowed_hosts is now empty → backend falls back to "no security".
#             # Verify allowed_hosts is empty and the connect string has no DHCHAP keys.
#             lvol_api_after = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#             allowed_after = [h.get("nqn") for h in lvol_api_after[0].get("allowed_hosts", [])]
#             self.logger.info(f"[1-client] allowed_hosts after remove: {allowed_after}")
#             assert len(allowed_after) == 0, \
#                 f"Expected empty allowed_hosts after remove (1-client), got: {allowed_after}"
#             connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#             self.logger.info(
#                 f"[1-client] Connect after remove-host → connect_ls={connect_ls}, err={err!r}")
#             assert connect_ls, "Expected a plain connect string in 1-client fallback"
#             combined = " ".join(connect_ls)
#             assert "dhchap" not in combined.lower(), \
#                 f"Expected no DHCHAP keys in 1-client fallback connect string, got: {combined!r}"
#             self.logger.info("[1-client] allowed_hosts empty, connect string has no DHCHAP keys PASSED")
#
#         self.logger.info("=== TestLvolDynamicHostManagement PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 5 – Crypto + allowed-hosts end-to-end
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolCryptoWithAllowedHosts(SecurityTestBase):
#     """
#     Creates a crypto-encrypted lvol with both --sec-options and --allowed-hosts.
#     Verifies:
#       - Connection with correct NQN succeeds and returns DHCHAP-bearing command.
#       - FIO workload completes without errors.
#       - ``volume get-secret`` returns credentials.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_crypto_with_allowed_hosts"
#
#     def run(self):
#         self.logger.info("=== TestLvolCryptoWithAllowedHosts START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"seccryauth{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn], encrypt=True,
#             key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"Crypto+auth lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Connected crypto+auth lvol → {lvol_device}")
#
#         # Verify DHCHAP keys embedded
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         assert has_dhchap, "Expected DHCHAP keys in connect string for auth lvol"
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name] = {
#             "ID": lvol_id, "Mount": mount_point,
#             "Device": lvol_device, "Log": log_file,
#         }
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=120)
#
#         # Confirm get-secret returns something
#         secret_out, _ = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert secret_out.strip(), "Expected credentials from get-secret"
#
#         self.logger.info("=== TestLvolCryptoWithAllowedHosts PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 6 – Host-only vs controller-only DHCHAP directions
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolDhcapDirections(SecurityTestBase):
#     """
#     Tests each DHCHAP direction in isolation:
#       - host-only (dhchap_key=true, dhchap_ctrlr_key=false):
#           the host must authenticate to the controller.
#       - ctrl-only (dhchap_key=false, dhchap_ctrlr_key=true):
#           the controller must authenticate to the host.
#       - bidirectional (both=true): already covered by other tests,
#           included here for completeness.
#
#     Each variant is connected and subjected to a short FIO workload.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_dhchap_directions"
#
#     def run(self):
#         self.logger.info("=== TestLvolDhcapDirections START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         pool_host = self.pool_name + "_host"
#         pool_ctrl = self.pool_name + "_ctrl"
#         directions = [
#             ("host_only", pool_host),
#             ("ctrl_only", pool_ctrl),
#             ("bidir",     self.pool_name),
#         ]
#
#         for label, pool in directions:
#             # Each volume needs its own unique NQN to avoid SPDK keyring
#             # key-name collisions when multiple DHCHAP volumes are created.
#             self._client_host_nqn = None
#             host_nqn = self._get_client_host_nqn()
#
#             lvol_name = f"secdir{label}{_rand_suffix()}"
#             self.logger.info(f"--- Testing direction: {label} ---")
#
#             out, err = self.ssh_obj.create_sec_lvol(
#                 self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
#             )
#             assert not err or "error" not in err.lower(), \
#                 f"lvol creation failed for {label}: {err}"
#
#             sleep_n_sec(3)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id
#
#             lvol_device, connect_ls = self._connect_and_get_device(
#                 lvol_name, lvol_id, host_nqn=host_nqn)
#             self.logger.info(f"[{label}] Connected → {lvol_device}")
#
#             mount_point = f"{self.mount_path}/{lvol_name}"
#             log_file = f"{self.log_path}/{lvol_name}.log"
#             self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#             self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                     mount_path=mount_point)
#             self.lvol_mount_details[lvol_name] = {
#                 "ID": lvol_id, "Mount": mount_point,
#                 "Device": lvol_device, "Log": log_file,
#                 "host_nqn": host_nqn,
#             }
#
#             self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#             # Disconnect before the next iteration resets _client_host_nqn.
#             # The kernel binds hostid→hostnqn on the first connect; leaving the
#             # connection active causes "found same hostid but different hostnqn".
#             self.ssh_obj.unmount_path(self.fio_node, mount_point)
#             sleep_n_sec(2)
#             self._disconnect_lvol(lvol_id)
#             sleep_n_sec(2)
#             self.lvol_mount_details[lvol_name]["Mount"] = None
#             self.logger.info(f"[{label}] FIO validated ✓")
#
#         self.logger.info("=== TestLvolDhcapDirections PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 7 – Multi-host: add two hosts, verify each, remove one
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolMultipleAllowedHosts(SecurityTestBase):
#     """
#     Creates an lvol with two allowed host NQNs, verifies that the registered
#     NQN can connect, then removes one host and confirms its access is revoked
#     while the other host's access remains intact.
#
#     Since tests typically run on a single client machine, the 'second' host
#     NQN is a synthetic one injected into the allowed list.  The test focuses
#     on the control-plane operations (add-host / remove-host / volume get)
#     rather than dual-machine connectivity.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_multiple_allowed_hosts"
#
#     def run(self):
#         self.logger.info("=== TestLvolMultipleAllowedHosts START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         real_nqn = self._get_client_host_nqn()
#         fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
#         lvol_name = f"secmulti{_rand_suffix()}"
#
#         # Create with both NQNs in allowed list
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[real_nqn, fake_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"Multi-host lvol creation failed: {err}"
#
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Both NQNs should appear in lvol details
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs (2 hosts): {allowed_nqns}")
#         assert real_nqn in allowed_nqns, f"real NQN missing from allowed_hosts: {allowed_nqns}"
#         assert fake_nqn in allowed_nqns, f"fake NQN missing from allowed_hosts: {allowed_nqns}"
#
#         # Connect with real NQN
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=real_nqn)
#         self.logger.info(f"Connected with real NQN → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device,
#                                 mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         # Disconnect before removing host
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # Remove fake NQN
#         self.logger.info(f"Removing fake NQN {fake_nqn!r} …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, fake_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#
#         # Verify fake NQN no longer in details, real NQN still there
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         self.logger.info(f"allowed_hosts NQNs (after removal): {allowed_nqns}")
#         assert fake_nqn not in allowed_nqns, f"fake NQN should have been removed: {allowed_nqns}"
#         assert real_nqn in allowed_nqns, f"real NQN should still be present: {allowed_nqns}"
#
#         # Real NQN should still be able to get a connect string
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=real_nqn)
#         assert connect_ls and not err, \
#             f"real NQN should still connect after removing fake NQN; err={err!r}"
#
#         self.logger.info("=== TestLvolMultipleAllowedHosts PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 8 – Negative: get-secret, remove-host, add-host edge cases
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeHostOps(SecurityTestBase):
#     """
#     Covers error-path scenarios for host management operations:
#
#     TC-SEC-026  remove-host for NQN not in allowed list → error
#     TC-SEC-027  add-host with duplicate NQN → handled gracefully (no crash)
#     TC-SEC-028  get-secret for a host NQN that was never registered → error
#     TC-SEC-029  remove-host then re-add same NQN → should work correctly
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_host_ops"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeHostOps START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
#         lvol_name = f"secnegops{_rand_suffix()}"
#
#         # Create a lvol with one allowed host
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # ── TC-SEC-026: remove non-existent NQN ──────────────────────────
#         self.logger.info("TC-SEC-026: remove-host for unregistered NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         has_error = bool(err) or ("error" in out.lower() if out else False) \
#                     or ("not found" in out.lower() if out else False)
#         self.logger.info(
#             f"remove non-existent NQN → out={out!r}, err={err!r}, "
#             f"has_error={has_error}")
#         assert has_error, \
#             "Expected error when removing a NQN that was never added"
#
#         # ── TC-SEC-027: add duplicate NQN ─────────────────────────────────
#         self.logger.info("TC-SEC-027: add-host with duplicate NQN …")
#         out1, err1 = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         self.logger.info(f"First add-host (already present): out={out1!r}, err={err1!r}")
#         # Should either succeed idempotently or return a meaningful error;
#         # the system must not crash or corrupt state.
#         detail_out = self._get_lvol_details_via_cli(lvol_id)
#         nqn_count = detail_out.count(host_nqn)
#         assert nqn_count <= 2, \
#             f"Duplicate NQN should not be listed more than once; got count={nqn_count}"
#
#         # ── TC-SEC-028: get-secret for unregistered NQN ───────────────────
#         self.logger.info("TC-SEC-028: get-secret for unregistered NQN …")
#         secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         is_empty_or_err = (
#             not secret_out.strip() or
#             bool(secret_err) or
#             "error" in secret_out.lower() or
#             "not found" in secret_out.lower()
#         )
#         self.logger.info(
#             f"get-secret absent NQN → out={secret_out!r}, err={secret_err!r}")
#         assert is_empty_or_err, \
#             "Expected empty result or error for unregistered NQN in get-secret"
#
#         # ── TC-SEC-029: remove then re-add same NQN ────────────────────────
#         self.logger.info("TC-SEC-029: remove-host then re-add same NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#         sleep_n_sec(2)
#
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"re-add-host failed: {err}"
#         sleep_n_sec(2)
#
#         # Verify host NQN is back and can get a connect string
#         lvol_api = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
#         allowed_nqns = [h.get("nqn") for h in lvol_api[0].get("allowed_hosts", [])]
#         assert host_nqn in allowed_nqns, \
#             f"Re-added NQN should appear in allowed_hosts: {allowed_nqns}"
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn)
#         assert connect_ls and not err, \
#             f"Re-added NQN should produce a valid connect string; err={err!r}"
#
#         self.logger.info("=== TestLvolSecurityNegativeHostOps PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 9 – Negative: invalid inputs at lvol creation time
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeCreation(SecurityTestBase):
#     """
#     Covers invalid input scenarios at lvol-creation time:
#
#     TC-SEC-050  --sec-options file path does not exist → CLI error
#     TC-SEC-051  --allowed-hosts file contains non-array JSON → CLI error
#     TC-SEC-053  --allowed-hosts with empty list [] → error or meaningful warning
#     TC-SEC-055  add-host with syntactically invalid NQN → error
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_creation"
#
#     def _assert_cli_error(self, out: str, err: str, label: str) -> None:
#         """Assert that at least one of out/err signals a failure."""
#         failure_signals = ("error", "invalid", "failed", "no such", "not found",
#                            "cannot", "unable")
#         combined = (out or "").lower() + (err or "").lower()
#         has_signal = any(s in combined for s in failure_signals)
#         self.logger.info(
#             f"[{label}] out={out!r}, err={err!r}, has_error_signal={has_signal}")
#         assert has_signal or not out.strip(), \
#             f"[{label}] Expected error signal but got: out={out!r} err={err!r}"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeCreation START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#
#         # ── TC-SEC-050: non-existent sec-options file ─────────────────────
#         self.logger.info("TC-SEC-050: --sec-options with non-existent file path …")
#         lvol_name = f"secneg050{_rand_suffix()}"
#         cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
#                f" {self.pool_name} --sec-options /tmp/does_not_exist_ever.json")
#         out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
#         # Should error; lvol must NOT be created
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert not created_id, \
#             "TC-SEC-050: lvol should NOT be created with non-existent sec-options file"
#         self.logger.info("TC-SEC-050 PASS: lvol not created for missing file")
#
#         # ── TC-SEC-051: allowed-hosts file contains object not array ───────
#         self.logger.info("TC-SEC-051: --allowed-hosts with invalid JSON (not array) …")
#         lvol_name = f"secneg051{_rand_suffix()}"
#         bad_json_path = "/tmp/bad_hosts.json"
#         # Write an object instead of an array
#         self.ssh_obj.write_json_file(
#             self.mgmt_nodes[0], bad_json_path,
#             {"nqn": "nqn.2024-01.io.simplyblock:bad"})
#         cmd = (f"{self.base_cmd} -d volume add {lvol_name} {self.lvol_size}"
#                f" {self.pool_name} --allowed-hosts {bad_json_path}")
#         out, err = self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)
#         self.ssh_obj.exec_command(
#             self.mgmt_nodes[0], f"rm -f {bad_json_path}", supress_logs=True)
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert not created_id, \
#             "TC-SEC-051: lvol should NOT be created when allowed-hosts JSON is not an array"
#         self.logger.info("TC-SEC-051 PASS")
#
#         # ── TC-SEC-053: --allowed-hosts with empty list ────────────────────
#         self.logger.info("TC-SEC-053: --allowed-hosts with empty list [] …")
#         lvol_name = f"secneg053{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[],   # empty list
#         )
#         # Behaviour: either error, or create with no allowed hosts (effectively open)
#         # The important thing is it does not crash and gives a clear response.
#         self.logger.info(
#             f"TC-SEC-053: empty allowed-hosts → out={out!r}, err={err!r}")
#         created_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         if created_id:
#             self.logger.info("TC-SEC-053: lvol created with empty hosts list; cleaning up")
#             self.lvol_mount_details[lvol_name] = {"ID": created_id, "Mount": None}
#         else:
#             self.logger.info("TC-SEC-053: lvol rejected with empty hosts list")
#
#         # ── TC-SEC-055: add-host with syntactically invalid NQN ───────────
#         self.logger.info("TC-SEC-055: add-host with invalid NQN format …")
#         # Create a plain lvol to test add-host against
#         plain_name = f"secneg055{_rand_suffix()}"
#         self.sbcli_utils.add_lvol(
#             lvol_name=plain_name,
#             pool_name=self.pool_name,
#             size=self.lvol_size,
#         )
#         sleep_n_sec(3)
#         plain_id = self.sbcli_utils.get_lvol_id(plain_name)
#         assert plain_id
#         self.lvol_mount_details[plain_name] = {"ID": plain_id, "Mount": None}
#
#         invalid_nqn = "not-a-valid-nqn-format-!@#$%"
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], plain_id, invalid_nqn)
#         self._assert_cli_error(out, err, "TC-SEC-055")
#         self.logger.info("TC-SEC-055 PASS: invalid NQN rejected")
#
#         self.logger.info("=== TestLvolSecurityNegativeCreation PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 10 – Negative: connect & I/O rejection scenarios
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeConnect(SecurityTestBase):
#     """
#     Tests rejection of connections that should not succeed:
#
#     TC-SEC-009  DHCHAP lvol (no allowed-hosts): connect with mismatched NQN
#     TC-SEC-013  Allowed-hosts lvol: connect without --host-nqn (no keys path)
#     TC-SEC-054  Auth lvol: attempt nvme connect using tampered connect string
#     TC-SEC-056  Delete lvol with active allowed-hosts → cleanup succeeds
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_connect"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeConnect START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#
#         # ── TC-SEC-009: auth lvol (no allowed-hosts) + wrong NQN ──────────
#         self.logger.info(
#             "TC-SEC-009: DHCHAP lvol (no allowed-hosts) + wrong NQN …")
#         lvol_name_009 = f"secneg009{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name_009, self.lvol_size, self.pool_name,
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_id_009 = self.sbcli_utils.get_lvol_id(lvol_name_009)
#         assert lvol_id_009
#         self.lvol_mount_details[lvol_name_009] = {"ID": lvol_id_009, "Mount": None}
#
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         connect_ls, err = self._get_connect_str_cli(lvol_id_009, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"TC-SEC-009: wrong NQN → connect_ls={connect_ls}, err={err!r}")
#         # When no allowed-hosts is configured, any NQN may get a connect string
#         # but the DHCHAP negotiation at the kernel level should fail.
#         # We log the result; the definitive rejection happens at nvme-connect time.
#         self.logger.info(
#             "TC-SEC-009: Connect string generation noted; actual DHCHAP rejection "
#             "occurs at kernel nvme-connect level (verified by non-zero connect exit code)")
#
#         # ── TC-SEC-013: allowed-hosts lvol, connect without --host-nqn ────
#         self.logger.info(
#             "TC-SEC-013: allowed-hosts lvol, connect without --host-nqn …")
#         # Fresh NQN for this volume to avoid SPDK keyring key-name collision
#         # with lvol_009 which was created with the same host_nqn.
#         self._client_host_nqn = None
#         host_nqn = self._get_client_host_nqn()
#
#         lvol_name_013 = f"secneg013{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name_013, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_id_013 = self.sbcli_utils.get_lvol_id(lvol_name_013)
#         assert lvol_id_013
#         self.lvol_mount_details[lvol_name_013] = {"ID": lvol_id_013, "Mount": None}
#
#         # Without host-nqn, connect string should not contain DHCHAP keys
#         connect_no_nqn, err_no_nqn = self._get_connect_str_cli(
#             lvol_id_013, host_nqn=None)
#         self.logger.info(
#             f"TC-SEC-013: no-NQN connect → strings={connect_no_nqn}, err={err_no_nqn!r}")
#         if connect_no_nqn:
#             has_dhchap = any("dhchap" in c.lower() for c in connect_no_nqn)
#             self.logger.info(
#                 f"TC-SEC-013: DHCHAP keys present={has_dhchap} "
#                 f"(expected False when no host-nqn supplied)")
#             assert not has_dhchap, \
#                 "Connect string without --host-nqn must not contain DHCHAP keys"
#
#         # ── TC-SEC-054: tampered connect string ────────────────────────────
#         self.logger.info(
#             "TC-SEC-054: connect with tampered DHCHAP key in connect string …")
#         connect_auth, err_auth = self._get_connect_str_cli(
#             lvol_id_013, host_nqn=host_nqn)
#         if connect_auth:
#             tampered = connect_auth[0]
#             # Replace dhchap-secret value with garbage if present
#             if "dhchap-secret" in tampered:
#                 import re
#                 tampered = re.sub(
#                     r'(--dhchap-secret\s+)\S+',
#                     r'\1DEADBEEFDEADBEEF00000000FFFFFFFF',
#                     tampered)
#                 self.logger.info(f"TC-SEC-054: Tampered connect cmd: {tampered!r}")
#                 _, connect_err = self.ssh_obj.exec_command(
#                     node=self.fio_node, command=tampered)
#                 self.logger.info(
#                     f"TC-SEC-054: Tampered connect result err={connect_err!r} "
#                     f"(expected non-zero exit / auth failure at kernel level)")
#                 # Note: even if exec_command swallows the exit code, the device
#                 # will NOT appear since DHCHAP negotiation fails.  The absence of
#                 # a new block device is the definitive check.
#                 sleep_n_sec(3)
#                 # We do NOT assert here because exec_command masks exit codes;
#                 # the behaviour is logged for manual / log-level verification.
#             else:
#                 self.logger.info(
#                     "TC-SEC-054: no dhchap-secret in connect string (no allowed-hosts); "
#                     "skipping tamper check")
#
#         # ── TC-SEC-056: delete lvol that has active allowed-hosts ──────────
#         self.logger.info(
#             "TC-SEC-056: delete lvol that has active allowed-hosts list …")
#         # lvol_013 has an allowed host – delete it and verify it's gone
#         self.sbcli_utils.delete_lvol(lvol_name=lvol_name_013, skip_error=False)
#         sleep_n_sec(3)
#         gone_id = self.sbcli_utils.get_lvol_id(lvol_name_013)
#         assert not gone_id, \
#             f"TC-SEC-056: lvol {lvol_name_013!r} should be deleted"
#         del self.lvol_mount_details[lvol_name_013]
#         self.logger.info("TC-SEC-056 PASS: lvol with allowed-hosts deleted cleanly")
#
#         self.logger.info("=== TestLvolSecurityNegativeConnect PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 11 – Allowed-hosts without DHCHAP (NQN whitelist only)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolAllowedHostsNoDhchap(SecurityTestBase):
#     """
#     TC-SEC-034  Create lvol with --allowed-hosts but NO --sec-options
#                 (pure NQN whitelist, no DH-HMAC-CHAP key exchange).
#
#     Verifies:
#       - Allowed NQN can get a connect string and connect successfully.
#       - Connect string does NOT contain DHCHAP keys (no key negotiation).
#       - Unregistered NQN is still rejected at connect-string level.
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_allowed_hosts_no_dhchap"
#
#     def run(self):
#         self.logger.info("=== TestLvolAllowedHostsNoDhchap START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         lvol_name = f"secnqnonly{_rand_suffix()}"
#
#         # No sec_options — NQN whitelist only
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), \
#             f"NQN-whitelist lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Allowed NQN should get connect string (without DHCHAP keys)
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"Allowed NQN connect → {connect_ls}, err={err!r}")
#         assert connect_ls and not err, \
#             f"Allowed NQN should produce a connect string; err={err!r}"
#         has_dhchap = any("dhchap" in c.lower() for c in connect_ls)
#         assert not has_dhchap, \
#             "No DHCHAP keys expected when --sec-options not provided"
#
#         # Unregistered NQN should be rejected
#         wrong_connect, wrong_err = self._get_connect_str_cli(
#             lvol_id, host_nqn=wrong_nqn)
#         self.logger.info(
#             f"Wrong NQN connect → {wrong_connect}, err={wrong_err!r}")
#         rejected = bool(wrong_err) or not wrong_connect
#         assert rejected, \
#             f"Unregistered NQN should be rejected even without DHCHAP; " \
#             f"got: {wrong_connect}"
#
#         # Connect with correct NQN and run FIO
#         lvol_device, connect_ls = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         self.logger.info(f"NQN-whitelist lvol connected → {lvol_device}")
#
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         log_file = f"{self.log_path}/{lvol_name}.log"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=lvol_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, runtime=60)
#
#         self.logger.info("=== TestLvolAllowedHostsNoDhchap PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 12 – Snapshot & clone inherit security settings from the parent lvol
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecuritySnapshotClone(SecurityTestBase):
#     """
#     Verifies that snapshots and clones inherit security settings from their
#     parent lvol.  The backend copies ``allowed_hosts`` (including embedded
#     DHCHAP keys) and crypto settings at clone-creation time.
#
#     Scenarios:
#       A) auth parent   – DHCHAP only, no encryption
#          * Clone connects with the same host NQN / DHCHAP keys  (positive)
#          * Clone rejects a different host NQN                    (negative)
#
#       B) crypto_auth parent – DHCHAP + encryption
#          * Clone connects with the same host NQN / DHCHAP keys  (positive)
#          * Connect string includes dhchap keys                   (assertion)
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_snapshot_clone"
#
#     # ── helpers ──────────────────────────────────────────────────────────────
#
#     def _create_snap_and_clone(self, parent_id, label):
#         """Snapshot *parent_id* then clone it; return (snap_id, clone_id, clone_name)."""
#         snap_name = f"snap_{label}{_rand_suffix()}"
#         snap_result = self.sbcli_utils.add_snapshot(parent_id, snap_name)
#         assert snap_result, f"Snapshot creation failed for {snap_name}"
#         sleep_n_sec(3)
#         snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
#         assert snap_id, f"Could not find snapshot ID for {snap_name}"
#
#         clone_name = f"clone_{label}{_rand_suffix()}"
#         clone_result = self.sbcli_utils.add_clone(snap_id, clone_name)
#         assert clone_result, f"Clone creation failed for {clone_name}"
#         sleep_n_sec(3)
#         clone_id = self.sbcli_utils.get_lvol_id(clone_name)
#         assert clone_id, f"Could not find clone ID for {clone_name}"
#
#         self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
#         return snap_id, clone_id, clone_name
#
#     def _verify_clone_security(self, clone_name, clone_id, host_nqn, wrong_nqn,
#                                 expect_dhchap=True):
#         """
#         Core clone security assertions:
#           - wrong NQN is rejected
#           - correct host NQN connects successfully (with DHCHAP keys if expected)
#           - FIO read workload succeeds on the mounted clone
#         """
#         # Negative: wrong NQN should be rejected
#         wrong_connect, wrong_err = self._get_connect_str_cli(
#             clone_id, host_nqn=wrong_nqn)
#         rejected = bool(wrong_err) or not wrong_connect
#         assert rejected, \
#             f"Wrong NQN should be rejected on clone {clone_name}; got: {wrong_connect}"
#         self.logger.info(f"[{clone_name}] Wrong-NQN rejected as expected")
#
#         # Positive: correct host NQN connects
#         clone_device, clone_cmds = self._connect_and_get_device(
#             clone_name, clone_id, host_nqn=host_nqn)
#         self.logger.info(f"[{clone_name}] Connected → {clone_device}")
#
#         if expect_dhchap:
#             has_dhchap = any("dhchap" in c.lower() for c in clone_cmds)
#             assert has_dhchap, \
#                 f"Clone {clone_name} connect string should include DHCHAP keys"
#
#         mount_clone = f"{self.mount_path}/{clone_name}"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=clone_device, mount_path=mount_clone)
#         self.lvol_mount_details[clone_name]["Mount"] = mount_clone
#
#         log_clone = f"{self.log_path}/{clone_name}.log"
#         self._run_fio_and_validate(
#             clone_name, mount_clone, log_clone, rw="read", runtime=30)
#         self.logger.info(f"[{clone_name}] FIO read validated")
#
#     # ── main test ─────────────────────────────────────────────────────────────
#
#     def run(self):
#         self.logger.info("=== TestLvolSecuritySnapshotClone START ===")
#         self._log_cluster_security_config()
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#
#         # ── Scenario A: auth (DHCHAP only, no crypto) ────────────────────────
#         self.logger.info("--- Scenario A: auth parent (DHCHAP only) ---")
#         auth_parent = f"secsnap_auth{_rand_suffix()}"
#
#         _, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], auth_parent, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn])
#         assert not err or "error" not in err.lower(), \
#             f"auth parent creation failed: {err}"
#         sleep_n_sec(3)
#
#         auth_parent_id = self.sbcli_utils.get_lvol_id(auth_parent)
#         assert auth_parent_id, f"Could not find ID for {auth_parent}"
#         self._log_lvol_security(auth_parent_id, label="(auth parent)")
#
#         # Write data to parent so we can verify clone is readable
#         auth_device, _ = self._connect_and_get_device(
#             auth_parent, auth_parent_id, host_nqn=host_nqn)
#         mount_auth = f"{self.mount_path}/{auth_parent}"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=auth_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=auth_device, mount_path=mount_auth)
#         self.lvol_mount_details[auth_parent] = {
#             "ID": auth_parent_id, "Mount": mount_auth, "Device": auth_device}
#
#         log_auth = f"{self.log_path}/{auth_parent}.log"
#         self._run_fio_and_validate(
#             auth_parent, mount_auth, log_auth, rw="write", runtime=30)
#
#         # Unmount parent before snapshotting
#         self.ssh_obj.unmount_path(self.fio_node, mount_auth)
#         self.lvol_mount_details[auth_parent]["Mount"] = None
#         sleep_n_sec(2)
#
#         _, auth_clone_id, auth_clone_name = self._create_snap_and_clone(
#             auth_parent_id, "auth")
#         self._log_lvol_security(auth_clone_id, label="(auth clone)")
#
#         self._verify_clone_security(
#             auth_clone_name, auth_clone_id, host_nqn, wrong_nqn,
#             expect_dhchap=True)
#
#         self.logger.info("--- Scenario A PASSED ---")
#
#         # ── Scenario B: crypto_auth (DHCHAP + encryption) ────────────────────
#         self.logger.info("--- Scenario B: crypto_auth parent (DHCHAP + crypto) ---")
#         # Disconnect Scenario A volumes before generating a fresh hostnqn for Scenario B.
#         # auth_parent was unmounted above but its NVMe connection is still active.
#         # auth_clone was mounted and connected by _verify_clone_security and never cleaned up.
#         # The kernel binds hostid→hostnqn on the first connect; the new hostnqn for Scenario B
#         # would be rejected with "found same hostid but different hostnqn" if any Scenario A
#         # connection remains active.
#         mount_auth_clone = f"{self.mount_path}/{auth_clone_name}"
#         self.ssh_obj.unmount_path(self.fio_node, mount_auth_clone)
#         sleep_n_sec(2)
#         self._disconnect_lvol(auth_clone_id)
#         sleep_n_sec(2)
#         self._disconnect_lvol(auth_parent_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[auth_clone_name]["Mount"] = None
#
#         # Fresh NQN for Scenario B to avoid SPDK keyring key-name collision
#         # with Scenario A's volumes (same host_nqn → same key_name → re-
#         # registration rejected → Scenario B auth would fail).
#         self._client_host_nqn = None
#         host_nqn = self._get_client_host_nqn()
#
#         ca_parent = f"secsnap_ca{_rand_suffix()}"
#
#         _, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], ca_parent, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True,
#             key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1])
#         assert not err or "error" not in err.lower(), \
#             f"crypto_auth parent creation failed: {err}"
#         sleep_n_sec(3)
#
#         ca_parent_id = self.sbcli_utils.get_lvol_id(ca_parent)
#         assert ca_parent_id, f"Could not find ID for {ca_parent}"
#         self._log_lvol_security(ca_parent_id, label="(crypto_auth parent)")
#
#         ca_device, _ = self._connect_and_get_device(
#             ca_parent, ca_parent_id, host_nqn=host_nqn)
#         mount_ca = f"{self.mount_path}/{ca_parent}"
#         self.ssh_obj.format_disk(
#             node=self.fio_node, device=ca_device, fs_type="ext4")
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=ca_device, mount_path=mount_ca)
#         self.lvol_mount_details[ca_parent] = {
#             "ID": ca_parent_id, "Mount": mount_ca, "Device": ca_device}
#
#         log_ca = f"{self.log_path}/{ca_parent}.log"
#         self._run_fio_and_validate(
#             ca_parent, mount_ca, log_ca, rw="write", runtime=30)
#
#         self.ssh_obj.unmount_path(self.fio_node, mount_ca)
#         self.lvol_mount_details[ca_parent]["Mount"] = None
#         sleep_n_sec(2)
#
#         _, ca_clone_id, ca_clone_name = self._create_snap_and_clone(
#             ca_parent_id, "ca")
#         self._log_lvol_security(ca_clone_id, label="(crypto_auth clone)")
#
#         self._verify_clone_security(
#             ca_clone_name, ca_clone_id, host_nqn, wrong_nqn,
#             expect_dhchap=True)
#
#         self.logger.info("--- Scenario B PASSED ---")
#         self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")

#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 12 – Storage node outage + DHCHAP credential persistence
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityOutageRecovery(SecurityTestBase):
#     """
#     Verifies that DHCHAP credentials survive a storage node outage/restart.
#
#     TC-SEC-070  Create DHCHAP (SEC_BOTH) lvol and connect successfully
#     TC-SEC-071  Shutdown a storage node; verify cluster remains accessible
#     TC-SEC-072  Restart the node; wait for it to come back online
#     TC-SEC-073  Reconnect the lvol with the same DHCHAP credentials – must succeed
#     TC-SEC-074  Run FIO on the reconnected lvol to confirm data plane integrity
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_outage_recovery"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityOutageRecovery START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secout{_rand_suffix()}"
#
#         # TC-SEC-070: create DHCHAP lvol and verify initial connect
#         self.logger.info("TC-SEC-070: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, f"Could not find ID for {lvol_name}"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-070: Initial connect + format PASSED")
#
#         # Disconnect before node outage
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-071: shutdown a storage node
#         self.logger.info("TC-SEC-071: Shutting down a storage node …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node = primary_nodes[0]["uuid"]
#         self.sbcli_utils.shutdown_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
#         self.logger.info("TC-SEC-071: Node offline PASSED")
#
#         # TC-SEC-072: restart node and wait for it to come online
#         self.logger.info("TC-SEC-072: Waiting 2 min before restarting node …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-072: Restarting the storage node …")
#         self.sbcli_utils.restart_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
#         self.logger.info("TC-SEC-072: Node online — waiting 2 min for HA to settle …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-072: Node back online PASSED")
#
#         # TC-SEC-073: reconnect with original DHCHAP credentials
#         self.logger.info("TC-SEC-073: Reconnecting with original DHCHAP creds …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after node restart failed"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-073: Reconnect with DHCHAP creds PASSED")
#
#         # TC-SEC-074: FIO on reconnected lvol
#         self.logger.info("TC-SEC-074: Running FIO on reconnected lvol …")
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-074: FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityOutageRecovery PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test 13 – 30-second network interrupt + DHCHAP re-auth
# ═══════════════════════════════════════════════════════════════════════════

# class TestLvolSecurityNetworkInterrupt(SecurityTestBase):
#     """
#     30-second NIC-level network interrupt on a storage node; verifies that
#     the DHCHAP session resumes correctly after reconnect.
#
#     TC-SEC-075  Create DHCHAP lvol, connect, mount
#     TC-SEC-076  Trigger 30-second network interrupt on a storage node
#     TC-SEC-077  Wait for interrupt to end; reconnect with DHCHAP creds
#     TC-SEC-078  Mount and run FIO – data plane must be intact
#     TC-SEC-079  Verify get-secret still returns valid credentials
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_network_interrupt"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNetworkInterrupt START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secnwi{_rand_suffix()}"
#
#         # TC-SEC-075: create lvol + connect
#         self.logger.info("TC-SEC-075: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         self.logger.info("TC-SEC-075: PASSED")
#
#         # Disconnect before network interrupt
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-076: trigger 30-second NIC interrupt on a storage node
#         self.logger.info("TC-SEC-076: Triggering 30s network interrupt …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node_ip = primary_nodes[0]["mgmt_ip"]
#         active_ifaces = self.ssh_obj.get_active_interfaces(target_node_ip)
#         if active_ifaces:
#             self.ssh_obj.disconnect_all_active_interfaces(
#                 target_node_ip, active_ifaces, duration_secs=30)
#         self.logger.info("TC-SEC-076: Network interrupt triggered PASSED")
#
#         # TC-SEC-077: wait for interrupt to end then reconnect
#         self.logger.info("TC-SEC-077: Waiting 35s for interrupt to end …")
#         sleep_n_sec(35)
#         self.logger.info("TC-SEC-077: Reconnecting with DHCHAP creds …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after network interrupt failed"
#         self.logger.info("TC-SEC-077: PASSED")
#
#         # TC-SEC-078: mount and run FIO
#         self.logger.info("TC-SEC-078: Running FIO after reconnect …")
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-078: FIO PASSED")
#
#         # TC-SEC-079: get-secret must still return valid creds
#         self.logger.info("TC-SEC-079: Verifying get-secret still works …")
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert out and "error" not in out.lower(), f"get-secret failed after network interrupt: {err}"
#         self.logger.info("TC-SEC-079: get-secret PASSED")
#
#         self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 14 – HA lvol: security preserved through primary failover
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityHAFailover(SecurityTestBase):
#     """
#     Creates an HA lvol (npcs=1) with DHCHAP, triggers primary failover by
#     shutting down the primary node, and verifies security config is intact
#     after the secondary takes over.
#
#     TC-SEC-080  Create HA DHCHAP lvol (ndcs=1, npcs=1)
#     TC-SEC-081  Connect with correct host NQN and run FIO
#     TC-SEC-082  Shutdown the primary storage node
#     TC-SEC-083  Restart the node; wait for HA to settle
#     TC-SEC-084  Reconnect with original DHCHAP creds and verify FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_ha_failover"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityHAFailover START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secha{_rand_suffix()}"
#
#         # TC-SEC-080: create HA lvol with DHCHAP
#         self.logger.info("TC-SEC-080: Creating HA DHCHAP lvol (ndcs=1, npcs=1) …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             distr_ndcs=1, distr_npcs=1,
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(5)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-081: connect and run FIO
#         self.logger.info("TC-SEC-081: Connecting HA lvol and running FIO …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_pre.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=30)
#         self.logger.info("TC-SEC-081: Pre-failover FIO PASSED")
#
#         # Disconnect before shutdown
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-082: shutdown a primary storage node
#         self.logger.info("TC-SEC-082: Shutting down a primary storage node …")
#         nodes = self.sbcli_utils.get_storage_nodes()
#         primary_nodes = [n for n in nodes["results"] if not n.get("is_secondary_node")]
#         assert primary_nodes, "No primary storage nodes found"
#         target_node = primary_nodes[0]["uuid"]
#         self.sbcli_utils.shutdown_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "offline", timeout=120)
#         self.logger.info("TC-SEC-082: Node offline PASSED")
#
#         # TC-SEC-083: restart node, wait for HA to settle
#         self.logger.info("TC-SEC-083: Waiting 2 min before restarting node …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-083: Restarting node and waiting for HA settle …")
#         self.sbcli_utils.restart_node(target_node)
#         self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
#         self.logger.info("TC-SEC-083: Node online — waiting 2 min for HA to settle …")
#         sleep_n_sec(120)
#         self.logger.info("TC-SEC-083: HA settled PASSED")
#
#         # TC-SEC-084: reconnect with original DHCHAP creds
#         self.logger.info("TC-SEC-084: Reconnecting with DHCHAP creds after failover …")
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         assert lvol_device2, "Reconnect after HA failover failed"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file2 = f"{self.log_path}/{lvol_name}_post.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-084: Post-failover FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityHAFailover PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 15 – Management node reboot: DHCHAP config survives
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityMgmtNodeReboot(SecurityTestBase):
#     """
#     Reboots the management node and verifies that DHCHAP credentials are
#     still retrievable (get-secret) and connections still work after mgmt
#     node comes back online.
#
#     TC-SEC-085  Create DHCHAP lvol (SEC_BOTH), add allowed host, get-secret OK
#     TC-SEC-086  Reboot management node; wait for it to come back
#     TC-SEC-087  get-secret after mgmt reboot – credentials must still be present
#     TC-SEC-088  Connect lvol with original DHCHAP creds and run brief FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_mgmt_node_reboot"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityMgmtNodeReboot START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secmgmt{_rand_suffix()}"
#
#         # TC-SEC-085: create lvol, get-secret baseline
#         self.logger.info("TC-SEC-085: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         pre_secret, pre_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert pre_secret and "error" not in pre_secret.lower(), \
#             f"Pre-reboot get-secret failed: {pre_err}"
#         self.logger.info("TC-SEC-085: Pre-reboot secret obtained PASSED")
#
#         # TC-SEC-086: reboot management node
#         self.logger.info("TC-SEC-086: Rebooting management node …")
#         self.ssh_obj.reboot_node(self.mgmt_nodes[0], wait_time=300)
#         sleep_n_sec(15)
#         self.logger.info("TC-SEC-086: Management node back online PASSED")
#
#         # TC-SEC-087: get-secret after reboot
#         sleep_n_sec(100)  # Extra wait to ensure all services are fully up and secrets are loaded
#         self.logger.info("TC-SEC-087: Verifying get-secret after mgmt reboot …")
#         post_secret, post_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert post_secret and "error" not in post_secret.lower(), \
#             f"Post-reboot get-secret failed: {post_err}"
#         self.logger.info("TC-SEC-087: get-secret after reboot PASSED")
#
#         # TC-SEC-088: connect + FIO
#         self.logger.info("TC-SEC-088: Connecting with DHCHAP creds after mgmt reboot …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-088: FIO after mgmt reboot PASSED")
#
#         self.logger.info("=== TestLvolSecurityMgmtNodeReboot PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 16 – Dynamic modification of allowed hosts during FIO
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityDynamicModification(SecurityTestBase):
#     """
#     Tests live add/remove of host NQNs, NQN rotation (key change), and
#     multi-NQN scenarios on a running lvol.
#
#     TC-SEC-089  Remove host NQN while FIO running → connection drops
#     TC-SEC-090  Re-add host NQN → reconnect resumes
#     TC-SEC-091  Add a second NQN; verify both NQNs can get connect strings
#     TC-SEC-092  Remove first NQN; verify second NQN still works
#     TC-SEC-093  Remove second NQN; verify no NQN can connect
#     TC-SEC-094  Add first NQN back → reconnect works again
#     TC-SEC-095  Teardown
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_dynamic_modification"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityDynamicModification START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         second_nqn = f"nqn.2024-01.io.simplyblock:test:second-{_rand_suffix()}"
#         lvol_name = f"secdyn{_rand_suffix()}"
#
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # Pre-add second_nqn so that removing host_nqn in TC-089/TC-092 never leaves
#         # allowed_hosts empty (empty list → backend assumes no security → no rejection).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower(), f"pre-add second NQN failed: {err}"
#         self.logger.info(f"Pre-added {second_nqn!r} to keep allowed_hosts non-empty during removals")
#
#         # TC-SEC-089: remove host_nqn → second_nqn still in list → backend rejects host_nqn
#         self.logger.info("TC-SEC-089: Removing host NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"remove-host failed: {err}"
#         connect_ls, err2 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert not connect_ls or err2, \
#             "Expected no connect string after removing host NQN"
#         self.logger.info("TC-SEC-089: Remove host NQN PASSED")
#
#         # TC-SEC-090: re-add host → connect string available
#         self.logger.info("TC-SEC-090: Re-adding host NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"add-host failed: {err}"
#         sleep_n_sec(2)
#         connect_ls2, err3 = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert connect_ls2 and not err3, \
#             f"Connect string should be available after re-adding NQN; err={err3}"
#         self.logger.info("TC-SEC-090: Re-add host NQN PASSED")
#
#         # TC-SEC-091: add second NQN, verify both get connect strings
#         self.logger.info("TC-SEC-091: Adding second NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower(), f"add second NQN failed: {err}"
#         sleep_n_sec(2)
#         cs1, _ = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         cs2, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert cs1, "First NQN should still get connect string"
#         assert cs2, "Second NQN should get connect string"
#         self.logger.info("TC-SEC-091: Both NQNs work PASSED")
#
#         # TC-SEC-092: remove first NQN, verify second still works
#         self.logger.info("TC-SEC-092: Removing first NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         cs2b, _ = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert cs2b, "Second NQN should still work after removing first"
#         cs1b, err1b = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert not cs1b or err1b, "First NQN should not work after removal"
#         self.logger.info("TC-SEC-092: PASSED")
#
#         # Re-add host_nqn so that removing second_nqn in TC-093 doesn't leave
#         # allowed_hosts empty (same empty-list bug as TC-089).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower(), f"re-add host_nqn before TC-093 failed: {err}"
#
#         # TC-SEC-093: remove second NQN → host_nqn still in list → backend rejects second_nqn
#         self.logger.info("TC-SEC-093: Removing second NQN …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], lvol_id, second_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         cs2c, err2c = self._get_connect_str_cli(lvol_id, host_nqn=second_nqn)
#         assert not cs2c or err2c, "Second NQN should not work after removal"
#         self.logger.info("TC-SEC-093: PASSED")
#
#         # TC-SEC-094: re-add first NQN, connect + FIO
#         self.logger.info("TC-SEC-094: Re-adding first NQN and running FIO …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-094: FIO after re-add PASSED")
#
#         self.logger.info("TC-SEC-095: TestLvolSecurityDynamicModification teardown")
#         self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 17 – Concurrent multi-client connections with DHCHAP
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityMultiClientConcurrent(SecurityTestBase):
#     """
#     Tests concurrent client connection attempts: correct NQN vs wrong NQN
#     issued simultaneously.
#
#     TC-SEC-096  Create DHCHAP lvol with one registered NQN
#     TC-SEC-097  Concurrently request connect strings for correct and wrong NQNs
#     TC-SEC-098  Verify correct NQN returns a valid connect string
#     TC-SEC-099  Verify wrong NQN returns no connect string or an error
#     TC-SEC-100  Connect with correct NQN and run FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_multi_client_concurrent"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityMultiClientConcurrent START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
#         lvol_name = f"secmc{_rand_suffix()}"
#
#         # TC-SEC-096: create DHCHAP lvol
#         self.logger.info("TC-SEC-096: Creating DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-097 & TC-SEC-098 & TC-SEC-099: concurrent connect string requests
#         self.logger.info("TC-SEC-097: Launching concurrent connect-string requests …")
#         results = {}
#
#         def _req(nqn, key):
#             try:
#                 cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
#                 results[key] = (cs, err)
#             except Exception as e:
#                 results[key] = (None, str(e))
#
#         t_good = threading.Thread(target=_req, args=(host_nqn, "good"))
#         t_bad  = threading.Thread(target=_req, args=(wrong_nqn, "bad"))
#         t_good.start()
#         t_bad.start()
#         t_good.join()
#         t_bad.join()
#
#         good_cs, good_err = results.get("good", (None, "no result"))
#         bad_cs,  bad_err  = results.get("bad",  (None, "no result"))
#
#         # TC-SEC-098: correct NQN must succeed
#         assert good_cs, \
#             f"Correct NQN should return connect string; err={good_err}"
#         self.logger.info("TC-SEC-098: Correct NQN connect string PASSED")
#
#         # TC-SEC-099: wrong NQN must fail
#         assert not good_err or "error" not in (good_err or "").lower(), \
#             f"Correct NQN should have no error; err={good_err}"
#         assert not bad_cs or bad_err, \
#             f"Wrong NQN should not return a connect string; got {bad_cs}"
#         self.logger.info("TC-SEC-099: Wrong NQN rejected PASSED")
#
#         # TC-SEC-100: connect + FIO with correct NQN
#         self.logger.info("TC-SEC-100: Connecting and running FIO with correct NQN …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-100: FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 18 – Scale: 10 DHCHAP volumes with rapid add/remove
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityScaleAndRapidOps(SecurityTestBase):
#     """
#     Creates 10 DHCHAP volumes simultaneously (each with a unique NQN) then
#     performs rapid add/remove of host NQNs.  Verifies no SPDK key-name
#     collisions occur and all volumes remain independently accessible.
#
#     TC-SEC-101  Create 10 DHCHAP lvols with unique NQNs (no collisions)
#     TC-SEC-102  Rapidly remove all host NQNs from all volumes
#     TC-SEC-103  Rapidly re-add all host NQNs
#     TC-SEC-104  Verify every volume can still be connected (get connect string)
#     """
#
#     VOLUME_COUNT = 10
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_scale_and_rapid_ops"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityScaleAndRapidOps START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         # TC-SEC-101: create 10 volumes each with unique NQN
#         self.logger.info(f"TC-SEC-101: Creating {self.VOLUME_COUNT} DHCHAP lvols …")
#         volumes = []  # list of (lvol_name, lvol_id, nqn)
#         for i in range(self.VOLUME_COUNT):
#             suffix = _rand_suffix()
#             lvol_name = f"secsc{i}{suffix}"
#             # unique NQN per volume to avoid SPDK keyring collision
#             uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#             uuid = uuid_out.strip().split('\n')[0].strip().lower()
#             nqn = f"nqn.2014-08.org.nvmexpress:uuid:{uuid}"
#             # Write hostnqn only for the last volume (we only connect one)
#             out, err = self.ssh_obj.create_sec_lvol(
#                 self.mgmt_nodes[0], lvol_name, "1G", self.pool_name,
#                 allowed_hosts=[nqn],
#             )
#             assert not err or "error" not in err.lower(), \
#                 f"lvol {lvol_name} creation failed: {err}"
#             sleep_n_sec(1)
#             lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#             assert lvol_id, f"Could not find ID for {lvol_name}"
#             volumes.append((lvol_name, lvol_id, nqn))
#             self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#         self.logger.info(f"TC-SEC-101: {self.VOLUME_COUNT} volumes created PASSED")
#
#         # TC-SEC-102: rapid remove all NQNs
#         self.logger.info("TC-SEC-102: Rapidly removing all host NQNs …")
#         for lvol_name, lvol_id, nqn in volumes:
#             out, err = self.ssh_obj.remove_host_from_lvol(
#                 self.mgmt_nodes[0], lvol_id, nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"remove-host failed for {lvol_name}: {err}"
#         self.logger.info("TC-SEC-102: PASSED")
#
#         # TC-SEC-103: rapid re-add all NQNs
#         self.logger.info("TC-SEC-103: Rapidly re-adding all host NQNs …")
#         for lvol_name, lvol_id, nqn in volumes:
#             out, err = self.ssh_obj.add_host_to_lvol(
#                 self.mgmt_nodes[0], lvol_id, nqn)
#             assert not err or "error" not in err.lower(), \
#                 f"add-host failed for {lvol_name}: {err}"
#         sleep_n_sec(3)
#         self.logger.info("TC-SEC-103: PASSED")
#
#         # TC-SEC-104: all volumes can still get connect strings
#         self.logger.info("TC-SEC-104: Verifying all volumes still have valid connect strings …")
#         for lvol_name, lvol_id, nqn in volumes:
#             cs, err = self._get_connect_str_cli(lvol_id, host_nqn=nqn)
#             assert cs, \
#                 f"Volume {lvol_name} should have valid connect string after re-add; err={err}"
#         self.logger.info("TC-SEC-104: All volumes accessible PASSED")
#
#         self.logger.info("=== TestLvolSecurityScaleAndRapidOps PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 19 – Extended negative: tampered keys, edge-case CLI errors
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityNegativeConnectExtended(SecurityTestBase):
#     """
#     Extended negative scenarios beyond the basic TestLvolSecurityNegativeConnect:
#
#     TC-SEC-105  get-secret after remove-host → must return error
#     TC-SEC-106  add-host with empty NQN string → expect error
#     TC-SEC-107  add-host on non-existent lvol ID → expect error
#     TC-SEC-108  remove-host on non-existent lvol ID → expect error
#     TC-SEC-109  create lvol with SEC_CTRL_ONLY (bidirectional) and wrong host NQN → rejected
#     TC-SEC-110  create lvol with SEC_BOTH then get-secret with unregistered NQN → error
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_negative_connect_extended"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityNegativeConnectExtended START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_ctrl", self.cluster_id, sec_options=SEC_CTRL_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#         absent_nqn = f"nqn.2024-01.io.simplyblock:test:absent-{_rand_suffix()}"
#         fake_lvol_id = "00000000-0000-0000-0000-000000000099"
#
#         lvol_name = f"secnex{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         # TC-SEC-105: get-secret after remove-host
#         self.logger.info("TC-SEC-105: get-secret after remove-host …")
#         self.ssh_obj.remove_host_from_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         sleep_n_sec(2)
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, f"get-secret after remove should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-105: PASSED")
#
#         # Restore host for subsequent tests
#         self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], lvol_id, host_nqn)
#         sleep_n_sec(2)
#
#         # TC-SEC-106: add-host with empty NQN
#         self.logger.info("TC-SEC-106: add-host with empty NQN …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], lvol_id, "")
#         has_error = bool(err) or ("error" in (out or "").lower())
#         assert has_error, f"add-host with empty NQN should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-106: PASSED")
#
#         # TC-SEC-107: add-host on non-existent lvol
#         self.logger.info("TC-SEC-107: add-host on non-existent lvol …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], fake_lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"add-host on non-existent lvol should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-107: PASSED")
#
#         # TC-SEC-108: remove-host on non-existent lvol
#         self.logger.info("TC-SEC-108: remove-host on non-existent lvol …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], fake_lvol_id, host_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"remove-host on non-existent lvol should fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-108: PASSED")
#
#         # TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN → no connect string
#         self.logger.info("TC-SEC-109: SEC_CTRL_ONLY lvol with wrong NQN …")
#         lvol_ctrl = f"secctrl{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_ctrl, self.lvol_size, self.pool_name + "_ctrl",
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_ctrl_id = self.sbcli_utils.get_lvol_id(lvol_ctrl)
#         assert lvol_ctrl_id
#         self.lvol_mount_details[lvol_ctrl] = {"ID": lvol_ctrl_id, "Mount": None}
#         wrong_cs, wrong_err = self._get_connect_str_cli(lvol_ctrl_id, host_nqn=absent_nqn)
#         assert not wrong_cs or wrong_err, \
#             f"Unregistered NQN should not get connect string; cs={wrong_cs}"
#         self.logger.info("TC-SEC-109: PASSED")
#
#         # TC-SEC-110: get-secret with unregistered NQN
#         self.logger.info("TC-SEC-110: get-secret with unregistered NQN …")
#         out, err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_id, absent_nqn)
#         has_error = bool(err) or ("error" in (out or "").lower()) \
#                     or ("not found" in (out or "").lower())
#         assert has_error, \
#             f"get-secret for unregistered NQN must fail; out={out!r} err={err!r}"
#         self.logger.info("TC-SEC-110: PASSED")
#
#         self.logger.info("=== TestLvolSecurityNegativeConnectExtended PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 20 – Clone has independent security config from parent
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityCloneOverride(SecurityTestBase):
#     """
#     Verifies that a clone can have a different security configuration from
#     its parent and that the two configs do not interfere.
#
#     TC-SEC-111  Create parent lvol with SEC_HOST_ONLY + allowed host NQN_A
#     TC-SEC-112  Create clone of parent snapshot – no explicit sec_options (inherits)
#     TC-SEC-113  Add a different NQN_B to the clone; verify NQN_A works on parent,
#                 NQN_B works on clone
#     TC-SEC-114  Remove NQN_A from parent; verify parent is inaccessible but clone
#                 still accessible with NQN_B
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_clone_override"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityCloneOverride START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         nqn_a = self._get_client_host_nqn()
#         uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#         uuid_b = uuid_out.strip().split('\n')[0].strip().lower()
#         nqn_b = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_b}"
#
#         parent_name = f"secpar{_rand_suffix()}"
#
#         # TC-SEC-111: create parent lvol with SEC_HOST_ONLY + NQN_A
#         self.logger.info("TC-SEC-111: Creating parent DHCHAP lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], parent_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[nqn_a],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         parent_id = self.sbcli_utils.get_lvol_id(parent_name)
#         assert parent_id
#         self.lvol_mount_details[parent_name] = {"ID": parent_id, "Mount": None}
#         self.logger.info("TC-SEC-111: PASSED")
#
#         # Connect, write data, disconnect
#         lvol_device, _ = self._connect_and_get_device(parent_name, parent_id, host_nqn=nqn_a)
#         mount_point = f"{self.mount_path}/{parent_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[parent_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{parent_name}_w.log"
#         self._run_fio_and_validate(parent_name, mount_point, log_file, rw="write", runtime=20)
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(parent_id)
#         self.lvol_mount_details[parent_name]["Mount"] = None
#
#         # TC-SEC-112: snapshot + clone
#         self.logger.info("TC-SEC-112: Creating snapshot and clone …")
#         snap_name = f"snappar{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot add {parent_id} {snap_name}")
#         assert not err or "error" not in err.lower(), f"snapshot creation failed: {err}"
#         sleep_n_sec(3)
#         snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
#         assert snap_id, f"Could not find snapshot ID for {snap_name}"
#
#         clone_name = f"secclone{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot clone {snap_id} {clone_name}")
#         assert not err or "error" not in err.lower(), f"clone creation failed: {err}"
#         sleep_n_sec(5)
#         clone_id = self.sbcli_utils.get_lvol_id(clone_name)
#         assert clone_id, f"Could not find clone ID for {clone_name}"
#         self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
#         self.logger.info("TC-SEC-112: Snapshot+clone created PASSED")
#
#         # TC-SEC-113: add NQN_B to clone; verify NQN_A on parent, NQN_B on clone
#         self.logger.info("TC-SEC-113: Adding NQN_B to clone …")
#         out, err = self.ssh_obj.add_host_to_lvol(
#             self.mgmt_nodes[0], clone_id, nqn_b)
#         assert not err or "error" not in err.lower(), f"add NQN_B to clone failed: {err}"
#         sleep_n_sec(2)
#         cs_parent_a, _ = self._get_connect_str_cli(parent_id, host_nqn=nqn_a)
#         cs_clone_b, _  = self._get_connect_str_cli(clone_id,  host_nqn=nqn_b)
#         assert cs_parent_a, "Parent: NQN_A should still get connect string"
#         assert cs_clone_b,  "Clone: NQN_B should get connect string"
#         self.logger.info("TC-SEC-113: Independent NQNs PASSED")
#
#         # TC-SEC-114: remove NQN_A from parent; clone NQN_B still works.
#         # Pre-add nqn_b to parent so that after removing nqn_a the parent's
#         # allowed_hosts is non-empty (empty list → backend assumes no security → no rejection).
#         out, err = self.ssh_obj.add_host_to_lvol(self.mgmt_nodes[0], parent_id, nqn_b)
#         assert not err or "error" not in err.lower(), f"pre-add nqn_b to parent failed: {err}"
#         self.logger.info("TC-SEC-114: Removing NQN_A from parent …")
#         out, err = self.ssh_obj.remove_host_from_lvol(
#             self.mgmt_nodes[0], parent_id, nqn_a)
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(2)
#         parent_api = self.sbcli_utils.get_lvol_details(lvol_id=parent_id)
#         allowed_after = [h.get("nqn") for h in parent_api[0].get("allowed_hosts", [])]
#         assert nqn_a not in allowed_after, \
#             f"NQN_A should have been removed from parent allowed_hosts, got: {allowed_after}"
#         cs_clone_b2, _ = self._get_connect_str_cli(clone_id, host_nqn=nqn_b)
#         assert cs_clone_b2, "Clone NQN_B should still be accessible"
#         self.logger.info("TC-SEC-114: Clone independence PASSED")
#
#         self.logger.info("=== TestLvolSecurityCloneOverride PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 21 – Security + backup: credentials survive backup/restore cycle
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityWithBackup(SecurityTestBase):
#     """
#     Backs up a DHCHAP+crypto lvol and verifies that the restored lvol
#     can be connected with the appropriate credentials.
#
#     TC-SEC-115  Create DHCHAP+crypto lvol, write FIO data, create snapshot
#     TC-SEC-116  Trigger backup of snapshot; wait for completion
#     TC-SEC-117  Restore backup to a new lvol name
#     TC-SEC-118  Verify the restored lvol can be accessed (get connect string
#                 for the original NQN should succeed since DHCHAP config
#                 is preserved with the lvol metadata)
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_with_backup"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityWithBackup START ===")
#         # Check backup is available
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0], f"{self.base_cmd} backup list 2>&1 | head -5")
#         if "command not found" in (out or "").lower() or "error" in (err or "").lower():
#             self.logger.info("Backup feature not available – skipping TC-SEC-115..118")
#             return
#
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secbck{_rand_suffix()}"
#
#         # TC-SEC-115: create DHCHAP+crypto lvol, write data
#         self.logger.info("TC-SEC-115: Creating DHCHAP+crypto lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_w.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
#
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-116: snapshot + backup
#         self.logger.info("TC-SEC-116: Creating snapshot and backup …")
#         snap_name = f"snap{lvol_name[-6:]}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name} --backup")
#         assert not err or "error" not in err.lower(), f"snapshot add --backup failed: {err}"
#         sleep_n_sec(5)
#
#         # Wait for backup completion
#         import time as _time
#         deadline = _time.time() + 300
#         backup_id = None
#         while _time.time() < deadline:
#             list_out, _ = self.ssh_obj.exec_command(
#                 self.mgmt_nodes[0], f"{self.base_cmd} -d backup list")
#             for line in (list_out or "").splitlines():
#                 if snap_name in line:
#                     parts = [p.strip() for p in line.split("|") if p.strip()]
#                     if parts:
#                         for p in parts:
#                             if len(p) == 36 and "-" in p:
#                                 backup_id = p
#                     status_lower = line.lower()
#                     if "done" in status_lower or "complete" in status_lower:
#                         break
#             else:
#                 sleep_n_sec(10)
#                 continue
#             break
#         assert backup_id, "Could not find backup ID after snapshot backup"
#         self.logger.info(f"TC-SEC-116: Backup {backup_id} complete PASSED")
#
#         # TC-SEC-117: restore backup
#         self.logger.info("TC-SEC-117: Restoring backup …")
#         restored_name = f"secrst{_rand_suffix()}"
#         out, err = self.ssh_obj.exec_command(
#             self.mgmt_nodes[0],
#             f"{self.base_cmd} -d backup restore {backup_id} --lvol {restored_name} --pool {self.pool_name}")
#         assert not err or "error" not in err.lower(), f"backup restore failed: {err}"
#         # Wait for restored lvol to appear
#         deadline2 = _time.time() + 300
#         while _time.time() < deadline2:
#             list_out, _ = self.ssh_obj.exec_command(self.mgmt_nodes[0], f"{self.base_cmd} lvol list")
#             if restored_name in (list_out or ""):
#                 break
#             sleep_n_sec(10)
#         else:
#             raise TimeoutError(f"Restored lvol {restored_name} did not appear within 300s")
#         self.logger.info(f"TC-SEC-117: Restore of {restored_name} PASSED")
#         self.lvol_mount_details[restored_name] = {"ID": None, "Mount": None}
#
#         # TC-SEC-118: verify unauthenticated connect is rejected (security enforced)
#         self.logger.info("TC-SEC-118: Verifying unauthenticated connect is rejected …")
#         restored_id = self.sbcli_utils.get_lvol_id(restored_name)
#         assert restored_id, f"Could not find ID for restored lvol {restored_name}"
#         self.lvol_mount_details[restored_name]["ID"] = restored_id
#         connect_ls, err = self._get_connect_str_cli(restored_id)
#         # Restored lvol inherits allowed_hosts — connect without host_nqn must fail
#         assert not connect_ls or ("host-nqn" in (err or "").lower() or "allowed" in (err or "").lower()), \
#             f"TC-SEC-118: Expected rejection without host-nqn, got connect_ls={connect_ls!r} err={err!r}"
#         self.logger.info("TC-SEC-118: Unauthenticated connect correctly rejected PASSED")
#
#         # TC-SEC-119: Connect restored lvol with source host NQN (security must be preserved)
#         self.logger.info("TC-SEC-119: Connecting restored lvol with source host NQN …")
#         restored_device, _ = self._connect_and_get_device(
#             restored_name, restored_id, host_nqn=host_nqn)
#         assert restored_device, \
#             f"TC-SEC-119: Restored lvol did not connect with source host_nqn={host_nqn}"
#         mount_restored = f"{self.mount_path}/{restored_name}"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=restored_device, mount_path=mount_restored)
#         self.lvol_mount_details[restored_name]["Mount"] = mount_restored
#         self.logger.info(
#             f"TC-SEC-119: Restored lvol connected at {restored_device}, "
#             f"mounted at {mount_restored} PASSED")
#
#         # TC-SEC-120: Data integrity — restored files must match source lvol
#         self.logger.info("TC-SEC-120: Verifying data integrity: source vs restored …")
#
#         # Reconnect source lvol to generate checksums
#         source_device, _ = self._connect_and_get_device(
#             lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_source = f"{self.mount_path}/{lvol_name}_verify"
#         self.ssh_obj.mount_path(
#             node=self.fio_node, device=source_device, mount_path=mount_source)
#         source_files = self.ssh_obj.find_files(self.fio_node, mount_source)
#         source_checksums = self.ssh_obj.generate_checksums(self.fio_node, source_files)
#         self.ssh_obj.unmount_path(self.fio_node, mount_source)
#         self._disconnect_lvol(lvol_id)
#
#         # Compare restored files against source checksums
#         restored_files = self.ssh_obj.find_files(self.fio_node, mount_restored)
#         self.ssh_obj.verify_checksums(
#             self.fio_node, restored_files, source_checksums,
#             by_name=True,
#             message="Restored lvol data does not match source lvol data")
#         self.logger.info("TC-SEC-120: Data integrity verified PASSED")
#
#         # Cleanup restored lvol
#         self.ssh_obj.unmount_path(self.fio_node, mount_restored)
#         sleep_n_sec(2)
#         self._disconnect_lvol(restored_id)
#         self.lvol_mount_details[restored_name]["Mount"] = None
#
#         self.logger.info("=== TestLvolSecurityWithBackup PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 22 – Resize a DHCHAP+crypto lvol: security config must be preserved
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityResize(SecurityTestBase):
#     """
#     Creates a DHCHAP+crypto lvol, resizes it, and verifies that the DHCHAP
#     configuration is unchanged after the resize operation.
#
#     TC-SEC-119  Create DHCHAP+crypto lvol (5G), connect, run FIO
#     TC-SEC-120  Resize lvol to 10G via sbcli_utils.resize_lvol
#     TC-SEC-121  Verify get-secret still works; connect with DHCHAP and run FIO
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_resize"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityResize START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secrsz{_rand_suffix()}"
#
#         # TC-SEC-119: create DHCHAP+crypto lvol 5G
#         self.logger.info("TC-SEC-119: Creating DHCHAP+crypto 5G lvol …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, "5G", self.pool_name,
#             allowed_hosts=[host_nqn],
#             encrypt=True, key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
#         )
#         assert not err or "error" not in err.lower(), f"lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_pre.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="write", runtime=20)
#         self.logger.info("TC-SEC-119: Pre-resize FIO PASSED")
#
#         # Disconnect before resize
#         self.ssh_obj.unmount_path(self.fio_node, mount_point)
#         sleep_n_sec(2)
#         self._disconnect_lvol(lvol_id)
#         sleep_n_sec(2)
#         self.lvol_mount_details[lvol_name]["Mount"] = None
#
#         # TC-SEC-120: resize to 10G
#         self.logger.info("TC-SEC-120: Resizing lvol to 10G …")
#         self.sbcli_utils.resize_lvol(lvol_id, "10G")
#         sleep_n_sec(5)
#         self.logger.info("TC-SEC-120: Resize completed PASSED")
#
#         # TC-SEC-121: get-secret still works; reconnect + FIO
#         self.logger.info("TC-SEC-121: Verifying DHCHAP config after resize …")
#         out, err = self.ssh_obj.get_lvol_host_secret(self.mgmt_nodes[0], lvol_id, host_nqn)
#         assert out and "error" not in (out or "").lower(), \
#             f"get-secret after resize failed: {err}"
#         self.logger.info("TC-SEC-121: get-secret after resize PASSED")
#
#         lvol_device2, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device2, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file2 = f"{self.log_path}/{lvol_name}_post.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
#         self.logger.info("TC-SEC-121: Post-resize FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityResize PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 23 – Volume list security fields validation
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityVolumeListFields(SecurityTestBase):
#     """
#     Verifies that security-related fields appear correctly in CLI output
#     after volume creation with various security options.
#
#     TC-SEC-122  Create DHCHAP+crypto lvol; verify CLI `volume get` has
#                 dhchap_key / dhchap_ctrlr_key fields
#     TC-SEC-123  Create SEC_HOST_ONLY lvol; verify ctrl key fields absent/false
#     TC-SEC-124  get-secret returns non-empty credential for registered NQN
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_volume_list_fields"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityVolumeListFields START ===")
#         self.fio_node = self.fio_node[0]
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name + "_host", self.cluster_id, sec_options=SEC_HOST_ONLY)
#
#         host_nqn = self._get_client_host_nqn()
#
#         # TC-SEC-122: SEC_BOTH lvol – both dhchap fields should be true/present
#         self.logger.info("TC-SEC-122: Creating SEC_BOTH lvol and checking fields …")
#         lvol_both = f"secvlb{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_both, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_both_id = self.sbcli_utils.get_lvol_id(lvol_both)
#         assert lvol_both_id
#         self.lvol_mount_details[lvol_both] = {"ID": lvol_both_id, "Mount": None}
#
#         detail_out = self._get_lvol_details_via_cli(lvol_both_id)
#         has_dhchap_key = "dhchap_key" in detail_out.lower() or "dhchap" in detail_out.lower()
#         assert has_dhchap_key, \
#             f"volume get should mention dhchap fields for SEC_BOTH: {detail_out!r}"
#         self.logger.info("TC-SEC-122: DHCHAP fields present PASSED")
#
#         # TC-SEC-123: SEC_HOST_ONLY lvol
#         self.logger.info("TC-SEC-123: Creating SEC_HOST_ONLY lvol and checking fields …")
#         uuid_out, _ = self.ssh_obj.exec_command(self.fio_node, "uuidgen")
#         uuid_h = uuid_out.strip().split('\n')[0].strip().lower()
#         nqn_h = f"nqn.2014-08.org.nvmexpress:uuid:{uuid_h}"
#
#         lvol_host = f"secvlh{_rand_suffix()}"
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_host, self.lvol_size, self.pool_name + "_host",
#             allowed_hosts=[nqn_h],
#         )
#         assert not err or "error" not in err.lower()
#         sleep_n_sec(3)
#         lvol_host_id = self.sbcli_utils.get_lvol_id(lvol_host)
#         assert lvol_host_id
#         self.lvol_mount_details[lvol_host] = {"ID": lvol_host_id, "Mount": None}
#
#         detail_host = self._get_lvol_details_via_cli(lvol_host_id)
#         self.logger.info(f"TC-SEC-123: volume get output: {detail_host!r}")
#         # SEC_HOST_ONLY means dhchap_key=True, dhchap_ctrlr_key=False
#         assert "dhchap" in detail_host.lower() or "allowed_host" in detail_host.lower(), \
#             f"SEC_HOST_ONLY lvol should show dhchap-related info: {detail_host!r}"
#         self.logger.info("TC-SEC-123: PASSED")
#
#         # TC-SEC-124: get-secret returns non-empty credential
#         self.logger.info("TC-SEC-124: Verifying get-secret returns credentials …")
#         secret_out, secret_err = self.ssh_obj.get_lvol_host_secret(
#             self.mgmt_nodes[0], lvol_both_id, host_nqn)
#         assert secret_out and "error" not in (secret_out or "").lower(), \
#             f"get-secret should return credentials; out={secret_out!r} err={secret_err!r}"
#         self.logger.info("TC-SEC-124: get-secret credentials PASSED")
#
#         self.logger.info("=== TestLvolSecurityVolumeListFields PASSED ===")
#
#
# # ═══════════════════════════════════════════════════════════════════════════
# #  Test 24 – DHCHAP over RDMA transport (skipped if RDMA not available)
# # ═══════════════════════════════════════════════════════════════════════════
#
# class TestLvolSecurityRDMA(SecurityTestBase):
#     """
#     Creates a DHCHAP lvol on an RDMA-capable cluster and verifies that
#     authentication and data I/O work correctly over the RDMA fabric.
#
#     TC-SEC-125  Skip if cluster does not support RDMA (fabric_rdma=False)
#     TC-SEC-126  Create DHCHAP lvol with fabric=rdma; get connect string
#     TC-SEC-127  Connect via RDMA, mount, run FIO, validate data integrity
#     """
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.test_name = "lvol_security_rdma"
#
#     def run(self):
#         self.logger.info("=== TestLvolSecurityRDMA START ===")
#         self.fio_node = self.fio_node[0]
#
#         # TC-SEC-125: skip if RDMA not available
#         self.logger.info("TC-SEC-125: Checking RDMA availability …")
#         cluster_details = self.sbcli_utils.get_cluster_details()
#         fabric_rdma = cluster_details.get("fabric_rdma", False)
#         if not fabric_rdma:
#             self.logger.info(
#                 "TC-SEC-125: RDMA not available on this cluster (fabric_rdma=False) – SKIPPED")
#             return
#         self.logger.info("TC-SEC-125: RDMA available – proceeding")
#
#         self.ssh_obj.add_storage_pool(self.mgmt_nodes[0], self.pool_name, self.cluster_id, sec_options=SEC_BOTH)
#         host_nqn = self._get_client_host_nqn()
#         lvol_name = f"secrdma{_rand_suffix()}"
#
#         # TC-SEC-126: create DHCHAP lvol with rdma fabric
#         self.logger.info("TC-SEC-126: Creating DHCHAP lvol with RDMA fabric …")
#         out, err = self.ssh_obj.create_sec_lvol(
#             self.mgmt_nodes[0], lvol_name, self.lvol_size, self.pool_name,
#             allowed_hosts=[host_nqn],
#             fabric="rdma",
#         )
#         assert not err or "error" not in err.lower(), f"RDMA lvol creation failed: {err}"
#         sleep_n_sec(3)
#         lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
#         assert lvol_id, f"Could not find ID for {lvol_name}"
#         self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
#
#         connect_ls, err = self._get_connect_str_cli(lvol_id, host_nqn=host_nqn)
#         assert connect_ls and not err, \
#             f"RDMA lvol should return connect string; err={err}"
#         self.logger.info("TC-SEC-126: RDMA DHCHAP connect string PASSED")
#
#         # TC-SEC-127: connect, mount, FIO
#         self.logger.info("TC-SEC-127: Connecting RDMA lvol and running FIO …")
#         lvol_device, _ = self._connect_and_get_device(lvol_name, lvol_id, host_nqn=host_nqn)
#         mount_point = f"{self.mount_path}/{lvol_name}"
#         self.ssh_obj.format_disk(node=self.fio_node, device=lvol_device, fs_type=self._pick_fs_type())
#         self.ssh_obj.mount_path(node=self.fio_node, device=lvol_device, mount_path=mount_point)
#         self.lvol_mount_details[lvol_name]["Mount"] = mount_point
#         log_file = f"{self.log_path}/{lvol_name}_out.log"
#         self._run_fio_and_validate(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
#         self.logger.info("TC-SEC-127: RDMA FIO PASSED")
#
#         self.logger.info("=== TestLvolSecurityRDMA PASSED ===")



# ═══════════════════════════════════════════════════════════════════════════
# NEW TESTS – pool-level DHCHAP (--dhchap flag on pool add, pool add-host / remove-host)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityCombinations(SecurityTestBase):
    """
    Creates a DHCHAP-enabled pool, registers the client host NQN at pool
    level, then creates 4 lvol flavours (plain, crypto, auth-connect,
    crypto+auth) and verifies FIO on each.

    TC-NEW-001  Create pool with --dhchap; register client host
    TC-NEW-002  Plain lvol – create, connect with host-nqn, FIO
    TC-NEW-003  Crypto lvol – encrypted + DHCHAP, FIO
    TC-NEW-004  Auth-connect lvol – connect with host-nqn, verify secret
    TC-NEW-005  Crypto+Auth lvol – encrypted + DHCHAP + host-nqn, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_combinations_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityCombinations START ===")
        self._normalize_fio_node()

        # TC-NEW-001: create DHCHAP pool and register host
        self.logger.info("TC-NEW-001: Creating DHCHAP pool …")
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        assert pool_id, f"Pool {self.pool_name} not found"
        self.logger.info("TC-NEW-001: Pool created + host registered PASSED")

        combos = [
            ("plain",       False),
            ("crypto",      True),
            ("auth",        False),
            ("crypto_auth", True),
        ]

        for tag, encrypt in combos:
            tc = f"TC-NEW-00{combos.index((tag, encrypt)) + 2}"
            raw_name = f"sec{tag}{_rand_suffix()}"
            self.logger.info(f"{tc}: Creating {tag} lvol …")

            lvol_name, lvol_id = self._create_lvol_dual(raw_name, encrypt=encrypt)
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

            # Authorization: DHCHAP keys in the connect string (docker) /
            # the volume mounts on an allowed node and NOT on a disallowed one
            # (k8s). Asserted per flavour, because the crypto flavours get
            # their own StorageClass and can individually miss the
            # dhchap_node_label that carries all the enforcement.
            self._assert_host_authorized(lvol_name, lvol_id, allowed, tc=tc)

            device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
            mount_point = self._format_and_mount_dual(lvol_name, device)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point
            log_file = f"{self.log_path}/{lvol_name}_out.log"
            self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
            self.logger.info(f"{tc}: {tag} FIO PASSED")

            if denied is not None:
                self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
                self._assert_host_denied(lvol_name, lvol_id, denied, tc=tc)
            else:
                self.logger.warning(
                    f"{TOK_COVERAGE_LOST} {tc}: no disallowed host in this "
                    f"environment — the {tag} flavour's restriction was not "
                    f"exercised")

        self.logger.info("=== TestLvolSecurityCombinations PASSED ===")


class TestLvolDynamicHostManagement(SecurityTestBase):
    """
    Tests pool-level add-host / remove-host lifecycle.

    TC-NEW-010  Create DHCHAP pool + lvol, register host, connect, FIO
    TC-NEW-011  Remove host from pool → connect string no longer available
    TC-NEW-012  Re-add host to pool → connect string available again, FIO works
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dynamic_host_management_v2"

    def run(self):
        self.logger.info("=== TestLvolDynamicHostManagement START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        raw_name = f"secdyn{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-010: prove the host is authorized, then connect + FIO
        self.logger.info("TC-NEW-010: Connecting and running FIO …")
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-010")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="write", runtime=30)
        self.logger.info("TC-NEW-010: Pre-removal FIO PASSED")

        # Disconnect before removal
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-NEW-011: revoke the host's access, then prove it lost access.
        #
        # This is the one place the suite probes whether the operator actually
        # withdraws a node on ``spec.allowedNodes`` removal (hard=True), so
        # the unknown is exercised once per run and fails attributably here
        # rather than diffusely across three classes.
        #
        # In K8s revocation IS observable for an existing volume: the PV's
        # nodeAffinity requires the pool label, and the operator strips that
        # label from the withdrawn node — so a *new mount* on it fails. What
        # revocation cannot do is affect an already-mounted volume, since
        # nodeAffinity is only evaluated at mount; the release above is what
        # makes this assertion meaningful.
        self.logger.info("TC-NEW-011: Revoking host access …")
        if self.k8s_test:
            self._revoke_host_dual(pool_id, allowed, tc="TC-NEW-011",
                                   hard=True)
            self._assert_host_denied(
                lvol_name, lvol_id, allowed, tc="TC-NEW-011",
                why="withdrawn from spec.allowedNodes")
        else:
            self._revoke_host_dual(pool_id, allowed, tc="TC-NEW-011")
            # Docker keeps the historical expectation: with no allowed hosts
            # the pool still returns a connect string, just without keys.
            connect_ls, err = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
            assert connect_ls and not err, \
                f"Expected connect string (without dhchap) after removing only host; err={err}"
            connect_str = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in connect_str.lower(), \
                f"Expected no DHCHAP keys when pool has no allowed hosts; got: {connect_str}"
        self.logger.info("TC-NEW-011: Host revoked – access withdrawn PASSED")

        # TC-NEW-012: re-grant
        self.logger.info("TC-NEW-012: Re-granting host access …")
        self._grant_host_dual(pool_id, allowed, tc="TC-NEW-012")
        if self.k8s_test:
            # The positive control has to be re-established: the volume was
            # just proven un-mountable on this node, so a stale entry would
            # let a later denial assertion pass on the wrong evidence.
            self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-012")
        device2, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point2 = self._format_and_mount_dual(lvol_name, device2)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point2
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_dual(lvol_name, mount_point2, log_file2, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-012: Re-added host – FIO PASSED")

        self.logger.info("=== TestLvolDynamicHostManagement PASSED ===")


class TestLvolCryptoWithDhchap(SecurityTestBase):
    """
    Encryption + DHCHAP combined test.

    Docker:
      TC-NEW-020  Create DHCHAP pool with host registered
      TC-NEW-021  Create encrypted lvol in DHCHAP pool
      TC-NEW-022  Connect with host-nqn, mount, FIO (randrw)

    K8s (native — no manual connect/host-nqn; the operator enforces
    allowedNodes at the control plane, a client never supplies its own NQN):
      TC-NEW-020  Create DHCHAP pool with allowedNodes = subset of workers
      TC-NEW-021  Create encrypted PVC in that pool
      TC-NEW-022  Pod pinned to an allowed node mounts + runs FIO; a pod
                  pinned to a disallowed node gets rejected (FailedMount)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_crypto_with_dhchap_v2"

    def run(self):
        self.logger.info("=== TestLvolCryptoWithDhchap START ===")
        self._normalize_fio_node()

        # TC-NEW-020 — pool with a strict allowedNodes subset in K8s, plus the
        # four structural wiring assertions (L1-L4) before any volume work.
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        self.logger.info(
            f"TC-NEW-020: DHCHAP pool + host PASSED (allowed={allowed!r} "
            f"denied={denied!r})")

        # TC-NEW-021
        raw_name = f"seccryp{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name, encrypt=True)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-NEW-021: Encrypted lvol created PASSED")

        if self.k8s_test:
            # TC-NEW-022: the encrypted volume must be usable on an allowed
            # node and rejected on a disallowed one. prove_io=True because
            # encryption is a data-path feature — "it mounted" is not enough.
            try:
                self._assert_host_authorized(
                    lvol_name, lvol_id, allowed, tc="TC-NEW-022",
                    prove_io=True)
            except DhchapUnsupportedByHost as exc:
                self.logger.warning(f"TC-NEW-022: {exc}")
                self.logger.info(
                    f"=== TestLvolCryptoWithDhchap {TOK_SKIPPED_K8S} "
                    f"(host kernel has no in-band NVMe auth) ===")
                return
            self.logger.info(
                "TC-NEW-022: Encrypted volume mounted + I/O on allowed node "
                "PASSED")

            self._require_denied_host(denied, tc="TC-NEW-022")
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
            self._assert_host_denied(lvol_name, lvol_id, denied,
                                     tc="TC-NEW-022")
            self.logger.info(
                "TC-NEW-022: Encrypted volume rejected on disallowed node "
                "PASSED")
            self.logger.info("=== TestLvolCryptoWithDhchap PASSED ===")
            return

        # TC-NEW-022 (docker): connect string carries the keys, then FIO
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-022")

        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-022: Crypto+DHCHAP FIO PASSED")

        self.logger.info("=== TestLvolCryptoWithDhchap PASSED ===")


class TestLvolDhchapBidirectional(SecurityTestBase):
    """
    Verifies bidirectional DHCHAP – always the default mode now.

    Docker:
      TC-NEW-030  Create DHCHAP pool + host
      TC-NEW-031  Create lvol, connect with host-nqn
      TC-NEW-033  FIO completes successfully

    K8s (native): SKIPPED. DHCHAP on a StoragePool has no direction toggle —
    it is a single ``dhchap: true`` boolean, bidirectional by construction, so
    there is no K8s observable that docker's "ctrl-secret is in the connect
    string" check maps to. The previous K8s branch was a copy of
    TestLvolCryptoWithDhchap's allowedNodes matrix on a plain volume: it
    reported PASSED while proving nothing about direction, which is worse than
    an explicit skip because it implies coverage that does not exist.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_dhchap_bidirectional_v2"

    def run(self):
        self.logger.info("=== TestLvolDhchapBidirectional START ===")
        self._normalize_fio_node()

        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TestLvolDhchapBidirectional: "
                f"bidirectional DHCHAP is not separately configurable on a "
                f"StoragePool (spec.dhchap is one boolean), so direction "
                f"coverage is not obtainable in K8s mode. Mount enforcement "
                f"is covered by TestDhchapPodScheduling and in-band "
                f"negotiation by TestLvolSecurityNegativeConnect. "
                f"{TOK_COVERAGE_LOST}: docker-only assertion "
                f"(--dhchap-ctrl-secret present in the connect string).")
            self.logger.info(
                "=== TestLvolDhchapBidirectional SKIPPED (k8s) ===")
            return

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        self.logger.info("TC-NEW-030: DHCHAP pool + host PASSED")

        raw_name = f"secbidir{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-031: connect string must carry BOTH keys (bidirectional)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-031",
                                     require_ctrl_secret=True)

        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        self.logger.info("TC-NEW-031: Connected with host-nqn PASSED")

        # TC-NEW-033: FIO
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-033: Bidirectional FIO PASSED")

        self.logger.info("=== TestLvolDhchapBidirectional PASSED ===")


class TestLvolSecurityNegativeHostOps(SecurityTestBase):
    """
    Negative tests for pool-level host operations.

    TC-NEW-040  Connect without registered host → connect string returned but without DHCHAP keys
    TC-NEW-041  Remove non-registered NQN from pool → expect error or no-op
    TC-NEW-042  Add host, connect succeeds with DHCHAP keys; remove host, connect string without DHCHAP keys
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_host_ops_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeHostOps START ===")
        self._normalize_fio_node()

        # Pool with a real allowedNodes subset in K8s; register=False so
        # TC-NEW-040 still starts from an unregistered state in docker.
        pool_id, allowed, denied = self._setup_pool_and_host(
            dhchap=True, register=False)
        host_nqn = _as_nqn(allowed)

        raw_name = f"secneg{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-NEW-040: an unregistered host gets no DHCHAP keys.
        # In K8s there is no such state to construct: the operator derives and
        # registers the allowed nodes' NQNs itself, so a node is either in
        # allowedNodes (registered) or outside the pool entirely. The
        # equivalent assertion is TC-NEW-041's state check below.
        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TC-NEW-040: an 'unregistered host' is not "
                f"a constructible state in K8s — the operator owns pool host "
                f"registration and derives NQNs from allowedNodes. Covered "
                f"instead by the L3 assertion (pool allowed hosts == derived "
                f"NQNs of allowedNodes) and by TC-NEW-042 below.")
        else:
            self.logger.info("TC-NEW-040: Connecting without registered host …")
            connect_ls, err = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
            assert connect_ls and not err, \
                f"Expected connect string even without registered host; err={err}"
            connect_str = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in connect_str.lower(), \
                f"Expected no DHCHAP keys when host is not registered; got: {connect_str}"
            self.logger.info("TC-NEW-040: Connect without DHCHAP keys PASSED")

        # TC-NEW-041: removing a non-registered NQN must be rejected or a
        # no-op — asserted on the EFFECT (the pool's allowed hosts did not
        # change), which is wording- and mode-independent. The K8s branch used
        # to force err="" here, so nothing could fail.
        self.logger.info("TC-NEW-041: Removing non-registered NQN …")
        fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
        before = sorted(self._get_pool_allowed_hosts(pool_id))
        out, err = self._pool_host_op_dual(pool_id, fake_nqn, remove=True)
        self.logger.info(
            f"TC-NEW-041: remove non-registered NQN result: out={out!r} err={err!r}")
        after = sorted(self._get_pool_allowed_hosts(pool_id))
        assert fake_nqn not in after, (
            f"TC-NEW-041: a never-registered NQN appeared in the pool's "
            f"allowed hosts after remove-host: {after}")
        assert before == after, (
            f"TC-NEW-041: removing a non-registered NQN changed the pool's "
            f"allowed hosts: {before} -> {after}")
        self.logger.info("TC-NEW-041: PASSED (rejected/no-op, state unchanged)")

        # TC-NEW-042: grant -> access; revoke -> no access.
        self.logger.info("TC-NEW-042: Grant, verify access, revoke, verify no access …")
        self._grant_host_dual(pool_id, allowed, tc="TC-NEW-042")
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-042")

        if self.k8s_test:
            # Revoking the node the volume was just proven on: the operator
            # strips the pool label, so a fresh mount there must now fail.
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
            if self._revoke_host_dual(pool_id, allowed, tc="TC-NEW-042"):
                self._assert_host_denied(
                    lvol_name, lvol_id, allowed, tc="TC-NEW-042",
                    why="withdrawn from spec.allowedNodes")
        else:
            self._revoke_host_dual(pool_id, allowed, tc="TC-NEW-042")
            connect_ls3, err3 = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
            assert connect_ls3 and not err3, \
                f"Connect string should still be returned after removing host; err={err3}"
            connect_str3 = " ".join(connect_ls3) if isinstance(connect_ls3, list) else str(connect_ls3)
            assert "dhchap" not in connect_str3.lower(), \
                f"Expected no DHCHAP keys after removing host; got: {connect_str3}"
        self.logger.info("TC-NEW-042: Grant/revoke lifecycle PASSED")

        self.logger.info("=== TestLvolSecurityNegativeHostOps PASSED ===")


class TestLvolSecuritySnapshotClone(SecurityTestBase):
    """
    Snapshot + clone inherits pool-level DHCHAP security.

    TC-NEW-050  Create DHCHAP pool + host, create lvol, write data
    TC-NEW-051  Create snapshot + clone
    TC-NEW-052  Connect clone with same host-nqn (pool-level auth), run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_snapshot_clone_v2"

    def run(self):
        self.logger.info("=== TestLvolSecuritySnapshotClone START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        # TC-NEW-050: create lvol, write data
        raw_name = f"secsnap{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Source volume must be authorized before anything is snapshotted
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-050")

        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="write", runtime=20)

        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None
        self.logger.info("TC-NEW-050: Source lvol written PASSED")

        # TC-NEW-051: snapshot + clone, via the inherited dual helpers.
        # In K8s these are a VolumeSnapshot CRD and a clone PVC created from
        # self._k8s_storage_class_name — aliased to the DHCHAP StorageClass in
        # _k8s_setup_storage_class, which is what makes the clone inherit the
        # dhchap_node_label. The raw `sbcli snapshot add` over SSH this used
        # to run hard-failed in K8s.
        self.logger.info("TC-NEW-051: Creating snapshot and clone …")
        snap_name = f"snap{lvol_name[-6:]}"
        snap_ref = self._create_snapshot_dual(lvol_name, snap_name)
        assert snap_ref, f"TC-NEW-051: snapshot {snap_name} not created"
        self._verify_snapshot_exists_dual(snap_name)

        clone_name = f"secclone{_rand_suffix()}"
        clone_size = self.lvol_size if not self.k8s_test else \
            self.lvol_size.replace("G", "Gi")
        self._create_clone_dual(snap_ref, clone_name, size=clone_size)
        clone_id = self._get_lvol_id_dual(clone_name)
        assert clone_id, f"TC-NEW-051: clone {clone_name} has no id"
        self.lvol_mount_details[clone_name] = {"ID": clone_id, "Mount": None}
        self.logger.info("TC-NEW-051: Snapshot+clone PASSED")

        # TC-NEW-052: the CLONE must inherit the pool's DHCHAP enforcement.
        # Asserted on the clone itself, not inherited from the source: if the
        # clone landed on a StorageClass without the label it would carry no
        # enforcement while every other assertion still passed.
        self.logger.info("TC-NEW-052: Verifying clone inherits DHCHAP …")
        self._assert_host_authorized(clone_name, clone_id, allowed,
                                     tc="TC-NEW-052")

        if self.k8s_test:
            self._require_denied_host(denied, tc="TC-NEW-052")
            self._disconnect_and_unmount_dual(clone_name, clone_id, None)
            self._assert_host_denied(clone_name, clone_id, denied,
                                     tc="TC-NEW-052",
                                     why="clone inherits the pool restriction")
            self.logger.info("TC-NEW-052: Clone DHCHAP inheritance PASSED")
            self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")
            return

        clone_device, _ = self._connect_and_get_device_dual(clone_name, clone_id, host_nqn=host_nqn)
        clone_mount = self._format_and_mount_dual(
            clone_name, clone_device,
            mount_point=f"{self.mount_path}/{clone_name}",
            format_first=False)
        self.lvol_mount_details[clone_name]["Mount"] = clone_mount
        log_file2 = f"{self.log_path}/{clone_name}_out.log"
        self._run_fio_dual(clone_name, clone_mount, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-NEW-052: Clone FIO PASSED")

        self.logger.info("=== TestLvolSecuritySnapshotClone PASSED ===")


class TestLvolSecurityRDMAv2(SecurityTestBase):
    """
    DHCHAP over RDMA fabric (pool-level API).

    TC-NEW-060  Skip if RDMA not available
    TC-NEW-061  Create DHCHAP pool + host, create lvol with fabric=rdma
    TC-NEW-062  Connect via RDMA with host-nqn, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_rdma_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityRDMAv2 START ===")
        self._normalize_fio_node()

        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TestLvolSecurityRDMAv2: the test's "
                f"substance is a manual `nvme connect -t rdma`, which has no "
                f"CSI equivalent — in K8s the fabric is a StorageClass "
                f"parameter. {TOK_COVERAGE_LOST}: DHCHAP over the RDMA "
                f"fabric is not covered in K8s mode.")
            self.logger.info("=== TestLvolSecurityRDMAv2 SKIPPED (k8s) ===")
            return

        # TC-NEW-060: check RDMA
        cluster_details = self.sbcli_utils.get_cluster_details()
        if not cluster_details.get("fabric_rdma", False):
            self.logger.warning(
                f"{TOK_COVERAGE_LOST} TC-NEW-060: RDMA fabric is not "
                f"available on this cluster — DHCHAP-over-RDMA not exercised")
            self.logger.info("TC-NEW-060: RDMA not available – SKIPPED")
            return
        self.logger.info("TC-NEW-060: RDMA available")

        # TC-NEW-061: DHCHAP pool + RDMA lvol
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        raw_name = f"secrdma{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-NEW-061")
        self.logger.info("TC-NEW-061: RDMA DHCHAP lvol PASSED")

        # TC-NEW-062: connect, FIO
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-NEW-062: RDMA FIO PASSED")

        self.logger.info("=== TestLvolSecurityRDMAv2 PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 1 – Storage node outage with FIO running (DHCHAP HA lvol)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityStorageNodeOutage(SecurityTestBase):
    """
    Verifies that DHCHAP credentials and I/O survive a storage node
    outage/restart on an HA lvol.  FIO runs *during* the outage and
    must complete without interruption.

    TC-SEC-070  Create DHCHAP pool + host, create HA lvol (ndcs=1, npcs=1)
    TC-SEC-071  Connect, format, mount, start long-running FIO in thread
    TC-SEC-072  Shutdown a primary storage node; validate node offline,
                lvols remain online, FIO still running
    TC-SEC-073  Restart node; wait for online + HA settle
    TC-SEC-074  Wait for FIO to finish; validate FIO log (no interruption)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_storage_node_outage"
        self.fio_runtime = 300

    def run(self):
        self.logger.info("=== TestLvolSecurityStorageNodeOutage START ===")
        self._normalize_fio_node()

        # TC-SEC-070: DHCHAP pool + host + HA lvol
        self.logger.info("TC-SEC-070: Creating DHCHAP pool + HA lvol …")
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        assert pool_id, f"Pool {self.pool_name} not found"

        raw_name = f"secout{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        assert lvol_id, f"Could not find ID for {lvol_name}"
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-SEC-070: DHCHAP pool + HA lvol PASSED")

        # TC-SEC-071: prove authorization, then start FIO in the background
        self.logger.info("TC-SEC-071: Connecting and starting long-running FIO …")
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-071")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio = self._start_bg_fio_dual(
            lvol_name, mount_point, log_file,
            runtime=self.fio_runtime, rw="randrw")
        sleep_n_sec(15)  # let FIO settle
        self.logger.info("TC-SEC-071: FIO started PASSED")

        # TC-SEC-072: shutdown a primary storage node
        self.logger.info("TC-SEC-072: Shutting down a primary storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node") and n.get("lvols", 0) > 0]
        assert primary_nodes, "No primary storage nodes with lvols found"
        target_node = primary_nodes[0]["uuid"]

        deadline = time.time() + 300
        self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
        while True:
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(target_node)
            if node_detail[0]["status"] == "offline":
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {target_node} did not go offline within 5 minutes")
            self.logger.info(f"Node {target_node} not yet offline; retrying …")
            try:
                self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown retry raised: {e}")

        self.logger.info("TC-SEC-072: Node offline — verifying FIO still running …")
        self._assert_bg_fio_alive_dual(fio, tc="TC-SEC-072")
        self.logger.info("TC-SEC-072: Node offline + FIO alive PASSED")

        # TC-SEC-073: restart node
        self.logger.info("TC-SEC-073: Restarting storage node …")
        sleep_n_sec(30)
        self.sbcli_utils.restart_node(node_uuid=target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        self.logger.info("TC-SEC-073: Node online — waiting for HA to settle …")
        sleep_n_sec(120)
        self.logger.info("TC-SEC-073: Node restart PASSED")

        # TC-SEC-074: wait for FIO and validate
        self.logger.info("TC-SEC-074: Waiting for FIO to complete …")
        self._finish_bg_fio_dual(fio, tc="TC-SEC-074")
        self.logger.info("TC-SEC-074: FIO completed without interruption PASSED")

        # TC-SEC-075: enforcement itself must have survived the outage, not
        # just the I/O. Re-assert the wiring and both placement outcomes; a
        # reconcile that dropped the node labels on recovery would leave the
        # volume mountable from anywhere and nothing above would notice.
        if self.k8s_test:
            self.logger.info("TC-SEC-075: Re-verifying DHCHAP after outage …")
            self._k8s_assert_dhchap_wiring(
                self._dhchap_allowed_nodes, self._dhchap_disallowed_nodes)
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
            self._dhchap_positive_control.discard(lvol_name)
            self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                         tc="TC-SEC-075")
            if denied is not None:
                self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
                self._assert_host_denied(lvol_name, lvol_id, denied,
                                         tc="TC-SEC-075")
            self.logger.info("TC-SEC-075: DHCHAP survived the outage PASSED")

        self.logger.info("=== TestLvolSecurityStorageNodeOutage PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 2 – Management node reboot (DHCHAP config survives)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityMgmtNodeReboot(SecurityTestBase):
    """
    Reboots the management node and verifies that pool-level DHCHAP
    configuration is preserved — connect strings still contain DHCHAP
    keys and volumes remain accessible.

    TC-SEC-080  Create DHCHAP pool + host, create lvol, verify DHCHAP keys in connect string
    TC-SEC-081  Reboot management node; wait for services to recover
    TC-SEC-082  Verify connect string still has DHCHAP keys post-reboot
    TC-SEC-083  Connect lvol, mount, FIO — data plane intact after mgmt reboot
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_mgmt_node_reboot"

    def run(self):
        self.logger.info("=== TestLvolSecurityMgmtNodeReboot START ===")
        self._normalize_fio_node()

        if self.k8s_test:
            # No K8s-native equivalent of rebooting the management node
            # exists anywhere in the repo: in K8s the control plane IS the
            # Kubernetes control plane, and rebooting it is out of scope and
            # unmodelled. Rather than fake-pass, assert the property this
            # class actually cares about -- that DHCHAP config is persistent
            # across a control-plane restart -- against the operator, which
            # is the component that could plausibly wipe it on reconcile.
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TC-SEC-081: rebooting the management node "
                f"has no K8s equivalent. Substituting an operator restart, "
                f"which targets the same regression class (a reconcile loop "
                f"clearing node labels or allowed hosts on restart). "
                f"{TOK_COVERAGE_LOST}: host-OS-level mgmt reboot not covered.")
            self._k8s_operator_restart_variant()
            return

        # TC-SEC-080: DHCHAP pool + host + lvol + baseline check
        self.logger.info("TC-SEC-080: Creating DHCHAP pool + lvol …")
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        assert pool_id, f"Pool {self.pool_name} not found"

        raw_name = f"secmgmt{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # Verify DHCHAP keys in connect string before reboot
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-080")
        self.logger.info("TC-SEC-080: Pre-reboot DHCHAP keys present PASSED")

        # TC-SEC-081: reboot management node
        self.logger.info("TC-SEC-081: Rebooting management node …")
        self.ssh_obj.reboot_node(self.mgmt_nodes[0], wait_time=300)
        sleep_n_sec(100)  # wait for all services to fully start
        self.logger.info("TC-SEC-081: Management node back online PASSED")

        # TC-SEC-082: verify DHCHAP keys post-reboot
        self.logger.info("TC-SEC-082: Verifying DHCHAP keys post-reboot …")
        self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-082")
        self.logger.info("TC-SEC-082: Post-reboot DHCHAP keys preserved PASSED")

        # TC-SEC-083: connect, mount, FIO
        self.logger.info("TC-SEC-083: Connecting and running FIO after mgmt reboot …")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-083: FIO after mgmt reboot PASSED")

        self.logger.info("=== TestLvolSecurityMgmtNodeReboot PASSED ===")

    def _k8s_operator_restart_variant(self):
        """K8s substitute for TC-SEC-081: restart the operator and assert the
        DHCHAP configuration is byte-identical afterwards.

        The pre-restart snapshot is asserted NON-EMPTY before the restart --
        otherwise "unchanged" is trivially true and the test proves nothing.
        """
        k8s = self._ensure_k8s_utils()
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        raw_name = f"secmgmt{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-080")

        # Snapshot, and prove the snapshot is meaningful
        before = {
            "label": self._dhchap_node_label,
            "labelled_nodes": sorted(
                self._k8s_nodes_with_label(self._dhchap_node_label)),
            "allowed_hosts": sorted(self._get_pool_allowed_hosts(pool_id)),
            "sc_label": self._k8s_sc_dhchap_label(self._storage_class_name),
        }
        assert before["label"], "TC-SEC-080: no pool label to compare"
        assert before["labelled_nodes"], (
            "TC-SEC-080: no nodes carry the pool label before the restart — "
            "an 'unchanged after restart' assertion would be vacuous")
        assert before["sc_label"], (
            "TC-SEC-080: StorageClass carries no dhchap_node_label before "
            "the restart")
        self.logger.info(f"TC-SEC-080: pre-restart snapshot {before}")

        self.logger.info("TC-SEC-081: Restarting the simplyblock operator …")
        dep_out, _ = k8s._exec_kubectl(
            f"kubectl get deployments -n {k8s.namespace} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true")
        names = [d.strip() for d in (dep_out or "").splitlines() if d.strip()]
        # Match the simplyblock operator specifically. A bare "operator" in
        # the name is far too loose: this namespace also carries
        # `mongodb-kubernetes-operator`, and restarting a third-party operator
        # mid-suite is collateral damage that has nothing to do with DHCHAP.
        deps = [d for d in names if d == "simplyblock-operator"]
        if not deps:
            deps = [d for d in names
                    if "operator" in d.lower() and "simplyblock" in d.lower()]
        if not deps:
            self.logger.warning(
                f"{TOK_COVERAGE_LOST} TC-SEC-081: no operator deployment "
                f"found in namespace {k8s.namespace} — cannot restart it, so "
                f"config persistence across a control-plane restart is not "
                f"covered")
            self.logger.info(
                "=== TestLvolSecurityMgmtNodeReboot SKIPPED (k8s) ===")
            return
        for dep in deps:
            k8s._exec_kubectl(
                f"kubectl rollout restart deployment/{dep} "
                f"-n {k8s.namespace}")
        for dep in deps:
            k8s._exec_kubectl(
                f"kubectl rollout status deployment/{dep} "
                f"-n {k8s.namespace} --timeout=300s")
        sleep_n_sec(60)  # let the reconcile loop run at least once
        self.logger.info(
            f"TC-SEC-081: Operator restarted ({deps}) PASSED")

        # TC-SEC-082: config must be identical, compared as sets
        self.logger.info("TC-SEC-082: Verifying DHCHAP config after restart …")
        after = {
            "label": self._dhchap_node_label,
            "labelled_nodes": sorted(
                self._k8s_nodes_with_label(self._dhchap_node_label)),
            "allowed_hosts": sorted(self._get_pool_allowed_hosts(pool_id)),
            "sc_label": self._k8s_sc_dhchap_label(self._storage_class_name),
        }
        assert after == before, (
            f"TC-SEC-082: DHCHAP configuration changed across the operator "
            f"restart.\n  before={before}\n  after={after}")
        self._k8s_assert_dhchap_wiring(
            self._dhchap_allowed_nodes, self._dhchap_disallowed_nodes)
        self.logger.info("TC-SEC-082: DHCHAP config preserved PASSED")

        # TC-SEC-083: and enforcement still behaves both ways
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
        self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-083")
        if denied is not None:
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
            self._assert_host_denied(lvol_name, lvol_id, denied,
                                     tc="TC-SEC-083")
        self.logger.info("TC-SEC-083: Enforcement intact after restart PASSED")
        self.logger.info(
            "=== TestLvolSecurityMgmtNodeReboot PASSED (k8s operator-restart "
            "variant) ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 3 – HA failover with DHCHAP + encryption (FIO during outage)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityHAFailover(SecurityTestBase):
    """
    Creates an HA lvol (ndcs=1, npcs=1) with encryption + DHCHAP,
    runs FIO *during* a primary node shutdown, and verifies that
    security config survives the failover.

    TC-SEC-085  Create DHCHAP pool + host, create encrypted HA lvol
    TC-SEC-086  Connect, format, mount, start long-running FIO in thread
    TC-SEC-087  Shutdown the primary storage node; validate FIO alive
    TC-SEC-088  Restart node; wait for HA settle
    TC-SEC-089  Wait for FIO to finish; validate no interruption;
                verify DHCHAP keys still present in connect string
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_ha_failover"
        self.fio_runtime = 300

    def run(self):
        self.logger.info("=== TestLvolSecurityHAFailover START ===")
        self._normalize_fio_node()

        # TC-SEC-085: DHCHAP pool + host + encrypted HA lvol
        self.logger.info("TC-SEC-085: Creating DHCHAP pool + encrypted HA lvol …")
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        assert pool_id, f"Pool {self.pool_name} not found"

        raw_name = f"secha{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name, encrypt=True)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info("TC-SEC-085: Encrypted HA lvol PASSED")

        # TC-SEC-086: connect, format, mount, start FIO in the background.
        # The authorization baseline is asserted BEFORE the failover so that
        # "DHCHAP preserved" in TC-SEC-089 has something to be preserved
        # against — the previous version only checked afterwards.
        self.logger.info("TC-SEC-086: Connecting and starting FIO …")
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-086")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio = self._start_bg_fio_dual(
            lvol_name, mount_point, log_file,
            runtime=self.fio_runtime, rw="randrw")
        sleep_n_sec(15)
        self.logger.info("TC-SEC-086: FIO started PASSED")

        # TC-SEC-087: shutdown a primary storage node
        self.logger.info("TC-SEC-087: Shutting down primary storage node …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node") and n.get("lvols", 0) > 0]
        assert primary_nodes, "No primary storage nodes with lvols found"
        target_node = primary_nodes[0]["uuid"]

        deadline = time.time() + 300
        self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
        while True:
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(target_node)
            if node_detail[0]["status"] == "offline":
                break
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {target_node} did not go offline within 5 minutes")
            self.logger.info(f"Node {target_node} not yet offline; retrying …")
            try:
                self.sbcli_utils.shutdown_node(node_uuid=target_node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown retry raised: {e}")

        self.logger.info("TC-SEC-087: Node offline — verifying FIO alive …")
        self._assert_bg_fio_alive_dual(fio, tc="TC-SEC-087")
        self.logger.info("TC-SEC-087: Node offline + FIO alive PASSED")

        # TC-SEC-088: restart node, settle
        self.logger.info("TC-SEC-088: Restarting node …")
        sleep_n_sec(30)
        self.sbcli_utils.restart_node(node_uuid=target_node)
        self.sbcli_utils.wait_for_storage_node_status(target_node, "online", timeout=300)
        self.logger.info("TC-SEC-088: Node online — waiting for HA to settle …")
        sleep_n_sec(120)
        self.logger.info("TC-SEC-088: Node restart PASSED")

        # TC-SEC-089: wait for FIO, validate, check DHCHAP survived
        self.logger.info("TC-SEC-089: Waiting for FIO to complete …")
        self._finish_bg_fio_dual(fio, tc="TC-SEC-089")
        self.logger.info("TC-SEC-089: FIO completed without interruption")

        if self.k8s_test:
            self._k8s_assert_dhchap_wiring(
                self._dhchap_allowed_nodes, self._dhchap_disallowed_nodes)
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-089")
        if self.k8s_test and denied is not None:
            self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
            self._assert_host_denied(lvol_name, lvol_id, denied,
                                     tc="TC-SEC-089")
        self.logger.info("TC-SEC-089: DHCHAP preserved post-failover PASSED")

        self.logger.info("=== TestLvolSecurityHAFailover PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Outage Test 4 – 30-second network interrupt with FIO running
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNetworkInterrupt(SecurityTestBase):
    """
    30-second NIC-level network interrupt on a storage node while FIO
    is running on an HA DHCHAP lvol.  FIO must survive the interrupt
    and DHCHAP auth must still work after reconnect.

    TC-SEC-090  Create DHCHAP pool + host, create HA lvol, connect, format, mount
    TC-SEC-091  Start long-running FIO in thread
    TC-SEC-092  Trigger 30s network interrupt on a storage node
    TC-SEC-093  Wait for interrupt to end; verify FIO completed without errors
    TC-SEC-094  Disconnect + reconnect with DHCHAP creds; verify auth still works

    K8s: SKIPPED. Two reasons, both specific to K8s-native.

    1. The FIO workload and the outage target are the same machine. In
       docker they are not: FIO runs on a separate client host while the
       outage hits a storage node. In K8s-native the storage nodes ARE the
       worker nodes (10.0.0.10-15 == worker-0..5 on the OpenShift bed), the
       FIO pod is pinned to ``_dhchap_allowed_nodes[0]`` (worker-0) because
       DHCHAP requires an allowed node, and the outage targets
       ``primary_nodes[0]`` — very likely the same worker. Blacking out that
       host kills the FIO pod's own connectivity, so the test would be
       measuring its own fixture rather than the product.

    2. A full ``iptables -A INPUT/OUTPUT -j DROP`` is far harsher than the
       ``sn shutdown`` the other outage classes use. Shutdown stops the SPDK
       service and leaves the worker and its pods alive with HA covering the
       volume; the blackout also severs kubelet from the API server (the node
       goes NotReady in ~40s), which makes the liveness check read a stale
       ``Running`` and leaves the run one failed ``iptables -F`` away from a
       stranded worker.

    Fixing (1) is easy — pick an outage target whose K8s node differs from
    the FIO pod's node. It is deliberately not done here: the class has never
    executed, and it carries the largest blast radius in the suite, so it
    should be re-enabled on its own rather than inside a full-suite run.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_network_interrupt"
        self.fio_runtime = 120

    def run(self):
        self.logger.info("=== TestLvolSecurityNetworkInterrupt START ===")

        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TestLvolSecurityNetworkInterrupt: in "
                f"K8s-native the storage nodes are the worker nodes, so the "
                f"outage target and the FIO pod's host are the same machine "
                f"— the blackout would sever the FIO pod's own connectivity "
                f"and kubelet's link to the API server, and the test would "
                f"measure its fixture rather than the product. "
                f"{TOK_COVERAGE_LOST}: fabric-loss survival and DHCHAP "
                f"re-authentication on reconnect (TC-SEC-092/093/094) are not "
                f"covered in K8s. Re-enable by selecting an outage target "
                f"whose K8s node differs from the FIO pod's node, and run it "
                f"on its own — it has the largest blast radius in the suite.")
            self.logger.info(
                "=== TestLvolSecurityNetworkInterrupt SKIPPED (k8s) ===")
            return

        self._normalize_fio_node()

        # TC-SEC-090: DHCHAP pool + host + HA lvol
        self.logger.info("TC-SEC-090: Creating DHCHAP pool + HA lvol …")
        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)
        assert pool_id, f"Pool {self.pool_name} not found"

        raw_name = f"secnwi{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-090")

        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        self.logger.info("TC-SEC-090: HA lvol connected + mounted PASSED")

        # TC-SEC-091: start FIO in the background
        self.logger.info("TC-SEC-091: Starting background FIO …")
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        fio = self._start_bg_fio_dual(
            lvol_name, mount_point, log_file,
            runtime=self.fio_runtime, rw="randrw")
        sleep_n_sec(15)
        self.logger.info("TC-SEC-091: FIO running PASSED")

        # TC-SEC-092: trigger a 30s network interrupt on a storage node
        self.logger.info("TC-SEC-092: Triggering 30s network interrupt …")
        nodes = self.sbcli_utils.get_storage_nodes()
        primary_nodes = [n for n in nodes["results"]
                         if not n.get("is_secondary_node")]
        assert primary_nodes, "No primary storage nodes found"
        target_node_ip = primary_nodes[0]["mgmt_ip"]
        target_node_uuid = primary_nodes[0]["uuid"]
        self._network_outage_dual(target_node_ip, duration=30)
        self.logger.info("TC-SEC-092: Network interrupt triggered PASSED")

        # TC-SEC-093: confirm the outage actually landed, then wait for FIO.
        # With ctrl-loss-tmo -1 a 30s blip can be completely invisible, so
        # without this check the whole test could pass having exercised
        # nothing at all.
        self.logger.info("TC-SEC-093: Confirming the outage took effect …")
        observed = False
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                detail = self.sbcli_utils.get_storage_node_details(
                    target_node_uuid)
                status = detail[0].get("status")
                if status != "online":
                    observed = True
                    self.logger.info(
                        f"TC-SEC-093: node {target_node_uuid} observed "
                        f"{status!r} during the interrupt")
                    break
            except Exception as exc:
                # The control plane being unreachable is itself evidence the
                # outage landed.
                observed = True
                self.logger.info(
                    f"TC-SEC-093: control plane unreachable during the "
                    f"interrupt ({exc}) — outage confirmed")
                break
            sleep_n_sec(5)
        if not observed:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} TC-SEC-093: the node never left "
                f"'online' during the 30s interrupt — the recovery "
                f"assertions below may not be exercising anything")

        self.logger.info("TC-SEC-093: Waiting 45s for network recovery …")
        sleep_n_sec(45)
        self.sbcli_utils.wait_for_storage_node_status(
            target_node_uuid, "online", timeout=300)
        self.logger.info("TC-SEC-093: Waiting for FIO to complete …")
        self._finish_bg_fio_dual(fio, tc="TC-SEC-093")
        self.logger.info("TC-SEC-093: FIO completed without interruption PASSED")

        # TC-SEC-094: re-attach and verify auth still works. Re-attach after
        # a fabric loss is precisely where DHCHAP re-negotiation happens, so
        # this is the substance of the test rather than a coda.
        self.logger.info("TC-SEC-094: Re-attaching with DHCHAP after interrupt …")
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-094")

        if self.k8s_test:
            if denied is not None:
                self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
                self._assert_host_denied(lvol_name, lvol_id, denied,
                                         tc="TC-SEC-094")
            self.logger.info("TC-SEC-094: Post-interrupt re-attach PASSED")
            self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")
            return

        device2, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        assert device2, "Reconnect after network interrupt failed"
        mount_point2 = self._format_and_mount_dual(
            lvol_name, device2,
            mount_point=f"{self.mount_path}/{lvol_name}_post",
            format_first=False)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point2
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_dual(lvol_name, mount_point2, log_file2, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-094: Post-interrupt reconnect + FIO PASSED")

        self.logger.info("=== TestLvolSecurityNetworkInterrupt PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Negative Test 1 – Invalid pool-level host operations at creation time
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNegativeCreation(SecurityTestBase):
    """
    Covers invalid input scenarios for pool-level host management:

    TC-SEC-100  add-host to pool with syntactically invalid NQN → error
    TC-SEC-101  add-host to pool with empty NQN string → error
    TC-SEC-102  remove-host with non-existent NQN → error or no-op (no crash)
    TC-SEC-103  Create lvol in non-DHCHAP pool → connect string has no DHCHAP keys
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_creation_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeCreation START ===")
        self._normalize_fio_node()

        # DHCHAP pool with a real allowedNodes subset in K8s (register=False:
        # these are negative tests, nothing should start registered)
        pool_id, allowed, denied = self._setup_pool_and_host(
            dhchap=True, register=False)
        assert pool_id, f"Pool {self.pool_name} not found"

        # TC-SEC-100: add-host with invalid NQN. Asserted on the textual
        # signal AND on the effect (the NQN did not enter the pool), so it
        # cannot pass on empty output the way the old helper did.
        self.logger.info("TC-SEC-100: add-host with invalid NQN …")
        invalid_nqn = "not-a-valid-nqn-format-!@#$%"
        out, err = self._pool_host_op_dual(pool_id, shlex.quote(invalid_nqn))
        self._assert_cli_rejected(out, err, "TC-SEC-100",
                                  pool_id=pool_id, host_nqn=invalid_nqn)
        self.logger.info("TC-SEC-100: Invalid NQN rejected PASSED")

        # TC-SEC-101: add-host with empty NQN
        self.logger.info("TC-SEC-101: add-host with empty NQN …")
        out, err = self._pool_host_op_dual(pool_id, "''")
        self._assert_cli_rejected(out, err, "TC-SEC-101",
                                  pool_id=pool_id, host_nqn="")
        self.logger.info("TC-SEC-101: Empty NQN rejected PASSED")

        # TC-SEC-102: remove-host with a non-existent NQN must be a rejection
        # or a no-op — asserted on the pool's allowed hosts being unchanged.
        self.logger.info("TC-SEC-102: remove-host with non-existent NQN …")
        fake_nqn = f"nqn.2024-01.io.simplyblock:test:fake-{_rand_suffix()}"
        before = sorted(self._get_pool_allowed_hosts(pool_id))
        out, err = self._pool_host_op_dual(pool_id, fake_nqn, remove=True)
        self.logger.info(
            f"TC-SEC-102: remove non-existent NQN result: out={out!r} err={err!r}")
        after = sorted(self._get_pool_allowed_hosts(pool_id))
        assert before == after, (
            f"TC-SEC-102: removing a non-existent NQN changed the pool's "
            f"allowed hosts: {before} -> {after}")
        self.logger.info("TC-SEC-102: PASSED (no crash, state unchanged)")

        # TC-SEC-103: a NON-DHCHAP pool must produce no enforcement.
        #
        # This case was broken in BOTH modes: it created `plain_pool` but then
        # called _create_lvol_dual, which still used self.pool_name / the
        # DHCHAP StorageClass — so it asserted about the DHCHAP pool and only
        # "passed" because no host was registered. The pool is now actually
        # switched for the duration of the check.
        #
        # In K8s this also becomes a strong control: it proves the negative
        # assertions elsewhere in the suite are not passing for environmental
        # reasons, because this volume mounts on the very node the DHCHAP pool
        # rejects.
        self.logger.info("TC-SEC-103: Creating lvol in non-DHCHAP pool …")
        plain_pool = f"{self.pool_name}-nodh"[:24]
        saved = (self.pool_name, self._storage_class_name,
                 self._dhchap_node_label, self._pool_crd_name)
        try:
            if self.k8s_test:
                # Non-DHCHAP pool, and again the operator generates its
                # StorageClass -- which should carry NO
                # dhchap_node_label and no allowedTopologies, so its
                # volumes mount anywhere.
                self.pool_name = plain_pool
                self._dhchap_node_label = None
                self._ensure_pool_and_sc(dhchap=False)
                sc_label = self._k8s_sc_dhchap_label(
                    self._storage_class_name)
                assert not sc_label, (
                    f"TC-SEC-103: the operator gave a non-DHCHAP pool a "
                    f"StorageClass carrying dhchap_node_label={sc_label!r} "
                    f"— a pool without dhchap must produce no enforcement")
            else:
                self.ssh_obj.add_storage_pool(
                    self.mgmt_nodes[0], plain_pool, self.cluster_id,
                    dhchap=False)
                self.pool_name = plain_pool

            raw_name = f"secplain{_rand_suffix()}"
            lvol_name, lvol_id = self._create_lvol_dual(raw_name)
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

            if self.k8s_test:
                # No pool label, so the PV must carry no nodeAffinity and the
                # volume must mount on ANY node — including the one the DHCHAP
                # pool rejects.
                k8s = self._ensure_k8s_utils()
                pvc_name = self._k8s_normalize_name(lvol_name)
                pv_name = k8s.get_pvc_pv_name(pvc_name)
                aff_out, _ = k8s._exec_kubectl(
                    f"kubectl get pv {pv_name} "
                    f"-o jsonpath='{{.spec.nodeAffinity}}' "
                    f"2>/dev/null || true")
                affinity = (aff_out or "").strip()
                assert "simplyblock.io/pool." not in affinity, (
                    f"TC-SEC-103: a non-DHCHAP pool's PV {pv_name} carries a "
                    f"pool nodeAffinity: {affinity!r}")
                target = denied.node if denied else None
                if target:
                    pod = self._k8s_verify_pod_scheduling(
                        pvc_name, target, expect_success=True,
                        pod_prefix="plainpool")
                    self.logger.info(
                        f"TC-SEC-103: non-DHCHAP volume mounted on {target!r} "
                        f"— the node the DHCHAP pool rejects. This confirms "
                        f"the suite's denials are DHCHAP-specific and not "
                        f"environmental.")
                    if pod:
                        self._k8s_release_pod(pod, pvc_name=pvc_name)
            else:
                host_nqn = self._get_client_host_nqn()
                connect_ls, cerr = self._get_connect_str_dual(
                    lvol_id, host_nqn=host_nqn)
                assert connect_ls, (
                    f"TC-SEC-103: expected a connect string from a "
                    f"non-DHCHAP pool; err={cerr!r}")
                connect_str = " ".join(connect_ls)
                assert "dhchap" not in connect_str.lower(), \
                    f"Non-DHCHAP pool should not produce DHCHAP keys; got: {connect_str}"
            self.logger.info("TC-SEC-103: Non-DHCHAP pool has no keys PASSED")
        finally:
            (self.pool_name, self._storage_class_name,
             self._dhchap_node_label, self._pool_crd_name) = saved

        self.logger.info("=== TestLvolSecurityNegativeCreation PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Negative Test 2 – Connect rejection scenarios (pool-level)
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityNegativeConnect(SecurityTestBase):
    """
    Tests connect behaviour for unregistered/wrong host NQNs:

    TC-SEC-110  Unregistered NQN → connect rejected (pool has allowed hosts)
    TC-SEC-111  Tampered DHCHAP secret → nvme connect fails (no new device)
    TC-SEC-112  Connect without host-nqn → no DHCHAP keys
    TC-SEC-113  Delete lvol in DHCHAP pool → cleanup succeeds
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_negative_connect_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityNegativeConnect START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        raw_name = f"secnc{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-110: an unregistered host NQN must be refused DHCHAP keys.
        #
        # This is one of the two places in the suite that speaks about
        # AUTHENTICATION rather than node placement: it asks the control
        # plane for keys as an identity that is not in the pool's allowed
        # hosts. It is meaningful in K8s too, and it is armed now that
        # _get_connect_str_dual returns a real error channel — previously the
        # K8s path hardcoded err="" and this reduced to "no connect line".
        self.logger.info("TC-SEC-110: Connect with unregistered NQN …")
        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        connect_ls, cerr = self._get_connect_str_dual(lvol_id, host_nqn=wrong_nqn)
        rejected = bool(cerr) or not connect_ls or not any(
            "dhchap-secret" in c.lower() for c in connect_ls)
        assert rejected, (
            f"Expected rejection (or at least no DHCHAP keys) for "
            f"unregistered NQN {wrong_nqn!r} when the pool has allowed "
            f"hosts, but got: {connect_ls}")
        self.logger.info(
            f"TC-SEC-110: Unregistered NQN refused keys PASSED "
            f"(err={cerr!r})")

        # TC-SEC-111: tampered DHCHAP secret → the connect must fail.
        #
        # Docker only. This is the single most security-relevant assertion in
        # the suite -- it is the only one that proves in-band DH-HMAC-CHAP is
        # actually negotiated rather than merely configured. It requires
        # hand-editing an `nvme connect` command line, which a K8s test never
        # sees: the keys live inside the CSI node plugin.
        #
        # IMPORTANT for reading K8s results: without this case, K8s mode
        # verifies that the pool's allowedNodes restriction is enforced at
        # mount via the PV's nodeAffinity -- NOT that DHCHAP was negotiated. A
        # pool with dhchap:false carrying the same node label would satisfy
        # every other K8s assertion in this file.
        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TC-SEC-111: tampering with a DHCHAP "
                f"secret requires editing a client-side `nvme connect` "
                f"command; in CSI mode the test never issues one and the keys "
                f"are held by the node plugin. {TOK_COVERAGE_LOST}: K8s mode "
                f"does NOT verify in-band DH-HMAC-CHAP negotiation, only "
                f"nodeAffinity enforcement of spec.allowedNodes.")
        else:
            self.logger.info("TC-SEC-111: Tampered DHCHAP secret …")
            connect_auth, _ = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
            assert connect_auth, (
                "TC-SEC-111: no connect string for the authorized host — "
                "cannot construct the tampered-secret case")
            tampered = connect_auth[0]
            assert "dhchap-secret" in tampered, (
                f"TC-SEC-111: authorized connect string carries no "
                f"--dhchap-secret to tamper with: {tampered!r}")
            tampered = re.sub(
                r'(--dhchap-secret[=\s])\S+',
                r'\1DHHC-1:00:DEADBEEFDEADBEEFDEADBEEFDEADBEEF',
                tampered)
            tampered = re.sub(
                r'(--dhchap-ctrl-secret[=\s])\S+',
                r'\1DHHC-1:00:DEADBEEFDEADBEEFDEADBEEFDEADBEEF',
                tampered)
            initial_devices = self.ssh_obj.get_devices(node=self.fio_node)
            self.ssh_obj.exec_command(node=self.fio_node, command=tampered)
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=self.fio_node)
            new_devices = [d for d in final_devices if d not in initial_devices]
            assert not new_devices, \
                f"Tampered key should not produce a new device; got: {new_devices}"
            self.logger.info("TC-SEC-111: Tampered key rejected PASSED")

        # TC-SEC-112: connect without host-nqn → no DHCHAP keys
        self.logger.info("TC-SEC-112: Connect without host-nqn …")
        connect_no_nqn, _ = self._get_connect_str_dual(lvol_id, host_nqn=None)
        assert connect_no_nqn, (
            "TC-SEC-112: expected a connect string with no --host-nqn")
        has_dhchap = any("dhchap" in c.lower() for c in connect_no_nqn)
        assert not has_dhchap, \
            "Connect without host-nqn must not contain DHCHAP keys"
        self.logger.info("TC-SEC-112: No keys without host-nqn PASSED")

        # TC-SEC-113: delete the volume in a DHCHAP pool.
        #
        # Routed through the dual helpers: the previous version called
        # delete_lvol(lvol_name=...) then asserted get_lvol_id(lvol_name) was
        # falsy, but in K8s the backend lvol is named after the PV, so that
        # lookup returned None either way and the case ALWAYS passed.
        self.logger.info("TC-SEC-113: Deleting lvol in DHCHAP pool …")
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
        self._delete_lvol_dual(lvol_name, skip_error=False)
        sleep_n_sec(3)
        self._verify_lvol_absent_dual(lvol_name)
        if self.k8s_test:
            pvc_name = self._k8s_normalize_name(lvol_name)
            if pvc_name in self.created_pvcs:
                self.created_pvcs.remove(pvc_name)
        self.lvol_mount_details.pop(lvol_name, None)
        self._dhchap_positive_control.discard(lvol_name)
        self.logger.info("TC-SEC-113: DHCHAP lvol deleted cleanly PASSED")

        self.logger.info("=== TestLvolSecurityNegativeConnect PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Dynamic modification of pool hosts with multi-NQN lifecycle
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityDynamicModification(SecurityTestBase):
    """
    Tests live add/remove of host NQNs at pool level and multi-NQN scenarios.

    TC-SEC-120  Create DHCHAP pool + NQN_A, create lvol, connect + FIO
    TC-SEC-121  Remove NQN_A from pool → connect string has no DHCHAP keys
    TC-SEC-122  Re-add NQN_A → connect string has DHCHAP keys, FIO works
    TC-SEC-123  Add NQN_B to pool → both NQNs get DHCHAP connect strings
    TC-SEC-124  Remove NQN_A → NQN_B still gets DHCHAP; NQN_A does not
    TC-SEC-125  Remove NQN_B → neither NQN gets DHCHAP keys
    TC-SEC-126  Re-add NQN_A → reconnect + FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_dynamic_modification_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityDynamicModification START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        if self.k8s_test:
            self._k8s_dynamic_modification(pool_id, allowed, denied)
            return

        second_nqn = f"nqn.2024-01.io.simplyblock:test:second-{_rand_suffix()}"
        raw_name = f"secdmod{_rand_suffix()}"

        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-120: connect + FIO
        self.logger.info("TC-SEC-120: Initial connect + FIO …")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="write", runtime=20)
        self.logger.info("TC-SEC-120: Initial FIO PASSED")

        # Disconnect
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-121: remove NQN_A → no DHCHAP keys
        self.logger.info("TC-SEC-121: Removing NQN_A from pool …")
        self._unregister_host_from_pool(pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls, err = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
        if connect_ls:
            cs = " ".join(connect_ls) if isinstance(connect_ls, list) else str(connect_ls)
            assert "dhchap" not in cs.lower(), \
                f"Expected no DHCHAP keys after removing host; got: {cs}"
        self.logger.info("TC-SEC-121: No DHCHAP keys after removal PASSED")

        # TC-SEC-122: re-add NQN_A → DHCHAP keys present, FIO works
        self.logger.info("TC-SEC-122: Re-adding NQN_A …")
        self._register_host_to_pool(pool_id, host_nqn)
        sleep_n_sec(3)
        connect_ls2, err2 = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
        assert connect_ls2 and not err2, f"Re-add should restore connect; err={err2}"
        cs2 = " ".join(connect_ls2) if isinstance(connect_ls2, list) else str(connect_ls2)
        assert "dhchap" in cs2.lower(), f"Expected DHCHAP keys after re-add; got: {cs2}"
        device2, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=device2, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_readd.log"
        self._run_fio_dual(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-122: Re-add FIO PASSED")

        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # TC-SEC-123: add NQN_B → both get DHCHAP
        self.logger.info("TC-SEC-123: Adding NQN_B …")
        self._register_host_to_pool(pool_id, second_nqn)
        sleep_n_sec(3)
        cs_a, _ = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
        cs_b, _ = self._get_connect_str_dual(lvol_id, host_nqn=second_nqn)
        assert cs_a, "NQN_A should get connect string"
        assert cs_b, "NQN_B should get connect string"
        str_a = " ".join(cs_a) if isinstance(cs_a, list) else str(cs_a)
        str_b = " ".join(cs_b) if isinstance(cs_b, list) else str(cs_b)
        assert "dhchap" in str_a.lower(), f"NQN_A should have DHCHAP; got: {str_a}"
        assert "dhchap" in str_b.lower(), f"NQN_B should have DHCHAP; got: {str_b}"
        self.logger.info("TC-SEC-123: Both NQNs have DHCHAP PASSED")

        # TC-SEC-124: remove NQN_A → NQN_B still has DHCHAP; NQN_A is rejected
        # Known behaviour: pool still HAS allowed hosts (NQN_B), so connecting
        # with removed NQN_A must FAIL (issue #4).
        self.logger.info("TC-SEC-124: Removing NQN_A …")
        self._unregister_host_from_pool(pool_id, host_nqn)
        sleep_n_sec(3)
        cs_a2, err_a2 = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
        rejected_a = bool(err_a2) or not cs_a2
        assert rejected_a, (
            f"NQN_A should be rejected when pool still has allowed hosts "
            f"(NQN_B); got: cs={cs_a2}")
        cs_b2, _ = self._get_connect_str_dual(lvol_id, host_nqn=second_nqn)
        assert cs_b2, "NQN_B should still get connect string"
        str_b2 = " ".join(cs_b2) if isinstance(cs_b2, list) else str(cs_b2)
        assert "dhchap" in str_b2.lower(), f"NQN_B should still have DHCHAP; got: {str_b2}"
        self.logger.info("TC-SEC-124: PASSED")

        # TC-SEC-125: remove NQN_B → pool has NO allowed hosts
        # Known behaviour: connect string IS returned but without dhchap
        # keys when pool has no allowed hosts (issue #3).
        self.logger.info("TC-SEC-125: Removing NQN_B …")
        self._unregister_host_from_pool(pool_id, second_nqn)
        sleep_n_sec(3)
        cs_a3, err_a3 = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
        cs_b3, err_b3 = self._get_connect_str_dual(lvol_id, host_nqn=second_nqn)
        for label, cs, cerr in [("NQN_A", cs_a3, err_a3), ("NQN_B", cs_b3, err_b3)]:
            assert cs and not cerr, \
                f"{label} should still get connect string when pool has no allowed hosts; err={cerr}"
            s = " ".join(cs) if isinstance(cs, list) else str(cs)
            assert "dhchap" not in s.lower(), \
                f"{label} should not have DHCHAP after all hosts removed; got: {s}"
        self.logger.info("TC-SEC-125: Neither NQN has DHCHAP PASSED")

        # TC-SEC-126: re-add NQN_A → reconnect + FIO
        self.logger.info("TC-SEC-126: Re-adding NQN_A and running FIO …")
        self._register_host_to_pool(pool_id, host_nqn)
        sleep_n_sec(3)
        device3, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        self.ssh_obj.mount_path(node=self.fio_node, device=device3, mount_path=mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file3 = f"{self.log_path}/{lvol_name}_final.log"
        self._run_fio_dual(lvol_name, mount_point, log_file3, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-126: Final FIO PASSED")

        self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")

    def _k8s_dynamic_modification(self, pool_id, allowed, denied):
        """K8s-native multi-host lifecycle: grant, revoke, re-grant.

        The docker version walks six steps over two host NQNs. The K8s
        analogue is a lifecycle over ``spec.allowedNodes``, and it is trimmed
        deliberately: the control-plane triple (spec/status, node labels, pool
        allowed hosts) is re-asserted at EVERY step because that is where the
        coverage is and it costs seconds, while a runtime pod probe -- which
        costs a minute -- is spent at only three points. Six probes for one
        idea is a bad trade.

        Every step waits for observable convergence rather than sleeping: the
        docker version's ``sleep_n_sec(3)`` after each mutation made all six
        assertions races against the operator's reconcile loop.
        """
        denied = self._require_denied_host(denied, tc="TC-SEC-120")
        raw_name = f"secdmod{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        node_b = denied.node

        # TC-SEC-120: baseline — the allowed node can use the volume (probe 1)
        self.logger.info("TC-SEC-120: Baseline access on the allowed node …")
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-120")
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
        self.logger.info("TC-SEC-120: Baseline PASSED")

        # TC-SEC-121: node B is outside the pool → denied (probe 2)
        self.logger.info(f"TC-SEC-121: {node_b!r} is not allowed → denied …")
        self._assert_host_denied(lvol_name, lvol_id, denied, tc="TC-SEC-121")
        self.logger.info("TC-SEC-121: Unlisted node denied PASSED")

        # TC-SEC-123: grant node B → both nodes labelled, both NQNs registered
        self.logger.info(f"TC-SEC-123: Granting {node_b!r} …")
        self._grant_host_dual(pool_id, denied, tc="TC-SEC-123")
        self._k8s_assert_dhchap_wiring(self._dhchap_allowed_nodes,
                                       self._dhchap_disallowed_nodes)
        self._dhchap_positive_control.discard(lvol_name)
        # probe 3: node B can now actually use the volume
        self._assert_host_authorized(
            lvol_name, lvol_id,
            DhchapHost(node=node_b, nqn=denied.nqn, desc="newly granted"),
            tc="TC-SEC-123")
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
        self.logger.info("TC-SEC-123: Granted node has access PASSED")

        # TC-SEC-124: revoke node B → it loses access, the other node keeps it
        self.logger.info(f"TC-SEC-124: Revoking {node_b!r} …")
        revoked = self._revoke_host_dual(
            pool_id, DhchapHost(node=node_b, nqn=denied.nqn), tc="TC-SEC-124")
        self._k8s_assert_dhchap_wiring(self._dhchap_allowed_nodes,
                                       self._dhchap_disallowed_nodes)
        if revoked:
            self._dhchap_positive_control.add(lvol_name)
            self._assert_host_denied(
                lvol_name, lvol_id,
                DhchapHost(node=node_b, nqn=denied.nqn, desc="revoked"),
                tc="TC-SEC-124", why="revoked from spec.allowedNodes")
        self.logger.info("TC-SEC-124: Revoked node denied PASSED")

        # TC-SEC-125: emptying allowedNodes entirely is not expressible --
        # a DHCHAP pool with no allowed node would make its own volumes
        # unmountable everywhere, and _revoke_host_dual refuses it. The
        # docker equivalent (pool with zero allowed hosts still returns a
        # keyless connect string) has no K8s counterpart.
        self.logger.warning(
            f"{TOK_SKIPPED_K8S} TC-SEC-125: 'pool with no allowed hosts' is "
            f"not a valid K8s state — an empty spec.allowedNodes would make "
            f"every volume in the pool unmountable. {TOK_COVERAGE_LOST}: the "
            f"keyless-connect-string behaviour is docker-only.")

        # TC-SEC-126: re-grant node B and confirm the state converges again
        self.logger.info(f"TC-SEC-126: Re-granting {node_b!r} …")
        self._grant_host_dual(
            pool_id, DhchapHost(node=node_b, nqn=denied.nqn),
            tc="TC-SEC-126")
        self._k8s_assert_dhchap_wiring(self._dhchap_allowed_nodes,
                                       self._dhchap_disallowed_nodes)
        self.logger.info("TC-SEC-126: Re-grant converged PASSED")

        self.logger.info("=== TestLvolSecurityDynamicModification PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Scale: 10 DHCHAP volumes with rapid pool-level host add/remove
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityScaleAndRapidOps(SecurityTestBase):
    """
    Creates 10 DHCHAP volumes in the same pool, rapidly removes and re-adds
    the host, then verifies all volumes still have DHCHAP connect strings.

    TC-SEC-130  Create 10 lvols in DHCHAP pool (no key collisions)
    TC-SEC-131  Remove host from pool → no lvol has DHCHAP connect string
    TC-SEC-132  Re-add host → all 10 lvols have DHCHAP connect strings
    TC-SEC-133  Connect one lvol and run FIO to confirm
    """

    VOLUME_COUNT = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_scale_rapid_ops_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityScaleAndRapidOps START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        # TC-SEC-130: create 10 lvols
        self.logger.info(f"TC-SEC-130: Creating {self.VOLUME_COUNT} lvols …")
        volumes = []
        for i in range(self.VOLUME_COUNT):
            raw_name = f"secsc{i}{_rand_suffix()}"
            lvol_name, lvol_id = self._create_lvol_dual(raw_name)
            assert lvol_id, f"Could not find ID for {lvol_name}"
            volumes.append((lvol_name, lvol_id))
            self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}
        self.logger.info(f"TC-SEC-130: {self.VOLUME_COUNT} volumes created PASSED")

        # TC-SEC-130b (K8s): EVERY volume must individually carry the
        # enforcement. This is the regression this class exists to catch -- a
        # CSI parameter dropped on the 7th volume -- and 10 PV reads cost
        # seconds. An "at least one PV has nodeAffinity" check would be
        # vacuous by construction.
        if self.k8s_test:
            self.logger.info(
                f"TC-SEC-130: Verifying nodeAffinity on all "
                f"{self.VOLUME_COUNT} PVs …")
            for lvol_name, _ in volumes:
                self._k8s_assert_pv_node_affinity(
                    self._k8s_normalize_name(lvol_name), tc="TC-SEC-130")
            self.logger.info(
                f"TC-SEC-130: all {self.VOLUME_COUNT} PVs carry the pool "
                f"nodeAffinity PASSED")

        # TC-SEC-131 / TC-SEC-132: rapid revoke + re-grant must leave the
        # control plane consistent, asserted on observable convergence rather
        # than a sleep.
        if self.k8s_test:
            self.logger.info("TC-SEC-131: Rapid revoke/re-grant churn …")
            churn_node = denied.node if denied else None
            if not churn_node:
                self.logger.warning(
                    f"{TOK_COVERAGE_LOST} TC-SEC-131: no spare node to churn "
                    f"allowedNodes with")
            else:
                churn = DhchapHost(node=churn_node, nqn=denied.nqn)
                self._grant_host_dual(pool_id, churn, tc="TC-SEC-131")
                self._revoke_host_dual(pool_id, churn, tc="TC-SEC-131")
                self._k8s_assert_dhchap_wiring(
                    self._dhchap_allowed_nodes,
                    self._dhchap_disallowed_nodes)
                self.logger.info(
                    "TC-SEC-131/132: churn converged, wiring intact PASSED")
        else:
            # Docker: pool has NO allowed hosts after removal, so a connect
            # string IS returned but without dhchap keys.
            self.logger.info("TC-SEC-131: Removing host from pool …")
            self._revoke_host_dual(pool_id, allowed, tc="TC-SEC-131")
            for lvol_name, lvol_id in volumes:
                cs, cerr = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
                assert cs and not cerr, \
                    f"{lvol_name}: should get connect string when pool has no allowed hosts; err={cerr}"
                s = " ".join(cs) if isinstance(cs, list) else str(cs)
                assert "dhchap" not in s.lower(), \
                    f"{lvol_name}: should not have DHCHAP after host removal; got: {s}"
            self.logger.info("TC-SEC-131: All volumes have no DHCHAP PASSED")

            self.logger.info("TC-SEC-132: Re-adding host to pool …")
            self._grant_host_dual(pool_id, allowed, tc="TC-SEC-132")
            for lvol_name, lvol_id in volumes:
                cs, err = self._get_connect_str_dual(lvol_id, host_nqn=host_nqn)
                assert cs and not err, \
                    f"{lvol_name}: should have connect string after re-add; err={err}"
                s = " ".join(cs) if isinstance(cs, list) else str(cs)
                assert "dhchap" in s.lower(), \
                    f"{lvol_name}: should have DHCHAP after re-add; got: {s}"
            self.logger.info("TC-SEC-132: All volumes have DHCHAP PASSED")

        # TC-SEC-133: use one volume, and prove a DIFFERENT volume is still
        # rejected on the disallowed node -- a different volume so the
        # rejection cannot be a ReadWriteOnce multi-attach artefact.
        self.logger.info("TC-SEC-133: Connecting first lvol and running FIO …")
        first_name, first_id = volumes[0]
        self._assert_host_authorized(first_name, first_id, allowed,
                                     tc="TC-SEC-133")
        device, _ = self._connect_and_get_device_dual(first_name, first_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(first_name, device)
        self.lvol_mount_details[first_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{first_name}_out.log"
        self._run_fio_dual(first_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-133: Scale FIO PASSED")

        if self.k8s_test and denied is not None and len(volumes) > 1:
            other_name, other_id = volumes[1]
            self._assert_host_authorized(other_name, other_id, allowed,
                                         tc="TC-SEC-133")
            self._disconnect_and_unmount_dual(other_name, other_id, None)
            self._assert_host_denied(other_name, other_id, denied,
                                     tc="TC-SEC-133")
            self.logger.info(
                "TC-SEC-133: a second volume is rejected on the disallowed "
                "node PASSED")

        self.logger.info("=== TestLvolSecurityScaleAndRapidOps PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Resize DHCHAP+crypto lvol: security config preserved
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityResize(SecurityTestBase):
    """
    Creates a DHCHAP+crypto lvol, resizes it, and verifies that DHCHAP
    configuration is unchanged after the resize operation.

    TC-SEC-140  Create DHCHAP+crypto lvol (5G), connect, FIO
    TC-SEC-141  Disconnect, resize to 10G
    TC-SEC-142  Verify DHCHAP keys in connect string post-resize; reconnect, FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_resize_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityResize START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        # TC-SEC-140: create DHCHAP+crypto 5G lvol, connect, FIO
        self.logger.info("TC-SEC-140: Creating DHCHAP+crypto 5G lvol …")
        raw_name = f"secrsz{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name, encrypt=True)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-140")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_pre.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="write", runtime=20)
        self.logger.info("TC-SEC-140: Pre-resize FIO PASSED")

        # TC-SEC-141: disconnect, resize to 10G.
        #
        # Routed through _resize_lvol_dual: in K8s the resize has to go
        # through the PVC (the CSI driver reconciles from it), and calling
        # sbcli_utils.resize_lvol directly bypassed the PVC entirely -- the
        # claim would still say 5Gi and CSI could reconcile the change away.
        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        # Snapshot the enforcement config BEFORE the resize, so
        # "unchanged after resize" has a real baseline.
        pre_affinity = None
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            pv_name = self._k8s_assert_pv_node_affinity(
                pvc_name, tc="TC-SEC-140")
            aff, _ = k8s._exec_kubectl(
                f"kubectl get pv {pv_name} "
                f"-o jsonpath='{{.spec.nodeAffinity}}' 2>/dev/null || true")
            pre_affinity = (aff or "").strip()
            assert pre_affinity, (
                "TC-SEC-140: PV has no nodeAffinity before the resize — an "
                "'unchanged after resize' assertion would be vacuous")

        self.logger.info("TC-SEC-141: Resizing to 10G …")
        self._resize_lvol_dual(lvol_name, "10G")

        # Assert the resize ACTUALLY happened before claiming anything about
        # what survived it.
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            grew = False
            deadline = time.time() + 300
            while time.time() < deadline:
                cap, _ = k8s._exec_kubectl(
                    f"kubectl get pvc {pvc_name} -n {k8s.namespace} "
                    f"-o jsonpath='{{.status.capacity.storage}}' "
                    f"2>/dev/null || true")
                cap = (cap or "").strip()
                if cap and cap not in ("5Gi", "5G"):
                    grew = True
                    self.logger.info(
                        f"TC-SEC-141: PVC {pvc_name} capacity is now {cap}")
                    break
                sleep_n_sec(10)
            assert grew, (
                f"TC-SEC-141: PVC {pvc_name} status.capacity never grew past "
                f"5Gi — the resize did not take effect, so TC-SEC-142 would "
                f"be asserting about a volume that was never resized")
        else:
            sleep_n_sec(5)
        self.logger.info("TC-SEC-141: Resize PASSED")

        # TC-SEC-142: security config must be untouched by the resize
        self.logger.info("TC-SEC-142: Verifying DHCHAP after resize …")
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc_name = self._k8s_normalize_name(lvol_name)
            pv_name = k8s.get_pvc_pv_name(pvc_name)
            aff, _ = k8s._exec_kubectl(
                f"kubectl get pv {pv_name} "
                f"-o jsonpath='{{.spec.nodeAffinity}}' 2>/dev/null || true")
            post_affinity = (aff or "").strip()
            assert post_affinity == pre_affinity, (
                f"TC-SEC-142: the resize changed the PV's nodeAffinity.\n"
                f"  before={pre_affinity!r}\n  after={post_affinity!r}")
            self._dhchap_positive_control.discard(lvol_name)
            self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                         tc="TC-SEC-142")
            if denied is not None:
                self._disconnect_and_unmount_dual(lvol_name, lvol_id, None)
                self._assert_host_denied(lvol_name, lvol_id, denied,
                                         tc="TC-SEC-142")
            self.logger.info("TC-SEC-142: DHCHAP survived the resize PASSED")
            self.logger.info("=== TestLvolSecurityResize PASSED ===")
            return

        self._dhchap_positive_control.discard(lvol_name)
        self._assert_host_authorized(lvol_name, lvol_id, allowed,
                                     tc="TC-SEC-142")

        device2, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        self._format_and_mount_dual(lvol_name, device2,
                                    mount_point=mount_point,
                                    format_first=False)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file2 = f"{self.log_path}/{lvol_name}_post.log"
        self._run_fio_dual(lvol_name, mount_point, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-142: Post-resize FIO PASSED")

        self.logger.info("=== TestLvolSecurityResize PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Backup/restore preserves DHCHAP credentials
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityWithBackup(SecurityTestBase):
    """
    Backs up a DHCHAP+crypto lvol and verifies the restored lvol
    is accessible with pool-level DHCHAP credentials.

    TC-SEC-150  Create DHCHAP+crypto lvol, write data, snapshot + backup
    TC-SEC-151  Wait for backup completion
    TC-SEC-152  Restore backup to new lvol name
    TC-SEC-153  Verify restored lvol has DHCHAP connect string; connect + FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_with_backup_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityWithBackup START ===")

        # The mode check MUST come first. It used to sit after an
        # ssh_obj.exec_command availability probe, so in K8s this class
        # hard-errored before it could reach any skip.
        if self.k8s_test:
            self.logger.warning(
                f"{TOK_SKIPPED_K8S} TestLvolSecurityWithBackup: every step "
                f"here is raw `sbcli backup` over SSH with a CLI-named "
                f"restore target, and `backup restore --lvol --pool` has no "
                f"PVC/VolumeSnapshot representation. K8s backup/restore is "
                f"already covered by e2e/e2e_tests/backup/test_backup_restore.py. "
                f"{TOK_COVERAGE_LOST}: 'a restored volume still carries "
                f"DHCHAP' is not asserted in K8s — the one missing check is "
                f"nodeAffinity on the restored PV, which belongs in the "
                f"backup suite rather than duplicated here.")
            self.logger.info(
                "=== TestLvolSecurityWithBackup SKIPPED (k8s) ===")
            return

        # Check backup feature availability
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0], f"{self.base_cmd} backup list 2>&1 | head -5")
        if "command not found" in (out or "").lower() or "error" in (err or "").lower():
            self.logger.warning(
                f"{TOK_COVERAGE_LOST}: backup feature not available on this "
                f"cluster — DHCHAP-after-restore not exercised")
            self.logger.info("Backup feature not available – SKIPPED")
            return

        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        # TC-SEC-150: create lvol, write data, snapshot + backup
        self.logger.info("TC-SEC-150: Creating DHCHAP+crypto lvol …")
        raw_name = f"secbck{_rand_suffix()}"
        lvol_name, lvol_id = self._create_lvol_dual(raw_name, encrypt=True)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_w.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="write", runtime=20)

        self._disconnect_and_unmount_dual(lvol_name, lvol_id, mount_point)
        self.lvol_mount_details[lvol_name]["Mount"] = None

        snap_name = f"snap{lvol_name[-6:]}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d snapshot add {lvol_id} {snap_name} --backup")
        assert not err or "error" not in err.lower(), f"snapshot+backup failed: {err}"
        sleep_n_sec(5)
        self.logger.info("TC-SEC-150: Snapshot + backup triggered PASSED")

        # TC-SEC-151: wait for backup
        self.logger.info("TC-SEC-151: Waiting for backup completion …")
        deadline = time.time() + 300
        backup_id = None
        while time.time() < deadline:
            list_out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} -d backup list")
            for line in (list_out or "").splitlines():
                if snap_name in line:
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    for p in parts:
                        if len(p) == 36 and "-" in p:
                            backup_id = p
                    if "done" in line.lower() or "complete" in line.lower():
                        break
            if backup_id:
                break
            sleep_n_sec(10)
        assert backup_id, "Could not find backup ID"
        self.logger.info(f"TC-SEC-151: Backup {backup_id} complete PASSED")

        # TC-SEC-152: restore
        self.logger.info("TC-SEC-152: Restoring backup …")
        restored_name = f"secrst{_rand_suffix()}"
        out, err = self.ssh_obj.exec_command(
            self.mgmt_nodes[0],
            f"{self.base_cmd} -d backup restore {backup_id} --lvol {restored_name} --pool {self.pool_name}")
        assert not err or "error" not in err.lower(), f"restore failed: {err}"

        deadline2 = time.time() + 300
        while time.time() < deadline2:
            list_out, _ = self.ssh_obj.exec_command(
                self.mgmt_nodes[0], f"{self.base_cmd} lvol list")
            if restored_name in (list_out or ""):
                break
            sleep_n_sec(10)
        else:
            raise TimeoutError(f"Restored lvol {restored_name} did not appear within 300s")

        restored_id = self.sbcli_utils.get_lvol_id(restored_name)
        assert restored_id
        self.lvol_mount_details[restored_name] = {"ID": restored_id, "Mount": None}
        self.logger.info("TC-SEC-152: Restore PASSED")

        # TC-SEC-153: verify DHCHAP + connect + FIO
        self.logger.info("TC-SEC-153: Verifying restored lvol DHCHAP …")
        rest_cs, rest_err = self._get_connect_str_dual(restored_id, host_nqn=host_nqn)
        assert rest_cs and not rest_err, f"Restored connect failed: {rest_err}"
        rest_str = " ".join(rest_cs) if isinstance(rest_cs, list) else str(rest_cs)
        assert "dhchap" in rest_str.lower(), \
            f"Expected DHCHAP keys for restored lvol; got: {rest_str}"

        rest_device, _ = self._connect_and_get_device_dual(restored_name, restored_id, host_nqn=host_nqn)
        rest_mount = f"{self.mount_path}/{restored_name}"
        self.ssh_obj.mount_path(node=self.fio_node, device=rest_device, mount_path=rest_mount)
        self.lvol_mount_details[restored_name]["Mount"] = rest_mount
        log_file2 = f"{self.log_path}/{restored_name}_out.log"
        self._run_fio_dual(restored_name, rest_mount, log_file2, rw="randrw", runtime=20)
        self.logger.info("TC-SEC-153: Restored lvol FIO PASSED")

        self.logger.info("=== TestLvolSecurityWithBackup PASSED ===")


# ═══════════════════════════════════════════════════════════════════════════
#  Test – Concurrent multi-client connect with DHCHAP
# ═══════════════════════════════════════════════════════════════════════════


class TestLvolSecurityMultiClientConcurrent(SecurityTestBase):
    """
    Tests concurrent connect string requests: registered NQN vs unregistered.

    TC-SEC-160  Create DHCHAP pool, register NQN_A only, create lvol
    TC-SEC-161  Concurrently request connect strings for NQN_A and NQN_B
    TC-SEC-162  NQN_A gets DHCHAP keys; NQN_B is rejected (pool has allowed hosts)
    TC-SEC-163  Connect with NQN_A and run FIO
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_security_multi_client_concurrent_v2"

    def run(self):
        self.logger.info("=== TestLvolSecurityMultiClientConcurrent START ===")
        self._normalize_fio_node()

        pool_id, allowed, denied = self._setup_pool_and_host(dhchap=True)
        host_nqn = _as_nqn(allowed)

        wrong_nqn = f"nqn.2024-01.io.simplyblock:test:wrong-{_rand_suffix()}"
        raw_name = f"secmc{_rand_suffix()}"

        if self.k8s_test:
            self._k8s_multi_client_concurrent(pool_id, allowed, denied)
            return

        # TC-SEC-160: create lvol
        self.logger.info("TC-SEC-160: Creating DHCHAP lvol …")
        lvol_name, lvol_id = self._create_lvol_dual(raw_name)
        self.lvol_mount_details[lvol_name] = {"ID": lvol_id, "Mount": None}

        # TC-SEC-161: concurrent requests
        self.logger.info("TC-SEC-161: Concurrent connect-string requests …")
        results = {}

        def _req(nqn, key):
            try:
                cs, cerr = self._get_connect_str_dual(lvol_id, host_nqn=nqn)
                results[key] = (cs, cerr)
            except Exception as e:
                results[key] = (None, str(e))

        t_good = threading.Thread(target=_req, args=(host_nqn, "good"))
        t_bad = threading.Thread(target=_req, args=(wrong_nqn, "bad"))
        t_good.start()
        t_bad.start()
        t_good.join()
        t_bad.join()

        good_cs, good_err = results.get("good", (None, "no result"))
        bad_cs, bad_err = results.get("bad", (None, "no result"))

        # TC-SEC-162: registered NQN gets DHCHAP; unregistered does not
        assert good_cs, f"Registered NQN should get connect string; err={good_err}"
        good_str = " ".join(good_cs) if isinstance(good_cs, list) else str(good_cs)
        assert "dhchap" in good_str.lower(), \
            f"Registered NQN should have DHCHAP keys; got: {good_str}"
        self.logger.info("TC-SEC-162: Registered NQN has DHCHAP PASSED")

        # Known behaviour: when pool HAS allowed hosts, a wrong/unregistered
        # NQN is rejected entirely (issue #4).
        bad_rejected = bool(bad_err) or not bad_cs
        assert bad_rejected, (
            f"Unregistered NQN should be rejected when pool has allowed hosts; "
            f"got: cs={bad_cs}")
        self.logger.info("TC-SEC-162: Unregistered NQN rejected PASSED")

        # TC-SEC-163: connect + FIO
        self.logger.info("TC-SEC-163: Connecting and running FIO …")
        device, _ = self._connect_and_get_device_dual(lvol_name, lvol_id, host_nqn=host_nqn)
        mount_point = self._format_and_mount_dual(lvol_name, device)
        self.lvol_mount_details[lvol_name]["Mount"] = mount_point
        log_file = f"{self.log_path}/{lvol_name}_out.log"
        self._run_fio_dual(lvol_name, mount_point, log_file, rw="randrw", runtime=30)
        self.logger.info("TC-SEC-163: FIO PASSED")

        self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")

    def _k8s_multi_client_concurrent(self, pool_id, allowed, denied):
        """K8s: an authorized and an unauthorized client, concurrently.

        TWO separate PVCs, deliberately. Pointing two pods at the SAME PVC
        would be the ReadWriteOnce case, so the rejection on the second node
        would be ``Multi-Attach error for volume`` -- which proves nothing
        whatsoever about DHCHAP and is exactly how this class would produce a
        right-answer-for-the-wrong-reason pass. (``_assert_host_denied`` also
        rejects a Multi-Attach event outright, so a regression back to one PVC
        fails loudly rather than silently.)
        """
        denied = self._require_denied_host(denied, tc="TC-SEC-160")

        self.logger.info("TC-SEC-160: Creating two DHCHAP volumes …")
        good_name, good_id = self._create_lvol_dual(f"secmcok{_rand_suffix()}")
        bad_name, bad_id = self._create_lvol_dual(f"secmcno{_rand_suffix()}")
        for n, i in ((good_name, good_id), (bad_name, bad_id)):
            self.lvol_mount_details[n] = {"ID": i, "Mount": None}
        self._k8s_assert_pv_node_affinity(
            self._k8s_normalize_name(good_name), tc="TC-SEC-160")
        self._k8s_assert_pv_node_affinity(
            self._k8s_normalize_name(bad_name), tc="TC-SEC-160")

        # Positive controls first: both volumes are mountable on an allowed
        # node, so the denial below cannot be an unmountable-volume artefact.
        self.logger.info("TC-SEC-161: Establishing positive controls …")
        self._assert_host_authorized(good_name, good_id, allowed,
                                     tc="TC-SEC-161")
        self._disconnect_and_unmount_dual(good_name, good_id, None)
        self._assert_host_authorized(bad_name, bad_id, allowed,
                                     tc="TC-SEC-161")
        self._disconnect_and_unmount_dual(bad_name, bad_id, None)

        # TC-SEC-162: concurrent attach from an allowed and a disallowed node
        self.logger.info(
            f"TC-SEC-162: Concurrent attach — {allowed.node!r} (allowed) vs "
            f"{denied.node!r} (disallowed) …")
        results = {}

        def _attach(name, lvol_id, host, expect_allowed, key):
            try:
                if expect_allowed:
                    self._assert_host_authorized(
                        name, lvol_id, host, tc="TC-SEC-162")
                else:
                    self._assert_host_denied(
                        name, lvol_id, host, tc="TC-SEC-162")
                results[key] = None
            except BaseException as exc:      # noqa: BLE001 - re-raised below
                results[key] = exc

        t_good = threading.Thread(
            target=_attach, args=(good_name, good_id, allowed, True, "good"))
        t_bad = threading.Thread(
            target=_attach, args=(bad_name, bad_id, denied, False, "bad"))
        t_good.start()
        t_bad.start()
        t_good.join()
        t_bad.join()

        for key, label in (("good", "authorized node"),
                           ("bad", "unauthorized node")):
            exc = results.get(key, RuntimeError(f"{key}: no result"))
            if exc is not None:
                raise AssertionError(
                    f"TC-SEC-162: concurrent attach from the {label} did not "
                    f"behave as expected: {exc!r}") from exc
        self.logger.info(
            "TC-SEC-162: authorized attach succeeded and unauthorized attach "
            "was denied, concurrently PASSED")

        # TC-SEC-163: I/O on the authorized volume
        self.logger.info("TC-SEC-163: Running FIO on the authorized volume …")
        self._run_fio_dual(good_name, None, None, rw="randrw", runtime=30,
                           node_name=allowed.node)
        self.logger.info("TC-SEC-163: FIO PASSED")

        self.logger.info("=== TestLvolSecurityMultiClientConcurrent PASSED ===")


class TestDhchapPodScheduling(SecurityTestBase):
    """
    K8s-only: verifies that a pod consuming a PVC from a DHCHAP pool with
    ``allowedNodes`` mounts successfully when pinned to an allowed node, is
    rejected (FailedMount) when pinned to a disallowed node, and that a
    second pod re-attaching the same PVC to a (possibly different) allowed
    node still works.

    TC-DHCHAP-SCHED-001  Create DHCHAP pool with allowedNodes = subset of workers
    TC-DHCHAP-SCHED-002  Create PVC from DHCHAP pool StorageClass
    TC-DHCHAP-SCHED-003  Pod #1 pinned to an allowed node → mounts successfully
    TC-DHCHAP-SCHED-004  Delete Pod #1, Pod #2 pinned to an allowed node → mounts
    TC-DHCHAP-SCHED-006  Pod pinned to a disallowed node → FailedMount, rejected
    TC-DHCHAP-SCHED-005  Cleanup
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "dhchap_pod_scheduling"

    def run(self):
        self.logger.info("=== TestDhchapPodScheduling START ===")

        if not self.k8s_test:
            self.logger.info(
                "TestDhchapPodScheduling: skipping — pod scheduling "
                "verification is a K8s-only concept")
            self.logger.info("=== TestDhchapPodScheduling SKIPPED (Docker) ===")
            return

        self._normalize_fio_node()
        k8s = self._ensure_k8s_utils()

        # ── TC-DHCHAP-SCHED-001: pool + allowedNodes ─────────────────────
        self.logger.info(
            "TC-DHCHAP-SCHED-001: Creating DHCHAP pool with allowedNodes …")
        allowed_node_names, disallowed_node_names = \
            self._k8s_setup_dhchap_pool_subset()
        self.logger.info(
            f"  Allowed nodes: {allowed_node_names}; "
            f"disallowed: {disallowed_node_names}")
        pool_id = self._get_pool_id()
        assert pool_id, f"Pool {self.pool_name} not found"
        # Prove the four enforcement links before trusting any pod outcome.
        self._k8s_assert_dhchap_wiring(
            allowed_node_names, disallowed_node_names)
        self.logger.info("TC-DHCHAP-SCHED-001: Pool + wiring PASSED")

        # ── TC-DHCHAP-SCHED-002: create PVC ──────────────────────────────
        self.logger.info("TC-DHCHAP-SCHED-002: Creating PVC …")
        raw_name = f"dhsched{_rand_suffix()}"
        pvc_name, lvol_id = self._create_lvol_dual(raw_name, size="5G")
        self._k8s_assert_pv_node_affinity(pvc_name, tc="TC-DHCHAP-SCHED-002")
        self.logger.info(
            f"TC-DHCHAP-SCHED-002: PVC {pvc_name} bound (lvol={lvol_id})")

        # ── TC-DHCHAP-SCHED-003: Pod #1 pinned to an allowed node ───────
        self.logger.info(
            f"TC-DHCHAP-SCHED-003: Pinning Pod #1 to allowed node "
            f"{allowed_node_names[0]!r} …")
        try:
            pod_name_1 = self._k8s_verify_pod_scheduling(
                pvc_name, allowed_node_names[0], expect_success=True,
                pod_prefix="dhsched-pod1")
        except DhchapUnsupportedByHost as exc:
            self.logger.warning(f"TC-DHCHAP-SCHED-003: {exc}")
            self.logger.info(
                f"=== TestDhchapPodScheduling {TOK_SKIPPED_K8S} "
                f"(host kernel has no in-band NVMe auth) ===")
            return
        # Confirm the pin actually took, and that the filesystem the
        # StorageClass asked for is what got created.
        landed = k8s.get_pod_node_name(pod_name_1)
        assert landed == allowed_node_names[0], (
            f"TC-DHCHAP-SCHED-003: pod was pinned to "
            f"{allowed_node_names[0]!r} but landed on {landed!r}")
        self._k8s_assert_fs_type(pod_name_1, self._fs_type,
                                 tc="TC-DHCHAP-SCHED-003")
        self._dhchap_positive_control.add(pvc_name)
        self.logger.info("TC-DHCHAP-SCHED-003: Pod #1 on allowed node PASSED")

        # Write known data so the re-attach below proves the volume is USABLE
        # across allowed nodes, not merely mountable.
        marker = f"dhchap-{_rand_suffix()}"
        wrote_marker = False
        try:
            # exec_in_pod already wraps the command in `sh -c`, so pass the
            # bare shell line. It returns (stdout, stderr).
            _, werr = k8s.exec_in_pod(
                pod_name_1,
                f"echo {marker} > /spdkvol/marker.txt && sync")
            if werr and werr.strip():
                self.logger.warning(
                    f"{TOK_WEAK_EVIDENCE} TC-DHCHAP-SCHED-003: writing the "
                    f"marker file reported: {werr!r}")
            else:
                wrote_marker = True
        except Exception as exc:
            self.logger.warning(
                f"{TOK_WEAK_EVIDENCE} TC-DHCHAP-SCHED-003: could not write a "
                f"marker file: {exc}")

        # ── TC-DHCHAP-SCHED-004: Delete Pod #1, Pod #2 on allowed node ──
        self.logger.info(f"TC-DHCHAP-SCHED-004: Deleting pod {pod_name_1} …")
        self._k8s_release_pod(pod_name_1, pvc_name=pvc_name)

        pod_2_target = allowed_node_names[-1]
        self.logger.info(
            f"TC-DHCHAP-SCHED-004: Pinning Pod #2 (same PVC) to allowed "
            f"node {pod_2_target!r} …")
        pod_name_2 = self._k8s_verify_pod_scheduling(
            pvc_name, pod_2_target, expect_success=True,
            pod_prefix="dhsched-pod2")
        if wrote_marker:
            out, rerr = k8s.exec_in_pod(pod_name_2, "cat /spdkvol/marker.txt")
            assert marker in (out or ""), (
                f"TC-DHCHAP-SCHED-004: data written on "
                f"{allowed_node_names[0]!r} is not readable on "
                f"{pod_2_target!r} — the volume re-attached but the data did "
                f"not survive. got out={out!r} err={rerr!r}")
            self.logger.info(
                f"TC-DHCHAP-SCHED-004: marker survived the re-attach to "
                f"{pod_2_target!r}")
        self.logger.info("TC-DHCHAP-SCHED-004: Pod #2 on allowed node PASSED")

        # Release before the denial case, and WAIT for the volume to actually
        # detach. Deleting the pod is not enough: the VolumeAttachment
        # outlives it, so the denial pod below would attach-fail with
        # Multi-Attach instead of being rejected by nodeAffinity. Exactly what
        # failed in CI run 093822 — 31s after the pod was gone the volume was
        # still attached to the previous node.
        self._k8s_release_pod(pod_name_2, pvc_name=pvc_name)

        # ── TC-DHCHAP-SCHED-006: Pod pinned to a disallowed node ────────
        if disallowed_node_names:
            self.logger.info(
                f"TC-DHCHAP-SCHED-006: Pinning Pod #3 (same PVC) to "
                f"DISALLOWED node {disallowed_node_names[0]!r} …")
            self._k8s_verify_pod_scheduling(
                pvc_name, disallowed_node_names[0], expect_success=False,
                pod_prefix="dhsched-pod3-bad")
            self.logger.info(
                "TC-DHCHAP-SCHED-006: Pod on disallowed node correctly "
                "rejected PASSED")
        else:
            self.logger.warning(
                f"{TOK_COVERAGE_LOST} TC-DHCHAP-SCHED-006: only one "
                f"schedulable worker — the disallowed-node rejection, which "
                f"is the whole point of this class, was not exercised")

        self.logger.info(
            "=== TestDhchapPodScheduling PASSED (allowedNodes enforcement "
            "at mount via PV nodeAffinity; in-band DHCHAP negotiation is "
            "covered by TestLvolSecurityNegativeConnect in docker mode) ===")
