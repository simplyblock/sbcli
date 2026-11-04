"""Track A: run the existing k8s-native soaks on top of a shared pNFS claim.

The finite pNFS cases live in ``e2e_tests/pnfs/test_pnfs_rwx.py`` and all run
against a healthy cluster. This is the stress shape: the same outage loops the
NVMe soaks use, with the volumes underneath them turned into ReadWriteMany
pNFS claims, so the shared filesystem is exercised while nodes are going down
and coming back.

Why a mixin over the existing bases rather than new soaks: the outage loop,
the migration validation and the ANA assertions are the expensive part and
they are already right. What changes is the kind of volume, which is two
decisions -- the StorageClass and the access mode -- plus one extra thing to
check after every outage.

That extra check is the point. On NVMe, "IO kept flowing" is the whole
question. On pNFS it is not: if the block path cannot be re-established after
an outage, NFS 4.1 silently routes the IO through the metadata server
instead. Every byte still verifies and throughput quietly collapses onto one
pod. So after every outage this asserts the mounts still hold a layout and
the server is still serving no data -- the same evidence B-03 gates the
healthy suite on.

Mirrors the shape of ``lblk_stress.py`` deliberately; read that one first if
this is unfamiliar.
"""

from stress_test.continuous_k8s_native_failover import (
    K8sNativeFailoverTest,
    K8sNativeRapidFailoverNoGapTest,
    K8sNativeResilientFailoverTest,
)
from utils.pnfs_utils import (
    PnfsEvidenceError,
    assert_layouts_taken,
    nfsexport_status,
)


class _PnfsRwxMixin:
    """Turn a k8s-native soak's volumes into one shared pNFS claim."""

    #: Created once, on the first PVC this soak asks for.
    PNFS_SC = "sb-pnfs-stress-sc"

    #: Where create_fio_job mounts the claim inside the pod.
    MOUNT = "/spdkvol"

    # Class-level defaults for the same reason lblk_stress has them: the
    # guard that decides whether to run the one-time setup has to READ these
    # before the setup has ever run, and the mixin has no __init__ of its
    # own. Immutable values only.
    _pnfs_checked = False
    _pnfs_sc_made = False

    # ── bring-up gate ────────────────────────────────────────────────────

    def assert_pnfs_cluster(self):
        """Refuse to soak unless the cluster can actually serve pNFS.

        A pNFS soak on a cluster with no metadata server does not fail. The
        StorageClass is accepted, the claims stay Pending, the loop reports
        no IO and blames the outages. Worse, a claim that binds without a
        layout runs as plain NFS through one pod for the length of the soak
        and proves nothing -- that is the failure this whole plan exists to
        catch, and it has to be caught before the first outage rather than
        inferred from the end.
        """
        self._ensure_k8s_utils()
        k8s = self.k8s_utils
        out, _err = k8s._exec_kubectl(
            "kubectl get simplyblockdrivers -A "
            "-o jsonpath='{.items[0].spec.pnfs.mds.stateSize}' "
            "2>/dev/null || true", supress_logs=True)
        if not (out or "").strip().strip("'"):
            raise RuntimeError(
                "[pnfs-stress] no SimplyblockDriver configures a pNFS "
                "metadata server (spec.pnfs.mds is empty). Claims would stay "
                "Pending and the soak would report that as an outage "
                "problem.")
        out, _err = k8s._exec_kubectl(
            "kubectl get crd nfsexports.storage.simplyblock.io "
            "--no-headers 2>/dev/null || true", supress_logs=True)
        if not (out or "").strip():
            raise RuntimeError(
                "[pnfs-stress] the NFSExport CRD is not installed, so this "
                "cluster cannot serve a shared filesystem at all.")
        self.logger.info("[pnfs-stress] cluster serves pNFS; MDS configured")

    # ── volume shape ─────────────────────────────────────────────────────

    def _ensure_pnfs_sc(self):
        if self._pnfs_sc_made:
            return
        pool = getattr(self, "pool_name", None) or "simplyblock-pool"
        self.k8s_utils.create_storage_class(
            name=self.PNFS_SC, cluster_id=self.cluster_id, pool_name=pool,
            fs_type="pnfs")
        self._pnfs_sc_made = True
        self.logger.info("[pnfs-stress] StorageClass %s created (fstype=pnfs)",
                         self.PNFS_SC)

    def _pvc_class_for(self, index, existing_count, sc_name, fs_type):
        if not self._pnfs_checked:
            self.assert_pnfs_cluster()
            self._pnfs_checked = True
        self._ensure_pnfs_sc()
        return self.PNFS_SC, "pnfs"

    def _pvc_access_modes(self):
        return ("ReadWriteMany",)

    # ── the check that makes this a pNFS soak ────────────────────────────

    def _fio_pod_names(self):
        out, _err = self.k8s_utils._exec_kubectl(
            f"kubectl get pods -n {self.k8s_utils.namespace} "
            f"-l app=fio-benchmark --field-selector status.phase=Running "
            f"--no-headers -o custom-columns=:metadata.name "
            f"2>/dev/null || true", supress_logs=True)
        return [ln.strip() for ln in (out or "").splitlines() if ln.strip()]

    def _assert_still_pnfs(self, when):
        """Layouts still held, and every export still Ready.

        Never fails the soak on an empty pod list: between outages there are
        windows with no fio pod Running, and turning that into a failure
        would make the soak flaky for a reason that has nothing to do with
        the product. It says so loudly instead.
        """
        pods = self._fio_pod_names()
        if not pods:
            self.logger.warning(
                "[pnfs-stress] %s: no Running fio pod to read mountstats "
                "from, so the layout check was skipped this cycle", when)
        else:
            try:
                assert_layouts_taken(self.k8s_utils, pods,
                                     mountpoint=self.MOUNT)
                self.logger.info(
                    "[pnfs-stress] %s: %d mount(s) still hold a layout and "
                    "the server is serving no data", when, len(pods))
            except PnfsEvidenceError as exc:
                raise PnfsEvidenceError(
                    f"[pnfs-stress] {when}: the data path stopped being pNFS. "
                    f"This is the silent-fallback case -- IO would keep "
                    f"verifying while every byte went through the metadata "
                    f"server. {exc}") from exc

        exports = nfsexport_status(self.k8s_utils) or []
        bad = [((e.get("metadata") or {}).get("name"),
                (e.get("status") or {}).get("phase"))
               for e in exports
               if ((e.get("status") or {}).get("phase") or "") != "Ready"]
        if bad:
            self.logger.warning(
                "[pnfs-stress] %s: %d NFSExport(s) not Ready: %s",
                when, len(bad), bad[:4])

    def _perform_outage(self, *args, **kwargs):
        if not self._pnfs_checked:
            self.assert_pnfs_cluster()
            self._pnfs_checked = True
        self._assert_still_pnfs("before outage")
        out = super()._perform_outage(*args, **kwargs)
        self._assert_still_pnfs("after outage")
        return out


# ── leaf classes ─────────────────────────────────────────────────────────
# One per existing k8s-native soak. The bases are untouched.

class PnfsRwxFailoverK8s(_PnfsRwxMixin, K8sNativeFailoverTest):
    """A-01: baseline outage loop on a shared filesystem."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pnfs_rwx_failover_k8s"


class PnfsRwxRapidNoGapK8s(_PnfsRwxMixin, K8sNativeRapidFailoverNoGapTest):
    """A-02: outages landing during an unfinished migration.

    The interesting one of the three. A layout names devices, and a
    migration moves which node serves them; this is the loop that makes the
    two overlap.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pnfs_rwx_rapid_nogap_k8s"


class PnfsRwxResilientK8s(_PnfsRwxMixin, K8sNativeResilientFailoverTest):
    """A-03: long soak with snapshot and clone churn.

    The resilient base keeps permanent claims alive for the whole run, which
    matters more here than on NVMe: provisioning blocks whenever
    ``ndcs + npcs > online_nodes``, and a shared claim that cannot be
    provisioned during the degraded window is a window with no IO at all.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pnfs_rwx_resilient_k8s"
