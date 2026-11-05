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

    #: Server-side data ops per pod at the previous check, for growth.
    _pnfs_srv = None

    def _cluster_settled(self):
        """Is the cluster actually healthy right now?

        The growth check below is only meaningful once the block path could
        have been restored. "Between two outages" is not the same thing, and
        on this test it is emphatically not: K8sNativeRapidFailoverNoGapTest
        exists to land the next outage while migration from the previous one
        is still in flight, so the inter-outage window is deliberately never
        settled.

        Run 41959008874 failed on exactly that mistaken premise --
        fio-pvc-xx0axkn4x1io-q1-z6bch grew 240185 -> 281862 server-side ops
        in a window my check called "after the cluster recovered" and the
        test had designed to be anything but. Falling back there is the
        product keeping IO alive mid-migration.

        So ask the cluster, rather than inferring from where we are in the
        loop. Anything other than a fully active cluster with every node
        online means skip the comparison this cycle and re-baseline.
        """
        try:
            status = (self.sbcli_utils.get_cluster_status(self.cluster_id)
                      or "")
            if isinstance(status, (list, dict)):
                row = status[0] if isinstance(status, list) and status else status
                status = (row or {}).get("status", "")
            if str(status).strip().lower() != "active":
                return False
            nodes = self.sbcli_utils.get_storage_nodes() or {}
            results = nodes.get("results", nodes if isinstance(nodes, list) else [])
            if not results:
                return False
            return all((n or {}).get("status", "") == "online" for n in results)
        except Exception as exc:                      # noqa: BLE001
            # Never fail the soak on a health probe. An unreadable cluster
            # means this cycle cannot judge fallback, not that fallback
            # happened.
            self.logger.warning(
                "[pnfs-stress] could not read cluster health, skipping the "
                "fallback comparison this cycle: %s", str(exc)[:160])
            return False

    def _assert_still_pnfs(self, when):
        """A layout is held, and no mount is still falling back.

        Two different questions, and the first version of this conflated
        them. B-03's rule is that server-side READ/WRITE is exactly zero,
        which is right on a healthy cluster. It is wrong here: mountstats
        counters are cumulative since mount and never reset, so one fallback
        during one outage window marks that mount for the rest of the soak
        even after the layout is fully restored. Run 41923565845 died on
        exactly that after eight clean outages --
        fio-pvc-ywcan1kzf6eq-5-vcc2c reported READ=1421 WRITE=107478 while
        holding a valid layout, and the eight other mounts were clean.

        So the hard failure is "no layout", which assert_layouts_taken still
        enforces. Fallback is judged by growth across the settled window
        between one outage's recovery and the next outage: IO going to the
        server while the cluster is healthy again is the silent-fallback
        case; IO that went there during the outage is the product keeping
        the mount alive, which is what it is supposed to do.

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
            return

        try:
            proven = assert_layouts_taken(self.k8s_utils, pods,
                                          mountpoint=self.MOUNT,
                                          allow_server_io=True)
        except PnfsEvidenceError as exc:
            raise PnfsEvidenceError(
                f"[pnfs-stress] {when}: a mount is no longer holding a pNFS "
                f"layout. This is the silent-fallback case -- IO would keep "
                f"verifying while every byte went through the metadata "
                f"server. {exc}") from exc

        now = {p: v.get("server_ops", 0) for p, v in proven.items()}
        if when.startswith("before") and self._pnfs_srv and self._cluster_settled():
            grew = {p: (self._pnfs_srv[p], n) for p, n in now.items()
                    if p in self._pnfs_srv and n > self._pnfs_srv[p]}
            if grew:
                raise PnfsEvidenceError(
                    "[pnfs-stress] the data path is still falling back to "
                    "the metadata server after the cluster recovered. These "
                    "mounts held a layout and drove server-side IO anyway, "
                    "between one outage's recovery and the next: "
                    + ", ".join(f"{p} {a}->{b}" for p, (a, b) in grew.items()))
        self._pnfs_srv = now

        served = sum(1 for n in now.values() if n)
        self.logger.info(
            "[pnfs-stress] %s: %d mount(s) hold a layout%s", when, len(proven),
            f"; {served} carry historic server-side IO from earlier outages"
            if served else " and none has ever touched the server")

        exports = nfsexport_status(self.k8s_utils) or []
        bad = [((e.get("metadata") or {}).get("name"),
                (e.get("status") or {}).get("phase"))
               for e in exports
               if ((e.get("status") or {}).get("phase") or "") != "Ready"]
        if bad:
            self.logger.warning(
                "[pnfs-stress] %s: %d NFSExport(s) not Ready: %s",
                when, len(bad), bad[:4])

    #: The method on the k8s bases that actually injects an outage.
    #
    # Named explicitly and checked at first use. The first version of this
    # mixin wrapped _perform_outage(), copying lblk_stress -- but that method
    # only exists on the docker bases. On the three k8s bases the override
    # would have been dead code, the layout check would never have run, and
    # the soak would have reported green for a data path that had silently
    # stopped being pNFS. That is precisely the failure this file exists to
    # catch, so the name is asserted rather than trusted.
    OUTAGE_HOOK = "perform_n_plus_k_outages"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(super(), self.OUTAGE_HOOK):
            raise RuntimeError(
                f"[pnfs-stress] {type(self).__name__} has no "
                f"{self.OUTAGE_HOOK}() to wrap, so the layout check would "
                f"never run and this soak would prove nothing about pNFS. "
                f"The base changed; update OUTAGE_HOOK.")

    def perform_n_plus_k_outages(self, *args, **kwargs):
        if not self._pnfs_checked:
            self.assert_pnfs_cluster()
            self._pnfs_checked = True
        self._assert_still_pnfs("before outage")
        out = super().perform_n_plus_k_outages(*args, **kwargs)
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
