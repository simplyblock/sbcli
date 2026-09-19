"""Does an lblk cluster keep serving, and keep your bytes, through outages.

The other lblk cases each prove one narrow thing. This one asks the question a
prospect actually asks: take a node out, any way you like, one at a time, and
does IO carry on and does the data still match afterwards.

Two independent lanes, because they catch different failures:

* **Static volumes.** Written once before any fault and never touched again,
  then md5'd. No overlapping IO, no verify_backlog, no torn-write ambiguity --
  which makes md5 completely reliable here, unlike the live FIO lane where
  fio's own concurrency can manufacture a mismatch. This is the durability
  gate, and it is re-checked after *every* outage rather than only at the end,
  so a divergence names the outage that caused it.

* **Live FIO.** Runs continuously across the whole outage sequence. This is the
  availability gate: with ndcs/npcs 2/2 losing one node is meant to be
  survivable, so any io_u error is a defect, not a blip. Sudden loss is the
  normal case in the field -- most outages are not graceful -- so a crash-type
  outage gets exactly the same bar as a planned one.

The volume set is deliberately mixed: plain, encrypted and DHCHAP-authenticated
volumes, formatted and raw, plus a clone taken from a snapshot. Encryption and
DHCHAP on lblk are an untested combination; a failure confined to those lanes
is a real finding about lblk rather than a bug in this test.
"""

import os
import random

from e2e_tests.lblk.test_lblk import (
    _LblkBase,
    _LblkDockerMixin,
    _LblkK8sMixin,
    LblkPreconditionError,
)
from utils.common_utils import sleep_n_sec


class _LblkOutageMatrix(_LblkBase):
    """Every outage type once, on a different node each time."""

    #: One outage per node, in the order a cluster is most likely to meet them:
    #: planned first, then progressively less polite. Each is applied to a
    #: different node so no node is asked to survive two in a row, and the
    #: cluster is fully healthy again before the next.
    OUTAGES = (
        "graceful_shutdown",
        "container_stop",
        "storage_node_reboot",
        "interface_full_network_interrupt",
    )

    #: Static payload per volume. Large enough to span many stripes -- and so
    #: to involve every node's parity -- small enough that re-md5ing five
    #: volumes after each of four outages is not the bulk of the runtime.
    STATIC_MB = 256

    #: Budget per outage cycle when sizing the live FIO job. An outage plus
    #: full recovery plus the post-checks has measured at 8-12 minutes, so 900s
    #: leaves headroom. The job must outlast the WHOLE sequence -- every outage
    #: type on every node -- and with a fixed runtime it silently ended part
    #: way through and the availability lane stopped watching. Sized from the
    #: cycle count instead, and joined at the end regardless, so an
    #: over-estimate costs nothing.
    SEC_PER_CYCLE = 900

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._make_pool()
        self._baselines = {}      # lvol -> {path: md5}, taken before any fault
        self._static = []         # (lvol, mount) pairs in the durability lane

        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        if len(nodes) < 2:
            raise LblkPreconditionError(
                f"need at least 2 storage nodes to take one down, have "
                f"{len(nodes)}")

        # Every outage type against every node. Ordered by outage rather than
        # by node so consecutive cycles land on different machines: taking the
        # same node down four times in a row tests recovery from a warm cache
        # more than it tests the cluster, and it leaves the other nodes
        # untouched for a quarter of the run.
        cycles = [(node, outage)
                  for outage in self.OUTAGES for node in nodes]

        # Full coverage is every type on every node, and on a six-node cluster
        # that is 24 cycles at roughly ten minutes each -- about four hours.
        # That is the right default for the question this test answers, but it
        # does not fit a short pipeline slot, so LBLK_MATRIX_NODES trims the
        # node count without touching the code or dropping an outage type:
        # every type still runs, just against fewer machines.
        cap = int(os.environ.get("LBLK_MATRIX_NODES", "0") or 0)
        if cap and cap < len(nodes):
            trimmed = nodes[:cap]
            cycles = [(node, outage)
                      for outage in self.OUTAGES for node in trimmed]
            self.logger.warning(
                "[matrix] LBLK_MATRIX_NODES=%d: running %d of %d nodes, so "
                "%d cycles instead of %d. Coverage of outage TYPES is "
                "unchanged; coverage of nodes is not.",
                cap, len(trimmed), len(nodes), len(cycles),
                len(self.OUTAGES) * len(nodes))
        self.logger.info(
            "[matrix] %d cycles planned: %d outage type(s) x %d node(s), "
            "~%.1fh at %ds per cycle",
            len(cycles), len(self.OUTAGES), len(cycles) // len(self.OUTAGES),
            len(cycles) * self.SEC_PER_CYCLE / 3600.0, self.SEC_PER_CYCLE)

        self._build_static_set(pool)
        live = self._start_live_fio(pool, len(cycles))

        for i, (node, outage) in enumerate(cycles, start=1):
            self.logger.info("[matrix] cycle %d/%d: %s on %s",
                             i, len(cycles), outage, node.get("mgmt_ip"))
            self._outage_and_recover(node, outage)
            # Durability, checked here rather than only at the end so a
            # mismatch names the outage and the node that produced it.
            where = f"after {outage} on {node.get('mgmt_ip')}"
            self._assert_static_unchanged(where)
            self._verify_raw(where)
            self._scan_spdk_logs(where)
            self._assert_fio_alive(live, where)

        self._finish_live_fio(live)
        self._assert_static_unchanged("after all outages")
        self.logger.info(
            "[matrix] %d cycles survived (%d outage types x %d nodes): %d "
            "static volume(s) byte identical throughout, live FIO "
            "uninterrupted on %d volume(s)",
            len(cycles), len(self.OUTAGES), len(nodes), len(self._static),
            len(live))

    # ── the durability lane ───────────────────────────────────────────────

    def _build_static_set(self, pool):
        """Write the payload that must survive, and record what it hashes to.

        Each flavour is provisioned the way its platform does it, then filled
        with the same payload, so a divergence between flavours points at the
        flavour rather than at the workload.
        """
        flavours = [
            ("plain", dict()),
            ("crypto", dict(crypto=True)),
            ("dhchap", dict(dhchap=True)),
        ]
        for label, opts in flavours:
            name = f"mx{label}{random.randint(100, 999)}"
            mount = self._provision_typed(name, pool, **opts)
            self._write_static(name, mount)
            self._baselines[name] = self._static_md5(name, mount)
            self._static.append((name, mount))
            self.logger.info("[matrix] %s volume %s seeded, md5=%s",
                             label, name, self._baselines[name])

        # A clone carries its parent's bytes, so it is the cheapest check that
        # snapshot/clone on lblk preserves data across the same outages.
        parent = self._static[0][0]
        snap = f"mxsnap{random.randint(100, 999)}"
        snap_id = self._create_snapshot_dual(parent, snap)
        clone = f"mxclone{random.randint(100, 999)}"
        _dev, cmount = self._create_clone_dual(
            snap_id, clone, size=self.LVOL_SIZE,
            mount_path=f"/mnt/{clone}", format_disk=False)
        self._baselines[clone] = self._static_md5(clone, cmount)
        self._static.append((clone, cmount))
        self.logger.info("[matrix] clone %s of %s seeded, md5=%s",
                         clone, parent, self._baselines[clone])

        # Raw block device, stamped with crc32c rather than md5: no filesystem
        # in the way, which is the strongest integrity check available here and
        # the one the rest of the lblk suite gates on.
        raw = f"mxraw{random.randint(100, 999)}"
        self._create_and_connect(raw, pool)
        self._stamp_all()
        self.logger.info("[matrix] raw device %s stamped with crc32c", raw)

    def _provision_typed(self, name, pool, crypto=False, dhchap=False):
        """A formatted volume of the requested flavour, on either platform.

        Docker varies the lvol: crypto is a per-lvol flag, DHCHAP a property of
        the pool it lives in. K8s varies the StorageClass instead, because CSI
        is the only way in -- the parameters exist there (``encryption``,
        ``dhchap_node_label``) and map onto the same two product features.
        """
        label = "dhchap" if dhchap else ("crypto" if crypto else "plain")
        try:
            return self._provision_typed_inner(name, pool, crypto, dhchap)
        except Exception as exc:                      # noqa: BLE001
            # Encryption and DHCHAP on lblk have never been exercised. When one
            # cannot even be provisioned that IS the finding, so name the
            # flavour rather than leaving a bare "PVC not Bound within 300s"
            # that says nothing about which combination is unsupported.
            raise LblkPreconditionError(
                f"[matrix] could not provision a {label} volume on an lblk "
                f"cluster: {type(exc).__name__}: {exc}") from exc

    def _provision_typed_inner(self, name, pool, crypto, dhchap):
        if dhchap:
            pool = self._dhchap_pool()

        if not self.k8s_test:
            self._create_lvol_dual(name, self.LVOL_SIZE, pool_name=pool,
                                   crypto=crypto)
            _dev, mount = self._connect_and_mount_dual(
                name, mount_path=f"/mnt/{name}", format_disk=True)
            return mount

        k8s = self._ensure_k8s_utils()
        sc = self._typed_storage_class(crypto=crypto, dhchap=dhchap, pool=pool)
        pvc = self._k8s_normalize_name(name)
        k8s.create_pvc(name=pvc, size=self.LVOL_SIZE.replace("G", "Gi"),
                       storage_class=sc)
        k8s.wait_pvc_bound(pvc)
        self._volume_registry[name] = {"pvc_name": pvc, "mount": "/spdkvol"}
        # Deliberately NOT registered in _fs_volumes. _verify_all runs a write
        # workload over everything in there, and a static volume that gets
        # written to is no longer a static volume -- the md5 baseline would be
        # measuring the test's own IO.
        return "/spdkvol"

    def _dhchap_pool(self):
        """A DHCHAP-enabled pool, created once per run."""
        if getattr(self, "_dhchap_pool_name", None):
            return self._dhchap_pool_name
        self._dhchap_pool_name = f"mxdhchap{random.randint(100, 999)}"
        self._add_pool_dual(pool_name=self._dhchap_pool_name, dhchap=True)
        return self._dhchap_pool_name

    def _typed_storage_class(self, crypto, dhchap, pool):
        """StorageClass for a flavour, created on demand and reused."""
        key = f"crypto={crypto},dhchap={dhchap}"
        cache = getattr(self, "_sc_cache", None)
        if cache is None:
            cache = self._sc_cache = {}
        if key in cache:
            return cache[key]
        if not crypto and not dhchap:
            cache[key] = self._k8s_storage_class_name
            return cache[key]

        k8s = self._ensure_k8s_utils()
        name = f"{self._k8s_storage_class_name}-{'enc' if crypto else 'auth'}"
        label = None
        if dhchap:
            # Without the pool's node label the CSI driver provisions with no
            # nodeAffinity and DHCHAP is not actually enforced, so the lane
            # would pass while testing nothing.
            label = (f"simplyblock.io/pool.{k8s.namespace}."
                     f"{self.cluster_id}.{pool}")
        k8s.create_storage_class(
            name=name, cluster_id=self.cluster_id, pool_name=pool,
            ndcs=self.ndcs, npcs=self.npcs, encryption=crypto,
            dhchap_node_label=label)
        cache[key] = name
        self.logger.info("[matrix] StorageClass %s (%s)", name, key)
        return name

    def _write_static(self, lvol_name, mount):
        """Fill the volume once, with data nothing will touch again."""
        cmd = (f"dd if=/dev/urandom of={mount}/static.dat bs=1M "
               f"count={self.STATIC_MB} conv=fsync 2>&1 | tail -1")
        if self.k8s_test:
            k8s = self._ensure_k8s_utils()
            pvc = self._volume_registry[lvol_name]["pvc_name"]
            pod = f"seed-{pvc}"[:63]
            k8s.create_utility_pod(pod, pvc)
            try:
                k8s.wait_pod_running(pod)
                k8s.exec_in_pod(pod, cmd)
                k8s.exec_in_pod(pod, "sync")
            finally:
                k8s.delete_pod(pod, wait=True)
        else:
            node = self.client_machines[0]
            self.ssh_obj.exec_command(node=node, command=f"sudo {cmd}")
            self.ssh_obj.exec_command(node=node, command="sudo sync")

    def _assert_static_unchanged(self, context):
        """Every static volume must hash exactly as it did before any fault."""
        drifted = []
        for name, mount in self._static:
            expected = self._baselines[name]
            actual = self._static_md5(name, mount)
            if actual != expected:
                drifted.append(f"{name} {expected} -> {actual}")
        if drifted:
            raise LblkPreconditionError(
                f"[matrix] static data changed {context} -- nothing wrote to "
                f"these volumes after the baseline, so this is corruption, not "
                f"a workload artefact: {drifted}")
        self.logger.info("[matrix] %d static volume(s) unchanged %s",
                         len(self._static), context)

    def _verify_raw(self, context):
        """crc32c on the raw device only.

        _verify_all would also run a filesystem FIO over every registered
        formatted volume, which on this test means writing to the static set.
        The static md5 check above already covers those, and covers them more
        strictly than a fresh FIO would.
        """
        if not self.RAW_VERIFY:
            return
        for name, (client, dev) in self._lblk_devices.items():
            self._verifier.verify(client, dev,
                                  region_size=self.VERIFY_REGION,
                                  context=f"{context} [{name}]")

    def _static_pod(self, lvol_name):
        """A utility pod kept alive for the whole run, one per static volume.

        _generate_checksums_dual creates and deletes a pod per call. Across
        every outage type on every node that is cycles x volumes pod
        lifecycles -- on a six-node cluster, well over a hundred -- which would
        cost more wall-clock than the outages themselves. Registered in
        _k8s_utility_pods so the standard teardown removes it.
        """
        pods = getattr(self, "_static_pods", None)
        if pods is None:
            pods = self._static_pods = {}
        if lvol_name not in pods:
            k8s = self._ensure_k8s_utils()
            pvc = self._volume_registry[lvol_name]["pvc_name"]
            pod = f"md5-{pvc}"[:63]
            k8s.create_utility_pod(pod, pvc)
            k8s.wait_pod_running(pod)
            self._k8s_utility_pods.append(pod)
            pods[lvol_name] = pod
        return pods[lvol_name]

    def _static_md5(self, lvol_name, mount):
        """md5 of the one file on a static volume.

        Refuses to return anything that is not a digest. An md5sum that prints
        nothing -- pod gone, file missing, mount vanished -- would otherwise
        compare equal to the next empty result and report the data as
        unchanged, which is the failure mode this whole lane exists to catch.
        """
        path = f"{mount}/static.dat"
        cmd = f"md5sum {path}"
        if self.k8s_test:
            out, err = self._ensure_k8s_utils().exec_in_pod(
                self._static_pod(lvol_name), cmd)
        else:
            out, err = self.ssh_obj.exec_command(
                node=self.client_machines[0], command=f"sudo {cmd}")
        digest = (out or "").strip().split()[0] if (out or "").strip() else ""
        if len(digest) != 32:
            raise LblkPreconditionError(
                f"[matrix] could not hash {path} on {lvol_name}: md5sum gave "
                f"{out!r} / {err!r}. Refusing to treat an unreadable volume as "
                f"an unchanged one.")
        return digest

    # ── the availability lane ─────────────────────────────────────────────

    def _start_live_fio(self, pool, cycles):
        """FIO that must run, uninterrupted, across every outage.

        One volume per flavour, matching the static set. Encryption in
        particular belongs here rather than only in the static lane: the crypto
        layer sits in the IO path, so an encrypted volume carrying load while a
        node disappears is the most likely place for this to come apart, and a
        plain-only live lane would never touch it.
        """
        runtime = self._fio_runtime = cycles * self.SEC_PER_CYCLE + 600
        self.logger.info("[matrix] live FIO sized for %d cycles: %ds",
                         cycles, runtime)
        handles = []
        for label, opts in (("plain", dict()),
                            ("crypto", dict(crypto=True)),
                            ("dhchap", dict(dhchap=True))):
            name = f"mxlive{label}{random.randint(100, 999)}"
            mount = self._provision_typed(name, pool, **opts)
            log = (None if self.k8s_test
                   else f"{self.log_path}/fio_mx_{label}.log")
            handles.append((name, log, self._run_fio_dual(
                name, mount_path=mount, log_path=log,
                runtime=runtime, name=f"mxlive{label}",
                rw="randrw", bs="4K", numjobs=2, nrfiles=4, size="512M",
                time_based=True)))
            self.logger.info("[matrix] live FIO started on %s volume %s",
                             label, name)
        return handles

    def _assert_fio_alive(self, handles, outage):
        """FIO must still be running. A job that died is an interruption."""
        for name, _log, handle in handles:
            alive = (handle.is_alive() if hasattr(handle, "is_alive")
                     else bool(handle))
            if not alive:
                raise LblkPreconditionError(
                    f"[matrix] live FIO on {name} stopped during {outage}. "
                    f"With ndcs/npcs {self.ndcs}/{self.npcs} one node down is "
                    f"meant to be survivable, so IO ending here is a loss of "
                    f"availability, not an expected blip.")
        self.logger.info("[matrix] live FIO still running after %s", outage)

    def _finish_live_fio(self, handles):
        """Join, then hold every job to zero IO errors.

        Deliberately strict. Sudden node loss is the normal case in the field,
        and the cluster's erasure coding exists precisely so a client never
        sees it, so an io_u error during any outage -- graceful or not -- is a
        defect rather than something to triage away.
        """
        sleep_n_sec(10)
        for name, log, handle in handles:
            if hasattr(handle, "join"):
                handle.join(timeout=self._fio_runtime)
            if self.k8s_test:
                self._validate_fio_dual(handle)
            else:
                self.common_utils.validate_fio_test(
                    node=self.client_machines[0], log_file=log,
                    md5_severity=self._md5_severity)
            self.logger.info("[matrix] live FIO on %s completed clean", name)


class LblkOutageMatrixDocker(_LblkDockerMixin, _LblkOutageMatrix):
    """Availability and durability across every outage type, docker."""


class LblkOutageMatrixK8s(_LblkK8sMixin, _LblkOutageMatrix):
    """Availability and durability across every outage type, k8s-native."""
