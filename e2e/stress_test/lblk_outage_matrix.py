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
            ("nsvol", dict(namespaced=True)),
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

    def _provision_typed(self, name, pool, crypto=False, dhchap=False,
                         namespaced=False):
        """A formatted volume of the requested flavour, on either platform.

        Docker varies the lvol: crypto and namespace packing are per-lvol
        flags, DHCHAP a property of the pool it lives in.

        K8s goes through CSI, so the flavour lives in the StorageClass -- but
        not uniformly. Crypto and namespaced use a class we build ourselves,
        which is fine because nothing about them depends on node identity.
        DHCHAP must use the class the OPERATOR generates for its pool: that
        one carries dhchap_node_label, and the nodeAffinity CSI writes from it
        is the thing that actually enforces allowedNodes at mount time. See
        _dhchap_k8s for why building our own there does not work.
        """
        label = ("dhchap" if dhchap else "crypto" if crypto
                 else "nsvol" if namespaced else "plain")
        try:
            return self._provision_typed_inner(name, pool, crypto, dhchap,
                                               namespaced)
        except Exception as exc:                      # noqa: BLE001
            # Encryption and DHCHAP on lblk have never been exercised. When one
            # cannot even be provisioned that IS the finding, so name the
            # flavour rather than leaving a bare "PVC not Bound within 300s"
            # that says nothing about which combination is unsupported.
            raise LblkPreconditionError(
                f"[matrix] could not provision a {label} volume on an lblk "
                f"cluster: {type(exc).__name__}: {exc}") from exc

    def _provision_typed_inner(self, name, pool, crypto, dhchap,
                               namespaced=False):
        if not self.k8s_test:
            # Docker: every flavour is an lvol flag or a property of its pool.
            kwargs = dict(lvol_name=name, size=self.LVOL_SIZE,
                          pool_name=(self._dhchap_pool() if dhchap else pool))
            if crypto:
                kwargs["crypto"] = True
            if namespaced:
                # Several namespaces packed into one subsystem, rather than a
                # subsystem per lvol.
                kwargs["namespace"] = True
                kwargs["max_namespace_per_subsys"] = 4
            self.sbcli_utils.add_lvol(**kwargs)
            _dev, mount = self._connect_and_mount_dual(
                name, mount_path=f"/mnt/{name}", format_disk=True)
            return mount

        k8s = self._ensure_k8s_utils()
        if crypto:
            self._assert_kms_usable()
        if dhchap:
            sc, pin = self._dhchap_k8s()
        else:
            sc, pin = self._typed_storage_class(crypto, namespaced), None

        pvc = self._k8s_normalize_name(name)
        k8s.create_pvc(name=pvc, size=self.LVOL_SIZE.replace("G", "Gi"),
                       storage_class=sc)
        if pin:
            # The operator's DHCHAP class is WaitForFirstConsumer, so the PVC
            # stays Pending until something is scheduled against it -- and it
            # must be scheduled on an allowed node, because CSI writes a
            # matching nodeAffinity onto the PV. Bind it with a throwaway pod
            # pinned there.
            binder = f"bind-{pvc}"[:63]
            k8s.create_utility_pod(binder, pvc, node_selector=pin)
            try:
                k8s.wait_pod_running(binder)
            finally:
                k8s.delete_pod(binder, wait=True)
        k8s.wait_pvc_bound(pvc)
        self._volume_registry[name] = {"pvc_name": pvc, "mount": "/spdkvol",
                                       "node_selector": pin}
        # Deliberately NOT registered in _fs_volumes. _verify_all runs a write
        # workload over everything in there, and a static volume that gets
        # written to is no longer a static volume -- the md5 baseline would be
        # measuring the test's own IO.
        return "/spdkvol"

    def _assert_kms_usable(self):
        """An encrypted volume needs KMS, so check it before asking for one.

        Without this the failure is a 300s PVC timeout whose only detail is
        "not Bound", and the cause sits three layers down: the CSI driver's
        CreateVolume gets POST 500 from the control plane, whose log shows
        KMSException("Authentication failed") from _hcp.py -- because openbao
        is sealed.

        It re-seals on its own. The seal type is shamir with no auto-unseal,
        so any fresh openbao process comes up sealed, and the workflow unseals
        only once at setup. On 2026-09-20 the pod was recreated after setup
        (restartCount 0 but newer than its namespace), came back sealed, and
        every encrypted volume failed from then on.

        Checking pod readiness rather than the seal status directly: openbao's
        readiness probe already tracks sealed state, and `bao status` needs
        BAO_SKIP_VERIFY here because the server certificate carries no IP SAN.
        """
        k8s = self._ensure_k8s_utils()
        out, _err = k8s._exec_kubectl(
            "kubectl get pods -n vault -l app.kubernetes.io/name=openbao "
            "--no-headers -o custom-columns=NAME:.metadata.name,"
            "READY:.status.containerStatuses[0].ready 2>/dev/null || true")
        rows = [ln.split() for ln in (out or "").splitlines() if ln.strip()]
        if not rows:
            raise LblkPreconditionError(
                "[matrix] no openbao pod in namespace vault, so KMS cannot "
                "serve an encryption key and every crypto volume will fail to "
                "provision. Run the workflow's 'Setup KMS (vault)' step, or "
                "drop the crypto flavour for this run.")
        unready = [r[0] for r in rows if len(r) < 2 or r[1] != "true"]
        if unready:
            raise LblkPreconditionError(
                f"[matrix] openbao {unready} is not ready, which on a shamir "
                f"seal means sealed. The control plane will answer CreateVolume "
                f"with POST 500 (KMSException: Authentication failed) for every "
                f"encrypted volume. Unseal it -- the keys from setup are in "
                f"openbao-init.txt on the runner -- then re-run.")
        self.logger.info("[matrix] KMS ready: %s",
                         " ".join(r[0] for r in rows))

    def _pin_for(self, lvol_name):
        """nodeSelector a pod touching this volume must carry, or None."""
        return (self._volume_registry.get(lvol_name) or {}).get("node_selector")

    def _dhchap_pool(self):
        """A DHCHAP-enabled pool, created once per run (docker)."""
        if getattr(self, "_dhchap_pool_name", None):
            return self._dhchap_pool_name
        self._dhchap_pool_name = f"mxdhchap{random.randint(100, 999)}"
        self._add_pool_dual(pool_name=self._dhchap_pool_name, dhchap=True)
        return self._dhchap_pool_name

    def _dhchap_k8s(self):
        """DHCHAP on k8s, the operator's own path. Returns (sc, nodeSelector).

        The first attempt created a second pool with dhchap=True and no
        allowedNodes, then built its own StorageClass. The operator never
        reconciled it -- "Pool not visible in sbcli after 300s" -- because a
        DHCHAP pool with no allowedNodes has no hosts to authorise. Four
        things are required, and the security suite established all of them:

        1. allowedNodes must be a real subset, so the operator can derive each
           allowed node's NQN and label those nodes.
        2. The StorageClass has to be the operator's, not ours: it carries
           dhchap_node_label, which is what makes CSI write a nodeAffinity
           onto every PV, and that is what enforces the restriction at mount.
        3. The CSI node plugin snapshots node labels as topology keys only at
           REGISTRATION, so a pool created while it is already running is
           invisible to it and every PVC fails on topology. The daemonset has
           to be restarted once per pool.
        4. That class is WaitForFirstConsumer, so a PVC does not bind until a
           pod is scheduled against it on an allowed node.
        """
        cached = getattr(self, "_dhchap_k8s_cache", None)
        if cached:
            return cached
        k8s = self._ensure_k8s_utils()

        workers = self._k8s_worker_names()
        if len(workers) < 2:
            raise LblkPreconditionError(
                f"[matrix] DHCHAP needs at least two workers so allowedNodes "
                f"can be a strict subset; saw {workers}")
        allowed = workers[:-1]

        pool = f"mxdhchap{random.randint(100, 999)}"
        actual = k8s.add_storage_pool(
            pool_name=pool, cluster_id=self.cluster_id, dhchap=True,
            allowed_nodes=allowed,
            storage_class_parameters={"filesystem": "ext4"})
        pool = actual or pool

        crd = self._k8s_pool_crd_name(pool)
        sc = k8s.operator_storage_class_name(crd)
        if not k8s.wait_storage_class_exists(sc):
            raise LblkPreconditionError(
                f"[matrix] the operator did not generate StorageClass {sc!r} "
                f"for DHCHAP pool {crd!r}; nothing to provision from")

        label = f"simplyblock.io/pool.{k8s.namespace}.{self.cluster_id}.{crd}"
        if not k8s.restart_csi_node_driver(expect_topology_key=label,
                                           expect_on_nodes=allowed):
            self.logger.warning(
                "[matrix] CSI re-register did not surface %r as a topology "
                "key; DHCHAP provisioning may fail on allowedTopologies",
                label)

        pin = f"{label}=allowed"
        self.logger.info("[matrix] DHCHAP pool %s: allowed=%s, sc=%s",
                         crd, allowed, sc)
        self._dhchap_k8s_cache = (sc, pin)
        self._dhchap_allowed = allowed
        return self._dhchap_k8s_cache

    def _k8s_worker_names(self):
        k8s = self._ensure_k8s_utils()
        out, _ = k8s._exec_kubectl(
            "kubectl get nodes -l node-role.kubernetes.io/control-plane!= "
            "--no-headers -o custom-columns=NAME:.metadata.name")
        return [ln.strip() for ln in (out or "").splitlines() if ln.strip()]

    def _k8s_pool_crd_name(self, pool_name):
        """The StoragePool CRD name the operator built its label from.

        Not necessarily the backend pool name: the CRD name can pick up a
        suffix or be truncated to fit the 63-char label budget, and the label
        is derived from the CRD name, not from the backend name.
        """
        try:
            details = self.sbcli_utils.get_pool_by_id(
                self.sbcli_utils.get_pool_id(pool_name))
            if isinstance(details, list):
                details = details[0] if details else {}
            cr_name = (details or {}).get("cr_name")
            if cr_name:
                return cr_name
        except Exception as exc:                      # noqa: BLE001
            self.logger.info("[matrix] cr_name unavailable (%s); assuming the "
                             "CRD is named after the pool", str(exc)[:100])
        return pool_name

    def _typed_storage_class(self, crypto, namespaced):
        """Our own StorageClass for the non-DHCHAP flavours.

        Safe to build here, unlike DHCHAP: no node label is involved, so
        nothing depends on the operator's generated class, and ours is
        volumeBindingMode: Immediate, which keeps provisioning simple.
        """
        key = f"crypto={crypto},ns={namespaced}"
        cache = getattr(self, "_sc_cache", None)
        if cache is None:
            cache = self._sc_cache = {}
        if key in cache:
            return cache[key]
        if not crypto and not namespaced:
            cache[key] = self._k8s_storage_class_name
            return cache[key]

        k8s = self._ensure_k8s_utils()
        name = f"{self._k8s_storage_class_name}-{'enc' if crypto else 'ns'}"
        k8s.create_storage_class(
            name=name, cluster_id=self.cluster_id, pool_name=self.pool_name,
            ndcs=self.ndcs, npcs=self.npcs, encryption=crypto,
            max_namespace_per_subsys=(4 if namespaced else 1))
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
            # A DHCHAP volume's PV carries a nodeAffinity for the pool's
            # allowed nodes. An unpinned pod can land elsewhere and fail to
            # mount, which would look like a storage fault rather than a
            # scheduling one.
            k8s.create_utility_pod(pod, pvc,
                                   node_selector=self._pin_for(lvol_name))
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
            k8s.create_utility_pod(pod, pvc,
                                   node_selector=self._pin_for(lvol_name))
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
                            ("dhchap", dict(dhchap=True)),
                            ("nsvol", dict(namespaced=True))):
            name = f"mxlive{label}{random.randint(100, 999)}"
            mount = self._provision_typed(name, pool, **opts)
            log = (None if self.k8s_test
                   else f"{self.log_path}/fio_mx_{label}.log")
            handles.append((name, log, self._run_fio_dual(
                name, mount_path=mount, log_path=log,
                runtime=runtime, name=f"mxlive{label}",
                rw="randrw", bs="4K", numjobs=2, nrfiles=4, size="512M",
                time_based=True, node_selector=self._pin_for(name))))
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
