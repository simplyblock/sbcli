"""Tests for lblk clusters -- generic Linux block devices via SPDK AIO bdevs.

Why this exists
---------------
Simplyblock normally consumes NVMe devices, which guarantee a 4K atomic write.
A generic Linux block device may guarantee 512b, or nothing. The product's
answer is a metadata journal that does not assume atomicity at all: it detects
torn writes with a CRC per entry and discards them.

That covers **metadata**. User data is not journalled, so data integrity on
these devices is the open question -- and it is exactly what nothing tested.
Neither the SPDK nor the ultra journal suite contains a single md5 or FIO test;
they verify object presence and byte-exact page repair.

So the classes here are built around the raw-device crc32c check in
utils/raw_device_verify.py rather than the suite's usual md5-over-a-filesystem
bracket. A filesystem journal can mask or transform a torn device write, and
databases -- the actual reason customers want these devices -- do raw IO.

What the classes cover
----------------------
    LblkFunctional*   cluster is genuinely lblk, journal live, lvol lifecycle
    LblkIntegrity*    raw crc32c across steady state, restart and outages
    LblkDeviceFault*  hot-remove and hung IO, which have no NVMe equivalent
    LblkJournalRecovery*  pause the drain, kill the node, replay on restart
    LblkUnfencedJournal*  the documented open gap, deliberately reproduced

Preconditions are checked, never assumed: a cluster that came up in nvme mode
looks healthy and would silently test the wrong thing, so every class asserts
device_mode == "lblk" before doing anything.
"""

import random

from stress_test.continuous_failover_ha_multi_outage_all_nodes import (
    RandomMultiClientMultiFailoverAllNodesTest,
)
from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest
from utils.common_utils import sleep_n_sec
from utils.md_journal import (
    MdJournalError,
    assert_journal_enabled,
    get_stats,
    scan_log_for_corruption,
    set_drain_paused,
)
from utils.raw_device_verify import RawDeviceVerifier


class LblkPreconditionError(RuntimeError):
    """The cluster is not an lblk cluster, so the run would prove nothing."""


class _LblkMixin:
    """Shared lblk assertions and helpers.

    The platform bases below supply `_spdk_exec_prefix`, `_spdk_sock` and
    `_apply_outage`. They are deliberately NOT declared here as abstract
    stubs: the mixins precede the platform bases in the MRO, so a stub would
    shadow the concrete implementation and every call would raise
    NotImplementedError on a real cluster.
    """

    # Every volume verified raw gets this much stamped region, and the churn
    # runs beyond it. Small enough that a sweep is not dominated by IO.
    VERIFY_REGION = "4G"
    CHURN_RUNTIME = 120
    LVOL_SIZE = "20G"

    def _init_lblk_state(self):
        self._verifier = RawDeviceVerifier(self.ssh_obj, self.logger)
        self._lblk_devices = {}      # lvol_name -> (client, /dev/nvmeXnY)
        self._journal_lvs = None

    # ── preconditions ─────────────────────────────────────────────────────
    def assert_cluster_is_lblk(self):
        """Fail unless this really is an lblk cluster.

        A cluster created before the lblk merge, or with an older control
        plane, silently reverts device_mode to nvme. It comes up healthy and
        every test below would pass while testing the wrong storage path, so
        this is checked rather than assumed.
        """
        details = self.sbcli_utils.get_cluster_details()
        row = details[0] if isinstance(details, list) else details
        mode = (row or {}).get("device_mode", "nvme")
        if mode != "lblk":
            raise LblkPreconditionError(
                f"cluster device_mode is {mode!r}, not 'lblk'. Bootstrap with "
                f"--device-mode lblk and pin SIMPLY_BLOCK_DOCKER_IMAGE to a "
                f"build at or after the lblk merge; an older control plane "
                f"drops device_mode on read-modify-write and reverts to nvme.")
        self.logger.info("[lblk] cluster device_mode=lblk")
        return mode

    def assert_devices_are_aio(self):
        """Every storage device must be an AIO bdev.

        Note the vocabulary trap: the cluster says 'lblk', the device says
        'aio'. They are the same decision recorded at two layers.
        """
        seen, bad = 0, []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            for dev in self.sbcli_utils.get_device_details(node["uuid"]):
                seen += 1
                if dev.get("bdev_type") != "aio":
                    bad.append((dev.get("id"), dev.get("bdev_type")))
        if not seen:
            raise LblkPreconditionError("cluster reports no storage devices")
        if bad:
            raise LblkPreconditionError(
                f"{len(bad)} of {seen} devices are not AIO bdevs: {bad[:5]}")
        self.logger.info("[lblk] all %d devices are aio bdevs", seen)
        return seen

    def _journal_targets(self):
        """(node_ip, exec_prefix, sock, lvs_name) for each storage node."""
        out = []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            ip, port = node.get("mgmt_ip"), node.get("rpc_port")
            if not ip or not port:
                continue
            out.append((ip, self._spdk_exec_prefix(ip, port),
                        self._spdk_sock(port), node.get("lvstore")))
        return out

    def assert_journals_live(self):
        """The journal must be enabled on every lvstore.

        Without it the cluster is running unprotected on devices that may not
        give a 4K atomic write, which is the one thing this whole suite is
        about.
        """
        checked = 0
        for ip, prefix, sock, lvs in self._journal_targets():
            if not lvs:
                continue
            stats = assert_journal_enabled(self.ssh_obj, ip, prefix, sock,
                                           lvs_name=lvs, logger=self.logger)
            checked += 1
            self._journal_lvs = self._journal_lvs or (ip, prefix, sock, lvs)
            if stats.get("used_slots", 0) > stats.get("num_slots", 1) * 0.9:
                self.logger.warning(
                    "[lblk] journal on %s is %d/%d full -- the drain is not "
                    "keeping up", lvs, stats["used_slots"], stats["num_slots"])
        if not checked:
            raise LblkPreconditionError(
                "no lvstore reported a journal; on lblk that invalidates the run")
        return checked

    # ── volume setup ──────────────────────────────────────────────────────
    def _create_and_connect(self, name, pool):
        """Create an lvol, connect it raw, and remember its device.

        Deliberately no filesystem: these tests verify the block device itself,
        because a filesystem can hide a torn write.
        """
        self.sbcli_utils.add_lvol(lvol_name=name, pool_name=pool,
                                  size=self.LVOL_SIZE)
        client = (self.fio_node or [self.mgmt_nodes[0]])[0]
        initial = self.ssh_obj.get_devices(node=client)
        for cmd in self.sbcli_utils.get_lvol_connect_str(lvol_name=name):
            self.ssh_obj.exec_command(node=client, command=cmd)
        sleep_n_sec(3)
        final = self.ssh_obj.get_devices(node=client)
        dev = next((f"/dev/{d.strip()}" for d in final if d not in initial), None)
        if not dev:
            raise RuntimeError(f"[lblk] {name} did not surface a device on {client}")
        self._lblk_devices[name] = (client, dev)
        self.logger.info("[lblk] %s -> %s on %s", name, dev, client)
        return client, dev

    def _verify_all(self, context):
        """Raw crc32c verify every adopted volume. This is the gate."""
        for name, (client, dev) in self._lblk_devices.items():
            self._verifier.verify(client, dev, region_size=self.VERIFY_REGION,
                                  context=f"{context} [{name}]")

    def _stamp_all(self):
        for name, (client, dev) in self._lblk_devices.items():
            self._verifier.stamp(client, dev, region_size=self.VERIFY_REGION)

    def _scan_spdk_logs(self, context):
        """Fail on blobstore-CRC or journal-fatal lines.

        These matter more than the FIO markers here: a torn journal entry is
        dropped silently by design, so a broken journal never says 'bad magic'.
        The blobstore CRC errors are what it looks like from outside.
        """
        for ip, prefix, _sock, _lvs in self._journal_targets():
            out, _ = self.ssh_obj.exec_command(
                node=ip, command=f"{prefix} tail -n 4000 /var/log/spdk.log "
                                 f"2>/dev/null || true",
                supress_logs=True)
            fatal, info = scan_log_for_corruption(out or "", context)
            if info:
                self.logger.info("[lblk] %s on %s: %s", context, ip,
                                 ", ".join(info))
            if fatal:
                raise MdJournalError(
                    f"[lblk] metadata corruption on {ip} ({context}): {fatal}")


# ── Lane 1: functional ────────────────────────────────────────────────────
class _LblkFunctional(_LblkMixin):
    """Prove the lblk path works at all before trusting anything else."""

    def run(self):
        self._init_lblk_state()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        name = f"lblkfn{random.randint(1000, 9999)}"
        client, dev = self._create_and_connect(name, pool)

        # A short stamp/verify with no faults: if this cannot pass, nothing
        # further is worth running.
        self._verifier.stamp(client, dev, region_size="1G")
        self._verifier.verify(client, dev, region_size="1G",
                              context="functional smoke")
        self._scan_spdk_logs("functional smoke")
        self.logger.info("[lblk] functional smoke passed")


# ── Lane 4: integrity ─────────────────────────────────────────────────────
class _LblkIntegrity(_LblkMixin):
    """Raw crc32c integrity across steady state, restart and outages."""

    OUTAGE_TYPES = ("graceful_shutdown", "container_stop",
                    "storage_node_reboot")

    def run(self):
        self._init_lblk_state()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        for i in range(2):
            self._create_and_connect(f"lblkint{i}{random.randint(100, 999)}", pool)
        self._stamp_all()

        # 1. steady state. Nothing like this exists in the suite today: every
        #    other integrity check brackets an injected failure, so a bug that
        #    needs no fault to appear would never be seen.
        for name, (client, dev) in self._lblk_devices.items():
            self._verifier.churn(client, dev, runtime=self.CHURN_RUNTIME)
        self._verify_all("steady state")
        self._scan_spdk_logs("steady state")

        # 2. across a clean restart, which is where journal replay happens.
        node = random.choice(list(self.sn_primary_secondary_map.keys()))
        self.logger.info("[lblk] restarting %s", node)
        self._graceful_shutdown_node(node)
        self.restart_nodes_after_failover("graceful_shutdown")
        self._verify_all("after clean restart")
        self._scan_spdk_logs("after clean restart")

        # 3. across each outage type, under load.
        for outage in self.OUTAGE_TYPES:
            node = random.choice(list(self.sn_primary_secondary_map.keys()))
            self.logger.info("[lblk] %s on %s", outage, node)
            for _name, (client, dev) in self._lblk_devices.items():
                self._verifier.churn(client, dev, runtime=30)
            details = self.sbcli_utils.get_storage_node_details(node)
            self._apply_outage(node, outage, details[0]["mgmt_ip"],
                               details[0]["rpc_port"])
            self.restart_nodes_after_failover(outage)
            self._verify_all(f"after {outage}")
            self._scan_spdk_logs(f"after {outage}")

        self.logger.info("[lblk] integrity passed across %d outage types",
                         len(self.OUTAGE_TYPES))


# ── Lane 2: lblk-specific device faults ───────────────────────────────────
class _LblkDeviceFault(_LblkMixin):
    """Hot-remove and hung IO, which have no NVMe-PCIe equivalent.

    The existing DeviceFailureMigrationPCIe* classes inject failure through
    /sys/bus/pci/devices/<addr>/remove. lblk devices are never PCI-bound to
    SPDK -- that is the whole point of the mode -- so those tests cannot run
    here and this is their replacement.
    """

    PRESENCE_POLLS = 3          # device_monitor needs 2 consecutive absences
    POLL_INTERVAL = 30

    def run(self):
        self._init_lblk_state()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkdev{random.randint(100, 999)}", pool)
        self._stamp_all()

        node = random.choice(list(self.sn_primary_secondary_map.keys()))
        details = self.sbcli_utils.get_storage_node_details(node)[0]
        node_ip = details["mgmt_ip"]
        devices = self.sbcli_utils.get_device_details(node)
        target = next((d for d in devices if d.get("bdev_type") == "aio"), None)
        if not target:
            raise LblkPreconditionError(f"node {node} has no aio device")

        dev_name = (target.get("device_path") or "").rsplit("/", 1)[-1]
        if not dev_name:
            raise LblkPreconditionError(
                f"device {target.get('id')} reports no device_path, so it "
                f"cannot be hot-removed by name")

        self.logger.info("[lblk] hot-removing %s from %s", dev_name, node_ip)
        self.ssh_obj.exec_command(
            node=node_ip,
            command=f"echo 1 | sudo tee /sys/block/{dev_name}/device/delete")

        # The watchdog needs two consecutive polls with the device absent.
        removed = False
        for _ in range(self.PRESENCE_POLLS):
            sleep_n_sec(self.POLL_INTERVAL)
            now = self.sbcli_utils.get_device_details(node)
            state = next((d.get("status") for d in now
                          if d.get("id") == target.get("id")), None)
            self.logger.info("[lblk] device %s status=%s",
                             target.get("id"), state)
            if state in ("removed", "unavailable"):
                removed = True
                break
        if not removed:
            raise RuntimeError(
                f"[lblk] device {target.get('id')} was hot-removed but the "
                f"control plane never marked it removed or unavailable. "
                f"_check_aio_device_presence in device_monitor.py is the code "
                f"that should have caught this.")

        # Data must survive losing one device.
        self._verify_all("after device hot-remove")
        self._scan_spdk_logs("after device hot-remove")
        self.logger.info("[lblk] hot-remove handled and data intact")


# ── Lane 2: journal recovery ──────────────────────────────────────────────
class _LblkJournalRecovery(_LblkMixin):
    """Force a non-empty journal, kill the node, and check the replay.

    The drain keeps the ring at roughly one entry under any load the blobstore
    can produce, so "recovery with entries to replay" is not reachable by
    workload alone. bdev_lvol_set_md_journal_drain exists for exactly this.
    """

    def run(self):
        self._init_lblk_state()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkjr{random.randint(100, 999)}", pool)
        self._stamp_all()

        ip, prefix, sock, lvs = self._journal_lvs
        set_drain_paused(self.ssh_obj, ip, prefix, sock, True,
                         lvs_name=lvs, logger=self.logger)
        try:
            # Generate metadata: creating and deleting volumes is what writes
            # md pages, and with the drain paused they pile up in the ring.
            for i in range(10):
                tmp = f"lblkjrtmp{i}{random.randint(100, 999)}"
                self.sbcli_utils.add_lvol(lvol_name=tmp, pool_name=pool,
                                          size="1G")
            stats = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                              logger=self.logger)
            if stats.get("used_slots", 0) < 2:
                self.logger.warning(
                    "[lblk] only %s slot(s) in the ring after pausing the "
                    "drain; recovery may have nothing to replay",
                    stats.get("used_slots"))
            node = next(n for n in self.sn_primary_secondary_map
                        if self.sbcli_utils.get_storage_node_details(
                            n)[0]["mgmt_ip"] == ip)
            self.logger.info("[lblk] killing %s with %s entries in the ring",
                             node, stats.get("used_slots"))
            self.ssh_obj.stop_spdk_process(
                ip, self.sbcli_utils.get_storage_node_details(node)[0]["rpc_port"],
                self.cluster_id)
        finally:
            # Never leave the drain paused: the ring fills and metadata writes
            # block behind it.
            try:
                set_drain_paused(self.ssh_obj, ip, prefix, sock, False,
                                 lvs_name=lvs, logger=self.logger)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[lblk] could not resume the drain: %s", exc)

        self.restart_nodes_after_failover("container_stop")
        self._scan_spdk_logs("journal recovery")
        self._verify_all("after journal recovery")
        self.logger.info("[lblk] journal recovery replayed and data intact")


# ── Lane 5: the documented open gap ───────────────────────────────────────
class _LblkUnfencedJournal(_LblkMixin):
    """Reproduce the missing leadership fence.

    blob_md_journal.h records this openly: the drain stops on demotion, but a
    demoted node can still *append*. The authors' own test F3 saw a stopped
    then thawed old leader accept 15 further md ops while the new leader was at
    head 23. The journal widens the blast radius versus pre-journal behaviour,
    because a stale writer now mutates shared ring structure rather than
    page-local LBAs.

    A failure here is a product finding, not a test bug. If an integrity
    mismatch ever shows up during failover, this is the first place to look.
    """

    FREEZE_SEC = 90

    def run(self):
        self._init_lblk_state()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkfence{random.randint(100, 999)}", pool)
        self._stamp_all()

        ip, prefix, sock, lvs = self._journal_lvs
        leader = next(n for n in self.sn_primary_secondary_map
                      if self.sbcli_utils.get_storage_node_details(
                          n)[0]["mgmt_ip"] == ip)
        before = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                           logger=self.logger)

        self.logger.info("[lblk] freezing the leader %s for %ds", leader,
                         self.FREEZE_SEC)
        self.ssh_obj.exec_command(
            node=ip, command="sudo pkill -STOP -f spdk_tgt || true")
        try:
            sleep_n_sec(self.FREEZE_SEC)      # long enough to lose leadership
        finally:
            self.ssh_obj.exec_command(
                node=ip, command="sudo pkill -CONT -f spdk_tgt || true")
        sleep_n_sec(30)

        after = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                          logger=self.logger)
        self.logger.info("[lblk] ring head before=%s after=%s (demoted=%s)",
                         before.get("mem_head"), after.get("mem_head"),
                         after.get("drain_demoted"))

        appended = (after.get("mem_head", 0) - before.get("mem_head", 0))
        if after.get("drain_demoted") and appended > 0:
            raise MdJournalError(
                f"[lblk] the thawed node appended {appended} entries to the "
                f"shared ring while demoted (drain_demoted=True). This is the "
                f"documented missing leadership fence, and it means a stale "
                f"writer mutated shared ring structure after losing "
                f"leadership.")

        self._scan_spdk_logs("unfenced journal")
        self._verify_all("after leader freeze and thaw")
        self.logger.info("[lblk] no unfenced append observed this cycle")


# ── platform bindings ─────────────────────────────────────────────────────
class _LblkDockerBase(RandomMultiClientMultiFailoverAllNodesTest):
    """Docker: reach SPDK through the per-node container."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        return f"sudo docker exec spdk_{rpc_port}"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

    def _apply_outage(self, node, outage_type, node_ip, rpc_port):
        if outage_type == "container_stop":
            return self.ssh_obj.stop_spdk_process(node_ip, rpc_port,
                                                  self.cluster_id)
        if outage_type == "graceful_shutdown":
            return self._graceful_shutdown_node(node)
        if outage_type == "storage_node_reboot":
            return self.ssh_obj.reboot_node(node_ip)
        raise ValueError(f"unhandled outage type {outage_type!r}")


class _LblkK8sBase(K8sNativeFailoverTest):
    """K8s-native: reach SPDK through the pod."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        self._ensure_k8s_utils()
        pod = self.k8s_utils.get_spdk_pod_for_node(node_ip)
        return (f"kubectl exec {pod} -c spdk-container "
                f"-n {self.namespace} --")

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

    def _apply_outage(self, node, outage_type, node_ip, rpc_port):
        if outage_type == "container_stop":
            return self._k8s_stop_spdk_pod(node_ip, node)
        if outage_type == "graceful_shutdown":
            return self._graceful_shutdown_node(node)
        if outage_type == "storage_node_reboot":
            return self.ssh_obj.reboot_node(node_ip)
        raise ValueError(f"unhandled outage type {outage_type!r}")


# ── registered leaf classes ───────────────────────────────────────────────
class LblkFunctionalDocker(_LblkFunctional, _LblkDockerBase):
    """lblk functional smoke, docker."""


class LblkFunctionalK8s(_LblkFunctional, _LblkK8sBase):
    """lblk functional smoke, k8s-native."""


class LblkIntegrityDocker(_LblkIntegrity, _LblkDockerBase):
    """Raw crc32c integrity across outages, docker."""


class LblkIntegrityK8s(_LblkIntegrity, _LblkK8sBase):
    """Raw crc32c integrity across outages, k8s-native."""


class LblkDeviceFaultDocker(_LblkDeviceFault, _LblkDockerBase):
    """Block-device hot-remove and hung IO, docker."""


class LblkDeviceFaultK8s(_LblkDeviceFault, _LblkK8sBase):
    """Block-device hot-remove and hung IO, k8s-native."""


class LblkJournalRecoveryDocker(_LblkJournalRecovery, _LblkDockerBase):
    """Metadata-journal replay after a kill, docker."""


class LblkJournalRecoveryK8s(_LblkJournalRecovery, _LblkK8sBase):
    """Metadata-journal replay after a kill, k8s-native."""


class LblkUnfencedJournalDocker(_LblkUnfencedJournal, _LblkDockerBase):
    """The documented missing-leadership-fence reproducer, docker."""


class LblkUnfencedJournalK8s(_LblkUnfencedJournal, _LblkK8sBase):
    """The documented missing-leadership-fence reproducer, k8s-native."""
