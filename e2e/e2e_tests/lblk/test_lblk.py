"""Functional and integration tests for lblk clusters.

lblk backs storage with generic Linux block devices through SPDK AIO bdevs
instead of NVMe. NVMe guarantees a 4K atomic write; a generic block device may
guarantee 512b, or nothing. The product's answer is a metadata journal that
does not assume atomicity at all -- it detects torn writes with a per-entry CRC
and discards them.

That covers **metadata**. User data is not journalled, so data integrity on
these devices is the open question, and it is exactly what nothing tested:
neither the SPDK nor the ultra journal suite contains a single md5 or FIO test.

So these tests are built on the raw-device crc32c check in
utils/raw_device_verify.py rather than the suite's usual md5-over-a-filesystem
bracket. A filesystem journal can mask or transform a torn device write, and
databases -- the reason customers want these devices -- do raw IO.

These are finite scenario tests on TestClusterBase, in the same shape as
TestMultiNodeOutage*. The open-ended soak lives in stress_test/lblk_stress.py.
"""

import random
import re

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.md_journal import (
    MdJournalError,
    assert_journal_enabled,
    get_stats,
    scan_log_for_corruption,
    set_drain_paused,
)
from utils.raw_device_verify import RawDeviceVerifier


def _snake_case(name):
    """LblkFunctionalDocker -> lblk_functional_docker."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class LblkPreconditionError(RuntimeError):
    """The cluster is not an lblk cluster, so the run would prove nothing."""


class _LblkBase(TestClusterBase):
    """Shared lblk assertions, volume setup and raw verification.

    Outages go through sbcli_utils rather than the stress-suite helpers, so
    these stay on TestClusterBase like the rest of e2e_tests.
    """

    VERIFY_REGION = "4G"
    CHURN_RUNTIME = 120
    LVOL_SIZE = "20G"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        # TestClusterBase defaults test_name to "" and builds the run's log
        # directory as f"{test_name}-{timestamp}". Leaving it unset put every
        # lblk run in "<nfs>/-20260915-201040" -- a leading dash and no way to
        # tell one test's logs from another's, and the "Logs Path:" line the
        # workflow summary greps came back nameless too.
        #
        # Derived from the class rather than hardcoded in each of the ten leaf
        # classes, so a new one cannot be added without a name by forgetting a
        # line. Same source as the test_name already passed at setup().
        self.test_name = _snake_case(type(self).__name__)
        self._verifier = None
        self._lblk_devices = {}      # lvol_name -> (client, /dev/nvmeXnY)
        self._journal_lvs = None

    # ── platform hooks ────────────────────────────────────────────────────
    # Supplied by the Docker/K8s leaf classes. Deliberately not declared as
    # abstract stubs here: a stub on the base would be found first by any leaf
    # that mixes the platform in later, and would shadow the real one.

    def _init_lblk(self):
        self._verifier = RawDeviceVerifier(self.ssh_obj, self.logger)
        self._lblk_devices = {}
        self._journal_lvs = None

    # ── preconditions ─────────────────────────────────────────────────────
    def assert_cluster_is_lblk(self):
        """Fail unless this really is an lblk cluster.

        A cluster created against an older control plane silently reverts
        device_mode to nvme. It comes up healthy, and every assertion below
        would pass while testing the wrong storage path entirely.
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

        Vocabulary trap worth remembering: the cluster says 'lblk', the device
        says 'aio'. Same decision, recorded at two layers.
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

        Without it the cluster runs unprotected on devices that may not give a
        4K atomic write, which is the whole reason these tests exist.
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

    # ── volumes ───────────────────────────────────────────────────────────
    def _create_and_connect(self, name, pool):
        """Create an lvol and connect it raw -- no filesystem, on purpose."""
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

    def _stamp_all(self):
        for _name, (client, dev) in self._lblk_devices.items():
            self._verifier.stamp(client, dev, region_size=self.VERIFY_REGION)

    def _verify_all(self, context):
        """Raw crc32c verify every volume. This is the gate."""
        for name, (client, dev) in self._lblk_devices.items():
            self._verifier.verify(client, dev, region_size=self.VERIFY_REGION,
                                  context=f"{context} [{name}]")

    def _churn_all(self, runtime=None):
        for _name, (client, dev) in self._lblk_devices.items():
            self._verifier.churn(client, dev,
                                 runtime=runtime or self.CHURN_RUNTIME)

    def _scan_spdk_logs(self, context):
        """Fail on blobstore-CRC or journal-fatal lines.

        These matter more than the FIO markers: a torn journal entry is dropped
        silently by design, so a broken journal never reports "bad magic". The
        blobstore CRC errors are what it looks like from outside.
        """
        for ip, prefix, _sock, _lvs in self._journal_targets():
            out, _ = self.ssh_obj.exec_command(
                node=ip,
                command=f"{prefix} tail -n 4000 /var/log/spdk.log 2>/dev/null || true",
                supress_logs=True)
            fatal, info = scan_log_for_corruption(out or "", context)
            if info:
                self.logger.info("[lblk] %s on %s: %s", context, ip,
                                 ", ".join(info))
            if fatal:
                raise MdJournalError(
                    f"[lblk] metadata corruption on {ip} ({context}): {fatal}")

    def assert_journal_recovered(self, ip, prefix, min_entries=1):
        """Fail unless SPDK reports replaying entries from the ring.

        Without this the recovery test proves only that a killed node comes
        back and its data still verifies -- which a cluster with no journal at
        all would also manage. The replay is the thing under test, so it has to
        be read out of SPDK's own log rather than assumed from a clean verify.

        SPDK emits one of (blob_md_journal.c):
            "md journal recovery: N entries to drain (tail=T head=H)"
            "md journal recovery: ring empty"
            "md journal recovery: ring fully valid, order ambiguous"
            "md journal recovery failed: ..."
        "ring empty" is the dangerous one: it is not an error, the node comes up
        healthy, and the run would have proved nothing.
        """
        out, _ = self.ssh_obj.exec_command(
            node=ip,
            command=f"{prefix} tail -n 8000 /var/log/spdk.log 2>/dev/null || true",
            supress_logs=True)
        text = out or ""

        if "md journal recovery failed" in text:
            line = next((ln for ln in text.splitlines()
                         if "md journal recovery failed" in ln), "")
            raise MdJournalError(f"[lblk] journal recovery failed on {ip}: {line.strip()}")

        drained = re.findall(r"md journal recovery: (\d+) entries to drain"
                             r"(?: \(tail=(\d+) head=(\d+)\))?", text)
        if drained:
            total = sum(int(n) for n, _t, _h in drained)
            self.logger.info("[lblk] journal recovery on %s replayed %d "
                             "entries across %d lvstore(s): %s",
                             ip, total, len(drained), drained)
            if total < min_entries:
                raise MdJournalError(
                    f"[lblk] journal recovery on {ip} replayed {total} "
                    f"entries, expected at least {min_entries}")
            return total

        if "md journal recovery: ring empty" in text:
            raise MdJournalError(
                f"[lblk] journal recovery on {ip} found an EMPTY ring. The "
                f"node was killed with entries in it, so either they never "
                f"reached the disk or recovery did not read them. The node "
                f"comes up healthy either way, which is why this is checked "
                f"rather than inferred from a clean data verify.")

        raise MdJournalError(
            f"[lblk] no 'md journal recovery:' line on {ip} after restart. "
            f"Either the log rotated past it or recovery did not run.")

    # ── outages, through the API rather than the stress helpers ───────────
    def _journal_heads(self):
        """lvstore -> mem_head, sampled on every node.

        mem_head advances monotonically as entries are appended. used_slots is
        the wrong thing to watch: the drain runs continuously and returns it to
        ~0 between samples, which is exactly why the first passing lblk run
        reported 0/8192 slots used and looked as though the journal had never
        been touched.
        """
        heads = {}
        for ip, prefix, sock, lvs in self._journal_targets():
            if not lvs:
                continue
            try:
                st = get_stats(self.ssh_obj, ip, prefix, sock,
                               lvs_name=lvs, logger=None)
                heads[lvs] = (st or {}).get("mem_head")
            except MdJournalError as exc:
                self.logger.warning("[md-journal] could not sample %s: %s",
                                    lvs, exc)
        return heads

    def _fs_fio(self, lvol_name, mount, tag, runtime=60):
        """Filesystem FIO on a mounted clone, run to completion.

        md5 is demoted to a warning here and only here. SshUtils.run_fio_test
        hardcodes --verify=md5 and auto-enables verify_backlog for mixed
        workloads, which its own comment says "bypasses the rand_seed check" --
        so on a device with no 4K atomic-write guarantee an md5 mismatch is as
        likely to be an artefact of the harness as a real defect. The raw crc32c
        verify on the parent lvol stays the integrity gate precisely because it
        has no filesystem in the way and no overlapping IO by construction.

        Demoting is not ignoring: the mismatch is still logged, and the
        .hdr_fail dumps are still collected for triage.
        """
        log = None
        if not self.k8s_test:
            log = f"{self.log_path}/fio_lblk_{tag}.log"

        handle = self._run_fio_dual(
            lvol_name, mount_path=mount, log_path=log, runtime=runtime,
            name=f"lblk{tag}", rw="randrw", bs="4K", numjobs=2, nrfiles=4,
            size="512M")
        if hasattr(handle, "join"):
            handle.join()

        if self.k8s_test:
            self._validate_fio_dual(handle)
        else:
            self.common_utils.validate_fio_test(
                node=self.client_machines[0], log_file=log,
                md5_severity="warning")
        self.logger.info("[lblk] filesystem FIO clean on clone %s", lvol_name)

    def _metadata_churn(self, lvol_name, tag):
        """Snapshot + clone, and report what it did to the journal.

        Snapshot and clone are the metadata-heavy operations, so this is what
        actually puts entries in the ring. Without it an lblk run exercises the
        data path only and the journal -- the whole reason the mode is safe on
        a device with no atomic-write guarantee -- is never written to.
        """
        before = self._journal_heads()

        snap = f"snap{tag}{random.randint(100, 999)}"
        snap_id = self._create_snapshot_dual(lvol_name, snap)
        clone = f"clone{tag}{random.randint(100, 999)}"
        # Formatted and mounted, unlike the raw lvols above: this one exists to
        # carry filesystem FIO, which is the workload every other suite runs and
        # the one a real user's application looks like. The raw crc32c check on
        # the parent stays the integrity gate; this is coverage of the ordinary
        # path on top of it.
        _dev, mount = self._create_clone_dual(
            snap_id, clone, size=self.LVOL_SIZE,
            mount_path=f"/mnt/{clone}", format_disk=True)
        self._fs_fio(clone, mount, tag)

        after = self._journal_heads()
        moved = {k: (before.get(k), v) for k, v in after.items()
                 if before.get(k) != v}
        if moved:
            self.logger.info("[md-journal] %s: head advanced on %d lvstore(s): "
                             "%s", tag, len(moved), moved)
        else:
            # Deliberately not fatal. That metadata operations must advance the
            # ring is a reasonable expectation, not a contract this suite has
            # verified, and failing the run on it would be inventing one. If
            # this line shows up on every run, escalate it -- it would mean the
            # journal is enabled and inert.
            self.logger.warning(
                "[md-journal] %s: snapshot+clone advanced NO lvstore head "
                "(before=%s after=%s). The journal is enabled but may not be "
                "receiving entries.", tag, before, after)
        return snap, clone

    def _any_storage_node(self):
        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        if len(nodes) < 2:
            raise LblkPreconditionError(
                f"need at least 2 storage nodes to take one down, have {len(nodes)}")
        return random.choice(nodes)

    def _outage_and_recover(self, node, outage_type):
        """Take one node down the requested way and bring it back."""
        uuid, ip = node["uuid"], node["mgmt_ip"]
        self.logger.info("[lblk] %s on %s", outage_type, uuid)
        self.ssh_obj.notify_outage_started([ip])

        if outage_type == "graceful_shutdown":
            self.sbcli_utils.shutdown_node(node_uuid=uuid)
        elif outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(ip, node["rpc_port"], self.cluster_id)
        elif outage_type == "storage_node_reboot":
            self.ssh_obj.reboot_node(ip)
        else:
            raise ValueError(f"unhandled outage type {outage_type!r}")

        self.sbcli_utils.wait_for_storage_node_status(uuid, "offline", timeout=600)
        self.sbcli_utils.restart_node(node_uuid=uuid)
        self.sbcli_utils.wait_for_storage_node_status(uuid, "online", timeout=900)
        self.sbcli_utils.wait_for_health_status(uuid, True, timeout=300)
        self.logger.info("[lblk] %s recovered", uuid)


# ── Docker / K8s platform bindings ────────────────────────────────────────
class _LblkDockerMixin:
    """Reach SPDK through the per-node docker container."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        return f"sudo docker exec spdk_{rpc_port}"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"


class _LblkK8sMixin:
    """Reach SPDK through the pod."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        pod = self.k8s_utils.get_spdk_pod_for_node(node_ip)
        return f"kubectl exec {pod} -c spdk-container -n {self.namespace} --"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"


# ── functional ────────────────────────────────────────────────────────────
class _LblkFunctional(_LblkBase):
    """Prove the lblk path works at all before trusting anything else."""

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        name = f"lblkfn{random.randint(1000, 9999)}"
        client, dev = self._create_and_connect(name, pool)

        self._verifier.stamp(client, dev, region_size="1G")
        self._verifier.verify(client, dev, region_size="1G",
                              context="functional smoke")
        self._scan_spdk_logs("functional smoke")
        self.logger.info("[lblk] functional smoke passed")


# ── integration: integrity across faults ──────────────────────────────────
class _LblkIntegrity(_LblkBase):
    """Raw crc32c integrity across steady state, restart and outages."""

    OUTAGE_TYPES = ("graceful_shutdown", "container_stop", "storage_node_reboot")

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        names = []
        for i in range(2):
            name = f"lblkint{i}{random.randint(100, 999)}"
            self._create_and_connect(name, pool)
            names.append(name)
        self._stamp_all()

        # Steady state first. Nothing in the suite does this today: every other
        # integrity check brackets an injected failure, so a bug needing no
        # fault at all would never be seen.
        self._churn_all()
        self._verify_all("steady state")
        self._scan_spdk_logs("steady state")

        # Metadata before any fault, so the ring is non-empty going into the
        # first outage. A snapshot/clone taken only after an outage would leave
        # recovery replaying an empty journal, which proves nothing.
        self._metadata_churn(names[0], "steady")
        self._verify_all("after snapshot+clone")
        self._scan_spdk_logs("after snapshot+clone")

        for n, outage in enumerate(self.OUTAGE_TYPES):
            self._churn_all(runtime=30)
            # Alternate which lvol carries the metadata work so both the
            # primary and the secondary lvstore see some.
            self._metadata_churn(names[n % len(names)], f"o{n}")
            self._outage_and_recover(self._any_storage_node(), outage)
            self._verify_all(f"after {outage}")
            self._scan_spdk_logs(f"after {outage}")

        self.logger.info("[lblk] integrity held across %d outage types, with "
                         "snapshot+clone before each", len(self.OUTAGE_TYPES))


# ── integration: device faults with no NVMe equivalent ────────────────────
class _LblkDeviceFault(_LblkBase):
    """Hot-remove of a backing block device.

    The existing DeviceFailureMigrationPCIe* classes inject failure through
    /sys/bus/pci/devices/<addr>/remove. lblk devices are never PCI-bound to
    SPDK -- that is the point of the mode -- so those tests cannot run here and
    this is their replacement.
    """

    PRESENCE_POLLS = 3          # the watchdog needs 2 consecutive absences
    POLL_INTERVAL = 30

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkdev{random.randint(100, 999)}", pool)
        self._stamp_all()

        node = self._any_storage_node()
        devices = self.sbcli_utils.get_device_details(node["uuid"])
        target = next((d for d in devices if d.get("bdev_type") == "aio"), None)
        if not target:
            raise LblkPreconditionError(f"node {node['uuid']} has no aio device")
        dev_name = (target.get("device_path") or "").rsplit("/", 1)[-1]
        if not dev_name:
            raise LblkPreconditionError(
                f"device {target.get('id')} reports no device_path, so it "
                f"cannot be hot-removed by name")

        self.logger.info("[lblk] hot-removing %s from %s", dev_name,
                         node["mgmt_ip"])
        self.ssh_obj.exec_command(
            node=node["mgmt_ip"],
            command=f"echo 1 | sudo tee /sys/block/{dev_name}/device/delete")

        removed = False
        for _ in range(self.PRESENCE_POLLS):
            sleep_n_sec(self.POLL_INTERVAL)
            now = self.sbcli_utils.get_device_details(node["uuid"])
            state = next((d.get("status") for d in now
                          if d.get("id") == target.get("id")), None)
            self.logger.info("[lblk] device %s status=%s", target.get("id"), state)
            if state in ("removed", "unavailable"):
                removed = True
                break
        if not removed:
            raise RuntimeError(
                f"[lblk] device {target.get('id')} was hot-removed but the "
                f"control plane never marked it removed or unavailable. "
                f"_check_aio_device_presence in device_monitor.py is what "
                f"should have caught this.")

        self._verify_all("after device hot-remove")
        self._scan_spdk_logs("after device hot-remove")
        self.logger.info("[lblk] hot-remove handled and data intact")


# ── integration: journal replay ───────────────────────────────────────────
class _LblkJournalRecovery(_LblkBase):
    """Force a non-empty ring, kill the node, check the replay.

    The drain keeps the ring at roughly one entry under any load the blobstore
    can produce, so "recovery with entries to replay" is unreachable by
    workload alone. bdev_lvol_set_md_journal_drain exists for exactly this.
    """

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkjr{random.randint(100, 999)}", pool)
        self._stamp_all()

        ip, prefix, sock, lvs = self._journal_lvs
        node = next(n for n in self.sbcli_utils.get_storage_nodes()["results"]
                    if n.get("mgmt_ip") == ip)

        set_drain_paused(self.ssh_obj, ip, prefix, sock, True,
                         lvs_name=lvs, logger=self.logger)
        try:
            for i in range(10):
                self.sbcli_utils.add_lvol(
                    lvol_name=f"lblkjrtmp{i}{random.randint(100, 999)}",
                    pool_name=pool, size="1G")
            stats = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                              logger=self.logger)
            if stats.get("used_slots", 0) < 2:
                self.logger.warning(
                    "[lblk] only %s slot(s) in the ring after pausing the "
                    "drain; recovery may have nothing to replay",
                    stats.get("used_slots"))
            self.logger.info("[lblk] killing %s with %s entries in the ring",
                             node["uuid"], stats.get("used_slots"))
            self.ssh_obj.stop_spdk_process(ip, node["rpc_port"], self.cluster_id)
        finally:
            # Never leave the drain paused: the ring fills and metadata writes
            # block behind it.
            try:
                set_drain_paused(self.ssh_obj, ip, prefix, sock, False,
                                 lvs_name=lvs, logger=self.logger)
            except Exception as exc:                  # noqa: BLE001
                self.logger.warning("[lblk] could not resume the drain: %s", exc)

        self.sbcli_utils.restart_node(node_uuid=node["uuid"])
        self.sbcli_utils.wait_for_storage_node_status(node["uuid"], "online",
                                                      timeout=900)
        self._scan_spdk_logs("journal recovery")
        # The exec prefix has to be rebuilt: the container the node was killed
        # in is gone, and the restarted one is what carries the recovery log.
        replayed = self.assert_journal_recovered(
            ip, self._spdk_exec_prefix(ip, node["rpc_port"]),
            min_entries=1)
        self._verify_all("after journal recovery")
        self.logger.info("[lblk] journal recovery replayed %d entries and "
                         "data is intact", replayed)


# ── integration: the documented open gap ──────────────────────────────────
class _LblkUnfencedJournal(_LblkBase):
    """Reproduce the missing leadership fence.

    blob_md_journal.h records this openly: the drain stops on demotion, but a
    demoted node can still *append*. Their own test F3 saw a stopped-then-thawed
    old leader accept 15 further md ops while the new leader was at head 23.
    The journal widens the blast radius versus pre-journal behaviour, because a
    stale writer now mutates shared ring structure rather than page-local LBAs.

    A failure here is a product finding, not a test bug. If an integrity
    mismatch ever appears during failover, look here first.
    """

    FREEZE_SEC = 90

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkfence{random.randint(100, 999)}", pool)
        self._stamp_all()

        ip, prefix, sock, lvs = self._journal_lvs
        before = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                           logger=self.logger)

        self.logger.info("[lblk] freezing the leader on %s for %ds", ip,
                         self.FREEZE_SEC)
        self.ssh_obj.exec_command(
            node=ip, command="sudo pkill -STOP -f spdk_tgt || true")
        try:
            sleep_n_sec(self.FREEZE_SEC)     # long enough to lose leadership
        finally:
            self.ssh_obj.exec_command(
                node=ip, command="sudo pkill -CONT -f spdk_tgt || true")
        sleep_n_sec(30)

        after = get_stats(self.ssh_obj, ip, prefix, sock, lvs_name=lvs,
                          logger=self.logger)
        self.logger.info("[lblk] ring head before=%s after=%s (demoted=%s)",
                         before.get("mem_head"), after.get("mem_head"),
                         after.get("drain_demoted"))

        appended = after.get("mem_head", 0) - before.get("mem_head", 0)
        if after.get("drain_demoted") and appended > 0:
            raise MdJournalError(
                f"[lblk] the thawed node appended {appended} entries to the "
                f"shared ring while demoted (drain_demoted=True). This is the "
                f"documented missing leadership fence: a stale writer mutated "
                f"shared ring structure after losing leadership.")

        self._scan_spdk_logs("unfenced journal")
        self._verify_all("after leader freeze and thaw")
        self.logger.info("[lblk] no unfenced append observed this cycle")


# ── registered leaf classes ───────────────────────────────────────────────
class LblkFunctionalDocker(_LblkDockerMixin, _LblkFunctional):
    """lblk functional smoke, docker."""


class LblkFunctionalK8s(_LblkK8sMixin, _LblkFunctional):
    """lblk functional smoke, k8s-native."""


class LblkIntegrityDocker(_LblkDockerMixin, _LblkIntegrity):
    """Raw crc32c integrity across outages, docker."""


class LblkIntegrityK8s(_LblkK8sMixin, _LblkIntegrity):
    """Raw crc32c integrity across outages, k8s-native."""


class LblkDeviceFaultDocker(_LblkDockerMixin, _LblkDeviceFault):
    """Block-device hot-remove, docker."""


class LblkDeviceFaultK8s(_LblkK8sMixin, _LblkDeviceFault):
    """Block-device hot-remove, k8s-native."""


class LblkJournalRecoveryDocker(_LblkDockerMixin, _LblkJournalRecovery):
    """Metadata-journal replay after a kill, docker."""


class LblkJournalRecoveryK8s(_LblkK8sMixin, _LblkJournalRecovery):
    """Metadata-journal replay after a kill, k8s-native."""


class LblkUnfencedJournalDocker(_LblkDockerMixin, _LblkUnfencedJournal):
    """The documented missing-leadership-fence reproducer, docker."""


class LblkUnfencedJournalK8s(_LblkK8sMixin, _LblkUnfencedJournal):
    """The documented missing-leadership-fence reproducer, k8s-native."""
