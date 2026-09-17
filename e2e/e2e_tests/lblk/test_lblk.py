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
import threading

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.md_journal import (
    MdJournalError,
    call_rpc,
    assert_journal_enabled,
    get_stats,
    scan_log_for_corruption,
    set_drain_paused,
)
from exceptions.custom_exception import SkippedTestsException
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
        self._lblk_volumes = []      # names, both platforms
        self._journal_lvs = None

    # ── platform hooks ────────────────────────────────────────────────────
    # Supplied by the Docker/K8s leaf classes. Deliberately not declared as
    # abstract stubs here: a stub on the base would be found first by any leaf
    # that mixes the platform in later, and would shadow the real one.

    @property
    def _spdk_runner(self):
        """Whatever reaches SPDK on this platform.

        Anything with .exec_command(node, command, ...). On docker that is ssh
        to the storage node. On k8s it is kubectl from the runner -- storage
        nodes there are not ssh-reachable at all, and every other k8s test in
        the suite goes through kubectl for the same reason. Sending these
        through ssh_obj produced

          Exception: All usernames failed for 10.0.0.10. Last error: timed out

        on the first RPC. Client-directed work (FIO, nvme connect) still uses
        ssh_obj, because those are real machines.
        """
        return self.ssh_obj

    def _init_lblk(self):
        self._verifier = RawDeviceVerifier(self.ssh_obj, self.logger)
        self._lblk_devices = {}
        self._lblk_volumes = []
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
                if not isinstance(dev, dict):
                    raise LblkPreconditionError(
                        f"get_device_details returned {type(dev).__name__} "
                        f"rather than a device dict: {dev!r}")
                seen += 1
                ok, shown = self._device_is_lblk_backed(dev)
                if not ok:
                    bad.append((dev.get("id") or dev.get("UUID"), shown))
        if not seen:
            raise LblkPreconditionError("cluster reports no storage devices")
        if bad:
            raise LblkPreconditionError(
                f"{len(bad)} of {seen} devices are not lblk-backed: {bad[:5]}")
        self.logger.info("[lblk] all %d devices are lblk-backed", seen)
        return seen

    @staticmethod
    def _device_is_lblk_backed(dev):
        """(is_lblk, what_was_checked) for one device, on either platform.

        The two platforms expose different things, so this cannot just read one
        field:

        Docker goes through the v1 API and gets the model, including
        bdev_type -- "aio" on lblk. That is the direct answer, so use it when
        it is there.

        K8s shells out to `sbctl sn list-devices --json`, whose dicts carry
        UUID / Name / Size / Serial Number / PCIe / Status and no bdev_type at
        all. What it does expose is the column named "PCIe", which on lblk
        holds a device PATH (/dev/nvme2n1, /dev/sdb) rather than a PCI address
        (0000:00:02.0). That distinction IS the mode: lblk devices are named by
        path because they are never PCI-bound to SPDK.
        """
        bdev_type = dev.get("bdev_type")
        if bdev_type is not None:
            return bdev_type == "aio", f"bdev_type={bdev_type!r}"

        path = dev.get("PCIe") or dev.get("device_path") or ""
        return str(path).startswith("/dev/"), f"PCIe={path!r}"

    def _journal_targets(self):
        """(node_ip, rpc_port, exec_prefix, sock, lvs_name) per storage node."""
        out = []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            ip, port = node.get("mgmt_ip"), node.get("rpc_port")
            if not ip or not port:
                continue
            out.append((ip, port, self._spdk_exec_prefix(ip, port),
                        self._spdk_sock(port), node.get("lvstore")))
        return out

    def assert_journals_live(self):
        """The journal must be enabled on every lvstore.

        Without it the cluster runs unprotected on devices that may not give a
        4K atomic write, which is the whole reason these tests exist.
        """
        checked = 0
        for ip, _port, prefix, sock, lvs in self._journal_targets():
            if not lvs:
                continue
            stats = assert_journal_enabled(self._spdk_runner, ip, prefix, sock,
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
    #: Raw crc32c needs a block device on a client. Docker has one; k8s does
    #: not -- _connect_and_mount_dual is a documented no-op there and nothing in
    #: k8s_utils creates a volumeMode: Block PVC. Set False on the k8s mixin, so
    #: the k8s classes verify through FIO instead of silently skipping the gate.
    RAW_VERIFY = True

    def _create_and_connect(self, name, pool):
        """Provision a volume the way this platform does it.

        Docker: an lvol, connected raw over NVMe-oF. No filesystem, on purpose,
        so the crc32c verify sees the device.

        K8s: a PVC. There is no client device to hand back -- volumes are
        consumed by pods -- so the FIO lane works off the volume name.
        """
        if self.k8s_test:
            self._create_lvol_dual(name, self.LVOL_SIZE, pool_name=pool)
            self._lblk_volumes.append(name)
            self.logger.info("[lblk] %s provisioned as a PVC", name)
            return None, name

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
        self._lblk_volumes.append(name)
        self.logger.info("[lblk] %s -> %s on %s", name, dev, client)
        return client, dev

    def _stamp_all(self):
        if not self.RAW_VERIFY:
            return
        for _name, (client, dev) in self._lblk_devices.items():
            self._verifier.stamp(client, dev, region_size=self.VERIFY_REGION)

    def _verify_all(self, context):
        """Prove the data is intact, by whichever means this platform has.

        Docker re-reads the stamped region with crc32c on the raw device. That
        is the strong check: no filesystem to mask a torn write and no
        overlapping IO by construction.

        K8s runs FIO against the PVC, which is a filesystem check. Weaker --
        SshUtils.run_fio_test hardcodes --verify=md5 and enables verify_backlog,
        so a mismatch can be an artefact -- which is why md5 is demoted to a
        warning here and the blobstore-CRC scan is what actually gates a run.
        Saying so rather than skipping quietly: a check that cannot run must not
        look like one that ran and passed.
        """
        if self.RAW_VERIFY:
            for name, (client, dev) in self._lblk_devices.items():
                self._verifier.verify(client, dev,
                                      region_size=self.VERIFY_REGION,
                                      context=f"{context} [{name}]")
            return

        for name in self._lblk_volumes:
            self._fs_fio(name, None, f"{context}-{name}"[:40], runtime=30)
        self.logger.info("[lblk] %s: FIO verified %d volume(s) through the "
                         "filesystem (raw crc32c is docker-only)",
                         context, len(self._lblk_volumes))

    def _churn_all(self, runtime=None):
        if not self.RAW_VERIFY:
            return
        for _name, (client, dev) in self._lblk_devices.items():
            self._verifier.churn(client, dev,
                                 runtime=runtime or self.CHURN_RUNTIME)

    def _scan_spdk_logs(self, context):
        """Fail on blobstore-CRC or journal-fatal lines, where readable.

        These matter more than the FIO markers: a torn journal entry is dropped
        silently by design, so a broken journal never reports "bad magic". The
        blobstore CRC errors are what it looks like from outside.

        NOT readable on docker. The spdk_<port> container is created with the
        GELF log driver
        (simplyblock_web/api/internal/storage_node/docker.py:135) and
        `docker logs` cannot read GELF -- it returns
        "configured logging driver does not support reading". The product's own
        collector goes to Graylog instead, which is more plumbing than a test
        should carry.

        So this says so out loud rather than scanning an empty string and
        reporting a clean result. An earlier version read a path that did not
        exist, swallowed the error with "|| true", and silently passed every
        run; a check that cannot run must look different from one that ran and
        found nothing.
        """
        for ip, port, _prefix, _sock, _lvs in self._journal_targets():
            out, err = self._spdk_runner.exec_command(
                node=ip,
                command=self._spdk_log_cmd(ip, port, tail=4000),
                supress_logs=True)
            blob = out or ""
            if not blob.strip() or "does not support reading" in (err or ""):
                self.logger.warning(
                    "[lblk] %s: SPDK log NOT SCANNED on %s -- the container "
                    "logs to GELF and docker cannot read it back. This check "
                    "is a no-op here; the raw crc32c verify is the gate.",
                    context, ip)
                continue
            fatal, info = scan_log_for_corruption(blob, context)
            if info:
                self.logger.info("[lblk] %s on %s: %s", context, ip,
                                 ", ".join(info))
            if fatal:
                raise MdJournalError(
                    f"[lblk] metadata corruption on {ip} ({context}): {fatal}")


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
        for ip, _port, prefix, sock, lvs in self._journal_targets():
            if not lvs:
                continue
            try:
                st = get_stats(self._spdk_runner, ip, prefix, sock,
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
            # Control-plane API, so it works the same on both platforms.
            self.sbcli_utils.shutdown_node(node_uuid=uuid)
        elif outage_type == "container_stop":
            # Storage nodes are not ssh-reachable on k8s; the pod helper is
            # what every other k8s test uses for this.
            if self.k8s_test:
                self._ensure_k8s_utils().stop_spdk_pod(ip)
            else:
                self.ssh_obj.stop_spdk_process(ip, node["rpc_port"],
                                               self.cluster_id)
        elif outage_type == "storage_node_reboot":
            if self.k8s_test:
                # Rebooting a worker is an infrastructure operation with no
                # kubectl equivalent. Restarting the pod is the closest thing
                # the suite has, and is named honestly rather than pretending
                # a node reboot happened.
                self.logger.info("[lblk] no node-reboot equivalent on k8s; "
                                 "restarting the SPDK pod instead")
                self._ensure_k8s_utils().stop_spdk_pod(ip)
            else:
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

    def _spdk_freeze(self, node_ip, rpc_port, freeze):
        """Freeze or thaw the whole SPDK container.

        NOT "pkill -STOP -f spdk_tgt" on the host. The spdk_<port> container is
        created without pid_mode, so it has its own PID namespace
        (simplyblock_web/api/internal/storage_node/docker.py) and a host pkill
        matches nothing at all. The first version of this test did exactly that,
        swallowed the miss with "|| true", and reported a clean pass having
        frozen nothing.

        docker pause uses the cgroup freezer, which is the SIGSTOP semantics
        this test wants applied to every thread at once.
        """
        verb = "pause" if freeze else "unpause"
        _out, err = self.ssh_obj.exec_command(
            node=node_ip, command=f"sudo docker {verb} spdk_{rpc_port}",
            max_retries=1)
        state = self._spdk_container_state(node_ip, rpc_port)
        want = "paused" if freeze else "running"

        # Strict on the way in only. Thawing is best effort because by then the
        # control plane may have auto-restarted the node and removed the
        # container out from under us -- raising there would mask whatever the
        # test was about to report with an unrelated teardown error.
        if freeze and state != want:
            raise LblkPreconditionError(
                f"[lblk] docker pause of spdk_{rpc_port} on {node_ip} left the "
                f"container {state!r}, wanted {want!r} "
                f"({(err or 'no stderr').splitlines()[-1][:120]}). Without a "
                f"real freeze this test proves nothing.")
        if not freeze and state != want:
            self.logger.warning(
                "[lblk] unpause of spdk_%s on %s left it %r, not %r -- the "
                "control plane most likely replaced the container",
                rpc_port, node_ip, state, want)
            return state
        self.logger.info("[lblk] spdk_%s on %s is now %s", rpc_port, node_ip,
                         state)
        return state

    def _spdk_container_state(self, node_ip, rpc_port):
        """docker's own view of the container, or "" when it no longer exists."""
        out, _ = self.ssh_obj.exec_command(
            node=node_ip,
            command=f"sudo docker inspect -f '{{{{.State.Status}}}}' spdk_{rpc_port}",
            supress_logs=True, max_retries=1)
        return (out or "").strip()

    def _spdk_log_cmd(self, node_ip, rpc_port, tail=4000):
        # SPDK logs to the container's stdout, not to a file inside it. The
        # product's own collector proves it: collect_logs.py pulls these by
        # container_name out of Graylog rather than reading any path.
        return f"sudo docker logs --tail {tail} spdk_{rpc_port} 2>&1"


class _KubectlRunner:
    """Adapter giving K8sUtils the .exec_command shape ssh_obj has.

    The node argument is accepted and ignored: kubectl runs from the runner and
    already names its target pod, so there is nothing to connect to.
    """

    def __init__(self, k8s):
        self._k8s = k8s

    def exec_command(self, node=None, command=None, supress_logs=False,
                     timeout=300, **_kw):
        return self._k8s._exec_kubectl(command, supress_logs=supress_logs,
                                       timeout=timeout)


class _LblkK8sMixin:
    """Reach SPDK through the pod."""

    #: No raw block device on k8s -- see RAW_VERIFY on _LblkBase.
    RAW_VERIFY = False

    @property
    def _spdk_runner(self):
        return _KubectlRunner(self._ensure_k8s_utils())

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        # _ensure_k8s_utils(), not self.k8s_utils -- the base reaches K8sUtils
        # through sbcli_utils.k8s and there is no k8s_utils attribute at all.
        # And the method is get_spdk_pod_name; get_spdk_pod_for_node does not
        # exist. Both were invented, so every k8s lblk run died here before
        # reaching a single assertion.
        k8s = self._ensure_k8s_utils()
        pod = k8s.get_spdk_pod_name(node_ip)
        return f"kubectl exec {pod} -c spdk-container -n {k8s.namespace} --"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

    def _spdk_freeze(self, node_ip, rpc_port, freeze):
        """No equivalent of docker pause for a pod.

        Raised rather than approximated: a freeze that does not actually stop
        SPDK makes this test report a clean pass while reproducing nothing,
        which is exactly how it behaved before.
        """
        raise LblkPreconditionError(
            "[lblk] freezing a leader is not implemented on k8s-native: there "
            "is no pod equivalent of `docker pause`, and a partial freeze would "
            "make this test pass without reproducing anything. Run "
            "LblkUnfencedJournalDocker instead.")

    def _spdk_log_cmd(self, node_ip, rpc_port, tail=4000):
        k8s = self._ensure_k8s_utils()
        pod = k8s.get_spdk_pod_name(node_ip)
        return (f"kubectl logs {pod} -c spdk-container "
                f"-n {k8s.namespace} --tail={tail} 2>&1")


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

        if self.k8s_test:
            raise SkippedTestsException(
                "[lblk] device hot-remove is not implemented on k8s-native: it "
                "writes to /sys/block/<dev>/device/delete on the storage host, "
                "and kubectl gives no path to the host's sysfs. Run "
                "LblkDeviceFaultDocker for this scenario.")

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

        # Pause the drain on EVERY lvstore, then pick the victim afterwards
        # from where the lvols actually landed.
        #
        # Pausing one lvstore and hoping meant placement decided whether the
        # test worked: the cluster places an lvol where it likes, so one run
        # staged 2 of 10 on the paused node and the next staged 0 and could
        # not run at all. Pausing everything first removes the guess -- every
        # node's ring holds whatever it received.
        paused = []
        for t_ip, t_port, t_prefix, t_sock, t_lvs in self._journal_targets():
            if not t_lvs:
                continue
            set_drain_paused(self._spdk_runner, t_ip, t_prefix, t_sock, True,
                             lvs_name=t_lvs, logger=self.logger)
            paused.append((t_ip, t_port, t_prefix, t_sock, t_lvs))
        self.logger.info("[lblk] drain paused on %d lvstore(s)", len(paused))

        staged = []
        try:
            for i in range(10):
                name = f"lblkjrtmp{i}{random.randint(100, 999)}"
                self.sbcli_utils.add_lvol(lvol_name=name, pool_name=pool,
                                          size="1G")
                staged.append(name)

            # Group by owning node, then kill whichever holds the most. Those
            # lvols' metadata is in that node's ring and nowhere else.
            all_lvols = self.sbcli_utils.list_lvols() or {}
            by_node = {}
            for name in staged:
                if name not in all_lvols:
                    continue
                det = self.sbcli_utils.get_lvol_details(
                    lvol_id=all_lvols[name])[0]
                by_node.setdefault(det.get("node_id"), []).append(name)
            if not by_node:
                raise LblkPreconditionError(
                    f"[lblk] none of the {len(staged)} staged lvols is "
                    f"readable back from the control plane, so there is "
                    f"nothing to stage.")
            self.logger.info("[lblk] staged lvols per node: %s",
                             {k: len(v) for k, v in by_node.items()})

            victim_uuid = max(by_node, key=lambda k: len(by_node[k]))
            staged_here = by_node[victim_uuid]
            node = next(n for n in
                        self.sbcli_utils.get_storage_nodes()["results"]
                        if n.get("uuid") == victim_uuid)
            ip = node["mgmt_ip"]
            prefix = self._spdk_exec_prefix(ip, node["rpc_port"])
            sock = self._spdk_sock(node["rpc_port"])
            lvs = node.get("lvstore")

            stats = get_stats(self._spdk_runner, ip, prefix, sock, lvs_name=lvs,
                              logger=self.logger)
            if stats.get("used_slots", 0) < 2:
                self.logger.warning(
                    "[lblk] only %s slot(s) in the ring after pausing the "
                    "drain; recovery may have nothing to replay",
                    stats.get("used_slots"))
            self.logger.info("[lblk] killing %s with %s entries in the ring; "
                             "%d of %d staged lvols live on its lvstore %s: %s",
                             node["uuid"], stats.get("used_slots"),
                             len(staged_here), len(staged), lvs, staged_here)
            if self.k8s_test:
                self._ensure_k8s_utils().stop_spdk_pod(ip)
            else:
                self.ssh_obj.stop_spdk_process(ip, node["rpc_port"],
                                               self.cluster_id)
        finally:
            # Resume EVERY lvstore that was paused, not just the victim's. A
            # paused drain fills its ring and then metadata writes block behind
            # it, so leaving the three survivors paused would degrade the
            # cluster for the rest of the run and for teardown.
            #
            # The victim's own resume is expected to fail -- its container is
            # gone -- and the restart is what actually clears that one, since a
            # fresh process comes up unpaused.
            for r_ip, _r_port, r_prefix, r_sock, r_lvs in paused:
                try:
                    set_drain_paused(self._spdk_runner, r_ip, r_prefix, r_sock,
                                     False, lvs_name=r_lvs, logger=self.logger)
                except Exception as exc:              # noqa: BLE001
                    self.logger.info("[lblk] drain resume on %s skipped "
                                     "(expected for the killed node): %s",
                                     r_lvs, exc)

        # Wait for the kill to REGISTER before waiting for the recovery.
        # wait_for_storage_node_status(..., "online") returns immediately if the
        # node is still reported online, and right after a kill it is: the
        # control plane has not noticed yet. Skipping this step made the whole
        # restart-and-verify sequence run in 62ms against a node that had never
        # gone down, so every assertion after it was vacuous.
        self._wait_for_node_down(node["uuid"])
        self.sbcli_utils.restart_node(node_uuid=node["uuid"])
        self.sbcli_utils.wait_for_storage_node_status(node["uuid"], "online",
                                                      timeout=900)

        # The actual assertion. Metadata written before the crash has to
        # survive it -- that is the journal's whole job.
        self.assert_staged_lvols_survived(staged_here, ip, node["rpc_port"], lvs)

        # Corroboration from the journal's own counters: the ring should have
        # drained on the way back up.
        after = get_stats(self._spdk_runner, ip,
                          self._spdk_exec_prefix(ip, node["rpc_port"]),
                          sock, lvs_name=lvs, logger=self.logger)
        self.logger.info("[lblk] ring after recovery: %s/%s slots used, "
                         "disk_head=%s disk_tail=%s",
                         after.get("used_slots"), after.get("num_slots"),
                         after.get("disk_head"), after.get("disk_tail"))

        self._scan_spdk_logs("journal recovery")
        self._verify_all("after journal recovery")
        self.logger.info("[lblk] journal recovery: %d staged lvols survived "
                         "the kill and data is intact", len(staged))

    def _wait_for_node_down(self, node_uuid, timeout=300):
        """Block until the control plane stops reporting the node online.

        Killing the SPDK process does not change the node's status
        immediately -- the monitor has to notice. Asking "is it online?" in
        that window gets "yes", about the process we just killed, so any wait
        for "online" returns instantly and everything after it runs against a
        node that never went down.
        """
        for _ in range(timeout // 5):
            try:
                det = self.sbcli_utils.get_storage_node_details(
                    storage_node_id=node_uuid)[0]
            except Exception as exc:                  # noqa: BLE001
                self.logger.info("[lblk] node lookup failed while waiting for "
                                 "it to go down (%s); treating as down", exc)
                return "unreachable"
            status = det.get("status")
            if status != "online":
                self.logger.info("[lblk] node %s left online: status=%s",
                                 node_uuid, status)
                return status
            sleep_n_sec(5)
        raise MdJournalError(
            f"[lblk] node {node_uuid} was still reported online {timeout}s "
            f"after its SPDK process was killed. The kill did not take, so "
            f"there is no crash to recover from and the rest of this test "
            f"would pass without testing anything.")

    def assert_staged_lvols_survived(self, staged, ip, rpc_port, lvs):
        """Every lvol created while the drain was paused must still exist.

        This replaces scraping SPDK's log for "md journal recovery: N entries".
        That log is unreadable from here by design -- the spdk_<port> container
        is created with the GELF driver
        (simplyblock_web/api/internal/storage_node/docker.py:135), and
        `docker logs` cannot read GELF; it errors instead of returning output.
        The product's own collector goes to Graylog for the same reason.

        Asserting the outcome is better than asserting the log anyway. A
        replayed entry count proves SPDK said it replayed; a surviving lvol
        proves the metadata actually came back, which is the guarantee the
        journal exists to provide on a device with no atomic-write guarantee.
        """
        # The control-plane list is checked first, but it is NOT the evidence:
        # /lvol reads FoundationDB, which still holds these whatever the
        # lvstore did. The on-node check below is what proves the blobs came
        # back.
        present = set(self.sbcli_utils.list_lvols() or {})
        missing = [n for n in staged if n not in present]
        if missing:
            raise MdJournalError(
                f"[lblk] {len(missing)} of {len(staged)} lvols created while "
                f"the drain was paused are gone from the control plane after "
                f"the kill: {missing[:5]}")

        bdevs = call_rpc(self._spdk_runner, ip,
                         self._spdk_exec_prefix(ip, rpc_port),
                         self._spdk_sock(rpc_port), "bdev_get_bdevs",
                         logger=self.logger) or []
        names = {b.get("name") for b in bdevs}
        aliases = {a for b in bdevs for a in (b.get("aliases") or [])}
        on_node = names | aliases

        absent = []
        for name in staged:
            det = self.sbcli_utils.get_lvol_details(
                lvol_id=self.sbcli_utils.get_lvol_id(name))[0]
            # bdev_get_bdevs reports the lvol as "<lvs>/<LVOL_n>" in aliases
            # and "<LVOL_n>" as the name, so accept either spelling.
            bdev, base = det.get("lvol_bdev"), det.get("base_bdev")
            if bdev not in on_node and base not in on_node:
                absent.append(f"{name} ({base})")
        if absent:
            raise MdJournalError(
                f"[lblk] {len(absent)} of {len(staged)} lvols staged on {lvs} "
                f"are in the control plane but their bdevs are NOT on the "
                f"restarted node: {absent[:5]}. Their metadata existed only in "
                f"the journal ring, so the ring did not replay.")
        self.logger.info("[lblk] all %d lvols staged on %s came back on the "
                         "node after the kill", len(staged), lvs)
        return len(staged)


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

    #: How long the leader is cut off. Long enough for its peers to demote it,
    #: short enough that the control plane does not give up and restart the
    #: node -- a restarted process is a new one and has nothing stale to write.
    ISOLATION_SEC = 90

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_journals_live()

        pool = self._add_pool_dual()
        self._create_and_connect(f"lblkfence{random.randint(100, 999)}", pool)
        self._stamp_all()

        ip, prefix, sock, lvs = self._journal_lvs
        node = next(n for n in self.sbcli_utils.get_storage_nodes()["results"]
                    if n.get("mgmt_ip") == ip)
        port = node["rpc_port"]
        before = get_stats(self._spdk_runner, ip, prefix, sock, lvs_name=lvs,
                           logger=self.logger)

        # Isolate the node's network rather than pausing its container.
        #
        # docker pause did freeze SPDK, but the control plane treats an
        # unresponsive node as failed and auto-restarts it, which REMOVES the
        # paused container: the previous run came back to "No such container"
        # after 98s. A replaced process is not a thawed one, so there was never
        # a stale writer to observe.
        #
        # Dropping the NICs leaves the SPDK process running the whole time. It
        # simply cannot be reached, so its peers demote it; when the links come
        # back, the ORIGINAL process resumes still believing it is the leader.
        # That is the stale writer this gap is about, and it is the same shape
        # as SPDK's own F3 (stop, lose leadership, resume).
        #
        # The restore is scheduled on the node itself with nohup, so losing our
        # SSH session during the outage does not strand it down.
        if self.k8s_test:
            raise SkippedTestsException(
                "[lblk] the unfenced-journal reproducer is not implemented on "
                "k8s-native: it isolates the storage host's NICs, and there is "
                "no kubectl equivalent. Deleting the pod would restart SPDK, "
                "which destroys the stale writer the test exists to observe. "
                "Run LblkUnfencedJournalDocker.")

        if_names = node.get("if_names") or self.ssh_obj.get_active_interfaces(ip)
        if not if_names:
            raise LblkPreconditionError(
                f"[lblk] no interfaces found on {ip} to isolate; cannot "
                f"demote the leader without killing it.")
        self.logger.info("[lblk] isolating %s (%s) for %ds", ip,
                         ",".join(if_names), self.ISOLATION_SEC)

        outage = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(ip, if_names, self.ISOLATION_SEC), daemon=True)
        outage.start()

        # Drive metadata while it is cut off. The lvstore fails over to a peer,
        # which is what demotes the isolated node.
        for i in range(6):
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=f"lblkfencetmp{i}{random.randint(100, 999)}",
                    pool_name=pool, size="1G")
            except Exception as exc:                  # noqa: BLE001
                self.logger.info("[lblk] lvol create during the outage failed "
                                 "(expected while degraded): %s", str(exc)[:120])

        outage.join(timeout=self.ISOLATION_SEC + 120)
        self.logger.info("[lblk] links restored on %s", ip)

        # The container must still be the one we isolated. If the control plane
        # replaced it anyway there is no stale writer and nothing to conclude.
        state = self._spdk_container_state(ip, port)
        if state != "running":
            raise SkippedTestsException(
                f"[lblk] cannot reproduce here: spdk_{port} on {ip} is "
                f"{state!r} after the outage. The control plane restarted the "
                f"node -- storage_node_monitor queues an auto-restart as soon "
                f"as status reaches OFFLINE, and it polls every "
                f"NODE_MONITOR_INTERVAL_SEC=3s, so there is no window to race. "
                f"The gap needs the ORIGINAL process to resume; a restarted one "
                f"has nothing stale to write. "
                f"This is worth reporting as-is: on a cluster with auto-restart "
                f"enabled the unfenced-append window does not occur, because "
                f"the node is replaced before it can reconnect. Reproducing it "
                f"needs node.auto_restart_disabled for the duration, and no CLI "
                f"or API exposes that flag today -- it is set only by "
                f"`sn shutdown`, which stops SPDK and so removes the stale "
                f"writer too.")
        sleep_n_sec(30)

        after = get_stats(self._spdk_runner, ip,
                          self._spdk_exec_prefix(ip, port), sock,
                          lvs_name=lvs, logger=self.logger)
        appended = after.get("mem_head", 0) - before.get("mem_head", 0)
        self.logger.info("[lblk] ring head before=%s after=%s (+%s), "
                         "drain_demoted=%s",
                         before.get("mem_head"), after.get("mem_head"),
                         appended, after.get("drain_demoted"))

        if after.get("drain_demoted") and appended > 0:
            raise MdJournalError(
                f"[lblk] the reconnected node appended {appended} entries to the "
                f"shared ring while demoted (drain_demoted=True). This is the "
                f"documented missing leadership fence: a stale writer mutated "
                f"shared ring structure after losing leadership.")

        # A pass only means something if the node really lost leadership. If it
        # never did, the fence was never under test -- say so rather than
        # reporting a clean result, which is what this test did before.
        if not after.get("drain_demoted"):
            raise LblkPreconditionError(
                f"[lblk] INCONCLUSIVE: the isolated node was never demoted "
                f"(drain_demoted=False) after {self.ISOLATION_SEC}s cut off, so "
                f"the missing leadership fence was never exercised. Ring head "
                f"moved {before.get('mem_head')} -> {after.get('mem_head')}. "
                f"Isolate for longer, or drive more metadata, before reading "
                f"anything into a pass.")

        self._scan_spdk_logs("unfenced journal")
        self._verify_all("after leader freeze and thaw")
        self.logger.info("[lblk] node was demoted and appended nothing after "
                         "reconnecting -- fence held this cycle")


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
