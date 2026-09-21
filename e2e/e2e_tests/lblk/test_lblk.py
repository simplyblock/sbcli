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

import os
import random
import re
import shlex
import threading
import time

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
from utils.raw_device_verify import RawDeviceVerifier


def _snake_case(name):
    """LblkFunctionalDocker -> lblk_functional_docker."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


class LblkPreconditionError(RuntimeError):
    """The cluster is not an lblk cluster, so the run would prove nothing."""


class _PodDeviceRunner:
    """Gives RawDeviceVerifier the two methods it needs, backed by a pod.

    The verifier only ever calls exec_command(node, cmd) and
    is_block_device(node, device), so it does not need to know whether those
    land on a client over ssh or in a pod over kubectl. Same trick as
    _spdk_runner, and it means the docker and k8s raw lanes run byte-identical
    FIO jobs rather than two implementations that drift.
    """

    def __init__(self, k8s, pod_name, logger):
        self._k8s = k8s
        self._pod = pod_name
        self.logger = logger

    def exec_command(self, node=None, command=None, timeout=3600, **_kw):
        return self._k8s.exec_in_pod(self._pod, command, timeout=timeout)

    def is_block_device(self, node, device):
        out, _ = self._k8s.exec_in_pod(
            self._pod, f"test -b {device} && echo yes || echo no")
        return "yes" in (out or "")


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
        self._k8s_raw_pods = []      # block-mode pods, k8s only
        self._fs_volumes = {}        # formatted lvol/PVC -> mount
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

    def _make_pool(self):
        """Create the pool AND the StorageClass k8s provisions through.

        _add_pool_dual does not create a StorageClass. Every other k8s test in
        the suite -- 54 of them -- calls _k8s_ensure_storage_class() right
        after the pool, and the lblk tests did not, so on k8s every PVC sat
        Pending until the 300s wait gave up:

          TimeoutError: [K8sUtils] PVC 'lblkfnraw2545' not Bound within 300s

        Wrapped here rather than repeated in five run() methods, because the
        failure mode is a five-minute timeout with no mention of a
        StorageClass anywhere in it.
        """
        pool = self._add_pool_dual()
        if self.k8s_test:
            self._k8s_ensure_storage_class()
        return pool

    def _init_lblk(self):
        # Docker's verifier reaches a client machine over ssh. K8s has no
        # ssh-reachable target at all, so it gets no verifier here --
        # _provision_raw builds one over _PodDeviceRunner once the block pod
        # exists. Left as None rather than an ssh-backed one that happens to be
        # unused: an ssh verifier sitting on a k8s object is an invitation for
        # the next change to call it and hang for a timeout.
        self._verifier = (None if self.k8s_test
                          else RawDeviceVerifier(self.ssh_obj, self.logger))
        self._lblk_devices = {}
        self._lblk_volumes = []
        self._k8s_raw_pods = []
        self._fs_volumes = {}
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
    #: Both platforms now run the raw crc32c gate: docker over NVMe-oF to a
    #: client device, k8s through a volumeMode: Block PVC in a pod. Left as a
    #: switch so a platform that genuinely cannot do it says so in one place.
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

    def _provision_raw(self, name, pool):
        """A raw block device to stamp, on either platform.

        Docker connects the lvol over NVMe-oF and uses the client's
        /dev/nvmeXnY. K8s asks CSI for a volumeMode: Block PVC and attaches it
        to a pod through volumeDevices, which yields a device with no
        filesystem on it -- the same thing, reached differently.

        Raw matters because a filesystem journal can absorb or reshape a torn
        device write, which is exactly the failure these tests look for.
        """
        if not self.k8s_test:
            client, dev = self._create_and_connect(name, pool)
            return client, dev

        k8s = self._ensure_k8s_utils()
        pvc = self._k8s_normalize_name(name)
        k8s.create_pvc(name=pvc, size=self.LVOL_SIZE.replace("G", "Gi"),
                       storage_class=self._k8s_storage_class_name,
                       volume_mode="Block")
        k8s.wait_pvc_bound(pvc)
        pod = f"rawfio-{pvc}"[:63]
        device = k8s.create_raw_device_pod(pod, pvc)
        self._k8s_raw_pods.append(pod)
        # Point the verifier at the pod instead of a client machine.
        self._verifier = RawDeviceVerifier(
            _PodDeviceRunner(k8s, pod, self.logger), self.logger)
        self._lblk_devices[name] = (pod, device)
        self._lblk_volumes.append(name)
        self.logger.info("[lblk] %s -> raw %s in pod %s", name, device, pod)
        return pod, device

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
        # Formatted volumes, where the run created any, get the filesystem
        # lane. Both run when both exist -- they detect different things.
        for name, mount in (self._fs_volumes or {}).items():
            self._fs_fio(name, mount, f"{context}-{name}"[:40], runtime=30)
        if self._lblk_devices or self._fs_volumes:
            self.logger.info("[lblk] %s: verified %d raw + %d formatted",
                             context, len(self._lblk_devices),
                             len(self._fs_volumes or {}))

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

    #: Environments whose backing devices genuinely lack a 4K atomic write.
    #: GCP runs on pd-balanced disks, which is the hardware this whole mode
    #: exists for. The lab's Samsung PM983s and every other setup DO give a 4K
    #: atomic write, so an md5 mismatch there is a real defect.
    NON_ATOMIC_ENVS = ("gcp",)

    @property
    def _md5_severity(self):
        """Whether an md5 mismatch fails the run or only warns.

        Demoting it everywhere was wrong. run_fio_test hardcodes --verify=md5
        and enables verify_backlog, which its own comment says bypasses the
        rand_seed check -- so on a device with no atomic-write guarantee a
        mismatch really can be an artefact. On a device that DOES guarantee 4K
        atomicity there is no such excuse, and treating a mismatch as a warning
        there would hide exactly the corruption these tests are looking for.
        """
        env = (os.environ.get("CLUSTER_ENV") or "").strip().lower()
        if env in self.NON_ATOMIC_ENVS:
            return "warning"
        return "error"

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
        # The tag becomes a tmux session name, an fio job name and a log path,
        # every one of which the shell splits on whitespace. A context like
        # "after snapshot+clone" therefore started the session 'fio_lblkafter',
        # wrote no log, and the absent log then read back as "clean" -- a check
        # that silently verified nothing. Normalise at the one chokepoint every
        # caller goes through, so no future context string can reintroduce it.
        tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", tag).strip("_")[:40] or "fio"
        log = None
        if not self.k8s_test:
            log = f"{self.log_path}/fio_lblk_{tag}.log"

        # verify=md5 explicitly. Without it the k8s FIO job moves IO and checks
        # nothing -- only the docker path had --verify=md5, buried inside
        # run_fio_test -- so the "formatted" lane was not a data check on k8s
        # at all.
        #
        # verify_fatal follows the hardware. Where a 4K atomic write IS
        # guaranteed a mismatch is a real defect and should fail the job
        # outright; on GCP's pd-balanced disks it can be an artefact of
        # run_fio_test's own verify_backlog, so the mismatch is recorded and
        # triaged instead of failing the run. Same rule as _md5_severity, one
        # decision expressed in both places.
        fatal = self._md5_severity == "error"
        handle = self._run_fio_dual(
            lvol_name, mount_path=mount, log_path=log, runtime=runtime,
            name=f"lblk{tag}", rw="randrw", bs="4K", numjobs=2, nrfiles=4,
            size="512M", verify="md5", verify_fatal=fatal)
        # _run_fio_dual returns a Thread on docker and a job-name *string* on
        # k8s. hasattr(handle, "join") is true for both -- str.join exists --
        # so the old check called "jobname".join() and raised TypeError on
        # every k8s run. Test for the type we actually mean.
        if isinstance(handle, threading.Thread):
            handle.join()

        if self.k8s_test:
            self._validate_fio_dual(handle)
        else:
            self.common_utils.validate_fio_test(
                node=self.client_machines[0], log_file=log,
                md5_severity=self._md5_severity)
        self.logger.info("[lblk] filesystem FIO clean on clone %s", lvol_name)

    def _stats_after_recovery(self, ip, prefix, sock, lvs, timeout=240):
        """get_stats, but tolerant of a node that is up before it is reachable.

        sbcli reporting a node online and healthy does not mean kubectl can
        reach it yet: the transport is `kubectl exec`, which dials the kubelet
        on port 10250, and after a partition or a restart that listener can
        still be refusing connections -- "error dialing backend: dial tcp
        10.0.0.10:10250: i/o timeout". On docker the equivalent is the SPDK
        socket not being back yet.

        Retrying is right here and tolerating is not: the caller needs the
        counters to make its assertion, so a read that never succeeds must
        still fail the test. This only stops us from failing on the first
        attempt during a window we already know is unsettled.
        """
        deadline = time.time() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                return get_stats(self._spdk_runner, ip, prefix, sock,
                                 lvs_name=lvs, logger=self.logger)
            except MdJournalError as exc:
                if time.time() >= deadline:
                    raise MdJournalError(
                        f"[lblk] journal stats on {ip} were still unreadable "
                        f"{timeout}s after the node came back, over "
                        f"{attempt} attempts: {exc}") from exc
                self.logger.info("[lblk] journal stats on %s not readable yet "
                                 "(attempt %d): %s", ip, attempt, exc)
                sleep_n_sec(10)

    def _metadata_churn(self, lvol_name, tag):
        """Snapshot + clone, and report what it did to the journal.

        Snapshot and clone are the metadata-heavy operations, so this is what
        actually puts entries in the ring. Without it an lblk run exercises the
        data path only and the journal -- the whole reason the mode is safe on
        a device with no atomic-write guarantee -- is never written to.
        """
        before = self._journal_heads()

        # The clone below is formatted and mounted, so on k8s it is a
        # Filesystem PVC -- and Kubernetes refuses to clone a Block snapshot
        # into one: "requested volume ... modifies the mode of the source
        # volume but does not have permission to do so.
        # snapshot.storage.kubernetes.io/allow-volume-mode-change annotation is
        # not present on snapshotcontent". The raw lvols are Block PVCs, so
        # snapshotting one of those and cloning it here can never bind. Take a
        # formatted volume as the source instead, keeping the caller's
        # alternation so both lvstores still see metadata work. Docker has no
        # volumeMode and is untouched.
        if self.k8s_test and lvol_name in self._lblk_devices:
            fs_names = sorted(self._fs_volumes or {})
            if not fs_names:
                raise LblkPreconditionError(
                    "[lblk] this run created no formatted volume, so there is "
                    "nothing k8s will let us snapshot into a Filesystem clone")
            raw_names = list(self._lblk_devices)
            idx = raw_names.index(lvol_name) if lvol_name in raw_names else 0
            substitute = fs_names[idx % len(fs_names)]
            self.logger.info(
                "[lblk] churning %s rather than raw %s: the clone is a "
                "Filesystem PVC and its source must have the same mode",
                substitute, lvol_name)
            lvol_name = substitute

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

    def _host_cmd(self, node_ip, command, timeout=120):
        """Run a command on the storage HOST, on either platform.

        Docker: ssh. K8s: through the privileged hostNetwork SPDK pod with
        `nsenter --target 1`, which lands in the host's namespaces. That is how
        the suite already reaches host core dumps and host iptables, so host
        sysfs -- all device hot-remove needs -- comes free with it.
        """
        if not self.k8s_test:
            return self.ssh_obj.exec_command(node=node_ip, command=command,
                                             timeout=timeout, max_retries=1)
        k8s = self._ensure_k8s_utils()
        wrapped = (f"sudo nsenter --target 1 --mount --net -- "
                   f"bash -c {shlex.quote(command)}")
        return k8s.exec_in_spdk_container(node_ip, wrapped)

    def _network_outage(self, node_ip, duration):
        """Cut a storage node off the network for *duration*, self-restoring.

        The two platforms cut at different depths, and the difference matters
        when reading a result.

        Docker drops EVERY NIC over ssh, restored by a timer on the node. Total
        isolation: SPDK loses its peers and the control plane at once, so it
        usually aborts and gets restarted.

        K8s drops only traffic to and from the PEER STORAGE NODES, with
        iptables inside the hostNetwork SPDK pod (which therefore acts on the
        host's netns). The kubelet, the API server and our own kubectl exec
        stay reachable, so the restore can always be driven from outside. That
        is not a stylistic choice: the pod is not hostPID, so there is no way
        to leave a timer running on the host, and a blanket DROP that outlives
        the container cannot be undone -- it once left a worker NotReady until
        it was rebooted out of band.

        Same shape as _network_outage_dual in the security suite, which was
        itself ported from continuous_k8s_native_failover. Third copy, and it
        belongs on TestClusterBase eventually; kept local here rather than
        changing shared code the rest of the suite depends on mid-release.
        """
        if not self.k8s_test:
            if_names = self.ssh_obj.get_active_interfaces(node_ip)
            if not if_names:
                raise LblkPreconditionError(
                    f"[lblk] no active interfaces on {node_ip} to isolate")
            self.logger.info("[lblk] dropping NICs on %s (%s) for %ds",
                             node_ip, ",".join(if_names), duration)
            self.ssh_obj.disconnect_all_active_interfaces(
                node_ip, if_names, duration_secs=duration)
            return

        k8s = self._ensure_k8s_utils()
        peers = self._peer_ips(node_ip)
        # Cut this node off from its STORAGE PEERS only, not from everything.
        #
        # Three hard constraints on this platform, learned the expensive way:
        #
        # 1. The SPDK pod is hostNetwork, so iptables run inside the container
        #    act on the host's network namespace. That part works.
        # 2. The pod is NOT hostPID, so `nsenter --target 1` resolves /proc/1
        #    of the CONTAINER, not the host. Anything scheduled that way runs
        #    in the container ("System has not been booted with systemd as init
        #    system (PID 1). Can't operate.") and dies when SPDK's abort timer
        #    replaces it. There is no way to leave a timer on the host.
        # 3. exec_in_spdk_container never raises on a non-zero exit -- it
        #    returns (stdout, stderr) -- so a failed command looks exactly like
        #    a successful one unless the output is checked.
        #
        # A blanket INPUT/OUTPUT DROP therefore cannot be undone reliably: it
        # also blocks the kubelet, so once the in-container timer dies there is
        # no route left to fix it. That is what left worker-0 NotReady until it
        # was rebooted out of band.
        #
        # Blocking only the peer storage nodes gives the test what it actually
        # wants -- the peers stop hearing from this node and take leadership --
        # while leaving the kubelet, the API server and our own kubectl exec
        # reachable, so the restore can always be driven from outside. The
        # in-container timer stays as a backstop for the case where the test
        # process itself dies.
        if not peers:
            raise LblkPreconditionError(
                f"[lblk] no peer storage nodes to isolate {node_ip} from")
        add = "; ".join(f"iptables -A INPUT -s {p} -j DROP; "
                        f"iptables -A OUTPUT -d {p} -j DROP" for p in peers)
        undo = self._undo_rules(peers)
        k8s.exec_in_spdk_container(node_ip, f"sudo sh -c {shlex.quote(add)}")
        # Verify, because a silent no-op here is indistinguishable from a
        # successful isolation and produces a test that proves nothing.
        out, _err = k8s.exec_in_spdk_container(
            node_ip, "sudo iptables -S INPUT; sudo iptables -S OUTPUT")
        applied = sum(1 for p in peers if f"-s {p}/32" in (out or "")
                      or f"-d {p}/32" in (out or ""))
        if applied < len(peers):
            self._restore_network(node_ip)
            raise LblkPreconditionError(
                f"[lblk] iptables did not take on {node_ip}: wanted {len(peers)} "
                f"peers blocked, saw {applied}. Rules undone. iptables -S said: "
                f"{(out or '<nothing>')[:300]}")
        # Backstop only; the test restores explicitly in a finally.
        k8s.exec_in_spdk_container(node_ip, (
            f"sudo sh -c {shlex.quote(f'(sleep {duration + 30}; {undo}) >/dev/null 2>&1 &')}"))
        self.logger.info(
            "[lblk] %s cut off from %d peer(s) %s for %ds; kubelet left "
            "reachable so the restore cannot strand the node",
            node_ip, len(peers), ",".join(peers), duration)

    def _peer_ips(self, node_ip):
        """Every other storage node's mgmt_ip."""
        return [n["mgmt_ip"] for n
                in self.sbcli_utils.get_storage_nodes()["results"]
                if n.get("mgmt_ip") and n["mgmt_ip"] != node_ip]

    @staticmethod
    def _undo_rules(peers):
        return "; ".join(
            f"for i in 1 2 3; do iptables -D INPUT -s {p} -j DROP 2>/dev/null; "
            f"iptables -D OUTPUT -d {p} -j DROP 2>/dev/null; done" for p in peers
        ) + "; true"

    def _restore_network(self, node_ip):
        """Remove every peer DROP we may have added. Safe to call twice."""
        if not self.k8s_test:
            return
        k8s = self._ensure_k8s_utils()
        undo = self._undo_rules(self._peer_ips(node_ip))
        k8s.exec_in_spdk_container(node_ip, f"sudo sh -c {shlex.quote(undo)}")
        out, _err = k8s.exec_in_spdk_container(
            node_ip, "sudo iptables -S INPUT; sudo iptables -S OUTPUT")
        left = [ln for ln in (out or "").splitlines() if "-j DROP" in ln]
        if left:
            self.logger.warning("[lblk] DROP rules still on %s after restore: "
                                "%s", node_ip, left[:4])
        else:
            self.logger.info("[lblk] network restored on %s", node_ip)

    def _any_storage_node(self):
        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        if len(nodes) < 2:
            raise LblkPreconditionError(
                f"need at least 2 storage nodes to take one down, have {len(nodes)}")
        return random.choice(nodes)

    #: How long a network-interrupt outage holds. Short enough that the
    #: 600s offline wait in _outage_and_recover still has room afterwards.
    NETWORK_OUTAGE_SEC = 120

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
        elif outage_type == "interface_full_network_interrupt":
            # Self-restoring on both platforms, so unlike the others this one
            # does not need us to reach the node to end it. Held well short of
            # the 600s offline wait below so the links are back before we ask
            # the control plane to restart it.
            self._network_outage(ip, self.NETWORK_OUTAGE_SEC)
            sleep_n_sec(self.NETWORK_OUTAGE_SEC + 15)
            self._restore_network(ip)
        else:
            raise ValueError(f"unhandled outage type {outage_type!r}")

        self.sbcli_utils.wait_for_storage_node_status(uuid, "offline", timeout=600)
        self._restart_until_online(uuid, ip)
        self.sbcli_utils.wait_for_health_status(uuid, True, timeout=300)
        self.logger.info("[lblk] %s recovered", uuid)

    #: Restart attempts before giving up on a node.
    RESTART_ATTEMPTS = 3

    def _restart_until_online(self, uuid, ip, per_attempt=600):
        """Restart the node, and try again if the control plane gave up.

        A single restart is not reliable here. On 2026-09-20 the control plane
        killed SPDK on worker-4, then immediately tried to reach that node's
        per-node proxy address and aborted the whole operation:

          restart_storage_node raised unexpectedly
          NameResolutionError: Failed to resolve
            'worker-4.simplyblock-spdk-proxy.simplyblock.svc.cluster.local'

        That name is a headless-service endpoint, so it stops resolving while
        the pod is being recreated -- exactly the window the restart itself
        opens. The node was then left offline with nothing retrying, and a
        two-hour run ended on a DNS race rather than on anything it set out to
        measure.

        Waiting longer does not help, because the control plane has already
        stopped trying; the restart has to be re-issued. Each attempt is still
        given a generous window first, so a node that is merely slow is never
        restarted twice.
        """
        last = None
        for attempt in range(1, self.RESTART_ATTEMPTS + 1):
            self.sbcli_utils.restart_node(node_uuid=uuid)
            try:
                self.sbcli_utils.wait_for_storage_node_status(
                    uuid, "online", timeout=per_attempt)
                if attempt > 1:
                    self.logger.info(
                        "[lblk] %s came online on restart attempt %d", uuid,
                        attempt)
                return
            except Exception as exc:                  # noqa: BLE001
                last = exc
                self.logger.warning(
                    "[lblk] %s (%s) still offline %ds after restart attempt "
                    "%d/%d: %s", uuid, ip, per_attempt, attempt,
                    self.RESTART_ATTEMPTS, str(exc)[:160])
                sleep_n_sec(30)
        raise LblkPreconditionError(
            f"[lblk] {uuid} ({ip}) did not come online after "
            f"{self.RESTART_ATTEMPTS} restarts of {per_attempt}s each. Check "
            f"the control plane log for 'restart_storage_node raised "
            f"unexpectedly' -- a NameResolutionError on that node's "
            f"spdk-proxy address means the restart was abandoned rather than "
            f"failed, and is a product issue, not a slow node. Last error: "
            f"{last}")


# ── Docker / K8s platform bindings ────────────────────────────────────────
class _LblkDockerMixin:
    """Reach SPDK through the per-node docker container."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        return f"sudo docker exec spdk_{rpc_port}"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

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

    #: k8s gets the raw lane too, through a volumeMode: Block PVC in a pod.
    RAW_VERIFY = True

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

        pool = self._make_pool()

        # _provision_raw, not _create_and_connect: on k8s the latter makes a
        # FILESYSTEM PVC and hands back its name, which is not something the
        # raw verifier can stamp. _provision_raw asks for volumeMode: Block
        # there and an NVMe-oF device on docker, so both platforms end up
        # holding a real block device.
        raw = f"lblkfnraw{random.randint(1000, 9999)}"
        client, dev = self._provision_raw(raw, pool)
        self._verifier.stamp(client, dev, region_size="1G")
        self._verifier.verify(client, dev, region_size="1G",
                              context="functional smoke raw")

        # And the ordinary path a user takes, so a passing smoke test means
        # both shapes of volume work rather than only the one the gate uses.
        fs = f"lblkfnfs{random.randint(1000, 9999)}"
        self._create_lvol_dual(fs, self.LVOL_SIZE, pool_name=pool)
        _d, mount = self._connect_and_mount_dual(fs, mount_path=f"/mnt/{fs}",
                                                 format_disk=True)
        self._fs_volumes[fs] = mount
        self._fs_fio(fs, mount, "fnsmoke", runtime=30)

        self._scan_spdk_logs("functional smoke")
        self.logger.info("[lblk] functional smoke passed: raw device and "
                         "formatted volume both clean")


# ── integration: integrity across faults ──────────────────────────────────
class _LblkIntegrity(_LblkBase):
    """Raw crc32c integrity across steady state, restart and outages."""

    OUTAGE_TYPES = ("graceful_shutdown", "container_stop", "storage_node_reboot")

    def run(self):
        self._init_lblk()
        self.assert_cluster_is_lblk()
        self.assert_devices_are_aio()
        self.assert_journals_live()

        pool = self._make_pool()

        # A mix on purpose, because the two lanes fail differently.
        #
        # Raw: fio straight at the block device, crc32c, no filesystem in the
        # way. This is the gate -- a torn write shows up as a mismatch and
        # nothing can absorb it.
        #
        # Formatted: fio through a filesystem, which is what an application
        # actually does. Weaker as a detector (a journalling fs can mask or
        # reshape a torn write, and md5 is only trustworthy where the hardware
        # gives a 4K atomic write -- see _md5_severity) but it exercises the
        # path customers run, and a clone of it gets snapshot/clone coverage
        # too.
        raw_names, fs_names = [], []
        for i in range(2):
            name = f"lblkraw{i}{random.randint(100, 999)}"
            self._provision_raw(name, pool)
            raw_names.append(name)
        for i in range(2):
            name = f"lblkfs{i}{random.randint(100, 999)}"
            self._create_lvol_dual(name, self.LVOL_SIZE, pool_name=pool)
            _dev, mount = self._connect_and_mount_dual(
                name, mount_path=f"/mnt/{name}", format_disk=True)
            self._fs_volumes[name] = mount
            fs_names.append(name)
        names = raw_names + fs_names
        self.logger.info("[lblk] %d raw + %d formatted volume(s)",
                         len(raw_names), len(fs_names))
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
            # Symbolise any core the outage produced, here, while the node is
            # still running the image that made it. e2e.py's own sweep only
            # detects a core and stops the run, so without this the evidence
            # that explains the failure is a bare .zst nobody can read.
            #
            # Swept cluster-wide on purpose, not narrowed to the node we
            # outaged: on 2026-09-18 we shut down .201 and .203 was the node
            # that aborted, because the journal client that lost its JM quorum
            # is a peer of the node that went away, not the node itself.
            self.check_core_dump()

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

        pool = self._make_pool()
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
        # Host sysfs on both platforms -- see _host_cmd.
        self._host_cmd(node["mgmt_ip"],
                       f"echo 1 > /sys/block/{dev_name}/device/delete")

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

        pool = self._make_pool()
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
        after = self._stats_after_recovery(
            ip, self._spdk_exec_prefix(ip, node["rpc_port"]), sock, lvs)
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
