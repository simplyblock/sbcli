"""Open-ended lblk soak: outages in a loop, raw crc32c verified every cycle.

The finite lblk scenarios live in e2e_tests/lblk/test_lblk.py. This is the
stress shape: it keeps injecting outages until something breaks, like the rest
of stress_test/, and verifies the raw device after every one.

Why a separate class rather than pointing the existing stress families at an
lblk cluster: those verify through a filesystem with md5, and on a device
without a 4K atomic-write guarantee a filesystem journal can mask or transform
a torn write. The raw crc32c check here has no filesystem in the way and no
overlapping IO, so a mismatch it reports is a real integrity defect rather than
an artefact of the test.
"""

from stress_test.continuous_failover_ha_multi_outage_all_nodes import (
    RandomMultiClientMultiFailoverAllNodesTest,
)
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
)
from stress_test.continuous_k8s_native_failover import (
    K8sNativeFailoverTest,
    K8sNativeResilientFailoverTest,
)
from utils.md_journal import assert_journal_enabled, scan_log_for_corruption
from utils.raw_device_verify import RawDeviceVerifier


class _LblkStressMixin:
    """Verify the raw device after every outage the parent loop injects."""

    #: Region stamped once at the start and re-verified every cycle. Kept small
    #: so the soak is dominated by outages rather than by IO.
    VERIFY_REGION = "4G"

    def _init_lblk_stress(self):
        self._verifier = RawDeviceVerifier(self.ssh_obj, self.logger)
        self._raw_targets = []       # (client, device)
        self._lblk_checked = False

    # ── platform hooks, supplied by the bases below ───────────────────────

    def assert_lblk_cluster(self):
        """Refuse to soak an nvme cluster under an lblk name.

        An older control plane silently reverts device_mode to nvme. The run
        would look healthy for hours and prove nothing about block devices.
        """
        details = self.sbcli_utils.get_cluster_details()
        row = details[0] if isinstance(details, list) else details
        mode = (row or {}).get("device_mode", "nvme")
        if mode != "lblk":
            raise RuntimeError(
                f"[lblk-stress] cluster device_mode is {mode!r}, not 'lblk'. "
                f"This soak would exercise the NVMe path instead.")
        self.assert_devices_are_aio()
        for ip, _port, prefix, sock, lvs in self._journal_targets():
            if lvs:
                assert_journal_enabled(self.ssh_obj, ip, prefix, sock,
                                       lvs_name=lvs, logger=self.logger)
        self.logger.info("[lblk-stress] cluster is lblk and journalled")

    def assert_devices_are_aio(self):
        """Every storage device must be backed by an AIO bdev.

        device_mode is a cluster-level field, and checking it alone is not
        enough: it says what was asked for, not what each node ended up with.
        The device layer is where the answer actually is, and it uses a
        different word -- the cluster says 'lblk', the device says 'aio'. Same
        decision recorded twice, so both are worth checking before committing
        hours to a soak.

        The lvols this soak creates are ordinary lvols reached over NVMe-oF, so
        the client always sees /dev/nvmeXnY whatever the backend is. That is
        expected and is exactly why the backend has to be asserted here rather
        than inferred from anything visible on the client.
        """
        seen, bad = 0, []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            for dev in self.sbcli_utils.get_device_details(node["uuid"]):
                seen += 1
                if dev.get("bdev_type") != "aio":
                    bad.append((dev.get("id"), dev.get("bdev_type")))
        if not seen:
            raise RuntimeError("[lblk-stress] cluster reports no storage devices")
        if bad:
            raise RuntimeError(
                f"[lblk-stress] {len(bad)} of {seen} devices are not AIO "
                f"bdevs: {bad[:5]}. device_mode said lblk but the devices "
                f"disagree, so this soak would exercise the NVMe path.")
        self.logger.info("[lblk-stress] all %d devices are aio bdevs", seen)
        return seen

    def _journal_targets(self):
        out = []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            ip, port = node.get("mgmt_ip"), node.get("rpc_port")
            if ip and port:
                out.append((ip, port, self._spdk_exec_prefix(ip, port),
                            self._spdk_sock(port), node.get("lvstore")))
        return out

    def _adopt_raw_targets(self):
        """Stamp one already-connected volume per client.

        Reuses what the parent loop built rather than creating volumes of its
        own, so the soak measures the same objects the rest of the test is
        exercising.
        """
        for name, det in (getattr(self, "lvol_mount_details", {}) or {}).items():
            dev, client = det.get("Device"), det.get("Client")
            if dev and client and (client, dev) not in self._raw_targets:
                self._raw_targets.append((client, dev))
                self.logger.info("[lblk-stress] stamping %s (%s on %s)",
                                 name, dev, client)
                self._verifier.stamp(client, dev,
                                     region_size=self.VERIFY_REGION)
                break        # one is enough; the rest carry filesystems

    def _verify_raw(self, context):
        for client, dev in self._raw_targets:
            self._verifier.verify(client, dev, region_size=self.VERIFY_REGION,
                                  context=context)

    def _scan_spdk_logs(self, context):
        for ip, port, _prefix, _sock, _lvs in self._journal_targets():
            out, _ = self.ssh_obj.exec_command(
                node=ip,
                command=self._spdk_log_cmd(ip, port, tail=4000),
                supress_logs=True)
            fatal, _info = scan_log_for_corruption(out or "", context)
            if fatal:
                raise RuntimeError(
                    f"[lblk-stress] metadata corruption on {ip} ({context}): "
                    f"{fatal}")

    # ── hook into the parent's loop ───────────────────────────────────────
    def perform_n_plus_k_outages(self):
        """Check the cluster is lblk once, then verify after every outage set.

        The parent calls this once per iteration, which makes it the natural
        place to bracket a cycle without overriding the long run() loop.
        """
        if not self._lblk_checked:
            self._init_lblk_stress()
            self.assert_lblk_cluster()
            self._lblk_checked = True

        if self._raw_targets:
            self._verify_raw("before outage")
        else:
            self._adopt_raw_targets()

        events = super().perform_n_plus_k_outages()
        self._scan_spdk_logs("during outage")
        return events

    def restart_nodes_after_failover(self, outage_type, *args, **kwargs):
        out = super().restart_nodes_after_failover(outage_type, *args, **kwargs)
        if self._raw_targets:
            self._verify_raw(f"after {outage_type}")
            self._scan_spdk_logs(f"after {outage_type}")
        return out


class _LblkDockerPlatform:
    """SPDK access for the docker soaks."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        return f"sudo docker exec spdk_{rpc_port}"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

    def _spdk_log_cmd(self, node_ip, rpc_port, tail=4000):
        # stdout, not a file in the container -- see _LblkDockerMixin.
        return f"sudo docker logs --tail {tail} spdk_{rpc_port} 2>&1"


class LblkStressDocker(_LblkStressMixin, _LblkDockerPlatform,
                       RandomMultiClientMultiFailoverAllNodesTest):
    """lblk soak on docker: outages drawn from all nodes, widest outage mix.

    Every outage type, primaries and secondaries alike, round after round, with
    the raw crc32c bracket added around each one.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Otherwise this inherits the parent's name and its logs land in
        # "<nfs>/n_plus_k_failover_multi_client_ha_all_nodes-<ts>/", where an
        # lblk soak is indistinguishable from an NVMe one after the fact.
        self.test_name = "lblk_stress_all_nodes_docker"


class _LblkK8sPlatform:
    """SPDK access for the k8s-native soaks."""

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        # The stress base's _ensure_k8s_utils() only VALIDATES -- it returns
        # None, unlike TestClusterBase's, which returns the object. So call it
        # for the check and then read self.k8s_utils. The method is
        # get_spdk_pod_name; get_spdk_pod_for_node never existed.
        self._ensure_k8s_utils()
        k8s = self.k8s_utils
        pod = k8s.get_spdk_pod_name(node_ip)
        return f"kubectl exec {pod} -c spdk-container -n {k8s.namespace} --"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"

    def _spdk_log_cmd(self, node_ip, rpc_port, tail=4000):
        self._ensure_k8s_utils()
        k8s = self.k8s_utils
        pod = k8s.get_spdk_pod_name(node_ip)
        return (f"kubectl logs {pod} -c spdk-container "
                f"-n {k8s.namespace} --tail={tail} 2>&1")


class LblkStressK8s(_LblkStressMixin, _LblkK8sPlatform, K8sNativeFailoverTest):
    """lblk soak on k8s-native, single-outage loop."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lblk_stress_k8s"


# ── multi-outage iteration soaks ──────────────────────────────────────────
# The two above run one outage family. These run the multi-outage loops -- K
# parallel outages per iteration, round after round -- which is where a
# metadata journal is actually put under pressure: concurrent failovers mean
# concurrent lvstore metadata mutation, which is the thing the journal
# serialises. A single-outage loop rarely produces more than one writer.

class LblkMultiOutageStressDocker(_LblkStressMixin, _LblkDockerPlatform,
                                  RandomMultiClientMultiFailoverTest):
    """lblk soak on docker: K parallel outages per iteration.

    RandomMultiClientMultiFailoverTest takes K=npcs nodes down at once and
    skips secondaries, so outages are primary-only but simultaneous. That is a
    different shape from LblkStressDocker's all-nodes loop, not a subset of it:
    this one concentrates the failures on primaries, the other spreads them
    across primaries and secondaries with a wider outage-type mix.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lblk_stress_multi_outage_docker"


class LblkResilientStressK8s(_LblkStressMixin, _LblkK8sPlatform,
                             K8sNativeResilientFailoverTest):
    """lblk soak on k8s-native: the resilient multi-outage loop.

    The resilient base keeps permanent PVCs, snapshots and clones alive for the
    whole run. That matters here more than on NVMe: without them, PVC
    provisioning blocks whenever ``ndcs + npcs > online_nodes``, so a degraded
    cluster drops to zero IO and every iteration after the first outage proves
    nothing about the journal. Permanent volumes keep IO flowing through the
    degraded window, which is precisely the window worth watching.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lblk_stress_resilient_k8s"
