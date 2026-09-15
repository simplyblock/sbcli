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
from stress_test.continuous_k8s_native_failover import (
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
        for ip, prefix, sock, lvs in self._journal_targets():
            if lvs:
                assert_journal_enabled(self.ssh_obj, ip, prefix, sock,
                                       lvs_name=lvs, logger=self.logger)
        self.logger.info("[lblk-stress] cluster is lblk and journalled")

    def _journal_targets(self):
        out = []
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            ip, port = node.get("mgmt_ip"), node.get("rpc_port")
            if ip and port:
                out.append((ip, self._spdk_exec_prefix(ip, port),
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
        for ip, prefix, _sock, _lvs in self._journal_targets():
            out, _ = self.ssh_obj.exec_command(
                node=ip,
                command=f"{prefix} tail -n 4000 /var/log/spdk.log 2>/dev/null || true",
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


class LblkStressDocker(_LblkStressMixin, RandomMultiClientMultiFailoverAllNodesTest):
    """lblk soak on docker: the multi-client, multi-node, multi-outage loop.

    Same iteration shape as RandomMultiClientMultiFailoverAllNodesTest -- every
    outage type, every node, round after round -- with the raw crc32c bracket
    added around each one.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Otherwise this inherits the parent's name and its logs land in
        # "<nfs>/n_plus_k_failover_multi_client_ha_all_nodes-<ts>/", where an
        # lblk soak is indistinguishable from an NVMe one after the fact.
        self.test_name = "lblk_stress_multi_outage_docker"

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        return f"sudo docker exec spdk_{rpc_port}"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"


class LblkStressK8s(_LblkStressMixin, K8sNativeResilientFailoverTest):
    """lblk soak on k8s-native: the resilient multi-outage loop.

    On the resilient base rather than the plain K8sNativeFailoverTest, because
    that one keeps permanent PVCs, snapshots and clones alive across the whole
    run. Without them, PVC provisioning blocks whenever
    ``ndcs + npcs > online_nodes``, so a degraded cluster drops to zero IO and
    the iterations that follow prove nothing about the journal. Keeping IO on
    permanent volumes is what makes a multi-iteration lblk soak meaningful.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lblk_stress_multi_outage_k8s"

    def _spdk_exec_prefix(self, node_ip, rpc_port):
        pod = self.k8s_utils.get_spdk_pod_for_node(node_ip)
        return f"kubectl exec {pod} -c spdk-container -n {self.namespace} --"

    def _spdk_sock(self, rpc_port):
        return f"/mnt/ramdisk/spdk_{rpc_port}/spdk.sock"
