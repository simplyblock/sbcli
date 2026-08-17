"""TC-SN-DEV-CHECKSUM-001 — Inline checksum validation actually detects corruption.

Writing data and reading it back successfully only proves normal I/O still
works with inline checksum validation enabled -- it says nothing about
whether corruption is actually caught, since a silently-disabled checksum
feature would pass that same test. This test proves detection directly:

- Requires a cluster created with --enable-inline-checksum and a storage
  node added with --enable-test-device (a `passtest` bdev is only inserted
  into the alceml stack for devices added that way).
- Silently corrupts new writes at the passtest layer (below alceml, so
  alceml computes/stores its checksum over the original correct buffer)
  via `sn device-testing-mode <device_id> corrupt_data_on_write`.
- Writes known data through the lvol -- the data physically persisted no
  longer matches what alceml checksummed.
- Disables corruption (`full_pass_through`) so the read path itself is
  clean; only the earlier write is corrupted.
- Reads the data back and asserts the read fails (checksum mismatch
  caught) rather than silently returning corrupted bytes.
- Cleans up by disabling test mode and removing the lvol.
"""

import time

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestChecksumCorruptionDetection(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "checksum_corruption_detection"
        self.logger = setup_logger(__name__)

    def _find_test_device(self):
        """Return (node, device_id) for the first online node/device pair."""
        nodes = self.sbcli_utils.get_storage_nodes()
        for n in nodes.get("results", []):
            if n.get("status") != "online":
                continue
            devices = self.sbcli_utils.get_device_details(n["id"])
            for dev in devices if isinstance(devices, list) else [devices]:
                if dev.get("id"):
                    return n, dev["id"]
        return None, None

    # -- K8s-mode file I/O: a throwaway busybox pod mounting the target PVC.
    # There's no generic "run this command against a PVC" helper in
    # cluster_test_base (only the FIO-Job-specific one), so this test brings
    # its own minimal pod lifecycle rather than extending that shared helper
    # for a one-off need.

    def _k8s_canary_pod_yaml(self, pod_name, pvc_name):
        namespace = self._ensure_k8s_utils().namespace
        return f"""
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
spec:
  restartPolicy: Never
  containers:
  - name: canary
    image: busybox:latest
    command: ["sleep", "300"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: {pvc_name}
"""

    def _k8s_run_canary_pod(self, pod_name, pvc_name, timeout=120):
        k8s = self._ensure_k8s_utils()
        yaml_body = self._k8s_canary_pod_yaml(pod_name, pvc_name)
        apply_cmd = f"cat <<'CANARY_EOF' | kubectl apply -f -\n{yaml_body}\nCANARY_EOF"
        k8s._exec_kubectl(apply_cmd)
        deadline = time.time() + timeout
        while time.time() < deadline:
            phase, _ = k8s._exec_kubectl(
                f"kubectl get pod {pod_name} -n {k8s.namespace} -o jsonpath='{{.status.phase}}'",
                supress_logs=True,
            )
            if phase.strip() == "Running":
                return
            sleep_n_sec(5)
        raise TimeoutError(f"Canary pod {pod_name} did not reach Running within {timeout}s")

    def _k8s_delete_canary_pod(self, pod_name):
        k8s = self._ensure_k8s_utils()
        k8s._exec_kubectl(
            f"kubectl delete pod {pod_name} -n {k8s.namespace} --ignore-not-found --wait=true",
            supress_logs=True,
        )

    def _k8s_write_test_file(self, lvol_name, filename, content):
        k8s = self._ensure_k8s_utils()
        reg = self._volume_registry.get(lvol_name, {})
        pvc_name = reg.get("pvc_name", self._k8s_normalize_name(lvol_name))
        pod_name = f"canary-w-{self._k8s_normalize_name(lvol_name)}"[:50]
        self._k8s_run_canary_pod(pod_name, pvc_name)
        try:
            write_cmd = (
                f"kubectl exec {pod_name} -n {k8s.namespace} -- "
                f"sh -c \"printf '%s' '{content}' > /data/{filename} && sync\""
            )
            _, stderr = k8s._exec_kubectl(write_cmd)
            if stderr and "error" in stderr.lower():
                raise RuntimeError(f"write failed: {stderr}")
        finally:
            self._k8s_delete_canary_pod(pod_name)

    def _k8s_read_test_file(self, lvol_name, filename, fresh_mount=True):
        """Read the file back through a NEW pod (forces a fresh device read
        rather than any client-side page cache from the write pod)."""
        k8s = self._ensure_k8s_utils()
        reg = self._volume_registry.get(lvol_name, {})
        pvc_name = reg.get("pvc_name", self._k8s_normalize_name(lvol_name))
        pod_name = f"canary-r-{self._k8s_normalize_name(lvol_name)}"[:50]
        self._k8s_run_canary_pod(pod_name, pvc_name)
        try:
            stdout, stderr = k8s._exec_kubectl(
                f"kubectl exec {pod_name} -n {k8s.namespace} -- cat /data/{filename}"
            )
            if stderr and stderr.strip():
                raise RuntimeError(f"read failed: {stderr.strip()}")
            return stdout
        finally:
            self._k8s_delete_canary_pod(pod_name)

    def run(self):
        self.logger.info("=== TC-SN-DEV-CHECKSUM-001: Checksum Corruption Detection ===")

        node, device_id = self._find_test_device()
        assert device_id, (
            "No online storage device found. This test requires a cluster "
            "created with --enable-inline-checksum and a node added with "
            "--enable-test-device."
        )
        mgmt = self.mgmt_nodes[0]

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_cksum"
        self._create_lvol_dual(lvol_name=lvol_name, pool_name=self.pool_name, size="1G")
        device, mount = self._connect_and_mount_dual(lvol_name, mount_path=f"{self.mount_path}_cksum")

        try:
            # --- Arm corruption: every write to this device is silently flipped
            # before it reaches the base bdev, but alceml's stored checksum still
            # reflects the correct, pre-corruption buffer it was handed.
            out, _ = self.ssh_obj.exec_command(
                mgmt, f"{self.base_cmd} sn device-testing-mode {device_id} corrupt_data_on_write"
            )
            self.logger.info(f"Armed corrupt_data_on_write on device {device_id}: {out}")

            canary_content = "checksum-detection-canary-data\n"
            if self.k8s_test:
                self._k8s_write_test_file(lvol_name, "canary.txt", canary_content)
            else:
                self.ssh_obj.exec_command(
                    node=self.client_machines[0],
                    command=f"echo '{canary_content.strip()}' > {mount}/canary.txt && sync",
                )
            sleep_n_sec(5)

            # --- Disarm corruption before reading: only the write above should
            # have been corrupted. A real bit-rot event doesn't need read-time
            # interference to be caught -- disabling it here isolates that this
            # is genuinely the stored (write-time) corruption being detected,
            # not some read-path side effect of the testmode itself.
            out, _ = self.ssh_obj.exec_command(
                mgmt, f"{self.base_cmd} sn device-testing-mode {device_id} full_pass_through"
            )
            self.logger.info(f"Disarmed testmode on device {device_id}: {out}")

            # --- Force a fresh read from the device rather than any client-side
            # page cache: a brand-new pod/mount before reading back.
            read_failed = False
            content = None
            try:
                if self.k8s_test:
                    content = self._k8s_read_test_file(lvol_name, "canary.txt")
                else:
                    self.ssh_obj.unmount_path(node=self.client_machines[0], device=device)
                    self.ssh_obj.mount_path(node=self.client_machines[0], device=device, mount_path=mount)
                    content, _ = self.ssh_obj.exec_command(
                        node=self.client_machines[0], command=f"cat {mount}/canary.txt"
                    )
            except Exception as exc:
                read_failed = True
                self.logger.info(f"Read correctly failed after corruption: {exc}")

            if not read_failed:
                self.logger.error(
                    f"EXPECTED read failure did not occur -- corrupted data was "
                    f"returned silently: {content!r}"
                )

            assert read_failed, (
                "Checksum validation did not catch write-time corruption -- "
                "the read succeeded and silently returned corrupted data "
                "instead of failing."
            )
            self.logger.info(
                "PASS: inline checksum validation detected write-time corruption "
                "and the read failed instead of returning silently-corrupted data."
            )
        finally:
            # Always disarm, even on failure, so this device isn't left corrupting
            # writes for whatever runs next.
            try:
                self.ssh_obj.exec_command(
                    mgmt, f"{self.base_cmd} sn device-testing-mode {device_id} full_pass_through"
                )
            except Exception:
                pass
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(lvol_name)
                except Exception:
                    pass
            try:
                self.sbcli_utils.delete_lvol(lvol_name)
            except Exception:
                pass
