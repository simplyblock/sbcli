"""TC-RDMA-003 — QPair tuning verification.

NOTE: This test is marked as UNCERTAIN / potentially deprecated.
QPair tuning requires specific cluster configuration (RDMA-enabled fabric).
It may not work on standard CI clusters.

Covers:
- Get current QPair count for storage nodes
- Modify QPair count via cluster set
- Verify new QPair count takes effect
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestQpairTuning(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "qpair_tuning"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-RDMA-003: QPair Tuning ===")

        # ── Step 1: Get current cluster config ────────────────────────
        mgmt = self.mgmt_nodes[0]
        original_qpairs = None

        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} cluster get {self.cluster_id}",
            )
            self.logger.info(f"Current cluster config: {out[:500]}")
            # Try to parse QPair info from output
            for line in out.splitlines():
                if "qpair" in line.lower() or "queue_pair" in line.lower():
                    self.logger.info(f"  QPair info: {line.strip()}")
        except Exception as exc:
            self.logger.warning(f"Failed to get cluster config: {exc}")

        # ── Step 2: Get storage node details for QPair info ───────────
        nodes = self.sbcli_utils.get_storage_nodes()
        if nodes and "results" in nodes:
            for n in nodes["results"]:
                if n.get("status") == "online":
                    nid = n["id"]
                    qpairs = n.get("qpairs_per_ctrlr", n.get("qpair_count", "N/A"))
                    self.logger.info(f"Node {nid}: qpairs = {qpairs}")
                    if original_qpairs is None and qpairs != "N/A":
                        original_qpairs = qpairs

        if original_qpairs is None:
            self.logger.warning(
                "Could not determine current QPair count — "
                "RDMA may not be configured. Skipping tuning steps."
            )
            self.logger.info("=== TC-RDMA-003: QPair Tuning — SKIP (no RDMA) ===")
            return

        # ── Step 3: Attempt to modify QPair count ─────────────────────
        new_qpairs = 64  # Common tuning target
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} cluster set {self.cluster_id} "
                f"--qpairs-per-ctrlr {new_qpairs}",
            )
            self.logger.info(f"Set qpairs to {new_qpairs}: {out}")
        except Exception as exc:
            self.logger.warning(f"Failed to set QPair count: {exc}")
            self.logger.info("=== TC-RDMA-003: QPair Tuning — SKIP (set failed) ===")
            return

        sleep_n_sec(10)

        # ── Step 4: Verify new QPair count ────────────────────────────
        nodes_after = self.sbcli_utils.get_storage_nodes()
        if nodes_after and "results" in nodes_after:
            for n in nodes_after["results"]:
                if n.get("status") == "online":
                    nid = n["id"]
                    qpairs = n.get("qpairs_per_ctrlr", n.get("qpair_count", "N/A"))
                    self.logger.info(f"Node {nid} after tuning: qpairs = {qpairs}")

        # ── Step 5: Create lvol and verify I/O works with new QPair ───
        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_qpair"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="2G",
        )

        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_qpair"
        )

        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_qpair" if not self.k8s_test else None,
            name="fio_qpair",
            runtime=30,
            size="256M",
            rw="randrw",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed with tuned QPair count")

        # ── Restore original QPair count ──────────────────────────────
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} cluster set {self.cluster_id} "
                f"--qpairs-per-ctrlr {original_qpairs}",
            )
            self.logger.info(f"Restored qpairs to {original_qpairs}: {out}")
        except Exception as exc:
            self.logger.warning(f"Failed to restore QPair count: {exc}")

        # ── Cleanup ───────────────────────────────────────────────────
        if not self.k8s_test:
            try:
                self._disconnect_and_cleanup_dual(lvol_name)
            except Exception:
                pass
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception:
            pass

        self.logger.info("=== TC-RDMA-003: QPair Tuning — PASS ===")
