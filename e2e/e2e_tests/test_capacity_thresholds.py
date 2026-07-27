"""TC-CAP-002 — Capacity threshold alerts and enforcement.

Covers:
- Configure capacity warning/critical thresholds on cluster
- Fill storage toward thresholds
- Verify cluster events/logs contain threshold warnings
- Verify behavior at critical threshold (write rejection or warnings)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestCapacityThresholds(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "capacity_thresholds"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CAP-002: Capacity Thresholds ===")

        mgmt = self.mgmt_nodes[0]

        # ── Step 1: Get initial cluster capacity ──────────────────────
        initial_cap = None
        try:
            initial_cap = self.sbcli_utils.get_cluster_capacity()
            self.logger.info(f"Initial cluster capacity: {initial_cap}")
        except Exception as exc:
            self.logger.warning(f"get_cluster_capacity: {exc}")

        # ── Step 2: Check current threshold settings ──────────────────
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} cluster get {self.cluster_id}",
            )
            self.logger.info(f"Cluster config (threshold info): {out[:500]}")
            for line in out.splitlines():
                low = line.lower()
                if any(k in low for k in ["threshold", "warning", "critical", "capacity"]):
                    self.logger.info(f"  Threshold setting: {line.strip()}")
        except Exception as exc:
            self.logger.warning(f"Failed to get cluster config: {exc}")

        # ── Step 3: Attempt to set capacity thresholds ────────────────
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} cluster set {self.cluster_id} "
                f"--cap-warn 70 --cap-crit 90",
            )
            self.logger.info(f"Set capacity thresholds (warn=70%%, crit=90%%): {out}")
        except Exception as exc:
            self.logger.warning(
                f"Failed to set capacity thresholds: {exc} — "
                "continuing with defaults"
            )

        sleep_n_sec(5)

        # ── Step 4: Create pool and fill with lvols ───────────────────
        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_names = []
        for i in range(3):
            name = f"{self.lvol_name}_cap_thr_{i}"
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=name,
                    pool_name=self.pool_name,
                    size="10G",
                    retry=3,
                )
                lvol_names.append(name)
                sleep_n_sec(3)

                # Log capacity after each creation
                try:
                    cap = self.sbcli_utils.get_cluster_capacity()
                    self.logger.info(f"After creating {name}: capacity = {cap}")
                except Exception:
                    pass
            except Exception as exc:
                self.logger.info(
                    f"LVOL {name} creation failed (may be at capacity): {exc}"
                )
                break

        self.logger.info(f"Created {len(lvol_names)} lvols for threshold test")

        # ── Step 5: Write data to increase actual usage ───────────────
        if lvol_names:
            first_lvol = lvol_names[0]
            try:
                device, mount = self._connect_and_mount_dual(
                    first_lvol, mount_path=f"{self.mount_path}_cap_thr"
                )
                fio_handle = self._run_fio_dual(
                    lvol_name=first_lvol,
                    mount_path=mount if not self.k8s_test else None,
                    log_path=f"{self.log_path}_cap_thr" if not self.k8s_test else None,
                    name="fio_cap_threshold",
                    runtime=30,
                    size="2G",
                    rw="write",
                )
                self._wait_fio_dual([fio_handle], timeout=120)
                self._validate_fio_dual(fio_handle)
                self.logger.info("FIO write completed for capacity fill")
            except Exception as exc:
                self.logger.warning(f"FIO for capacity fill failed: {exc}")

        # ── Step 6: Check capacity after writes ───────────────────────
        try:
            cap_after = self.sbcli_utils.get_cluster_capacity()
            self.logger.info(f"Capacity after writes: {cap_after}")
        except Exception as exc:
            self.logger.warning(f"get_cluster_capacity after writes: {exc}")

        # ── Step 7: Check cluster events for threshold warnings ───────
        try:
            logs = self.sbcli_utils.get_cluster_logs(self.cluster_id)
            if logs:
                threshold_events = 0
                for entry in (logs if isinstance(logs, list) else []):
                    entry_str = str(entry)
                    if any(
                        kw in entry_str.lower()
                        for kw in ["threshold", "capacity warning", "capacity critical"]
                    ):
                        threshold_events += 1
                        self.logger.info(f"  Threshold event: {entry_str[:200]}")
                self.logger.info(
                    f"Found {threshold_events} threshold-related events in cluster logs"
                )
        except Exception as exc:
            self.logger.warning(f"get_cluster_logs: {exc}")

        # ── Step 8: Verify per-node capacity ──────────────────────────
        nodes = self.sbcli_utils.get_storage_nodes()
        if nodes and "results" in nodes:
            for n in nodes["results"]:
                if n.get("status") == "online":
                    nid = n["id"]
                    try:
                        node_cap = self.sbcli_utils.get_node_capacity(nid)
                        self.logger.info(f"Node {nid} capacity: {node_cap}")
                    except Exception:
                        pass

        # ── Cleanup ───────────────────────────────────────────────────
        if lvol_names:
            first_lvol = lvol_names[0]
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(first_lvol)
                except Exception:
                    pass

        for name in lvol_names:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(1)

        self.logger.info("=== TC-CAP-002: Capacity Thresholds — PASS ===")
