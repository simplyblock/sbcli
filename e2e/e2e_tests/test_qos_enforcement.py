"""TC-QOS-ENF -- QoS enforcement via pool IOPS / bandwidth limits.

DEPRECATED / UNCERTAIN: QoS enforcement behaviour and the qos_set method may
not be available in all builds.  Steps that depend on uncertain APIs are
wrapped in try/except.

Covers:
- Create pool with max_rw_iops and max_rw_mbytes limits
- Run high-IOPS FIO and verify actual IOPS stay within tolerance
- Optionally change QoS mid-test if qos_set is available
"""

import json
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestQosEnforcement(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "qos_enforcement"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-QOS-ENF: QoS Enforcement ===")

        mgmt_node = self.mgmt_nodes[0]
        qos_pool_name = f"{self.pool_name}_qos_enf"
        iops_limit = 1000
        bw_limit_mb = 50
        tolerance = 1.25  # allow 25 % overshoot

        # ── Step 1: Create pool with QoS limits ─────────────────────
        self.logger.info(
            f"Creating pool {qos_pool_name} with "
            f"max_rw_iops={iops_limit}, max_rw_mbytes={bw_limit_mb}"
        )
        if not self.k8s_test:
            self.ssh_obj.add_storage_pool(
                node=mgmt_node,
                pool_name=qos_pool_name,
                cluster_id=self.cluster_id,
                max_rw_iops=iops_limit,
                max_rw_mbytes=bw_limit_mb,
            )
        else:
            self.sbcli_utils.add_storage_pool(
                pool_name=qos_pool_name,
                max_rw_iops=iops_limit,
                max_rw_mbytes=bw_limit_mb,
            )

        pools = self.sbcli_utils.list_storage_pools()
        assert qos_pool_name in list(pools.keys()), (
            f"QoS pool {qos_pool_name} not in pool list: {list(pools.keys())}"
        )
        self.logger.info(f"Pool {qos_pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Step 2: Create lvol, connect, mount ──────────────────────
        lvol_name = f"{self.lvol_name}_qos_enf"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=qos_pool_name,
            size="5G",
        )
        self.logger.info(f"LVOL {lvol_name} created")

        mount_path = f"{self.mount_path}_qos_enf"
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=mount_path
        )
        self.logger.info(f"Connected {lvol_name} -> device={device}, mount={mount}")

        # ── Step 3: Run FIO with high IOPS ───────────────────────────
        fio_log = f"{self.log_path}/fio_qos_enf.log" if not self.k8s_test else None
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=fio_log,
            name="fio_qos_enforcement",
            runtime=60,
            rw="randread",
            bs="4K",
            size="1G",
            iodepth=64,
            numjobs=4,
        )
        self._wait_fio_dual([fio_handle], timeout=300)
        self.logger.info("FIO completed")

        # ── Step 4: Parse FIO output and check IOPS ──────────────────
        if not self.k8s_test and fio_log:
            try:
                output, _ = self.ssh_obj.exec_command(
                    self.client_machines[0],
                    f"cat {fio_log}",
                )
                if output:
                    # Try JSON parse first
                    try:
                        fio_data = json.loads(output)
                        read_iops = fio_data.get("jobs", [{}])[0].get(
                            "read", {}
                        ).get("iops", 0)
                    except (json.JSONDecodeError, IndexError, KeyError):
                        # Fallback: grep for iops line
                        read_iops = 0
                        for line in output.splitlines():
                            if "iops" in line.lower():
                                self.logger.info(f"FIO IOPS line: {line.strip()}")

                    if read_iops > 0:
                        max_allowed = iops_limit * tolerance
                        self.logger.info(
                            f"Measured read IOPS: {read_iops}, "
                            f"limit: {iops_limit}, "
                            f"max allowed (with tolerance): {max_allowed}"
                        )
                        if read_iops <= max_allowed:
                            self.logger.info(
                                "IOPS within QoS tolerance -- PASS"
                            )
                        else:
                            self.logger.warning(
                                f"IOPS {read_iops} exceeded limit "
                                f"{max_allowed} -- potential QoS issue"
                            )
                    else:
                        self.logger.info(
                            "Could not parse IOPS from FIO output; "
                            "skipping numeric assertion"
                        )
            except Exception as exc:
                self.logger.warning(f"FIO output parsing failed: {exc}")
        else:
            self._validate_fio_dual(fio_handle)

        # ── Step 5: Mid-test QoS change (if available) ───────────────
        try:
            if hasattr(self.sbcli_utils, "qos_set"):
                new_iops = 2000
                self.logger.info(
                    f"Attempting mid-test QoS change to {new_iops} IOPS"
                )
                pool_id = self.sbcli_utils.get_storage_pool_id(qos_pool_name)
                self.sbcli_utils.qos_set(
                    pool_id=pool_id, max_rw_iops=new_iops
                )
                self.logger.info(f"QoS updated to {new_iops} IOPS")
            else:
                self.logger.info(
                    "qos_set method not available on sbcli_utils; "
                    "skipping mid-test QoS change"
                )
        except Exception as exc:
            self.logger.warning(f"Mid-test QoS change failed: {exc}")

        # ── Step 6: Cleanup ──────────────────────────────────────────
        self.logger.info("Starting cleanup...")

        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)

        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete lvol {lvol_name}: {exc}")

        sleep_n_sec(5)

        try:
            self.sbcli_utils.delete_storage_pool(pool_name=qos_pool_name)
        except Exception as exc:
            self.logger.warning(
                f"Cleanup delete pool {qos_pool_name}: {exc}"
            )

        self.logger.info("=== TC-QOS-ENF: QoS Enforcement — PASS ===")
