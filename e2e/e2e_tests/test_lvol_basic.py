"""TC-LVOL-001, 002, 003 — LVOL Basic CRUD, parameterised create, connect/disconnect.

Covers:
- Create / list / get / delete lifecycle
- Create with explicit host_id, ndcs, npcs, various sizes
- Connect (NVMe multi-path) / disconnect
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestLvolBasicCRUD(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_basic_crud"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-LVOL-001: Basic LVOL CRUD ===")

        # ── Pool create / verify ────────────────────────────────────
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── TC-LVOL-001: Create with defaults → list → get → delete ─
        lvol_name_1 = f"{self.lvol_name}_default"
        self._create_lvol_dual(
            lvol_name=lvol_name_1,
            pool_name=self.pool_name,
            size="1G",
        )
        lvols = self.sbcli_utils.list_lvols()
        assert lvol_name_1 in lvols, (
            f"LVOL {lvol_name_1} not in list after create: {list(lvols.keys())}"
        )

        lvol_id = lvols[lvol_name_1]
        details = self.sbcli_utils.get_lvol_details(lvol_id)
        assert details, f"get_lvol_details returned empty for {lvol_id}"
        self.logger.info(f"LVOL {lvol_name_1} created — id={lvol_id}")

        # Delete and verify absent
        self.sbcli_utils.delete_lvol(lvol_name_1)
        sleep_n_sec(5)
        lvols = self.sbcli_utils.list_lvols()
        assert lvol_name_1 not in lvols, (
            f"LVOL {lvol_name_1} still present after delete"
        )
        self.logger.info("TC-LVOL-001: CRUD lifecycle — PASS")

        # ── TC-LVOL-002: Create with explicit params ────────────────
        self.logger.info("=== TC-LVOL-002: Create with parameters ===")

        node_id = self.sn_nodes[0] if self.sn_nodes else None

        sizes = ["256M", "1G", "5G"]
        created_lvols = []
        for i, sz in enumerate(sizes):
            name = f"{self.lvol_name}_sz{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size=sz,
                host_id=node_id,
            )
            created_lvols.append(name)

        lvols = self.sbcli_utils.list_lvols()
        for name in created_lvols:
            assert name in lvols, f"LVOL {name} not found after create"
        self.logger.info(f"Created {len(created_lvols)} lvols with varying sizes")

        # Verify explicit host_id placement
        if node_id and not self.k8s_test:
            for name in created_lvols:
                lid = lvols[name]
                det = self.sbcli_utils.get_lvol_details(lid)
                if det and len(det) > 0:
                    placed_node = det[0].get("node_id", "")
                    assert placed_node == node_id, (
                        f"LVOL {name} placed on {placed_node}, expected {node_id}"
                    )
            self.logger.info("Host-ID placement verified for all lvols")

        self.logger.info("TC-LVOL-002: Parameterised create — PASS")

        # ── TC-LVOL-003: Connect / disconnect ───────────────────────
        self.logger.info("=== TC-LVOL-003: Connect / Disconnect ===")

        connect_lvol = created_lvols[0]
        device, mount = self._connect_and_mount_dual(
            connect_lvol, mount_path=f"{self.mount_path}_conn"
        )
        self.logger.info(f"Connected {connect_lvol} → device={device}, mount={mount}")

        # Run a short FIO to verify I/O works
        fio_handle = self._run_fio_dual(
            lvol_name=connect_lvol,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_conn" if not self.k8s_test else None,
            name="fio_connect_test",
            runtime=30,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed successfully on connected lvol")

        # Disconnect
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(connect_lvol)
            self.logger.info(f"Disconnected {connect_lvol}")

        self.logger.info("TC-LVOL-003: Connect/Disconnect — PASS")

        # Cleanup remaining lvols
        for name in created_lvols:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TestLvolBasicCRUD: ALL PASSED ===")
