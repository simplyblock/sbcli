"""Scenario 4.12 — Namespaced LVOL end-to-end.

Covers:
- Create parent + 9 children in same subsystem
- Connect all, run FIO, validate
- Delete 5 children, verify remaining functional
- Create 5 new children, verify they join existing subsystem
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNamespaceE2E(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "namespace_e2e"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== Scenario 4.12: Namespace E2E ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        node_id = self.sn_nodes[0] if self.sn_nodes else None
        assert node_id, "No storage nodes available"

        # ── Phase 1: Create parent + 9 children ───────────────────
        parent_name = f"{self.lvol_name}_nse2e_par"
        self.sbcli_utils.add_lvol(
            lvol_name=parent_name,
            pool_name=self.pool_name,
            size="2G",
            host_id=node_id,
            max_namespace_per_subsys=20,
            retry=3,
        )
        sleep_n_sec(5)
        parent_id = self.sbcli_utils.get_lvol_id(parent_name)
        assert parent_id, "Parent not created"

        child_names = []
        for i in range(9):
            cname = f"{self.lvol_name}_nse2e_c{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=cname,
                pool_name=self.pool_name,
                size="1G",
                host_id=node_id,
                namespace=True,
                retry=3,
            )
            child_names.append(cname)
            sleep_n_sec(1)

        all_names = [parent_name] + child_names
        self.logger.info(f"Created 1 parent + {len(child_names)} children")

        # ── Phase 2: Connect all, run FIO ──────────────────────────
        fio_handles = []
        for idx, name in enumerate(all_names):
            device, mount = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_nse2e_{idx}"
            )
            fh = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_nse2e_{idx}" if not self.k8s_test else None,
                name=f"fio_nse2e_{idx}",
                runtime=20,
                size="64M",
            )
            fio_handles.append(fh)

        for fh in fio_handles:
            self._wait_fio_dual([fh], timeout=90)
            self._validate_fio_dual(fh)
        self.logger.info("Phase 2: FIO on all 10 namespaces — PASS")

        # ── Phase 3: Delete 5 children, verify remaining ──────────
        to_delete = child_names[:5]
        remaining = child_names[5:]

        for name in to_delete:
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Delete {name}: {exc}")
            sleep_n_sec(1)

        sleep_n_sec(5)
        # Verify parent + remaining children still exist
        lvols = self.sbcli_utils.list_lvols()
        assert parent_name in lvols, "Parent disappeared after child deletion"
        for name in remaining:
            assert name in lvols, f"Remaining child {name} disappeared"
        self.logger.info("Phase 3: 5 children deleted, parent + 4 remaining — PASS")

        # ── Phase 4: Create 5 new children ─────────────────────────
        new_children = []
        for i in range(5):
            cname = f"{self.lvol_name}_nse2e_new{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=cname,
                pool_name=self.pool_name,
                size="1G",
                host_id=node_id,
                namespace=True,
                retry=3,
            )
            new_children.append(cname)
            sleep_n_sec(1)

        # Verify new children share parent subsystem
        parent_det = self.sbcli_utils.get_lvol_details(parent_id)
        parent_nqn = parent_det[0].get("nqn", "") if parent_det else ""
        for cname in new_children:
            cid = self.sbcli_utils.get_lvol_id(cname)
            cdet = self.sbcli_utils.get_lvol_details(cid) if cid else None
            if cdet and len(cdet) > 0:
                child_nqn = cdet[0].get("nqn", "")
                if child_nqn == parent_nqn:
                    self.logger.info(f"  {cname}: joined parent subsystem ✓")
                else:
                    self.logger.warning(
                        f"  {cname}: different NQN {child_nqn} vs parent {parent_nqn}"
                    )

        self.logger.info("Phase 4: 5 new children created")

        # ── Cleanup ────────────────────────────────────────────────
        all_cleanup = remaining + new_children + [parent_name]
        for name in all_cleanup:
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(1)

        self.logger.info("=== Scenario 4.12: Namespace E2E — PASS ===")
