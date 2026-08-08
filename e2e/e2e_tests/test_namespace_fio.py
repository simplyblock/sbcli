"""TC-NS-003 — Namespaced lvol FIO validation.

Covers:
- Create parent + 9 children in same subsystem
- Connect, mount, run FIO on all 10 namespaces
- Verify all FIO instances complete without error
- Delete children → verify parent still functional
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNamespaceFio(TestClusterBase):

    NUM_CHILDREN = 9

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "namespace_fio"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-NS-003: Namespaced LVOL FIO ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        node_id = self.sn_nodes[0] if self.sn_nodes else None
        assert node_id, "No storage nodes available"

        # ── Create parent ──────────────────────────────────────────
        parent_name = f"{self.lvol_name}_nsfio_par"
        max_ns = self.NUM_CHILDREN + 1
        self.sbcli_utils.add_lvol(
            lvol_name=parent_name,
            pool_name=self.pool_name,
            size="2G",
            host_id=node_id,
            max_namespace_per_subsys=max_ns,
            retry=3,
        )
        sleep_n_sec(5)
        all_lvol_names = [parent_name]

        # ── Create children ────────────────────────────────────────
        for i in range(self.NUM_CHILDREN):
            cname = f"{self.lvol_name}_nsfio_ch{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=cname,
                pool_name=self.pool_name,
                size="1G",
                host_id=node_id,
                namespace=True,
                retry=3,
            )
            all_lvol_names.append(cname)
            sleep_n_sec(1)

        self.logger.info(f"Created 1 parent + {self.NUM_CHILDREN} children")

        # ── Connect, mount, run FIO on all ─────────────────────────
        fio_handles = []
        for idx, name in enumerate(all_lvol_names):
            device, mount = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_nsfio_{idx}"
            )
            fh = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_nsfio_{idx}" if not self.k8s_test else None,
                name=f"fio_ns_{idx}",
                runtime=30,
                size="128M",
                rw="randrw",
                bs="4K",
            )
            fio_handles.append((name, fh))

        self.logger.info(f"FIO running on {len(fio_handles)} namespaces")

        # ── Wait for all FIO ───────────────────────────────────────
        fio_failures = 0
        for name, fh in fio_handles:
            try:
                self._wait_fio_dual([fh], timeout=120)
                self._validate_fio_dual(fh)
            except Exception as exc:
                self.logger.error(f"FIO failed on {name}: {exc}")
                fio_failures += 1

        assert fio_failures == 0, (
            f"{fio_failures}/{len(fio_handles)} FIO instances failed"
        )
        self.logger.info("All FIO instances completed successfully")

        # ── Delete children → verify parent still works ────────────
        for name in all_lvol_names[1:]:  # children only
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Delete child {name}: {exc}")
            sleep_n_sec(1)

        sleep_n_sec(5)
        parent_id = self.sbcli_utils.get_lvol_id(parent_name)
        assert parent_id, f"Parent {parent_name} disappeared after child deletion"
        self.logger.info("Parent lvol still exists after all children deleted")

        # ── Cleanup parent ─────────────────────────────────────────
        if not self.k8s_test:
            try:
                self._disconnect_and_cleanup_dual(parent_name)
            except Exception:
                pass
        try:
            self.sbcli_utils.delete_lvol(parent_name)
        except Exception:
            pass

        self.logger.info("=== TC-NS-003: Namespaced LVOL FIO — PASS ===")
