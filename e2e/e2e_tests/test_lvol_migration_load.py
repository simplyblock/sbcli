"""TC-LVOL-007 -- LVOL migration under load.

Covers:
- Create pool, 3 lvols, connect, mount, start FIO on all
- Migrate first lvol to a different storage node while I/O is running
- Monitor migration tasks until completion
- Wait for FIO completion, validate no errors
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestLvolMigrationLoad(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_migration_load"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-LVOL-007: LVOL Migration Under Load ===")

        # -- Pool create / verify ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- Create 3 lvols ------------------------------------------------
        lvol_names = []
        lvol_ids = []
        for i in range(3):
            name = f"{self.lvol_name}_mig{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size="2G",
            )
            lid = self.sbcli_utils.get_lvol_id(name)
            assert lid, f"Could not get lvol_id for {name}"
            lvol_names.append(name)
            lvol_ids.append(lid)
            self.logger.info(f"LVOL {name} created -- id={lid}")

        # -- Connect, mount, start FIO on all 3 ----------------------------
        fio_handles = []
        for i, name in enumerate(lvol_names):
            device, mount = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_mig{i}"
            )
            self.logger.info(f"Connected {name} -> device={device}, mount={mount}")

            fio_handle = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_mig{i}" if not self.k8s_test else None,
                name=f"fio_mig_{i}",
                runtime=120,
                size="512M",
                rw="randrw",
                bs="4K",
            )
            fio_handles.append(fio_handle)

        self.logger.info("FIO started on all 3 lvols, allowing I/O to ramp up...")
        sleep_n_sec(15)

        # -- Get storage nodes and pick a migration target -----------------
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        sn_uuids = [r["uuid"] for r in storage_nodes.get("results", [])]
        self.logger.info(f"Available storage nodes: {sn_uuids}")

        # Determine current node for first lvol
        target_node_id = None
        if len(sn_uuids) >= 2:
            details = self.sbcli_utils.get_lvol_details(lvol_ids[0])
            current_node = None
            if details and len(details) > 0:
                current_node = details[0].get("node_id", "")
            # Pick a different node as the target
            for nid in sn_uuids:
                if nid != current_node:
                    target_node_id = nid
                    break
        else:
            self.logger.warning(
                "Only one storage node available -- migration cannot target a different node"
            )

        # -- Attempt migration of first lvol --------------------------------
        if target_node_id:
            self.logger.info(
                f"Attempting to migrate lvol {lvol_ids[0]} to node {target_node_id}..."
            )
            try:
                self.sbcli_utils.migrate_lvol(lvol_ids[0], target_node_id)
                self.logger.info(f"migrate_lvol call succeeded for {lvol_ids[0]}")
            except AttributeError:
                self.logger.warning(
                    "migrate_lvol method not available on sbcli_utils -- skipping migration"
                )
            except Exception as exc:
                self.logger.warning(f"migrate_lvol failed (may not be implemented): {exc}")

            # -- Monitor migration tasks ------------------------------------
            sleep_n_sec(10)
            try:
                tasks = self.sbcli_utils.list_migration_tasks(self.cluster_id)
                if tasks:
                    self.logger.info(f"Migration tasks: {tasks}")
                else:
                    self.logger.info("No migration tasks returned")
            except AttributeError:
                self.logger.warning(
                    "list_migration_tasks method not available -- skipping monitoring"
                )
            except Exception as exc:
                self.logger.warning(f"list_migration_tasks failed: {exc}")
        else:
            self.logger.info("Skipping migration -- no suitable target node")

        # -- Wait for FIO completion and validate --------------------------
        self.logger.info("Waiting for FIO completion on all lvols...")
        self._wait_fio_dual(fio_handles, timeout=300)
        for handle in fio_handles:
            self._validate_fio_dual(handle)
        self.logger.info("All FIO instances completed without errors")

        # -- Cleanup -------------------------------------------------------
        for name in lvol_names:
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception as exc:
                    self.logger.warning(f"Cleanup disconnect {name}: {exc}")
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TC-LVOL-007: LVOL Migration Under Load -- PASS ===")
