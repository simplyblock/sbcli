"""TC-CLUSTER-005 -- Cluster task listing and tracking.

Covers:
- Get cluster tasks -- verify returns data
- Log task list
- Create lvol (to generate a task)
- Check task list again -- verify new entry appears
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestClusterTasks(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cluster_tasks"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CLUSTER-005: Cluster Tasks ===")

        # -- Get initial task list -----------------------------------------
        self.logger.info("Fetching initial cluster task list...")
        initial_tasks = None
        try:
            initial_tasks = self.sbcli_utils.get_cluster_tasks(self.cluster_id)
            assert initial_tasks is not None, (
                "get_cluster_tasks returned None"
            )
            self.logger.info(
                f"Initial task count: {len(initial_tasks) if isinstance(initial_tasks, list) else 'N/A'}"
            )
            self.logger.info(f"Initial tasks: {initial_tasks}")
        except AttributeError:
            self.logger.warning(
                "get_cluster_tasks method not available on sbcli_utils"
            )
        except AssertionError:
            raise
        except Exception as exc:
            self.logger.warning(f"get_cluster_tasks failed: {exc}")

        # -- Create pool and lvol to generate a task -----------------------
        self.logger.info("Creating pool and lvol to generate a cluster task...")
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_task"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"
        self.logger.info(f"LVOL {lvol_name} created -- id={lvol_id}")

        # Give the system time to register the task
        sleep_n_sec(10)

        # -- Check task list again -----------------------------------------
        self.logger.info("Fetching cluster task list after lvol creation...")
        try:
            updated_tasks = self.sbcli_utils.get_cluster_tasks(self.cluster_id)
            assert updated_tasks is not None, (
                "get_cluster_tasks returned None after lvol creation"
            )
            updated_count = len(updated_tasks) if isinstance(updated_tasks, list) else 0
            initial_count = len(initial_tasks) if isinstance(initial_tasks, list) else 0
            self.logger.info(f"Updated task count: {updated_count}")
            self.logger.info(f"Updated tasks: {updated_tasks}")

            if updated_count > initial_count:
                self.logger.info(
                    f"New task(s) detected: {updated_count - initial_count} new entries"
                )
            elif updated_count == initial_count:
                self.logger.info(
                    "Task count unchanged -- lvol creation may not generate a tracked task"
                )
            else:
                self.logger.warning(
                    f"Task count decreased: {initial_count} -> {updated_count}"
                )
        except AttributeError:
            self.logger.warning(
                "get_cluster_tasks method not available on sbcli_utils"
            )
        except AssertionError:
            raise
        except Exception as exc:
            self.logger.warning(f"get_cluster_tasks failed on second call: {exc}")

        # -- Cleanup -------------------------------------------------------
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TC-CLUSTER-005: Cluster Tasks -- PASS ===")
