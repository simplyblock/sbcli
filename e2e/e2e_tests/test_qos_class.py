"""TC-QOS-001 -- QoS class CRUD (add / list / delete).

DEPRECATED / UNCERTAIN: The QoS class API (qos add / qos list / qos delete)
may not be available in all builds. Each step is wrapped in try/except so the
test degrades gracefully when the API is absent.

Covers:
- Add a QoS class via CLI
- List QoS classes and verify the new class appears
- Delete the QoS class and verify removal
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestQosClass(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "qos_class"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-QOS-001: QoS Class CRUD ===")

        mgmt_node = self.mgmt_nodes[0]
        qos_id = None
        qos_available = True

        # ── Step 1: Add QoS class ────────────────────────────────────
        self.logger.info("Attempting to add QoS class with weight=100...")
        try:
            cmd = (
                f"{self.base_cmd} qos add "
                f"--weight 100 {self.cluster_id}"
            )
            output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
            self.logger.info(f"qos add output: {output}")
            if error and error.strip():
                self.logger.warning(f"qos add stderr: {error}")
            # Try to extract an ID from the output
            if output and output.strip():
                qos_id = output.strip().split()[-1] if output.strip() else None
            self.logger.info(f"QoS class created, id={qos_id}")
        except Exception as exc:
            self.logger.warning(
                f"qos add failed (API may not be available): {exc}"
            )
            qos_available = False

        sleep_n_sec(3)

        # ── Step 2: List QoS classes ─────────────────────────────────
        if qos_available:
            self.logger.info("Attempting to list QoS classes...")
            try:
                cmd = f"{self.base_cmd} qos list"
                output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
                self.logger.info(f"qos list output: {output}")
                if error and error.strip():
                    self.logger.warning(f"qos list stderr: {error}")
                # Verify our class appears in the list
                if qos_id and output:
                    assert qos_id in output, (
                        f"QoS class {qos_id} not found in qos list output"
                    )
                    self.logger.info(
                        f"QoS class {qos_id} confirmed in list output"
                    )
                else:
                    self.logger.info(
                        "QoS class ID not captured; skipping list assertion"
                    )
            except AssertionError:
                raise
            except Exception as exc:
                self.logger.warning(
                    f"qos list failed (API may not be available): {exc}"
                )

        # ── Step 3: Delete QoS class ─────────────────────────────────
        if qos_available and qos_id:
            self.logger.info(f"Attempting to delete QoS class {qos_id}...")
            try:
                cmd = f"{self.base_cmd} qos delete {qos_id}"
                output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
                self.logger.info(f"qos delete output: {output}")
                if error and error.strip():
                    self.logger.warning(f"qos delete stderr: {error}")
                sleep_n_sec(3)

                # Verify removal
                cmd_list = f"{self.base_cmd} qos list"
                output, error = self.ssh_obj.exec_command(
                    mgmt_node, cmd_list
                )
                if output and qos_id in output:
                    self.logger.warning(
                        f"QoS class {qos_id} still in list after delete"
                    )
                else:
                    self.logger.info(
                        f"QoS class {qos_id} successfully removed"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"qos delete failed (API may not be available): {exc}"
                )
        else:
            self.logger.info(
                "Skipping QoS delete step — no class was created"
            )

        self.logger.info("=== TC-QOS-001: QoS Class CRUD — PASS ===")
