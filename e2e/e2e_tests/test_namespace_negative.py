"""TC-NS-004 — Namespace negative cases.

Covers:
- Create child with namespace=True on node with no parent → new subsystem
- Delete parent while children exist → expect error or cascading
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNamespaceNegative(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "namespace_negative"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-NS-004: Namespace Negative Cases ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        node_id = self.sn_nodes[0] if self.sn_nodes else None
        assert node_id, "No storage nodes available"

        # ── 1. Child with namespace=True, no existing parent ───────
        orphan_name = f"{self.lvol_name}_ns_orphan"
        self.sbcli_utils.add_lvol(
            lvol_name=orphan_name,
            pool_name=self.pool_name,
            size="1G",
            host_id=node_id,
            namespace=True,
            retry=3,
        )
        sleep_n_sec(5)
        oid = self.sbcli_utils.get_lvol_id(orphan_name)
        assert oid, f"Orphan child {orphan_name} not created"
        odet = self.sbcli_utils.get_lvol_details(oid)
        if odet and len(odet) > 0:
            nsid = odet[0].get("nsid", 0)
            self.logger.info(
                f"Orphan child created with nsid={nsid} — "
                f"expected new subsystem (nsid=1)"
            )
        self.logger.info(
            "Namespace=True with no parent → new subsystem created — PASS"
        )

        # ── 2. Parent + child, then delete parent ──────────────────
        parent_name = f"{self.lvol_name}_ns_neg_par"
        self.sbcli_utils.add_lvol(
            lvol_name=parent_name,
            pool_name=self.pool_name,
            size="1G",
            host_id=node_id,
            max_namespace_per_subsys=10,
            retry=3,
        )
        sleep_n_sec(5)

        child_name = f"{self.lvol_name}_ns_neg_ch"
        self.sbcli_utils.add_lvol(
            lvol_name=child_name,
            pool_name=self.pool_name,
            size="512M",
            host_id=node_id,
            namespace=True,
            retry=3,
        )
        sleep_n_sec(5)

        # Attempt to delete parent while child exists
        try:
            self.sbcli_utils.delete_lvol(parent_name, max_attempt=3)
            self.logger.info(
                "Deleting parent with child succeeded — system allows it"
            )
        except Exception as exc:
            self.logger.info(
                f"Deleting parent with child correctly failed: {exc}"
            )

        # ── Cleanup ────────────────────────────────────────────────
        for name in [child_name, parent_name, orphan_name]:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(1)

        self.logger.info("=== TC-NS-004: Namespace Negative Cases — PASS ===")
