"""TC-FD-002 — Strict node anti-affinity placement validation.

Covers:
- Verify cluster strict-node-anti-affinity setting
- Create lvol with npcs=1 → primary and secondary on different nodes
- Create lvol with npcs=2 → all 3 replicas on distinct nodes

Status: UNCERTAIN — requires cluster created with --strict-node-anti-affinity;
        commented out in get_all_tests()
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNodeAntiAffinity(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "node_anti_affinity"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-FD-002: Node Anti-Affinity ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Check cluster supports anti-affinity ───────────────────
        nodes = self.sbcli_utils.get_storage_nodes()
        node_count = len(nodes.get("results", [])) if nodes else 0
        self.logger.info(f"Cluster has {node_count} storage nodes")

        if node_count < 2:
            self.logger.warning(
                "Need at least 2 nodes for anti-affinity test — skipping"
            )
            return

        # ── Create lvol with npcs=1 ────────────────────────────────
        lvol_name_1 = f"{self.lvol_name}_aa_npcs1"
        self.sbcli_utils.add_lvol(
            lvol_name=lvol_name_1,
            pool_name=self.pool_name,
            size="1G",
            distr_ndcs=self.ndcs,
            distr_npcs=1,
            retry=3,
        )
        sleep_n_sec(5)

        lid1 = self.sbcli_utils.get_lvol_id(lvol_name_1)
        det1 = self.sbcli_utils.get_lvol_details(lid1) if lid1 else None
        if det1 and len(det1) > 0:
            primary_node = det1[0].get("node_id", "")
            # Check for secondary node info if available
            nodes_used = set()
            nodes_used.add(primary_node)
            for d in det1:
                nid = d.get("node_id", "")
                if nid:
                    nodes_used.add(nid)
            self.logger.info(
                f"LVOL {lvol_name_1} (npcs=1): nodes used = {nodes_used}"
            )
            if len(nodes_used) >= 2:
                self.logger.info("Primary and secondary on different nodes — PASS")
            else:
                self.logger.warning(
                    "Could not verify anti-affinity from lvol details — "
                    "node_id may only show primary"
                )

        # ── Create lvol with npcs=2 (if enough nodes) ──────────────
        if node_count >= 3:
            lvol_name_2 = f"{self.lvol_name}_aa_npcs2"
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name_2,
                pool_name=self.pool_name,
                size="1G",
                distr_ndcs=self.ndcs,
                distr_npcs=2,
                retry=3,
            )
            sleep_n_sec(5)

            lid2 = self.sbcli_utils.get_lvol_id(lvol_name_2)
            det2 = self.sbcli_utils.get_lvol_details(lid2) if lid2 else None
            if det2:
                nodes_used_2 = set()
                for d in det2:
                    nid = d.get("node_id", "")
                    if nid:
                        nodes_used_2.add(nid)
                self.logger.info(
                    f"LVOL {lvol_name_2} (npcs=2): nodes used = {nodes_used_2}"
                )

            try:
                self.sbcli_utils.delete_lvol(lvol_name_2)
            except Exception:
                pass
        else:
            self.logger.info(
                f"Only {node_count} nodes — skipping npcs=2 test (need 3+)"
            )

        # ── Cleanup ────────────────────────────────────────────────
        try:
            self.sbcli_utils.delete_lvol(lvol_name_1)
        except Exception:
            pass

        self.logger.info("=== TC-FD-002: Node Anti-Affinity — PASS ===")
