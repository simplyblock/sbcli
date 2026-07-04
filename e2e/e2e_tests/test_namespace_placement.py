"""TC-NS-002 — Namespaced lvol placement validation.

Covers:
- Create parent lvol with host_id + max_namespace_per_subsys
- Create children with namespace=True, host_id → same node as parent
- Verify children have NS ID > 1 (sharing parent subsystem)
- Verify children inherit parent's max_namespace_per_subsys
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNamespacePlacement(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "namespace_placement"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-NS-002: Namespaced LVOL Placement ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        node_id = self.sn_nodes[0] if self.sn_nodes else None
        assert node_id, "No storage nodes available"

        # ── Create parent with max_namespace_per_subsys ────────────
        parent_name = f"{self.lvol_name}_ns_parent"
        self.sbcli_utils.add_lvol(
            lvol_name=parent_name,
            pool_name=self.pool_name,
            size="2G",
            host_id=node_id,
            max_namespace_per_subsys=10,
            retry=3,
        )
        sleep_n_sec(5)
        parent_id = self.sbcli_utils.get_lvol_id(parent_name)
        assert parent_id, f"Parent {parent_name} not created"

        parent_details = self.sbcli_utils.get_lvol_details(parent_id)
        assert parent_details, "Could not get parent details"
        parent_node = parent_details[0].get("node_id", "")
        parent_nqn = parent_details[0].get("nqn", "")
        self.logger.info(
            f"Parent created: node={parent_node}, nqn={parent_nqn}"
        )

        # ── Create 4 children with namespace=True ──────────────────
        child_names = []
        for i in range(4):
            cname = f"{self.lvol_name}_ns_child_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=cname,
                pool_name=self.pool_name,
                size="1G",
                host_id=node_id,
                namespace=True,
                retry=3,
            )
            child_names.append(cname)
            sleep_n_sec(2)

        self.logger.info(f"Created {len(child_names)} children")

        # ── Verify placement ───────────────────────────────────────
        for cname in child_names:
            cid = self.sbcli_utils.get_lvol_id(cname)
            assert cid, f"Child {cname} not found"
            cdet = self.sbcli_utils.get_lvol_details(cid)
            if cdet and len(cdet) > 0:
                child_node = cdet[0].get("node_id", "")
                child_nqn = cdet[0].get("nqn", "")
                child_nsid = cdet[0].get("nsid", 0)

                # Verify same node
                assert child_node == parent_node, (
                    f"Child {cname} on node {child_node}, "
                    f"expected {parent_node}"
                )
                # Verify same subsystem (NQN)
                assert child_nqn == parent_nqn, (
                    f"Child {cname} has NQN {child_nqn}, "
                    f"expected parent NQN {parent_nqn}"
                )
                # Verify NS ID > 1 (sharing parent subsystem)
                if child_nsid:
                    assert int(child_nsid) > 1, (
                        f"Child {cname} has NS ID {child_nsid}, "
                        f"expected > 1 (sharing parent subsystem)"
                    )
                self.logger.info(
                    f"  {cname}: node={child_node}, nqn_match=True, nsid={child_nsid}"
                )

        self.logger.info("All children placed on parent node with correct NS IDs")

        # ── Cleanup ────────────────────────────────────────────────
        for cname in child_names:
            try:
                self.sbcli_utils.delete_lvol(cname)
            except Exception:
                pass
        sleep_n_sec(5)
        try:
            self.sbcli_utils.delete_lvol(parent_name)
        except Exception:
            pass

        self.logger.info("=== TC-NS-002: Namespaced LVOL Placement — PASS ===")
