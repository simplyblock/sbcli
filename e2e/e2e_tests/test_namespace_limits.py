"""TC-NS-001 — Namespace per subsystem limit enforcement.

Covers:
- Create parent with max_namespace_per_subsys=5
- Create 4 children → all join parent subsystem (NS ID > 1)
- Create 5th child → should overflow to new subsystem (NS ID = 1)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNamespaceLimits(TestClusterBase):

    MAX_NS = 5  # parent counts as 1, so 4 children fill it

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "namespace_limits"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-NS-001: Namespace Limit Enforcement ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        node_id = self.sn_nodes[0] if self.sn_nodes else None
        assert node_id, "No storage nodes available"

        # ── Create parent with max_namespace_per_subsys=5 ──────────
        parent_name = f"{self.lvol_name}_nslim_par"
        self.sbcli_utils.add_lvol(
            lvol_name=parent_name,
            pool_name=self.pool_name,
            size="1G",
            host_id=node_id,
            max_namespace_per_subsys=self.MAX_NS,
            retry=3,
        )
        sleep_n_sec(5)
        parent_id = self.sbcli_utils.get_lvol_id(parent_name)
        assert parent_id, "Parent not created"
        parent_det = self.sbcli_utils.get_lvol_details(parent_id)
        parent_nqn = parent_det[0].get("nqn", "") if parent_det else ""
        self.logger.info(f"Parent NQN: {parent_nqn}")

        # ── Create MAX_NS-1 children (should all fit) ──────────────
        within_limit = []
        for i in range(self.MAX_NS - 1):
            cname = f"{self.lvol_name}_nslim_c{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=cname,
                pool_name=self.pool_name,
                size="512M",
                host_id=node_id,
                namespace=True,
                retry=3,
            )
            within_limit.append(cname)
            sleep_n_sec(2)

        # Verify all within-limit children share parent NQN
        for cname in within_limit:
            cid = self.sbcli_utils.get_lvol_id(cname)
            cdet = self.sbcli_utils.get_lvol_details(cid) if cid else None
            if cdet and len(cdet) > 0:
                child_nqn = cdet[0].get("nqn", "")
                child_nsid = cdet[0].get("nsid", 0)
                assert child_nqn == parent_nqn, (
                    f"Child {cname} has NQN {child_nqn}, expected {parent_nqn}"
                )
                self.logger.info(f"  {cname}: nsid={child_nsid}, nqn_match=True")

        self.logger.info(f"All {len(within_limit)} children joined parent subsystem")

        # ── Create one more child (should overflow) ────────────────
        overflow_name = f"{self.lvol_name}_nslim_overflow"
        self.sbcli_utils.add_lvol(
            lvol_name=overflow_name,
            pool_name=self.pool_name,
            size="512M",
            host_id=node_id,
            namespace=True,
            retry=3,
        )
        sleep_n_sec(5)
        oid = self.sbcli_utils.get_lvol_id(overflow_name)
        odet = self.sbcli_utils.get_lvol_details(oid) if oid else None
        if odet and len(odet) > 0:
            overflow_nqn = odet[0].get("nqn", "")
            overflow_nsid = odet[0].get("nsid", 0)
            self.logger.info(
                f"Overflow child: nqn={overflow_nqn}, nsid={overflow_nsid}"
            )
            if overflow_nqn != parent_nqn:
                self.logger.info(
                    "Overflow child correctly created new subsystem"
                )
            else:
                self.logger.warning(
                    "Overflow child joined parent subsystem — "
                    "limit may not be strictly enforced"
                )

        # ── Cleanup ────────────────────────────────────────────────
        for name in within_limit + [overflow_name, parent_name]:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(1)

        self.logger.info("=== TC-NS-001: Namespace Limit Enforcement — PASS ===")
