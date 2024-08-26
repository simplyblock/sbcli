from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_failure import TestSingleNodeFailure
from e2e_tests.single_node_multi_fio_perf import (
    TestSingleNodeMultipleFioPerfValidation,
)
from e2e_tests.multi_lvol_run_fio import TestManyLvolSameNode
from e2e_tests.batch_lvol_limit import TestBatchLVOLsLimit
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.cloning_and_snapshot.lvol_batch_clone import TestSnapshotBatchCloneLVOLs
from e2e_tests.cloning_and_snapshot.single_lvol_multi_clone import TestManyClonesFromSameSnapshot


def get_all_tests():
    tests = [
        TestSingleNodeOutage,
        TestSingleNodeMultipleFioPerfValidation,
        # TestSingleNodeFailure,
        # TestBatchLVOLsLimit,
        # TestManyLvolSameNode

        # Enable when testing snapshot and cloning

        # TestMultiLvolFio,
        # TestSnapshotBatchCloneLVOLs
        # TestManyClonesFromSameSnapshot
    ]
    return tests
