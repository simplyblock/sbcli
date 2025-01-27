from e2e_tests.single_node_outage import TestSingleNodeOutage
from e2e_tests.single_node_failure import (
    TestSingleNodeFailure,
    TestHASingleNodeFailure
)
from e2e_tests.single_node_reboot import (
    TestSingleNodeInstanceReboot,
    TestHASingleNodeReboot
)
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0, TestLvolFioNpcs1, TestLvolFioNpcs2, TestLvolFioNpcsCustom
)
from e2e_tests.multi_lvol_run_fio import TestManyLvolSameNode
from e2e_tests.batch_lvol_limit import TestBatchLVOLsLimit
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.ha_journal.lvol_journal_device_node_restart import TestDeviceNodeRestart
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from e2e_tests.multi_node_crash_fio_clone import TestMultiFioSnapshotDowntime


from stress_test.lvol_stress_fio_run import TestStressLvolClusterFioRun
from stress_test.lvol_ha_stress_fio import (
    TestLvolHAClusterGracefulShutdown,
    TestLvolHAClusterStorageNodeCrash,
    TestLvolHAClusterNetworkInterrupt,
    TestLvolHAClusterPartialNetworkOutage,
    TestLvolHAClusterRunAllScenarios
)
from stress_test.lvol_snap_clone_fio_failover import(
    TestFailoverScenariosStorageNodes
)
from stress_test.continuous_failover_ha import RandomFailoverTest


ALL_TESTS = [
    TestLvolFioNpcsCustom,
    TestLvolFioNpcs0,
    TestLvolFioNpcs1,
    TestLvolFioNpcs2,
    TestSingleNodeOutage,
    TestSingleNodeFailure,
    FioWorkloadTest,
    TestMultiFioSnapshotDowntime,
    TestManyLvolSameNode,
    TestBatchLVOLsLimit,
    TestMultiLvolFio,
    TestDeviceNodeRestart,
    TestHASingleNodeFailure,
    TestSingleNodeInstanceReboot,
    TestHASingleNodeReboot
]

def get_all_tests(custom=True, ha_test=False):
    tests = [
        TestLvolFioNpcsCustom,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        TestSingleNodeOutage,
        TestSingleNodeFailure,
        TestHASingleNodeFailure,
        TestSingleNodeInstanceReboot,
        TestHASingleNodeReboot
        # FioWorkloadTest,
        # TestMultiFioSnapshotDowntime,
        # TestManyLvolSameNode,
        # TestBatchLVOLsLimit,

        # Enable when testing snapshot and cloning

        # TestMultiLvolFio,
        # TestSnapshotBatchCloneLVOLs,
        # TestManyClonesFromSameSnapshot,
        # TestDeviceNodeRestart
    ]
    if not custom:
        tests.remove(TestLvolFioNpcsCustom)
    else:
        tests.remove(TestLvolFioNpcs0)
        tests.remove(TestLvolFioNpcs1)
        tests.remove(TestLvolFioNpcs2)
    if not ha_test:
        tests.remove(TestHASingleNodeFailure)
        tests.remove(TestHASingleNodeReboot)
    return tests

def get_stress_tests():
    tests = [
        TestStressLvolClusterFioRun,
        TestLvolHAClusterGracefulShutdown,
        TestLvolHAClusterStorageNodeCrash,
        TestLvolHAClusterNetworkInterrupt,
        TestLvolHAClusterPartialNetworkOutage,
        TestLvolHAClusterRunAllScenarios,
        TestFailoverScenariosStorageNodes,
        RandomFailoverTest,
    ]
    return tests
