from e2e_tests.single_node_outage import (
    TestSingleNodeOutage,
    TestHASingleNodeOutage
)
from e2e_tests.single_node_failure import (
    TestSingleNodeFailure,
    TestHASingleNodeFailure
)
from e2e_tests.single_node_reboot import (
    TestSingleNodeReboot,
    TestHASingleNodeReboot
)
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0, TestLvolFioNpcs1, TestLvolFioNpcs2, TestLvolFioNpcsCustom
)

from e2e_tests.single_node_qos import (
    TestLvolFioQOSBW,
    TestLvolFioQOSIOPS
)

from e2e_tests.single_node_resize import TestSingleNodeResizeLvolCone

from e2e_tests.multi_lvol_run_fio import TestManyLvolSameNode
from e2e_tests.batch_lvol_limit import TestBatchLVOLsLimit
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.ha_journal.lvol_journal_device_node_restart import TestDeviceNodeRestart
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from e2e_tests.multi_node_crash_fio_clone import TestMultiFioSnapshotDowntime


from e2e_tests.add_node_fio_run import (
    TestAddNodesDuringFioRun,
    TestAddK8sNodesDuringFioRun
)
from e2e_tests.reboot_on_another_node_fio_run import TestRestartNodeOnAnotherHost
from e2e_tests.mgmt_restart_fio_run import TestMgmtNodeReboot
from e2e_tests.single_node_vm_reboot import TestRebootNodeHost


from stress_test.lvol_stress_fio_run import TestStressLvolCloneClusterFioRun
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
from stress_test.continuous_failover_ha_multi_client import RandomMultiClientFailoverTest
from stress_test.continuous_failover_ha_multi_outage import RandomMultiClientMultiFailoverTest
from stress_test.continuous_failover_ha_geomtery import RandomMultiGeometryFailoverTest
from stress_test.continuous_failover_ha_2node import RandomMultiClient2NodeFailoverTest
from stress_test.continuous_failover_ha_rdma import RandomRDMAFailoverTest


from e2e_tests.upgrade_tests.major_upgrade import TestMajorUpgrade


from load_tests.lvol_outage_load import TestLvolOutageLoadTest


ALL_TESTS = [
    TestLvolFioNpcsCustom,
    TestLvolFioNpcs0,
    TestLvolFioNpcs1,
    TestLvolFioNpcs2,
    TestSingleNodeOutage,
    TestSingleNodeFailure,
    TestAddNodesDuringFioRun,
    TestRestartNodeOnAnotherHost,
    TestRebootNodeHost,
    TestMgmtNodeReboot,
    FioWorkloadTest,
    TestLvolFioQOSBW,
    TestLvolFioQOSIOPS,
    TestMultiFioSnapshotDowntime,
    TestManyLvolSameNode,
    TestBatchLVOLsLimit,
    TestMultiLvolFio,
    TestDeviceNodeRestart,
    TestHASingleNodeFailure,
    TestSingleNodeReboot,
    TestHASingleNodeReboot,
    TestHASingleNodeOutage,
    TestSingleNodeResizeLvolCone,
    TestAddK8sNodesDuringFioRun
]

def get_all_tests(custom=True, ha_test=False):
    tests = [
        TestLvolFioNpcsCustom,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        TestLvolFioQOSBW,
        TestLvolFioQOSIOPS,
        TestSingleNodeOutage,
        # TestSingleNodeReboot,
        # TestHASingleNodeReboot,
        TestHASingleNodeOutage,
        TestSingleNodeFailure,
        TestHASingleNodeFailure,
        # TestAddNodesDuringFioRun,
        # TestRestartNodeOnAnotherHost,
        TestSingleNodeResizeLvolCone,
        # TestMgmtNodeReboot,
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
        # tests.remove(TestHASingleNodeReboot)
        # tests.remove(TestHASingleNodeOutage)
    return tests

def get_stress_tests():
    tests = [
        TestStressLvolCloneClusterFioRun,
        TestLvolHAClusterGracefulShutdown,
        TestLvolHAClusterStorageNodeCrash,
        TestLvolHAClusterNetworkInterrupt,
        TestLvolHAClusterPartialNetworkOutage,
        TestLvolHAClusterRunAllScenarios,
        TestFailoverScenariosStorageNodes,
        RandomFailoverTest,
        RandomMultiClientFailoverTest,
        RandomMultiClientMultiFailoverTest,
        RandomMultiGeometryFailoverTest,
        RandomMultiClient2NodeFailoverTest,
        RandomRDMAFailoverTest,
    ]
    return tests

def get_upgrade_tests():
    tests = [
        TestMajorUpgrade
    ]
    return tests


def get_load_tests():
    tests = [
        TestLvolOutageLoadTest
    ]
    return tests