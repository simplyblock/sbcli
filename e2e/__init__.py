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
from stress_test.continuous_failover_ha_multi_outage_all_nodes import RandomMultiClientMultiFailoverAllNodesTest
from stress_test.continuous_failover_ha_geomtery import RandomMultiGeometryFailoverTest
from stress_test.continuous_failover_ha_2node import RandomMultiClient2NodeFailoverTest
from stress_test.continuous_failover_ha_rdma import RandomRDMAFailoverTest
from stress_test.continuous_failover_ha_rdma_multi_outage import RandomRDMAMultiFailoverTest
from stress_test.continuous_failover_ha_k8s import RandomK8sMultiOutageFailoverTest
from stress_test.continuous_failover_ha_multi_client_quick_outage import RandomRapidFailoverNoGap
from stress_test.continuous_parallel_lvol_snapshot_clone import TestParallelLvolSnapshotCloneAPI
from stress_test.continuous_failover_ha_namespace import RandomMultiClientFailoverNamespaceTest
from stress_test.continuous_single_node_outage import RandomMultiClientSingleNodeTest
from stress_test.continuous_failover_ha_security import (
    RandomSecurityFailoverTest,
    RandomAllSecurityFailoverTest,
)

from e2e_tests.security.test_lvol_security import (
    TestLvolSecurityCombinations,
    TestLvolAllowedHostsPositive,
    TestLvolAllowedHostsNegative,
    TestLvolDynamicHostManagement,
    TestLvolCryptoWithAllowedHosts,
    TestLvolDhcapDirections,
    TestLvolMultipleAllowedHosts,
    TestLvolSecurityNegativeHostOps,
    TestLvolSecurityNegativeCreation,
    TestLvolSecurityNegativeConnect,
    TestLvolAllowedHostsNoDhchap,
    # Extended security tests (TC-SEC-070..127)
    TestLvolSecurityOutageRecovery,
    TestLvolSecurityNetworkInterrupt,
    TestLvolSecurityHAFailover,
    TestLvolSecurityMgmtNodeReboot,
    TestLvolSecurityDynamicModification,
    TestLvolSecurityMultiClientConcurrent,
    TestLvolSecurityScaleAndRapidOps,
    TestLvolSecurityNegativeConnectExtended,
    TestLvolSecurityCloneOverride,
    TestLvolSecurityWithBackup,
    TestLvolSecurityResize,
    TestLvolSecurityVolumeListFields,
    TestLvolSecurityRDMA,
)

from e2e_tests.upgrade_tests.major_upgrade import TestMajorUpgrade, TestMajorUpgradeSingleNode

from e2e_tests.backup.test_backup_restore import (
    TestBackupBasicPositive,
    TestBackupRestoreDataIntegrity,
    TestBackupPolicy,
    TestBackupNegative,
    TestBackupCryptoLvol,
    TestBackupCustomGeometry,
    TestBackupDeleteAndRestore,
    TestBackupCrossClusterRestore,  # NOT in get_backup_tests(); run explicitly only
    # Extra coverage tests (TC-BCK-100..148)
    TestBackupConcurrentIO,
    TestBackupMultipleRestores,
    TestBackupDeltaChainPointInTime,
    TestBackupEmptyLvol,
    TestBackupPoolRecreateRestore,
    TestBackupPolicyAgeOnly,
    TestBackupSnapshotClone,
    TestBackupFilesystemXFS,
    TestBackupLargeLvol,
    TestBackupDeleteInProgress,
    TestBackupPolicyMultipleLvols,
    # Extended backup tests (TC-BCK-150..190)
    TestBackupSecurityLvol,
    TestBackupPolicyVersionsOne,
    TestBackupPolicyMultipleOnSameLvol,
    TestBackupPolicyLvolLevel,
    TestBackupResizedLvol,
    TestBackupListFields,
    TestBackupUpgradeCompatibility,
    TestBackupRestoreEdgeCases,
    TestBackupSourceSwitch,
)

from stress_test.continuous_backup_stress import (
    BackupStressParallelSnapshots,
    BackupStressTcpFailover,
    BackupStressRdmaFailover,
    BackupStressCryptoMix,
    BackupStressPolicyRetention,
    BackupStressRestoreConcurrent,
    TestBackupInterruptedBackup,
    TestBackupInterruptedRestore,
    BackupStressMarathon,
)


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
    TestAddK8sNodesDuringFioRun,
    # Security E2E tests
    TestLvolSecurityCombinations,
    TestLvolAllowedHostsPositive,
    TestLvolAllowedHostsNegative,
    TestLvolDynamicHostManagement,
    TestLvolCryptoWithAllowedHosts,
    TestLvolDhcapDirections,
    TestLvolMultipleAllowedHosts,
    TestLvolAllowedHostsNoDhchap,
    # Security negative tests
    TestLvolSecurityNegativeHostOps,
    TestLvolSecurityNegativeCreation,
    TestLvolSecurityNegativeConnect,
    # Extended security E2E tests (TC-SEC-070..127)
    TestLvolSecurityOutageRecovery,
    TestLvolSecurityNetworkInterrupt,
    TestLvolSecurityHAFailover,
    TestLvolSecurityMgmtNodeReboot,
    TestLvolSecurityDynamicModification,
    TestLvolSecurityMultiClientConcurrent,
    TestLvolSecurityScaleAndRapidOps,
    TestLvolSecurityNegativeConnectExtended,
    TestLvolSecurityCloneOverride,
    TestLvolSecurityWithBackup,
    TestLvolSecurityResize,
    TestLvolSecurityVolumeListFields,
    TestLvolSecurityRDMA,
    # Security stress tests
    RandomSecurityFailoverTest,
    RandomAllSecurityFailoverTest,
    # RDMA stress tests
    RandomRDMAFailoverTest,
    RandomRDMAMultiFailoverTest,
    # Backup E2E tests
    TestBackupBasicPositive,
    TestBackupRestoreDataIntegrity,
    TestBackupPolicy,
    TestBackupNegative,
    TestBackupCryptoLvol,
    TestBackupCustomGeometry,
    TestBackupDeleteAndRestore,
    TestBackupInterruptedBackup,
    TestBackupInterruptedRestore,
    # Backup extra E2E tests (TC-BCK-100..148)
    TestBackupConcurrentIO,
    TestBackupMultipleRestores,
    TestBackupDeltaChainPointInTime,
    TestBackupEmptyLvol,
    TestBackupPoolRecreateRestore,
    TestBackupPolicyAgeOnly,
    TestBackupSnapshotClone,
    TestBackupFilesystemXFS,
    TestBackupLargeLvol,
    TestBackupDeleteInProgress,
    TestBackupPolicyMultipleLvols,
    # Extended backup E2E tests (TC-BCK-150..190)
    TestBackupSecurityLvol,
    TestBackupPolicyVersionsOne,
    TestBackupPolicyMultipleOnSameLvol,
    TestBackupPolicyLvolLevel,
    TestBackupResizedLvol,
    TestBackupListFields,
    TestBackupUpgradeCompatibility,
    TestBackupRestoreEdgeCases,
    TestBackupSourceSwitch,
    # Backup stress tests
    BackupStressParallelSnapshots,
    BackupStressTcpFailover,
    BackupStressRdmaFailover,
    BackupStressCryptoMix,
    BackupStressPolicyRetention,
    BackupStressRestoreConcurrent,
    BackupStressMarathon,
    # Cross-cluster restore — explicit-only (requires CLUSTER2_* env vars)
    TestBackupCrossClusterRestore,
]

def get_all_tests(custom=True, ha_test=False):
    tests = [
        TestLvolFioNpcsCustom,
        TestLvolFioNpcs0,
        TestLvolFioNpcs1,
        TestLvolFioNpcs2,
        # TestLvolFioQOSBW,
        # TestLvolFioQOSIOPS,
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
    # tests += [
    #     # Security E2E tests
    #     TestLvolSecurityCombinations,
    #     TestLvolAllowedHostsPositive,
    #     TestLvolAllowedHostsNegative,
    #     TestLvolDynamicHostManagement,
    #     TestLvolCryptoWithAllowedHosts,
    #     TestLvolDhcapDirections,
    #     TestLvolMultipleAllowedHosts,
    #     TestLvolAllowedHostsNoDhchap,
    #     # Security negative tests
    #     TestLvolSecurityNegativeHostOps,
    #     TestLvolSecurityNegativeCreation,
    #     TestLvolSecurityNegativeConnect,
    # ]
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

def get_security_tests():
    return [
        # Security E2E tests
        TestLvolSecurityCombinations,
        TestLvolAllowedHostsPositive,
        TestLvolAllowedHostsNegative,
        TestLvolDynamicHostManagement,
        TestLvolCryptoWithAllowedHosts,
        TestLvolDhcapDirections,
        TestLvolMultipleAllowedHosts,
        TestLvolAllowedHostsNoDhchap,
        # Security negative tests
        TestLvolSecurityNegativeHostOps,
        TestLvolSecurityNegativeCreation,
        TestLvolSecurityNegativeConnect,
        # Extended security tests (TC-SEC-070..127)
        TestLvolSecurityOutageRecovery,
        TestLvolSecurityNetworkInterrupt,
        TestLvolSecurityHAFailover,
        TestLvolSecurityMgmtNodeReboot,
        TestLvolSecurityDynamicModification,
        TestLvolSecurityMultiClientConcurrent,
        TestLvolSecurityScaleAndRapidOps,
        TestLvolSecurityNegativeConnectExtended,
        TestLvolSecurityCloneOverride,
        TestLvolSecurityWithBackup,
        TestLvolSecurityResize,
        TestLvolSecurityVolumeListFields,
        TestLvolSecurityRDMA,
    ]


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
        RandomMultiClientMultiFailoverAllNodesTest,
        RandomMultiGeometryFailoverTest,
        RandomMultiClient2NodeFailoverTest,
        RandomRDMAFailoverTest,
        RandomRDMAMultiFailoverTest,
        RandomK8sMultiOutageFailoverTest,
        RandomRapidFailoverNoGap,
        TestParallelLvolSnapshotCloneAPI,
        RandomMultiClientFailoverNamespaceTest,
        RandomMultiClientSingleNodeTest,
    ]
    return tests

def get_backup_tests():
    return [
        # E2E backup tests
        TestBackupBasicPositive,
        TestBackupRestoreDataIntegrity,
        TestBackupPolicy,
        TestBackupNegative,
        TestBackupCryptoLvol,
        # TestBackupCustomGeometry, # Will re-enable when we have a way to reliably test it in CI (currently requires manual setup of custom geometry pool)
        TestBackupDeleteAndRestore,
        # Interrupted-operation tests
        TestBackupInterruptedBackup,
        TestBackupInterruptedRestore,
        # Extra coverage tests (TC-BCK-100..148)
        TestBackupConcurrentIO,
        TestBackupMultipleRestores,
        TestBackupDeltaChainPointInTime,
        TestBackupEmptyLvol,
        TestBackupPoolRecreateRestore,
        TestBackupPolicyAgeOnly,
        TestBackupSnapshotClone,
        TestBackupFilesystemXFS,
        TestBackupLargeLvol,
        TestBackupDeleteInProgress,
        TestBackupPolicyMultipleLvols,
        # Extended backup tests (TC-BCK-150..190)
        TestBackupSecurityLvol,
        TestBackupPolicyVersionsOne,
        TestBackupPolicyMultipleOnSameLvol,
        TestBackupPolicyLvolLevel,
        TestBackupResizedLvol,
        TestBackupListFields,
        TestBackupUpgradeCompatibility,
        TestBackupRestoreEdgeCases,
        TestBackupSourceSwitch,
    ]


def get_backup_stress_tests():
    return [
        BackupStressParallelSnapshots,
        BackupStressTcpFailover,
        BackupStressRdmaFailover,
        BackupStressCryptoMix,
        BackupStressPolicyRetention,
        BackupStressRestoreConcurrent,
        BackupStressMarathon,
    ]


def get_upgrade_tests():
    tests = [
        TestMajorUpgrade,
        TestMajorUpgradeSingleNode
    ]
    return tests


def get_load_tests():
    tests = [
        TestLvolOutageLoadTest
    ]
    return tests
