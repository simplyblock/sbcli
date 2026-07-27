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
from e2e_tests.test_multi_node_outage import (
    TestMultiNodeOutageDocker,
    TestMultiNodeOutageK8s,
    TestMultiNodeVMRebootDocker
)


from e2e_tests.add_node_fio_run import (
    TestAddNodesDuringFioRun,
    TestAddK8sNodesDuringFioRun
)
from e2e_tests.k8s_native_add_node import K8sNativeAddNodeTest
from e2e_tests.k8s_native_node_migration import K8sNativeNodeMigrationTest
from e2e_tests.test_add_node_edge_cases import (
    TestSequentialNodeAdd,
    TestAddNodeSnapshotCloneOnNewNode,
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
from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest, K8sNativeBasicFailoverTest, K8sNativeResilientFailoverTest, K8sNativeQuickFailoverTest, K8sNativeScaleBreakTest
from stress_test.continuous_failover_ha_multi_client_quick_outage import (
    RandomRapidFailoverNoGap,
    RandomRapidFailoverNoGapV2WithMigration,
    RandomRapidFailoverNoGapV2NoMigration,
)
from stress_test.continuous_parallel_lvol_snapshot_clone import TestParallelLvolSnapshotCloneAPI
from stress_test.continuous_lvol_dirfill_stress import TestLvolDirFillStress
from stress_test.continuous_failover_ha_namespace import RandomMultiClientFailoverNamespaceTest
from stress_test.continuous_single_node_outage import RandomMultiClientSingleNodeTest
from stress_test.continuous_parallel_namespace_lvol import (
    TestParallelNamespaceLvolDocker,
    TestParallelNamespaceLvolK8s,
)
from stress_test.continuous_bulk_lvol_delete import (
    BulkLvolDeleteDocker,
    BulkLvolDeleteK8s,
    BulkLvolHotDeleteDocker,
    BulkLvolHotDeleteK8s,
)
from stress_test.large_scale_lvol_stress import (
    LargeScaleLvolDocker,
    LargeScaleLvolK8s,
)
from stress_test.mass_create_delete_stress import (
    MassCreateDelete_1x500_Docker,
    MassCreateDelete_30x100_Docker,
    MassCreateDelete_300x10_Docker,
    MassCreateDelete_500x1_Docker,
    MassCreateDelete_3000x1_Docker,
    MassCreateDelete_1x500_K8s,
    MassCreateDelete_30x100_K8s,
    MassCreateDelete_300x10_K8s,
    MassCreateDelete_500x1_K8s,
    MassCreateDelete_3000x1_K8s,
    MassCreateDeletePersistent_1x500_Docker,
    MassCreateDeletePersistent_30x100_Docker,
    MassCreateDeletePersistent_300x10_Docker,
    MassCreateDeletePersistent_300x10_6Snap_Docker,
    MassCreateDeletePersistent_500x1_Docker,
    MassCreateDeletePersistent_3000x1_Docker,
    MassCreateDeletePersistent_1x500_K8s,
    MassCreateDeletePersistent_30x100_K8s,
    MassCreateDeletePersistent_300x10_K8s,
    MassCreateDeletePersistent_500x1_K8s,
    MassCreateDeletePersistent_3000x1_K8s,
)
from stress_test.device_failure_migration import (
    DeviceFailureMigrationNoLoadDocker,
    DeviceFailureMigrationUnderLoadDocker,
    DeviceFailureMigrationPCIeNoLoadDocker,
    DeviceFailureMigrationPCIeUnderLoadDocker,
    DeviceFailureMigrationNoLoadK8s,
    DeviceFailureMigrationUnderLoadK8s,
    DeviceFailureMigrationPCIeNoLoadK8s,
    DeviceFailureMigrationPCIeUnderLoadK8s,
    DevicePCIeRestartNoLoadDocker,
    DevicePCIeRestartUnderLoadDocker,
    DevicePCIeRestartNoLoadK8s,
    DevicePCIeRestartUnderLoadK8s,
    DeviceAddAfterBootstrapDocker,
    DeviceAddAfterBootstrapUnderLoadDocker,
)
from stress_test.continuous_failover_ha_security import (
    RandomSecurityFailoverTest,
    RandomAllSecurityFailoverTest,
)
from stress_test.mgmt_node_network_outage import MgmtNodeNetworkOutageTest, MgmtNodeRebootTest
from stress_test.k8s_native_namespace_failover import (
    K8sNativeNamespacedFailoverTest,
    K8sNativeRapidLifecycleTest,
    K8sNativeMountVerifiedFailoverTest,
)

from e2e_tests.security.test_lvol_security import (
    TestLvolSecurityCombinations,
    TestLvolDynamicHostManagement,
    TestLvolCryptoWithDhchap,
    TestLvolDhchapBidirectional,
    TestLvolSecurityNegativeHostOps,
    TestLvolSecuritySnapshotClone,
    TestLvolSecurityRDMAv2,
    TestLvolSecurityStorageNodeOutage,
    TestLvolSecurityMgmtNodeReboot,
    TestLvolSecurityHAFailover,
    TestLvolSecurityNetworkInterrupt,
    TestLvolSecurityNegativeCreation,
    TestLvolSecurityNegativeConnect,
    TestLvolSecurityDynamicModification,
    TestLvolSecurityScaleAndRapidOps,
    TestLvolSecurityResize,
    TestLvolSecurityWithBackup,
    TestLvolSecurityMultiClientConcurrent,
)

from e2e_tests.upgrade_tests.major_upgrade import TestMajorUpgrade, TestMajorUpgradeSingleNode
from e2e_tests.upgrade_tests.k8s_major_upgrade import K8sNativeMajorUpgrade

# ── Phase 1 functional E2E tests ─────────────────────────────────────
from e2e_tests.test_lvol_basic import TestLvolBasicCRUD
from e2e_tests.test_lvol_stats import TestLvolCapacityIOStats
from e2e_tests.test_lvol_negative import TestLvolNegativeCases
from e2e_tests.test_snapshot_negative import TestSnapshotNegativeCases
# from e2e_tests.test_pool_attributes import TestPoolAttributes  # DISABLED: QoS crash
from e2e_tests.test_pool_enable_disable import TestPoolEnableDisable
from e2e_tests.test_pool_negative import TestPoolNegativeCases
from e2e_tests.test_node_suspend_resume import TestNodeSuspendResume          # DEPRECATED: sn suspend/resume are no-ops
from e2e_tests.test_pool_disable_io import TestPoolDisableIO
from e2e_tests.test_negative_cases import TestCrossResourceNegative
from e2e_tests.test_namespace_placement import TestNamespacePlacement
from e2e_tests.test_namespace_fio import TestNamespaceFio
from e2e_tests.test_namespace_limits import TestNamespaceLimits
from e2e_tests.test_namespace_negative import TestNamespaceNegative
from e2e_tests.test_volume_suspend_resume import TestVolumeSuspendResume      # UNCERTAIN: volume suspend/resume may not be wired
from e2e_tests.test_volume_clone_lvol import TestVolumeCloneLvol
from e2e_tests.test_node_anti_affinity import TestNodeAntiAffinity            # UNCERTAIN: requires --strict-node-anti-affinity cluster flag

# ── Phase 2 functional E2E tests ─────────────────────────────────────
from e2e_tests.test_lvol_inflate import TestLvolInflate
from e2e_tests.test_lvol_migration_load import TestLvolMigrationLoad
from e2e_tests.test_storage_node_stats import TestStorageNodeStats
from e2e_tests.test_storage_node_ports import TestStorageNodePorts
from e2e_tests.test_cluster_stats import TestClusterStats
from e2e_tests.test_cluster_tasks import TestClusterTasks
from e2e_tests.test_cluster_secret import TestClusterSecret
from e2e_tests.test_qos_class import TestQosClass                            # DEPRECATED: QoS class API not verified
from e2e_tests.test_qos_enforcement import TestQosEnforcement                # DEPRECATED: QoS enforcement API not verified
from e2e_tests.test_pool_stats import TestPoolStats
from e2e_tests.test_cluster_graceful_shutdown import TestClusterGracefulShutdown
from e2e_tests.test_multi_client_connect import TestMultiClientConnect
from e2e_tests.test_pool_dhchap import TestPoolDhchap
from e2e_tests.test_pool_capacity_limits import TestPoolCapacityLimits
from e2e_tests.test_namespace_e2e import TestNamespaceE2E
from e2e_tests.test_device_restart import TestDeviceRestart
from e2e_tests.test_volume_priority import TestVolumePriority                # UNCERTAIN: volume priority enforcement unclear
from e2e_tests.test_shared_placement import TestSharedPlacement              # UNCERTAIN: requires cluster set-shared-placement
from e2e_tests.test_qpair_tuning import TestQpairTuning                     # UNCERTAIN: requires RDMA-enabled cluster
from e2e_tests.test_capacity_thresholds import TestCapacityThresholds

# ── Phase 3 functional E2E tests (new coverage gaps) ────────────────
from e2e_tests.test_health_checks import TestHealthChecks
from e2e_tests.test_snapshot_lifecycle import TestSnapshotLifecycle
from e2e_tests.test_migration_lifecycle import TestMigrationLifecycle
from e2e_tests.test_storage_node_listing import TestStorageNodeListing
from e2e_tests.test_device_capacity_io import TestDeviceCapacityIO
from e2e_tests.test_lvol_connect_lifecycle import TestLvolConnectLifecycle
from e2e_tests.test_volume_qos_dynamic import TestVolumeQosDynamic
from e2e_tests.test_cluster_operations import TestClusterOperations
from e2e_tests.test_concurrent_operations import TestConcurrentOperations
from e2e_tests.test_pool_host_management import TestPoolHostManagement
from e2e_tests.test_lvol_placement import TestLvolPlacement
from e2e_tests.test_node_shutdown_restart import TestNodeShutdownRestart

from e2e_tests.backup.test_backup_restore import (
    TestBackupBasicPositive,
    TestBackupRestoreDataIntegrity,
    TestBackupPolicy,
    TestBackupNegative,
    TestBackupCryptoLvol,
    TestBackupCustomGeometry,
    TestBackupRetentionMergeAfterDelete,
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
    # Interrupted backup/restore E2E tests (TC-BCK-080..097)
    TestBackupInterruptedBackup,
    TestBackupInterruptedRestore,
)

from e2e_tests.backup.test_backup_node_add import (
    TestBackupAfterNodeAdd,
    TestBackupWithFioOnNewNode,
)
from e2e_tests.backup.test_backup_node_migration import (
    TestBackupAfterNodeMigration,
    TestBackupDuringMigration,
)

from stress_test.continuous_backup_stress import (
    BackupStressParallelSnapshots,
    BackupStressTcpFailover,
    BackupStressRdmaFailover,
    BackupStressCryptoMix,
    BackupStressPolicyRetention,
    BackupStressRestoreConcurrent,
    BackupStressMarathon,
    BackupStressLargeScale,
    BackupStressFilesystemSecurityMix,
    BackupStressRetentionMergeCycles,
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
    K8sNativeAddNodeTest,
    K8sNativeNodeMigrationTest,
    TestSequentialNodeAdd,
    TestAddNodeSnapshotCloneOnNewNode,
    K8sNativeMajorUpgrade,
    # Security E2E tests
    TestLvolSecurityCombinations,
    TestLvolDynamicHostManagement,
    TestLvolCryptoWithDhchap,
    TestLvolDhchapBidirectional,
    TestLvolSecurityNegativeHostOps,
    TestLvolSecuritySnapshotClone,
    TestLvolSecurityRDMAv2,
    # Security outage tests
    TestLvolSecurityStorageNodeOutage,
    TestLvolSecurityMgmtNodeReboot,
    TestLvolSecurityHAFailover,
    TestLvolSecurityNetworkInterrupt,
    # Security negative / advanced E2E tests
    TestLvolSecurityNegativeCreation,
    TestLvolSecurityNegativeConnect,
    TestLvolSecurityDynamicModification,
    TestLvolSecurityScaleAndRapidOps,
    TestLvolSecurityResize,
    TestLvolSecurityWithBackup,
    TestLvolSecurityMultiClientConcurrent,
    # Security stress tests
    RandomSecurityFailoverTest,
    RandomAllSecurityFailoverTest,
    # RDMA stress tests
    RandomRDMAFailoverTest,
    RandomRDMAMultiFailoverTest,
    # Backup E2E tests
    TestBackupRetentionMergeAfterDelete,
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
    # Backup node-add / node-migration edge cases
    TestBackupAfterNodeAdd,
    TestBackupWithFioOnNewNode,
    TestBackupAfterNodeMigration,
    TestBackupDuringMigration,
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
    # K8s-native failover stress test
    K8sNativeFailoverTest,
    K8sNativeBasicFailoverTest,
    K8sNativeResilientFailoverTest,
    K8sNativeQuickFailoverTest,
    K8sNativeNamespacedFailoverTest,
    K8sNativeRapidLifecycleTest,
    K8sNativeMountVerifiedFailoverTest,
    K8sNativeScaleBreakTest,
    TestParallelNamespaceLvolDocker,
    TestParallelNamespaceLvolK8s,
    BulkLvolDeleteDocker,
    BulkLvolDeleteK8s,
    BulkLvolHotDeleteDocker,
    BulkLvolHotDeleteK8s,
    LargeScaleLvolDocker,
    LargeScaleLvolK8s,
    MassCreateDelete_1x500_Docker,
    MassCreateDelete_30x100_Docker,
    MassCreateDelete_300x10_Docker,
    MassCreateDelete_500x1_Docker,
    MassCreateDelete_3000x1_Docker,
    MassCreateDelete_1x500_K8s,
    MassCreateDelete_30x100_K8s,
    MassCreateDelete_300x10_K8s,
    MassCreateDelete_500x1_K8s,
    MassCreateDelete_3000x1_K8s,
    MassCreateDeletePersistent_1x500_Docker,
    MassCreateDeletePersistent_30x100_Docker,
    MassCreateDeletePersistent_300x10_Docker,
    MassCreateDeletePersistent_300x10_6Snap_Docker,
    MassCreateDeletePersistent_500x1_Docker,
    MassCreateDeletePersistent_3000x1_Docker,
    MassCreateDeletePersistent_1x500_K8s,
    MassCreateDeletePersistent_30x100_K8s,
    MassCreateDeletePersistent_300x10_K8s,
    MassCreateDeletePersistent_500x1_K8s,
    MassCreateDeletePersistent_3000x1_K8s,
    DeviceFailureMigrationNoLoadDocker,
    DeviceFailureMigrationUnderLoadDocker,
    DeviceFailureMigrationPCIeNoLoadDocker,
    DeviceFailureMigrationPCIeUnderLoadDocker,
    DeviceFailureMigrationNoLoadK8s,
    DeviceFailureMigrationUnderLoadK8s,
    DeviceFailureMigrationPCIeNoLoadK8s,
    DeviceFailureMigrationPCIeUnderLoadK8s,
    DevicePCIeRestartNoLoadDocker,
    DevicePCIeRestartUnderLoadDocker,
    DevicePCIeRestartNoLoadK8s,
    DevicePCIeRestartUnderLoadK8s,
    DeviceAddAfterBootstrapDocker,
    DeviceAddAfterBootstrapUnderLoadDocker,
    TestMultiNodeOutageDocker,
    TestMultiNodeOutageK8s,
    TestMultiNodeVMRebootDocker,
    MgmtNodeNetworkOutageTest,
    MgmtNodeRebootTest,
    # ── Phase 1 functional E2E tests ─────────────────────────────────
    TestLvolBasicCRUD,
    TestLvolCapacityIOStats,
    TestLvolNegativeCases,
    TestSnapshotNegativeCases,
    # TestPoolAttributes,  # DISABLED: QoS causes SPDK crash (corrupted double-linked list in bdev_set_qos_limit_done)
    TestPoolEnableDisable,
    TestPoolNegativeCases,
    TestNodeSuspendResume,
    TestPoolDisableIO,
    TestCrossResourceNegative,
    TestNamespacePlacement,
    TestNamespaceFio,
    TestNamespaceLimits,
    TestNamespaceNegative,
    TestVolumeSuspendResume,
    TestVolumeCloneLvol,
    TestNodeAntiAffinity,
    # ── Phase 2 functional E2E tests ─────────────────────────────────
    TestLvolInflate,
    TestLvolMigrationLoad,
    TestStorageNodeStats,
    TestStorageNodePorts,
    TestClusterStats,
    TestClusterTasks,
    TestClusterSecret,
    TestQosClass,
    TestQosEnforcement,
    TestPoolStats,
    TestClusterGracefulShutdown,
    TestMultiClientConnect,
    TestPoolDhchap,
    TestPoolCapacityLimits,
    TestNamespaceE2E,
    TestDeviceRestart,
    TestVolumePriority,
    TestSharedPlacement,
    TestQpairTuning,
    TestCapacityThresholds,
    # ── Phase 3 functional E2E tests (new coverage gaps) ───────────────
    TestHealthChecks,
    TestSnapshotLifecycle,
    TestMigrationLifecycle,
    TestStorageNodeListing,
    TestDeviceCapacityIO,
    TestLvolConnectLifecycle,
    TestVolumeQosDynamic,
    TestClusterOperations,
    TestConcurrentOperations,
    TestPoolHostManagement,
    TestLvolPlacement,
    TestNodeShutdownRestart,
]

def get_all_tests(custom=True, ha_test=False):
    tests = [
        # TestLvolFioNpcsCustom,
        # TestLvolFioNpcs0,
        # TestLvolFioNpcs1,
        # TestLvolFioNpcs2,
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

        # ── Phase 1 functional E2E tests ─────────────────────────────
        # TestLvolBasicCRUD,
        # TestLvolCapacityIOStats,
        # TestLvolNegativeCases,
        # TestSnapshotNegativeCases,
        # # TestPoolAttributes,  # DISABLED: QoS causes SPDK crash (corrupted double-linked list in bdev_set_qos_limit_done)
        # TestPoolEnableDisable,
        # TestPoolNegativeCases,
        # # TestNodeSuspendResume,          # DEPRECATED: sn suspend/resume are no-ops in CLI
        # TestPoolDisableIO,
        # TestCrossResourceNegative,
        # TestNamespacePlacement,
        # TestNamespaceFio,
        # TestNamespaceLimits,
        # TestNamespaceNegative,
        # # TestVolumeSuspendResume,        # UNCERTAIN: volume suspend/resume may not be wired to API
        # TestVolumeCloneLvol,
        # # TestNodeAntiAffinity,           # UNCERTAIN: requires --strict-node-anti-affinity cluster flag

        # # ── Phase 2 functional E2E tests ─────────────────────────────
        # TestLvolInflate,
        # TestLvolMigrationLoad,
        # TestStorageNodeStats,
        # TestStorageNodePorts,
        # TestClusterStats,
        # TestClusterTasks,
        # TestClusterSecret,
        # # TestQosClass,                   # DEPRECATED: QoS class API not verified
        # # TestQosEnforcement,             # DEPRECATED: QoS enforcement API not verified
        # TestPoolStats,
        # TestClusterGracefulShutdown,
        # TestMultiClientConnect,
        # TestPoolDhchap,
        # TestPoolCapacityLimits,
        # TestNamespaceE2E,
        # TestDeviceRestart,
        # # TestVolumePriority,             # UNCERTAIN: volume priority enforcement unclear
        # # TestSharedPlacement,            # UNCERTAIN: requires cluster set-shared-placement
        # # TestQpairTuning,               # UNCERTAIN: requires RDMA-enabled cluster
        # TestCapacityThresholds,

        # # ── Phase 3 functional E2E tests (new coverage gaps) ───────────
        # TestHealthChecks,
        # TestSnapshotLifecycle,
        # TestMigrationLifecycle,
        # TestStorageNodeListing,
        # TestDeviceCapacityIO,
        # TestLvolConnectLifecycle,
        # # TestVolumeQosDynamic,
        # TestClusterOperations,
        # TestConcurrentOperations,
        # TestPoolHostManagement,
        # TestLvolPlacement,
        # TestNodeShutdownRestart,
    ]
    # tests += [
    #     # Security E2E tests
    #     TestLvolSecurityCombinations,
    #     TestLvolDynamicHostManagement,
    #     TestLvolCryptoWithDhchap,
    #     TestLvolDhchapBidirectional,
    #     TestLvolSecurityNegativeHostOps,
    #     TestLvolSecuritySnapshotClone,
    #     TestLvolSecurityRDMAv2,
    # ]
    # if not custom:
    #     tests.remove(TestLvolFioNpcsCustom)
    # else:
    #     tests.remove(TestLvolFioNpcs0)
    #     tests.remove(TestLvolFioNpcs1)
    #     tests.remove(TestLvolFioNpcs2)
    if not ha_test:
        tests.remove(TestHASingleNodeFailure)
        # tests.remove(TestHASingleNodeReboot)
        # tests.remove(TestHASingleNodeOutage)
    return tests

def get_security_tests():
    return [
        # Security E2E tests
        TestLvolSecurityCombinations,
        TestLvolDynamicHostManagement,
        TestLvolCryptoWithDhchap,
        TestLvolDhchapBidirectional,
        TestLvolSecurityNegativeHostOps,
        TestLvolSecuritySnapshotClone,
        TestLvolSecurityRDMAv2,
        # Security negative / advanced E2E tests
        TestLvolSecurityNegativeCreation,
        TestLvolSecurityNegativeConnect,
        TestLvolSecurityDynamicModification,
        TestLvolSecurityScaleAndRapidOps,
        TestLvolSecurityResize,
        TestLvolSecurityWithBackup,
        TestLvolSecurityMultiClientConcurrent,
        # Security outage tests — run last (involves node shutdown/restart)
        TestLvolSecurityStorageNodeOutage,
        TestLvolSecurityMgmtNodeReboot,
        TestLvolSecurityHAFailover,
        TestLvolSecurityNetworkInterrupt,
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
        MgmtNodeNetworkOutageTest,
        MgmtNodeRebootTest,
        RandomRapidFailoverNoGap,
        RandomRapidFailoverNoGapV2WithMigration,
        RandomRapidFailoverNoGapV2NoMigration,
        TestParallelLvolSnapshotCloneAPI,
        TestLvolDirFillStress,
        RandomMultiClientFailoverNamespaceTest,
        RandomMultiClientSingleNodeTest,
        K8sNativeFailoverTest,
        K8sNativeBasicFailoverTest,
        K8sNativeResilientFailoverTest,
        K8sNativeQuickFailoverTest,
        K8sNativeNamespacedFailoverTest,
        K8sNativeRapidLifecycleTest,
        K8sNativeMountVerifiedFailoverTest,
        K8sNativeScaleBreakTest,
        TestParallelNamespaceLvolDocker,
        TestParallelNamespaceLvolK8s,
        BulkLvolDeleteDocker,
        BulkLvolDeleteK8s,
        BulkLvolHotDeleteDocker,
        BulkLvolHotDeleteK8s,
        LargeScaleLvolDocker,
        LargeScaleLvolK8s,
        MassCreateDelete_1x500_Docker,
        MassCreateDelete_30x100_Docker,
        MassCreateDelete_300x10_Docker,
        MassCreateDelete_500x1_Docker,
        MassCreateDelete_3000x1_Docker,
        MassCreateDelete_1x500_K8s,
        MassCreateDelete_30x100_K8s,
        MassCreateDelete_300x10_K8s,
        MassCreateDelete_500x1_K8s,
        MassCreateDelete_3000x1_K8s,
        MassCreateDeletePersistent_1x500_Docker,
        MassCreateDeletePersistent_30x100_Docker,
        MassCreateDeletePersistent_300x10_Docker,
        MassCreateDeletePersistent_300x10_6Snap_Docker,
        MassCreateDeletePersistent_500x1_Docker,
        MassCreateDeletePersistent_3000x1_Docker,
        MassCreateDeletePersistent_1x500_K8s,
        MassCreateDeletePersistent_30x100_K8s,
        MassCreateDeletePersistent_300x10_K8s,
        MassCreateDeletePersistent_500x1_K8s,
        MassCreateDeletePersistent_3000x1_K8s,
        DeviceFailureMigrationNoLoadDocker,
        DeviceFailureMigrationUnderLoadDocker,
        DeviceFailureMigrationPCIeNoLoadDocker,
        DeviceFailureMigrationPCIeUnderLoadDocker,
        DeviceFailureMigrationNoLoadK8s,
        DeviceFailureMigrationUnderLoadK8s,
        DeviceFailureMigrationPCIeNoLoadK8s,
        DeviceFailureMigrationPCIeUnderLoadK8s,
        DevicePCIeRestartNoLoadDocker,
        DevicePCIeRestartUnderLoadDocker,
        DevicePCIeRestartNoLoadK8s,
        DevicePCIeRestartUnderLoadK8s,
        DeviceAddAfterBootstrapDocker,
        DeviceAddAfterBootstrapUnderLoadDocker,
    ]
    return tests


def get_monitoring_tests():
    """Tests that produce timing/performance data for the monitoring suite."""
    return [
        TestParallelNamespaceLvolDocker,
        TestParallelNamespaceLvolK8s,
        BulkLvolDeleteDocker,
        BulkLvolDeleteK8s,
        BulkLvolHotDeleteDocker,
        BulkLvolHotDeleteK8s,
        LargeScaleLvolDocker,
        LargeScaleLvolK8s,
        MassCreateDelete_1x500_Docker,
        MassCreateDelete_30x100_Docker,
        MassCreateDelete_300x10_Docker,
        MassCreateDelete_500x1_Docker,
        MassCreateDelete_3000x1_Docker,
        MassCreateDelete_1x500_K8s,
        MassCreateDelete_30x100_K8s,
        MassCreateDelete_300x10_K8s,
        MassCreateDelete_500x1_K8s,
        MassCreateDelete_3000x1_K8s,
        MassCreateDeletePersistent_1x500_Docker,
        MassCreateDeletePersistent_30x100_Docker,
        MassCreateDeletePersistent_300x10_Docker,
        MassCreateDeletePersistent_300x10_6Snap_Docker,
        MassCreateDeletePersistent_500x1_Docker,
        MassCreateDeletePersistent_3000x1_Docker,
        MassCreateDeletePersistent_1x500_K8s,
        MassCreateDeletePersistent_30x100_K8s,
        MassCreateDeletePersistent_300x10_K8s,
        MassCreateDeletePersistent_500x1_K8s,
        MassCreateDeletePersistent_3000x1_K8s,
        DeviceFailureMigrationNoLoadDocker,
        DeviceFailureMigrationUnderLoadDocker,
        DeviceFailureMigrationPCIeNoLoadDocker,
        DeviceFailureMigrationPCIeUnderLoadDocker,
        DeviceFailureMigrationNoLoadK8s,
        DeviceFailureMigrationUnderLoadK8s,
        DeviceFailureMigrationPCIeNoLoadK8s,
        DeviceFailureMigrationPCIeUnderLoadK8s,
        DevicePCIeRestartNoLoadDocker,
        DevicePCIeRestartUnderLoadDocker,
        DevicePCIeRestartNoLoadK8s,
        DevicePCIeRestartUnderLoadK8s,
        DeviceAddAfterBootstrapDocker,
        DeviceAddAfterBootstrapUnderLoadDocker,
        TestLvolOutageLoadTest,
        TestParallelLvolSnapshotCloneAPI,
    ]

def get_backup_tests():
    return [
        # Regression: retention merge after delete must run first (clean S3 bucket)
        TestBackupRetentionMergeAfterDelete,
        # E2E backup tests
        TestBackupBasicPositive,
        TestBackupRestoreDataIntegrity,
        TestBackupPolicy,
        TestBackupNegative,
        TestBackupCryptoLvol,
        # TestBackupCustomGeometry, # Will re-enable when we have a way to reliably test it in CI (currently requires manual setup of custom geometry pool)
        TestBackupDeleteAndRestore,
        # Extra coverage tests (TC-BCK-100..148)
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
        TestBackupRestoreEdgeCases,
        TestBackupSourceSwitch,
        # Outage tests — run last (involves node shutdown/restart)
        TestBackupUpgradeCompatibility,
        TestBackupInterruptedBackup,
        TestBackupInterruptedRestore,
        TestBackupConcurrentIO,
        # Backup node-add / node-migration edge cases
        TestBackupAfterNodeAdd,
        TestBackupWithFioOnNewNode,
        TestBackupAfterNodeMigration,
        TestBackupDuringMigration,
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
        BackupStressLargeScale,
        BackupStressFilesystemSecurityMix,
        BackupStressRetentionMergeCycles,
    ]


def get_upgrade_tests():
    tests = [
        TestMajorUpgrade,
        TestMajorUpgradeSingleNode,
        K8sNativeMajorUpgrade,
    ]
    return tests


def get_load_tests():
    tests = [
        TestLvolOutageLoadTest
    ]
    return tests


def get_parity_tests():
    """API parity audit — CLI vs v1 vs v2 three-way comparison."""
    try:
        from e2e_tests.test_api_parity_audit import TestAPIParityAudit
        return [TestAPIParityAudit]
    except ImportError:
        return []
