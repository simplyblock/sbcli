# First, and before any test module: a few tests import the product for its
# constants, and e2e runs from e2e/ where the repo root is not on sys.path.
# See _product_path.py -- this used to work only by accident.
import _product_path  # noqa: F401
from e2e_tests.add_node_fio_run import (
    TestAddK8sNodesDualNodePerHost,
    TestAddK8sNodesDuringFioRun,
    TestAddNodesDualNodePerHost,
    TestAddNodesDuringFioRun,
)
from e2e_tests.backup.test_backup_node_add import (
    TestBackupAfterNodeAdd,
    TestBackupWithFioOnNewNode,
)
from e2e_tests.backup.test_backup_node_migration import (
    TestBackupAfterNodeMigration,
    TestBackupDuringMigration,
)
from e2e_tests.backup.test_backup_restore import (
    TestBackupBasicPositive,
    TestBackupBulkLoadIntegrity,
    # Extra coverage tests (TC-BCK-100..148)
    TestBackupConcurrentIO,
    # DISABLED: lvol-level backup delete not supported (SFAM-2792)
    # TestBackupRetentionMergeAfterDelete,
    # TestBackupDeleteAndRestore,
    TestBackupCrossClusterRestore,  # NOT in get_backup_tests(); run explicitly only
    TestBackupCryptoLvol,
    TestBackupCustomGeometry,
    TestBackupDeltaChainPointInTime,
    TestBackupEmptyLvol,
    TestBackupFilesystemXFS,
    TestBackupHighVolumeCombosSequential,
    # TestBackupSourceSwitch,  # COMMENTED OUT: source-switch no longer required
    # Interrupted backup/restore E2E tests (TC-BCK-080..097)
    TestBackupInterruptedBackup,
    TestBackupInterruptedRestore,
    TestBackupLargeLvol,
    TestBackupListFields,
    TestBackupMultipleRestores,
    TestBackupNegative,
    TestBackupPolicy,
    TestBackupPolicyAgeOnly,
    TestBackupPolicyLvolLevel,
    # DISABLED: lvol-level backup delete not supported (SFAM-2792)
    # TestBackupDeleteInProgress,
    TestBackupPolicyMultipleLvols,
    TestBackupPolicyMultipleOnSameLvol,
    TestBackupPolicyVersionsOne,
    TestBackupPoolRecreateRestore,
    TestBackupResizedLvol,
    TestBackupRestoreDataIntegrity,
    TestBackupRestoreEdgeCases,
    # Extended backup tests (TC-BCK-150..190)
    TestBackupSecurityLvol,
    TestBackupSnapshotClone,
    TestBackupUpgradeCompatibility,
)
from e2e_tests.batch_lvol_limit import TestBatchLVOLsLimit
from e2e_tests.cloning_and_snapshot.multi_lvol_snapshot_fio import TestMultiLvolFio
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from e2e_tests.ha_journal.lvol_journal_device_node_restart import TestDeviceNodeRestart
from e2e_tests.k8s_native_add_node import K8sNativeAddNodeTest
from e2e_tests.k8s_native_node_migration import K8sNativeNodeMigrationTest
from e2e_tests.lblk.test_lblk import (
    LblkDeviceFaultDocker,
    LblkDeviceFaultK8s,
    LblkFunctionalDocker,
    LblkFunctionalK8s,
    LblkIntegrityDocker,
    LblkIntegrityK8s,
    LblkJournalRecoveryDocker,
    LblkJournalRecoveryK8s,
)
from e2e_tests.mgmt_restart_fio_run import TestMgmtNodeReboot
from e2e_tests.migration.test_migration_batch import (
    MigrationBatchFlat,
    MigrationBatchNegative,
    MigrationBatchNsGaps,
    MigrationBatchWithTrees,
)
from e2e_tests.migration.test_migration_fault import (
    MigrationCancel,
    MigrationClusterRestartBetween,
    MigrationHaPartnerRestart,
    MigrationRetryAfterTargetReboot,
    MigrationSourceAndTargetFaults,
    MigrationTargetOfflinePerPhase,
)
from e2e_tests.migration.test_migration_happy import (
    MigrationHappyPath,
    MigrationRoundTrip,
    MigrationWithSnapshots,
)
from e2e_tests.migration.test_migration_live import (
    MigrationLiveBystanders,
    MigrationLiveGroup,
    MigrationLiveSingle,
)
from e2e_tests.migration.test_migration_negative import (
    MigrationNegativeCapacity,
    MigrationNegativeTargets,
    MigrationProtectionGuards,
    MigrationUnderReplication,
)
from e2e_tests.migration.test_migration_scale import (
    MigrationLargeVolume,
    MigrationManyVolumes,
    MigrationSoak,
)
from e2e_tests.migration.test_migration_smoke import MigrationSmoke
from e2e_tests.migration.test_migration_topology import (
    MigrationConcurrent,
    MigrationHaOverlapMatrix,
    MigrationSnapshotCloneTrees,
)
from e2e_tests.multi_lvol_run_fio import TestManyLvolSameNode
from e2e_tests.multi_node_crash_fio_clone import TestMultiFioSnapshotDowntime
from e2e_tests.objectstore.test_objectstore_gateway import (
    S3BucketDeliveredWithoutRestartK8s,
    S3GatewayNodeSelectorK8s,
    S3GatewayPodKilledK8s,
    S3GatewayRescheduledK8s,
    S3GatewayScaleOutK8s,
    S3GatewayTLSK8s,
    S3TwoObjectStoresSameNameK8s,
)
from e2e_tests.objectstore.test_objectstore_lifecycle import (
    S3BucketCapacityExhaustionK8s,
    S3BucketCapacityPatchK8s,
    S3BucketLifecycleK8s,
    S3BucketNameBoundaryK8s,
    S3ObjectStoreDeletedWithBucketsK8s,
    S3ObjectStoreReplicasZeroK8s,
)
from e2e_tests.objectstore.test_objectstore_resilience import (
    S3BucketProvisioningLatencyK8s,
    S3MissingEnvFromSecretK8s,
    S3OpenBaoSealedK8s,
    S3OperatorRestartK8s,
    S3RBACFullCycleK8s,
)
from e2e_tests.pnfs.test_pnfs_failure import (
    PnfsClientNodeRebootK8s,
    PnfsClientPathLossK8s,
    PnfsCsiKilledMidOperationK8s,
    PnfsGuestCrashInPlaceK8s,
    PnfsMdsPartitionedFromClientK8s,
    PnfsProvisioningChurnK8s,
    PnfsUnalignedDirectIoReportK8s,
)
from e2e_tests.pnfs.test_pnfs_load import (
    PnfsExportCeilingK8s,
    PnfsManyClientsLoadK8s,
    PnfsRestartRebootSoakK8s,
)
from e2e_tests.pnfs.test_pnfs_mds_standby import (
    PnfsMdsActiveDeletedK8s,
    PnfsMdsDefaultTopologyK8s,
    PnfsMdsExportGrownBeforeTakeoverK8s,
    PnfsMdsLockAcrossTakeoverK8s,
    PnfsMdsManyExportsTakeoverK8s,
    PnfsMdsNodeDrainedK8s,
    PnfsMdsNodeRebootK8s,
    PnfsMdsNoStandbyRecoversK8s,
    PnfsMdsOperatorRestartNoTakeoverK8s,
    PnfsMdsRestartWithClientDownK8s,
    PnfsMdsSecondTakeoverK8s,
    PnfsMdsStandbyMissingK8s,
    PnfsMdsStandbyScaleK8s,
)
from e2e_tests.pnfs.test_pnfs_rwx import (
    PnfsByteRangeLockK8s,
    PnfsClaimCeilingK8s,
    PnfsCloseToOpenK8s,
    PnfsCrossClientReadK8s,
    PnfsDeleteLeakGateK8s,
    PnfsDeviceProofK8s,
    PnfsFsidUniquenessK8s,
    PnfsLayoutProofK8s,
    PnfsMetadataStormK8s,
    PnfsMixedRwxRwoK8s,
    PnfsNoMetadataServerK8s,
    PnfsOnlineResizeK8s,
    PnfsScaleThirtyPodsK8s,
    PnfsSharedWriteVerifyK8s,
    PnfsSnapshotRestoreK8s,
    PnfsTwoNamespacesK8s,
)
from e2e_tests.pnfs.test_pnfs_security import (
    PnfsExportAllowListK8s,
    PnfsGuestFencingPreemptsOldEpochK8s,
    PnfsGuestHoldsNoCredentialsK8s,
    PnfsPodCidrDirectMountK8s,
    PnfsRootSquashK8s,
    PnfsUnauthorizedNqnRefusedK8s,
)
from e2e_tests.reboot_on_another_node_fio_run import TestRestartNodeOnAnotherHost
from e2e_tests.replication.test_replication_consistency import (
    ConsistencyGroupGeneration,
    ConsistencyGroupPlacement,
)
from e2e_tests.replication.test_replication_csi_addons import (
    CsiAddonsEnableAndReport,
    CsiAddonsForcedFailoverAndOutage,
    CsiAddonsGroupReplication,
    CsiAddonsOwnership,
    CsiAddonsPromoteDemote,
)
from e2e_tests.replication.test_replication_functional import (
    ReplicationHarness,
    ReplicationPolicyVariants,
    ReplicationStartsAndReports,
    ReplicationTargetsAndPolicies,
)
from e2e_tests.replication.test_replication_integrity import (
    ReplicationConvergesUnderLoad,
    ReplicationDataIsIdentical,
    ReplicationNoMetadataCorruption,
)
from e2e_tests.replication.test_replication_negative import (
    ReplicationNegativeConfiguration,
    ReplicationNegativeOperations,
)
from e2e_tests.replication.test_replication_outage import (
    ReplicationConsistencyGroupNodeLoss,
    ReplicationControlPlaneAndDeviceOutages,
    ReplicationNetworkOutages,
    ReplicationOutageDuringOperations,
    ReplicationSourceNodeOutages,
    ReplicationTargetNodeOutage,
)
from e2e_tests.replication.test_replication_recovery import (
    ReplicationFailback,
    ReplicationFailoverDataLoss,
    ReplicationFailoverScopes,
    ReplicationMigration,
)
from e2e_tests.replication.test_replication_scale import (
    ReplicationLargeVolume,
    ReplicationManyVolumes,
    ReplicationOverlappingIntervals,
    ReplicationSoak,
)
from e2e_tests.replication.test_replication_upgrade import (
    ReplicationAnnotationToCsiAddonsMigration,
    ReplicationSurvivesControlPlaneUpgrade,
    ReplicationSurvivesOperatorUpgrade,
)
from e2e_tests.security.test_lvol_security import (
    TestDhchapPodScheduling,
    TestLvolCryptoWithDhchap,
    TestLvolDhchapBidirectional,
    TestLvolDynamicHostManagement,
    TestLvolSecurityCombinations,
    TestLvolSecurityDynamicModification,
    TestLvolSecurityHAFailover,
    TestLvolSecurityMgmtNodeReboot,
    TestLvolSecurityMultiClientConcurrent,
    TestLvolSecurityNegativeConnect,
    TestLvolSecurityNegativeCreation,
    TestLvolSecurityNegativeHostOps,
    TestLvolSecurityNetworkInterrupt,
    TestLvolSecurityRDMAv2,
    TestLvolSecurityResize,
    TestLvolSecurityScaleAndRapidOps,
    TestLvolSecuritySnapshotClone,
    TestLvolSecurityStorageNodeOutage,
    TestLvolSecurityWithBackup,
)
from e2e_tests.single_node_failure import TestHASingleNodeFailure, TestSingleNodeFailure
from e2e_tests.single_node_multi_fio_perf import (
    TestLvolFioNpcs0,
    TestLvolFioNpcs1,
    TestLvolFioNpcs2,
    TestLvolFioNpcsCustom,
)
from e2e_tests.single_node_outage import TestHASingleNodeOutage, TestSingleNodeOutage
from e2e_tests.single_node_qos import TestLvolFioQOSBW, TestLvolFioQOSIOPS
from e2e_tests.single_node_reboot import TestHASingleNodeReboot, TestSingleNodeReboot
from e2e_tests.single_node_resize import TestSingleNodeResizeLvolCone
from e2e_tests.single_node_vm_reboot import TestRebootNodeHost
from e2e_tests.test_add_node_edge_cases import (
    TestAddNodeSnapshotCloneOnNewNode,
    TestSequentialNodeAdd,
)
from e2e_tests.test_api_parity_audit import TestAPIParityAudit
from e2e_tests.test_capacity_thresholds import TestCapacityThresholds
from e2e_tests.test_cluster_graceful_shutdown import TestClusterGracefulShutdown
from e2e_tests.test_cluster_operations import TestClusterOperations
from e2e_tests.test_cluster_secret import TestClusterSecret
from e2e_tests.test_cluster_stats import TestClusterStats
from e2e_tests.test_cluster_tasks import TestClusterTasks
from e2e_tests.test_concurrent_operations import TestConcurrentOperations
from e2e_tests.test_device_capacity_io import TestDeviceCapacityIO
from e2e_tests.test_device_restart import TestDeviceRestart

# ── Phase 3 functional E2E tests (new coverage gaps) ────────────────
from e2e_tests.test_health_checks import TestHealthChecks

# ── Phase 1 functional E2E tests ─────────────────────────────────────
from e2e_tests.test_lvol_basic import TestLvolBasicCRUD
from e2e_tests.test_lvol_connect_lifecycle import TestLvolConnectLifecycle

# ── Phase 2 functional E2E tests ─────────────────────────────────────
from e2e_tests.test_lvol_inflate import TestLvolInflate
from e2e_tests.test_lvol_migration_load import TestLvolMigrationLoad
from e2e_tests.test_lvol_negative import TestLvolNegativeCases
from e2e_tests.test_lvol_placement import TestLvolPlacement
from e2e_tests.test_lvol_stats import TestLvolCapacityIOStats
from e2e_tests.test_migration_lifecycle import TestMigrationLifecycle
from e2e_tests.test_multi_client_connect import TestMultiClientConnect
from e2e_tests.test_multi_node_outage import (
    TestMultiNodeOutageDocker,
    TestMultiNodeOutageK8s,
    TestMultiNodeVMRebootDocker,
)
from e2e_tests.test_namespace_e2e import TestNamespaceE2E
from e2e_tests.test_namespace_fio import TestNamespaceFio
from e2e_tests.test_namespace_limits import TestNamespaceLimits
from e2e_tests.test_namespace_negative import TestNamespaceNegative
from e2e_tests.test_namespace_placement import TestNamespacePlacement
from e2e_tests.test_negative_cases import TestCrossResourceNegative
from e2e_tests.test_node_anti_affinity import (
    TestNodeAntiAffinity,  # UNCERTAIN: requires --strict-node-anti-affinity cluster flag
)
from e2e_tests.test_node_shutdown_restart import TestNodeShutdownRestart
from e2e_tests.test_object_limits import TestObjectLimits
from e2e_tests.test_pool_capacity_limits import TestPoolCapacityLimits
from e2e_tests.test_pool_dhchap import TestPoolDhchap
from e2e_tests.test_pool_disable_io import TestPoolDisableIO

# from e2e_tests.test_pool_attributes import TestPoolAttributes  # DISABLED: QoS crash
from e2e_tests.test_pool_enable_disable import TestPoolEnableDisable
from e2e_tests.test_pool_host_management import TestPoolHostManagement
from e2e_tests.test_pool_negative import TestPoolNegativeCases
from e2e_tests.test_pool_stats import TestPoolStats
from e2e_tests.test_qos_class import (
    TestQosClass,  # DEPRECATED: QoS class API not verified
)
from e2e_tests.test_qos_enforcement import (
    TestQosEnforcement,  # DEPRECATED: QoS enforcement API not verified
)
from e2e_tests.test_qpair_tuning import (
    TestQpairTuning,  # UNCERTAIN: requires RDMA-enabled cluster
)
from e2e_tests.test_shared_placement import (
    TestSharedPlacement,  # UNCERTAIN: requires cluster set-shared-placement
)
from e2e_tests.test_snapshot_lifecycle import TestSnapshotLifecycle
from e2e_tests.test_snapshot_negative import TestSnapshotNegativeCases
from e2e_tests.test_storage_node_listing import TestStorageNodeListing
from e2e_tests.test_storage_node_ports import TestStorageNodePorts
from e2e_tests.test_storage_node_stats import TestStorageNodeStats
from e2e_tests.test_volume_clone_lvol import TestVolumeCloneLvol
from e2e_tests.test_volume_priority import (
    TestVolumePriority,  # UNCERTAIN: volume priority enforcement unclear
)
from e2e_tests.test_volume_qos_dynamic import TestVolumeQosDynamic
from e2e_tests.test_volume_suspend_resume import (
    TestVolumeSuspendResume,  # UNCERTAIN: volume suspend/resume may not be wired
)
from e2e_tests.upgrade_tests.k8s_major_upgrade import (
    K8sNativeMajorUpgrade,
    K8sNativeMajorUpgradeDualNode,
)
from e2e_tests.upgrade_tests.major_upgrade import (
    TestMajorUpgrade,
    TestMajorUpgradeDualNode,
    TestMajorUpgradeSingleNode,
)
from load_tests.lvol_outage_load import TestLvolOutageLoadTest
from load_tests.migration_load import (
    MigrationConcurrencyCurve,
    MigrationFreezeByVolumeSize,
    MigrationTimeBySnapshotCount,
)
from load_tests.replication_load import (
    ReplicationCycleTimeBySize,
    ReplicationDemoteDowntimeBySize,
)
from stress_test.continuous_backup_stress import (
    BackupStressComprehensive,
    BackupStressCryptoMix,
    BackupStressFilesystemSecurityMix,
    BackupStressLargeScale,
    BackupStressMarathon,
    BackupStressParallelSnapshots,
    BackupStressPolicyRetention,
    BackupStressRdmaFailover,
    BackupStressRestoreConcurrent,
    BackupStressRetentionMergeCycles,
    BackupStressTcpFailover,
)
from stress_test.continuous_bulk_lvol_delete import (
    BulkLvolDeleteDocker,
    BulkLvolDeleteK8s,
    BulkLvolHotDeleteDocker,
    BulkLvolHotDeleteK8s,
)
from stress_test.continuous_failover_ha import RandomFailoverTest
from stress_test.continuous_failover_ha_2node import RandomMultiClient2NodeFailoverTest
from stress_test.continuous_failover_ha_geomtery import RandomMultiGeometryFailoverTest
from stress_test.continuous_failover_ha_k8s import RandomK8sMultiOutageFailoverTest
from stress_test.continuous_failover_ha_multi_client import (
    RandomMultiClientFailoverTest,
)
from stress_test.continuous_failover_ha_multi_client_quick_outage import (
    RandomRapidFailoverNoGap,
    RandomRapidFailoverNoGapV2NoMigration,
    RandomRapidFailoverNoGapV2WithMigration,
)
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
)
from stress_test.continuous_failover_ha_multi_outage_all_nodes import (
    RandomMultiClientMultiFailoverAllNodesTest,
)
from stress_test.continuous_failover_ha_namespace import (
    RandomMultiClientFailoverNamespaceTest,
)
from stress_test.continuous_failover_ha_rdma import RandomRDMAFailoverTest
from stress_test.continuous_failover_ha_rdma_multi_outage import (
    RandomRDMAMultiFailoverTest,
)
from stress_test.continuous_failover_ha_security import (
    RandomAllSecurityFailoverTest,
    RandomSecurityFailoverTest,
)
from stress_test.continuous_k8s_native_failover import (
    K8sNativeBasicFailoverTest,
    K8sNativeFailoverTest,
    K8sNativeQuickFailoverTest,
    K8sNativeRapidFailoverNoGapTest,
    K8sNativeResilientFailoverTest,
    K8sNativeScaleBreakTest,
)
from stress_test.continuous_lvol_dirfill_stress import TestLvolDirFillStress
from stress_test.continuous_parallel_lvol_snapshot_clone import (
    TestParallelLvolSnapshotCloneAPI,
)
from stress_test.continuous_parallel_namespace_lvol import (
    TestParallelNamespaceLvolDocker,
    TestParallelNamespaceLvolK8s,
)
from stress_test.continuous_single_node_outage import RandomMultiClientSingleNodeTest
from stress_test.device_failure_migration import (
    DeviceAddAfterBootstrapDocker,
    DeviceAddAfterBootstrapUnderLoadDocker,
    DeviceFailureMigrationNoLoadDocker,
    DeviceFailureMigrationNoLoadK8s,
    DeviceFailureMigrationPCIeNoLoadDocker,
    DeviceFailureMigrationPCIeNoLoadK8s,
    DeviceFailureMigrationPCIeUnderLoadDocker,
    DeviceFailureMigrationPCIeUnderLoadK8s,
    DeviceFailureMigrationUnderLoadDocker,
    DeviceFailureMigrationUnderLoadK8s,
    DevicePCIeRestartNoLoadDocker,
    DevicePCIeRestartNoLoadK8s,
    DevicePCIeRestartUnderLoadDocker,
    DevicePCIeRestartUnderLoadK8s,
)
from stress_test.dual_outage_matrix import (
    DualOutageMatrixDocker,
    DualOutageMatrixK8s,
    LblkDualOutageMatrixDocker,
    LblkDualOutageMatrixK8s,
)
from stress_test.k8s_native_namespace_failover import (
    K8sNativeMountVerifiedFailoverTest,
    K8sNativeNamespacedFailoverTest,
    K8sNativeRapidLifecycleTest,
)
from stress_test.large_scale_lvol_stress import (
    LargeScaleLvolDocker,
    LargeScaleLvolK8s,
)
from stress_test.lblk_outage_matrix import (
    LblkOutageMatrixDocker,
    LblkOutageMatrixK8s,
)
from stress_test.lblk_stress import (
    LblkMultiOutageStressDocker,
    LblkQuickOutageStressDocker,
    LblkRapidOutageStressDocker,
    LblkRapidOutageStressK8s,
    LblkResilientStressK8s,
    LblkStressDocker,
    LblkStressK8s,
)
from stress_test.lvol_ha_stress_fio import (
    TestLvolHAClusterGracefulShutdown,
    TestLvolHAClusterNetworkInterrupt,
    TestLvolHAClusterPartialNetworkOutage,
    TestLvolHAClusterRunAllScenarios,
    TestLvolHAClusterStorageNodeCrash,
)
from stress_test.lvol_snap_clone_fio_failover import TestFailoverScenariosStorageNodes
from stress_test.lvol_stress_fio_run import TestStressLvolCloneClusterFioRun
from stress_test.mass_create_delete_stress import (
    MassCreateDelete_1x500_Docker,
    MassCreateDelete_1x500_K8s,
    MassCreateDelete_30x100_Docker,
    MassCreateDelete_30x100_K8s,
    MassCreateDelete_300x10_Docker,
    MassCreateDelete_300x10_K8s,
    MassCreateDelete_500x1_Docker,
    MassCreateDelete_500x1_K8s,
    MassCreateDelete_3000x1_Docker,
    MassCreateDelete_3000x1_K8s,
    MassCreateDeletePersistent_1x500_Docker,
    MassCreateDeletePersistent_1x500_K8s,
    MassCreateDeletePersistent_30x100_Docker,
    MassCreateDeletePersistent_30x100_K8s,
    MassCreateDeletePersistent_300x10_6Snap_Docker,
    MassCreateDeletePersistent_300x10_6Snap_K8s,
    MassCreateDeletePersistent_300x10_10Snap_Docker,
    MassCreateDeletePersistent_300x10_10Snap_K8s,
    MassCreateDeletePersistent_300x10_Docker,
    MassCreateDeletePersistent_300x10_K8s,
    MassCreateDeletePersistent_500x1_Docker,
    MassCreateDeletePersistent_500x1_K8s,
    MassCreateDeletePersistent_3000x1_Docker,
    MassCreateDeletePersistent_3000x1_K8s,
    MassCreateDeleteRestart_300x10_6Snap_Docker,
    MassCreateDeleteRestart_300x10_6Snap_K8s,
    MassCreateDeleteRestart_300x10_10Snap_Docker,
    MassCreateDeleteRestart_300x10_10Snap_K8s,
    MassCreateDeleteRestart_300x10_Docker,
    MassCreateDeleteRestart_300x10_K8s,
    MassCreateRapidRestart_6k_3Snap_Docker,
    MassCreateRapidRestart_6k_3Snap_K8s,
)
from stress_test.mgmt_node_network_outage import (
    MgmtNodeNetworkOutageTest,
    MgmtNodeRebootTest,
)
from stress_test.objectstore_stress import (
    S3BucketChurnUnderLoadK8s,
    S3CsiNodePluginRestartK8s,
    S3GatewayLargeReplicaCountK8s,
    S3GatewayNodeDrainedK8s,
    S3LoadAcrossOutagesK8s,
    S3ManyBucketsK8s,
    S3MdsMoveDuringProvisioningK8s,
    S3MdsMoveUnderLoadK8s,
)
from stress_test.pnfs_stress import (
    PnfsRwxFailoverK8s,
    PnfsRwxRapidNoGapK8s,
    PnfsRwxResilientK8s,
)
from stress_test.product_limits_stress import (
    ProductLimits_70TiB_Docker,
    ProductLimits_70TiB_K8s,
    ProductLimits_Docker,
    ProductLimits_K8s,
)

ALL_TESTS = [
    TestLvolFioNpcsCustom,
    TestLvolFioNpcs0,
    TestLvolFioNpcs1,
    TestLvolFioNpcs2,
    TestSingleNodeOutage,
    TestSingleNodeFailure,
    TestAddNodesDuringFioRun,
    TestAddNodesDualNodePerHost,
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
    TestAddK8sNodesDualNodePerHost,
    K8sNativeAddNodeTest,
    K8sNativeNodeMigrationTest,
    TestSequentialNodeAdd,
    TestAddNodeSnapshotCloneOnNewNode,
    K8sNativeMajorUpgrade,
    K8sNativeMajorUpgradeDualNode,
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
    # DISABLED: lvol-level backup delete not supported (SFAM-2792)
    # TestBackupRetentionMergeAfterDelete,
    TestBackupBasicPositive,
    TestBackupRestoreDataIntegrity,
    TestBackupPolicy,
    TestBackupNegative,
    TestBackupCryptoLvol,
    TestBackupCustomGeometry,
    # DISABLED: lvol-level backup delete not supported (SFAM-2792)
    # TestBackupDeleteAndRestore,
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
    # DISABLED: lvol-level backup delete not supported (SFAM-2792)
    # TestBackupDeleteInProgress,
    TestBackupPolicyMultipleLvols,
    TestBackupBulkLoadIntegrity,
    TestBackupHighVolumeCombosSequential,
    # Extended backup E2E tests (TC-BCK-150..190)
    TestBackupSecurityLvol,
    TestBackupPolicyVersionsOne,
    TestBackupPolicyMultipleOnSameLvol,
    TestBackupPolicyLvolLevel,
    TestBackupResizedLvol,
    TestBackupListFields,
    TestBackupUpgradeCompatibility,
    TestBackupRestoreEdgeCases,
    # TestBackupSourceSwitch,  # COMMENTED OUT: source-switch no longer required
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
    K8sNativeRapidFailoverNoGapTest,
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
    DualOutageMatrixDocker,
    DualOutageMatrixK8s,
    LblkDualOutageMatrixDocker,
    LblkDualOutageMatrixK8s,
    LblkFunctionalDocker,
    LblkFunctionalK8s,
    LblkIntegrityDocker,
    LblkIntegrityK8s,
    LblkDeviceFaultDocker,
    LblkDeviceFaultK8s,
    LblkJournalRecoveryDocker,
    LblkJournalRecoveryK8s,
    LblkOutageMatrixDocker,
    LblkOutageMatrixK8s,
    LblkStressDocker,
    PnfsRwxFailoverK8s,
    PnfsRwxRapidNoGapK8s,
    PnfsRwxResilientK8s,
    LblkStressK8s,
    LblkMultiOutageStressDocker,
    LblkQuickOutageStressDocker,
    LblkRapidOutageStressDocker,
    LblkRapidOutageStressK8s,
    LblkResilientStressK8s,
    ProductLimits_Docker,
    ProductLimits_K8s,
    ProductLimits_70TiB_Docker,
    ProductLimits_70TiB_K8s,
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
    MassCreateDeletePersistent_300x10_10Snap_Docker,
    MassCreateDeletePersistent_500x1_Docker,
    MassCreateDeletePersistent_3000x1_Docker,
    MassCreateDeletePersistent_1x500_K8s,
    MassCreateDeletePersistent_30x100_K8s,
    MassCreateDeletePersistent_300x10_K8s,
    MassCreateDeletePersistent_300x10_6Snap_K8s,
    MassCreateDeletePersistent_300x10_10Snap_K8s,
    MassCreateDeletePersistent_500x1_K8s,
    MassCreateDeletePersistent_3000x1_K8s,
    MassCreateDeleteRestart_300x10_Docker,
    MassCreateDeleteRestart_300x10_6Snap_Docker,
    MassCreateDeleteRestart_300x10_10Snap_Docker,
    MassCreateDeleteRestart_300x10_K8s,
    MassCreateDeleteRestart_300x10_6Snap_K8s,
    MassCreateDeleteRestart_300x10_10Snap_K8s,
    MassCreateRapidRestart_6k_3Snap_Docker,
    MassCreateRapidRestart_6k_3Snap_K8s,
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
    TestPoolDisableIO,
    TestCrossResourceNegative,
    TestNamespacePlacement,
    TestNamespaceFio,
    TestNamespaceLimits,
    TestObjectLimits,
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
    TestDhchapPodScheduling,
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
    # API parity audit
    TestAPIParityAudit,

    # backup-stress: in its group but never in ALL_TESTS, so these four could
    # only ever run as part of --testname backup-stress. Pre-existing; found by
    # scripts/check_test_registry.py.
    BackupStressLargeScale,
    BackupStressFilesystemSecurityMix,
    BackupStressRetentionMergeCycles,
    BackupStressComprehensive,

    # ── replication, migration and their stress/load lanes ───────────────
    # These must be here as well as in their group functions. ALL_TESTS is
    # what e2e.py searches when a class is named by hand (--testname
    # MigrationSmoke); a class reachable only through a group key cannot be
    # run on its own, which is how `--testname MigrationSmoke` failed with
    # "Test not found" while `--testname migration` worked.
    ReplicationHarness,
    ReplicationTargetsAndPolicies,
    ReplicationStartsAndReports,
    ReplicationPolicyVariants,
    ReplicationDataIsIdentical,
    ReplicationConvergesUnderLoad,
    ReplicationNoMetadataCorruption,
    ConsistencyGroupPlacement,
    ConsistencyGroupGeneration,
    ReplicationFailoverScopes,
    ReplicationFailoverDataLoss,
    ReplicationFailback,
    ReplicationMigration,
    ReplicationSourceNodeOutages,
    ReplicationTargetNodeOutage,
    ReplicationNetworkOutages,
    ReplicationOutageDuringOperations,
    ReplicationConsistencyGroupNodeLoss,
    ReplicationControlPlaneAndDeviceOutages,
    ReplicationNegativeConfiguration,
    ReplicationNegativeOperations,
    CsiAddonsEnableAndReport,
    CsiAddonsOwnership,
    CsiAddonsPromoteDemote,
    CsiAddonsForcedFailoverAndOutage,
    CsiAddonsGroupReplication,
    ReplicationOverlappingIntervals,
    ReplicationManyVolumes,
    ReplicationLargeVolume,
    ReplicationSoak,
    ReplicationSurvivesControlPlaneUpgrade,
    ReplicationSurvivesOperatorUpgrade,
    ReplicationAnnotationToCsiAddonsMigration,
    MigrationSmoke,
    MigrationLiveSingle,
    MigrationLiveBystanders,
    MigrationLiveGroup,
    MigrationHappyPath,
    MigrationRoundTrip,
    MigrationWithSnapshots,
    MigrationHaOverlapMatrix,
    MigrationSnapshotCloneTrees,
    MigrationConcurrent,
    MigrationBatchFlat,
    MigrationBatchNsGaps,
    MigrationBatchNegative,
    MigrationBatchWithTrees,
    MigrationNegativeTargets,
    MigrationNegativeCapacity,
    MigrationUnderReplication,
    MigrationProtectionGuards,
    MigrationTargetOfflinePerPhase,
    MigrationSourceAndTargetFaults,
    MigrationCancel,
    MigrationRetryAfterTargetReboot,
    MigrationHaPartnerRestart,
    MigrationClusterRestartBetween,
    MigrationManyVolumes,
    MigrationLargeVolume,
    MigrationSoak,
    MigrationFreezeByVolumeSize,
    MigrationTimeBySnapshotCount,
    MigrationConcurrencyCurve,
    ReplicationCycleTimeBySize,
    ReplicationDemoteDowntimeBySize,
    # get_monitoring_tests has returned these two since it was written, but
    # neither was ever listed here, so `--testname TestLvolOutageLoadTest`
    # reported "not found" while `--testname monitoring` ran it. Found by
    # tests/unit/test_e2e_registry.py, which exists because the same slip
    # cost run 41604777129.
    TestLvolOutageLoadTest,
    TestParallelLvolSnapshotCloneAPI,
    # ObjectStore / S3Bucket. Listed here as well as in their own group for
    # the reason the comment above gives: a class only reachable by group
    # reports "not found" when somebody names it directly.
    S3BucketLifecycleK8s,
    S3ObjectStoreDeletedWithBucketsK8s,
    S3BucketCapacityExhaustionK8s,
    S3BucketCapacityPatchK8s,
    S3BucketNameBoundaryK8s,
    S3ObjectStoreReplicasZeroK8s,
    S3GatewayScaleOutK8s,
    S3GatewayTLSK8s,
    S3BucketDeliveredWithoutRestartK8s,
    S3GatewayRescheduledK8s,
    S3GatewayNodeSelectorK8s,
    S3TwoObjectStoresSameNameK8s,
    S3GatewayPodKilledK8s,
    S3OpenBaoSealedK8s,
    S3BucketProvisioningLatencyK8s,
    S3OperatorRestartK8s,
    S3RBACFullCycleK8s,
    S3MissingEnvFromSecretK8s,
    S3MdsMoveUnderLoadK8s,
    S3LoadAcrossOutagesK8s,
    S3CsiNodePluginRestartK8s,
    S3GatewayNodeDrainedK8s,
    S3ManyBucketsK8s,
    S3MdsMoveDuringProvisioningK8s,
    S3GatewayLargeReplicaCountK8s,
    S3BucketChurnUnderLoadK8s,
    PnfsSharedWriteVerifyK8s,
    PnfsLayoutProofK8s,
    PnfsCrossClientReadK8s,
    PnfsDeviceProofK8s,
    PnfsCloseToOpenK8s,
    PnfsByteRangeLockK8s,
    PnfsMetadataStormK8s,
    PnfsOnlineResizeK8s,
    PnfsSnapshotRestoreK8s,
    PnfsDeleteLeakGateK8s,
    PnfsFsidUniquenessK8s,
    PnfsScaleThirtyPodsK8s,
    PnfsMixedRwxRwoK8s,
    PnfsTwoNamespacesK8s,
    PnfsClaimCeilingK8s,
    PnfsNoMetadataServerK8s,
    # Being in a group is NOT enough: --testname <ClassName> resolves
    # against ALL_TESTS only (e2e.py:338), so a class listed in a group
    # and omitted here can be run as part of its group and not on its
    # own. Track C/D/E/F were all in that state -- 29 classes that
    # e2e.py would have answered "not found" for, after the bring-up.
    # Track C: the metadata server.
    PnfsMdsDefaultTopologyK8s,
    PnfsMdsActiveDeletedK8s,
    PnfsMdsLockAcrossTakeoverK8s,
    PnfsMdsExportGrownBeforeTakeoverK8s,
    PnfsMdsManyExportsTakeoverK8s,
    PnfsMdsSecondTakeoverK8s,
    PnfsMdsOperatorRestartNoTakeoverK8s,
    PnfsMdsRestartWithClientDownK8s,
    PnfsMdsNodeDrainedK8s,
    PnfsMdsNodeRebootK8s,
    PnfsMdsStandbyScaleK8s,
    PnfsMdsNoStandbyRecoversK8s,
    PnfsMdsStandbyMissingK8s,
    # Track D: security.
    PnfsGuestHoldsNoCredentialsK8s,
    PnfsRootSquashK8s,
    PnfsExportAllowListK8s,
    PnfsPodCidrDirectMountK8s,
    PnfsUnauthorizedNqnRefusedK8s,
    PnfsGuestFencingPreemptsOldEpochK8s,
    # Track E: failure injection.
    PnfsUnalignedDirectIoReportK8s,
    PnfsProvisioningChurnK8s,
    PnfsCsiKilledMidOperationK8s,
    PnfsGuestCrashInPlaceK8s,
    PnfsClientPathLossK8s,
    PnfsMdsPartitionedFromClientK8s,
    PnfsClientNodeRebootK8s,
    # Track F: load, scale and soak.
    PnfsManyClientsLoadK8s,
    PnfsExportCeilingK8s,
    PnfsRestartRebootSoakK8s,
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
        TestDhchapPodScheduling,
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
        K8sNativeRapidFailoverNoGapTest,
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
        DualOutageMatrixDocker,
        DualOutageMatrixK8s,
        LblkDualOutageMatrixDocker,
        LblkDualOutageMatrixK8s,
        LblkStressDocker,
        LblkStressK8s,
        LblkMultiOutageStressDocker,
        LblkQuickOutageStressDocker,
        LblkRapidOutageStressDocker,
        LblkRapidOutageStressK8s,
        LblkResilientStressK8s,
        LblkOutageMatrixDocker,
        LblkOutageMatrixK8s,
        # Track A: the same k8s outage loops on a shared pNFS claim. These
        # have to be HERE and not only in get_pnfs_stress_tests(), because
        # stress.py resolves --testname against get_stress_tests() +
        # get_backup_stress_tests() and nothing else. Registering them only
        # in ALL_TESTS and their own group is what made run 41919726297 die
        # with "Test 'PnfsRwxRapidNoGapK8s' not found" after a 75-minute
        # cluster bring-up.
        PnfsRwxFailoverK8s,
        PnfsRwxRapidNoGapK8s,
        PnfsRwxResilientK8s,
        # The S3 chaos lane, here for the same reason the three above are:
        # stress.py resolves --testname against get_stress_tests() +
        # get_backup_stress_tests() and nothing else.
        S3MdsMoveUnderLoadK8s,
        S3LoadAcrossOutagesK8s,
        S3CsiNodePluginRestartK8s,
        S3GatewayNodeDrainedK8s,
        S3ManyBucketsK8s,
        S3MdsMoveDuringProvisioningK8s,
        S3GatewayLargeReplicaCountK8s,
        S3BucketChurnUnderLoadK8s,
        ProductLimits_Docker,
        ProductLimits_K8s,
        ProductLimits_70TiB_Docker,
        ProductLimits_70TiB_K8s,
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
        MassCreateDeletePersistent_300x10_10Snap_Docker,
        MassCreateDeletePersistent_500x1_Docker,
        MassCreateDeletePersistent_3000x1_Docker,
        MassCreateDeletePersistent_1x500_K8s,
        MassCreateDeletePersistent_30x100_K8s,
        MassCreateDeletePersistent_300x10_K8s,
        MassCreateDeletePersistent_300x10_6Snap_K8s,
        MassCreateDeletePersistent_300x10_10Snap_K8s,
        MassCreateDeletePersistent_500x1_K8s,
        MassCreateDeletePersistent_3000x1_K8s,
        MassCreateDeleteRestart_300x10_Docker,
        MassCreateDeleteRestart_300x10_6Snap_Docker,
        MassCreateDeleteRestart_300x10_10Snap_Docker,
        MassCreateDeleteRestart_300x10_K8s,
        MassCreateDeleteRestart_300x10_6Snap_K8s,
        MassCreateDeleteRestart_300x10_10Snap_K8s,
        MassCreateRapidRestart_6k_3Snap_Docker,
        MassCreateRapidRestart_6k_3Snap_K8s,
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
        MassCreateDeletePersistent_300x10_10Snap_Docker,
        MassCreateDeletePersistent_500x1_Docker,
        MassCreateDeletePersistent_3000x1_Docker,
        MassCreateDeletePersistent_1x500_K8s,
        MassCreateDeletePersistent_30x100_K8s,
        MassCreateDeletePersistent_300x10_K8s,
        MassCreateDeletePersistent_300x10_6Snap_K8s,
        MassCreateDeletePersistent_300x10_10Snap_K8s,
        MassCreateDeletePersistent_500x1_K8s,
        MassCreateDeletePersistent_3000x1_K8s,
        MassCreateDeleteRestart_300x10_Docker,
        MassCreateDeleteRestart_300x10_6Snap_Docker,
        MassCreateDeleteRestart_300x10_10Snap_Docker,
        MassCreateDeleteRestart_300x10_K8s,
        MassCreateDeleteRestart_300x10_6Snap_K8s,
        MassCreateDeleteRestart_300x10_10Snap_K8s,
        MassCreateRapidRestart_6k_3Snap_Docker,
        MassCreateRapidRestart_6k_3Snap_K8s,
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
        # DISABLED: lvol-level backup delete not supported (SFAM-2792)
        # TestBackupRetentionMergeAfterDelete,
        # E2E backup tests
        TestBackupBasicPositive,
        TestBackupRestoreDataIntegrity,
        TestBackupPolicy,
        TestBackupNegative,
        TestBackupCryptoLvol,
        # TestBackupCustomGeometry, # Will re-enable when we have a way to reliably test it in CI (currently requires manual setup of custom geometry pool)
        # DISABLED: lvol-level backup delete not supported (SFAM-2792)
        # TestBackupDeleteAndRestore,
        # Extra coverage tests (TC-BCK-100..148)
        TestBackupMultipleRestores,
        TestBackupDeltaChainPointInTime,
        TestBackupEmptyLvol,
        TestBackupPoolRecreateRestore,
        TestBackupPolicyAgeOnly,
        TestBackupSnapshotClone,
        TestBackupFilesystemXFS,
        TestBackupLargeLvol,
        # DISABLED: lvol-level backup delete not supported (SFAM-2792)
        # TestBackupDeleteInProgress,
        TestBackupPolicyMultipleLvols,
        TestBackupBulkLoadIntegrity,
        # Extended backup tests (TC-BCK-150..190)
        TestBackupSecurityLvol,
        TestBackupPolicyVersionsOne,
        TestBackupPolicyMultipleOnSameLvol,
        TestBackupPolicyLvolLevel,
        TestBackupResizedLvol,
        TestBackupListFields,
        TestBackupRestoreEdgeCases,
        # TestBackupSourceSwitch,  # COMMENTED OUT: source-switch no longer required
        # Outage tests — run last (involves node shutdown/restart)
        TestBackupUpgradeCompatibility,
        TestBackupInterruptedBackup,
        TestBackupInterruptedRestore,
        TestBackupConcurrentIO,
    ]


def get_backup_topology_tests():
    """Backup tests that modify cluster topology (add-node / migration).

    These require extra infrastructure (NEW_NODE_IPS, migrate_to_worker)
    and must run in dedicated topology pipelines, not the regular backup suite.
    """
    return [
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
        BackupStressComprehensive,
        TestBackupHighVolumeCombosSequential,
    ]


def get_upgrade_tests():
    tests = [
        TestMajorUpgrade,
        TestMajorUpgradeSingleNode,
        TestMajorUpgradeDualNode,
        K8sNativeMajorUpgrade,
        # K8sNativeMajorUpgradeDualNode,  # disabled: focus on single-node upgrade first
    ]
    return tests


def get_migration_load_tests():
    """MIG-L: migration timings, as curves rather than verdicts.

    A LOAD lane, which is a different kind of thing from e2e or stress --
    the output is a number per step, appended to CSV, resumable, and
    plotted. "Did it pass" is close to meaningless; the deliverable is
    where the curve bends.

        MIG-L-001  freeze duration vs volume size      <- the headline
        MIG-L-002  duration vs snapshot count
        MIG-L-003  wall-clock vs concurrency

    MIG-L-001 is the one to run first. lvol_migrate is the only phase that
    stops the volume serving, the design calls it a "short freeze", and
    nobody has published what it is at the sizes customers use.

        MIG_LOAD_SIZES (2,10,50,100)   MIG_LOAD_SNAPS (0,10,50,100)
        MIG_LOAD_CONCURRENCY (1,2,4,8) MIG_LOAD_BUDGET_SEC (5400)
        LOAD_CONTINUE (1)  -- resume from the CSV instead of redoing steps
    """
    return [
        MigrationFreezeByVolumeSize,     # MIG-L-001
        MigrationTimeBySnapshotCount,    # MIG-L-002
        MigrationConcurrencyCurve,       # MIG-L-003
    ]


def get_replication_load_tests():
    """AR-L: the two replication numbers nobody has.

    Neither has a threshold to assert against, which is exactly why they
    are load and not stress: the answer is a curve that tells you how big
    a volume you can honestly offer the feature on.

        AR-L-001  cycle time vs volume size -- the shortest honest interval,
                  which does NOT improve when little changes because
                  allow_partial is disabled and every transfer is full
        AR-L-002  planned-relocation downtime vs size -- demote fences
                  first and then ships a whole volume

        AR_LOAD_SIZES (2,10,50,100)   AR_LOAD_BUDGET_SEC (7200)
    """
    return [
        ReplicationCycleTimeBySize,      # AR-L-001
        ReplicationDemoteDowntimeBySize, # AR-L-002
    ]


def get_load_tests():
    """The load runner's registry -- e2e/load.py resolves --testname here.

    A load test sweeps a parameter and records a number per step; see
    load_tests/_load_base.py. These are ALSO in e2e.py's group map, under
    migration-load and replication-load, because no workflow calls load.py --
    so the e2e route is the only way to run them from a pipeline. Same
    classes, two front doors: load.py for a single sweep by hand, e2e.py for
    CI.
    """
    tests = [
        TestLvolOutageLoadTest,
        MigrationFreezeByVolumeSize,
        MigrationTimeBySnapshotCount,
        MigrationConcurrencyCurve,
        ReplicationCycleTimeBySize,
        ReplicationDemoteDowntimeBySize,
    ]
    return tests


def get_replication_tests():
    """Async replication: two simplyblock clusters under one control plane.

    Ordered so the harness runs first. If AR-S cannot stand two clusters up
    nothing below it means anything, and the run should say that by name
    rather than reporting whichever case happened to run next.

    Needs enough storage nodes for two clusters -- see
    ReplicationTestBase.MIN_NODES_PER_CLUSTER -- passed via
    STORAGE_PRIVATE_IPS and/or NEW_NODE_IPS.

    Test IDs map to documentation/async_replication_QA_test_plan.xlsx.
    """
    return [
        # AR-S: if two clusters cannot be stood up, stop here.
        ReplicationHarness,                       # AR-S-001, AR-S-002
        # AR-F: configuration surface. Cheap, and fails fast on a broken build.
        ReplicationTargetsAndPolicies,            # AR-F-001/003/004/005, AR-N-007
        ReplicationStartsAndReports,              # AR-F-006/007/009/010/011
        ReplicationPolicyVariants,                # AR-F-002/008/012/013/014/015
        # AR-I: the core risk. Before anything destructive runs.
        ReplicationDataIsIdentical,               # AR-I-001
        ReplicationConvergesUnderLoad,            # AR-I-002
        ReplicationNoMetadataCorruption,          # AR-I-003
        # AR-C: a working group has to exist before AR-O-009 breaks one.
        ConsistencyGroupPlacement,                # AR-C-001/005/006
        ConsistencyGroupGeneration,               # AR-C-002/003/004/007/008
        # AR-R: moves volumes between clusters; leaves the lab rearranged.
        ReplicationFailoverScopes,                # AR-R-001/002/003/005
        ReplicationFailoverDataLoss,              # AR-R-004
        ReplicationFailback,                      # AR-R-006/007/008/009
        ReplicationMigration,                     # AR-R-010/011/012
        # AR-O: destructive and slowest.
        ReplicationSourceNodeOutages,             # AR-O-001/002/003
        ReplicationTargetNodeOutage,              # AR-O-004
        ReplicationNetworkOutages,                # AR-O-005/006
        ReplicationOutageDuringOperations,        # AR-O-007/008
        ReplicationConsistencyGroupNodeLoss,      # AR-O-009
        ReplicationControlPlaneAndDeviceOutages,  # AR-O-010/011/012
        # AR-N: cheap, but AR-N-006 deletes a volume under replication.
        ReplicationNegativeConfiguration,         # AR-N-001/002/003
        ReplicationNegativeOperations,            # AR-N-004/005/006
        # AR-K: the Kubernetes DR surface. Last because it is the newest and
        # the only lane that no-ops on docker -- a failure here should not
        # cost the engine coverage above it. Every case skips by name when
        # the csi-addons CRDs are absent, so this is safe to leave enabled
        # before operator PR #548 lands.
        CsiAddonsEnableAndReport,                 # AR-K-001/002/003/010/011
        CsiAddonsOwnership,                       # AR-K-004
        CsiAddonsPromoteDemote,                   # AR-K-005/006/007/009
        CsiAddonsForcedFailoverAndOutage,         # AR-K-008/012/013/014
        CsiAddonsGroupReplication,                # AR-K-015 (Phase 4, records a skip)
    ]


def get_migration_tests():
    """lvol migration: one volume between two nodes of ONE cluster.

    The two-phase `volume migrate` / `volume migrate-continue` handshake,
    which had NO e2e coverage before this lane -- our other "migration"
    classes are device-failure rebalance, node migration, or cross-cluster
    replication, none of which touch these verbs.

    Ported from origin/lvol-migration-test-scripts, which were run by hand
    and found most of this feature's bugs. Ordered cheapest-and-safest
    first: the happy path has to work before a fault case means anything,
    and the fault lane leaves nodes down and restarted, so it runs after
    the topology work that needs a quiet cluster.

        MIG-H  8 cases   happy path + the volume is still usable
        MIG-T  11 cases  HA overlap matrix, trees, concurrency
        MIG-B  8 cases   shared-namespace groups
        MIG-N  10 cases  must-be-refused, migrate-while-replicating, and
                         the three protection guards (delete/resize/
                         snapshot-delete must be refused mid-migration)
        MIG-F  16 cases  faults in a named phase -- longest, most
                         disruptive. MIG-F-016 cancels while still
                         preparing, which is the one cancel dev says
                         is honoured.
    """
    return [
        # MIG-S first, always. Ten minutes, six named links, and a failure
        # tells you WHICH link -- the CLI surface, the topology read, the
        # pre-create, the record parsing, placement, or the data. The first
        # full run of this lane spent nine hours proving nothing because a
        # CLI flag did not exist; this is the guard against repeating that.
        MigrationSmoke,                  # MIG-S-001
        # MIG-L: live migration -- the only kind anyone performs, and
        # the only cases that measure the cutover gap. Before MIG-H
        # because an idle-volume pass says nothing about a loaded one,
        # and after MIG-S because they cost more than the smoke test.
        MigrationLiveSingle,             # MIG-L-001
        MigrationLiveBystanders,         # MIG-L-002
        MigrationLiveGroup,              # MIG-L-003
        # MIG-H: if a plain migration does not work, nothing below matters.
        MigrationHappyPath,              # MIG-H-001..004
        MigrationRoundTrip,              # MIG-H-005/006
        MigrationWithSnapshots,          # MIG-H-007/008
        # MIG-T: needs a quiet cluster, so before anything destructive.
        MigrationHaOverlapMatrix,        # MIG-T-001..005
        MigrationSnapshotCloneTrees,     # MIG-T-006..009
        MigrationConcurrent,             # MIG-T-010/011
        # MIG-B: the batch path.
        MigrationBatchFlat,              # MIG-B-001..003
        MigrationBatchNsGaps,            # MIG-B-004
        MigrationBatchNegative,          # MIG-B-005/006
        MigrationBatchWithTrees,         # MIG-B-007/008
        # MIG-N: cheap, but MIG-N-003 takes a node down.
        MigrationNegativeTargets,        # MIG-N-001..004
        MigrationNegativeCapacity,       # MIG-N-005
        MigrationUnderReplication,       # MIG-N-006/007
        MigrationProtectionGuards,       # MIG-N-008..010
        # MIG-F: last. Kills nodes, reboots them, restarts the cluster.
        MigrationTargetOfflinePerPhase,  # MIG-F-001..003
        MigrationSourceAndTargetFaults,  # MIG-F-004..009
        MigrationCancel,                 # MIG-F-010..012, MIG-F-016
        MigrationRetryAfterTargetReboot, # MIG-F-013
        MigrationHaPartnerRestart,       # MIG-F-014
        MigrationClusterRestartBetween,  # MIG-F-015
    ]


def get_migration_stress_tests():
    """MIG-P: migration at scale and over time. A SEPARATE lane.

    Same split, same reason as the replication lanes: a soak measured in
    hours must never stand between a correctness run and its answer.

        MIG_LARGE_SIZE (100G)   MIG_LARGE_BUDGET_SEC (7200)
        MIG_MANY_COUNT (20)     MIG_MANY_SETTLE_SEC (3600)
        MIG_SOAK_HOURS (6)      MIG_VOL_SIZE (2G)  MIG_POST_OP_SEC (300)
    """
    return [
        MigrationManyVolumes,   # MIG-P-003/004  minutes to an hour
        MigrationLargeVolume,   # MIG-P-001/002  needs capacity; times the freeze
        MigrationSoak,          # MIG-P-005/006  MIG_SOAK_HOURS
    ]


def get_replication_stress_tests():
    """AR-P and AR-U: scale, endurance and upgrade. A SEPARATE lane.

    Deliberately not part of get_replication_tests(). The correctness suite
    answers "is this right" in a couple of hours; this one answers "does it
    hold up" and a single soak can run for a weekend. Putting a 20-hour case
    in front of a 2-hour answer is how a suite stops being run.

    Ordered cheapest-first, because the early cases need nothing special and
    the later ones need lab capacity and a time budget:

        AR-U-007..009  migration      minutes   -- and it has a customer behind it
        AR-P-001..003  overlap        ~20 min
        AR-U-001..006  upgrade        ~30 min
        AR-P-004..006  many volumes   ~45 min   needs headroom for N volumes
        AR-P-007..009  large volume   hours     needs AR_LARGE_SIZE of capacity
        AR-P-010..012  soak           AR_SOAK_HOURS (default 6)

    Scale knobs, all environment-overridable because the right value is a
    property of the lab and not of the test:

        AR_OVERLAP_SIZE (20G)   AR_MANY_COUNT (25)    AR_MANY_SIZE (1G)
        AR_LARGE_SIZE (100G)    AR_SOAK_HOURS (6)     AR_SOAK_RETENTION (3)
    """
    return [
        # Cheap, no blockers, and the one with a customer waiting on the answer.
        ReplicationAnnotationToCsiAddonsMigration,  # AR-U-007/008/009
        ReplicationOverlappingIntervals,            # AR-P-001/002/003
        # Upgrade: survives a control-plane roll, and the helm crds/ trap.
        ReplicationSurvivesControlPlaneUpgrade,     # AR-U-001/002/003
        ReplicationSurvivesOperatorUpgrade,         # AR-U-004/005/006
        # Scale by count, then by size.
        ReplicationManyVolumes,                     # AR-P-004/005/006
        ReplicationLargeVolume,                     # AR-P-007/008/009
        # Last: the only case measured in hours.
        ReplicationSoak,                            # AR-P-010/011/012
    ]


def get_lblk_tests():
    """Functional and integration coverage for lblk (non-NVMe) clusters.

    Every one of these asserts device_mode == "lblk" first and fails fast if
    not, so pointing this at an NVMe cluster reports a precondition error
    rather than quietly passing against the wrong storage path.

    The open-ended soak is LblkStress* in get_stress_tests().
    """
    return [
        LblkFunctionalDocker,
        LblkFunctionalK8s,
        LblkIntegrityDocker,
        LblkIntegrityK8s,
        LblkDeviceFaultDocker,
        LblkDeviceFaultK8s,
        LblkJournalRecoveryDocker,
        LblkJournalRecoveryK8s,
    ]


def get_pnfs_tests():
    """pNFS RWX, Track B -- the shared filesystem on a healthy cluster.

    Ordered as a gate, not alphabetically. B-01 proves a ReadWriteMany
    claim can be provisioned and mounted everywhere at once; B-03 proves
    those mounts are doing pNFS rather than plain NFS. If either fails,
    the two below it are measuring something that is not the feature,
    so they run after and their failures read as consequences.

    Every class ends in K8s: pNFS has no Docker variant, so the platform
    filter drops the whole group on a Docker cluster rather than running
    it against something that cannot serve it.
    """
    return [
        PnfsSharedWriteVerifyK8s,    # B-01  the baseline
        PnfsLayoutProofK8s,          # B-03  gates everything
        PnfsCrossClientReadK8s,      # B-02  the data crossed the wire
        PnfsDeviceProofK8s,          # B-04  and reached the devices
        # Semantics, once the data path is proven.
        PnfsCloseToOpenK8s,          # B-06  no stale read after a close
        PnfsByteRangeLockK8s,        # B-05  fcntl ranges serialise
        PnfsMetadataStormK8s,        # B-07  the namespace under contention
        # Lifecycle.
        PnfsOnlineResizeK8s,         # B-09  grow, every client sees it
        PnfsSnapshotRestoreK8s,      # B-10  snapshot, refusal, restore
        PnfsDeleteLeakGateK8s,       # B-11  and it all goes away again
        # Scale and multi-tenancy.
        PnfsFsidUniquenessK8s,       # B-08  20 claims, 20 fsids
        PnfsScaleThirtyPodsK8s,      # B-12  30 pods on one claim
        PnfsMixedRwxRwoK8s,          # B-13  RWX beside RWO
        PnfsTwoNamespacesK8s,        # B-14  same name, two tenants
        PnfsClaimCeilingK8s,         # B-15  31 claims on one MDS, all serving
        # Last, and alone: it takes the metadata server away from the
        # whole cluster for the length of the case.
        PnfsNoMetadataServerK8s,     # B-16  negative
    ]


def get_pnfs_mds_tests():
    """Track C -- the warm-standby metadata server, test-plan-pnfs-mds-standby.

    Its own group, not folded into ``pnfs``. Every case here deliberately
    destroys the active metadata server, and several change
    ``spec.pnfs.mds.standbys`` on the driver. Running them inside the Track B
    group would mean every ``--testname pnfs`` run paid for a dozen
    takeovers, and a Track B failure after one of these would be ambiguous
    between the feature and the disturbance.

    Ordered as a gate. E-01 proves the topology is an active plus a standby
    on separate nodes; if it is not, every F- case below it is measuring
    something else, so it runs first and the rest read as consequences.
    The two that change the standby count run last, after the cases that
    need the default shape, and each restores it from teardown.
    """
    return [
        PnfsMdsDefaultTopologyK8s,              # E-01  the gate
        PnfsMdsActiveDeletedK8s,                # F-01  the headline takeover
        PnfsMdsLockAcrossTakeoverK8s,           # F-12  locks survive it
        PnfsMdsExportGrownBeforeTakeoverK8s,    # F-19  resize races it
        PnfsMdsManyExportsTakeoverK8s,          # F-10  it scales
        PnfsMdsSecondTakeoverK8s,               # F-09  twice, inside grace
        PnfsMdsOperatorRestartNoTakeoverK8s,    # F-06  and it is not spurious
        # F-16 last of the takeover cases: it is the only one that
        # takes a client node down, so it is the only one whose
        # failure costs the group a worker.
        PnfsMdsRestartWithClientDownK8s,  # F-16  grace with a client gone
        # The node under the metadata server, not its pod. These are
        # the only cases that produce the state the fencing design
        # exists for -- a guest the cluster thinks is gone, on a host
        # it cannot reach, which then comes back. They take minutes,
        # not seconds, and they cost the group a worker if the node
        # does not return, so they run after every pod-level case.
        PnfsMdsNodeDrainedK8s,                  # planned: the drain path
        PnfsMdsNodeRebootK8s,                   # unplanned: and it rejoins
        # Last: these change spec.pnfs.mds.standbys. Each restores it, but
        # a case that failed between the patch and the undo leaves the
        # cluster in a shape the cases above would silently pass in.
        PnfsMdsStandbyScaleK8s,                 # E-04  1 -> 0 -> 1
        PnfsMdsNoStandbyRecoversK8s,            # E-03  standbys=0
        PnfsMdsStandbyMissingK8s,               # E-02  nowhere to put one
    ]


def get_pnfs_failure_tests():
    """Track E -- failure injection and churn, the `F-`/`L-` rows of
    test-plan-pnfs-rwx that this lab can reach.

    Pod-level and client-level faults only. The rows that need a client
    node actually down, a network partition, a pre-6.11 kernel or RHCOS
    5.14 clients are listed in the module docstring with the reason, not
    written as cases that would fail for the lab rather than the product.

    Ordered cheapest-blast-radius first: the churn and the reporting
    cases touch only their own claims, F-05 restarts the CSI plugins, and
    F-15 crashes the metadata server guest, so a failure part-way leaves
    progressively less behind in that order reversed.
    """
    return [
        PnfsUnalignedDirectIoReportK8s,   # F-22  client-side only
        PnfsProvisioningChurnK8s,         # L-05  its own claims
        PnfsCsiKilledMidOperationK8s,     # F-05  restarts the CSI plugins
        PnfsGuestCrashInPlaceK8s,         # F-15  crashes the guest
        # The client-node faults, last and in this order. Each one
        # disturbs a shared lab host and restores it by a mechanism that
        # does not depend on this process surviving (see the module
        # docstring), but a failure here still costs the lab more than a
        # pod kill does -- so everything that can be proved without
        # touching a host is proved first.
        PnfsClientPathLossK8s,            # F-18  a controller reset, self-healing
        PnfsMdsPartitionedFromClientK8s,  # F-10  port 2049 only, host-armed undo
        PnfsClientNodeRebootK8s,          # F-04  a real reboot, ~20min
    ]


def get_pnfs_load_tests():
    """Track F -- the `L-` load, scale and soak rows.

    Separate from `pnfs-failure` because of what a failure costs and how
    long it takes: L-01 runs 24 clients for five minutes, L-07 provisions
    two dozen exports, and L-08 is the vehicle for the multi-day L-03
    soak. None of it belongs in a group somebody runs to check a fix.

    Ordered by what it leaves behind. L-01 and L-07 touch only their own
    claims; L-08 destroys the metadata server and reboots client nodes
    repeatedly, so it runs last.

    `PNFS_L01_CLIENTS`, `PNFS_L07_EXPORTS` and `PNFS_L08_CYCLES` are the
    knobs. Raising the last one is how this group becomes the soak lane
    rather than a CI case; the assertions do not change with the
    duration.
    """
    return [
        PnfsManyClientsLoadK8s,           # L-01  many clients, one claim
        PnfsExportCeilingK8s,             # L-07  the export boundary
        PnfsRestartRebootSoakK8s,         # L-08 (and L-03) the soak
    ]


def get_pnfs_security_tests():
    """Track D -- pNFS security, the ``SEC-`` rows of test-plan-pnfs-rwx.

    The plan's §7 says "No security suite exists for pNFS"; this is it.
    Separate from ``pnfs`` because these assert mechanism rather than
    behaviour -- registrant lists, uids, refusals -- and because SEC-01
    drives a takeover, which Track B assumes does not happen under it.

    Read-only cases first, so a cluster that fails them fails before
    anything has been disturbed.
    """
    return [
        PnfsGuestHoldsNoCredentialsK8s,     # SEC-08  read-only
        PnfsRootSquashK8s,                  # SEC-03  tenancy
        PnfsExportAllowListK8s,             # SEC-05  gated on design §15
        PnfsPodCidrDirectMountK8s,          # SEC-07  the known exposure
        PnfsUnauthorizedNqnRefusedK8s,      # SEC-04  the layer underneath
        PnfsGuestFencingPreemptsOldEpochK8s,  # SEC-01  drives a takeover
    ]


def get_pnfs_stress_tests():
    """pNFS RWX, Track A -- the existing outage loops on a shared claim.

    Separate from get_pnfs_tests() because these are soaks: they run until
    something breaks, where Track B's cases finish. Ordered cheapest first,
    so a bring-up problem surfaces in the single-outage loop rather than
    three hours into the resilient one.

    K8s only, for the same reason Track B is: pNFS has no Docker variant.
    """
    return [
        PnfsRwxFailoverK8s,          # A-01  baseline outage loop
        PnfsRwxRapidNoGapK8s,        # A-02  outages during a migration
        PnfsRwxResilientK8s,         # A-03  long soak, snapshot/clone churn
    ]


def get_objectstore_tests():
    """ObjectStore / S3Bucket -- what dev's own suite leaves uncovered.

    Ordered as a gate. TC-S3-001 proves a bucket can be provisioned through
    the CRD and that its volume is released again; until it passes, every
    case below is measuring something other than the feature. The TLS case
    is second because a gateway that is not serving a real certificate makes
    every data assertion after it meaningless.

    Each class ends in K8s: the CRDs exist only on the k8s path, so the
    platform filter drops the group on a Docker cluster rather than failing
    it obscurely.
    """
    return [
        # The gate.
        S3BucketLifecycleK8s,              # TC-S3-001  CRD lifecycle + release
        S3GatewayTLSK8s,                   # TC-S3-007/8/9  obj E-01..E-03
        # Delivery: the invariant the whole design rests on.
        S3BucketDeliveredWithoutRestartK8s,  # TC-S3-011/012  bucket E-01/E-03
        S3GatewayScaleOutK8s,              # TC-S3-005  obj U-18 live, bucket E-02
        S3GatewayRescheduledK8s,           # TC-S3-013  bucket E-04
        S3GatewayNodeSelectorK8s,          # TC-S3-018  heterogeneous pool
        # Validation and boundaries.
        S3BucketCapacityPatchK8s,          # TC-S3-004  grow/shrink/immutable
        S3BucketNameBoundaryK8s,           # TC-S3-023  U-17/U-18 live
        S3ObjectStoreReplicasZeroK8s,      # TC-S3-019  the excluded boundary
        S3BucketCapacityExhaustionK8s,     # TC-S3-003  full bucket, live neighbour
        # Failure paths that need a real kubelet or a real GC.
        S3MissingEnvFromSecretK8s,         # TC-S3-010  obj I-07
        S3OperatorRestartK8s,              # TC-S3-015/016  bucket E-08 + mid-delete
        S3RBACFullCycleK8s,                # TC-S3-017  bucket I-11
        S3TwoObjectStoresSameNameK8s,      # TC-S3-020  U-17/U-19 live
        S3GatewayPodKilledK8s,             # TC-S3-021/022  obj M-01 + OQ 2
        S3OpenBaoSealedK8s,                # TC-S3-006  the deploy-script note
        # The measurement, last: it is the slowest and it fails nothing.
        S3BucketProvisioningLatencyK8s,    # TC-S3-014  bucket E-05/6/7, OQ 4
        # Deliberately last of all: it deletes an ObjectStore out from under
        # live buckets, which leaves the namespace in whichever state the
        # product chooses.
        S3ObjectStoreDeletedWithBucketsK8s,  # TC-S3-002  obj I-04 live half
    ]


def get_objectstore_stress_tests():
    """S3 under a cluster that breaks -- the axis the feature has none of.

    Ordered cheapest first so a bring-up problem surfaces in the shortest
    case rather than three hours into the scale one. TC-S3-101 is first among
    the long ones because the MDS singleton is in every bucket's data path
    and is the likeliest finding.
    """
    return [
        S3MdsMoveUnderLoadK8s,             # TC-S3-101  bucket-prov §7, widened
        S3CsiNodePluginRestartK8s,         # TC-S3-103  Open Question 6
        S3GatewayNodeDrainedK8s,           # TC-S3-104  required anti-affinity
        S3MdsMoveDuringProvisioningK8s,    # TC-S3-106  bucket-prov §7, exact
        S3GatewayLargeReplicaCountK8s,     # TC-S3-107  replica-count axis
        S3BucketChurnUnderLoadK8s,         # TC-S3-108  leak detection
        S3LoadAcrossOutagesK8s,            # TC-S3-102  the full outage matrix
        S3ManyBucketsK8s,                  # TC-S3-105  Open Question 4 at scale
    ]


def get_parity_tests():
    """API parity audit — CLI vs v1 vs v2 three-way comparison."""
    return [TestAPIParityAudit]


def get_e2e_all_tests():
    """Comprehensive Docker-safe functional E2E suite (~40 tests).

    Includes all Phase 1-3 functional tests, core outage/failure tests,
    and resize tests.  Excludes: K8s-only, RDMA, topology-modifying,
    stress/load, backup (use 'backup' keyword), security (use 'security'
    keyword), upgrade, QoS (SPDK crash), tests requiring special cluster
    flags, and tests with uncertain API support.

    Usage: TEST_CLASS=e2e-all
    """
    return [
        # ── Phase 1: API / CRUD / Negative tests ────────────────────────
        TestLvolBasicCRUD,
        TestLvolCapacityIOStats,
        TestLvolNegativeCases,
        TestSnapshotNegativeCases,
        TestSnapshotLifecycle,
        TestPoolEnableDisable,
        TestPoolNegativeCases,
        TestPoolDisableIO,
        TestPoolStats,
        TestPoolDhchap,
        TestDhchapPodScheduling,
        TestPoolCapacityLimits,
        TestPoolHostManagement,
        TestCrossResourceNegative,
        TestVolumeCloneLvol,
        # ── Phase 2: Cluster / Node / Namespace tests ───────────────────
        TestLvolInflate,
        TestLvolMigrationLoad,
        TestLvolConnectLifecycle,
        TestLvolPlacement,
        TestStorageNodeStats,
        TestStorageNodePorts,
        TestStorageNodeListing,
        TestClusterStats,
        TestClusterTasks,
        TestClusterSecret,
        TestClusterOperations,
        TestClusterGracefulShutdown,
        TestMultiClientConnect,
        TestCapacityThresholds,
        TestHealthChecks,
        TestMigrationLifecycle,
        TestConcurrentOperations,
        TestNamespacePlacement,
        TestNamespaceFio,
        TestNamespaceLimits,
        TestNamespaceNegative,
        TestNamespaceE2E,
        # ── Phase 3: Device / Outage / Failure tests ────────────────────
        TestDeviceRestart,
        TestDeviceCapacityIO,
        TestSingleNodeOutage,
        TestHASingleNodeOutage,
        TestSingleNodeFailure,
        TestHASingleNodeFailure,
        TestSingleNodeReboot,
        TestHASingleNodeReboot,
        TestSingleNodeResizeLvolCone,
        TestNodeShutdownRestart,
    ]
