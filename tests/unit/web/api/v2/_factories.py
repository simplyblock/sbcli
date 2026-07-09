# coding=utf-8
"""Model factories for API v2 endpoint unit tests.

Each factory builds a *real* core model (not a MagicMock) carrying exactly the
fields the v2 DTOs read in ``from_model``, so responses can be asserted against
the entity without a live database.
"""

from pydantic import SecretStr

from simplyblock_core.models.backup import Backup, BackupPolicy
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


CLUSTER_ID = '11111111-1111-1111-1111-111111111111'
POOL_ID = '22222222-2222-2222-2222-222222222222'
VOLUME_ID = '33333333-3333-3333-3333-333333333333'
STORAGE_NODE_ID = '44444444-4444-4444-4444-444444444444'
DEVICE_ID = '55555555-5555-5555-5555-555555555555'
SNAPSHOT_ID = '66666666-6666-6666-6666-666666666666'
TASK_ID = '77777777-7777-7777-7777-777777777777'
MANAGEMENT_NODE_ID = '88888888-8888-8888-8888-888888888888'
BACKUP_ID = '99999999-9999-9999-9999-999999999999'
POLICY_ID = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
MIGRATION_ID = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'


def _apply(model, attrs):
    for name, value in attrs.items():
        setattr(model, name, value)
    return model


def make_cluster(**attrs) -> Cluster:
    cluster = Cluster()
    cluster.uuid = CLUSTER_ID
    cluster.cluster_name = 'cluster-1'
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.nqn = 'nqn.2023-02.io.simplyblock:cluster-1'
    cluster.blk_size = 512
    cluster.ha_type = 'ha'
    cluster.secret = SecretStr('cluster-secret')
    return _apply(cluster, attrs)


def make_pool(**attrs) -> Pool:
    pool = Pool()
    pool.uuid = POOL_ID
    pool.cluster_id = CLUSTER_ID
    pool.pool_name = 'pool-1'
    pool.status = Pool.STATUS_ACTIVE
    return _apply(pool, attrs)


def make_volume(**attrs) -> LVol:
    volume = LVol()
    volume.uuid = VOLUME_ID
    volume.lvol_name = 'volume-1'
    volume.status = LVol.STATUS_ONLINE
    volume.pool_uuid = POOL_ID
    volume.pool_name = 'pool-1'
    volume.node_id = STORAGE_NODE_ID
    volume.nodes = [STORAGE_NODE_ID]
    volume.nqn = 'nqn.2023-02.io.simplyblock:volume-1'
    volume.hostname = 'snode-1'
    volume.ha_type = 'single'
    volume.size = 10 * 1024 ** 3
    return _apply(volume, attrs)


def make_snapshot(**attrs) -> SnapShot:
    snapshot = SnapShot()
    snapshot.uuid = SNAPSHOT_ID
    snapshot.snap_name = 'snapshot-1'
    snapshot.status = SnapShot.STATUS_ONLINE
    snapshot.cluster_id = CLUSTER_ID
    snapshot.pool_uuid = POOL_ID
    snapshot.size = 10 * 1024 ** 3
    snapshot.used_size = 1024 ** 3
    snapshot.created_at = 1700000000
    return _apply(snapshot, attrs)


def make_storage_node(**attrs) -> StorageNode:
    node = StorageNode()
    node.uuid = STORAGE_NODE_ID
    node.cluster_id = CLUSTER_ID
    node.status = StorageNode.STATUS_ONLINE
    node.hostname = 'snode-1'
    node.host_nqn = 'nqn.2023-02.io.simplyblock:snode-1'
    node.mgmt_ip = '10.0.0.10'
    node.cpu = 8
    node.spdk_cpu_mask = '0xF'
    node.memory = 16 * 1024 ** 3
    node.rpc_port = 8080
    node.nvmf_port = 4420
    return _apply(node, attrs)


def make_device(**attrs) -> NVMeDevice:
    device = NVMeDevice()
    device.uuid = DEVICE_ID
    device.cluster_id = CLUSTER_ID
    device.node_id = STORAGE_NODE_ID
    device.status = NVMeDevice.STATUS_ONLINE
    device.model_id = 'test-model'
    device.serial_number = 'SN0001'
    device.nvme_controller = 'nvme0'
    device.pcie_address = '0000:00:1e.0'
    device.size = 1024 ** 4
    device.cluster_device_order = 0
    device.nvmf_ip = '10.0.0.10'
    device.nvmf_nqn = 'nqn.2023-02.io.simplyblock:device-1'
    device.nvmf_port = 4420
    return _apply(device, attrs)


def make_task(**attrs) -> JobSchedule:
    task = JobSchedule()
    task.uuid = TASK_ID
    task.cluster_id = CLUSTER_ID
    task.function_name = JobSchedule.FN_NODE_RESTART
    task.status = JobSchedule.STATUS_NEW
    return _apply(task, attrs)


def make_management_node(**attrs) -> MgmtNode:
    node = MgmtNode()
    node.uuid = MANAGEMENT_NODE_ID
    node.cluster_id = CLUSTER_ID
    node.status = MgmtNode.STATUS_ONLINE
    node.hostname = 'mgmt-1'
    node.mgmt_ip = '10.0.0.5'
    return _apply(node, attrs)


def make_backup(**attrs) -> Backup:
    backup = Backup()
    backup.uuid = BACKUP_ID
    backup.cluster_id = CLUSTER_ID
    backup.lvol_id = VOLUME_ID
    backup.lvol_name = 'volume-1'
    backup.snapshot_id = SNAPSHOT_ID
    backup.snapshot_name = 'snapshot-1'
    backup.node_id = STORAGE_NODE_ID
    backup.status = Backup.STATUS_COMPLETED
    backup.size = 1024 ** 3
    backup.created_at = 1700000000
    backup.completed_at = 1700000100
    return _apply(backup, attrs)


def make_backup_policy(**attrs) -> BackupPolicy:
    policy = BackupPolicy()
    policy.uuid = POLICY_ID
    policy.cluster_id = CLUSTER_ID
    policy.policy_name = 'policy-1'
    policy.max_versions = 7
    policy.max_age_display = '7d'
    policy.backup_schedule = '15m,4'
    policy.status = BackupPolicy.STATUS_ACTIVE
    return _apply(policy, attrs)


def make_migration(**attrs) -> LVolMigration:
    migration = LVolMigration()
    migration.uuid = MIGRATION_ID
    migration.cluster_id = CLUSTER_ID
    migration.lvol_id = VOLUME_ID
    migration.source_node_id = STORAGE_NODE_ID
    migration.target_node_id = '44444444-4444-4444-4444-444444444445'
    migration.phase = LVolMigration.PHASE_SNAP_COPY
    migration.status = LVolMigration.STATUS_RUNNING
    return _apply(migration, attrs)
