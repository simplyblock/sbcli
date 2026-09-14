"""Node selection in ``backup_controller.restore_backup``.

A backup's ``node_id`` records the node the backup was taken from, which for an
imported backup belongs to the source cluster.  Restore must never place the
new volume there: without an explicit target node the placement is left to
``add_lvol_ha``, which picks a node of the cluster owning the pool.
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.storage_node import StorageNode


TARGET_CLUSTER = "00000000-0000-0000-0000-00000000000c"
SOURCE_CLUSTER = "00000000-0000-0000-0000-00000000000f"


def _backup(node_id, cluster_id=TARGET_CLUSTER, source_cluster_id=""):
    backup = Backup()
    backup.uuid = "backup-1"
    backup.s3_id = 5
    backup.node_id = node_id
    backup.cluster_id = cluster_id
    backup.source_cluster_id = source_cluster_id
    backup.size = 1024
    backup.status = Backup.STATUS_COMPLETED
    return backup


def _node(uuid, cluster_id, status=StorageNode.STATUS_ONLINE, lvstore="lvs_test"):
    node = StorageNode()
    node.uuid = uuid
    node.cluster_id = cluster_id
    node.status = status
    node.lvstore = lvstore
    return node


@pytest.fixture
def db():
    with patch("simplyblock_core.controllers.backup_controller.db_controller") as db:
        pool = MagicMock()
        pool.cluster_id = TARGET_CLUSTER
        db.get_pool_by_id_or_name.return_value = pool

        cluster = MagicMock()
        cluster.uuid = TARGET_CLUSTER
        cluster.backup_source = ""
        db.get_cluster_by_id.return_value = cluster

        lvol = MagicMock()
        lvol.node_id = "target-node"
        lvol.lvs_name = "lvs_test"
        lvol.lvol_bdev = "LVOL_123"
        db.get_lvol_by_id.return_value = lvol
        yield db


@pytest.fixture
def add_lvol_ha():
    with patch("simplyblock_core.controllers.lvol_controller.add_lvol_ha") as add_lvol_ha:
        add_lvol_ha.return_value = ("lvol-new", None)
        yield add_lvol_ha


@pytest.fixture
def tasks():
    with patch("simplyblock_core.controllers.backup_controller.tasks_controller") as tasks:
        tasks.add_backup_restore_task.return_value = True
        yield tasks


def _restore(**kwargs):
    from simplyblock_core.controllers.backup_controller import restore_backup
    return restore_backup("backup-1", "restored_lvol", "pool-1", **kwargs)


class TestImplicitNode:

    def test_backup_node_is_not_used_for_placement(self, db, add_lvol_ha, tasks):
        """An imported backup's node_id points into the source cluster."""
        backup = _backup(node_id="source-cluster-node", source_cluster_id=SOURCE_CLUSTER)
        db.get_backup_by_id.return_value = backup
        db.get_backup_chain.return_value = [backup]
        db.get_cluster_by_id.return_value.backup_source = SOURCE_CLUSTER

        assert _restore() == "lvol-new"
        assert not add_lvol_ha.call_args.kwargs["host_id_or_name"]

    def test_no_node_lookup_without_explicit_target(self, db, add_lvol_ha, tasks):
        backup = _backup(node_id="source-cluster-node", source_cluster_id=SOURCE_CLUSTER)
        db.get_backup_by_id.return_value = backup
        db.get_backup_chain.return_value = [backup]
        db.get_cluster_by_id.return_value.backup_source = SOURCE_CLUSTER

        _restore()

        db.get_storage_node_by_id.assert_not_called()

    def test_restore_task_targets_the_node_the_volume_landed_on(self, db, add_lvol_ha, tasks):
        backup = _backup(node_id="source-cluster-node", source_cluster_id=SOURCE_CLUSTER)
        db.get_backup_by_id.return_value = backup
        db.get_backup_chain.return_value = [backup]
        db.get_cluster_by_id.return_value.backup_source = SOURCE_CLUSTER

        _restore()

        cluster_id, node_id, *_ = tasks.add_backup_restore_task.call_args.args
        assert cluster_id == TARGET_CLUSTER
        assert node_id == "target-node"


class TestExplicitNode:

    @pytest.fixture(autouse=True)
    def _local_backup(self, db):
        backup = _backup(node_id="target-node")
        db.get_backup_by_id.return_value = backup
        db.get_backup_chain.return_value = [backup]

    def test_target_node_is_passed_through(self, db, add_lvol_ha, tasks):
        db.get_storage_node_by_id.return_value = _node("other-node", TARGET_CLUSTER)

        assert _restore(target_node_id="other-node") == "lvol-new"
        assert add_lvol_ha.call_args.kwargs["host_id_or_name"] == "other-node"

    def test_node_of_another_cluster_is_rejected(self, db, add_lvol_ha):
        db.get_storage_node_by_id.return_value = _node("foreign-node", SOURCE_CLUSTER)

        with pytest.raises(PreconditionError, match="belongs to cluster"):
            _restore(target_node_id="foreign-node")

        add_lvol_ha.assert_not_called()

    def test_offline_node_is_rejected(self, db, add_lvol_ha):
        db.get_storage_node_by_id.return_value = _node(
            "other-node", TARGET_CLUSTER, status=StorageNode.STATUS_OFFLINE)

        with pytest.raises(PreconditionError, match="not online"):
            _restore(target_node_id="other-node")

        add_lvol_ha.assert_not_called()

    def test_node_without_lvstore_is_rejected(self, db, add_lvol_ha):
        db.get_storage_node_by_id.return_value = _node("other-node", TARGET_CLUSTER, lvstore="")

        with pytest.raises(PreconditionError, match="no lvstore"):
            _restore(target_node_id="other-node")

        add_lvol_ha.assert_not_called()

    def test_unknown_node_is_rejected(self, db, add_lvol_ha):
        db.get_storage_node_by_id.side_effect = KeyError("Storage node not found: ghost-node")

        with pytest.raises(PreconditionError, match="not found"):
            _restore(target_node_id="ghost-node")

        add_lvol_ha.assert_not_called()


class TestFailures:

    @pytest.fixture(autouse=True)
    def _local_backup(self, db):
        backup = _backup(node_id="target-node")
        db.get_backup_by_id.return_value = backup
        db.get_backup_chain.return_value = [backup]

    def test_source_mismatch_is_a_precondition(self, db, add_lvol_ha):
        db.get_cluster_by_id.return_value.backup_source = SOURCE_CLUSTER

        with pytest.raises(PreconditionError, match="source-switch"):
            _restore()

        add_lvol_ha.assert_not_called()

    def test_incomplete_chain_is_rejected_before_creating_a_volume(self, db, add_lvol_ha):
        db.get_backup_chain.return_value = [_backup(node_id="target-node")]
        db.get_backup_chain.return_value[0].status = Backup.STATUS_PENDING

        with pytest.raises(PreconditionError, match="Incomplete backups in chain"):
            _restore()

        add_lvol_ha.assert_not_called()

    def test_volume_creation_failure_is_a_runtime_error(self, db, add_lvol_ha):
        add_lvol_ha.return_value = (None, "Pool not found")

        with pytest.raises(RuntimeError, match="Failed to create restore volume"):
            _restore()

    def test_task_creation_failure_is_a_runtime_error(self, db, add_lvol_ha, tasks):
        tasks.add_backup_restore_task.return_value = False

        with pytest.raises(RuntimeError, match="Failed to create restore task"):
            _restore()
