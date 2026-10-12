"""Direct backup creation: `backup_controller.backup_lvol` takes its own
snapshot instead of requiring the caller to have made one, then hands off to
`ensure_snapshot_chain_backed_up` -- the same function the manual/API/
auto-backup paths all use to walk and back up a snapshot chain. This
exercises that handoff end to end against a real FoundationDB.
"""
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def cluster(db):
    c = Cluster()
    c.uuid = str(uuid_mod.uuid4())
    c.status = Cluster.STATUS_ACTIVE
    c.nqn = f"nqn.2023-02.io.simplyblock:{c.uuid[:8]}"
    c.page_size_in_blocks = 2097152
    c.blk_size = 4096
    c.prov_cap_crit = 0  # unlimited, so snapshot-capacity checks admit
    c.backup_config = BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-primary",
        "region": "eu-central-1",
    }).model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


@pytest.fixture
def pool(db, cluster):
    p = Pool()
    p.uuid = str(uuid_mod.uuid4())
    p.pool_name = "direct-backup-pool"
    p.cluster_id = cluster.uuid
    p.status = Pool.STATUS_ACTIVE
    p.lvol_max_size = 0
    p.pool_max_size = 0
    p.write_to_db(db.kv_store)
    return p


@pytest.fixture
def node(db, cluster):
    n = StorageNode()
    n.uuid = str(uuid_mod.uuid4())
    n.cluster_id = cluster.uuid
    n.status = StorageNode.STATUS_ONLINE
    n.hostname = "direct-backup-host"
    n.mgmt_ip = "127.0.0.1"
    n.lvstore = "LVS_DIRECT"
    n.lvstore_status = "ready"
    n.lvstore_stack = []
    n.max_lvol = 10_000
    n.write_to_db(db.kv_store)
    return n


@pytest.fixture
def lvol(db, pool, node):
    v = LVol()
    v.uuid = str(uuid_mod.uuid4())
    v.lvol_name = "direct-backup-vol"
    v.pool_uuid = pool.get_id()
    v.node_id = node.get_id()
    v.nodes = [node.get_id()]
    v.status = LVol.STATUS_ONLINE
    v.ha_type = "single"
    v.size = 1024 ** 3
    v.max_size = 0
    v.lvs_name = node.lvstore
    v.lvol_bdev = "LVOL_direct"
    v.top_bdev = f"{v.lvs_name}/{v.lvol_bdev}"
    v.base_bdev = "raid_0"
    v.write_to_db(db.kv_store)
    return v


@pytest.fixture(autouse=True)
def mock_snapshot_rpc():
    """The data plane's only RPC surface on the non-HA snapshot path
    (`snapshot_controller.py:751-769`). Backup creation itself makes no RPC
    call -- it only writes a `Backup` record and a task."""
    rpc = MagicMock()
    rpc.lvol_create_snapshot.return_value = True
    rpc.bdev_get.side_effect = lambda *_, **__: {
        "uuid": str(uuid_mod.uuid4()),
        "driver_specific": {"lvol": {"blobid": 1, "num_allocated_clusters": 1}},
    }
    with patch.object(StorageNode, "rpc_client", lambda *_, **__: rpc):
        yield


class TestDirectBackup:

    def test_creates_a_snapshot_and_backs_it_up(self, db, lvol):
        backup_id = backup_controller.backup_lvol(lvol.get_id())

        assert backup_id

        snapshots = db.get_snapshots_by_lvol_id(lvol.get_id())
        assert len(snapshots) == 1, "backup_lvol must take exactly one snapshot"

        backup = db.get_backup_by_id(backup_id)
        assert backup.snapshot_id == snapshots[0].get_id()
        assert backup.lvol_id == lvol.get_id()
        assert backup.status == Backup.STATUS_PENDING

    def test_names_its_own_snapshot_under_a_fixed_non_overridable_prefix(self, db, lvol):
        """A caller cannot choose the name -- a future cleanup pass has to be
        able to tell this snapshot apart from one a user took directly, and
        that only holds if the marker is never something a caller supplies."""
        backup_controller.backup_lvol(lvol.get_id())

        [snapshot] = db.get_snapshots_by_lvol_id(lvol.get_id())
        assert snapshot.snap_name.startswith(backup_controller.DIRECT_BACKUP_SNAPSHOT_PREFIX)

    def test_refuses_before_taking_a_snapshot_when_the_chain_would_be_unrestorable(
            self, db, cluster, lvol):
        from simplyblock_core.controllers import snapshot_controller

        # An existing, already-backed-up snapshot anchors a chain in one
        # bucket; repointing the cluster at another makes extending that
        # chain unrestorable.
        existing_snap_id, error = snapshot_controller.add(lvol.get_id(), "existing")
        assert not error

        existing_backup = Backup()
        existing_backup.uuid = str(uuid_mod.uuid4())
        existing_backup.s3_id = 1
        existing_backup.cluster_id = cluster.get_id()
        existing_backup.lvol_id = lvol.get_id()
        existing_backup.snapshot_id = existing_snap_id
        existing_backup.location = BackupConfig.model_validate({
            "bucket_name": "the-original-bucket",
            "region": "eu-central-1",
        }).model_dump(mode="json", exclude_none=True)
        existing_backup.status = Backup.STATUS_COMPLETED
        existing_backup.write_to_db(db.kv_store)

        cluster.backup_config = BackupConfig.model_validate({
            "bucket_name": "a-different-bucket",
            "region": "eu-central-1",
        }).model_dump(exclude_none=True)
        cluster.write_to_db(db.kv_store)

        with pytest.raises(PreconditionError):
            backup_controller.backup_lvol(lvol.get_id())

        assert [s.get_id() for s in db.get_snapshots_by_lvol_id(lvol.get_id())] == [existing_snap_id], (
            "a refused backup must not leave an orphaned snapshot behind")

    def test_a_direct_backup_chains_onto_an_earlier_manual_snapshot(self, db, lvol):
        """A manual snapshot exists already; backup_lvol's own snapshot must
        chain onto it rather than standing as a new root."""
        from simplyblock_core.controllers import snapshot_controller

        manual_snap_id, error = snapshot_controller.add(lvol.get_id(), "manual-snap")
        assert not error

        backup_id = backup_controller.backup_lvol(lvol.get_id())

        backup = db.get_backup_by_id(backup_id)
        [manual_backup] = db.get_backups_by_snapshot_id(manual_snap_id)
        assert backup.prev_backup_id == manual_backup.uuid
