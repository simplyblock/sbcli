"""An auto-backup taken after a failed ancestor must still chain to it.

``policy._auto_backup_lvol`` used to derive ``prev_backup_id`` from
``get_latest_backup_for_lvol`` -- the lvol's latest *non-failed* ``Backup``
record -- rather than from the new snapshot's actual parent
(``SnapShot.prev_snap_uuid``). Those agree only as long as every earlier
scheduled backup succeeded. The moment one failed, the next auto-backup was
recorded as a full/root backup while the data plane still only ever uploads
the delta since the snapshot's real parent (``bdev_lvol_s3_backup`` is never
told about ``prev_backup_id`` at all). A restore then pulled only the newest
backup's objects and silently produced a volume missing everything the failed
ancestor covered. See ``backup-retry-empty-issue.md`` for the production
incident this reproduces.

``backup_snapshot`` (the manual/API path) never had this bug: it walks the
real ancestor chain via ``_get_snapshot_chain`` and backs up any ancestor
without a valid backup. This test drives the real ``snapshot_controller.add``
and ``policy._auto_backup_lvol`` against a real FoundationDB, so the
``prev_snap_uuid`` pointers the chain walk reads are the ones the product
actually writes.
"""
import time
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers.backup import policy as backup_policy
from simplyblock_core.controllers.backup.chain import BackupChain
from simplyblock_core.db_controller import DBController
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
    p.pool_name = "auto-backup-pool"
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
    n.hostname = "auto-backup-host"
    n.mgmt_ip = "127.0.0.1"
    n.lvstore = "LVS_AUTO"
    n.lvstore_status = "ready"
    n.lvstore_stack = []
    n.max_lvol = 10_000
    n.write_to_db(db.kv_store)
    return n


@pytest.fixture
def lvol(db, pool, node):
    v = LVol()
    v.uuid = str(uuid_mod.uuid4())
    v.lvol_name = "auto-backup-vol"
    v.pool_uuid = pool.get_id()
    v.node_id = node.get_id()
    v.nodes = [node.get_id()]
    v.status = LVol.STATUS_ONLINE
    v.ha_type = "single"
    v.size = 1024 ** 3
    v.max_size = 0
    v.lvs_name = node.lvstore
    v.lvol_bdev = "LVOL_auto"
    v.top_bdev = f"{v.lvs_name}/{v.lvol_bdev}"
    v.base_bdev = "raid_0"
    v.write_to_db(db.kv_store)
    return v


@pytest.fixture(autouse=True)
def mock_snapshot_rpc():
    """The data plane's only RPC surface on the non-HA snapshot path
    (``snapshot_controller.py:751-769``). Backup creation itself makes no RPC
    call -- it only writes a ``Backup`` record and a task."""
    rpc = MagicMock()
    rpc.lvol_create_snapshot.return_value = True
    rpc.bdev_get.side_effect = lambda *_, **__: {
        "uuid": str(uuid_mod.uuid4()),
        "driver_specific": {"lvol": {"blobid": 1, "num_allocated_clusters": 1}},
    }
    with patch.object(StorageNode, "rpc_client", lambda *_, **__: rpc):
        yield


def _new_snapshot_id(db, lvol_id, known_ids: set) -> tuple[str, set]:
    """The one snapshot id that appeared since ``known_ids`` was captured."""
    current = {s.get_id() for s in db.get_snapshots_by_lvol_id(lvol_id)}
    added = current - known_ids
    assert len(added) == 1, f"expected exactly one new snapshot, got {added}"
    return added.pop(), current


class TestAutoBackupChainsPastAFailedAncestor:

    def test_retries_a_failed_ancestor_instead_of_rooting_the_next_backup(
            self, db, cluster, node, lvol):
        known_snapshots: set = set()

        # Round 1: the schedule's first tick. One snapshot, one backup --
        # then it fails, the way a data-plane transfer OOM-killed mid-upload
        # does.
        backup_policy._auto_backup_lvol(lvol)
        snap1_id, known_snapshots = _new_snapshot_id(db, lvol.get_id(), known_snapshots)
        [backup1] = db.get_backups_by_snapshot_id(snap1_id)
        backup1.status = Backup.STATUS_FAILED
        backup1.write_to_db(db.kv_store)

        # Round 2: the schedule fires again. The new snapshot is still a
        # child of the first one at the blob level (SnapShot.prev_snap_uuid)
        # -- that relationship does not depend on whether the first
        # snapshot's backup succeeded.
        time.sleep(1)  # auto-backup names its snapshot off the epoch second
        backup_policy._auto_backup_lvol(lvol)
        snap2_id, known_snapshots = _new_snapshot_id(db, lvol.get_id(), known_snapshots)
        [backup2] = db.get_backups_by_snapshot_id(snap2_id)

        # The failed ancestor must have been retried, not silently left
        # uncovered.
        retried = [b for b in db.get_backups_by_snapshot_id(snap1_id)
                   if b.status != Backup.STATUS_FAILED]
        assert len(retried) == 1, (
            f"snapshot {snap1_id}'s only backup failed; the next auto-backup "
            "must back it up again, not leave it without a valid backup")
        [backup1_retry] = retried

        # And the new backup must chain to that retry rather than stand as a
        # root: a restore of it has to pull snap1's objects too, which is
        # only possible if the chain says so.
        assert backup2.prev_backup_id == backup1_retry.uuid, (
            "the new backup is a delta against snap1 at the data-plane level "
            "regardless of what the control plane recorded; prev_backup_id "
            "must reflect that")

        chain = BackupChain.of_backups(
            uuid_mod.UUID(backup2.uuid), db.get_backups(cluster.get_id()))
        assert [b.snapshot_id for b in chain.records()] == [snap1_id, snap2_id], (
            "restoring this auto-backup must read both snapshots' objects; "
            "under the bug the chain is only [snap2], and the restore "
            "silently produces a volume missing everything snap1 covered")
