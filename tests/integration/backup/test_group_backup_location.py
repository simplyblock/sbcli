"""Where a group backup's manifest lives comes from its member backups, against
real FoundationDB.

A group backup's members share one location (design-consistency-group-backup.md
§3.4), and each member Backup records it. The group record keeps no copy of its
own: the bucket its group manifest is written to and deleted from is the one its
members live in, and only the credentials come from the cluster.
"""
import pytest

from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster

CLUSTER = "c2000000-0000-4000-8000-000000000001"
GROUP_BACKUP_UUID = "9c000000-0000-4000-8000-000000000001"
BUCKET = "simplyblock-backup-primary"


def _config(bucket=BUCKET):
    return BackupConfig.model_validate({"bucket_name": bucket, "region": "eu-central-1"})


@pytest.fixture
def db():
    db = DBController()
    c = Cluster()
    c.uuid = CLUSTER
    c.backup_config = _config().model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return db


def _member(db, index, bucket=BUCKET):
    b = Backup()
    b.uuid = f"0ad00000-0000-4000-8000-{index:012d}"
    b.cluster_id = CLUSTER
    b.lvol_id = f"lv-{index}"
    b.group_backup_id = f"{CLUSTER}/{GROUP_BACKUP_UUID}"
    b.status = Backup.STATUS_COMPLETED
    b.location = _config(bucket).location().model_dump(mode="json")
    b.write_to_db(db.kv_store)
    return b


def _group_backup(db, members):
    gb = GroupBackup()
    gb.uuid = GROUP_BACKUP_UUID
    gb.cluster_id = CLUSTER
    gb.members = [{"lvol_id": m.lvol_id, "backup_id": m.get_id()} for m in members]
    gb.status = GroupBackup.STATUS_IN_PROGRESS
    gb.write_to_db(db.kv_store)
    return gb


def test_the_group_record_keeps_no_location_of_its_own():
    assert "location" not in GroupBackup().get_attrs_map()


def test_the_group_manifest_goes_to_the_bucket_its_members_live_in(db):
    gb = _group_backup(db, [_member(db, 1), _member(db, 2)])

    assert group_backup._config_for_group(gb).bucket_name == BUCKET


def test_a_member_outside_the_configured_bucket_is_refused(db):
    """The cluster's credentials reach only its configured bucket: a member that
    lives elsewhere (the configuration was repointed mid-flight) is refused, as
    the single-volume path refuses it."""
    gb = _group_backup(db, [_member(db, 1), _member(db, 2, bucket="the-old-bucket")])

    with pytest.raises(PreconditionError, match="the-old-bucket"):
        group_backup._config_for_group(gb)


def test_a_group_backup_with_no_member_record_left_has_no_location(db):
    gb = _group_backup(db, [])

    with pytest.raises(PreconditionError, match="no member backup"):
        group_backup._config_for_group(gb)
