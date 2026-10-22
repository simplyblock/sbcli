"""Group-backup export, import and discover against real FoundationDB
(design-consistency-group-backup.md §6.5).

A group backup is portable the way a single backup is: export emits the group
manifest together with the member backup chains it names, import preserves ids
(the member uuids and s3_ids, the group backup's own uuid, and the generation)
all-or-nothing, and discover reads the ``groups/`` prefix with no cluster
reference. These prove the round-trip the way the single-volume
``test_manifest_flow.py`` does: build records, throw the database away, rebuild
from the export alone.

The database is real; nothing here mocks the DB layer. Only boto3 (reached
through ``backup_manifest.list_all_groups`` / ``list_all``) is patched, since it
is an external service client.
"""
from unittest.mock import patch

import pytest

from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster

# Every id a manifest carries is a UUID, so the records these tests build get real
# ones rather than readable stand-ins.
SOURCE_CLUSTER = "c1000000-0000-4000-8000-000000000001"
TARGET_CLUSTER = "c1000000-0000-4000-8000-000000000002"
NODE_ID = "d0de0000-0000-4000-8000-000000000001"
GROUP_BACKUP_UUID = "9b000000-0000-4000-8000-000000000001"
CG_ID = f"{SOURCE_CLUSTER}/cccc0000-0000-4000-8000-000000000001"

BUCKET = "simplyblock-backup-primary"


def _backup_id(index: int) -> str:
    return f"0ac00000-0000-4000-8000-{index:012d}"


def _lvol_id(index: int) -> str:
    return f"10101000-0000-4000-8000-{index:012d}"


def _snapshot_id(index: int) -> str:
    return f"50a50000-0000-4000-8000-{index:012d}"


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": BUCKET, "region": "eu-central-1", **overrides})


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def source_cluster(db):
    c = Cluster()
    c.uuid = SOURCE_CLUSTER
    c.cluster_name = "primary"
    c.backup_config = _config().model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


def _member_backup(db, index, cluster=SOURCE_CLUSTER, prev=None,
                   group_backup_id="", status=Backup.STATUS_COMPLETED):
    b = Backup()
    b.uuid = _backup_id(index)
    b.s3_id = index
    b.cluster_id = cluster
    b.lvol_id = _lvol_id(index)
    b.lvol_name = f"vol-{index}"
    b.snapshot_id = _snapshot_id(index)
    b.snapshot_name = f"snap-{index}"
    b.node_id = NODE_ID
    b.prev_backup_id = _backup_id(prev) if prev is not None else ""
    b.size = 4096
    b.created_at = 1000 + index
    b.completed_at = 2000 + index
    b.status = status
    b.location = _config().location().model_dump(mode="json")
    b.group_backup_id = group_backup_id
    b.group_seq = 7 if group_backup_id else 0
    b.write_to_db(db.kv_store)
    return b


def _group_backup(db, members, cluster=SOURCE_CLUSTER,
                  status=GroupBackup.STATUS_COMPLETED):
    gb = GroupBackup()
    gb.uuid = GROUP_BACKUP_UUID
    gb.cluster_id = cluster
    gb.consistency_group_id = CG_ID
    gb.consistency_group_name = "gitea"
    gb.group_seq = 7
    gb.status = status
    gb.members = [{"lvol_id": m.lvol_id, "backup_id": m.get_id()} for m in members]
    gb.encrypted = False
    gb.size = sum(m.size for m in members)
    gb.created_at = 100
    gb.completed_at = 200
    gb.write_to_db(db.kv_store)
    return gb


def _seed_two_member_group(db):
    """A committed group backup of two members, one of which has a two-link chain.

    Member 1 is a full backup; member 2 chains onto its own ancestor (backup 3),
    so the export has to carry that ancestor for the group to be restorable.
    """
    composite = f"{SOURCE_CLUSTER}/{GROUP_BACKUP_UUID}"
    ancestor = _member_backup(db, 3)                      # member 2's parent
    m1 = _member_backup(db, 1, group_backup_id=composite)
    m2 = _member_backup(db, 2, prev=3, group_backup_id=composite)
    return _group_backup(db, [m1, m2]), ancestor, m1, m2


class TestExport:

    def test_export_carries_the_group_manifest_and_member_chains(
            self, db, source_cluster):
        gb, ancestor, m1, m2 = _seed_two_member_group(db)

        export = group_backup.export_group_backup(gb.get_id(), cluster_id=SOURCE_CLUSTER)

        assert str(export.group_manifest.group_backup_id) == GROUP_BACKUP_UUID
        assert export.group_manifest.source.generation == 7
        # Member 2's ancestor travels too, so the group is restorable.
        exported = {str(m.backup_id)
                    for group in export.members.groups for m in group.manifests}
        assert exported == {_backup_id(1), _backup_id(2), _backup_id(3)}

    def test_export_refuses_an_uncommitted_group_backup(self, db, source_cluster):
        gb, *_ = _seed_two_member_group(db)
        gb.status = GroupBackup.STATUS_IN_PROGRESS
        gb.write_to_db(db.kv_store)

        with pytest.raises(PreconditionError, match="not.*completed"):
            group_backup.export_group_backup(gb.get_id(), cluster_id=SOURCE_CLUSTER)

    def test_export_refuses_when_a_member_record_is_missing(self, db, source_cluster):
        gb, ancestor, m1, m2 = _seed_two_member_group(db)
        db.get_backup_by_id(m2.get_id()).remove(db.kv_store)

        with pytest.raises(PreconditionError, match="missing"):
            group_backup.export_group_backup(gb.get_id(), cluster_id=SOURCE_CLUSTER)


class TestExportImportRoundTrip:
    """Export, wipe the database, import into another cluster."""

    def _export_and_wipe(self, db):
        gb, ancestor, m1, m2 = _seed_two_member_group(db)
        export = group_backup.export_group_backup(gb.get_id(), cluster_id=SOURCE_CLUSTER)
        for record in db.get_backups():
            record.remove(db.kv_store)
        db.get_group_backup_by_id(gb.get_id()).remove(db.kv_store)
        assert db.get_backups() == []
        return export

    def test_group_backup_survives_losing_the_database(self, db, source_cluster):
        export = self._export_and_wipe(db)

        composite = group_backup.import_group_backup(export, cluster_id=TARGET_CLUSTER)

        assert composite == f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}"
        restored = db.get_group_backup_by_id(composite)
        assert restored.status == GroupBackup.STATUS_COMPLETED
        assert restored.group_seq == 7
        assert restored.consistency_group_name == "gitea"
        assert {m["backup_id"] for m in restored.members} == {
            f"{TARGET_CLUSTER}/{_backup_id(1)}", f"{TARGET_CLUSTER}/{_backup_id(2)}"}
        # Where the group lives is its members': each imported member records it.
        for member_id in restored.member_backup_ids():
            assert db.get_backup_by_id(member_id).get_location().bucket_name == BUCKET

    def test_member_ids_and_group_stamp_are_preserved(self, db, source_cluster):
        export = self._export_and_wipe(db)

        group_backup.import_group_backup(export, cluster_id=TARGET_CLUSTER)

        member = db.get_backup_by_id(_backup_id(2))
        assert member.s3_id == 2                      # names the objects; never reassigned
        assert member.cluster_id == TARGET_CLUSTER
        assert member.group_backup_id == f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}"
        assert member.group_seq == 7
        # The index resolves "the members of this group backup".
        found = db.get_backups_by_group_backup_id(f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}")
        assert {b.uuid for b in found} == {_backup_id(1), _backup_id(2)}

    def test_the_ancestor_lands_as_a_standalone_backup(self, db, source_cluster):
        """A member's chain ancestor is not itself a group member, so it imports
        without a group stamp -- but it has to be present for the restore."""
        export = self._export_and_wipe(db)

        group_backup.import_group_backup(export, cluster_id=TARGET_CLUSTER)

        ancestor = db.get_backup_by_id(_backup_id(3))
        assert ancestor.group_backup_id == ""
        assert ancestor.cluster_id == TARGET_CLUSTER

    def test_a_group_id_collision_rejects_the_whole_batch(self, db, source_cluster):
        export = group_backup.export_group_backup(
            _seed_two_member_group(db)[0].get_id(), cluster_id=SOURCE_CLUSTER)

        # Importing back into the SAME cluster collides on the group backup id.
        with pytest.raises(PreconditionError, match="already exists"):
            group_backup.import_group_backup(export, cluster_id=SOURCE_CLUSTER)

    def test_a_member_id_collision_rejects_the_whole_batch(self, db, source_cluster):
        """import_backups' per-backup collision check refuses the member, and the
        GroupBackup record is never written -- the whole group imports or none."""
        export = self._export_and_wipe(db)
        # A pre-existing member record in the target cluster.
        _member_backup(db, 1, cluster=TARGET_CLUSTER)

        with pytest.raises(PreconditionError, match="already exists"):
            group_backup.import_group_backup(export, cluster_id=TARGET_CLUSTER)

        with pytest.raises(KeyError):
            db.get_group_backup_by_id(f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}")

    def test_a_torn_export_is_refused(self, db, source_cluster):
        """The group names a member whose manifest the export does not carry."""
        export = self._export_and_wipe(db)
        # Drop member 2's manifest from the members, leaving the group naming it.
        for group in export.members.groups:
            group.manifests = [m for m in group.manifests
                               if str(m.backup_id) != _backup_id(2)]

        with pytest.raises(ValueError, match="missing member manifests"):
            group_backup.import_group_backup(export, cluster_id=TARGET_CLUSTER)

        with pytest.raises(KeyError):
            db.get_group_backup_by_id(f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}")


class TestDiscoverAndImportFromBucket:
    """The disaster-recovery path: nothing but a bucket and credentials."""

    def test_discover_reads_the_group_prefix(self, db, source_cluster):
        gb, *_ = _seed_two_member_group(db)
        group_manifest = group_backup.export_group_backup(
            gb.get_id(), cluster_id=SOURCE_CLUSTER).group_manifest

        with patch.object(backup_manifest, "list_all_groups",
                          return_value=[group_manifest]) as list_groups:
            found = group_backup.discover_group_backups(_config())

        list_groups.assert_called_once()
        assert [str(m.group_backup_id) for m in found] == [GROUP_BACKUP_UUID]
        assert all(isinstance(m, backup_manifest.GroupBackupManifest) for m in found)

    def test_import_from_bucket_needs_no_prior_records(self, db, source_cluster):
        export = group_backup.export_group_backup(
            _seed_two_member_group(db)[0].get_id(), cluster_id=SOURCE_CLUSTER)
        member_manifests = [m for group in export.members.groups for m in group.manifests]
        for record in db.get_backups():
            record.remove(db.kv_store)
        db.get_group_backup_by_id(f"{SOURCE_CLUSTER}/{GROUP_BACKUP_UUID}").remove(db.kv_store)

        with patch.object(backup_manifest, "list_all_groups",
                          return_value=[export.group_manifest]), \
                patch.object(backup_manifest, "list_all",
                             return_value=member_manifests):
            imported = group_backup.import_group_backup_from_bucket(
                _config(), cluster_id=TARGET_CLUSTER)

        assert imported == [f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}"]
        restored = db.get_group_backup_by_id(f"{TARGET_CLUSTER}/{GROUP_BACKUP_UUID}")
        assert {m["backup_id"] for m in restored.members} == {
            f"{TARGET_CLUSTER}/{_backup_id(1)}", f"{TARGET_CLUSTER}/{_backup_id(2)}"}

    def test_import_from_bucket_refuses_a_group_missing_its_members(
            self, db, source_cluster):
        """A group manifest in the bucket names a member whose manifest is not
        there -- the bucket is torn, and the import says so rather than landing a
        group that cannot restore."""
        export = group_backup.export_group_backup(
            _seed_two_member_group(db)[0].get_id(), cluster_id=SOURCE_CLUSTER)
        for record in db.get_backups():
            record.remove(db.kv_store)

        with patch.object(backup_manifest, "list_all_groups",
                          return_value=[export.group_manifest]), \
                patch.object(backup_manifest, "list_all", return_value=[]):
            with pytest.raises(ValueError, match="does not hold"):
                group_backup.import_group_backup_from_bucket(
                    _config(), cluster_id=TARGET_CLUSTER)
