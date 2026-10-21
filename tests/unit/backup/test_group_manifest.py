"""Group backup manifest schema: serialization, version handling, completeness.

Pure logic -- the S3 plumbing (write_group/read_group/list_all_groups) is the
integration tier's, like the per-backup manifest (test_manifest.py).
"""
import json
import zlib
from datetime import UTC, datetime
from uuid import UUID

import pytest

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    GROUP_EXPORT_SCHEMA_VERSION,
    GROUP_MANIFEST_SCHEMA_VERSION,
    BackupExport,
    BackupManifest,
    GroupBackupExport,
    GroupBackupManifest,
    GroupManifestSource,
    LocatedManifests,
    ManifestDataPlane,
    ManifestError,
    ManifestSource,
    ManifestVolume,
)
from simplyblock_core.models.backup_config import BackupLocation


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


CLUSTER_ID = _id("cluster")
CREATED = datetime.fromtimestamp(1700000000, tz=UTC)
COMPLETED = datetime.fromtimestamp(1700000100, tz=UTC)


def _group_manifest(**overrides):
    names = overrides.pop("names", ["db", "data"])
    fields = {
        "group_backup_id": _id("gb-1"),
        "created_at": CREATED,
        "completed_at": COMPLETED,
        "size": 8192,
        "source": GroupManifestSource(
            cluster_id=CLUSTER_ID,
            consistency_group_id=f"{CLUSTER_ID}/{_id('cg')}",
            consistency_group_name="gitea",
            generation=7),
        "member_backup_ids": [_id("b-" + n) for n in names],
    }
    fields.update(overrides)
    return GroupBackupManifest(**fields)


class TestGroupManifestKey:
    def test_key_is_under_its_own_prefix(self):
        """A group manifest lives under groups/, disjoint from manifests/ and the
        data plane's decimal keyspace."""
        key = backup_manifest.group_manifest_key(_id("gb-1"))
        assert key.startswith("groups/")
        assert not key.split("/")[0].isdigit()


class TestSchema:
    def test_round_trip(self):
        original = _group_manifest()
        restored = backup_manifest._parse_group(original.model_dump_json().encode(), "k")
        assert restored == original

    def test_round_trip_keeps_the_encrypted_disposition(self):
        original = _group_manifest(encrypted=True)
        restored = backup_manifest._parse_group(original.model_dump_json().encode(), "k")
        assert restored.encrypted is True
        assert restored == original

    def test_lists_member_backup_ids(self):
        data = json.loads(_group_manifest().model_dump_json())
        assert data["member_backup_ids"] == [str(_id("b-db")), str(_id("b-data"))]

    def test_names_members_by_backup_id_only(self):
        """Each member's volume, s3 id, and the rest are in its own manifest,
        manifests/<backup_id>.json; the group manifest only links to it."""
        data = json.loads(_group_manifest().model_dump_json())
        assert "members" not in data
        assert "membership" not in data

    def test_says_nothing_about_how_to_reach_the_bucket(self):
        data = json.dumps(json.loads(_group_manifest().model_dump_json()))
        assert "bucket_name" not in data
        assert "endpoint" not in data
        assert "region" not in data

    def test_carries_no_credential_field(self):
        assert "access_key_id" not in json.dumps(
            json.loads(_group_manifest().model_dump_json()))

    def test_an_unencrypted_group_is_marked_plain(self):
        assert _group_manifest().encrypted is False

    def test_times_are_utc_datetimes_on_the_wire(self):
        data = json.loads(_group_manifest().model_dump_json())
        assert datetime.fromisoformat(data["created_at"]) == CREATED
        assert datetime.fromisoformat(data["completed_at"]) == COMPLETED

    def test_a_time_without_a_timezone_is_rejected(self):
        with pytest.raises(ValueError):
            _group_manifest(created_at=datetime(2026, 10, 21, 12, 0, 0))

    def test_a_negative_size_is_rejected(self):
        with pytest.raises(ValueError):
            _group_manifest(size=-1)

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValueError):
            GroupBackupManifest.model_validate({
                **json.loads(_group_manifest().model_dump_json()), "extra": 1})


class TestCompleteness:
    """Completeness is a property of the bucket, not of the group manifest alone:
    the group manifest is written only once every member completed, so what can
    go missing afterwards is a member's own manifest."""

    def test_complete_when_every_member_manifest_is_present(self):
        members = [_backup_manifest("db"), _backup_manifest("data")]
        assert _group_manifest().is_complete(members) is True

    def test_incomplete_when_a_member_manifest_is_missing(self):
        assert _group_manifest().is_complete([_backup_manifest("db")]) is False

    def test_other_manifests_in_the_bucket_do_not_count(self):
        members = [_backup_manifest("db"), _backup_manifest("web")]
        assert _group_manifest().is_complete(members) is False


class TestParse:
    def test_rejects_a_newer_schema_version(self):
        data = json.loads(_group_manifest().model_dump_json())
        data["schema_version"] = GROUP_MANIFEST_SCHEMA_VERSION + 1
        with pytest.raises(ManifestError, match="schema version"):
            backup_manifest._parse_group(json.dumps(data).encode(), "k")

    def test_rejects_invalid_json(self):
        with pytest.raises(ManifestError, match="not valid JSON"):
            backup_manifest._parse_group(b"{not json", "k")

    def test_rejects_a_malformed_manifest(self):
        with pytest.raises(ManifestError, match="malformed"):
            backup_manifest._parse_group(
                json.dumps({"schema_version": GROUP_MANIFEST_SCHEMA_VERSION}).encode(), "k")

    def test_names_the_key_it_could_not_read(self):
        with pytest.raises(ManifestError, match="groups/gb-9.json"):
            backup_manifest._parse_group(b"{not json", "groups/gb-9.json")


def _backup_manifest(name):
    """A single-volume member manifest for the member named *name*."""
    return BackupManifest(
        backup_id=_id("b-" + name),
        s3_id=zlib.crc32(name.encode()),
        created_at=100,
        completed_at=200,
        size=4096,
        source=ManifestSource(cluster_id=CLUSTER_ID, node_id=_id("node")),
        volume=ManifestVolume(
            lvol_id=_id(name), lvol_name=name,
            snapshot_id=_id("snap-" + name), snapshot_name="snap", size=4096),
        dataplane=ManifestDataPlane(),
    )


LOCATION = BackupLocation(bucket_name="backups", region="eu-central-1")


def _group_export(**overrides):
    """A group export: the group manifest plus its member backup manifests."""
    members = overrides.pop("members", [_backup_manifest("db"), _backup_manifest("data")])
    fields = {
        "group_manifest": _group_manifest(),
        "members": BackupExport(groups=[
            LocatedManifests(location=LOCATION, manifests=members)]),
    }
    fields.update(overrides)
    return GroupBackupExport(**fields)


class TestGroupExportSchema:
    """GroupBackupExport is the unit of transfer: the group manifest together with
    the member backup manifests it names (design §6.5)."""

    def test_round_trip(self):
        original = _group_export()
        restored = backup_manifest.parse_group_export(
            original.model_dump_json().encode(), "k")
        assert restored == original

    def test_carries_the_group_manifest_and_the_member_manifests(self):
        export = _group_export()
        assert export.group_manifest.source.consistency_group_name == "gitea"
        assert [m.volume.lvol_name for m in export.members.groups[0].manifests] == [
            "db", "data"]

    def test_names_where_the_members_live(self):
        """Unlike a manifest, an export records a location -- it is not stored in
        the bucket it describes, so the reader has nothing else to go on."""
        export = _group_export()
        assert export.members.groups[0].location.bucket_name == "backups"

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValueError):
            GroupBackupExport.model_validate({
                **json.loads(_group_export().model_dump_json()), "extra": 1})


class TestParseGroupExport:
    def test_rejects_a_newer_schema_version(self):
        data = json.loads(_group_export().model_dump_json())
        data["schema_version"] = GROUP_EXPORT_SCHEMA_VERSION + 1
        with pytest.raises(ManifestError, match="group export schema version"):
            backup_manifest.parse_group_export(json.dumps(data).encode(), "file.json")

    def test_rejects_invalid_json(self):
        with pytest.raises(ManifestError, match="not valid JSON"):
            backup_manifest.parse_group_export(b"{not json", "file.json")

    def test_rejects_a_malformed_export(self):
        with pytest.raises(ManifestError, match="malformed"):
            backup_manifest.parse_group_export(
                json.dumps({"schema_version": GROUP_EXPORT_SCHEMA_VERSION}).encode(),
                "file.json")

    def test_names_the_source_it_could_not_read(self):
        with pytest.raises(ManifestError, match="file.json"):
            backup_manifest.parse_group_export(b"{not json", "file.json")
