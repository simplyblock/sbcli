"""Group backup manifest schema: serialization, version handling, completeness.

Pure logic -- the S3 plumbing (write_group/read_group/list_all_groups) is the
integration tier's, like the per-backup manifest (test_manifest.py).
"""
import json
import zlib
from uuid import UUID

import pytest

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    GROUP_MANIFEST_SCHEMA_VERSION,
    GroupBackupManifest,
    GroupManifestMember,
    GroupManifestMembership,
    GroupManifestSource,
    ManifestError,
)


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


CLUSTER_ID = _id("cluster")


def _member(name):
    return GroupManifestMember(lvol_id=_id(name), lvol_name=name,
                               backup_id=_id("b-" + name),
                               s3_id=zlib.crc32(name.encode()))


def _group_manifest(**overrides):
    members = overrides.pop("members", [_member("db"), _member("data")])
    fields = {
        "group_backup_id": _id("gb-1"),
        "created_at": 100,
        "completed_at": 200,
        "size": 8192,
        "source": GroupManifestSource(
            cluster_id=CLUSTER_ID,
            consistency_group_id=f"{CLUSTER_ID}/{_id('cg')}",
            consistency_group_name="gitea",
            generation=7),
        "members": members,
        "membership": GroupManifestMembership(
            expected_lvol_ids=[m.lvol_id for m in members]),
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
        assert [m["backup_id"] for m in data["members"]] == [
            str(_id("b-db")), str(_id("b-data"))]

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

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValueError):
            GroupBackupManifest.model_validate({
                **json.loads(_group_manifest().model_dump_json()), "extra": 1})


class TestCompleteness:
    def test_complete_when_every_expected_member_is_present(self):
        assert _group_manifest().is_complete() is True

    def test_incomplete_when_an_expected_member_is_missing(self):
        gm = _group_manifest()
        gm.membership.expected_lvol_ids.append(_id("web"))   # expected 3, present 2
        assert gm.is_complete() is False


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
