"""Manifest schema: serialization, version handling, and malformed input.

Pure logic -- the S3 plumbing around it is exercised in the integration tier.
"""
import json
import zlib
from uuid import UUID

import pytest

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest,
    DataPlane,
    FDBKeyDescriptor,
    HCPKeyDescriptor,
    ManifestError,
    MANIFEST_SCHEMA_VERSION,
    Source,
    Volume,
)


#: Ids are UUIDs in the manifest, so the readable short names the tests talk in
#: are mapped onto stable ones rather than spelled out at every call site.
def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


CLUSTER_ID = _id("cluster")
NODE_ID = _id("node")
VOLUME_ID = _id("volume")
SNAPSHOT_ID = _id("snapshot")


def _manifest(**overrides):
    fields = {
        "backup_id": _id("b-1"),
        "s3_id": 7,
        "created_at": 100,
        "completed_at": 200,
        "size": 4096,
        "source": Source(cluster_id=CLUSTER_ID, node_id=NODE_ID),
        "volume": Volume(lvol_id=VOLUME_ID, lvol_name="vol",
                         snapshot_id=SNAPSHOT_ID, snapshot_name="snap",
                         size=4096),
        "dataplane": DataPlane(),
    }
    fields.update(overrides)
    return BackupManifest(**fields)


class TestManifestKey:
    def test_key_cannot_collide_with_the_data_plane_keyspace(self):
        """Data objects are {s3_id}/{mid}/{extent}, all decimal segments."""
        key = backup_manifest.manifest_key(_id("b-1"))
        assert key.startswith("manifests/")
        assert not key.split("/")[0].isdigit()


class TestSchema:
    def test_round_trip(self):
        original = _manifest(prev_backup_id=_id("b-0"),
                             encryption=FDBKeyDescriptor(dek_path="p"))

        restored = backup_manifest._parse(original.model_dump_json().encode(), "k")

        assert restored == original

    def test_round_trip_keeps_the_backend_that_wrote_the_descriptor(self):
        """The tag is what makes a Vault descriptor read back as one."""
        original = _manifest(encryption=HCPKeyDescriptor(
            dek_path="p", kek_name="k", transit_mount="sb/transit"))

        restored = backup_manifest._parse(original.model_dump_json().encode(), "k")

        assert isinstance(restored.encryption, HCPKeyDescriptor)
        assert restored == original

    def test_an_unencrypted_backup_describes_no_key(self):
        assert _manifest().encryption is None

    def test_serializes_to_plain_json(self):
        data = json.loads(_manifest().model_dump_json())
        assert data["schema_version"] == MANIFEST_SCHEMA_VERSION
        # A plain string on the wire, in the canonical UUID spelling.
        assert data["backup_id"] == str(_id("b-1"))

    def test_carries_no_credential_field(self):
        """A manifest sits next to the ciphertext; it must not carry keys."""
        assert "access_key_id" not in json.dumps(
            json.loads(_manifest().model_dump_json()))

    def test_says_nothing_about_how_to_reach_the_bucket(self):
        """The reader named the bucket to fetch this at all, and a stored copy
        could only go stale: replicate a bucket and every manifest in the copy
        still names the original."""
        data = json.dumps(json.loads(_manifest().model_dump_json()))
        assert "location" not in data
        assert "bucket_name" not in data
        assert "endpoint" not in data
        assert "region" not in data

    def test_records_the_encoding_the_bucket_cannot_reveal(self):
        """Compression is not detectable from the objects, and reading them
        under the wrong answer yields garbage rather than an error."""
        manifest = _manifest(dataplane=DataPlane(with_compression=True))
        restored = backup_manifest._parse(manifest.model_dump_json().encode(), "k")
        assert restored.dataplane.with_compression is True

    def test_an_id_that_is_not_a_uuid_is_rejected(self):
        """Every id in here names a control-plane object, all of which are
        UUIDs. Typing them as such is what stops a manifest from advertising an
        id nothing can be looked up by -- discovered during a recovery, which is
        the one moment there is nobody left to ask."""
        with pytest.raises(ValueError):
            BackupManifest.model_validate({
                **json.loads(_manifest().model_dump_json()), "backup_id": "b-1"})

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValueError):
            BackupManifest.model_validate({
                **json.loads(_manifest().model_dump_json()), "extra": 1})

    def test_a_root_backup_has_no_predecessor(self):
        """Absent, not "" -- a chain root is a state, not a missing value."""
        assert _manifest().prev_backup_id is None

    def test_the_chain_is_not_stored(self):
        """It is derived from prev_backup_id, so a merge rewrites two objects
        rather than every descendant's manifest."""
        assert "chain" not in json.loads(_manifest().model_dump_json())

    def test_volume_settings_are_absent_rather_than_zero(self):
        """0 is a real answer for a QoS cap -- it means unlimited."""
        volume = _manifest().volume
        assert volume.rw_ios_per_sec is None
        assert volume.pool_name is None


class TestParse:
    def test_rejects_a_newer_schema_version(self):
        """Guessing at an unknown schema restores a corrupt volume with no error."""
        data = json.loads(_manifest().model_dump_json())
        data["schema_version"] = MANIFEST_SCHEMA_VERSION + 1

        with pytest.raises(ManifestError, match="schema version"):
            backup_manifest._parse(json.dumps(data).encode(), "k")

    def test_rejects_a_missing_schema_version(self):
        data = json.loads(_manifest().model_dump_json())
        del data["schema_version"]

        with pytest.raises(ManifestError, match="schema version"):
            backup_manifest._parse(json.dumps(data).encode(), "k")

    def test_rejects_invalid_json(self):
        with pytest.raises(ManifestError, match="not valid JSON"):
            backup_manifest._parse(b"{not json", "k")

    def test_rejects_a_malformed_manifest(self):
        with pytest.raises(ManifestError, match="malformed"):
            backup_manifest._parse(
                json.dumps({"schema_version": MANIFEST_SCHEMA_VERSION}).encode(), "k")

    def test_names_the_key_it_could_not_read(self):
        """An operator sweeping a recovery bucket needs to know which object failed."""
        with pytest.raises(ManifestError, match="manifests/b-9.json"):
            backup_manifest._parse(b"{not json", "manifests/b-9.json")
