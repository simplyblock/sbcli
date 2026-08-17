"""Manifest schema: serialization, version handling, and malformed input.

Pure logic -- the S3 plumbing around it is exercised in the integration tier.
"""
import json

import pytest

from simplyblock_core import backup_manifest
from simplyblock_core.backup_manifest import (
    BackupManifest,
    ChainEntry,
    ManifestError,
    MANIFEST_SCHEMA_VERSION,
)
from simplyblock_core.models.backup_config import BackupLocation


LOCATION = {"bucket_name": "backups", "region": "eu-central-1"}


def _manifest(**overrides):
    return BackupManifest(**{
        "backup_id": "b-1",
        "s3_id": 7,
        "location": BackupLocation.model_validate(LOCATION),
        **overrides,
    })


class TestManifestKey:
    def test_key_cannot_collide_with_the_data_plane_keyspace(self):
        """Data objects are {s3_id}/{mid}/{extent}, all decimal segments."""
        key = backup_manifest.manifest_key("b-1")
        assert key.startswith("manifests/")
        assert not key.split("/")[0].isdigit()


class TestSchema:
    def test_round_trip(self):
        original = _manifest(
            created_at=100, completed_at=200, size=4096, encrypted=True,
            prev_backup_id="b-0",
            chain=[ChainEntry(backup_id="b-0", s3_id=6),
                   ChainEntry(backup_id="b-1", s3_id=7)],
        )

        restored = backup_manifest._parse(original.model_dump_json().encode(), "k")

        assert restored == original

    def test_serializes_to_plain_json(self):
        data = json.loads(_manifest().model_dump_json())
        assert data["schema_version"] == MANIFEST_SCHEMA_VERSION
        assert data["location"]["bucket_name"] == "backups"

    def test_carries_no_credential_field(self):
        """A manifest sits next to the ciphertext; it must not carry keys."""
        data = json.loads(_manifest().model_dump_json())
        assert "credentials" not in data["location"]
        assert "escrow_secret" not in data["location"]
        assert "access_key_id" not in json.dumps(data)

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValueError):
            BackupManifest.model_validate({
                "backup_id": "b-1", "s3_id": 7, "location": LOCATION, "extra": 1})

    def test_chain_is_ordered_oldest_first(self):
        m = _manifest(chain=[ChainEntry(backup_id="b-0", s3_id=6),
                             ChainEntry(backup_id="b-1", s3_id=7)])
        assert [e.backup_id for e in m.chain] == ["b-0", "b-1"]
        assert m.chain[-1].backup_id == m.backup_id


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


class TestFindLocation:
    def test_single_shared_location(self):
        manifests = [_manifest(backup_id="b-1"), _manifest(backup_id="b-2")]
        assert backup_manifest.find_location(manifests) == BackupLocation.model_validate(LOCATION)

    def test_divergent_locations_yield_none(self):
        """A chain split across buckets cannot be restored -- nothing can express it."""
        other = BackupLocation.model_validate({**LOCATION, "bucket_name": "elsewhere"})
        manifests = [_manifest(backup_id="b-1"), _manifest(backup_id="b-2", location=other)]

        assert backup_manifest.find_location(manifests) is None

    def test_differing_only_in_encoding_still_diverges(self):
        """Compression changes how the objects must be read, not just where they are."""
        other = BackupLocation.model_validate({**LOCATION, "with_compression": True})
        manifests = [_manifest(backup_id="b-1"), _manifest(backup_id="b-2", location=other)]

        assert backup_manifest.find_location(manifests) is None

    def test_empty_yields_none(self):
        assert backup_manifest.find_location([]) is None
