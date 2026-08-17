"""Manifest schema: serialization, version handling, and malformed input.

Pure logic -- the S3 plumbing around it is exercised in the integration tier.
"""
import json

import pytest

from simplyblock_core import backup_manifest
from simplyblock_core.backup_manifest import (
    BackupManifest,
    DataPlane,
    ManifestError,
    MANIFEST_SCHEMA_VERSION,
    Source,
    Volume,
)
from simplyblock_core.models.backup_config import BackupLocation


LOCATION = {"bucket_name": "backups", "region": "eu-central-1"}


def _manifest(**overrides):
    fields = {
        "backup_id": "b-1",
        "s3_id": 7,
        "created_at": 100,
        "completed_at": 200,
        "size": 4096,
        "encrypted": False,
        "location": BackupLocation.model_validate(LOCATION),
        "source": Source(cluster_id="c-1", node_id="n-1"),
        "volume": Volume(lvol_id="l-1", lvol_name="vol", snapshot_id="s-1",
                         snapshot_name="snap", size=4096),
        "dataplane": DataPlane(),
    }
    fields.update(overrides)
    return BackupManifest(**fields)


class TestManifestKey:
    def test_key_cannot_collide_with_the_data_plane_keyspace(self):
        """Data objects are {s3_id}/{mid}/{extent}, all decimal segments."""
        key = backup_manifest.manifest_key("b-1")
        assert key.startswith("manifests/")
        assert not key.split("/")[0].isdigit()


class TestSchema:
    def test_round_trip(self):
        original = _manifest(encrypted=True, prev_backup_id="b-0")

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
        assert "access_key_id" not in json.dumps(data)

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


class TestChainOf:
    def _line(self):
        return [
            _manifest(backup_id="b-0", s3_id=1),
            _manifest(backup_id="b-1", s3_id=2, prev_backup_id="b-0"),
            _manifest(backup_id="b-2", s3_id=3, prev_backup_id="b-1"),
        ]

    def test_walks_to_the_root_oldest_first(self):
        line = self._line()
        chain = backup_manifest.chain_of(line[-1], line)
        assert [m.backup_id for m in chain] == ["b-0", "b-1", "b-2"]

    def test_a_full_backup_is_its_own_chain(self):
        line = self._line()
        assert backup_manifest.chain_of(line[0], line) == [line[0]]

    def test_order_does_not_matter(self):
        line = self._line()
        chain = backup_manifest.chain_of(line[-1], list(reversed(line)))
        assert [m.backup_id for m in chain] == ["b-0", "b-1", "b-2"]

    def test_ignores_manifests_outside_the_chain(self):
        line = self._line()
        unrelated = _manifest(backup_id="other", s3_id=9)
        chain = backup_manifest.chain_of(line[-1], line + [unrelated])
        assert [m.backup_id for m in chain] == ["b-0", "b-1", "b-2"]

    def test_a_missing_ancestor_is_reported_not_truncated(self):
        """Truncating would restore a volume with holes in it."""
        line = self._line()
        with pytest.raises(ManifestError, match="b-0"):
            backup_manifest.chain_of(line[-1], line[1:])

    def test_a_cycle_is_reported_rather_than_looping(self):
        a = _manifest(backup_id="b-a", prev_backup_id="b-b")
        b = _manifest(backup_id="b-b", prev_backup_id="b-a")
        with pytest.raises(ManifestError, match="cyclic"):
            backup_manifest.chain_of(a, [a, b])


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
