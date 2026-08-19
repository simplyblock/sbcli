"""Encrypted backups: what is recorded about their key, and what a restore reaches.

An encrypted backup is ciphertext in a bucket; the key is in a KMS. Nothing used
to record which KMS, so the dependency was implicit and only discovered during a
recovery. These tests pin down that it is written down now, that it never carries
key material, and that a restore which cannot reach the key fails instead of
producing a plaintext volume over ciphertext.
"""
import pytest
from pydantic import HttpUrl

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.kms import LocalKMS, backup_dek_path, backup_kek_name
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster, HashicorpVaultSettings


CLUSTER_ID = "cluster-1"
KEYS = ("a" * 64, "b" * 64)


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-cluster-1",
        "region": "eu-central-1",
        **overrides,
    })


@pytest.fixture
def db():
    return DBController()


def _cluster(db, **config_overrides):
    c = Cluster()
    c.uuid = CLUSTER_ID
    c.backup_config = _config(**config_overrides).model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


def _descriptor(**overrides):
    return {
        "type": "fdb",
        "dek_path": backup_dek_path(CLUSTER_ID, "b-1"),
        **overrides,
    }


def _vault_cluster(db):
    cluster = _cluster(db)
    cluster.hashicorp_vault_settings = HashicorpVaultSettings()
    cluster.hashicorp_vault_settings.base_url = "https://vault.example.com"
    cluster.hashicorp_vault_settings.transit_mount = "sb/transit"
    cluster.hashicorp_vault_settings.kv_mount = "sb/kv"
    return cluster


def _backup(db, uuid="b-1", encrypted=True, encryption=None):
    b = Backup()
    b.uuid = uuid
    b.s3_id = 1
    b.cluster_id = CLUSTER_ID
    b.lvol_id = "lvol-1"
    b.lvol_name = "vol"
    b.size = 4096
    b.status = Backup.STATUS_COMPLETED
    b.location = _config().location().model_dump(exclude_none=True)
    b.encrypted = encrypted
    b.encryption = encryption or {}
    b.write_to_db(db.kv_store)
    return b


class TestKeyDescriptor:

    def test_the_local_backend_is_recorded_as_such(self, db):
        descriptor = backup_controller._build_key_descriptor(
            _cluster(db), _backup(db))

        assert isinstance(descriptor, backup_manifest.FDBKeyDescriptor)
        assert descriptor.type == "fdb"

    def test_vault_settings_are_recorded(self, db):
        descriptor = backup_controller._build_key_descriptor(
            _vault_cluster(db), _backup(db))

        assert isinstance(descriptor, backup_manifest.HCPKeyDescriptor)
        assert descriptor.vault_base_url == HttpUrl("https://vault.example.com")
        assert descriptor.transit_mount == "sb/transit"
        assert descriptor.kv_mount == "sb/kv"

    def test_a_vault_key_names_the_transit_key_that_wraps_it(self, db):
        descriptor = backup_controller._build_key_descriptor(
            _vault_cluster(db), _backup(db))

        assert descriptor.kek_name == backup_kek_name("b-1")

    def test_the_local_backend_has_no_key_encryption_key_to_name(self, db):
        """LocalKMS stores its DEKs as they are; its KEK operations are no-ops.

        The field does not exist on this descriptor at all, rather than existing
        and being None, so nothing can read a name that would describe nothing.
        """
        descriptor = backup_controller._build_key_descriptor(
            _cluster(db), _backup(db))

        assert not hasattr(descriptor, "kek_name")
        with pytest.raises(ValueError):
            backup_manifest.FDBKeyDescriptor(
                dek_path="cluster/c/backup/b", kek_name="backup-b")

    def test_a_vault_descriptor_without_its_transit_key_is_refused(self):
        """The DEK is wrapped under it; without the name nothing unwraps it."""
        with pytest.raises(ValueError):
            backup_manifest.parse_key_descriptor(
                {"type": "hcp", "dek_path": "cluster/c/backup/b"})

    def test_descriptor_points_at_the_key_path(self, db):
        descriptor = backup_controller._build_key_descriptor(
            _cluster(db), _backup(db))

        assert descriptor.dek_path == backup_dek_path(CLUSTER_ID, "b-1")

    def test_descriptor_carries_no_key_material(self, db):
        cluster = _cluster(db)
        backup = _backup(db)
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)

        descriptor = backup_controller._build_key_descriptor(cluster, backup)

        assert KEYS[0] not in descriptor.model_dump_json()

    def test_a_stored_descriptor_reads_back_as_the_backend_that_wrote_it(self, db):
        """What the tag is for: the reader gets the fields its backend defines."""
        for cluster in (_cluster(db), _vault_cluster(db)):
            written = backup_controller._build_key_descriptor(cluster, _backup(db))

            read = backup_manifest.parse_key_descriptor(
                written.model_dump(mode="json"))

            assert read == written

    def test_an_unknown_backend_is_refused_rather_than_guessed(self):
        """Which fields mean anything depends on which backend holds the key."""
        with pytest.raises(ValueError):
            backup_manifest.parse_key_descriptor(_descriptor(type="something-new"))

    def test_an_untagged_descriptor_is_refused_rather_than_guessed(self):
        record = _descriptor()
        del record["type"]

        with pytest.raises(ValueError):
            backup_manifest.parse_key_descriptor(record)


class TestManifestEncryption:

    def test_an_unencrypted_backup_describes_no_key(self, db):
        """Absence is the answer, so there is no flag left to contradict it."""
        _cluster(db)

        manifest = backup_controller.build_manifest(
            _backup(db, encrypted=False))

        assert manifest.encryption is None

    def test_a_legacy_encrypted_backup_cannot_be_described(self, db):
        """A record predating self-describing backups says `encrypted` and no more.

        No manifest expresses that -- an absent descriptor means plaintext -- so
        writing one would advertise ciphertext as readable, and a restore from it
        would produce a plaintext volume over the ciphertext.
        """
        _cluster(db)
        backup = _backup(db, encrypted=True, encryption={})

        with pytest.raises(ValueError, match="predates self-describing"):
            backup_controller.build_manifest(backup)

    def test_survives_a_manifest_round_trip(self, db):
        cluster = _cluster(db)
        backup = _backup(db)
        backup.encryption = backup_controller._build_key_descriptor(
            cluster, backup).model_dump(mode="json", exclude_none=True)
        backup.write_to_db(db.kv_store)

        parsed = backup_manifest.BackupManifest.model_validate(
            backup_controller.build_manifest(backup).model_dump(mode="json"))

        assert parsed.encryption.dek_path == backup_dek_path(CLUSTER_ID, "b-1")

    def test_a_vault_backup_survives_a_manifest_round_trip(self, db):
        cluster = _vault_cluster(db)
        backup = _backup(db)
        backup.encryption = backup_controller._build_key_descriptor(
            cluster, backup).model_dump(mode="json", exclude_none=True)
        backup.write_to_db(db.kv_store)

        parsed = backup_manifest.BackupManifest.model_validate(
            backup_controller.build_manifest(backup).model_dump(mode="json"))

        assert isinstance(parsed.encryption, backup_manifest.HCPKeyDescriptor)
        assert parsed.encryption.kek_name == backup_kek_name("b-1")


class TestKeyResolutionOnRestore:

    def test_unencrypted_backup_needs_no_key(self, db):
        cluster = _cluster(db)
        assert backup_controller._resolve_crypto_key(
            _backup(db, encrypted=False), cluster) is None

    def test_the_key_comes_from_the_kms_the_descriptor_names(self, db):
        cluster = _cluster(db)
        backup = _backup(db, encryption=_descriptor())
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)

        assert backup_controller._resolve_crypto_key(backup, cluster) == KEYS

    def test_unreachable_key_names_what_is_missing(self, db):
        """The operator needs to know which cluster and KMS held the key."""
        cluster = _cluster(db)
        backup = _backup(db, encryption=_descriptor(
            dek_path="cluster/gone/backup/b-1"))

        with pytest.raises(RuntimeError) as excinfo:
            backup_controller._resolve_crypto_key(backup, cluster)

        message = str(excinfo.value)
        assert "cluster/gone/backup/b-1" in message
        assert "fdb" in message

    def test_encrypted_backup_with_no_encryption_record_is_refused(self, db):
        """Rather than silently restoring a plaintext volume over ciphertext."""
        cluster = _cluster(db)
        backup = _backup(db, encrypted=True, encryption={})

        with pytest.raises(PreconditionError, match="records nothing about its"):
            backup_controller._resolve_crypto_key(backup, cluster)
