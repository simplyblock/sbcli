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
        "kms": "local",
        "dek_path": backup_dek_path(CLUSTER_ID, "b-1"),
        "kek_name": backup_kek_name("b-1"),
        **overrides,
    }


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

    def test_local_kms_is_recorded_as_such(self, db):
        cluster = _cluster(db)
        backup = _backup(db)

        encryption = backup_controller._build_encryption(cluster, backup)

        assert encryption.descriptor.kms == "local"

    def test_vault_settings_are_recorded(self, db):
        cluster = _cluster(db)
        cluster.hashicorp_vault_settings = HashicorpVaultSettings()
        cluster.hashicorp_vault_settings.base_url = "https://vault.example.com"
        cluster.hashicorp_vault_settings.transit_mount = "sb/transit"
        cluster.hashicorp_vault_settings.kv_mount = "sb/kv"
        backup = _backup(db)

        encryption = backup_controller._build_encryption(cluster, backup)

        assert encryption.descriptor.kms == "hashicorp_vault"
        assert encryption.descriptor.vault_base_url == HttpUrl("https://vault.example.com")
        assert encryption.descriptor.transit_mount == "sb/transit"
        assert encryption.descriptor.kv_mount == "sb/kv"

    def test_local_kms_records_no_vault_mounts(self, db):
        """Absent rather than "" -- they mean nothing for this backend."""
        encryption = backup_controller._build_encryption(_cluster(db), _backup(db))

        assert encryption.descriptor.vault_base_url is None
        assert encryption.descriptor.transit_mount is None
        assert encryption.descriptor.kv_mount is None

    def test_descriptor_points_at_the_key_path(self, db):
        cluster = _cluster(db)
        backup = _backup(db)

        encryption = backup_controller._build_encryption(cluster, backup)

        assert encryption.descriptor.dek_path == backup_dek_path(CLUSTER_ID, "b-1")
        assert encryption.descriptor.kek_name == backup_kek_name("b-1")

    def test_descriptor_carries_no_key_material(self, db):
        cluster = _cluster(db)
        backup = _backup(db)
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)

        encryption = backup_controller._build_encryption(cluster, backup)

        assert KEYS[0] not in encryption.model_dump_json()

    def test_an_unknown_backend_is_refused_rather_than_guessed(self, db):
        """Which fields of a descriptor mean anything depends on the backend."""
        with pytest.raises(ValueError):
            backup_manifest.KeyDescriptor.model_validate(_descriptor(kms="something-new"))


class TestEncryptionDocument:

    def test_an_encrypted_backup_must_say_where_its_key_is(self):
        """Without that, nothing can decrypt it and nothing can say why."""
        with pytest.raises(ValueError):
            backup_manifest.Encryption(encrypted=True)

    def test_an_unencrypted_backup_has_no_key_to_describe(self):
        with pytest.raises(ValueError):
            backup_manifest.Encryption.model_validate(
                {"encrypted": False, "descriptor": _descriptor()})

    def test_manifest_cannot_disagree_with_the_backup(self, db):
        """Backup.encrypted is authoritative; the stored sub-document is a copy.

        Two places recording the same fact can drift, and drift here means a
        restore reads the wrong one. build_manifest overlays the authoritative
        value so a manifest can never carry the stale copy.
        """
        _cluster(db)
        backup = _backup(db, encrypted=True,
                         encryption={"encrypted": False, "descriptor": _descriptor()})

        manifest = backup_controller.build_manifest(backup)

        assert manifest.encryption.encrypted is True

    def test_survives_a_manifest_round_trip(self, db):
        cluster = _cluster(db)
        backup = _backup(db)
        backup.encryption = backup_controller._build_encryption(
            cluster, backup).model_dump(exclude_none=True)
        backup.write_to_db(db.kv_store)

        parsed = backup_manifest.BackupManifest.model_validate(
            backup_controller.build_manifest(backup).model_dump(mode="json"))

        assert parsed.encryption.descriptor.dek_path == backup_dek_path(CLUSTER_ID, "b-1")


class TestKeyResolutionOnRestore:

    def test_unencrypted_backup_needs_no_key(self, db):
        cluster = _cluster(db)
        assert backup_controller._resolve_crypto_key(
            _backup(db, encrypted=False), cluster) is None

    def test_the_key_comes_from_the_kms_the_descriptor_names(self, db):
        cluster = _cluster(db)
        backup = _backup(db, encryption={"encrypted": True, "descriptor": _descriptor()})
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)

        assert backup_controller._resolve_crypto_key(backup, cluster) == KEYS

    def test_unreachable_key_names_what_is_missing(self, db):
        """The operator needs to know which cluster and KMS held the key."""
        cluster = _cluster(db)
        backup = _backup(db, encryption={
            "encrypted": True,
            "descriptor": _descriptor(dek_path="cluster/gone/backup/b-1")})

        with pytest.raises(RuntimeError) as excinfo:
            backup_controller._resolve_crypto_key(backup, cluster)

        message = str(excinfo.value)
        assert "cluster/gone/backup/b-1" in message
        assert "local" in message

    def test_encrypted_backup_with_no_encryption_record_is_refused(self, db):
        """Rather than silently restoring a plaintext volume over ciphertext."""
        cluster = _cluster(db)
        backup = _backup(db, encrypted=True, encryption={})

        with pytest.raises(PreconditionError, match="records nothing about its"):
            backup_controller._resolve_crypto_key(backup, cluster)
