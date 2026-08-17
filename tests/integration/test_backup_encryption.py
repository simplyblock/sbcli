"""Encrypted backups: key descriptors, wrapped_key, and what a restore can reach.

The question these answer is the one a disaster recovery asks: given a bucket and
a passphrase but no originating cluster, can this backup be decrypted? A backup
that answers "no" must say so at creation time and refuse at restore time, not
produce a plaintext volume over ciphertext.
"""
import pytest
from pydantic import SecretStr

from simplyblock_core import backup_key_wrapping, backup_manifest
from simplyblock_core.controllers import backup_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.kms import LocalKMS, backup_dek_path, backup_kek_name
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster, HashicorpVaultSettings


CLUSTER_ID = "cluster-1"
PASSPHRASE = SecretStr("correct horse battery staple")
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
    c.backup_config = _config(**config_overrides).to_storage_dict()
    c.write_to_db(db.kv_store)
    return c


def _backup(db, uuid="b-1", encrypted=True, encryption=None):
    b = Backup()
    b.uuid = uuid
    b.s3_id = 1
    b.cluster_id = CLUSTER_ID
    b.source_cluster_id = CLUSTER_ID
    b.lvol_id = "lvol-1"
    b.lvol_name = "vol"
    b.size = 4096
    b.status = Backup.STATUS_COMPLETED
    b.location = _config().location().model_dump(mode="json")
    b.encrypted = encrypted
    b.encryption = encryption or {}
    b.write_to_db(db.kv_store)
    return b


class TestKeyDescriptor:

    def test_local_kms_is_recorded_as_such(self, db):
        """A "local" backup with no wrapped key is unrecoverable once its cluster is gone."""
        cluster = _cluster(db)
        backup = _backup(db, encryption=None)

        with LocalKMS(cluster) as kms:
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert encryption.descriptor.kms == "local"
        assert encryption.wrapped_key is None
        assert encryption.recoverable_without_source_kms is False

    def test_vault_settings_are_recorded(self, db):
        cluster = _cluster(db)
        cluster.hashicorp_vault_settings = HashicorpVaultSettings()
        cluster.hashicorp_vault_settings.base_url = "https://vault.example.com"
        cluster.hashicorp_vault_settings.transit_mount = "sb/transit"
        cluster.hashicorp_vault_settings.kv_mount = "sb/kv"
        backup = _backup(db)

        with LocalKMS(cluster) as kms:
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert encryption.descriptor.kms == "hashicorp_vault"
        assert encryption.descriptor.vault_base_url == "https://vault.example.com"
        assert encryption.descriptor.transit_mount == "sb/transit"
        assert encryption.descriptor.kv_mount == "sb/kv"

    def test_descriptor_points_at_the_key_path(self, db):
        cluster = _cluster(db)
        backup = _backup(db)

        with LocalKMS(cluster) as kms:
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert encryption.descriptor.dek_path == backup_dek_path(CLUSTER_ID, "b-1")
        assert encryption.descriptor.kek_name == backup_kek_name("b-1")

    def test_descriptor_carries_no_key_material(self, db):
        cluster = _cluster(db)
        backup = _backup(db)
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert KEYS[0] not in encryption.model_dump_json()


class TestKeyWrapping:

    def test_configured_cluster_wraps_the_key(self, db):
        cluster = _cluster(db, key_wrapping_secret=PASSPHRASE.get_secret_value())
        backup = _backup(db)

        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert encryption.wrapped_key is not None
        assert encryption.recoverable_without_source_kms is True
        assert backup_key_wrapping.unwrap(encryption.wrapped_key, PASSPHRASE) == KEYS

    def test_wrapped_key_works_for_localkms_too(self, db):
        """LocalKMS keeps keys in the originating cluster's own database, so
        wrapping is the only thing that makes such a backup recoverable at all."""
        cluster = _cluster(db, key_wrapping_secret=PASSPHRASE.get_secret_value())
        backup = _backup(db)

        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)
            encryption = backup_controller._build_encryption(cluster, backup, kms)

        assert encryption.descriptor.kms == "local"
        assert encryption.wrapped_key is not None

    def test_wrapped_key_survives_a_manifest_round_trip(self, db):
        cluster = _cluster(db, key_wrapping_secret=PASSPHRASE.get_secret_value())
        backup = _backup(db)
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)
            backup.encryption = backup_controller._build_encryption(
                cluster, backup, kms).model_dump(mode="json")
        backup.write_to_db(db.kv_store)

        manifest = backup_controller.build_manifest(backup)
        parsed = backup_manifest.BackupManifest.model_validate(
            manifest.model_dump(mode="json"))

        assert backup_key_wrapping.unwrap(parsed.encryption.wrapped_key, PASSPHRASE) == KEYS


class TestDrCapableFlag:

    def test_unencrypted_backup_is_always_dr_capable(self, db):
        assert _backup(db, encrypted=False).dr_capable is True

    def test_encrypted_without_a_wrapped_key_is_not(self, db):
        backup = _backup(db, encryption={"encrypted": True, "descriptor": {"kms": "local"}})
        assert backup.dr_capable is False

    def test_encrypted_with_a_wrapped_key_is(self, db):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        backup = _backup(db, encryption={
            "encrypted": True, "wrapped_key": wrapped.model_dump(mode="json")})

        assert backup.dr_capable is True

    def test_manifest_cannot_disagree_with_the_backup(self, db):
        """Backup.encrypted is authoritative; the stored sub-document is a copy.

        Two places recording the same fact can drift, and drift here means a
        restore reads the wrong one. build_manifest overlays the authoritative
        value so a manifest can never carry the stale copy.
        """
        _cluster(db)
        backup = _backup(db, encrypted=True,
                         encryption={"encrypted": False, "descriptor": {"kms": "local"}})

        manifest = backup_controller.build_manifest(backup)

        assert manifest.encryption.encrypted is True

    def test_flag_is_visible_in_the_listing(self, db):
        _cluster(db)
        _backup(db, uuid="b-plain", encrypted=False)
        _backup(db, uuid="b-locked",
                encryption={"encrypted": True, "descriptor": {"kms": "local"}})

        rows = {r["ID"]: r["DR"] for r in backup_controller.list_backups(CLUSTER_ID)}

        assert rows["b-plain"] == "yes"
        assert rows["b-locked"] == "needs source KMS"


class TestKeyResolutionOnRestore:

    def test_unencrypted_backup_needs_no_key(self, db):
        cluster = _cluster(db)
        assert backup_controller._resolve_crypto_key(
            _backup(db, encrypted=False), cluster, None) is None

    def test_wrapped_key_is_unwrapped_with_the_passphrase(self, db):
        cluster = _cluster(db)
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        backup = _backup(db, encryption={
            "encrypted": True, "wrapped_key": wrapped.model_dump(mode="json")})

        assert backup_controller._resolve_crypto_key(backup, cluster, PASSPHRASE) == KEYS

    def test_wrapped_key_without_a_passphrase_is_refused(self, db):
        cluster = _cluster(db)
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        backup = _backup(db, encryption={
            "encrypted": True, "wrapped_key": wrapped.model_dump(mode="json")})

        with pytest.raises(PreconditionError, match="supply the wrapped_key passphrase"):
            backup_controller._resolve_crypto_key(backup, cluster, None)

    def test_wrong_passphrase_is_refused(self, db):
        cluster = _cluster(db)
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        backup = _backup(db, encryption={
            "encrypted": True, "wrapped_key": wrapped.model_dump(mode="json")})

        with pytest.raises(PreconditionError, match="Cannot open wrapped key"):
            backup_controller._resolve_crypto_key(backup, cluster, SecretStr("wrong"))

    def test_falls_back_to_the_descriptor_when_not_wrapped(self, db):
        cluster = _cluster(db)
        backup = _backup(db, encryption={
            "encrypted": True,
            "descriptor": {"kms": "local",
                           "dek_path": backup_dek_path(CLUSTER_ID, "b-1"),
                           "kek_name": backup_kek_name("b-1")}})
        with LocalKMS(cluster) as kms:
            kms.import_data_encryption_keys(
                backup_dek_path(CLUSTER_ID, "b-1"), backup_kek_name("b-1"), KEYS)

        assert backup_controller._resolve_crypto_key(backup, cluster, None) == KEYS

    def test_unreachable_key_names_what_is_missing(self, db):
        """The operator needs to know which cluster and KMS held the key."""
        cluster = _cluster(db)
        backup = _backup(db, encryption={
            "encrypted": True,
            "descriptor": {"kms": "local",
                           "dek_path": "cluster/gone/backup/b-1",
                           "kek_name": "backup-b-1"}})

        with pytest.raises(PreconditionError) as excinfo:
            backup_controller._resolve_crypto_key(backup, cluster, None)

        message = str(excinfo.value)
        assert "cluster/gone/backup/b-1" in message
        assert "no key was wrapped" in message

    def test_encrypted_backup_with_no_encryption_record_is_refused(self, db):
        """Rather than silently restoring a plaintext volume over ciphertext."""
        cluster = _cluster(db)
        backup = _backup(db, encrypted=True, encryption={})

        with pytest.raises(PreconditionError, match="records nothing about its"):
            backup_controller._resolve_crypto_key(backup, cluster, None)
