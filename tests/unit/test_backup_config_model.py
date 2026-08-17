"""Unit tests for the typed backup configuration models."""
import pytest
from pydantic import SecretStr, ValidationError

from simplyblock_core.models.backup_config import (
    BackupConfig,
    BackupLocation,
    S3Credentials,
    SecondaryTarget,
)
from simplyblock_core.models.cluster import Cluster


MINIMAL = {"bucket_name": "backups", "region": "eu-central-1"}


class TestBackupLocation:
    def test_minimal_location(self):
        location = BackupLocation.model_validate(MINIMAL)
        assert location.bucket_name == "backups"
        assert location.region == "eu-central-1"
        assert location.endpoint is None
        assert location.secondary_target is SecondaryTarget.S3
        assert location.snapshot_backups is True
        assert location.verify_tls is True
        assert location.use_path_style is False

    def test_a_bucket_name_is_mandatory(self):
        """Nothing can invent one: a device without a bucket services no I/O."""
        with pytest.raises(ValidationError):
            BackupLocation.model_validate(
                {k: v for k, v in MINIMAL.items() if k != "bucket_name"})

    def test_an_absent_region_defers_to_the_sdk(self):
        """Like credentials and endpoint -- absent means "resolve it", not a gap."""
        location = BackupLocation.model_validate(
            {k: v for k, v in MINIMAL.items() if k != "region"})

        assert location.region is None

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValidationError):
            BackupLocation.model_validate({**MINIMAL, "buckt_name": "typo"})

    def test_is_frozen(self):
        location = BackupLocation.model_validate(MINIMAL)
        with pytest.raises(ValidationError):
            location.bucket_name = "other"

    def test_endpoint_url_strips_trailing_slash(self):
        """pydantic normalises a bare authority to a trailing slash; the SDK wants it gone."""
        location = BackupLocation.model_validate({**MINIMAL, "endpoint": "http://minio:9000"})
        assert str(location.endpoint) == "http://minio:9000/"
        assert location.endpoint_url == "http://minio:9000"

    def test_endpoint_url_is_none_when_unset(self):
        assert BackupLocation.model_validate(MINIMAL).endpoint_url is None

    def test_equality_is_by_value(self):
        """Chain-homogeneity checks compare locations directly."""
        assert BackupLocation.model_validate(MINIMAL) == BackupLocation.model_validate(MINIMAL)
        assert BackupLocation.model_validate(MINIMAL) != BackupLocation.model_validate(
            {**MINIMAL, "bucket_name": "other"}
        )


class TestSecondaryTarget:
    def test_numbering_matches_the_data_plane_rpc(self):
        """The members ARE the wire values; bdev_s3_create is passed them directly."""
        assert SecondaryTarget.S3 == 0
        assert SecondaryTarget.FILESYSTEM == 1

    def test_unknown_value_is_rejected(self):
        with pytest.raises(ValidationError):
            BackupLocation.model_validate({**MINIMAL, "secondary_target": 7})


class TestS3Credentials:
    def test_half_a_pair_is_unrepresentable(self):
        with pytest.raises(ValidationError):
            S3Credentials.model_validate({"access_key_id": "AKIA"})

    def test_secrets_are_masked_in_repr(self):
        creds = S3Credentials(
            access_key_id=SecretStr("AKIAEXAMPLE"),
            secret_access_key=SecretStr("s3cr3t"),
        )
        assert "AKIAEXAMPLE" not in repr(creds)
        assert "s3cr3t" not in repr(creds)


class TestBackupConfig:
    def test_optional_fields_default_to_none_not_sentinels(self):
        config = BackupConfig.model_validate(MINIMAL)
        assert config.credentials is None
        assert config.s3_thread_pool_size is None

    def test_thread_pool_size_must_be_positive(self):
        """Legacy 0 is rewritten to absent by the migrator; anything else below 1 is a bug."""
        with pytest.raises(ValidationError):
            BackupConfig.model_validate({**MINIMAL, "s3_thread_pool_size": -1})

    def test_location_drops_credentials(self):
        config = BackupConfig.model_validate(
            {
                **MINIMAL,
                "credentials": {"access_key_id": "AKIA", "secret_access_key": "s3cr3t"},
                "s3_thread_pool_size": 16,
            }
        )
        location = config.location()

        assert type(location) is BackupLocation
        assert set(location.model_dump()) == set(BackupLocation.model_fields)
        assert "credentials" not in location.model_dump()
        assert "s3_thread_pool_size" not in location.model_dump()

    def test_location_preserves_every_interpretation_field(self):
        config = BackupConfig.model_validate(
            {
                **MINIMAL,
                "endpoint": "https://s3.example.com",
                "with_compression": True,
                "use_path_style": True,
                "verify_tls": False,
                "credentials": {"access_key_id": "AKIA", "secret_access_key": "s3cr3t"},
            }
        )
        location = config.location()

        assert location.endpoint_url == "https://s3.example.com"
        assert location.with_compression is True
        assert location.use_path_style is True
        assert location.verify_tls is False

    def test_secrets_are_masked_in_repr(self):
        config = BackupConfig.model_validate(
            {**MINIMAL, "credentials": {
                "access_key_id": "AKIA", "secret_access_key": "s3cr3t"}}
        )
        assert "AKIA" not in repr(config)
        assert "s3cr3t" not in repr(config)


class TestLegacyMigration:
    """The untyped dicts already stored on existing clusters must keep working."""

    def test_local_endpoint_becomes_endpoint(self):
        config = BackupConfig.model_validate(
            {**MINIMAL, "local_endpoint": "http://minio:9000"}
        )
        assert config.endpoint_url == "http://minio:9000"

    def test_flat_keys_become_a_credential_pair(self):
        config = BackupConfig.model_validate(
            {**MINIMAL, "access_key_id": "AKIA", "secret_access_key": "s3cr3t"}
        )
        assert config.credentials is not None
        assert config.credentials.access_key_id.get_secret_value() == "AKIA"
        assert config.credentials.secret_access_key.get_secret_value() == "s3cr3t"

    def test_a_lone_access_key_does_not_produce_credentials(self):
        config = BackupConfig.model_validate({**MINIMAL, "access_key_id": "AKIA"})
        assert config.credentials is None

    def test_local_testing_unpacks_into_the_properties_it_stood_for(self):
        """It bundled scheme, TLS verification, addressing style and region into one flag."""
        config = BackupConfig.model_validate(
            {"bucket_name": "backups", "local_testing": True,
             "local_endpoint": "http://minio:9000"}
        )
        assert config.verify_tls is False
        assert config.use_path_style is True
        assert config.region == "us-east-1"

    def test_explicit_values_win_over_local_testing_defaults(self):
        config = BackupConfig.model_validate(
            {"bucket_name": "backups", "region": "eu-west-1",
             "local_testing": True, "verify_tls": True}
        )
        assert config.region == "eu-west-1"
        assert config.verify_tls is True

    def test_numeric_secondary_target_becomes_an_enum(self):
        assert BackupConfig.model_validate(
            {**MINIMAL, "secondary_target": 1}
        ).secondary_target is SecondaryTarget.FILESYSTEM

    def test_zero_thread_pool_size_becomes_absent(self):
        assert BackupConfig.model_validate(
            {**MINIMAL, "s3_thread_pool_size": 0}
        ).s3_thread_pool_size is None

    def test_full_legacy_dict(self):
        """The shape from tests/perf/backup_config.json."""
        config = BackupConfig.model_validate({
            "access_key_id": "minioadmin",
            "secret_access_key": "minioadmin",
            "local_endpoint": "http://127.0.0.1:9000",
            "bucket_name": "simplyblock-backup",
            "snapshot_backups": True,
            "with_compression": False,
            "secondary_target": 0,
            "local_testing": True,
            "s3_thread_pool_size": 0,
        })

        assert config.bucket_name == "simplyblock-backup"
        assert config.region == "us-east-1"
        assert config.endpoint_url == "http://127.0.0.1:9000"
        assert config.secondary_target is SecondaryTarget.S3
        assert config.s3_thread_pool_size is None
        assert config.credentials is not None

    def test_legacy_config_without_a_region_still_loads(self):
        """No config written before this model has one, and they keep working."""
        config = BackupConfig.model_validate({
            "bucket_name": "simplyblock-backup",
            "access_key_id": "AKIA",
            "secret_access_key": "s3cr3t",
        })

        assert config.region is None


class TestClusterAccessor:
    def test_returns_a_validated_config(self):
        cluster = Cluster()
        cluster.backup_config = dict(MINIMAL)
        assert cluster.get_backup_config().bucket_name == "backups"

    def test_unconfigured_cluster_raises(self):
        cluster = Cluster()
        assert cluster.backup_config == {}
        with pytest.raises(ValueError, match="no backup configuration"):
            cluster.get_backup_config()

    def test_invalid_config_raises(self):
        """ValidationError is a ValueError, so one except clause covers both cases."""
        cluster = Cluster()
        cluster.backup_config = {"region": "eu-central-1"}  # no bucket to write to
        with pytest.raises(ValueError):
            cluster.get_backup_config()
