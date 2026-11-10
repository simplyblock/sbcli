"""Cluster.kms_settings: a real pydantic discriminated union, validated lazily
on read/write the same way backup_config is (see Cluster.get_backup_config),
plus the legacy-shape migration for records written before this field existed.
"""
import pytest
from pydantic import TypeAdapter, ValidationError

from simplyblock_core.kms import KMIPKMSSettings, KMSSettings, VaultKMSSettings
from simplyblock_core.models.cluster import Cluster, HashicorpVaultSettings


class TestKMSSettingsUnion:

    def test_vault_is_the_default_provider_absent_a_tag(self):
        """Legacy flat shape (no discriminator) resolves to Vault -- the only
        provider that ever existed before kms_settings did."""
        settings = TypeAdapter(KMSSettings).validate_python({
            "provider": "vault", "base_url": "https://vault.example.com:8200",
        })
        assert isinstance(settings, VaultKMSSettings)

    def test_kmip_requires_its_own_fields(self):
        settings = TypeAdapter(KMSSettings).validate_python({
            "provider": "kmip", "server_url": "kmip.internal:5696",
        })
        assert isinstance(settings, KMIPKMSSettings)
        assert settings.server_url == "kmip.internal:5696"

    def test_unknown_provider_is_refused(self):
        with pytest.raises(ValidationError):
            TypeAdapter(KMSSettings).validate_python({"provider": "azure_key_vault"})

    def test_vault_without_base_url_is_refused(self):
        with pytest.raises(ValidationError):
            TypeAdapter(KMSSettings).validate_python({"provider": "vault"})

    def test_extra_fields_are_refused(self):
        """Catches a typo (or a stale field from a provider that was removed)
        rather than silently ignoring it."""
        with pytest.raises(ValidationError):
            TypeAdapter(KMSSettings).validate_python({
                "provider": "kmip", "server_url": "kmip.internal:5696", "bogus": "x",
            })


class TestClusterGetSetKMSSettings:

    def test_default_is_none(self):
        assert Cluster().get_kms_settings() is None

    def test_set_then_get_round_trips_vault(self):
        cluster = Cluster()
        cluster.set_kms_settings({
            "provider": "vault", "base_url": "https://vault.example.com:8200",
        })

        settings = cluster.get_kms_settings()

        assert isinstance(settings, VaultKMSSettings)
        assert str(settings.base_url) == "https://vault.example.com:8200/"

    def test_set_then_get_round_trips_kmip(self):
        cluster = Cluster()
        cluster.set_kms_settings({"provider": "kmip", "server_url": "kmip.internal:5696"})

        settings = cluster.get_kms_settings()

        assert isinstance(settings, KMIPKMSSettings)
        assert settings.server_url == "kmip.internal:5696"

    def test_set_none_clears_it(self):
        cluster = Cluster()
        cluster.set_kms_settings({"provider": "kmip", "server_url": "kmip.internal:5696"})

        cluster.set_kms_settings(None)

        assert cluster.get_kms_settings() is None
        assert cluster.kms_settings == {}

    def test_set_validates_before_storing(self):
        """A configuration no backend could act on is rejected at set time,
        not discovered later at create_kms_connection time."""
        cluster = Cluster()
        with pytest.raises(ValidationError):
            cluster.set_kms_settings({"provider": "kmip"})  # missing server_url

        assert cluster.kms_settings == {}

    def test_falls_back_to_legacy_hashicorp_vault_settings_field(self):
        """A record written before kms_settings existed: still readable until
        kms_settings_migration (release_upgrades/) rewrites it."""
        cluster = Cluster()
        cluster.hashicorp_vault_settings = HashicorpVaultSettings({
            "base_url": "https://legacy.example.com:8200",
            "transit_mount": "sb/transit",
            "kv_mount": "sb/kv",
        })

        settings = cluster.get_kms_settings()

        assert isinstance(settings, VaultKMSSettings)
        assert str(settings.base_url) == "https://legacy.example.com:8200/"
        assert settings.transit_mount == "sb/transit"
        assert settings.kv_mount == "sb/kv"

    def test_kms_settings_takes_precedence_over_the_legacy_field(self):
        cluster = Cluster()
        cluster.hashicorp_vault_settings = HashicorpVaultSettings(
            {"base_url": "https://legacy.example.com:8200"})
        cluster.set_kms_settings({"provider": "kmip", "server_url": "kmip.internal:5696"})

        settings = cluster.get_kms_settings()

        assert isinstance(settings, KMIPKMSSettings)

    def test_survives_an_fdb_round_trip(self):
        """to_dict()/from_dict() is what a real write_to_db/read_from_db does."""
        cluster = Cluster()
        cluster.set_kms_settings({"provider": "vault", "base_url": "https://vault.example.com:8200"})

        restored = Cluster().from_dict(cluster.to_dict(unwrap_secrets=True))

        assert isinstance(restored.get_kms_settings(), VaultKMSSettings)
