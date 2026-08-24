"""The backup CLI's argument handling.

Focused on the two helpers that turn command-line arguments into a
``BackupConfig``, because that is where a disaster-recovery operator's typing
becomes the thing that decides whether a bucket can be read at all.
"""
import zlib
from unittest.mock import patch
from uuid import UUID

import pytest

from simplyblock_cli import clibase
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest, DataPlane, FDBKeyDescriptor, Source, Volume)
from simplyblock_core.models.backup_config import BackupConfig


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


class _Args:
    """Stand-in for the argparse namespace, with only the attributes set."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestCredentials:

    def test_absent_keys_mean_the_instance_role(self):
        """Not "empty credentials" -- that is what broke IAM roles in the first place."""
        assert clibase._s3_credentials(_Args()) is None

    def test_both_keys_are_carried(self):
        credentials = clibase._s3_credentials(
            _Args(access_key_id="AKIA", secret_access_key="shh"))

        assert credentials.access_key_id.get_secret_value() == "AKIA"
        assert credentials.secret_access_key.get_secret_value() == "shh"

    @pytest.mark.parametrize("given", [
        {"access_key_id": "AKIA"},
        {"secret_access_key": "shh"},
    ])
    def test_half_a_pair_is_refused(self, given):
        """Half a key pair reaches S3 as an authentication failure with no clue why."""
        with pytest.raises(ValueError, match="both"):
            clibase._s3_credentials(_Args(**given))


class TestBucketConfig:

    def _args(self, **overrides):
        return _Args(**{"bucket": "backups", "region": "eu-central-1", **overrides})

    def test_minimal_bucket(self):
        config = clibase._bucket_config(self._args())

        assert config.bucket_name == "backups"
        assert config.region == "eu-central-1"
        assert config.endpoint is None
        assert config.credentials is None
        assert config.verify_tls is True
        assert config.use_path_style is False

    def test_endpoint_and_addressing(self):
        config = clibase._bucket_config(self._args(
            endpoint="http://minio:9000", path_style=True, no_verify_tls=True))

        assert config.endpoint_url == "http://minio:9000"
        assert config.use_path_style is True
        assert config.verify_tls is False

    def test_empty_endpoint_is_absent_not_blank(self):
        """argparse gives "" for an unset str; the model must not see that."""
        assert clibase._bucket_config(self._args(endpoint="")).endpoint is None

    def test_the_flag_is_negated_the_way_it_reads(self):
        """--no-verify-tls sets verify_tls=False, not the other way round."""
        assert clibase._bucket_config(self._args()).verify_tls is True
        assert clibase._bucket_config(
            self._args(no_verify_tls=True)).verify_tls is False

    def test_an_omitted_region_defers_to_the_sdk(self):
        """--region is optional, like the credentials: absent means "resolve it".

        Both spellings argparse can produce for an unsupplied option reach the
        model as absence, rather than one of them becoming a region named "".
        """
        assert clibase._bucket_config(_Args(bucket="backups", region=None)).region is None
        assert clibase._bucket_config(_Args(bucket="backups", region="")).region is None

    def test_produces_a_usable_config(self):
        config = clibase._bucket_config(self._args(
            access_key_id="AKIA", secret_access_key="shh"))

        assert isinstance(config, BackupConfig)
        assert config.location().bucket_name == "backups"


class TestDiscoverRendering:
    """`backup discover` reports what a bucket holds, including bad news.

    A chain that could not be restored is the finding, not an error: refusing to
    render the table would hide exactly what the operator opened the bucket to
    see.
    """

    def _run(self, manifests):
        with patch.object(backup_controller, "discover_backups",
                          return_value=manifests):
            return clibase.CLIWrapperBase.backup__discover(
                None, "discover", _Args(bucket="backups", region="eu-central-1"))

    def _manifest(self, name, prev=None, encrypted=False):
        return BackupManifest(
            backup_id=_id(name),
            s3_id=1,
            created_at=100,
            completed_at=200,
            size=4096,
            prev_backup_id=_id(prev) if prev is not None else None,
            encryption=FDBKeyDescriptor(dek_path="keys/x") if encrypted else None,
            source=Source(cluster_id=_id("cluster"), node_id=_id("node")),
            volume=Volume(lvol_id=_id("volume"), lvol_name="vol",
                          snapshot_id=_id("snapshot"), snapshot_name="snap",
                          size=4096),
            dataplane=DataPlane(),
        )

    def test_a_complete_chain_shows_its_length(self):
        line = [self._manifest("b-0"),
                self._manifest("b-1", prev="b-0"),
                self._manifest("b-2", prev="b-1")]

        rows = {row["ID"]: row["Chain"] for row in self._run(line)}

        assert rows[str(_id("b-0"))] == "1"
        assert rows[str(_id("b-2"))] == "3"

    def test_a_missing_ancestor_shows_as_broken(self):
        line = [self._manifest("b-1", prev="b-0")]

        assert self._run(line)[0]["Chain"] == "broken"

    def test_an_incoherent_chain_shows_as_broken(self):
        """Complete, but half of it is encrypted -- unrestorable all the same."""
        line = [self._manifest("b-0", encrypted=True),
                self._manifest("b-1", prev="b-0", encrypted=False)]

        rows = {row["ID"]: row["Chain"] for row in self._run(line)}

        assert rows[str(_id("b-1"))] == "broken"


class TestRegisteredCommands:

    def _cli(self):
        import sys
        sys.argv = ['sbcli']
        from simplyblock_cli.cli import CLIWrapper
        return CLIWrapper()

    def test_discover_is_registered(self):
        """The disaster-recovery entry point: needs a bucket, not a cluster."""
        assert hasattr(self._cli(), 'init_backup__discover')
        assert hasattr(clibase.CLIWrapperBase, 'backup__discover')

    def test_source_switch_is_gone(self):
        """It never switched anything; see the data-plane bucket selection."""
        cli = self._cli()
        assert not hasattr(cli, 'init_backup__source_switch')
        assert not hasattr(cli, 'init_backup__source_list')
        assert not hasattr(clibase.CLIWrapperBase, 'backup__source_switch')
        assert not hasattr(clibase.CLIWrapperBase, 'backup__source_list')
