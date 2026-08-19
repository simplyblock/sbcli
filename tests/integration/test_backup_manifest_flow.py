"""Manifest assembly and the export -> import round-trip, against real FoundationDB.

The point of a manifest is that a backup can be understood without the cluster
that wrote it, so the test that matters is: build manifests, throw the database
away, and rebuild usable Backup records from the manifests alone.

Only boto3 is mocked -- it is an external service client. The database is real.
"""
from unittest.mock import patch

import pytest

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool


CLUSTER_ID = "cluster-1"


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-cluster-1",
        "region": "eu-central-1",
        **overrides,
    })


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def cluster(db):
    c = Cluster()
    c.uuid = CLUSTER_ID
    c.cluster_name = "primary"
    c.backup_config = _config().model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


@pytest.fixture
def lvol(db):
    pool = Pool()
    pool.uuid = "pool-1"
    pool.pool_name = "testpool"
    pool.cluster_id = CLUSTER_ID
    pool.write_to_db(db.kv_store)

    volume = LVol()
    volume.uuid = "lvol-1"
    volume.lvol_name = "vol"
    volume.pool_uuid = "pool-1"
    volume.pool_name = "testpool"
    volume.node_id = "node-1"
    volume.size = 4096
    volume.ha_type = "ha"
    volume.fabric = "tcp"
    volume.rw_ios_per_sec = 5000
    volume.max_size = 8192
    volume.write_to_db(db.kv_store)
    return volume


def _backup(db, uuid, s3_id, prev="", **overrides):
    b = Backup()
    b.uuid = uuid
    b.s3_id = s3_id
    b.cluster_id = CLUSTER_ID
    b.lvol_id = "lvol-1"
    b.lvol_name = "vol"
    b.snapshot_id = f"snap-{uuid}"
    b.snapshot_name = f"snap_{uuid}"
    b.node_id = "node-1"
    b.pool_uuid = "pool-1"
    b.prev_backup_id = prev
    b.size = 4096
    b.created_at = 1000 + s3_id
    b.completed_at = 2000 + s3_id
    b.allowed_hosts = [{"nqn": "nqn.2024-01.io.test:host"}]
    b.status = Backup.STATUS_COMPLETED
    b.location = _config().location().model_dump(mode="json")
    for key, value in overrides.items():
        setattr(b, key, value)
    b.write_to_db(db.kv_store)
    return b


class TestBuildManifest:

    def test_records_the_backup_location(self, db, cluster, lvol):
        manifest = backup_controller.build_manifest(_backup(db, "b-1", 1))

        assert manifest.location.bucket_name == "simplyblock-backup-cluster-1"
        assert manifest.location.region == "eu-central-1"

    def test_records_only_the_immediate_predecessor(self, db, cluster, lvol):
        """Not the whole chain: storing that would make a merge rewrite the
        manifest of every descendant of the backup it folded away."""
        _backup(db, "b-1", 1)
        _backup(db, "b-2", 2, prev="b-1")
        third = _backup(db, "b-3", 3, prev="b-2")

        manifest = backup_controller.build_manifest(third)

        assert manifest.prev_backup_id == "b-2"

    def test_a_full_backup_has_no_predecessor(self, db, cluster, lvol):
        """Absent rather than "", so a chain root is a state and not a blank."""
        assert backup_controller.build_manifest(_backup(db, "b-1", 1)).prev_backup_id is None

    def test_the_chain_reconstructs_from_the_links(self, db, cluster, lvol):
        """What the stored chain was for, done from the bucket's own contents."""
        _backup(db, "b-1", 1)
        _backup(db, "b-2", 2, prev="b-1")
        _backup(db, "b-3", 3, prev="b-2")
        manifests = [backup_controller.build_manifest(b) for b in db.get_backups()]
        last = next(m for m in manifests if m.backup_id == "b-3")

        chain = backup_manifest.chain_of(last, manifests)

        assert [(m.backup_id, m.s3_id) for m in chain] == [
            ("b-1", 1), ("b-2", 2), ("b-3", 3)]

    def test_records_the_volume_shape(self, db, cluster, lvol):
        """Restore currently hardcodes these; recording them is what lets it stop."""
        manifest = backup_controller.build_manifest(_backup(db, "b-1", 1))

        assert manifest.volume.pool_name == "testpool"
        assert manifest.volume.ha_type == "ha"
        assert manifest.volume.rw_ios_per_sec == 5000
        assert manifest.volume.max_size == 8192
        assert manifest.volume.allowed_hosts == ["nqn.2024-01.io.test:host"]

    def test_host_keys_never_reach_the_bucket(self, db, cluster, lvol):
        """A manifest carries no authentication material, host keys included.

        Restore takes the NQNs and mints fresh keys from the target pool, so
        publishing these would be a plaintext copy of the volume's DHCHAP keys
        and PSK in a bucket, for no reader.
        """
        backup = _backup(db, "b-1", 1, allowed_hosts=[{
            "nqn": "nqn.2024-01.io.test:host",
            "dhchap_key": "DHHC-1:00:secret-dhchap:",
            "psk": "NVMeTLSkey-1:01:secret-psk:",
        }])

        manifest = backup_controller.build_manifest(backup)

        assert manifest.volume.allowed_hosts == ["nqn.2024-01.io.test:host"]
        assert "secret-dhchap" not in manifest.model_dump_json()
        assert "secret-psk" not in manifest.model_dump_json()

    def test_survives_a_deleted_volume(self, db, cluster, lvol):
        """A backup outlives its volume; that must not stop the manifest."""
        backup = _backup(db, "b-1", 1)
        lvol.remove(db.kv_store)

        manifest = backup_controller.build_manifest(backup)

        assert manifest.volume.lvol_name == "vol"    # carried on the backup itself
        assert manifest.volume.pool_name is None     # only on the volume
        # Absent, not 0 -- which for a QoS cap would read as "unlimited".
        assert manifest.volume.rw_ios_per_sec is None

    def test_records_source_as_provenance(self, db, cluster, lvol):
        manifest = backup_controller.build_manifest(_backup(db, "b-1", 1))

        assert manifest.source.cluster_id == CLUSTER_ID
        assert manifest.source.cluster_name == "primary"
        assert manifest.source.node_id == "node-1"

    def test_records_the_object_layout(self, db, cluster, lvol):
        manifest = backup_controller.build_manifest(_backup(db, "b-1", 1))

        assert manifest.dataplane.key_format == "{s3_id}/{mid}/{extent}"
        assert manifest.dataplane.cluster_size == cluster.page_size_in_blocks

    def test_backup_without_a_location_is_refused(self, db, cluster, lvol):
        backup = _backup(db, "b-1", 1)
        backup.location = {}

        with pytest.raises(ValueError):
            backup_controller.build_manifest(backup)


class TestExportImportRoundTrip:
    """Export, wipe the database, import -- the disaster-recovery shape."""

    def test_backups_survive_losing_the_database(self, db, cluster, lvol):
        _backup(db, "b-1", 1)
        _backup(db, "b-2", 2, prev="b-1", encrypted=True,
                encryption={"encrypted": True, "descriptor": {"kms": "local"}})

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        for backup in db.get_backups():
            backup.remove(db.kv_store)
        assert db.get_backups() == []

        count = backup_controller.import_backups(exported, cluster_id="cluster-2")

        assert count == 2
        restored = db.get_backup_by_id("b-2")
        assert restored.s3_id == 2
        assert restored.prev_backup_id == "b-1"
        assert restored.cluster_id == "cluster-2"
        assert restored.get_location().bucket_name == "simplyblock-backup-cluster-1"
        assert restored.encrypted is True

    def test_encrypted_flag_survives(self, db, cluster, lvol):
        """It used to be dropped, restoring a plaintext volume over ciphertext."""
        _backup(db, "b-1", 1, encrypted=True,
                encryption={"encrypted": True, "descriptor": {"kms": "local"}})
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id("b-1").remove(db.kv_store)

        backup_controller.import_backups(exported, cluster_id="cluster-2")

        assert db.get_backup_by_id("b-1").encrypted is True

    def test_export_emits_the_same_shape_as_the_bucket(self, db, cluster, lvol):
        """One format, so a file and a bucket read are interchangeable."""
        _backup(db, "b-1", 1)

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        assert backup_manifest._parse(
            exported[0].model_dump_json().encode(), "k") == exported[0]

    def test_only_completed_backups_are_exported(self, db, cluster, lvol):
        _backup(db, "b-1", 1)
        _backup(db, "b-2", 2, status=Backup.STATUS_FAILED)

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        assert [m.backup_id for m in exported] == ["b-1"]

    def test_a_malformed_entry_never_reaches_the_controller(self, db, cluster, lvol):
        """import_backups takes manifests, not dicts, so an unusable entry is
        rejected by whoever read the bytes -- naming the file or the request."""
        with pytest.raises(ValueError):
            backup_manifest.BackupManifest.model_validate(
                {"backup_id": "b-9", "s3_id": "not-an-int"})

    def test_duplicate_id_rejects_the_whole_batch(self, db, cluster, lvol):
        _backup(db, "b-1", 1)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        with pytest.raises(PreconditionError, match="already exists"):
            backup_controller.import_backups(exported, cluster_id="cluster-2")

    def test_same_id_listed_twice_rejects_the_whole_batch(self, db, cluster, lvol):
        """Backup lookups are not cluster-scoped, so a reused uuid unaddresses both."""
        _backup(db, "b-1", 1)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id("b-1").remove(db.kv_store)

        with pytest.raises(ValueError, match="listed more than once"):
            backup_controller.import_backups(exported + exported, cluster_id="cluster-2")

        assert db.get_backups() == []

    def test_nothing_to_import_is_not_an_error(self, db, cluster, lvol):
        assert backup_controller.import_backups([], cluster_id="cluster-2") == 0


class TestBucketDiscovery:
    """The path that needs nothing but a bucket and credentials."""

    def test_discover_reads_every_manifest(self, db, cluster, lvol):
        _backup(db, "b-1", 1)
        _backup(db, "b-2", 2, prev="b-1")
        manifests = [backup_controller.build_manifest(b) for b in db.get_backups()]

        with patch.object(backup_manifest, "list_all", return_value=manifests):
            found = backup_controller.discover_backups(_config())

        assert {m.backup_id for m in found} == {"b-1", "b-2"}
        # Models, not dicts: rendering them is the caller's decision.
        assert all(isinstance(m, backup_manifest.BackupManifest) for m in found)

    def test_import_from_bucket_needs_no_prior_records(self, db, cluster, lvol):
        _backup(db, "b-1", 1)
        manifests = [backup_controller.build_manifest(db.get_backup_by_id("b-1"))]
        db.get_backup_by_id("b-1").remove(db.kv_store)

        with patch.object(backup_manifest, "list_all", return_value=manifests):
            count = backup_controller.import_from_bucket(_config(), cluster_id="cluster-2")

        assert count == 1
        assert db.get_backup_by_id("b-1").cluster_id == "cluster-2"


class TestManifestPublication:

    def test_write_manifest_puts_the_object_in_the_backup_bucket(self, db, cluster, lvol):
        backup = _backup(db, "b-1", 1)

        with patch.object(backup_manifest, "s3_client") as mock_client:
            backup_controller.write_manifest(backup)

        _, kwargs = mock_client.return_value.put_object.call_args
        assert kwargs["Bucket"] == "simplyblock-backup-cluster-1"
        assert kwargs["Key"] == "manifests/b-1.json"

    def test_refuses_when_the_cluster_points_at_another_bucket(self, db, cluster, lvol):
        """The cluster's credentials cannot be assumed to reach a foreign bucket."""
        backup = _backup(db, "b-1", 1)
        backup.location = _config(bucket_name="somewhere-else").location().model_dump(mode="json")
        backup.write_to_db(db.kv_store)

        with pytest.raises(PreconditionError, match="somewhere-else"):
            backup_controller.write_manifest(backup)
