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
from simplyblock_core.controllers.backup.chain import BackupChain
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool


# Every id a manifest carries is a UUID, so the objects these tests build are
# given real ones rather than readable stand-ins.
CLUSTER_ID = "c1000000-0000-4000-8000-000000000001"
OTHER_CLUSTER_ID = "c1000000-0000-4000-8000-000000000002"
POOL_ID = "b0010000-0000-4000-8000-000000000001"
LVOL_ID = "10101000-0000-4000-8000-000000000001"
NODE_ID = "d0de0000-0000-4000-8000-000000000001"

BUCKET = "simplyblock-backup-primary"


def _backup_id(index: int) -> str:
    return f"0ac00000-0000-4000-8000-{index:012d}"


def _snapshot_id(index: int) -> str:
    return f"50a50000-0000-4000-8000-{index:012d}"


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": BUCKET,
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
    pool.uuid = POOL_ID
    pool.pool_name = "testpool"
    pool.cluster_id = CLUSTER_ID
    pool.write_to_db(db.kv_store)

    volume = LVol()
    volume.uuid = LVOL_ID
    volume.lvol_name = "vol"
    volume.pool_uuid = POOL_ID
    volume.pool_name = "testpool"
    volume.node_id = NODE_ID
    volume.size = 4096
    volume.ha_type = "ha"
    volume.fabric = "tcp"
    volume.rw_ios_per_sec = 5000
    volume.max_size = 8192
    volume.write_to_db(db.kv_store)
    return volume


def _encryption(backup_id):
    """What `_build_key_descriptor` records for a cluster without Vault."""
    return {
        "type": "fdb",
        "dek_path": f"cluster/{CLUSTER_ID}/backup/{backup_id}",
    }


def _backup(db, index, prev=None, **overrides):
    """A completed backup, addressed by index rather than by uuid.

    The index feeds the uuid, the s3_id and the snapshot id together, so a chain
    reads as `_backup(db, 2, prev=1)` instead of three parallel UUID literals.
    """
    b = Backup()
    b.uuid = _backup_id(index)
    b.s3_id = index
    b.cluster_id = CLUSTER_ID
    b.lvol_id = LVOL_ID
    b.lvol_name = "vol"
    b.snapshot_id = _snapshot_id(index)
    b.snapshot_name = f"snap_{index}"
    b.node_id = NODE_ID
    b.pool_uuid = POOL_ID
    b.prev_backup_id = _backup_id(prev) if prev is not None else ""
    b.size = 4096
    b.created_at = 1000 + index
    b.completed_at = 2000 + index
    b.allowed_hosts = [{"nqn": "nqn.2024-01.io.test:host"}]
    b.status = Backup.STATUS_COMPLETED
    b.location = _config().location().model_dump(mode="json")
    for key, value in overrides.items():
        setattr(b, key, value)
    b.write_to_db(db.kv_store)
    return b


class TestBuildManifest:

    def test_says_nothing_about_where_the_bucket_is(self, db, cluster, lvol):
        """Whoever reads a manifest supplied the bucket to read it, and a stored
        copy could only go stale -- a replicated bucket would hand out manifests
        naming the original."""
        manifest = backup_controller.build_manifest(_backup(db, 1))

        rendered = manifest.model_dump_json()
        assert BUCKET not in rendered
        assert "eu-central-1" not in rendered

    def test_records_only_the_immediate_predecessor(self, db, cluster, lvol):
        """Not the whole chain: storing that would make a merge rewrite the
        manifest of every descendant of the backup it folded away."""
        _backup(db, 1)
        _backup(db, 2, prev=1)
        third = _backup(db, 3, prev=2)

        manifest = backup_controller.build_manifest(third)

        assert str(manifest.prev_backup_id) == _backup_id(2)

    def test_a_full_backup_has_no_predecessor(self, db, cluster, lvol):
        """Absent rather than "", so a chain root is a state and not a blank."""
        assert backup_controller.build_manifest(_backup(db, 1)).prev_backup_id is None

    def test_the_chain_reconstructs_from_the_links(self, db, cluster, lvol):
        """What the stored chain was for, done from the bucket's own contents."""
        _backup(db, 1)
        _backup(db, 2, prev=1)
        _backup(db, 3, prev=2)
        manifests = [backup_controller.build_manifest(b) for b in db.get_backups()]
        last = next(m for m in manifests if str(m.backup_id) == _backup_id(3))

        chain = BackupChain.of_manifests(last, manifests, _config().location())

        assert [(str(m.backup_id), m.s3_id) for m in chain.links] == [
            (_backup_id(1), 1), (_backup_id(2), 2), (_backup_id(3), 3)]

    def test_records_the_volume_shape(self, db, cluster, lvol):
        """Restore currently hardcodes these; recording them is what lets it stop."""
        manifest = backup_controller.build_manifest(_backup(db, 1))

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
        backup = _backup(db, 1, allowed_hosts=[{
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
        backup = _backup(db, 1)
        lvol.remove(db.kv_store)

        manifest = backup_controller.build_manifest(backup)

        assert manifest.volume.lvol_name == "vol"    # carried on the backup itself
        assert manifest.volume.pool_name is None     # only on the volume
        # Absent, not 0 -- which for a QoS cap would read as "unlimited".
        assert manifest.volume.rw_ios_per_sec is None

    def test_records_source_as_provenance(self, db, cluster, lvol):
        manifest = backup_controller.build_manifest(_backup(db, 1))

        assert str(manifest.source.cluster_id) == CLUSTER_ID
        assert manifest.source.cluster_name == "primary"
        assert str(manifest.source.node_id) == NODE_ID

    def test_records_the_object_layout(self, db, cluster, lvol):
        manifest = backup_controller.build_manifest(_backup(db, 1))

        assert manifest.dataplane.key_format == "{s3_id}/{mid}/{extent}"
        assert manifest.dataplane.cluster_size == cluster.page_size_in_blocks

    def test_records_whether_the_objects_are_compressed(self, db, cluster, lvol):
        """The one part of the encoding that varies and that reading the bucket
        cannot reveal."""
        backup = _backup(db, 1)
        backup.location = _config(with_compression=True).location().model_dump(mode="json")

        assert backup_controller.build_manifest(backup).dataplane.with_compression is True

    def test_backup_without_a_location_is_refused(self, db, cluster, lvol):
        backup = _backup(db, 1)
        backup.location = {}

        with pytest.raises(ValueError):
            backup_controller.build_manifest(backup)

    def test_backup_whose_ids_are_not_uuids_is_refused(self, db, cluster, lvol):
        """The record types its ids as plain strings and so can hold a blank one;
        a manifest that advertised it would name a backup nobody can look up."""
        backup = _backup(db, 1)
        backup.node_id = ""

        with pytest.raises(ValueError):
            backup_controller.build_manifest(backup)


class TestExportImportRoundTrip:
    """Export, wipe the database, import -- the disaster-recovery shape."""

    def test_backups_survive_losing_the_database(self, db, cluster, lvol):
        # Encrypted throughout: one key decrypts the whole chain, so a line whose
        # root is plaintext and whose child is not could never be restored, and
        # is refused rather than round-tripped.
        _backup(db, 1, encrypted=True, encryption=_encryption(_backup_id(1)))
        _backup(db, 2, prev=1, encrypted=True,
                encryption=_encryption(_backup_id(2)))

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        for backup in db.get_backups():
            backup.remove(db.kv_store)
        assert db.get_backups() == []

        count = backup_controller.import_backups(
            exported, _config().location(), cluster_id=OTHER_CLUSTER_ID)

        assert count == 2
        restored = db.get_backup_by_id(_backup_id(2))
        assert restored.s3_id == 2
        assert restored.prev_backup_id == _backup_id(1)
        assert restored.cluster_id == OTHER_CLUSTER_ID
        assert restored.get_location().bucket_name == BUCKET
        assert restored.encrypted is True

    def test_encrypted_flag_survives(self, db, cluster, lvol):
        """It used to be dropped, restoring a plaintext volume over ciphertext."""
        _backup(db, 1, encrypted=True, encryption=_encryption(_backup_id(1)))
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id(_backup_id(1)).remove(db.kv_store)

        backup_controller.import_backups(
            exported, _config().location(), cluster_id=OTHER_CLUSTER_ID)

        assert db.get_backup_by_id(_backup_id(1)).encrypted is True

    def test_export_emits_the_same_shape_as_the_bucket(self, db, cluster, lvol):
        """One format, so a file and a bucket read are interchangeable."""
        _backup(db, 1)

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        assert backup_manifest._parse(
            exported[0].model_dump_json().encode(), "k") == exported[0]

    def test_only_completed_backups_are_exported(self, db, cluster, lvol):
        _backup(db, 1)
        _backup(db, 2, status=Backup.STATUS_FAILED)

        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        assert [str(m.backup_id) for m in exported] == [_backup_id(1)]

    def test_a_malformed_entry_never_reaches_the_controller(self, db, cluster, lvol):
        """import_backups takes manifests, not dicts, so an unusable entry is
        rejected by whoever read the bytes -- naming the file or the request."""
        with pytest.raises(ValueError):
            backup_manifest.BackupManifest.model_validate(
                {"backup_id": _backup_id(9), "s3_id": "not-an-int"})

    def test_duplicate_id_rejects_the_whole_batch(self, db, cluster, lvol):
        _backup(db, 1)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)

        with pytest.raises(PreconditionError, match="already exists"):
            backup_controller.import_backups(
                exported, _config().location(), cluster_id=OTHER_CLUSTER_ID)

    def test_same_id_listed_twice_rejects_the_whole_batch(self, db, cluster, lvol):
        """Backup lookups are not cluster-scoped, so a reused uuid unaddresses both."""
        _backup(db, 1)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id(_backup_id(1)).remove(db.kv_store)

        with pytest.raises(ValueError, match="listed more than once"):
            backup_controller.import_backups(
                exported + exported, _config().location(), cluster_id=OTHER_CLUSTER_ID)

        assert db.get_backups() == []

    def test_import_records_the_bucket_it_was_read_from(self, db, cluster, lvol):
        """Replicate a bucket and its manifests are unchanged -- they describe
        objects, not a location. So the copy imports as itself, rather than as
        the original it would then try and fail to restore from."""
        _backup(db, 1)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id(_backup_id(1)).remove(db.kv_store)

        backup_controller.import_backups(
            exported, _config(bucket_name="dr-copy", region="us-east-1").location(),
            cluster_id=OTHER_CLUSTER_ID)

        location = db.get_backup_by_id(_backup_id(1)).get_location()
        assert location.bucket_name == "dr-copy"
        assert location.region == "us-east-1"

    def test_import_keeps_the_encoding_the_manifest_states(self, db, cluster, lvol):
        """The bucket says where; the manifest says how the bodies are encoded."""
        backup = _backup(db, 1)
        backup.location = _config(with_compression=True).location().model_dump(mode="json")
        backup.write_to_db(db.kv_store)
        exported = backup_controller.export_backups(cluster_id=CLUSTER_ID)
        db.get_backup_by_id(_backup_id(1)).remove(db.kv_store)

        backup_controller.import_backups(
            exported, _config(bucket_name="dr-copy").location(),
            cluster_id=OTHER_CLUSTER_ID)

        assert db.get_backup_by_id(_backup_id(1)).get_location().with_compression is True

    def test_nothing_to_import_is_not_an_error(self, db, cluster, lvol):
        assert backup_controller.import_backups(
            [], _config().location(), cluster_id=OTHER_CLUSTER_ID) == 0


class TestBucketDiscovery:
    """The path that needs nothing but a bucket and credentials."""

    def test_discover_reads_every_manifest(self, db, cluster, lvol):
        _backup(db, 1)
        _backup(db, 2, prev=1)
        manifests = [backup_controller.build_manifest(b) for b in db.get_backups()]

        with patch.object(backup_manifest, "list_all", return_value=manifests):
            found = backup_controller.discover_backups(_config())

        assert {str(m.backup_id) for m in found} == {_backup_id(1), _backup_id(2)}
        # Models, not dicts: rendering them is the caller's decision.
        assert all(isinstance(m, backup_manifest.BackupManifest) for m in found)

    def test_import_from_bucket_needs_no_prior_records(self, db, cluster, lvol):
        _backup(db, 1)
        manifests = [backup_controller.build_manifest(
            db.get_backup_by_id(_backup_id(1)))]
        db.get_backup_by_id(_backup_id(1)).remove(db.kv_store)

        with patch.object(backup_manifest, "list_all", return_value=manifests):
            count = backup_controller.import_from_bucket(
                _config(), cluster_id=OTHER_CLUSTER_ID)

        assert count == 1
        assert db.get_backup_by_id(_backup_id(1)).cluster_id == OTHER_CLUSTER_ID


class TestManifestPublication:

    def test_write_manifest_puts_the_object_in_the_backup_bucket(self, db, cluster, lvol):
        backup = _backup(db, 1)

        with patch.object(backup_manifest, "s3_client") as mock_client:
            backup_controller.write_manifest(backup)

        _, kwargs = mock_client.return_value.put_object.call_args
        assert kwargs["Bucket"] == BUCKET
        assert kwargs["Key"] == f"manifests/{_backup_id(1)}.json"

    def test_refuses_when_the_cluster_points_at_another_bucket(self, db, cluster, lvol):
        """The cluster's credentials cannot be assumed to reach a foreign bucket."""
        backup = _backup(db, 1)
        backup.location = _config(bucket_name="somewhere-else").location().model_dump(mode="json")
        backup.write_to_db(db.kv_store)

        with pytest.raises(PreconditionError, match="somewhere-else"):
            backup_controller.write_manifest(backup)
