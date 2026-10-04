"""s3_id allocation against real FoundationDB.

An s3_id names a backup's object keys in S3 (``{s3_id}/{mid}/{extent}``), and
nothing on the data plane reclaims those objects. Reusing an id therefore aims a
new backup's writes at another backup's keys, so the properties worth pinning are
monotonicity and non-reuse -- not just "returns a number".
"""
import json

import pytest

from simplyblock_core import constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup


@pytest.fixture
def db():
    return DBController()


def _backup(uuid, cluster_id, s3_id):
    backup = Backup()
    backup.uuid = uuid
    backup.cluster_id = cluster_id
    backup.s3_id = s3_id
    backup.status = Backup.STATUS_COMPLETED
    return backup


class TestS3IdAllocation:

    def test_allocations_are_strictly_increasing(self, db):
        allocated = [db.next_s3_id() for _ in range(5)]
        assert allocated == sorted(allocated)
        assert len(set(allocated)) == len(allocated)

    def test_first_allocation_is_usable(self, db):
        """0 is rejected by the data plane (s3_id == 0 -> -EINVAL)."""
        assert db.next_s3_id() > 0

    def test_seeds_above_pre_existing_backups(self, db):
        """Upgrade path: ids handed out by the old max-plus-one allocator must not repeat."""
        _backup("b-legacy", "cl-1", 42).write_to_db(db.kv_store)

        assert db.next_s3_id() > 42

    def test_seeds_above_imported_foreign_backups(self, db):
        """Imported backups keep their originating cluster's ids; seeding ignores cluster scope."""
        _backup("b-foreign", "cl-other", 900).write_to_db(db.kv_store)

        assert db.next_s3_id() > 900

    def test_deleting_a_backup_does_not_recycle_its_id(self, db):
        """The old allocator recycled the top id, aiming new writes at orphaned objects."""
        first = db.next_s3_id()
        backup = _backup("b-1", "cl-1", first)
        backup.write_to_db(db.kv_store)
        backup.remove(db.kv_store)

        assert db.next_s3_id() > first

    def test_exhaustion_is_reported_not_wrapped(self, db):
        """The data plane masks s3_id to 30 bits, so an overflow would silently alias."""
        db.kv_store[DBController._S3_ID_SEQ_KEY] = json.dumps(
            constants.BACKUP_MAX_S3_ID).encode()

        with pytest.raises(ValueError, match="exhausted"):
            db.next_s3_id()

    def test_max_s3_id_matches_the_data_plane_field_width(self):
        """S3_ID_BITS is 30 in spdk_internal/lvolstore.h; a wider value aliases."""
        assert constants.BACKUP_MAX_S3_ID == (1 << 30) - 1
