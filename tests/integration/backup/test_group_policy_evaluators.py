"""The consistency-group backup-policy evaluators against real FoundationDB
(design-consistency-group-backup.md §9).

These are the integration-tier counterpart of the pure-logic
``tests/unit/backup/test_group_retention.py``: there the retention arithmetic is
tested in isolation, here the whole flow runs -- policy resolution through the
``BackupPolicyAttachment`` index, the group-backup listing, and the schedule /
retention decision -- for real against FDB.

The database is real; nothing here mocks the DB layer. The only boundary patched
is the data plane: ``group_backup.create_group_backup`` and
``group_backup.delete_group_backup`` are what reach snapshots, the backup runner,
and S3, which the integration tier never drives to a real endpoint. Both return a
``(result, error)`` tuple, so the mocks return one too -- the evaluators unpack it.
"""
import time
import zlib
from unittest.mock import patch

import pytest

from simplyblock_core.controllers.backup import policy
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import (
    BackupPolicy,
    BackupPolicyAttachment,
    GroupBackup,
)
from simplyblock_core.models.replication import ConsistencyGroup


def _uuid(name: str) -> str:
    # A deterministic, valid UUID string, matching the sibling group-backup
    # suites. Nothing in the evaluators converts these ids with UUID(...) -- the
    # data-plane boundary that would is patched -- but keeping real UUIDs matches
    # the fixture style and stays safe if a path ever does.
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")
CREATE = "simplyblock_core.controllers.backup.group_backup.create_group_backup"
DELETE = "simplyblock_core.controllers.backup.group_backup.delete_group_backup"


@pytest.fixture
def db():
    return DBController()


def _group(db):
    group = ConsistencyGroup()
    group.uuid = _uuid("cg")
    group.cluster_id = CLUSTER
    group.group_name = "gitea"
    group.write_to_db(db.kv_store)
    return group


def _policy(db, max_versions=0, max_age_seconds=0, schedule=""):
    p = BackupPolicy()
    p.uuid = _uuid("policy")
    p.cluster_id = CLUSTER
    p.policy_name = "nightly"
    p.max_versions = max_versions
    p.max_age_seconds = max_age_seconds
    p.backup_schedule = schedule
    p.status = BackupPolicy.STATUS_ACTIVE
    p.write_to_db(db.kv_store)
    return p


def _attach(db, group, p):
    att = BackupPolicyAttachment()
    att.uuid = _uuid("attachment")
    att.cluster_id = CLUSTER
    att.policy_id = p.get_id()
    att.target_type = "consistency_group"
    att.target_id = group.get_id()
    att.write_to_db(db.kv_store)
    return att


def _group_backup(db, group, name, age_seconds, status=GroupBackup.STATUS_COMPLETED):
    """A group backup of ``group`` created ``age_seconds`` ago."""
    gb = GroupBackup()
    gb.uuid = _uuid(name)
    gb.cluster_id = CLUSTER
    gb.consistency_group_id = group.get_id()
    gb.consistency_group_name = group.group_name
    gb.group_seq = 1
    gb.created_at = int(time.time()) - age_seconds
    gb.status = status
    gb.write_to_db(db.kv_store)
    return gb


class TestGroupSchedule:
    """evaluate_group_schedule: take a generation when due, prune the excess."""

    def test_no_prior_group_backup_takes_one(self, db):
        group = _group(db)
        _attach(db, group, _policy(db, schedule="15m,4 60m,11"))

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_schedule(group)

        create.assert_called_once_with(group.get_id())
        delete.assert_not_called()

    def test_recent_group_backup_within_interval_takes_none(self, db):
        group = _group(db)
        _attach(db, group, _policy(db, schedule="15m,4"))
        # One completed generation a minute ago: well inside the 15m smallest
        # tier, so nothing is due, and one generation is under the keep_count.
        _group_backup(db, group, "gb-recent", age_seconds=60)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_schedule(group)

        create.assert_not_called()
        delete.assert_not_called()

    def test_in_flight_group_backup_takes_none(self, db):
        group = _group(db)
        _attach(db, group, _policy(db, schedule="15m,4"))
        # A generation still committing gates a second one, even though the
        # smallest interval has long elapsed (created an hour ago).
        _group_backup(db, group, "gb-flight", age_seconds=3600,
                      status=GroupBackup.STATUS_IN_PROGRESS)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_schedule(group)

        create.assert_not_called()
        delete.assert_not_called()

    def test_tier_over_keep_count_prunes_the_oldest(self, db):
        group = _group(db)
        # Smallest tier keeps 2; its age band is [0, 3600). Four generations all
        # under a minute old sit in that band -- the latest is recent enough that
        # nothing is due, so the evaluator reaches the pruning pass.
        _attach(db, group, _policy(db, schedule="15m,2 60m,11"))
        oldest = _group_backup(db, group, "gb-1", age_seconds=400)
        _group_backup(db, group, "gb-2", age_seconds=300)
        _group_backup(db, group, "gb-3", age_seconds=200)
        _group_backup(db, group, "gb-4", age_seconds=100)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_schedule(group)

        create.assert_not_called()
        # One delete per cycle, and it is the oldest generation in the tier.
        delete.assert_called_once_with(oldest.get_id())


class TestGroupPolicy:
    """evaluate_group_policy: count/age retention, whole generations deleted."""

    def test_over_max_versions_deletes_the_oldest_excess(self, db):
        group = _group(db)
        _attach(db, group, _policy(db, max_versions=2))
        gb_a = _group_backup(db, group, "gb-a", age_seconds=400)
        gb_b = _group_backup(db, group, "gb-b", age_seconds=300)
        _group_backup(db, group, "gb-c", age_seconds=200)
        _group_backup(db, group, "gb-d", age_seconds=100)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_policy(group)

        create.assert_not_called()
        # Keep the newest two of four: the two oldest are deleted, oldest first.
        assert [c.args[0] for c in delete.call_args_list] == [gb_a.get_id(), gb_b.get_id()]

    def test_older_than_max_age_is_deleted(self, db):
        group = _group(db)
        _attach(db, group, _policy(db, max_age_seconds=100))
        old = _group_backup(db, group, "gb-old", age_seconds=1000)
        _group_backup(db, group, "gb-recent", age_seconds=10)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_policy(group)

        create.assert_not_called()
        delete.assert_called_once_with(old.get_id())


class TestNoAttachedPolicy:
    """A group with no attached policy: both evaluators are a no-op."""

    def test_neither_evaluator_acts(self, db):
        group = _group(db)
        # A policy exists in the cluster but is NOT attached to this group, and
        # there are generations that would be pruned if one were.
        _policy(db, max_versions=1, schedule="15m,1")
        _group_backup(db, group, "gb-1", age_seconds=400)
        _group_backup(db, group, "gb-2", age_seconds=100)

        with patch(CREATE, return_value=(None, None)) as create, \
                patch(DELETE, return_value=(True, None)) as delete:
            policy.evaluate_group_schedule(group)
            policy.evaluate_group_policy(group)

        create.assert_not_called()
        delete.assert_not_called()
