"""Which group backups a count/age policy prunes (design-consistency-group-backup.md §9).

Pure logic -- the schedule and retention evaluators around it touch the database
and the data plane and are the integration tier's. Retention is per generation: a
group backup is the unit kept or deleted, never merged.
"""
from types import SimpleNamespace

from simplyblock_core.controllers.backup import policy
from simplyblock_core.models.backup import GroupBackup


def _gb(uuid, created_at):
    gb = GroupBackup()
    gb.uuid = uuid
    gb.created_at = created_at
    return gb


def _policy(max_versions=0, max_age_seconds=0):
    return SimpleNamespace(max_versions=max_versions, max_age_seconds=max_age_seconds)


NOW = 1_000_000


class TestMaxVersions:
    def test_keeps_the_newest_and_prunes_the_oldest(self):
        completed = [_gb("a", NOW - 400), _gb("b", NOW - 300),
                     _gb("c", NOW - 200), _gb("d", NOW - 100)]
        doomed = policy._group_backups_to_prune(completed, _policy(max_versions=2), NOW)
        assert [g.uuid for g in doomed] == ["a", "b"]

    def test_within_the_limit_prunes_nothing(self):
        completed = [_gb("a", NOW - 200), _gb("b", NOW - 100)]
        assert policy._group_backups_to_prune(completed, _policy(max_versions=2), NOW) == []

    def test_zero_versions_is_no_count_limit(self):
        completed = [_gb("a", NOW - 200), _gb("b", NOW - 100)]
        assert policy._group_backups_to_prune(completed, _policy(max_versions=0), NOW) == []


class TestMaxAge:
    def test_prunes_those_older_than_the_age(self):
        completed = [_gb("old", NOW - 1000), _gb("recent", NOW - 10)]
        doomed = policy._group_backups_to_prune(completed, _policy(max_age_seconds=100), NOW)
        assert [g.uuid for g in doomed] == ["old"]


class TestBoth:
    def test_count_and_age_union_without_double_listing(self):
        # a and b exceed the count (keep 2 of 4); a is also past the age. a must
        # appear once, not twice.
        completed = [_gb("a", NOW - 1000), _gb("b", NOW - 300),
                     _gb("c", NOW - 200), _gb("d", NOW - 100)]
        doomed = policy._group_backups_to_prune(
            completed, _policy(max_versions=2, max_age_seconds=500), NOW)
        assert [g.uuid for g in doomed] == ["a", "b"]
