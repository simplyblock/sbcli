"""Which group backups a count/age policy prunes (design-consistency-group-backup.md §9).

Pure logic -- ``evaluate_group_backups`` around it touches the database and the
data plane and is the integration tier's. Retention is per generation: a
group backup is the unit kept or deleted. Its member copies are retired under
it: merged into the next generation's copy while that chains against them,
deleted once nothing does (TestOrphanedMemberBackups).
"""
from types import SimpleNamespace

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import group_backup, policy
from simplyblock_core.models.backup import Backup, GroupBackup


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
        doomed = policy._group_backups_to_prune(completed, _policy(max_versions=2), [], NOW)
        assert [g.uuid for g in doomed] == ["a", "b"]

    def test_within_the_limit_prunes_nothing(self):
        completed = [_gb("a", NOW - 200), _gb("b", NOW - 100)]
        assert policy._group_backups_to_prune(completed, _policy(max_versions=2), [], NOW) == []

    def test_zero_versions_is_no_count_limit(self):
        completed = [_gb("a", NOW - 200), _gb("b", NOW - 100)]
        assert policy._group_backups_to_prune(completed, _policy(max_versions=0), [], NOW) == []


class TestMaxAge:
    def test_prunes_those_older_than_the_age(self):
        completed = [_gb("old", NOW - 1000), _gb("recent", NOW - 10)]
        doomed = policy._group_backups_to_prune(completed, _policy(max_age_seconds=100), [], NOW)
        assert [g.uuid for g in doomed] == ["old"]


class TestBoth:
    def test_count_and_age_union_without_double_listing(self):
        # a and b exceed the count (keep 2 of 4); a is also past the age. a must
        # appear once, not twice.
        completed = [_gb("a", NOW - 1000), _gb("b", NOW - 300),
                     _gb("c", NOW - 200), _gb("d", NOW - 100)]
        doomed = policy._group_backups_to_prune(
            completed, _policy(max_versions=2, max_age_seconds=500), [], NOW)
        assert [g.uuid for g in doomed] == ["a", "b"]


class TestTiers:
    """A schedule's tiers keep ``keep_count`` per age band. One generation per
    pass: the oldest of the first band over its count."""

    TIERS = ((900, 2), (3600, 11))          # "15m,2 60m,11"

    def test_the_oldest_of_a_band_over_its_count(self):
        completed = [_gb("a", NOW - 400), _gb("b", NOW - 300), _gb("c", NOW - 200)]
        doomed = policy._group_backups_to_prune(completed, _policy(), self.TIERS, NOW)
        assert [g.uuid for g in doomed] == ["a"]

    def test_a_band_within_its_count_selects_nothing(self):
        completed = [_gb("a", NOW - 300), _gb("b", NOW - 200)]
        assert policy._group_backups_to_prune(completed, _policy(), self.TIERS, NOW) == []

    def test_the_tiers_and_the_count_are_one_union(self):
        # The band keeps 2 of 3 (selects a); the count keeps 1 of 3 (a and b).
        completed = [_gb("a", NOW - 400), _gb("b", NOW - 300), _gb("c", NOW - 200)]
        doomed = policy._group_backups_to_prune(
            completed, _policy(max_versions=1), self.TIERS, NOW)
        assert [g.uuid for g in doomed] == ["a", "b"]


def _backup(uuid, lvol, prev="", group="gone", status=Backup.STATUS_COMPLETED, created_at=0):
    b = Backup()
    b.uuid, b.lvol_id, b.prev_backup_id = uuid, lvol, prev
    b.group_backup_id, b.status, b.created_at = group, status, created_at
    return b


class _DB:
    """Backups by volume, and the group backups that still exist."""

    def __init__(self, backups, group_backups=()):
        self._backups = list(backups)
        self._groups = set(group_backups)

    def get_backups_by_lvol_id(self, lvol_id):
        return [b for b in self._backups if b.lvol_id == lvol_id]

    def get_group_backup_by_id(self, group_backup_id):
        if group_backup_id not in self._groups:
            raise KeyError(group_backup_id)
        return SimpleNamespace(uuid=group_backup_id)


class TestOrphanedMemberBackups:
    """A pruned generation's member copies must not outlive it in the bucket:
    with a 5m,2 schedule the oldest generation's three full copies stayed in
    `backup list` for good while the chain grew under them (live 2026-10-20)."""

    def _retire(self, monkeypatch, db, lvols=("v1",)):
        merged, deleted = [], []
        monkeypatch.setattr(backup_controller, "trigger_merge", lambda keep, old: merged.append((old.uuid, keep.uuid)))
        monkeypatch.setattr(backup_controller, "delete_backup", deleted.append)
        result = group_backup.retire_orphaned_member_backups(list(lvols), db=db)
        return result, merged, deleted

    def test_a_base_the_next_generation_chains_against_is_merged_into_it(self, monkeypatch):
        db = _DB([_backup("g9", "v1", created_at=1),
                  _backup("g10", "v1", prev="g9", group="kept", created_at=2)], group_backups={"kept"})
        (merges, dels), merged, deleted = self._retire(monkeypatch, db)
        assert merged == [("g9", "g10")] and deleted == []
        assert (merges, dels) == (1, 0)

    def test_a_leaf_nothing_chains_against_is_deleted(self, monkeypatch):
        db = _DB([_backup("g9", "v1", created_at=1)])
        (merges, dels), merged, deleted = self._retire(monkeypatch, db)
        assert deleted == ["g9"] and merged == []
        assert (merges, dels) == (0, 1)

    def test_a_member_of_a_living_group_backup_is_left_alone(self, monkeypatch):
        db = _DB([_backup("g10", "v1", group="kept", created_at=2)], group_backups={"kept"})
        _, merged, deleted = self._retire(monkeypatch, db)
        assert merged == [] and deleted == []

    def test_one_merge_per_chain_oldest_first(self, monkeypatch):
        """Two pruned generations at once: the base merges first; the next cycle
        takes the rest, as the single-volume schedule paces its merges."""
        db = _DB([_backup("g8", "v1", created_at=1),
                  _backup("g9", "v1", prev="g8", created_at=2),
                  _backup("g10", "v1", prev="g9", group="kept", created_at=3)], group_backups={"kept"})
        _, merged, deleted = self._retire(monkeypatch, db)
        assert merged == [("g8", "g9")] and deleted == []

    def test_a_chain_with_a_merge_in_flight_is_skipped(self, monkeypatch):
        db = _DB([_backup("g8", "v1", status=Backup.STATUS_MERGING, created_at=1),
                  _backup("g9", "v1", prev="g8", created_at=2),
                  _backup("g10", "v1", prev="g9", group="kept", created_at=3)], group_backups={"kept"})
        _, merged, deleted = self._retire(monkeypatch, db)
        assert merged == [] and deleted == []

    def test_a_successor_still_uploading_defers_the_merge(self, monkeypatch):
        db = _DB([_backup("g9", "v1", created_at=1),
                  _backup("g10", "v1", prev="g9", group="kept",
                          status=Backup.STATUS_IN_PROGRESS, created_at=2)], group_backups={"kept"})
        _, merged, deleted = self._retire(monkeypatch, db)
        assert merged == [] and deleted == []

    def test_each_member_volume_is_retired_independently(self, monkeypatch):
        db = _DB([_backup("a9", "v1", created_at=1), _backup("a10", "v1", prev="a9", group="kept", created_at=2),
                  _backup("b9", "v2", created_at=1)], group_backups={"kept"})
        _, merged, deleted = self._retire(monkeypatch, db, lvols=("v1", "v2"))
        assert merged == [("a9", "a10")] and deleted == ["b9"]
