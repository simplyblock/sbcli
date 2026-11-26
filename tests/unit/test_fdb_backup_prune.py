"""FDB backup retention actually deletes backups older than the retention.

Two faults kept ``prune_old_backups`` from ever deleting anything. It asked
``list_backups`` for the rendered table instead of the data, so the list
arrived as a string and the function bailed out on its own type check. And
the cutoff was computed with ``replace(day=day - retention_days)``, which
raises ``ValueError`` on the first ``retention_days`` days of every month.
"""
# The pruner compares naive local datetimes (datetime.now() against a
# strptime() without a zone), so the fixtures are naive on purpose.
# ruff: noqa: DTZ001
import datetime as _dt
from unittest import mock

import pytest

from simplyblock_core.services import tasks_runner_fdb_backup as runner


class _Cluster:
    backup_retention_days = 7

    @staticmethod
    def get_backup_path(name=""):
        return f"file:///etc/foundationdb/backup/{name}"


def _item(name, when):
    return {"Name": name, "Date": when.strftime("%Y-%m-%d %H:%M:%S")}


@pytest.fixture
def prune(monkeypatch):
    """Wire the pruner to fakes and return (list_backups mock, backup_delete mock)."""
    list_backups = mock.MagicMock()
    backup_delete = mock.MagicMock()
    monkeypatch.setattr(runner.fdb_backup_controller, "list_backups", list_backups)
    monkeypatch.setattr(runner.fdb_backup_controller, "backup_delete", backup_delete)
    monkeypatch.setattr(runner.db, "get_cluster_by_id", lambda _cid: _Cluster())
    return list_backups, backup_delete


def _freeze_now(monkeypatch, now: _dt.datetime):
    class _Frozen(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(runner, "datetime", _Frozen)


def test_asks_for_data_not_a_rendered_table(prune):
    list_backups, _ = prune
    list_backups.return_value = []

    runner.prune_old_backups("cluster-1")

    list_backups.assert_called_once_with("cluster-1", is_json=True)


def test_deletes_only_backups_older_than_retention(prune, monkeypatch):
    list_backups, backup_delete = prune
    now = _dt.datetime(2026, 11, 20, 12, 0, 0)
    _freeze_now(monkeypatch, now)
    list_backups.return_value = [
        _item("backup-old", now - _dt.timedelta(days=8)),
        _item("backup-edge", now - _dt.timedelta(days=6, hours=23)),
        _item("backup-new", now - _dt.timedelta(hours=3)),
    ]

    runner.prune_old_backups("cluster-1")

    backup_delete.assert_called_once_with("file:///etc/foundationdb/backup/backup-old")


def test_cutoff_crosses_the_month_boundary(prune, monkeypatch):
    """On the 3rd with 7-day retention the old code raised ValueError
    (day=-4); the cutoff must be the 27th of the previous month."""
    list_backups, backup_delete = prune
    _freeze_now(monkeypatch, _dt.datetime(2026, 11, 3, 8, 0, 0))
    list_backups.return_value = [
        _item("backup-oct-26", _dt.datetime(2026, 10, 26, 23, 0, 0)),
        _item("backup-oct-28", _dt.datetime(2026, 10, 28, 2, 0, 0)),
    ]

    runner.prune_old_backups("cluster-1")

    backup_delete.assert_called_once_with("file:///etc/foundationdb/backup/backup-oct-26")
