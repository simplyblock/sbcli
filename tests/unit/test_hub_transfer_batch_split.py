"""Test-only split of the transfer batch among the members of a migration
group, which share one transfer hub (LVOL_MIG_SPLIT_TRANSFER_BATCH_BY_MEMBERS)."""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants, utils
from simplyblock_core.services import tasks_runner_lvol_migration as runner


@pytest.mark.parametrize("members, expected", [
    (None, 256), (0, 256), (1, 256), (2, 128), (5, 51), (256, 1), (1000, 1),
])
def test_batch_is_divided_among_members(members, expected):
    assert utils.hub_transfer_batch_size(members) == expected


def test_split_off_keeps_the_full_batch(monkeypatch):
    monkeypatch.setattr(constants, "LVOL_MIG_SPLIT_TRANSFER_BATCH_BY_MEMBERS", False)
    assert utils.hub_transfer_batch_size(5) == constants.LVOL_MIG_TRANSFER_BATCH_SIZE


def test_the_hub_total_never_exceeds_one_batch():
    for members in range(1, 20):
        assert members * utils.hub_transfer_batch_size(members) <= constants.LVOL_MIG_TRANSFER_BATCH_SIZE


def test_member_count_of_a_single_migration_is_one():
    assert runner._group_member_count(None) == 1
    assert runner._group_member_count(MagicMock(migration_group_id="")) == 1


def test_member_count_of_a_group_member():
    group = MagicMock()
    group.member_count.return_value = 5
    with patch.object(runner, "db") as db:
        db.get_migration_group_by_id.return_value = group
        assert runner._group_member_count(MagicMock(migration_group_id="g1")) == 5


def test_a_missing_group_falls_back_to_one():
    with patch.object(runner, "db") as db:
        db.get_migration_group_by_id.side_effect = KeyError("gone")
        assert runner._group_member_count(MagicMock(migration_group_id="g1")) == 1
