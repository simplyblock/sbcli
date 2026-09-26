"""The cadence snapshot groups replicated volumes by consistency-group
membership, so members of one group are snapshotted as a single crash-consistent
generation rather than individually (design-csi-addons-replication.md §14.4). The
grouping is keyed off membership, not a policy flag. Pure logic, no DB.
"""
from simplyblock_core.services import snapshot_monitor as sm


class _L:
    def __init__(self, i):
        self._i = i

    def get_id(self):
        return self._i


def test_members_of_one_group_are_partitioned_together():
    a, b, c = _L("a"), _L("b"), _L("c")
    grouped, ungrouped = sm.partition_by_group([(a, "cl/g1"), (b, "cl/g1"), (c, "")])
    assert grouped == {"cl/g1": [a, b]}
    assert ungrouped == [c]


def test_distinct_groups_stay_separate():
    a, b = _L("a"), _L("b")
    grouped, ungrouped = sm.partition_by_group([(a, "cl/g1"), (b, "cl/g2")])
    assert grouped == {"cl/g1": [a], "cl/g2": [b]}
    assert ungrouped == []


def test_ungrouped_volumes_snapshot_individually():
    a, b = _L("a"), _L("b")
    grouped, ungrouped = sm.partition_by_group([(a, ""), (b, "")])
    assert grouped == {}
    assert ungrouped == [a, b]
