"""Test-only core layout: the nvmf pollers and the lvol poller group share
one set of cores (constants.MERGE_LVOL_POLLER_WITH_POLLER_CORES)."""

from simplyblock_core import constants, utils
from simplyblock_core.storage_node_ops import merged_poller_cores


def test_union_is_sorted_and_unique():
    assert merged_poller_cores([5, 3, 4], [2]) == [2, 3, 4, 5]
    assert merged_poller_cores([3, 4], [4]) == [3, 4]


def test_either_side_may_be_missing():
    assert merged_poller_cores(None, [2]) == [2]
    assert merged_poller_cores([3], []) == [3]
    assert merged_poller_cores(None, None) == []


def test_both_masks_cover_the_same_cores():
    cores = merged_poller_cores([3, 4], [2])
    assert utils.generate_mask(cores) == "0x1C"


def test_merge_is_on_for_this_test_build():
    assert constants.MERGE_LVOL_POLLER_WITH_POLLER_CORES is True
