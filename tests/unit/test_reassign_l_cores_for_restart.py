# coding=utf-8
"""reassign_l_cores_for_restart(): restart-time index->physical placement.

A restart must never change a role's INDEX SET (its core count, and any
sharing with another role at the same index) -- that was decided once, at
add time (see test_restart_does_not_rederive_core_roles.py). What legitimately
changes across a restart is which physical core the OS/k8s CPU manager hands
back for each index. This function chooses that mapping so that
distrib/poller/alceml -- in that priority order, matching the product's
existing allocation priority -- get first claim on any intact hyperthread
sibling pairs present in the fresh cpuset, using the real sysfs topology
(parse_thread_siblings) rather than calculate_core_allocations' machine-wide
pair_hyperthreads() guess.
"""
from unittest.mock import patch

from simplyblock_core import utils

# A 40-logical-CPU host, siblings at i / i+20 (the common low-half/high-half
# hyperthread numbering convention).
SIBLINGS_40 = {i: sorted([i, i + 20]) for i in range(20)}
SIBLINGS_40.update({i + 20: sorted([i, i + 20]) for i in range(20)})


def _reassign(cores, distrib, poller, alceml, siblings=SIBLINGS_40):
    with patch("simplyblock_core.utils.parse_thread_siblings", return_value=siblings):
        return utils.reassign_l_cores_for_restart(cores, distrib, poller, alceml)


class TestPreservesShapeAndCompleteness:

    def test_every_index_gets_exactly_one_core_no_duplicates(self):
        cores = list(range(20))
        placement = _reassign(cores, distrib=[0, 1, 2, 3, 4, 5, 6], poller=[7, 8, 9, 10, 11, 12, 13, 14],
                              alceml=[15, 16])
        assert len(placement) == 20
        assert sorted(placement) == sorted(cores), "must be a bijection onto the fresh cpuset"

    def test_role_counts_are_unchanged_by_construction(self):
        """The function only ever fills the index positions it's handed --
        it can't grow or shrink a role's slot count."""
        cores = list(range(10))
        placement = _reassign(cores, distrib=[0, 1], poller=[2, 3, 4], alceml=[5, 6])
        assert len({placement[i] for i in (0, 1)}) == 2
        assert len({placement[i] for i in (2, 3, 4)}) == 3
        assert len({placement[i] for i in (5, 6)}) == 2


class TestSiblingPreferenceAndPriority:

    def test_cpuset_with_intact_pairs_gives_distrib_a_real_sibling_pair(self):
        """The function doesn't promise to reuse the SAME physical cores a
        role had before (that's a stability nice-to-have the caller can add
        on top, e.g. by short-circuiting when the cpuset hasn't changed at
        all) -- only that whatever pair it picks is a REAL sibling pair."""
        isolated = sorted(c for c in range(40) if c % 2 == 1)  # sibling-closed subset
        placement = _reassign(isolated, distrib=[3, 13], poller=[0], alceml=[1])
        a, b = placement[3], placement[13]
        assert SIBLINGS_40[a] == sorted([a, b]), "distrib's pair must be real siblings"

    def test_different_but_whole_core_cpuset_still_pairs_distrib(self):
        """k8s hands back a DIFFERENT 10 physical cores than before, but
        still both hyperthreads of each -- distrib's own two indices must
        still land on a real sibling pair, even though the specific cores
        changed entirely."""
        new_cores = sorted(list(range(5)) + list(range(20, 25)))  # 5 whole cores
        placement = _reassign(new_cores, distrib=[0, 1], poller=[2, 3], alceml=[4])
        a, b = placement[0], placement[1]
        assert {a, b} in ({0, 20}, {1, 21}, {2, 22}, {3, 23}, {4, 24})

    def test_distrib_takes_priority_over_poller_and_alceml(self):
        """Only ONE real sibling pair exists in the fresh cpuset; distrib
        must get it even though poller is asked for first... no, distrib is
        asked for first by priority and must win it."""
        # cores: one true pair (0,20), plus three unrelated singles
        cores = [0, 20, 5, 9, 13]
        siblings = {0: [0, 20], 20: [0, 20], 5: [5], 9: [9], 13: [13]}
        placement = _reassign(cores, distrib=[0, 1], poller=[2, 3], alceml=[4], siblings=siblings)
        assert {placement[0], placement[1]} == {0, 20}, "distrib must claim the only real sibling pair"

    def test_poller_takes_priority_over_alceml(self):
        """Same scarcity setup one priority level down: with only one real
        pair for two multi-core roles that each need one, poller (checked
        first) must be the one that ends up paired, not alceml."""
        cores = [0, 20, 5, 9]
        siblings = {0: [0, 20], 20: [0, 20], 5: [5], 9: [9]}
        placement = _reassign(cores, distrib=[], poller=[0, 1], alceml=[2, 3], siblings=siblings)
        assert {placement[0], placement[1]} == {0, 20}
        assert {placement[2], placement[3]} == {5, 9}

    def test_broken_nonsibling_closed_cpuset_still_completes_without_raising(self):
        """No real sibling pairs at all in the fresh cpuset -- every role
        just gets unpaired singles instead of failing the restart."""
        cores = [1, 3, 5, 7]
        siblings = {c: [c] for c in cores}  # nobody has a sibling present
        placement = _reassign(cores, distrib=[0, 1], poller=[2], alceml=[3], siblings=siblings)
        assert sorted(placement) == sorted(cores)


class TestLeftoverSingleCoreRoles:

    def test_indices_outside_the_three_named_roles_are_still_filled(self):
        """app_thread/jm/jc_singleton/lvol_poller/compression aren't passed
        in by name (sibling-pairing is moot for a 1-core role) -- indices
        they occupy must still end up with a real physical core."""
        cores = list(range(6))
        placement = _reassign(cores, distrib=[0, 1], poller=[2], alceml=[3])
        assert placement[4] is not None and placement[5] is not None
        assert sorted(placement) == sorted(cores)
