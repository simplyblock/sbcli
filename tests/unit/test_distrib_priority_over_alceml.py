"""calculate_core_allocations() gives distrib/poller priority over alceml.

Previously alceml claimed its cores (scaled by the node's actual device
count) before distrib/poller ever saw the budget, so a node with many
devices could starve distrib down to almost nothing even on a host with
plenty of vCPUs -- distrib's share was accidental, not a capacity decision.
Below 22 vCPUs this never actually mattered: alceml's share there was
already a fixed constant (1 core under 12 vCPUs, 2 from 12-21), never
scaled by the device count, so nothing changes in that range. At 22+
vCPUs, where alceml really did scale with the device count, distrib now
claims its share first, as a pure function of vCPU count; alceml takes
its real device-scaled count from whatever's left (clipped if there
genuinely isn't room); poller -- already the "whatever's left" role --
absorbs the true remainder.

Below 6 vCPUs there's no room left for the general formula's role
co-location choices to make sense at all, so 2-5 vCPU hosts get literal,
hand-specified layouts instead of a derived split.
"""

from typing import ClassVar
from unittest.mock import patch

from simplyblock_core import utils

_FIELDS = ("app_thread_core", "jm_cpu_core", "poller_cpu_cores", "alceml_cpu_cores",
          "alceml_worker_cpu_cores", "distrib_cpu_cores", "jc_singleton_core",
          "lvol_poller_core", "compression_core")


def _calc(vcpu_list, alceml_count=2):
    with patch("simplyblock_core.utils.is_hyperthreading_enabled_via_siblings", return_value=False):
        result = utils.calculate_core_allocations(vcpu_list, alceml_count=alceml_count)
    return dict(zip(_FIELDS, result))


class TestTinyNodeLayouts:
    """2-5 vCPU hosts: every role has to double up somewhere, so these are
    the literal layouts the product wants, not a derived split."""

    def test_two_vcpus(self):
        assigned = _calc([0, 1])
        assert assigned["app_thread_core"] == [0]
        assert assigned["jc_singleton_core"] == [0]
        assert assigned["jm_cpu_core"] == [0]
        assert assigned["lvol_poller_core"] == [0]
        assert assigned["alceml_cpu_cores"] == [0, 1]
        assert assigned["distrib_cpu_cores"] == [1]
        assert assigned["poller_cpu_cores"] == []

    def test_three_vcpus(self):
        assigned = _calc([0, 1, 2])
        assert assigned["app_thread_core"] == [0]
        assert assigned["jc_singleton_core"] == [0]
        assert assigned["jm_cpu_core"] == [0]
        assert assigned["alceml_cpu_cores"] == [0]
        assert assigned["lvol_poller_core"] == [1]
        assert assigned["poller_cpu_cores"] == [1]
        assert assigned["distrib_cpu_cores"] == [2]

    def test_four_vcpus(self):
        assigned = _calc([0, 1, 2, 3])
        assert assigned["app_thread_core"] == [0]
        assert assigned["jc_singleton_core"] == [0]
        assert assigned["jm_cpu_core"] == [1]
        assert assigned["alceml_cpu_cores"] == [1]
        assert assigned["lvol_poller_core"] == [2]
        assert assigned["poller_cpu_cores"] == [2]
        assert assigned["distrib_cpu_cores"] == [3]

    def test_five_vcpus(self):
        assigned = _calc([0, 1, 2, 3, 4])
        assert assigned["app_thread_core"] == [0]
        assert assigned["jc_singleton_core"] == [0]
        assert assigned["jm_cpu_core"] == [1]
        assert assigned["lvol_poller_core"] == [1]
        assert assigned["poller_cpu_cores"] == [2]
        assert assigned["distrib_cpu_cores"] == [3]
        assert assigned["alceml_cpu_cores"] == [4]

    def test_layouts_use_real_core_ids_not_positional_indices(self):
        """The layout must key off the actual core ids handed in, not
        assume 0..N-1 -- add_node's isolated-core lists are never that."""
        assigned = _calc([5, 9, 14])
        assert assigned["app_thread_core"] == [5]
        assert assigned["jc_singleton_core"] == [5]
        assert assigned["jm_cpu_core"] == [5]
        assert assigned["alceml_cpu_cores"] == [5]
        assert assigned["lvol_poller_core"] == [9]
        assert assigned["poller_cpu_cores"] == [9]
        assert assigned["distrib_cpu_cores"] == [14]


class TestUnder22VcpusUnchanged:
    """alceml's share below 22 vCPUs was never scaled by the device count,
    so there's nothing to reorder here -- confirms the boundary tiers still
    match what shipped before this change."""

    def test_below_12_vcpus_alceml_gets_one_fixed_core(self):
        for alceml_count in (1, 3, 8):
            assigned = _calc(list(range(10)), alceml_count)
            assert len(assigned["alceml_cpu_cores"]) == 1, alceml_count

    def test_12_to_21_vcpus_alceml_gets_two_fixed_cores(self):
        for alceml_count in (1, 3, 8):
            assigned = _calc(list(range(16)), alceml_count)
            assert len(assigned["alceml_cpu_cores"]) == 2, alceml_count


class TestDistribPriorityAt22PlusVcpus:
    """22+ vCPUs is the tier where alceml used to scale with the device
    count and eat into distrib/poller's budget before they saw it."""

    # V, expected distrib-core count -- independent of alceml_count.
    DISTRIB_CORES_BY_VCPU: ClassVar[dict] = {
        22: 9, 23: 10, 24: 10, 25: 11, 26: 11,
        27: 12, 30: 12, 37: 12,  # capped at 12 through this range
        38: 24, 40: 24,          # jumps straight to 24, no ramp
    }

    def test_distrib_count_is_independent_of_alceml_count(self):
        for vcpu_count, expected in self.DISTRIB_CORES_BY_VCPU.items():
            for alceml_count in (1, 2, 5, 10):
                assigned = _calc(list(range(vcpu_count)), alceml_count)
                got = len(assigned["distrib_cpu_cores"])
                assert got == expected, (
                    f"V={vcpu_count} A={alceml_count}: expected {expected} "
                    f"distrib cores, got {got}")

    def test_alceml_gets_its_real_share_when_there_is_room(self):
        assigned = _calc(list(range(22)), alceml_count=2)
        assert len(assigned["distrib_cpu_cores"]) == 9
        assert len(assigned["alceml_cpu_cores"]) == 2
        assert len(assigned["poller_cpu_cores"]) == 8, "poller absorbs the true remainder"

    def test_alceml_is_clipped_when_the_request_exceeds_what_is_left(self):
        """V=22: base=3, remaining=19, distrib takes 9, leaving 10 for
        alceml+poller. Ask for 15 -- more than exists -- and it must clip,
        not raise or overrun into cores distrib/poller already hold."""
        assigned = _calc(list(range(22)), alceml_count=15)
        assert len(assigned["distrib_cpu_cores"]) == 9
        assert len(assigned["alceml_cpu_cores"]) == 10
        assert len(assigned["poller_cpu_cores"]) == 0

    def test_no_core_is_assigned_to_more_than_one_role(self):
        for vcpu_count in range(22, 45):
            assigned = _calc(list(range(vcpu_count)), alceml_count=3)
            exclusive = (assigned["app_thread_core"] + assigned["jm_cpu_core"]
                        + assigned["jc_singleton_core"] + assigned["alceml_cpu_cores"]
                        + assigned["distrib_cpu_cores"] + assigned["poller_cpu_cores"])
            # lvol_poller co-locates with jc_singleton by design below 32 vCPU
            # -- only count it separately once it has its own core.
            if assigned["lvol_poller_core"] != assigned["jc_singleton_core"]:
                exclusive += assigned["lvol_poller_core"]
            dupes = {c for c in exclusive if exclusive.count(c) > 1}
            assert not dupes, f"V={vcpu_count}: {dupes} assigned to more than one role"
            assert len(exclusive) <= vcpu_count
