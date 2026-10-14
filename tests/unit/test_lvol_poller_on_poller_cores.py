"""calculate_core_allocations() runs the lvstore poller group on the nvmf
poller core set.

The lvol poller group used to be pinned to one core: the JC singleton's
below 32 vCPU, a dedicated one above. A single busy core then throttled
every lvstore / transfer-hub bdev at once. It now shares the poller cores,
which costs no extra core -- lvol_poller_core is a view of
poller_cpu_cores -- so the rest of every layout is unchanged. The JC
singleton core is the fallback only when the layout has no poller core.
"""

from unittest.mock import patch

from simplyblock_core import utils

_FIELDS = ("app_thread_core", "jm_cpu_core", "poller_cpu_cores", "alceml_cpu_cores",
           "alceml_worker_cpu_cores", "distrib_cpu_cores", "jc_singleton_core",
           "lvol_poller_core", "compression_core")


def _calc(vcpu_list, alceml_count=2):
    with patch("simplyblock_core.utils.is_hyperthreading_enabled_via_siblings", return_value=False):
        result = utils.calculate_core_allocations(vcpu_list, alceml_count=alceml_count)
    return dict(zip(_FIELDS, result))


class TestLvolPollerOnPollerCores:

    def test_every_general_tier_shares_the_poller_core_set(self):
        # 6..11, 12..21, 22..31 and >= 32 vCPU: the four derived layouts.
        for vcpu_count in (6, 8, 10, 12, 16, 21, 22, 24, 31, 32, 34, 48, 64):
            assigned = _calc(list(range(vcpu_count)))
            assert assigned["lvol_poller_core"] == assigned["poller_cpu_cores"], vcpu_count
            assert assigned["lvol_poller_core"], vcpu_count

    def test_no_core_is_reserved_for_the_lvol_poller_at_32_vcpus_and_above(self):
        # The dedicated lvol-poller core of the old >= 32 layout goes back to
        # the pool: the poller set grows by one, nothing else moves.
        assigned = _calc(list(range(34)), alceml_count=3)
        exclusive = (assigned["app_thread_core"] + assigned["jm_cpu_core"]
                     + assigned["jc_singleton_core"] + assigned["alceml_cpu_cores"]
                     + assigned["distrib_cpu_cores"] + assigned["poller_cpu_cores"])
        assert sorted(exclusive) == list(range(34))
        assert assigned["lvol_poller_core"] == assigned["poller_cpu_cores"]

    def test_poller_set_is_the_multi_core_mask_sent_to_spdk(self):
        assigned = _calc(list(range(16)))
        assert len(assigned["lvol_poller_core"]) > 1
        assert utils.generate_mask(assigned["lvol_poller_core"]) == utils.generate_mask(assigned["poller_cpu_cores"])

    def test_falls_back_to_the_jc_singleton_core_when_no_poller_core_is_left(self):
        # 22 vCPU with 15 devices: alceml takes everything after distrib and
        # the poller remainder clips to nothing; the group still needs a core.
        assigned = _calc(list(range(22)), alceml_count=15)
        assert assigned["poller_cpu_cores"] == []
        assert assigned["lvol_poller_core"] == assigned["jc_singleton_core"]
        assert assigned["lvol_poller_core"]

    def test_tiny_layouts_follow_the_same_rule(self):
        for vcpu_count in (3, 4, 5):
            assigned = _calc(list(range(vcpu_count)))
            assert assigned["lvol_poller_core"] == assigned["poller_cpu_cores"], vcpu_count
        # Two cores leave no poller core at all: the group sits with JC on core 0.
        assigned = _calc([0, 1])
        assert assigned["poller_cpu_cores"] == []
        assert assigned["lvol_poller_core"] == assigned["jc_singleton_core"] == [0]

    def test_view_is_a_copy_not_the_same_list(self):
        assigned = _calc(list(range(10)))
        assert assigned["lvol_poller_core"] is not assigned["poller_cpu_cores"]
