"""calculate_core_allocations() must colocate lvol_poller_core with
jc_singleton_core whenever the config doesn't give lvol_poller its own
dedicated core (colocate_lvs=True, i.e. below 32 vCPU) — this is what lets
storage_node_ops.py use lvol_poller_mask directly for
bdev_lvol_create_poller_group() without an unconditional jc_singleton_mask
override (which used to silently clobber the dedicated-core case; see
e3e8fd08 and its follow-up fix).

Previously lvol_poller_core colocated with app_thread_core instead, which is
why the override existed in the first place.
"""

from unittest.mock import patch

from simplyblock_core import utils


def _calc(vcpu_list, alceml_count=2):
    # calculate_core_allocations returns a fixed-order tuple, not a dict:
    # (app_thread_core, jm_cpu_core, poller_cpu_cores, alceml_cpu_cores,
    #  alceml_worker_cpu_cores, distrib_cpu_cores, jc_singleton_core,
    #  lvol_poller_core, compression_core)
    with patch("simplyblock_core.utils.is_hyperthreading_enabled_via_siblings", return_value=False):
        result = utils.calculate_core_allocations(vcpu_list, alceml_count=alceml_count)
    return {
        "app_thread_core": result[0],
        "jc_singleton_core": result[6],
        "lvol_poller_core": result[7],
    }


class TestLvolPollerJcSingletonColocation:

    def test_small_node_colocates_lvol_poller_with_jc_singleton(self):
        # len(vcpu_list) < 12 branch; below 32 vCPU, so colocate_lvs is True.
        assigned = _calc(list(range(10)))

        assert assigned["lvol_poller_core"] == assigned["jc_singleton_core"]
        assert assigned["lvol_poller_core"] != []

    def test_mid_size_node_colocates_lvol_poller_with_jc_singleton(self):
        # 12 <= len(vcpu_list) < 22 branch.
        assigned = _calc(list(range(16)))

        assert assigned["lvol_poller_core"] == assigned["jc_singleton_core"]

    def test_large_node_below_32_colocates_lvol_poller_with_jc_singleton(self):
        # len(vcpu_list) >= 22 branch, still < 32 so colocate_lvs is True.
        assigned = _calc(list(range(24)))

        assert assigned["lvol_poller_core"] == assigned["jc_singleton_core"]

    def test_large_node_at_or_above_32_gives_lvol_poller_its_own_dedicated_core(self):
        # len(vcpu_list) >= 32: colocate_lvs is False -- lvol_poller gets its
        # own dedicated core, distinct from jc_singleton_core and app_thread.
        assigned = _calc(list(range(34)))

        assert assigned["lvol_poller_core"] != assigned["jc_singleton_core"]
        assert assigned["lvol_poller_core"] != assigned["app_thread_core"]
        assert assigned["lvol_poller_core"] != []