"""iobuf pool sizing: a fixed large-pool reserve, a larger small pool for
replication, and small per-thread caches for modules that do not draw buffers
(bed bf18 2026-11-09: the hugepage heap ran out)."""
from unittest.mock import MagicMock

from simplyblock_core import constants, utils
from simplyblock_core.rpc_client import RPCClient


def test_large_pool_is_a_fixed_100_mib_reserve():
    _, large = utils.calculate_pool_count(alceml_count=8, number_of_distribs=12, cpu_count=8,
                                          poller_count=8, max_lvol=200)
    assert large == constants.IOBUF_LARGE_POOL_BYTES // constants.IOBUF_LARGE_BUFSIZE
    assert large * constants.IOBUF_LARGE_BUFSIZE <= 100 * 1024 * 1024
    # independent of the node's object counts
    _, large2 = utils.calculate_pool_count(alceml_count=40, number_of_distribs=60, cpu_count=32,
                                           poller_count=32, max_lvol=1000)
    assert large2 == large


def test_large_pool_still_covers_the_per_thread_caches():
    _, large = utils.calculate_pool_count(alceml_count=1, number_of_distribs=1, cpu_count=2000,
                                          poller_count=2000, max_lvol=1)
    per_thread = constants.BDEV_IOBUF_LARGE_CACHE_SIZE + constants.ACCEL_LARGE_CACHE_SIZE
    assert large >= per_thread * 2000


def test_small_pool_is_sized_for_saturation_not_max_lvol():
    small, _ = utils.calculate_pool_count(alceml_count=8, number_of_distribs=12, cpu_count=16,
                                          poller_count=16, max_lvol=30)
    small2, _ = utils.calculate_pool_count(alceml_count=8, number_of_distribs=12, cpu_count=16,
                                           poller_count=16, max_lvol=250)
    assert small == small2
    threads = constants.BDEV_IO_POOL_THREADS_PER_CORE * 16 + 8
    expected = (constants.IOBUF_SMALL_DATA_COUNT + constants.IOBUF_SMALL_MERGE_COW_COUNT
                + 300 * 1024 * 1024 // (8 * 1024)        # replication reserve
                + constants.NVMF_BUF_CACHE_SIZE * 17
                + (constants.BDEV_IOBUF_SMALL_CACHE_SIZE + constants.ACCEL_SMALL_CACHE_SIZE) * threads)
    assert small == expected
    assert 90000 < small < 100000  # ~96k on a 16-core node (2026-11-16 sizing)


def test_small_pool_grows_with_cores_for_the_thread_caches():
    s8, _ = utils.calculate_pool_count(alceml_count=4, number_of_distribs=4, cpu_count=8,
                                       poller_count=8, max_lvol=30)
    s32, _ = utils.calculate_pool_count(alceml_count=4, number_of_distribs=4, cpu_count=32,
                                        poller_count=32, max_lvol=30)
    assert s32 > s8


def _client():
    c = RPCClient.__new__(RPCClient)
    c._request = MagicMock(return_value=True)  # type: ignore[method-assign]
    return c


def test_accel_caches_are_small():
    c = _client()
    c.accel_set_options()
    c._request.assert_called_once_with("accel_set_options",
                                       small_cache_size=constants.ACCEL_SMALL_CACHE_SIZE,
                                       large_cache_size=constants.ACCEL_LARGE_CACHE_SIZE)
    assert constants.ACCEL_LARGE_CACHE_SIZE <= 4


def test_bdev_large_cache_is_sent_on_its_own():
    # it used to be sent only when the small cache size was positive
    c = _client()
    c.bdev_set_options(0, 0, 0, 2)
    assert c._request.call_args.kwargs.get("iobuf_large_cache_size") == 2
    assert "iobuf_small_cache_size" not in c._request.call_args.kwargs
