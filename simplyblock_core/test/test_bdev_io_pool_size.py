import unittest

from simplyblock_core import constants, utils


class TestBdevIoPoolSize(unittest.TestCase):
    """The bdev_io pool is sized for a node's saturation point: the SPDK
    default of 65535, never cut (a smaller pool caps write IOPS)."""

    def test_keeps_the_saturation_size(self):
        for mask in ("0x66", "0xff", "0xffff", "0xffffffff"):
            self.assertEqual(utils.calculate_bdev_io_pool_size(mask), constants.BDEV_IO_POOL_SIZE)

    def test_thread_caches_take_at_most_half(self):
        for mask in ("0xff", "0xffffffff", "0x" + "f" * 64):
            cores = int(mask, 16).bit_count()
            threads = constants.BDEV_IO_POOL_THREADS_PER_CORE * cores + 8
            size = utils.calculate_bdev_io_pool_size(mask)
            self.assertGreaterEqual(size, 2 * constants.BDEV_IO_CACHE_SIZE * (threads + 1))

    def test_unknown_mask_keeps_the_saturation_size(self):
        for mask in (None, "", "garbage"):
            self.assertEqual(utils.calculate_bdev_io_pool_size(mask), constants.BDEV_IO_POOL_SIZE)


if __name__ == "__main__":
    unittest.main()
