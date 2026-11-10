import unittest

from simplyblock_core import constants, utils


class TestBdevIoPoolSize(unittest.TestCase):

    def test_small_node_gets_floor(self):
        # bed bf20: mask 0x66 (4 reactor cores), peak 6861 in use
        self.assertEqual(utils.calculate_bdev_io_pool_size("0x66"), constants.BDEV_IO_POOL_SIZE_MIN)

    def test_covers_thread_caches(self):
        for mask in ("0xff", "0xffff", "0xffffffff"):
            cores = bin(int(mask, 16)).count("1")
            threads = constants.BDEV_IO_POOL_THREADS_PER_CORE * cores + 8
            size = utils.calculate_bdev_io_pool_size(mask)
            self.assertGreaterEqual(size, constants.BDEV_IO_CACHE_SIZE * (threads + 1))

    def test_capped_at_spdk_default(self):
        self.assertEqual(utils.calculate_bdev_io_pool_size("0x" + "f" * 32), constants.BDEV_IO_POOL_SIZE_MAX)

    def test_unknown_mask_keeps_spdk_default(self):
        for mask in (None, "", "garbage"):
            self.assertEqual(utils.calculate_bdev_io_pool_size(mask), constants.BDEV_IO_POOL_SIZE_MAX)


if __name__ == "__main__":
    unittest.main()
