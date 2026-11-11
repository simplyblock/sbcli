"""The user's share of a NUMA node's huge-page pool, as both pool writers
compute it.

``set_hugepages_if_needed`` (the sysfs writer on hosts the node may write to)
and the Talos managed flow (a machine-config patch through the Talos API) each
add simplyblock's pages on top of what the user reserved. The arithmetic that
tells the two apart -- what the host carried the first time simplyblock saw
it, plus any later manual increase, never a decrease -- lives once, in
``user_hugepages_baseline``, keyed on a state directory each writer picks for
itself: a tmpfs where a reboot zeroes the pool, a persistent one where the
host re-applies it.
"""
import os
import shutil
import tempfile
import unittest

from simplyblock_core import utils as core_utils


class TestUserHugepagesBaseline(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def _files(self, numa=0):
        out = {}
        for name in (f'hugepages_baseline_node{numa}', f'hugepages_sb_node{numa}'):
            path = os.path.join(self.dir, name)
            out[name] = int(open(path).read()) if os.path.exists(path) else None
        return out

    def test_first_sight_takes_the_whole_pool_as_the_users(self):
        self.assertEqual(core_utils.user_hugepages_baseline(0, 1024, self.dir), 1024)
        self.assertEqual(self._files(), {'hugepages_baseline_node0': 1024, 'hugepages_sb_node0': None})

    def test_the_recorded_total_separates_the_users_pages_from_simplyblocks(self):
        core_utils.user_hugepages_baseline(0, 1024, self.dir)
        self.assertTrue(core_utils.save_sb_hugepages_allocation(0, 1024 + 6942, self.dir))
        # The host now carries the total: none of the extra is the user's.
        self.assertEqual(core_utils.user_hugepages_baseline(0, 1024 + 6942, self.dir), 1024)

    def test_a_manual_increase_above_the_total_is_absorbed_and_kept(self):
        core_utils.user_hugepages_baseline(0, 1024, self.dir)
        core_utils.save_sb_hugepages_allocation(0, 7966, self.dir)
        self.assertEqual(core_utils.user_hugepages_baseline(0, 7966 + 500, self.dir), 1524)
        self.assertEqual(self._files()['hugepages_baseline_node0'], 1524)

    def test_a_pool_below_the_total_is_a_reset_not_a_removal(self):
        """A reboot zeroed the pool (or Talos has not re-applied it yet):
        the user's share stays what it was rather than going negative."""
        core_utils.user_hugepages_baseline(0, 1024, self.dir)
        core_utils.save_sb_hugepages_allocation(0, 7966, self.dir)
        self.assertEqual(core_utils.user_hugepages_baseline(0, 0, self.dir), 1024)

    def test_numa_nodes_are_recorded_apart(self):
        core_utils.user_hugepages_baseline(0, 100, self.dir)
        core_utils.user_hugepages_baseline(1, 200, self.dir)
        self.assertEqual(self._files(0)['hugepages_baseline_node0'], 100)
        self.assertEqual(self._files(1)['hugepages_baseline_node1'], 200)

    def test_the_default_directory_is_the_sysfs_writers_tmpfs(self):
        self.assertEqual(core_utils._HUGEPAGES_BASELINE_DIR, '/var/run/simplyblock')
