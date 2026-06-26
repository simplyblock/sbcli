# coding=utf-8
"""
Integration-ish unit test for storage_node_ops._create_jm_stack_on_raid: it
verifies the bdev_raid_create call sequence that builds the RAID 0+1 JM stack
for different drive counts, with the RPC client and dependencies mocked.

  1 device   -> no raid_create at all (bare device)
  2 devices  -> one raid1 over the two bare devices (mirror)
  3 devices  -> raid0 over the 2-drive leg + top raid1 over [leg0, bare-drive]
  6 devices  -> raid0 over each 3-drive leg + top raid1 over the two legs
"""

import os
import unittest
from unittest.mock import MagicMock, patch

LD = os.environ.get("LD_LIBRARY_PATH", "")


def _import_sno():
    from simplyblock_core import storage_node_ops
    return storage_node_ops


class TestCreateJmStackOnRaid(unittest.TestCase):
    def setUp(self):
        self.sno = _import_sno()

    def _run(self, members):
        rpc = MagicMock()
        rpc.bdev_raid_create.return_value = True
        rpc.bdev_jm_create.return_value = True
        rpc.get_bdevs.return_value = [{"block_size": 512, "num_blocks": 1000}]

        snode = MagicMock()
        snode.get_id.return_value = "n1"
        snode.enable_ha_jm = False          # skip PT/subsystem/listener path
        snode.data_nics = []
        snode.jm_cpu_mask = ""
        snode.jm_device = None
        snode.create_alceml.return_value = True

        cluster = MagicMock()
        cluster.page_size_in_blocks = 4096
        cluster.full_page_unmap = False
        cluster.shared_placement = False

        with patch.object(self.sno, "DBController") as db_cls:
            db_cls.return_value.get_cluster_by_id.return_value = cluster
            jm = self.sno._create_jm_stack_on_raid(rpc, list(members), snode, after_restart=False)
        # extract the (name, members, level) of each bdev_raid_create call
        calls = [(c.args[0], list(c.args[1]), c.args[2] if len(c.args) > 2 else c.kwargs.get("raid_level"))
                 for c in rpc.bdev_raid_create.call_args_list]
        return jm, calls

    def test_single_device_no_raid(self):
        jm, calls = self._run(["dev_a"])
        self.assertEqual(calls, [])                 # no raid created
        self.assertEqual(jm.raid_bdev, "dev_a")     # bare device is the base
        self.assertEqual(jm.jm_leg_bdevs, [])

    def test_two_devices_mirror(self):
        jm, calls = self._run(["a", "b"])
        self.assertEqual(calls, [("raid_jm_n1", ["a", "b"], "1")])
        self.assertEqual(jm.raid_bdev, "raid_jm_n1")
        self.assertEqual(jm.jm_leg_bdevs, ["a", "b"])

    def test_three_devices_one_raid0_leg_plus_bare(self):
        jm, calls = self._run(["a", "b", "c"])  # split (2,1)
        self.assertEqual(calls[0], ("raid_jm_n1_l0", ["a", "b"], "0"))
        self.assertEqual(calls[1], ("raid_jm_n1", ["raid_jm_n1_l0", "c"], "1"))
        self.assertEqual(jm.jm_leg_bdevs, ["raid_jm_n1_l0", "c"])

    def test_six_devices_two_raid0_legs(self):
        jm, calls = self._run(list("abcdef"))  # split (3,3)
        self.assertEqual(calls[0], ("raid_jm_n1_l0", ["a", "b", "c"], "0"))
        self.assertEqual(calls[1], ("raid_jm_n1_l1", ["d", "e", "f"], "0"))
        self.assertEqual(calls[2], ("raid_jm_n1", ["raid_jm_n1_l0", "raid_jm_n1_l1"], "1"))
        self.assertEqual(jm.jm_leg_bdevs, ["raid_jm_n1_l0", "raid_jm_n1_l1"])

    def test_raid0_leg_failure_aborts(self):
        # if a leg's raid0 create fails, the whole build returns False
        self.sno = _import_sno()
        rpc = MagicMock()
        rpc.bdev_raid_create.return_value = False
        snode = MagicMock()
        snode.get_id.return_value = "n1"
        snode.jm_device = None
        with patch.object(self.sno, "DBController"):
            ret = self.sno._create_jm_stack_on_raid(rpc, ["a", "b", "c"], snode, after_restart=False)
        self.assertFalse(ret)


if __name__ == "__main__":
    unittest.main()
