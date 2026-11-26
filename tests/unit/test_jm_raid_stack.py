"""
Integration-ish unit test for storage_node_ops._create_jm_stack_on_raid: it
verifies the bdev_raid_create call sequence that builds the RAID 0+1 JM stack
for different drive counts, with the RPC client and dependencies mocked.

  1 device   -> no raid_create at all (bare device)
  2 devices  -> one raid1 over the two bare devices (mirror)
  3 devices  -> raid0 over the 2-drive leg + top raid1 over [leg0, bare-drive]
  6 devices  -> raid0 over each 3-drive leg + top raid1 over the two legs

A restart that finds a member missing keeps that geometry: it builds the legs
whose members are all there and runs the JM on what is left, instead of
re-planning a raid over the members found.
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

    def _run(self, members, missing=(), stale=(), layout="raid01", after_restart=False):
        rpc = MagicMock()
        rpc.bdev_raid_create.return_value = True
        rpc.bdev_jm_create.return_value = True
        rpc.bdev_get.return_value = {"block_size": 512, "num_blocks": 1000}

        snode = MagicMock()
        snode.get_id.return_value = "n1"
        snode.enable_ha_jm = False          # skip PT/subsystem/listener path
        snode.data_nics = []
        snode.jm_cpu_mask = ""
        snode.jm_device = None
        if after_restart:
            snode.jm_device = MagicMock()
            snode.jm_device.stale_jm_legs = list(stale)
        snode.create_alceml.return_value = True

        cluster = MagicMock()
        cluster.jm_raid_layout = layout
        cluster.page_size_in_blocks = 4096
        cluster.full_page_unmap = False
        cluster.shared_placement = False

        with patch.object(self.sno, "DBController") as db_cls:
            db_cls.return_value.get_cluster_by_id.return_value = cluster
            jm = self.sno._create_jm_stack_on_raid(
                rpc, list(members), snode, after_restart=after_restart, missing=missing)
        self.rpc = rpc
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

    # A restart with a member missing.

    def test_missing_member_of_a_mirror_runs_on_the_other(self):
        jm, calls = self._run(["a", "b"], missing=["b"], after_restart=True)
        self.assertEqual(calls, [])                 # no raid1 over one base
        self.assertEqual(jm.raid_bdev, "a")
        # The full member list and geometry stay on record for the restart
        # that finds b again, and b is rebuilt then, not mounted as current.
        self.assertEqual(jm.jm_nvme_bdev_list, ["a", "b"])
        self.assertEqual(jm.jm_leg_bdevs, ["a", "b"])
        self.assertEqual(jm.stale_jm_legs, ["b"])

    def test_missing_member_never_re_plans_the_legs(self):
        # 4 drives are legs (a,b) and (c,d). Without c, the members found
        # (a,b,d) must not become legs (a,b) and (d): d holds half a stripe.
        jm, calls = self._run(list("abcd"), missing=["c"], after_restart=True)
        self.assertEqual(calls, [("raid_jm_n1_l0", ["a", "b"], "0")])
        self.assertEqual(jm.raid_bdev, "raid_jm_n1_l0")
        self.assertEqual(jm.jm_nvme_bdev_list, list("abcd"))
        self.assertEqual(jm.jm_leg_members, [["a", "b"], ["c", "d"]])
        self.assertEqual(jm.stale_jm_legs, ["raid_jm_n1_l1"])

    def test_missing_single_drive_leg_runs_on_the_raid0_leg(self):
        jm, calls = self._run(list("abc"), missing=["c"], after_restart=True)
        self.assertEqual(calls, [("raid_jm_n1_l0", ["a", "b"], "0")])
        self.assertEqual(jm.raid_bdev, "raid_jm_n1_l0")
        self.assertEqual(jm.stale_jm_legs, ["c"])

    def test_missing_member_of_the_raid0_leg_runs_on_the_single_drive_leg(self):
        jm, calls = self._run(list("abc"), missing=["a"], after_restart=True)
        self.assertEqual(calls, [])                 # the raid0 leg cannot be built
        self.assertEqual(jm.raid_bdev, "c")
        self.assertEqual(jm.stale_jm_legs, ["raid_jm_n1_l0"])

    def test_all_members_back_rebuilds_the_leg_that_was_missing(self):
        # The restart after: both legs build, the one recorded stale is taken
        # out before the JM alceml loads and re-added for a rebuild.
        jm, calls = self._run(list("abcd"), stale=["raid_jm_n1_l1"], after_restart=True)
        self.assertEqual(calls[-1], ("raid_jm_n1", ["raid_jm_n1_l0", "raid_jm_n1_l1"], "1"))
        self.rpc.bdev_raid_remove_base_bdev.assert_called_once_with("raid_jm_n1_l1")
        self.rpc.bdev_raid_add_base_bdev.assert_called_once_with("raid_jm_n1", "raid_jm_n1_l1")
        self.assertEqual(jm.stale_jm_legs, ["raid_jm_n1_l1"])

    def test_only_a_stale_leg_left_is_no_journal(self):
        with self.assertRaises(self.sno.JMRaidNoCurrentLeg):
            self._run(["a", "b"], missing=["b"], stale=["a"], after_restart=True)

    def test_a_member_missing_from_both_legs_is_no_journal(self):
        with self.assertRaises(self.sno.JMRaidNoCurrentLeg):
            self._run(list("abcd"), missing=["a", "d"], after_restart=True)

    def test_missing_single_device_is_no_journal(self):
        with self.assertRaises(self.sno.JMRaidNoCurrentLeg):
            self._run(["a"], missing=["a"], after_restart=True)

    def test_legacy_layout_is_one_mirror_over_all_members(self):
        jm, calls = self._run(list("abc"), layout="legacy_nway")
        self.assertEqual(calls, [("raid_jm_n1", ["a", "b", "c"], "1")])
        self.assertEqual((jm.jm_leg_bdevs, jm.jm_leg_members), ([], []))

    def test_legacy_mirror_keeps_the_members_found(self):
        # Every member of the N-way mirror is a full copy.
        jm, calls = self._run(list("abc"), missing=["b"], layout="legacy_nway", after_restart=True)
        self.assertEqual(calls, [("raid_jm_n1", ["a", "c"], "1")])
        self.assertEqual(jm.jm_nvme_bdev_list, list("abc"))
        self.assertEqual(jm.stale_jm_legs, ["b"])

    def test_legacy_mirror_with_one_member_left_runs_on_it(self):
        jm, calls = self._run(list("abc"), missing=["a", "b"], layout="legacy_nway", after_restart=True)
        self.assertEqual(calls, [])
        self.assertEqual(jm.raid_bdev, "c")
        self.assertEqual(jm.stale_jm_legs, ["a", "b"])

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
