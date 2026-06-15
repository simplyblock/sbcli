# coding=utf-8
"""
Unit tests for simplyblock_core/jm_raid.py — the Journal-Manager RAID topology
and reconfiguration planner.

Background: the per-node JM device used to be an N-way RAID1 mirror across the
JM partition (`p1`) of every drive, which costs N physical writes per journal
record (× 4 JM copies at FTT=2 ⇒ up to 40×). The new layout is RAID 0+1:

  - 1 device  -> no raid (test-only single-device node)
  - 2 devices -> raid1 over two single-device legs  == a 2-way mirror
  - >2 devices-> split drives into two ±1 balanced groups, raid0 each group,
                 raid1 over the two raid0 legs

That caps amplification at 2× per node regardless of drive count, and avoids
SPDK's raid5f full-stripe-write constraint. These tests pin the pure planning
logic that decides the topology and the per-leg rebuild actions for the drive
lifecycle (fail / add / re-insert / both-legs-lost).
"""

import importlib.util
import os
import unittest


def _load():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "simplyblock_core", "jm_raid.py")
    spec = importlib.util.spec_from_file_location("jm_raid", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


JM = _load()


class TestSplitTwoGroups(unittest.TestCase):
    def test_sizes_balanced_within_one(self):
        # (input length) -> expected (len groupA, len groupB)
        expected = {2: (1, 1), 3: (2, 1), 4: (2, 2), 5: (3, 2),
                    6: (3, 3), 7: (4, 3), 8: (4, 4), 10: (5, 5)}
        for n, (la, lb) in expected.items():
            a, b = JM.split_two_groups(list(range(n)))
            self.assertEqual((len(a), len(b)), (la, lb), f"n={n}")
            self.assertLessEqual(abs(len(a) - len(b)), 1, f"n={n}")

    def test_partition_is_complete_and_disjoint(self):
        items = [f"d{i}" for i in range(7)]
        a, b = JM.split_two_groups(items)
        self.assertEqual(sorted(a + b), sorted(items))
        self.assertFalse(set(a) & set(b))

    def test_larger_group_first(self):
        a, b = JM.split_two_groups([1, 2, 3, 4, 5])  # 5 -> (3,2)
        self.assertGreaterEqual(len(a), len(b))


class TestPlanTopology(unittest.TestCase):
    def test_zero_raises(self):
        with self.assertRaises(ValueError):
            JM.plan_topology([])

    def test_single_device_no_raid(self):
        p = JM.plan_topology(["d0"])
        self.assertEqual(p["level"], JM.RAID_NONE)
        self.assertEqual(p["base"], "d0")

    def test_two_devices_is_two_single_member_legs(self):
        p = JM.plan_topology(["a", "b"])
        self.assertEqual(p["level"], JM.RAID_0PLUS1)
        self.assertEqual(p["legs"], [["a"], ["b"]])  # == 2-way mirror

    def test_three_devices_2_1(self):
        p = JM.plan_topology(["a", "b", "c"])
        self.assertEqual(p["level"], JM.RAID_0PLUS1)
        self.assertEqual([len(x) for x in p["legs"]], [2, 1])

    def test_six_devices_3_3(self):
        p = JM.plan_topology(list("abcdef"))
        self.assertEqual([len(x) for x in p["legs"]], [3, 3])
        # all members present exactly once across the two legs
        self.assertEqual(sorted(p["legs"][0] + p["legs"][1]), list("abcdef"))


class TestLegHelpers(unittest.TestCase):
    def test_is_balanced(self):
        self.assertTrue(JM.is_balanced([["a", "b"], ["c"]]))      # 2,1
        self.assertTrue(JM.is_balanced([["a"], ["b"]]))           # 1,1
        self.assertFalse(JM.is_balanced([["a", "b", "c"], ["d"]]))  # 3,1

    def test_smaller_leg_index(self):
        self.assertEqual(JM.smaller_leg_index([["a", "b"], ["c"]]), 1)
        self.assertEqual(JM.smaller_leg_index([["a"], ["b", "c"]]), 0)
        # tie -> leg 0
        self.assertEqual(JM.smaller_leg_index([["a"], ["b"]]), 0)


class TestPlanReconfigure(unittest.TestCase):
    def test_failed_device_rebuilds_only_its_leg(self):
        legs = [["a", "b"], ["c", "d"]]
        r = JM.plan_reconfigure(legs, failed="b")
        self.assertEqual(r["rebuild"], [0])
        self.assertEqual(r["legs"], [["a"], ["c", "d"]])

    def test_added_device_goes_to_smaller_leg(self):
        legs = [["a", "b"], ["c"]]   # leg 1 is smaller
        r = JM.plan_reconfigure(legs, added="x")
        self.assertEqual(r["rebuild"], [1])
        self.assertEqual(r["legs"], [["a", "b"], ["c", "x"]])

    def test_failure_on_both_legs_rebuilds_both(self):
        legs = [["a", "b"], ["c", "d"]]
        r = JM.plan_reconfigure(legs, failed=["b", "d"])
        self.assertEqual(r["rebuild"], [0, 1])
        self.assertEqual(r["legs"], [["a"], ["c"]])

    def test_reinserted_device_is_added_to_smaller_leg(self):
        # a device that is no longer part of any leg's raid0 is re-incorporated
        legs = [["a", "b", "c"], ["d", "e"]]
        r = JM.plan_reconfigure(legs, added="f")
        self.assertEqual(r["rebuild"], [1])
        self.assertEqual(r["legs"], [["a", "b", "c"], ["d", "e", "f"]])

    def test_no_event_no_rebuild(self):
        legs = [["a", "b"], ["c", "d"]]
        r = JM.plan_reconfigure(legs)
        self.assertEqual(r["rebuild"], [])
        self.assertEqual(r["legs"], legs)

    def test_failed_device_not_present_is_noop(self):
        legs = [["a", "b"], ["c", "d"]]
        r = JM.plan_reconfigure(legs, failed="zzz")
        self.assertEqual(r["rebuild"], [])
        self.assertEqual(r["legs"], legs)


if __name__ == "__main__":
    unittest.main()
