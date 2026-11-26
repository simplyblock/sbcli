"""
Unit tests for simplyblock_core/jm_raid.py — the Journal-Manager RAID topology
planner.

Background: the per-node JM device used to be an N-way RAID1 mirror across the
JM partition (`p1`) of every drive, which costs N physical writes per journal
record (× 4 JM copies at FTT=2 ⇒ up to 40×). The new layout is RAID 0+1:

  - 1 device  -> no raid (test-only single-device node)
  - 2 devices -> raid1 over two single-device legs  == a 2-way mirror
  - >2 devices-> split drives into ±1 balanced groups of at most four (two
                 groups up to eight drives), raid0 each group, raid1 over
                 the raid0 legs

That costs one device write per leg instead of one per drive, and avoids
SPDK's raid5f full-stripe-write constraint. These tests pin the pure planning
logic that decides the topology. What happens when a node's drives change is
in test_jm_raid_reshape.py.
"""

import importlib.util
import os
import unittest


def _load():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(root, "simplyblock_core", "jm_raid.py")
    spec = importlib.util.spec_from_file_location("jm_raid", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


JM = _load()


class TestSplitGroups(unittest.TestCase):
    def test_group_sizes(self):
        # (input length) -> expected group sizes: at most four devices per
        # group, at least two groups, sizes within one of each other.
        expected = {2: (1, 1), 3: (2, 1), 4: (2, 2), 5: (3, 2),
                    6: (3, 3), 7: (4, 3), 8: (4, 4),
                    9: (3, 3, 3), 10: (4, 3, 3), 11: (4, 4, 3), 12: (4, 4, 4),
                    13: (4, 3, 3, 3), 16: (4, 4, 4, 4), 17: (4, 4, 3, 3, 3)}
        for n, sizes in expected.items():
            groups = JM.split_groups(list(range(n)))
            self.assertEqual(tuple(len(g) for g in groups), sizes, f"n={n}")

    def test_no_group_wider_than_the_cap_and_never_fewer_than_two(self):
        for n in range(2, 40):
            groups = JM.split_groups(list(range(n)))
            self.assertGreaterEqual(len(groups), 2, f"n={n}")
            self.assertLessEqual(max(len(g) for g in groups), JM.GROUP_MAX_DEVICES, f"n={n}")
            self.assertLessEqual(max(map(len, groups)) - min(map(len, groups)), 1, f"n={n}")

    def test_up_to_eight_devices_it_is_the_split_into_two_halves(self):
        """Every RAID 0+1 JM built before the groups were capped was split
        into two halves, the first one taking the odd device. The capped
        split must give the same two legs for the same member list, or a
        restart would read those journals back scrambled."""
        for n in range(2, 9):
            items = [f"d{i}" for i in range(n)]
            half = (n + 1) // 2
            self.assertEqual(JM.split_groups(items), [items[:half], items[half:]], f"n={n}")

    def test_partition_is_complete_in_order_and_disjoint(self):
        items = [f"d{i}" for i in range(11)]
        groups = JM.split_groups(items)
        self.assertEqual([m for g in groups for m in g], items)


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


if __name__ == "__main__":
    unittest.main()
