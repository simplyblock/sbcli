"""Every path of a shared subsystem gets a disjoint controller-id window.

The host rejects a controller that presents a cntlid it has already seen for
the same subsystem::

    nvme nvme31: Duplicate cntlid 1 with nvme5,
                 subsys ...lvol:78726d0e..., rejecting

and that path is then silently absent — `nvme connect` still reports success
on a later retry ("already connected"), so nothing ever repairs it. In the
2026-08-09 run this permanently cost a path on 78726d0e (608 degraded-path
reports), 3f171cfb and a2d300d3.

Two defects produced the collisions and both are pinned here:

  * the create path used ``1000 * (secondary_index + 1)`` while the restart
    path used ``1 + 1000 * ha_inode_self`` — the same node could be given a
    different window depending on which flow created its subsystem;
  * the path index silently fell back to 0 for a node not found in
    ``lvol.nodes``, putting a replica in the PRIMARY's window.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol


def _node(node_id):
    node = MagicMock()
    node.get_id.return_value = node_id
    return node


class TestCntlidWindows(unittest.TestCase):

    def test_windows_are_disjoint_across_paths(self):
        windows = [lvol_controller.lvol_min_cntlid(i) for i in range(3)]
        self.assertEqual(windows, [1, 1000, 2000])
        self.assertEqual(len(set(windows)), len(windows),
                         "each path must get its own cntlid window")

    def test_windows_do_not_overlap(self):
        # A path may allocate up to LVOL_CNTLID_WINDOW ids before running into
        # the next path's window.
        for i in range(1, 4):
            lo = lvol_controller.lvol_min_cntlid(i)
            nxt = lvol_controller.lvol_min_cntlid(i + 1)
            self.assertGreaterEqual(
                nxt - lo, lvol_controller.LVOL_CNTLID_WINDOW,
                f"window {i} overlaps window {i + 1}")

    def test_primary_window_is_reserved_for_the_primary(self):
        self.assertEqual(lvol_controller.lvol_min_cntlid(0), 1)
        for i in range(1, 4):
            self.assertNotEqual(
                lvol_controller.lvol_min_cntlid(i), 1,
                "a replica must never land in the primary's cntlid window")


class TestPathIndex(unittest.TestCase):

    def setUp(self):
        self.lvol = LVol()
        self.lvol.uuid = "lvol-1"
        self.lvol.node_id = "primary"
        self.lvol.nodes = ["primary", "secondary", "tertiary"]

    def test_index_follows_lvol_nodes_order(self):
        self.assertEqual(lvol_controller._lvol_path_index(self.lvol, _node("primary")), 0)
        self.assertEqual(lvol_controller._lvol_path_index(self.lvol, _node("secondary")), 1)
        self.assertEqual(lvol_controller._lvol_path_index(self.lvol, _node("tertiary")), 2)

    def test_unknown_node_does_not_collapse_onto_the_primary(self):
        idx = lvol_controller._lvol_path_index(self.lvol, _node("stranger"))
        self.assertNotEqual(idx, 0,
                            "an unknown node must not take the primary's window")
        self.assertNotEqual(lvol_controller.lvol_min_cntlid(idx), 1)

    def test_unknown_node_does_not_collide_with_assigned_paths(self):
        idx = lvol_controller._lvol_path_index(self.lvol, _node("stranger"))
        assigned = {lvol_controller.lvol_min_cntlid(i) for i in range(3)}
        self.assertNotIn(lvol_controller.lvol_min_cntlid(idx), assigned)

    def test_leader_is_recognised_without_populated_nodes(self):
        self.lvol.nodes = []
        self.assertEqual(
            lvol_controller._lvol_path_index(self.lvol, _node("primary")), 0,
            "the leader owns window 0 even before `nodes` is populated")

    def test_secondary_index_is_derived_from_path_index(self):
        self.assertEqual(
            lvol_controller._lvol_secondary_index(self.lvol, _node("secondary")), 0)
        self.assertEqual(
            lvol_controller._lvol_secondary_index(self.lvol, _node("tertiary")), 1)


if __name__ == "__main__":
    unittest.main()
