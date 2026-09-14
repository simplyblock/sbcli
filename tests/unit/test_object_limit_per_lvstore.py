"""Unit tests for the hard per-lvstore object cap (2026-08-04).

An lvstore serves at most constants.MAX_OBJECTS_PER_LVSTORE objects
(lvols + clones + snapshots). Enforced on every create path (lvol create,
snapshot create, clone). Objects count against their owning node — each
node owns exactly one lvstore, so the owning-node count is the per-lvstore
count, and a host temporarily serving a second LVS (takeover) gives each
active lvstore an independent budget.
"""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from simplyblock_core import constants
from simplyblock_core.controllers.lvol_controller import check_lvstore_object_limit
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode

LIMIT = constants.MAX_OBJECTS_PER_LVSTORE


def _node(uuid="n1", mask="0xFF"):
    n = MagicMock(spec=StorageNode)
    n.get_id = MagicMock(return_value=uuid)
    n.spdk_cpu_mask = mask
    return n


def _lvol(node_id="n1", status=LVol.STATUS_ONLINE):
    return SimpleNamespace(node_id=node_id, status=status)


def _snap(node_id="n1", deleted=False):
    return SimpleNamespace(lvol=SimpleNamespace(node_id=node_id), deleted=deleted)


class TestCheckLvstoreObjectLimit(unittest.TestCase):
    def test_limit_is_6000_flat(self):
        # Pinned deliberately: the cap is a product decision, not an
        # implementation detail, so a change to it must be a change to this
        # test too. 6000 -> 12000 on 2026-08-20, back to 6000 on 2026-08-28.
        self.assertEqual(LIMIT, 6000)
        lvols = [_lvol() for _ in range(LIMIT)]
        self.assertIsNotNone(check_lvstore_object_limit(_node(), lvols, []))
        self.assertIsNone(check_lvstore_object_limit(_node(), lvols[:-1], []))

    def test_limit_does_not_scale_with_core_count(self):
        # The former per-core cap scaled with the SPDK core mask; the hard
        # per-lvstore cap must not.
        lvols = [_lvol() for _ in range(LIMIT)]
        self.assertIsNotNone(
            check_lvstore_object_limit(_node(mask="0xFFFFFFFF"), lvols, []))

    def test_snapshots_count_toward_the_limit(self):
        lvols = [_lvol() for _ in range(LIMIT // 2)]
        snaps = [_snap() for _ in range(LIMIT - LIMIT // 2)]
        err = check_lvstore_object_limit(_node(), lvols, snaps)
        self.assertIsNotNone(err)
        self.assertIn(str(LIMIT), err)

    def test_only_owning_node_objects_counted(self):
        # Objects of another node's lvstore never count against this one —
        # this is also what gives a takeover host an independent budget per
        # active LVS.
        lvols = [_lvol(node_id="other") for _ in range(LIMIT)]
        snaps = [_snap(node_id="other") for _ in range(LIMIT)]
        self.assertIsNone(check_lvstore_object_limit(_node(), lvols, snaps))

    def test_deleted_objects_do_not_count(self):
        lvols = [_lvol(status=LVol.STATUS_DELETED) for _ in range(LIMIT)]
        snaps = [_snap(deleted=True) for _ in range(LIMIT)]
        self.assertIsNone(check_lvstore_object_limit(_node(), lvols, snaps))

    def test_enforced_even_without_core_mask(self):
        # The per-core cap skipped nodes with a missing/invalid mask; a hard
        # limit must not have that escape hatch.
        lvols = [_lvol() for _ in range(LIMIT)]
        self.assertIsNotNone(check_lvstore_object_limit(_node(mask=""), lvols, []))
        self.assertIsNotNone(
            check_lvstore_object_limit(_node(mask="not-hex"), lvols, []))

    def test_new_objects_headroom_respected(self):
        lvols = [_lvol() for _ in range(LIMIT - 2)]
        self.assertIsNone(check_lvstore_object_limit(_node(), lvols, [], new_objects=2))
        self.assertIsNotNone(
            check_lvstore_object_limit(_node(), lvols, [], new_objects=3))

    def test_error_message_names_the_node_and_budget(self):
        lvols = [_lvol(node_id="node-abc") for _ in range(LIMIT)]
        err = check_lvstore_object_limit(_node(uuid="node-abc"), lvols, [])
        self.assertIn("node-abc", err)
        self.assertIn(str(LIMIT), err)


class TestCreatePathsEnforceLimit(unittest.TestCase):
    """Source-level invariants: all three create paths call the guard."""

    @staticmethod
    def _src(rel):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, rel)) as f:
            return f.read()

    @staticmethod
    def _function_source(src, name):
        start = src.index(f"\ndef {name}(")
        end = src.find("\ndef ", start + 1)
        return src[start:end if end != -1 else len(src)]

    def test_lvol_create_checks_limit(self):
        src = self._src("simplyblock_core/controllers/lvol_controller.py")
        self.assertIn("check_lvstore_object_limit(host_node",
                      self._function_source(src, "add_lvol_ha"))

    def test_snapshot_create_checks_limit(self):
        src = self._src("simplyblock_core/controllers/snapshot_controller.py")
        self.assertIn("check_lvstore_object_limit(",
                      self._function_source(src, "add"))

    def test_clone_checks_limit(self):
        src = self._src("simplyblock_core/controllers/snapshot_controller.py")
        self.assertIn("check_lvstore_object_limit(",
                      self._function_source(src, "clone"))


if __name__ == "__main__":
    unittest.main()
