"""Unit tests for the hard subsystem limits (2026-08-04):

  - at most constants.MAX_NAMESPACES_PER_SUBSYSTEM (50) lvols (namespaces)
    per nvmf subsystem — bounds caller-supplied max_namespace_per_subsys,
    joins into legacy subsystems recorded with a larger max, and the value
    seeded into new subsystems inside the FDB claim transaction;
  - at most constants.MAX_SUBSYSTEMS_PER_NODE (75) subsystems per node —
    enforced at every surface that SETS max_lvol (configure, add, restart,
    cluster update); admission honours the node's configured max_lvol so
    legacy nodes from releases predating the cap keep provisioning as
    configured (grandfathered).
"""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from simplyblock_core import constants
from simplyblock_core.controllers.lvol_controller import (
    get_next_available_subsystem_on_node,
    max_subsystems_for_node,
)
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode

NS_CAP = constants.MAX_NAMESPACES_PER_SUBSYSTEM
SUBSYS_CAP = constants.MAX_SUBSYSTEMS_PER_NODE


def _node(max_lvol):
    n = MagicMock(spec=StorageNode)
    n.max_lvol = max_lvol
    return n


def _ns_lvol(nqn, uuid, node_id="n1", status=LVol.STATUS_ONLINE, subsys_max=NS_CAP):
    return SimpleNamespace(node_id=node_id, status=status, nqn=nqn, uuid=uuid,
                           max_namespace_per_subsys=subsys_max)


class TestMaxSubsystemsForNode(unittest.TestCase):
    def test_hard_cap_values(self):
        self.assertEqual(NS_CAP, 50)
        self.assertEqual(SUBSYS_CAP, 75)

    def test_legacy_over_cap_max_is_grandfathered(self):
        # A max_lvol above the cap can only come from a record written by a
        # release predating the cap (every configuration surface clamps new
        # values). Such nodes were sized for their configured value and must
        # keep provisioning as configured after an upgrade — the cap is
        # enforced where values are SET, not at admission.
        self.assertEqual(max_subsystems_for_node(_node(max_lvol=100)), 100)
        self.assertEqual(max_subsystems_for_node(_node(max_lvol=SUBSYS_CAP)), SUBSYS_CAP)

    def test_smaller_configured_max_kept(self):
        self.assertEqual(max_subsystems_for_node(_node(max_lvol=50)), 50)
        # max_lvol == 0 historically means "no subsystems"; the cap must not
        # loosen that.
        self.assertEqual(max_subsystems_for_node(_node(max_lvol=0)), 0)


class TestNamespaceJoinCeiling(unittest.TestCase):
    def _subsystem(self, count, subsys_max):
        return [_ns_lvol("nqn-A", f"lv{i}", subsys_max=subsys_max)
                for i in range(count)]

    def test_join_allowed_below_hard_cap(self):
        lvols = self._subsystem(NS_CAP - 1, subsys_max=NS_CAP)
        self.assertIsNotNone(get_next_available_subsystem_on_node("n1", lvols))

    def test_join_refused_at_hard_cap_even_with_larger_recorded_max(self):
        # Legacy subsystem recorded with max 64: at 50 active namespaces no
        # further joins are offered.
        lvols = self._subsystem(NS_CAP, subsys_max=64)
        self.assertIsNone(get_next_available_subsystem_on_node("n1", lvols))

    def test_smaller_recorded_max_still_respected(self):
        lvols = self._subsystem(32, subsys_max=32)
        self.assertIsNone(get_next_available_subsystem_on_node("n1", lvols))
        lvols = self._subsystem(31, subsys_max=32)
        self.assertIsNotNone(get_next_available_subsystem_on_node("n1", lvols))


class TestEnforcementSites(unittest.TestCase):
    """Source-level invariants: the hard caps are applied at every
    enforcement site, including the authoritative FDB claim transaction."""

    @staticmethod
    def _src(rel):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, rel)) as f:
            return f.read()

    @staticmethod
    def _function_source(src, name, indent=""):
        start = src.index(f"\n{indent}def {name}(")
        end = src.find(f"\n{indent}def ", start + 1)
        return src[start:end if end != -1 else len(src)]

    def test_lvol_create_rejects_oversized_subsys_max(self):
        src = self._src("simplyblock_core/controllers/lvol_controller.py")
        self.assertIn("MAX_NAMESPACES_PER_SUBSYSTEM",
                      self._function_source(src, "add_lvol_ha"))

    def test_claim_tx_enforces_both_caps(self):
        src = self._src("simplyblock_core/db_controller.py")
        tx = self._function_source(src, "_claim_lvol_ns_slot_tx", indent="    ")
        self.assertIn("max_subsystems_for_node", tx)
        self.assertIn("MAX_NAMESPACES_PER_SUBSYSTEM", tx)

    def test_placement_and_prechecks_use_effective_max(self):
        src = self._src("simplyblock_core/controllers/lvol_controller.py")
        for fn in ("_get_next_3_nodes", "_resolve_lvol_subsystem"):
            self.assertIn("max_subsystems_for_node",
                          self._function_source(src, fn))


if __name__ == "__main__":
    unittest.main()
