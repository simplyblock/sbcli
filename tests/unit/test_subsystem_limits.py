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


POOL = "pool-1"


def _ns_lvol(nqn, uuid, node_id="n1", status=LVol.STATUS_ONLINE, subsys_max=NS_CAP,
             pool=POOL):
    return SimpleNamespace(node_id=node_id, status=status, nqn=nqn, uuid=uuid,
                           max_namespace_per_subsys=subsys_max, pool_uuid=pool)


def _pick(node_id, lvols, pool=POOL, **kw):
    return get_next_available_subsystem_on_node(node_id, lvols, pool_id=pool, **kw)


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
        self.assertIsNotNone(_pick("n1", lvols))

    def test_join_refused_at_hard_cap_even_with_larger_recorded_max(self):
        # Legacy subsystem recorded with max 64: at 50 active namespaces no
        # further joins are offered.
        lvols = self._subsystem(NS_CAP, subsys_max=64)
        self.assertIsNone(_pick("n1", lvols))

    def test_smaller_recorded_max_still_respected(self):
        lvols = self._subsystem(32, subsys_max=32)
        self.assertIsNone(_pick("n1", lvols))
        lvols = self._subsystem(31, subsys_max=32)
        self.assertIsNotNone(_pick("n1", lvols))


class TestSubsystemPoolAlignment(unittest.TestCase):
    """A shared subsystem belongs to exactly one pool: namespaced lvols only
    join subsystems made up solely of their own pool, fill the most-occupied
    one first, and open a new subsystem only when every subsystem of the
    pool on the node is full."""

    @staticmethod
    def _subsystem(nqn, count, pool=POOL, subsys_max=NS_CAP, **kw):
        return [_ns_lvol(nqn, f"{nqn}-lv{i}", subsys_max=subsys_max, pool=pool, **kw)
                for i in range(count)]

    def test_never_joins_another_pools_subsystem(self):
        lvols = self._subsystem("nqn-A", 1, pool="pool-other")
        self.assertIsNone(_pick("n1", lvols, pool=POOL))
        # ... while the owning pool still can
        self.assertIsNotNone(_pick("n1", lvols, pool="pool-other"))

    def test_only_own_pool_subsystem_is_offered(self):
        lvols = (self._subsystem("nqn-other", 1, pool="pool-other")
                 + self._subsystem("nqn-mine", 1, pool=POOL))
        pick = _pick("n1", lvols, pool=POOL)
        self.assertEqual(pick.nqn, "nqn-mine")

    def test_new_pool_opens_new_subsystem_even_when_others_have_room(self):
        lvols = self._subsystem("nqn-A", 1, pool="pool-a") + self._subsystem("nqn-B", 1, pool="pool-b")
        self.assertIsNone(_pick("n1", lvols, pool="pool-c"))

    def test_legacy_mixed_pool_subsystem_is_frozen(self):
        # Created before alignment was enforced: members from two pools.
        # Neither pool may keep growing it.
        lvols = (self._subsystem("nqn-mixed", 1, pool="pool-a")
                 + [_ns_lvol("nqn-mixed", "b-lv", pool="pool-b")])
        self.assertIsNone(_pick("n1", lvols, pool="pool-a"))
        self.assertIsNone(_pick("n1", lvols, pool="pool-b"))

    def test_in_creation_member_of_another_pool_blocks_join(self):
        # A committed in_creation record is a slot claim AND a pool claim.
        lvols = (self._subsystem("nqn-A", 1, pool=POOL)
                 + [_ns_lvol("nqn-A", "x", pool="pool-other",
                             status=LVol.STATUS_IN_CREATION)])
        self.assertIsNone(_pick("n1", lvols, pool=POOL))

    def test_deleted_member_of_another_pool_does_not_block_join(self):
        lvols = (self._subsystem("nqn-A", 1, pool=POOL)
                 + [_ns_lvol("nqn-A", "x", pool="pool-other",
                             status=LVol.STATUS_IN_DELETION)])
        self.assertEqual(_pick("n1", lvols, pool=POOL).nqn, "nqn-A")

    def test_fills_most_occupied_subsystem_first(self):
        lvols = (self._subsystem("nqn-A", 3) + self._subsystem("nqn-B", 10)
                 + self._subsystem("nqn-C", 7))
        self.assertEqual(_pick("n1", lvols).nqn, "nqn-B")
        # B full -> next fullest (C), never the emptiest while others have room
        lvols = (self._subsystem("nqn-A", 3) + self._subsystem("nqn-B", NS_CAP)
                 + self._subsystem("nqn-C", 7))
        self.assertEqual(_pick("n1", lvols).nqn, "nqn-C")

    def test_new_subsystem_only_when_all_of_pool_are_full(self):
        lvols = (self._subsystem("nqn-A", NS_CAP) + self._subsystem("nqn-B", NS_CAP)
                 + self._subsystem("nqn-other", 1, pool="pool-other"))
        self.assertIsNone(_pick("n1", lvols))
        lvols[-1 - NS_CAP] = _ns_lvol("nqn-B", "gone", status=LVol.STATUS_DELETED)
        self.assertEqual(_pick("n1", lvols).nqn, "nqn-B")

    def test_pick_is_deterministic(self):
        lvols = self._subsystem("nqn-A", 5) + self._subsystem("nqn-B", 5)
        picks = {_pick("n1", lvols).nqn for _ in range(20)}
        self.assertEqual(picks, {"nqn-A"})  # tie -> NQN order

    def test_exclude_nqns_moves_to_next_of_same_pool(self):
        lvols = self._subsystem("nqn-A", 5) + self._subsystem("nqn-B", 4)
        self.assertEqual(_pick("n1", lvols, exclude_nqns={"nqn-A"}).nqn, "nqn-B")

    def test_pool_id_is_required(self):
        with self.assertRaises(TypeError):
            get_next_available_subsystem_on_node("n1", [])  # type: ignore[call-arg]


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
        # subsystem/pool alignment is decided inside the transaction too
        self.assertIn("pool_id=lvol.pool_uuid", tx)

    def test_placement_and_prechecks_use_effective_max(self):
        src = self._src("simplyblock_core/controllers/lvol_controller.py")
        for fn in ("_get_next_3_nodes", "_resolve_lvol_subsystem"):
            self.assertIn("max_subsystems_for_node",
                          self._function_source(src, fn))


if __name__ == "__main__":
    unittest.main()
