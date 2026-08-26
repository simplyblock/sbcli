"""A fail-over copy's nsid is claimed by the CONTROL PLANE, not auto-assigned.

Soak case 7 failed six times on the same shape. Twenty volumes share two
subsystems; their fail-over copies must all land in ONE subsystem per group on
the target cluster. When the group is split across two target primaries, each
primary counts only the namespaces it holds itself, hands out an nsid the HA
PEER already gave to a sibling, and the peer's add_ns is rejected:

    Namespace add rejected ... [node a3220281 wanted nsid=1 max_namespaces=10
    holds=[(1, 531e4060-...), (2, b63ed6f1-...), ... (7, ebdd3e0d-...)]]

Two independent defects produced that split, and both are covered here:

  * ``_resolve_lvol_subsystem`` is only ADVISORY -- ``claim_lvol_ns_slot``'s
    transaction may join the lvol to a different subsystem than the one the
    target node was picked against, so the pick is re-derived afterwards.
  * the nsid itself is claimed from the UNION of what every node of the target
    HA set holds, so even a split group cannot collide.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.storage_node import StorageNode

NQN = "nqn.2023-02.io.simplyblock:cl:lvol:shared"


def _lvol(uuid, nqn=NQN, repl_node="", max_ns=10):
    lv = MagicMock(name="lvol-" + uuid)
    lv.uuid = uuid
    lv.nqn = nqn
    lv.get_id.return_value = uuid
    lv.replication_node_id = repl_node
    lv.max_namespace_per_subsys = max_ns
    lv.namespaced = True
    lv.lvol_name = uuid
    return lv


def _node(node_id, namespaces=None, exists=True, status=StorageNode.STATUS_ONLINE,
          max_ns=10, raises=False):
    n = MagicMock(name="node-" + node_id)
    n.get_id.return_value = node_id
    n.status = status
    n.secondary_node_id = ""
    n.tertiary_node_id = ""
    rpc = MagicMock()
    if raises:
        rpc.subsystem_get.side_effect = RuntimeError("connection refused")
    else:
        rpc.subsystem_get.return_value = (
            {"nqn": NQN, "max_namespaces": max_ns,
             "namespaces": namespaces or []} if exists else None)
    n.rpc_client.return_value = rpc
    return n


def _db(nodes):
    db = MagicMock()
    by_id = {n.get_id.return_value: n for n in nodes}

    def _get(node_id):
        if node_id not in by_id:
            raise KeyError(node_id)
        return by_id[node_id]

    db.get_storage_node_by_id.side_effect = _get
    return db


class TestClaimTargetNsid(unittest.TestCase):

    def test_absent_subsystem_leaves_auto_assignment(self):
        primary = _node("P", exists=False)
        nsid = lvol_controller._claim_target_nsid(
            _db([primary]), _lvol("copy-1"), primary)
        self.assertEqual(nsid, 0, "nothing to collide with -- SPDK assigns")

    def test_claims_across_the_union_not_just_the_primary(self):
        """The exact case-7 shape: peer holds 1..7, primary holds none."""
        peer = _node("Q", namespaces=[
            {"nsid": i, "uuid": "sibling-%d" % i} for i in range(1, 8)])
        primary = _node("P", namespaces=[])
        primary.secondary_node_id = "Q"
        nsid = lvol_controller._claim_target_nsid(
            _db([primary, peer]), _lvol("copy-8"), primary)
        self.assertEqual(
            nsid, 8,
            "the primary holds nothing but the peer holds 1..7; auto-assigning "
            "1 here is what the peer rejected")

    def test_reuses_the_lowest_free_slot(self):
        peer = _node("Q", namespaces=[{"nsid": 1, "uuid": "a"},
                                      {"nsid": 3, "uuid": "b"}])
        primary = _node("P", namespaces=[{"nsid": 1, "uuid": "a"}])
        primary.secondary_node_id = "Q"
        self.assertEqual(
            lvol_controller._claim_target_nsid(
                _db([primary, peer]), _lvol("copy-x"), primary), 2)

    def test_own_namespace_is_not_an_obstacle(self):
        """A retry evicts this copy's own namespace, so its slot is free."""
        primary = _node("P", namespaces=[{"nsid": 1, "uuid": "copy-me"},
                                         {"nsid": 2, "uuid": "other"}])
        self.assertEqual(
            lvol_controller._claim_target_nsid(
                _db([primary]), _lvol("copy-me"), primary), 1)

    def test_offline_peer_does_not_shrink_the_claim(self):
        """An offline peer holds no namespaces the add can collide with."""
        peer = _node("Q", namespaces=[{"nsid": 1, "uuid": "a"}],
                     status=StorageNode.STATUS_OFFLINE)
        primary = _node("P", namespaces=[{"nsid": 1, "uuid": "a"},
                                         {"nsid": 2, "uuid": "b"}])
        primary.secondary_node_id = "Q"
        self.assertEqual(
            lvol_controller._claim_target_nsid(
                _db([primary, peer]), _lvol("copy-y"), primary), 3)

    def test_unreadable_node_falls_back_rather_than_guessing(self):
        peer = _node("Q", raises=True)
        primary = _node("P", namespaces=[{"nsid": 1, "uuid": "a"}])
        primary.secondary_node_id = "Q"
        self.assertEqual(
            lvol_controller._claim_target_nsid(
                _db([primary, peer]), _lvol("copy-z"), primary), 0,
            "claiming against a partial view is the bug, not the fix")

    def test_full_subsystem_defers_to_the_node_diagnostic(self):
        primary = _node("P", max_ns=3, namespaces=[
            {"nsid": i, "uuid": "s%d" % i} for i in range(1, 4)])
        self.assertEqual(
            lvol_controller._claim_target_nsid(
                _db([primary]), _lvol("copy-full"), primary), 0)


class TestSiblingAffinity(unittest.TestCase):

    def test_sibling_in_the_same_subsystem_sets_the_target(self):
        cl = MagicMock()
        lv = _lvol("new")
        siblings = [_lvol("old", repl_node="T1"),
                    _lvol("other", nqn="nqn:x", repl_node="T2")]
        self.assertEqual(
            lvol_controller._sibling_replication_node(lv, cl, siblings), "T1")

    def test_other_subsystems_are_not_siblings(self):
        cl = MagicMock()
        lv = _lvol("new")
        others = [_lvol("elsewhere", nqn="nqn:other", repl_node="T2")]
        self.assertEqual(
            lvol_controller._sibling_replication_node(lv, cl, others), "")

    def test_non_namespaced_lvol_has_no_affinity(self):
        cl = MagicMock()
        lv = _lvol("new", max_ns=1)
        lv.namespaced = False
        self.assertEqual(
            lvol_controller._sibling_replication_node(
                lv, cl, [_lvol("old", repl_node="T1")]), "")


class TestRealignAfterClaim(unittest.TestCase):
    """The advisory pick and the transaction can disagree about the subsystem."""

    def setUp(self):
        self.cl = MagicMock()
        patcher = patch.object(lvol_controller, "DBController")
        self.db = patcher.start().return_value
        self.addCleanup(patcher.stop)

    def test_moves_to_the_siblings_target_when_the_claim_rehomed_it(self):
        lv = _lvol("new", repl_node="T2")
        self.db.get_lvols.return_value = [_lvol("old", repl_node="T1")]
        lvol_controller._realign_replication_node_after_claim(lv, self.cl)
        self.assertEqual(lv.replication_node_id, "T1",
                         "a volume the transaction put in this subsystem must "
                         "replicate where the subsystem already replicates")
        lv.write_to_db.assert_called_once()

    def test_no_write_when_the_pick_already_agrees(self):
        lv = _lvol("new", repl_node="T1")
        self.db.get_lvols.return_value = [_lvol("old", repl_node="T1")]
        lvol_controller._realign_replication_node_after_claim(lv, self.cl)
        self.assertEqual(lv.replication_node_id, "T1")
        lv.write_to_db.assert_not_called()

    def test_non_replicated_lvol_is_untouched(self):
        lv = _lvol("new", repl_node="")
        self.db.get_lvols.return_value = [_lvol("old", repl_node="T1")]
        lvol_controller._realign_replication_node_after_claim(lv, self.cl)
        self.assertEqual(lv.replication_node_id, "")
        lv.write_to_db.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class TestSubsystemHomeNode(unittest.TestCase):
    """One shared subsystem lives on exactly one primary per cluster."""

    def _db_with(self, lvols, nodes):
        db = MagicMock()
        db.get_lvols.return_value = lvols
        by_id = {n.get_id.return_value: n for n in nodes}

        def _get(node_id):
            if node_id not in by_id:
                raise KeyError(node_id)
            return by_id[node_id]

        db.get_storage_node_by_id.side_effect = _get
        return db

    def _copy(self, uuid, node_id, nqn=NQN, status="online", deleted=False):
        lv = MagicMock()
        lv.uuid = uuid
        lv.get_id.return_value = uuid
        lv.nqn = nqn
        lv.node_id = node_id
        lv.status = status
        lv.deleted = deleted
        return lv

    def _node(self, node_id, cluster_id):
        n = MagicMock()
        n.get_id.return_value = node_id
        n.cluster_id = cluster_id
        return n

    def test_finds_the_node_already_hosting_the_subsystem(self):
        db = self._db_with(
            [self._copy("c1", "N1")],
            [self._node("N1", "CL_tgt")])
        self.assertEqual(
            lvol_controller._subsystem_home_node(db, NQN, "CL_tgt"), "N1")

    def test_ignores_copies_in_a_different_cluster(self):
        db = self._db_with(
            [self._copy("c1", "N_src")],
            [self._node("N_src", "CL_src")])
        self.assertEqual(
            lvol_controller._subsystem_home_node(db, NQN, "CL_tgt"), "")

    def test_ignores_other_subsystems(self):
        db = self._db_with(
            [self._copy("c1", "N1", nqn="nqn:other")],
            [self._node("N1", "CL_tgt")])
        self.assertEqual(
            lvol_controller._subsystem_home_node(db, NQN, "CL_tgt"), "")

    def test_a_volume_being_deleted_does_not_own_the_subsystem(self):
        from simplyblock_core.models.lvol_model import LVol
        db = self._db_with(
            [self._copy("c1", "N1", status=LVol.STATUS_IN_DELETION)],
            [self._node("N1", "CL_tgt")])
        self.assertEqual(
            lvol_controller._subsystem_home_node(db, NQN, "CL_tgt"), "")


class TestRetireSupersededOriginal(unittest.TestCase):
    """A fail-back must free the slot the original still holds."""

    def setUp(self):
        from simplyblock_core.models.lvol_model import LVol
        self.LVol = LVol
        patcher = patch.object(lvol_controller, "delete_lvol")
        self.delete = patcher.start()
        self.addCleanup(patcher.stop)

    def _rep(self, source, target):
        r = MagicMock()
        r.source_lvol = source
        r.target_lvol = target
        return r

    def _vol(self, uuid, node_id="N1", status="online"):
        lv = MagicMock()
        lv.uuid = uuid
        lv.get_id.return_value = uuid
        lv.node_id = node_id
        lv.status = status
        lv.nqn = NQN
        lv.ns_id = 3
        return lv

    def _db(self, reps, lvols, node_cluster):
        db = MagicMock()
        db.get_lvol_replication_objects.return_value = reps
        by_id = {lv.get_id.return_value: lv for lv in lvols}

        def _get_lvol(uid):
            if uid not in by_id:
                raise KeyError(uid)
            return by_id[uid]

        def _get_node(nid):
            if nid not in node_cluster:
                raise KeyError(nid)
            n = MagicMock()
            n.cluster_id = node_cluster[nid]
            return n

        db.get_lvol_by_id.side_effect = _get_lvol
        db.get_storage_node_by_id.side_effect = _get_node
        return db

    def test_first_failover_deletes_nothing(self):
        """No record names this volume as a copy, so it IS the original."""
        lvol = self._vol("orig")
        db = self._db([], [lvol], {"N1": "CL_src"})
        ok, err = lvol_controller._retire_superseded_original(db, lvol, "CL_tgt")
        self.assertTrue(ok)
        self.assertEqual(err, "")
        self.delete.assert_not_called()

    def test_failback_deletes_the_original_on_the_destination(self):
        original, copy = self._vol("orig"), self._vol("copy", node_id="N2")
        db = self._db([self._rep(original, copy)], [original, copy],
                      {"N1": "CL_src", "N2": "CL_tgt"})
        ok, err = lvol_controller._retire_superseded_original(db, copy, "CL_src")
        self.assertTrue(ok, err)
        self.delete.assert_called_once_with(original)

    def test_an_original_on_another_cluster_is_not_in_the_way(self):
        original, copy = self._vol("orig"), self._vol("copy", node_id="N2")
        db = self._db([self._rep(original, copy)], [original, copy],
                      {"N1": "CL_src", "N2": "CL_tgt"})
        ok, _ = lvol_controller._retire_superseded_original(db, copy, "CL_third")
        self.assertTrue(ok)
        self.delete.assert_not_called()

    def test_already_deleting_original_is_left_alone(self):
        original = self._vol("orig", status=self.LVol.STATUS_IN_DELETION)
        copy = self._vol("copy", node_id="N2")
        db = self._db([self._rep(original, copy)], [original, copy],
                      {"N1": "CL_src", "N2": "CL_tgt"})
        ok, _ = lvol_controller._retire_superseded_original(db, copy, "CL_src")
        self.assertTrue(ok)
        self.delete.assert_not_called()

    def test_a_failed_delete_aborts_the_failback_with_a_clear_reason(self):
        original, copy = self._vol("orig"), self._vol("copy", node_id="N2")
        db = self._db([self._rep(original, copy)], [original, copy],
                      {"N1": "CL_src", "N2": "CL_tgt"})
        self.delete.side_effect = RuntimeError("lvstore restart in progress")
        ok, err = lvol_controller._retire_superseded_original(db, copy, "CL_src")
        self.assertFalse(ok)
        self.assertIn("lvstore restart in progress", err)
        self.assertIn("namespace", err,
                      "the message must say WHY the fail-back cannot proceed")
