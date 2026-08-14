# coding=utf-8
"""Unit tests for single-node (non-HA) cluster support.

Covered:
  - lvol_controller.resolve_effective_ha_type: HA requests downgrade to
    single on hosts without a secondary (every lifecycle op must run on
    exactly one node), all other combinations pass through.
  - lvol_controller.role_secondary_ids: never emits empty role ids into
    lvol.nodes (non-HA topologies).
  - snapshot_monitor.sync_delete_peer_ids: ha_type=single snapshots get no
    peer sync deletes (they were never registered on peers); HA snapshots
    get every LVS member except the phase-1 node.
  - storage_node_ops.resolve_enable_ha_jm: single-node clusters force the
    single-local-journal shape at add-node.
  - cluster_ops.is_single_node_activation / activation_minimum_devices:
    1-node clusters activate as non-HA regardless of ha_type and without
    the +1 spare-device requirement.
  - health_controller.check_snap: secondary probe gated on the lvol's
    ha_type, not on node topology.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_monitor


def _snode(node_id="node-1", secondary="", tertiary=""):
    n = StorageNode()
    n.uuid = node_id
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    return n


def _cluster(is_single_node=False, ha_type="ha", ndcs=1, npcs=1):
    c = Cluster()
    c.uuid = "cluster-1"
    c.is_single_node = is_single_node
    c.ha_type = ha_type
    c.distr_ndcs = ndcs
    c.distr_npcs = npcs
    return c


class TestResolveEffectiveHaType(unittest.TestCase):

    def test_ha_without_secondary_downgrades_to_single(self):
        self.assertEqual(
            lvol_controller.resolve_effective_ha_type("ha", _snode()), "single")

    def test_ha_with_secondary_stays_ha(self):
        self.assertEqual(
            lvol_controller.resolve_effective_ha_type("ha", _snode(secondary="sec-1")),
            "ha")

    def test_single_stays_single_regardless_of_secondary(self):
        self.assertEqual(
            lvol_controller.resolve_effective_ha_type("single", _snode(secondary="sec-1")),
            "single")

    def test_single_stays_single_without_secondary(self):
        self.assertEqual(
            lvol_controller.resolve_effective_ha_type("single", _snode()), "single")


class TestRoleSecondaryIds(unittest.TestCase):

    def test_no_roles_yields_empty(self):
        self.assertEqual(lvol_controller.role_secondary_ids(_snode()), [])

    def test_secondary_only(self):
        self.assertEqual(
            lvol_controller.role_secondary_ids(_snode(secondary="sec-1")), ["sec-1"])

    def test_secondary_and_tertiary_in_role_order(self):
        self.assertEqual(
            lvol_controller.role_secondary_ids(
                _snode(secondary="sec-1", tertiary="tert-1")),
            ["sec-1", "tert-1"])

    def test_tertiary_without_secondary_never_emits_empty_string(self):
        # A demoted secondary must not leave "" in lvol.nodes.
        self.assertEqual(
            lvol_controller.role_secondary_ids(_snode(tertiary="tert-1")),
            ["tert-1"])


class TestSyncDeletePeerIds(unittest.TestCase):

    def test_single_lvol_same_node_has_no_peers(self):
        snode = _snode("node-1", secondary="sec-1", tertiary="tert-1")
        self.assertEqual(
            snapshot_monitor.sync_delete_peer_ids("single", snode, "node-1"), [])

    def test_single_lvol_ignores_topology_even_across_nodes(self):
        # Phase-1 completed elsewhere: the home node still owes its own sync
        # delete, but topology peers must NOT be added for a single lvol.
        snode = _snode("node-1", secondary="sec-1")
        self.assertEqual(
            snapshot_monitor.sync_delete_peer_ids("single", snode, "sec-1"),
            ["node-1"])

    def test_ha_lvol_full_member_set_minus_primary(self):
        snode = _snode("node-1", secondary="sec-1", tertiary="tert-1")
        self.assertEqual(
            snapshot_monitor.sync_delete_peer_ids("ha", snode, "node-1"),
            ["sec-1", "tert-1"])

    def test_ha_lvol_phase1_on_secondary(self):
        snode = _snode("node-1", secondary="sec-1", tertiary="tert-1")
        self.assertEqual(
            snapshot_monitor.sync_delete_peer_ids("ha", snode, "sec-1"),
            ["node-1", "tert-1"])

    def test_ha_lvol_without_roles(self):
        snode = _snode("node-1")
        self.assertEqual(
            snapshot_monitor.sync_delete_peer_ids("ha", snode, "node-1"), [])


class TestResolveEnableHaJm(unittest.TestCase):

    def test_single_node_cluster_forces_disabled(self):
        self.assertFalse(storage_node_ops.resolve_enable_ha_jm(
            _cluster(is_single_node=True), True))

    def test_single_node_cluster_disabled_stays_disabled(self):
        self.assertFalse(storage_node_ops.resolve_enable_ha_jm(
            _cluster(is_single_node=True), False))

    def test_multi_node_cluster_passthrough_true(self):
        self.assertTrue(storage_node_ops.resolve_enable_ha_jm(
            _cluster(is_single_node=False), True))

    def test_multi_node_cluster_passthrough_false(self):
        self.assertFalse(storage_node_ops.resolve_enable_ha_jm(
            _cluster(is_single_node=False), False))


class TestSingleNodeActivation(unittest.TestCase):

    def test_flagged_cluster_is_single_node(self):
        self.assertTrue(cluster_ops.is_single_node_activation(
            _cluster(is_single_node=True), [_snode(), _snode("node-2")]))

    def test_one_online_node_is_single_node_even_unflagged(self):
        self.assertTrue(cluster_ops.is_single_node_activation(
            _cluster(is_single_node=False), [_snode()]))

    def test_two_nodes_unflagged_is_not_single_node(self):
        self.assertFalse(cluster_ops.is_single_node_activation(
            _cluster(is_single_node=False), [_snode(), _snode("node-2")]))

    def test_minimum_devices_drops_spare_for_single_node(self):
        cluster = _cluster(ndcs=1, npcs=0)
        self.assertEqual(cluster_ops.activation_minimum_devices(cluster, True), 1)
        self.assertEqual(cluster_ops.activation_minimum_devices(cluster, False), 2)

    def test_minimum_devices_ec21(self):
        cluster = _cluster(ndcs=2, npcs=1)
        self.assertEqual(cluster_ops.activation_minimum_devices(cluster, True), 3)
        self.assertEqual(cluster_ops.activation_minimum_devices(cluster, False), 4)


class TestCheckSnapHaGating(unittest.TestCase):

    def _run_check_snap(self, ha_type, secondary_node_id):
        from simplyblock_core.controllers import health_controller
        snap = MagicMock()
        snap.snap_bdev = "LVS_1/SNAP_1"
        snap.lvol.node_id = "node-1"
        snap.lvol.ha_type = ha_type

        primary = MagicMock()
        primary.secondary_node_id = secondary_node_id
        primary.rpc_client.return_value.get_bdevs.return_value = [{"name": "x"}]
        secondary = MagicMock()
        secondary.rpc_client.return_value.get_bdevs.return_value = [{"name": "x"}]

        db = MagicMock()
        db.get_snapshot_by_id.return_value = snap
        db.get_storage_node_by_id.side_effect = (
            lambda nid: primary if nid == "node-1" else secondary)
        with patch.object(health_controller, "DBController", return_value=db):
            health_controller.check_snap("snap-1")
        return primary, secondary

    def test_single_snap_never_probes_secondary(self):
        primary, secondary = self._run_check_snap("single", "sec-1")
        self.assertTrue(primary.rpc_client.called)
        self.assertFalse(secondary.rpc_client.called)

    def test_ha_snap_probes_secondary(self):
        primary, secondary = self._run_check_snap("ha", "sec-1")
        self.assertTrue(primary.rpc_client.called)
        self.assertTrue(secondary.rpc_client.called)
