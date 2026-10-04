"""Shared-placement auto-migration: data-plane capability probe, monitor
trigger, and upgrade-complete arming.

Regression context (2026-09-04): the only site arming
``shared_placement_migration_pending`` sat in ``update_cluster`` behind a
``restart=True`` parameter that no CLI invocation sets, so every cluster
created before 26.2.5-PRE and upgraded through the CLI stayed on legacy
per-page placement forever. The fix (a) arms the flag in
``upgrade_complete`` (the documented final upgrade step), and (b) lets
``storage_node_monitor`` self-heal already-stranded clusters by probing the
data plane for the runtime shared-placement RPCs via ``rpc_get_methods``.
"""

import inspect
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as snm

CAPABLE = ["bdev_get_bdevs", "distr_shared_placement",
           "jm_set_shared_placement", "distr_write_protection_v2"]
LEGACY_262 = ["bdev_get_bdevs", "distr_shared_placement"]   # no JM RPC yet
LEGACY_25 = ["bdev_get_bdevs"]                              # pre-feature image


def _node(methods=CAPABLE, raise_rpc=False, status=StorageNode.STATUS_ONLINE):
    n = MagicMock()
    n.status = status
    n.get_id.return_value = "node-1234abcd"
    rpc = MagicMock()
    if raise_rpc:
        rpc.rpc_get_methods.side_effect = RuntimeError("connection refused")
    else:
        rpc.rpc_get_methods.return_value = methods
    n.rpc_client.return_value = rpc
    return n


class TestCapabilityProbe(unittest.TestCase):
    """cluster_ops.all_nodes_support_shared_placement"""

    def test_all_nodes_capable(self):
        self.assertTrue(cluster_ops.all_nodes_support_shared_placement(
            [_node(), _node()]))

    def test_mixed_rolling_upgrade_holds_off(self):
        """One node still on a pre-feature image: the flip must wait."""
        self.assertFalse(cluster_ops.all_nodes_support_shared_placement(
            [_node(), _node(methods=LEGACY_25)]))

    def test_262_image_without_jm_rpc_is_not_capable(self):
        """A 26.2.x data plane has the distr RPC but not the JM RPC; the
        R26.3 flip dispatches both, so the probe must require both."""
        self.assertFalse(cluster_ops.all_nodes_support_shared_placement(
            [_node(methods=LEGACY_262)]))

    def test_unreachable_node_counts_as_unsupported(self):
        self.assertFalse(cluster_ops.all_nodes_support_shared_placement(
            [_node(), _node(raise_rpc=True)]))

    def test_empty_method_list_is_unsupported(self):
        self.assertFalse(cluster_ops.all_nodes_support_shared_placement(
            [_node(methods=None)]))

    def test_write_protection_probe_capable(self):
        self.assertTrue(cluster_ops.all_nodes_support_write_protection_v2(
            [_node(), _node()]))

    def test_write_protection_probe_pre_v2_image(self):
        self.assertFalse(cluster_ops.all_nodes_support_write_protection_v2(
            [_node(methods=LEGACY_25)]))


class _MonitorCtx:
    """Patch the monitor's collaborators around _maybe_enable_shared_placement."""

    def __init__(self, nodes, armed, set_result=True, set_raises=False):
        self.nodes = nodes
        self.cluster = MagicMock()
        self.cluster.shared_placement = False
        self.cluster.shared_placement_migration_pending = armed
        self.cluster.write_protection_v2 = False
        self.cluster.write_protection_migration_pending = armed
        self.cluster.is_re_balancing = False
        self.set_result = set_result
        self.set_raises = set_raises

    def __enter__(self):
        self._db = patch.object(snm, "db")
        self.db = self._db.start()
        self.db.get_storage_nodes_by_cluster_id.return_value = self.nodes
        self.db.get_cluster_by_id.return_value = self.cluster
        self._ops = patch.object(snm, "cluster_ops")
        self.ops = self._ops.start()
        if self.set_raises:
            self.ops.set_shared_placement.side_effect = RuntimeError("boom")
            self.ops.switch_write_protection.side_effect = RuntimeError("boom")
        else:
            self.ops.set_shared_placement.return_value = self.set_result
            self.ops.switch_write_protection.return_value = self.set_result
        # The probes delegate to the real implementation semantics in their
        # own tests above; here they are switches the scenarios control.
        self.ops.all_nodes_support_shared_placement.return_value = False
        self.ops.all_nodes_support_write_protection_v2.return_value = False
        return self

    def __exit__(self, *a):
        self._ops.stop()
        self._db.stop()
        return False

    def run(self, status=Cluster.STATUS_ACTIVE):
        snm._maybe_enable_shared_placement(self.cluster, "cl-1", status)

    def run_wp(self, status=Cluster.STATUS_ACTIVE):
        snm._maybe_switch_write_protection(self.cluster, "cl-1", status)


class TestMonitorTrigger(unittest.TestCase):
    """storage_node_monitor._maybe_enable_shared_placement"""

    def test_armed_and_settled_flips_and_disarms(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.run()
        ctx.ops.set_shared_placement.assert_called_once_with("cl-1", enable=True)
        # success disarms the one-shot flag
        ctx.db.atomic_update.assert_called_once()
        mutator = ctx.db.atomic_update.call_args[0][1]
        c = Cluster()
        c.shared_placement_migration_pending = True
        mutator(c)
        self.assertFalse(c.shared_placement_migration_pending)

    def test_unarmed_but_capable_data_plane_self_heals(self):
        """The stranded-customer case: pending was never armed, but every
        node already runs a capable image — the monitor flips anyway."""
        with _MonitorCtx([_node()], armed=False) as ctx:
            ctx.ops.all_nodes_support_shared_placement.return_value = True
            ctx.run()
        ctx.ops.set_shared_placement.assert_called_once_with("cl-1", enable=True)

    def test_unarmed_and_incapable_does_nothing(self):
        """Mid-rolling-upgrade window: settled but the probe says no."""
        with _MonitorCtx([_node()], armed=False) as ctx:
            ctx.run()
        ctx.ops.all_nodes_support_shared_placement.assert_called_once()
        ctx.ops.set_shared_placement.assert_not_called()

    def test_armed_skips_the_probe(self):
        """When armed, the flip must not depend on (or pay for) the probe."""
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.run()
        ctx.ops.all_nodes_support_shared_placement.assert_not_called()

    def test_non_online_node_blocks(self):
        nodes = [_node(), _node(status=StorageNode.STATUS_RESTARTING)]
        with _MonitorCtx(nodes, armed=True) as ctx:
            ctx.run()
        ctx.ops.set_shared_placement.assert_not_called()

    def test_non_active_cluster_blocks(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.run(status=Cluster.STATUS_DEGRADED)
        ctx.ops.set_shared_placement.assert_not_called()

    def test_rebalancing_blocks(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.cluster.is_re_balancing = True
            ctx.run()
        ctx.ops.set_shared_placement.assert_not_called()

    def test_already_shared_does_nothing(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.cluster.shared_placement = True
            ctx.run()
        ctx.ops.set_shared_placement.assert_not_called()
        ctx.db.get_storage_nodes_by_cluster_id.assert_not_called()

    def test_failed_flip_keeps_pending_armed(self):
        """set_shared_placement returning False must NOT disarm — the next
        monitor cycle has to retry."""
        with _MonitorCtx([_node()], armed=True, set_result=False) as ctx:
            ctx.run()
        ctx.db.atomic_update.assert_not_called()

    def test_raising_flip_is_contained_and_keeps_pending(self):
        with _MonitorCtx([_node()], armed=True, set_raises=True) as ctx:
            ctx.run()  # must not propagate
        ctx.db.atomic_update.assert_not_called()


class TestWriteProtectionMonitorTrigger(unittest.TestCase):
    """storage_node_monitor._maybe_switch_write_protection — the sibling
    migration; only the trigger-specific paths are re-tested here since the
    gating structure is shared with the shared-placement tests above."""

    def test_armed_and_settled_switches_and_disarms(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.run_wp()
        ctx.ops.switch_write_protection.assert_called_once_with("cl-1")
        ctx.db.atomic_update.assert_called_once()
        mutator = ctx.db.atomic_update.call_args[0][1]
        c = Cluster()
        c.write_protection_migration_pending = True
        mutator(c)
        self.assertFalse(c.write_protection_migration_pending)

    def test_unarmed_but_capable_self_heals(self):
        with _MonitorCtx([_node()], armed=False) as ctx:
            ctx.ops.all_nodes_support_write_protection_v2.return_value = True
            ctx.run_wp()
        ctx.ops.switch_write_protection.assert_called_once_with("cl-1")

    def test_unarmed_and_incapable_does_nothing(self):
        with _MonitorCtx([_node()], armed=False) as ctx:
            ctx.run_wp()
        ctx.ops.switch_write_protection.assert_not_called()

    def test_already_v2_does_nothing(self):
        with _MonitorCtx([_node()], armed=True) as ctx:
            ctx.cluster.write_protection_v2 = True
            ctx.run_wp()
        ctx.ops.switch_write_protection.assert_not_called()

    def test_failed_switch_keeps_pending_armed(self):
        with _MonitorCtx([_node()], armed=True, set_result=False) as ctx:
            ctx.run_wp()
        ctx.db.atomic_update.assert_not_called()

    def test_raising_switch_is_contained(self):
        with _MonitorCtx([_node()], armed=True, set_raises=True) as ctx:
            ctx.run_wp()  # must not propagate
        ctx.db.atomic_update.assert_not_called()


class TestUpgradeCompleteArming(unittest.TestCase):

    def _run(self, shared, sp_pending, wp_v2=True, wp_pending=False):
        cluster = MagicMock()
        cluster.shared_placement = shared
        cluster.shared_placement_migration_pending = sp_pending
        cluster.write_protection_v2 = wp_v2
        cluster.write_protection_migration_pending = wp_pending
        with patch.object(cluster_ops, "db_controller") as db, \
             patch.object(cluster_ops, "release_upgrades") as rel:
            rel.run_upgrade_complete.return_value = []
            db.get_cluster_by_id.return_value = cluster
            self.assertTrue(cluster_ops.upgrade_complete("cl-1"))
            return db

    @staticmethod
    def _applied_flags(db):
        """Apply every atomic_update mutator to a fresh Cluster and report
        which pending flags ended up set."""
        c = Cluster()
        for call in db.atomic_update.call_args_list:
            call[0][1](c)
        return (c.shared_placement_migration_pending,
                c.write_protection_migration_pending)

    def test_legacy_cluster_arms_shared_placement(self):
        db = self._run(shared=False, sp_pending=False)
        self.assertEqual(self._applied_flags(db), (True, False))

    def test_post_upgrade_cluster_arms_write_protection(self):
        """update_cluster demotes write_protection_v2 to False before the
        rolling restart, so a routine upgrade of a shared cluster must arm
        exactly the write-protection migration."""
        db = self._run(shared=True, sp_pending=False, wp_v2=False)
        self.assertEqual(self._applied_flags(db), (False, True))

    def test_fully_legacy_cluster_arms_both(self):
        db = self._run(shared=False, sp_pending=False, wp_v2=False)
        self.assertEqual(self._applied_flags(db), (True, True))

    def test_fully_migrated_cluster_arms_nothing(self):
        db = self._run(shared=True, sp_pending=False, wp_v2=True)
        db.atomic_update.assert_not_called()

    def test_already_pending_is_not_rearmed(self):
        db = self._run(shared=False, sp_pending=True, wp_v2=False,
                       wp_pending=True)
        db.atomic_update.assert_not_called()


class TestArmingMovedOutOfUpdateCluster(unittest.TestCase):
    """Pin the fix itself: update_cluster's restart-gated tail must no longer
    be the (unreachable) home of the arming."""

    def test_update_cluster_does_not_arm(self):
        src = inspect.getsource(cluster_ops.update_cluster)
        self.assertNotIn("shared_placement_migration_pending = True", src)

    def test_upgrade_complete_arms(self):
        src = inspect.getsource(cluster_ops.upgrade_complete)
        self.assertIn("shared_placement_migration_pending", src)


class TestGetClusterPlacementState(unittest.TestCase):

    def _get(self, shared, pending, wp_v2=False, wp_pending=False):
        cluster = MagicMock()
        cluster.shared_placement = shared
        cluster.shared_placement_migration_pending = pending
        cluster.write_protection_v2 = wp_v2
        cluster.write_protection_migration_pending = wp_pending
        cluster.get_clean_dict.return_value = {}
        with patch.object(cluster_ops, "db_controller") as db:
            db.get_cluster_by_id.return_value = cluster
            return cluster_ops.get_cluster("cl-1")

    def test_placement_states(self):
        self.assertEqual(self._get(True, False)["data_placement"], "per-chunk")
        self.assertEqual(self._get(False, True)["data_placement"],
                         "per-page (migration pending)")
        self.assertEqual(self._get(False, False)["data_placement"],
                         "per-page (legacy)")

    def test_write_protection_states(self):
        self.assertEqual(self._get(True, False, wp_v2=True)["write_protection"],
                         "v2")
        self.assertEqual(
            self._get(True, False, wp_pending=True)["write_protection"],
            "v1 (migration pending)")
        self.assertEqual(self._get(True, False)["write_protection"],
                         "v1 (legacy)")


if __name__ == "__main__":
    unittest.main()
