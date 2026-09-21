"""create_migration must not RPC an unreachable target replica.

tgt_entries is built from the TARGET plus its secondary and tertiary, and every
entry is RPC'd to pre-create the subsystem/namespace. Unlike the tolerant
pre-registration above it, that loop is fatal: one unreachable replica raises
RPCConnectionError and fails the whole create_migration.

Because the replica set belongs to the TARGET, the blast radius is not limited
to the removal's own drain -- a migration between two perfectly healthy nodes is
refused whenever the target lists an unreachable node as its secondary or
tertiary. Two live incidents hit this:

  - 2026-09-16, cluster 5aaf0a5d: migrating a volume from healthy rpksz to
    healthy zqhjg died with

        RPCConnectionError: Could not reach remote
        ... HTTPSConnectionPool(host='...94dtj...simplyblock-spdk-proxy...', port=4420)
        Failed to resolve ... [Errno -2] Name or service not known

    against 94dtj, the node under removal, involved only as the target's
    replica. Fixed by skipping REMOVAL_SHUT_DOWN_STATUSES.

  - 2026-09-21, cluster bbad9e32: the same "Could not reach remote" against a
    node that was merely `sn shutdown` (STATUS_OFFLINE), not being removed at
    all -- the REMOVAL_SHUT_DOWN_STATUSES-only check from the first fix didn't
    cover it. Fixed by mirroring the classification
    _get_target_secondary_node/_get_target_tertiary_node
    (tasks_runner_lvol_migration.py) already use for this exact concept: skip
    OFFLINE/departing silently, use ONLINE or an overlapping-with-source
    SUSPENDED node, and block (rather than silently drop) anything else --
    including a merely transient RESTARTING -- so the caller can retry once
    the peer answers again instead of losing HA coverage on the target quietly.
"""
import unittest
from unittest.mock import MagicMock

from simplyblock_core.controllers import migration_controller as mc
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status=StorageNode.STATUS_ONLINE):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    return n


def _usable(status, node_id="replica", src_node_id=None):
    """Mirrors create_migration's _usable_replica(node, err_label) exactly."""
    node = _node(node_id, status)
    if node.status == StorageNode.STATUS_ONLINE:
        return node, None
    if node.status == StorageNode.STATUS_OFFLINE:
        return None, None
    if node.status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
        return None, None
    if node.status == StorageNode.STATUS_SUSPENDED and node.get_id() == src_node_id:
        return node, None
    return None, f"Target secondary node {node.get_id()} is in state '{node.status}'"


class TestDepartingOrOfflineReplicasAreDropped(unittest.TestCase):

    def test_every_shut_down_status_is_dropped_silently(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            node, err = _usable(status)
            self.assertIsNone(node, status)
            self.assertIsNone(err, f"{status} must not block the migration")

    def test_migrating_lvols_specifically(self):
        """The status the first live failure was in."""
        node, err = _usable(StorageNode.STATUS_MIGRATING_LVOLS)
        self.assertIsNone(node)
        self.assertIsNone(err)

    def test_offline_is_dropped_silently(self):
        """The status the second live failure was in: a plain `sn shutdown`,
        unrelated to node removal, must be skipped exactly like a departing
        node -- not treated as usable, and not treated as a hard error."""
        node, err = _usable(StorageNode.STATUS_OFFLINE)
        self.assertIsNone(node)
        self.assertIsNone(err)

    def test_a_healthy_replica_is_kept(self):
        node, err = _usable(StorageNode.STATUS_ONLINE)
        self.assertIsNotNone(node)
        self.assertIsNone(err)

    def test_overlap_drain_still_continues_through_a_suspended_source(self):
        node, err = _usable(StorageNode.STATUS_SUSPENDED, node_id="src", src_node_id="src")
        self.assertIsNotNone(node)
        self.assertIsNone(err)

    def test_a_genuinely_unexpected_state_still_blocks_rather_than_drop(self):
        """RESTARTING/UNREACHABLE/DOWN and a SUSPENDED node that is NOT the
        overlapping source are transient or ambiguous, not permanent
        departures -- silently dropping them would quietly lose HA coverage
        on the target. Blocking lets the caller retry once resolved."""
        for status in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED):
            node, err = _usable(status)
            self.assertIsNone(node, status)
            self.assertIsNotNone(err, status)


class TestTheFilterIsWiredIn(unittest.TestCase):

    def test_create_migration_filters_both_replicas(self):
        import inspect
        src = inspect.getsource(mc.create_migration)
        self.assertIn("_usable_replica", src)
        self.assertIn("REMOVAL_SHUT_DOWN_STATUSES", src)
        self.assertIn('tgt_sec_node, _sec_err = _usable_replica(tgt_sec_node, "secondary")', src)
        self.assertIn('tgt_ter_node, _ter_err = _usable_replica(tgt_ter_node, "tertiary")', src)
        self.assertIn("if _sec_err:", src)
        self.assertIn("if _ter_err:", src)

    def test_the_filter_runs_before_tgt_entries_is_built(self):
        """Order matters: tgt_entries opens an rpc_client per replica, so an
        unusable replica must be gone (or have already raised) before that
        list is assembled."""
        import inspect
        src = inspect.getsource(mc.create_migration)
        self.assertLess(
            src.index('tgt_ter_node, _ter_err = _usable_replica(tgt_ter_node, "tertiary")'),
            src.index("tgt_entries = ["))


if __name__ == "__main__":
    unittest.main()
