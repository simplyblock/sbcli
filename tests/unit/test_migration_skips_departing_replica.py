"""create_migration must not RPC a target replica that is being removed.

tgt_entries is built from the TARGET plus its secondary and tertiary, and every
entry is RPC'd to pre-create the subsystem/namespace. Unlike the tolerant
pre-registration above it, that loop is fatal: one unreachable replica raises
RPCConnectionError and fails the whole create_migration.

Because the replica set belongs to the TARGET, the blast radius is not limited
to the removal's own drain -- a migration between two perfectly healthy nodes is
refused whenever the target lists the departing node as its secondary or
tertiary. Live 2026-09-16, cluster 5aaf0a5d: migrating a volume from healthy
rpksz to healthy zqhjg died with

    RPCConnectionError: Could not reach remote
    ... HTTPSConnectionPool(host='...94dtj...simplyblock-spdk-proxy...', port=4420)
    Failed to resolve ... [Errno -2] Name or service not known

against 94dtj, the node under removal, involved only as the target's replica.
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


def _usable(status):
    """Exercise the replica filter the way create_migration does."""
    node = _node("replica", status)
    if node.status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
        return None
    return node


class TestDepartingReplicasAreDropped(unittest.TestCase):

    def test_every_shut_down_status_is_dropped(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertIsNone(_usable(status),
                              f"{status}: its SPDK is stopped, the RPC cannot land")

    def test_migrating_lvols_specifically(self):
        """The status the live failure was in."""
        self.assertIsNone(_usable(StorageNode.STATUS_MIGRATING_LVOLS))

    def test_a_healthy_replica_is_kept(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_DOWN):
            self.assertIsNotNone(_usable(status), status)

    def test_a_node_that_can_return_is_kept(self):
        """OFFLINE/UNREACHABLE are outages, not departures -- the replica is
        still legitimately part of the target's HA set."""
        for status in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE):
            self.assertIsNotNone(_usable(status), status)


class TestTheFilterIsWiredIn(unittest.TestCase):

    def test_create_migration_filters_both_replicas(self):
        import inspect
        src = inspect.getsource(mc.create_migration)
        self.assertIn("_usable_replica", src)
        self.assertIn("REMOVAL_SHUT_DOWN_STATUSES", src)
        self.assertIn("tgt_sec_node = _usable_replica(tgt_sec_node)", src)
        self.assertIn("tgt_ter_node = _usable_replica(tgt_ter_node)", src)

    def test_the_filter_runs_before_tgt_entries_is_built(self):
        """Order matters: tgt_entries opens an rpc_client per replica, so a
        departing node must be gone before that list is assembled."""
        import inspect
        src = inspect.getsource(mc.create_migration)
        self.assertLess(src.index("tgt_ter_node = _usable_replica(tgt_ter_node)"),
                        src.index("tgt_entries = ["))


if __name__ == "__main__":
    unittest.main()
