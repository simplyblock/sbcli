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
    """Exercise the replica filter the way create_migration does: through the
    one shared rule, mc.replica_is_departing."""
    node = _node("replica", status)
    if mc.replica_is_departing(node):
        return None
    return node


class TestOneRuleForBothSides(unittest.TestCase):
    """create_migration used REMOVAL_SHUT_DOWN_STATUSES, the runner's target
    replica lookups DEPARTING_STATUSES: the same PENDING_REMOVAL replica could
    be given a subsystem by the create and then skipped by every runner step.
    Both ask mc.replica_is_departing now, and it means DEPARTING_STATUSES."""

    def test_the_rule_is_the_departing_set(self):
        for status in StorageNode._STATUS_CODE_MAP:
            self.assertEqual(mc.replica_is_departing(_node("r", status)),
                             status in StorageNode.DEPARTING_STATUSES, status)
        for status in StorageNode.DEPARTING_STATUSES:
            self.assertTrue(mc.replica_is_departing(_node("r", status)), status)
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_RESTARTING):
            self.assertFalse(mc.replica_is_departing(_node("r", status)), status)
        self.assertFalse(mc.replica_is_departing(None))

    def test_pending_removal_is_departing_for_the_create_too(self):
        """Its shutdown is seconds away; whatever the create put on it would
        go down with it and be redone."""
        self.assertIsNone(_usable(StorageNode.STATUS_PENDING_REMOVAL))

    def test_the_runner_lookups_use_the_same_rule(self):
        import inspect

        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        for fn in (runner._get_target_secondary_node, runner._get_target_tertiary_node):
            body = inspect.getsource(fn)
            self.assertIn("migration_controller.replica_is_departing(", body, fn.__name__)
            self.assertNotIn("DEPARTING_STATUSES:", body.split('"""')[-1], fn.__name__)
        for fn in (mc._target_replica_for_registration,):
            self.assertIn("replica_is_departing(", inspect.getsource(fn))
        self.assertIn("if replica_is_departing(node):", inspect.getsource(mc.create_migration))


class TestRegistrationSkipsADepartingReplica(unittest.TestCase):
    """The tolerant pre-registration step used the same replica set without
    the filter: it RPC'd the departing node, burnt ~6 s of connect retries
    per volume and moved on. A five-member batch create therefore outlived
    the operator's 30 s client timeout, the operator never saw the answer and
    re-created the migration every minute (2026-09-28, run 8)."""

    def _call(self, status):
        node = _node("replica", status)
        with unittest.mock.patch.object(mc, "db") as db:
            db.get_storage_node_by_id.return_value = node
            return mc._target_replica_for_registration("replica", "secondary", "LVS_1/LVOL_1m")

    def test_a_node_under_removal_is_skipped(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertIsNone(self._call(status), status)

    def test_a_usable_replica_is_returned(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE):
            self.assertIsNotNone(self._call(status), status)


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
        self.assertIn("replica_is_departing(node)", src)  # the one shared rule
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
