"""The two removal phases a Kubernetes drain drives on its own.

Each is a POST that starts the work and a GET that reports it. The tests cover
what that shape gets wrong: starting the same work twice, calling it done while
it is still running, and reporting success for a step that gave up -- each of
which ends with a node deleted while something still depends on it.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import node_drain_steps
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _device(status):
    dev = MagicMock(spec=NVMeDevice)
    dev.status = status
    dev.get_id = MagicMock(return_value=f"dev-{id(dev)}")
    return dev


def _node(node_id="n1", devices=(), secondary="", tertiary="", cluster_id="c1",
          status=StorageNode.STATUS_ONLINE):
    node = MagicMock(spec=StorageNode)
    node.get_id = MagicMock(return_value=node_id)
    node.nvme_devices = list(devices)
    node.secondary_node_id = secondary
    node.tertiary_node_id = tertiary
    node.cluster_id = cluster_id
    # Explicit, because start_device_decommission reads it to decide whether the
    # node still needs stamping as departing; left as a child mock it is truthy
    # and compares equal to nothing, so the stamp would always fire.
    node.status = status
    return node


class DeviceDecommissionTests(unittest.TestCase):
    def setUp(self):
        node_drain_steps._reset_for_test()
        self.addCleanup(node_drain_steps._reset_for_test)

    def _with_db(self, node, cluster_nodes=None):
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(return_value=node)
        db.get_storage_nodes_by_cluster_id = MagicMock(return_value=cluster_nodes or [node])
        return patch.object(node_drain_steps, 'DBController', MagicMock(return_value=db))

    def _stamping_suppressed(self):
        """The shutdown and the departing stamp both go through the real
        control plane; these tests are about the poll loop, not either write.

        shutdown_storage_node is stubbed to True: the step now stops the node
        before rebuilding its devices, and a False here would (correctly) abort
        the step before the loop under test ever runs.
        """
        return patch.multiple(
            'simplyblock_core.storage_node_ops',
            set_node_status=MagicMock(),
            shutdown_storage_node=MagicMock(return_value=True),
        )

    def test_a_rebuild_already_running_is_not_started_again(self):
        """Re-POSTing must not restart the work the caller is waiting for."""
        release = threading.Event()
        calls = []

        def slow(_node):
            calls.append(1)
            release.wait(timeout=5)
            return True

        node = _node(devices=[_device(NVMeDevice.STATUS_ONLINE)])
        with self._with_db(node), self._stamping_suppressed(), \
                patch('simplyblock_core.storage_node_ops._decommission_node_devices', slow):
            self.assertTrue(node_drain_steps.start_device_decommission("n1"),
                            "the first call should have started the rebuild")
            for _ in range(50):
                if calls:
                    break
                time.sleep(0.02)
            self.assertFalse(node_drain_steps.start_device_decommission("n1"),
                             "a second call restarted a rebuild that was already running")
            release.set()
        self.assertEqual(len(calls), 1, f"the rebuild ran {len(calls)} times, want 1")

    def test_progress_counts_migrated_devices_and_ignores_the_journal(self):
        node = _node(devices=[
            _device(NVMeDevice.STATUS_FAILED_AND_MIGRATED),
            _device(NVMeDevice.STATUS_FAILED_AND_MIGRATED),
            _device(NVMeDevice.STATUS_FAILED),
            _device(NVMeDevice.STATUS_JM),          # not data, never counted
        ])
        with self._with_db(node):
            progress = node_drain_steps.device_decommission_progress("n1")

        self.assertEqual(progress['total'], 3, "the journal device was counted as data")
        self.assertEqual(progress['completed'], 2)
        self.assertFalse(progress['done'], "reported done with a device still unmigrated")

    def test_all_devices_migrated_and_nothing_running_is_done(self):
        node = _node(devices=[
            _device(NVMeDevice.STATUS_FAILED_AND_MIGRATED),
            _device(NVMeDevice.STATUS_JM),
        ])
        with self._with_db(node):
            progress = node_drain_steps.device_decommission_progress("n1")

        self.assertTrue(progress['done'])
        self.assertEqual(progress['failed'], 0)

    def test_a_step_that_raised_reports_failed_rather_than_done(self):
        """A rebuild that died must not read as finished.

        The counts alone would say so the moment the devices happened to line
        up, and the caller's next move is deleting the node.
        """
        node = _node(devices=[_device(NVMeDevice.STATUS_FAILED_AND_MIGRATED)])

        def boom(_node):
            raise RuntimeError("rebuild exploded")

        with self._with_db(node), self._stamping_suppressed(), \
                patch('simplyblock_core.storage_node_ops._decommission_node_devices', boom):
            node_drain_steps.start_device_decommission("n1")
            for _ in range(50):
                if node_drain_steps._last_error.get(("n1", 'devices')):
                    break
                time.sleep(0.02)
            progress = node_drain_steps.device_decommission_progress("n1")

        self.assertEqual(progress['failed'], 1,
                         "a step that raised was not reported as failed")
        self.assertIn("rebuild exploded", progress['message'])


class ReplicaReshuffleTests(unittest.TestCase):
    def setUp(self):
        node_drain_steps._reset_for_test()
        self.addCleanup(node_drain_steps._reset_for_test)

    def _with_db(self, node, cluster_nodes):
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(return_value=node)
        db.get_storage_nodes_by_cluster_id = MagicMock(return_value=cluster_nodes)
        return patch.object(node_drain_steps, 'DBController', MagicMock(return_value=db))

    def test_a_peer_still_pointing_here_is_not_done(self):
        """The question before deleting a node is whether anything still names
        it, and a peer holding it as secondary is exactly that."""
        dying = _node("n1")
        holder = _node("n2", secondary="n1")
        bystander = _node("n3", secondary="n4")

        with self._with_db(dying, [dying, holder, bystander]):
            progress = node_drain_steps.replica_reshuffle_progress("n1")

        self.assertFalse(progress['done'],
                         "reported done while a peer still held a replica role here")
        self.assertEqual(progress['total'], 1)
        self.assertIn("n2", progress['message'])

    def test_a_tertiary_pointing_here_counts_too(self):
        dying = _node("n1")
        holder = _node("n2", secondary="n5", tertiary="n1")

        with self._with_db(dying, [dying, holder]):
            progress = node_drain_steps.replica_reshuffle_progress("n1")

        self.assertFalse(progress['done'], "a tertiary back-reference was missed")

    def test_no_remaining_back_references_is_done(self):
        dying = _node("n1")
        peer = _node("n2", secondary="n3")

        with self._with_db(dying, [dying, peer]):
            progress = node_drain_steps.replica_reshuffle_progress("n1")

        self.assertTrue(progress['done'])
        self.assertEqual(progress['total'], 0)

    def test_the_node_itself_is_not_counted_as_holding_its_own_role(self):
        """A node whose own secondary pointer still names itself is not a peer
        depending on it, and counting it would block the drain forever."""
        dying = _node("n1", secondary="n1")

        with self._with_db(dying, [dying]):
            progress = node_drain_steps.replica_reshuffle_progress("n1")

        self.assertTrue(progress['done'],
                        "the node counted its own pointer and would never finish")


if __name__ == '__main__':
    unittest.main()


class ShutdownBeforeDrainTests(unittest.TestCase):
    """Every removal stops the node before it moves anything.

    The CLI removal always did; the drain used to leave it serving and stamp
    PENDING_REMOVAL instead. That made MIGRATING_* mean "down" on one path and
    "up" on the other, so every check asking "can this node still answer?" was
    right for one path and wrong for the other -- found one at a time, each on a
    live cluster.
    """

    def setUp(self):
        node_drain_steps._reset_for_test()
        self.addCleanup(node_drain_steps._reset_for_test)

    def _run(self, status, shutdown_result=True):
        node = _node(status=status, devices=[_device(NVMeDevice.STATUS_ONLINE)])
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(return_value=node)
        calls = []
        shutdown = MagicMock(side_effect=lambda *a, **k: (calls.append('shutdown'),
                                                          shutdown_result)[1])
        stamp = MagicMock(side_effect=lambda *a, **k: calls.append(('stamp', a[1])))
        decommission = MagicMock(side_effect=lambda *a: (calls.append('devices'), True)[1])
        with patch.object(node_drain_steps, 'DBController', MagicMock(return_value=db)), \
             patch.multiple('simplyblock_core.storage_node_ops',
                            shutdown_storage_node=shutdown,
                            set_node_status=stamp,
                            _decommission_node_devices=decommission):
            started = node_drain_steps.start_device_decommission("n1")
            for _ in range(50):
                if 'devices' in calls or not started:
                    break
                time.sleep(0.02)
        return started, calls

    def test_the_node_is_shut_down_before_its_devices_are_rebuilt(self):
        started, calls = self._run(StorageNode.STATUS_ONLINE)
        self.assertTrue(started)
        self.assertEqual(calls[0], 'shutdown',
                         f"the rebuild started without stopping the node: {calls}")
        self.assertIn(('stamp', StorageNode.STATUS_MIGRATING_DEVICES), calls)

    def test_a_failed_shutdown_stops_the_step(self):
        """Rebuilding a live node's devices out from under it is the thing the
        shutdown exists to prevent, so a failed shutdown must not proceed."""
        started, calls = self._run(StorageNode.STATUS_ONLINE, shutdown_result=False)
        self.assertFalse(started)
        self.assertNotIn('devices', calls)

    def test_a_node_already_shut_down_is_not_shut_down_again(self):
        _, calls = self._run(StorageNode.STATUS_MIGRATING_DEVICES)
        self.assertNotIn('shutdown', calls,
                         "re-POSTing the step tried to stop an already-stopped node")
