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


class NoReshuffleStepTests(unittest.TestCase):
    """Replica-role reallocation must not be reachable as a drain step.

    It is phase 3b of the removal and depends on phase 3a having freed the
    departing node's own replica slots. Exposed here it ran without 3a, so on a
    cluster whose slots are all occupied it had nowhere to move to and refused
    on a cycle -- retried for four hours (2026-09-26).
    """

    def test_the_module_offers_no_reshuffle_step(self):
        for name in ('start_replica_reshuffle', 'replica_reshuffle_progress'):
            self.assertFalse(
                hasattr(node_drain_steps, name),
                f'{name} is back; 3b outside the removal skips the 3a it needs')
            self.assertNotIn(name, node_drain_steps.__all__)


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
