# coding=utf-8
"""
test_device_flap_counter.py — unit tests for the per-device flap counter
and failed-state guard added to ``device_controller.device_set_state`` plus
the health-check escalation in ``services.health_check_service``.

The counter advances only when the device's home node spontaneously reports
an unsolicited per-device failure event (cause=CAUSE_LOCAL_FAILURE) on a
device that's currently online and whose parent node is also online.
Operator-driven CLI commands and node-cascade transitions use the default
cause and must not advance the counter. After ``DEVICE_FLAP_LIMIT`` slots
are exhausted, the next countable transition forces the device into
STATUS_FAILED instead of the requested state. A 10 s debounce prevents
error storms from burning multiple slots for a single underlying failure.

STATUS_FAILED has two recognized exits:
  * STATUS_FAILED_AND_MIGRATED via failure data migration
  * STATUS_ONLINE via explicit operator device-restart

All other transitions out of STATUS_FAILED must be rejected.
"""

import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _device(
    dev_id="dev-1",
    status=NVMeDevice.STATUS_ONLINE,
    flap_count=0,
    last_flap_tsc=0.0,
):
    """Build a real NVMeDevice instance so the controller's mutations
    (.flap_count, .last_flap_tsc, .status, .previous_status) actually
    stick — MagicMock would silently accept attribute writes without
    surfacing any sequencing bugs."""
    d = NVMeDevice()
    d.uuid = dev_id
    d.cluster_device_order = 0
    d.status = status
    d.flap_count = flap_count
    d.last_flap_tsc = last_flap_tsc
    d.node_id = "node-1"
    d.cluster_id = "cluster-1"
    return d


def _node(status=StorageNode.STATUS_ONLINE, devices=None):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = "node-1"
    n.status = status
    n.cluster_id = "cluster-1"
    n.nvme_devices = devices or []
    n.rpc_client.return_value = MagicMock()
    return n


class _ControllerEnv:
    """Patches the DB and downstream controllers used by device_set_state.

    device_set_state pulls the device twice: once from
    db.get_storage_device_by_id (must return a stub with .node_id) and
    once from the iteration over snode.nvme_devices (must return the
    real NVMeDevice we want to mutate). Both must reference the same
    object for state to be observable from the test.
    """

    def __enter__(self):
        from simplyblock_core.controllers import device_controller as mod
        self.mod = mod
        self.device = _device()
        self.snode = _node(devices=[self.device])

        self._patches = [
            patch.object(mod, "DBController"),
            patch.object(mod, "device_events"),
            patch.object(mod, "distr_controller"),
            patch.object(mod, "storage_node_ops"),
            patch.object(mod, "tasks_controller"),
        ]
        starts = [p.start() for p in self._patches]
        DBCtor, self.events, self.distr, self.snops, self.tasks = starts
        self.db = MagicMock()
        DBCtor.return_value = self.db
        self.db.get_storage_device_by_id.return_value = self.device
        self.db.get_storage_node_by_id.return_value = self.snode
        # set_state's online path iterates peers; keep it empty.
        self.db.get_storage_nodes_by_cluster_id.return_value = [self.snode]
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()


# ---------------------------------------------------------------------------
# Counter increments only on local-failure cause
# ---------------------------------------------------------------------------


class TestFlapCounterCause(unittest.TestCase):

    def test_default_cause_does_not_count(self):
        """CLI / node cascade flips must not advance the counter."""
        with _ControllerEnv() as env:
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE)
            self.assertTrue(ok)
            self.assertEqual(env.device.flap_count, 0)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_UNAVAILABLE)

    def test_local_failure_increments_counter(self):
        with _ControllerEnv() as env:
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertTrue(ok)
            self.assertEqual(env.device.flap_count, 1)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_UNAVAILABLE)

    def test_node_offline_skips_counter_even_on_local_failure(self):
        """If the parent node is mid-cascade, transitions are collateral."""
        with _ControllerEnv() as env:
            env.snode.status = StorageNode.STATUS_DOWN
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertEqual(env.device.flap_count, 0)

    def test_online_to_failed_does_not_double_count(self):
        """Force-failed transitions are themselves not countable to avoid
        the counter going past the limit recursively."""
        with _ControllerEnv() as env:
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_FAILED,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertEqual(env.device.flap_count, 0)


# ---------------------------------------------------------------------------
# Counter exhaustion forces failed
# ---------------------------------------------------------------------------


class TestFlapCounterExhaustion(unittest.TestCase):

    def test_third_local_failure_forces_failed(self):
        """Two flaps tolerated, the third forces STATUS_FAILED instead of
        the requested state. Each flap is separated by > 10 s so debounce
        doesn't suppress the increments."""
        with _ControllerEnv() as env:
            env.device.flap_count = 2
            # Last flap 11 s ago — outside debounce, so this is the 3rd.
            env.device.last_flap_tsc = time.time() - 11
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_FAILED)
            # Post-failed bookkeeping: failure-migration task was queued.
            env.tasks.add_device_failed_mig_task.assert_called_once_with(
                env.device.uuid)

    def test_second_flap_within_debounce_does_not_advance(self):
        with _ControllerEnv() as env:
            env.device.flap_count = 1
            env.device.last_flap_tsc = time.time() - 1  # 1 s ago
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            # Counter unchanged but status still transitioned.
            self.assertEqual(env.device.flap_count, 1)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_UNAVAILABLE)

    def test_second_flap_after_debounce_advances(self):
        with _ControllerEnv() as env:
            env.device.flap_count = 1
            env.device.last_flap_tsc = time.time() - 11
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertEqual(env.device.flap_count, 2)


# ---------------------------------------------------------------------------
# Failed terminal state guard
# ---------------------------------------------------------------------------


class TestFailedStateGuard(unittest.TestCase):

    def test_failed_to_unavailable_is_rejected(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_UNAVAILABLE,
                cause=env.mod.CAUSE_LOCAL_FAILURE)
            self.assertFalse(ok)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_FAILED)

    def test_failed_to_online_requires_device_restart_cause(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_ONLINE)
            self.assertFalse(ok)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_FAILED)

    def test_failed_to_online_with_device_restart_succeeds(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            env.device.flap_count = 3
            env.device.last_flap_tsc = time.time()
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_ONLINE,
                cause=env.mod.CAUSE_DEVICE_RESTART)
            self.assertTrue(ok)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_ONLINE)
            # Both counter and timestamp wiped.
            self.assertEqual(env.device.flap_count, 0)
            self.assertEqual(env.device.last_flap_tsc, 0.0)

    def test_failed_to_failed_and_migrated_requires_failure_migration_cause(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_FAILED_AND_MIGRATED)
            self.assertFalse(ok)

    def test_failed_to_failed_and_migrated_with_failure_migration_succeeds(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_FAILED_AND_MIGRATED,
                cause=env.mod.CAUSE_FAILURE_MIGRATION)
            self.assertTrue(ok)
            self.assertEqual(
                env.device.status, NVMeDevice.STATUS_FAILED_AND_MIGRATED)

    def test_failed_to_failed_is_idempotent(self):
        """Re-setting an already-failed device to failed should not error
        out — useful so callers don't need to special-case the no-op."""
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_FAILED
            ok = env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_FAILED)
            self.assertTrue(ok)
            self.assertEqual(env.device.status, NVMeDevice.STATUS_FAILED)


# ---------------------------------------------------------------------------
# Restart-path counter reset
# ---------------------------------------------------------------------------


class TestRestartResetsCounter(unittest.TestCase):

    def test_device_restart_to_online_resets_counter(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_UNAVAILABLE
            env.device.flap_count = 2
            env.device.last_flap_tsc = time.time()
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_ONLINE,
                cause=env.mod.CAUSE_DEVICE_RESTART)
            self.assertEqual(env.device.flap_count, 0)
            self.assertEqual(env.device.last_flap_tsc, 0.0)

    def test_default_to_online_does_not_reset_counter(self):
        with _ControllerEnv() as env:
            env.device.status = NVMeDevice.STATUS_UNAVAILABLE
            env.device.flap_count = 2
            env.device.last_flap_tsc = 1234.5
            env.mod.device_set_state(
                env.device.uuid, NVMeDevice.STATUS_ONLINE)
            self.assertEqual(env.device.flap_count, 2)
            self.assertEqual(env.device.last_flap_tsc, 1234.5)


# ---------------------------------------------------------------------------
# Health-check escalation (Option 1)
# ---------------------------------------------------------------------------


class TestHealthCheckEscalation(unittest.TestCase):

    def _setup(self, device_status, node_status):
        from simplyblock_core.services import health_check_service as mod
        device = _device(status=device_status)
        node = _node(status=node_status, devices=[device])

        p_db = patch.object(mod, "db", create=True)
        p_devctl = patch.object(mod, "device_controller")
        p_events = patch.object(mod, "device_events")
        self._patches = [p_db, p_devctl, p_events]
        starts = [p.start() for p in self._patches]
        self.mock_db, self.mock_devctl, self.mock_events = starts
        self.mock_db.get_storage_nodes_by_cluster_id.return_value = [node]
        # Re-read fresh fetch.
        self.mock_db.get_storage_node_by_id.return_value = node
        self.mock_devctl.CAUSE_LOCAL_FAILURE = "local_failure"
        self.mod = mod
        self.device = device
        self.node = node
        return mod

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()

    def test_online_device_on_online_node_escalates_to_unavailable(self):
        mod = self._setup(NVMeDevice.STATUS_ONLINE, StorageNode.STATUS_ONLINE)
        # Pre-condition: device.health_check is True (default).
        self.device.health_check = True
        mod.set_device_health_check("cluster-1", self.device, False)
        self.mock_devctl.device_set_unavailable.assert_called_once_with(
            self.device.get_id(), cause="local_failure")

    def test_already_unhealthy_device_does_not_re_escalate(self):
        """No-op if the new status equals the old status."""
        mod = self._setup(NVMeDevice.STATUS_ONLINE, StorageNode.STATUS_ONLINE)
        self.device.health_check = False
        mod.set_device_health_check("cluster-1", self.device, False)
        self.mock_devctl.device_set_unavailable.assert_not_called()

    def test_offline_node_does_not_escalate(self):
        """Node-cascade window: don't trigger the local-failure path."""
        mod = self._setup(NVMeDevice.STATUS_ONLINE, StorageNode.STATUS_OFFLINE)
        self.device.health_check = True
        mod.set_device_health_check("cluster-1", self.device, False)
        self.mock_devctl.device_set_unavailable.assert_not_called()

    def test_already_unavailable_device_does_not_escalate(self):
        """If the device is already non-online, the health_check flip is
        a status reflection, not a fresh failure."""
        mod = self._setup(
            NVMeDevice.STATUS_UNAVAILABLE, StorageNode.STATUS_ONLINE)
        self.device.health_check = True
        mod.set_device_health_check("cluster-1", self.device, False)
        self.mock_devctl.device_set_unavailable.assert_not_called()

    def test_health_check_recovery_does_not_escalate(self):
        """False→True is a recovery signal, not a failure."""
        mod = self._setup(NVMeDevice.STATUS_ONLINE, StorageNode.STATUS_ONLINE)
        self.device.health_check = False
        mod.set_device_health_check("cluster-1", self.device, True)
        self.mock_devctl.device_set_unavailable.assert_not_called()


if __name__ == "__main__":
    unittest.main()
