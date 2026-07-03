# coding=utf-8
"""
test_node_online_device_readmit.py — regression tests for the 2026-07-02
suspend series (three cluster suspensions, one mechanism).

Mechanism: ``device_set_state`` has a *stale re-online guard* — a device may
only go ONLINE while its parent node is ONLINE. For a node recovery WITHOUT a
full restart (network outage, DOWN fast-clear) no path re-onlined the node's
devices:

- ``port_allow`` runs ~2s BEFORE the monitor flips the node ONLINE, so its
  device re-admit hit the guard and was refused — silently, because
  ``device_set_online`` returns ``False`` instead of raising and the caller
  did not check it.
- ``device_monitor`` auto-restart only touches ``io_error`` devices.

The node then reads ONLINE with devices stuck unavailable, counts toward
``affected_nodes`` forever, and the next unrelated dual outage pushes
``affected_nodes > k`` → cluster SUSPENDED.

Fix under test: ``storage_node_monitor.readmit_devices_after_node_online``,
called right after the DOWN/UNREACHABLE → ONLINE clear, where the guard passes
by construction.

Test-design note (the five-iteration lesson): flow tests that mock out
``device_set_online`` hide the stale re-online guard entirely — every previous
fix passed such tests and was then refused by the guard in production. The
``_RealGuard*`` tests below therefore run the REAL
``device_controller.device_set_state`` with only the DB / event / RPC
boundaries faked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _device(uuid, node_id, status, io_error=False, retries_exhausted=False,
            order=0):
    d = NVMeDevice()
    d.uuid = uuid
    d.node_id = node_id
    d.status = status
    d.io_error = io_error
    d.retries_exhausted = retries_exhausted
    d.cluster_device_order = order
    return d


def _node(uuid, status):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = "c1"
    n.nvme_devices = []
    return n


class _FakeDB:
    """In-memory stand-in for DBController covering exactly the calls the
    guard-path code makes."""

    def __init__(self, nodes, cluster=None):
        self.nodes = {n.get_id(): n for n in nodes}
        self.cluster = cluster

    def get_cluster_by_id(self, cluster_id):
        return self.cluster

    def get_primary_storage_nodes_by_cluster_id(self, cluster_id):
        return list(self.nodes.values())

    def get_storage_node_by_id(self, node_id):
        if node_id not in self.nodes:
            raise KeyError(f"node {node_id} not found")
        return self.nodes[node_id]

    def get_storage_device_by_id(self, device_id):
        for n in self.nodes.values():
            for d in n.nvme_devices:
                if d.get_id() == device_id:
                    return d
        raise KeyError(f"device {device_id} not found")

    def get_storage_nodes_by_cluster_id(self, cluster_id):
        return list(self.nodes.values())

    def atomic_update(self, obj, fn):
        fn(obj)
        return obj


class _RealGuardBase(unittest.TestCase):
    """Run the REAL ``device_set_state`` (stale re-online guard, FAILED
    transition table, flap counter) against in-memory models; fake only the
    persistence / event / RPC boundaries."""

    def setUp(self):
        self.node = _node("node-a", StorageNode.STATUS_DOWN)
        self.dev = _device("dev-1", "node-a", NVMeDevice.STATUS_UNAVAILABLE)
        self.node.nvme_devices = [self.dev]
        self.db = _FakeDB([self.node])

        def _apply(dbc, node_id, device_id, fn):
            fn(self.db.get_storage_device_by_id(device_id))

        base = "simplyblock_core.controllers.device_controller"
        for target, kw in [
            (f"{base}.DBController", {"return_value": self.db}),
            (f"{base}._atomic_device_set", {"side_effect": _apply}),
            (f"{base}.device_events", {}),
            (f"{base}.distr_controller", {}),
            (f"{base}.tasks_controller", {}),
            (f"{base}.storage_node_ops", {}),
        ]:
            p = patch(target, **kw)
            p.start()
            self.addCleanup(p.stop)


class TestStaleReonlineGuardInteraction(_RealGuardBase):
    """The two sides of the guard that the port-allow fixes tripped over."""

    def test_readmit_refused_while_node_not_online(self):
        # This is the port_allow-time call of the incident: node still DOWN.
        from simplyblock_core.controllers import device_controller
        ret = device_controller.device_set_online("dev-1")
        self.assertFalse(
            ret,
            "device_set_online must refuse (return False) while the parent "
            "node is not ONLINE — the stale re-online guard")
        self.assertEqual(
            self.dev.status, NVMeDevice.STATUS_UNAVAILABLE,
            "a refused re-admit must not change the device status")

    def test_readmit_succeeds_once_node_online(self):
        from simplyblock_core.controllers import device_controller
        self.node.status = StorageNode.STATUS_ONLINE
        ret = device_controller.device_set_online("dev-1")
        self.assertTrue(ret)
        self.assertEqual(
            self.dev.status, NVMeDevice.STATUS_ONLINE,
            "with the node ONLINE the same call must bring the device online")

    def test_failed_device_stays_failed_even_with_node_online(self):
        from simplyblock_core.controllers import device_controller
        self.node.status = StorageNode.STATUS_ONLINE
        self.dev.status = NVMeDevice.STATUS_FAILED
        ret = device_controller.device_set_online("dev-1")
        self.assertFalse(
            ret,
            "FAILED is terminal for automatic recovery; only an explicit "
            "device restart may bring it back")
        self.assertEqual(self.dev.status, NVMeDevice.STATUS_FAILED)


class TestEndToEndRecoveryChain(_RealGuardBase):
    """The full incident sequence against the REAL guard: port_allow-time
    re-admit refused (node DOWN), then the monitor's node-ONLINE clear must
    re-admit for real. This is the test that would have caught fixes 1–4."""

    def test_port_allow_refused_then_monitor_clear_readmits(self):
        from simplyblock_core.controllers import device_controller
        from simplyblock_core.services import storage_node_monitor as mon

        # t0: port_allow fires ~2s before the node flips ONLINE → refused.
        self.assertFalse(device_controller.device_set_online("dev-1"))
        self.assertEqual(self.dev.status, NVMeDevice.STATUS_UNAVAILABLE)

        # t0+2s: the monitor clears the node to ONLINE and must then own the
        # device re-online — with the guard passing for real.
        self.node.status = StorageNode.STATUS_ONLINE
        with patch.object(mon, "db", self.db):
            mon.readmit_devices_after_node_online("node-a")
        self.assertEqual(
            self.dev.status, NVMeDevice.STATUS_ONLINE,
            "after the node-ONLINE clear the device must be online — this is "
            "the recovery path that was missing for five iterations")


class TestMonitorClearReadmit(unittest.TestCase):
    """Behavior of readmit_devices_after_node_online itself (boundaries
    mocked; the guard interaction is covered by the _RealGuard tests)."""

    def setUp(self):
        self.node = _node("node-a", StorageNode.STATUS_ONLINE)
        self.devs = {
            "dev-unavail": _device(
                "dev-unavail", "node-a", NVMeDevice.STATUS_UNAVAILABLE),
            "dev-ioerr": _device(
                "dev-ioerr", "node-a", NVMeDevice.STATUS_UNAVAILABLE,
                io_error=True),
            "dev-online": _device(
                "dev-online", "node-a", NVMeDevice.STATUS_ONLINE),
            "dev-removed": _device(
                "dev-removed", "node-a", NVMeDevice.STATUS_REMOVED),
            "dev-failed": _device(
                "dev-failed", "node-a", NVMeDevice.STATUS_FAILED),
        }
        self.node.nvme_devices = list(self.devs.values())
        self.db = _FakeDB([self.node])

        from simplyblock_core.services import storage_node_monitor as mon
        self.mon = mon
        self.set_online = MagicMock(return_value=True)
        for target, kw in [
            ("db", {"new": self.db}),
            ("device_controller", {"new": MagicMock(
                device_set_online=self.set_online)}),
        ]:
            p = patch.object(mon, target, **kw)
            p.start()
            self.addCleanup(p.stop)

    def _readmitted(self):
        return [c.args[0] for c in self.set_online.call_args_list]

    def test_readmits_every_non_terminal_device(self):
        self.mon.readmit_devices_after_node_online("node-a")
        self.assertIn("dev-unavail", self._readmitted())
        self.assertIn(
            "dev-ioerr", self._readmitted(),
            "io_error devices are re-admitted too: the node passed full "
            "health checks, so every non-terminal device must serve")

    def test_terminal_and_online_devices_untouched(self):
        self.mon.readmit_devices_after_node_online("node-a")
        for dev_id in ("dev-online", "dev-removed", "dev-failed"):
            self.assertNotIn(
                dev_id, self._readmitted(),
                f"{dev_id} must not be re-admitted by the node-ONLINE clear")

    def test_refusal_is_logged_not_raised(self):
        self.set_online.return_value = False
        with self.assertLogs(level="ERROR") as logs:
            self.mon.readmit_devices_after_node_online("node-a")
        self.assertTrue(
            any("was refused" in line for line in logs.output),
            "a refused re-admit must be loudly logged — the five-iteration "
            "bug survived because the refusal was silent")

    def test_db_error_does_not_propagate(self):
        # The clear-to-ONLINE path must never crash the monitor loop.
        self.mon.readmit_devices_after_node_online("no-such-node")


class TestMonitorClearCallsReadmit(unittest.TestCase):
    """The fast-clear block in check_node must actually invoke the re-admit —
    guards against the call being dropped in a refactor."""

    def test_source_wires_clear_to_readmit(self):
        import inspect
        from simplyblock_core.services import storage_node_monitor as mon
        src = inspect.getsource(mon.check_node)
        idx_clear = src.find("clearing to ONLINE")
        idx_call = src.find("readmit_devices_after_node_online", idx_clear)
        self.assertGreater(idx_clear, -1)
        self.assertGreater(
            idx_call, idx_clear,
            "check_node's DOWN/UNREACHABLE→ONLINE clear must call "
            "readmit_devices_after_node_online right after set_node_status")


def _cluster(n=1, k=1):
    c = Cluster()
    c.uuid = "c1"
    c.status = Cluster.STATUS_ACTIVE
    c.distr_ndcs = n
    c.distr_npcs = k
    c.enable_failure_domain = False
    c.strict_node_anti_affinity = False
    return c


def _online_node(uuid):
    n = _node(uuid, StorageNode.STATUS_ONLINE)
    n.nvme_devices = [
        _device(f"{uuid}-d0", uuid, NVMeDevice.STATUS_ONLINE, order=0),
        _device(f"{uuid}-d1", uuid, NVMeDevice.STATUS_ONLINE, order=1),
    ]
    # Falsy lvstore list → get_next_cluster_status skips the JM-replication RPC.
    n.rpc_client = MagicMock(return_value=MagicMock(
        bdev_lvol_get_lvstores=MagicMock(return_value=[])))
    return n


class TestUnreachableCountsOnlyOnDataPlaneLoss(unittest.TestCase):
    """Suspension must be driven by data-plane truth. A node UNREACHABLE on the
    mgmt plane (API blip, mgmt-NIC flap) whose NVMe-TCP targets still serve IO
    must NOT count toward affected_nodes — clients see zero impact. It counts
    once a peer majority actually lost its data plane (or once its devices are
    flipped non-online, which the device branch already covers)."""

    def setUp(self):
        self.nodes = [_online_node(f"node-{i}") for i in range(4)]
        self.db = _FakeDB(self.nodes, cluster=_cluster(n=1, k=1))

        from simplyblock_core.services import storage_node_monitor as mon
        self.mon = mon
        self.probe = MagicMock(return_value=False)
        for target, kw in [
            ("db", {"new": self.db}),
            ("is_node_data_plane_disconnected_quorum", {"new": self.probe}),
            ("is_new_migrated_node", {"new": MagicMock(return_value=False)}),
        ]:
            p = patch.object(mon, target, **kw)
            p.start()
            self.addCleanup(p.stop)

    def test_mgmt_unreachable_with_live_data_plane_does_not_count(self):
        self.nodes[0].status = StorageNode.STATUS_UNREACHABLE
        self.probe.return_value = False  # peers still see its data plane
        status = self.mon.get_next_cluster_status("c1")
        self.assertEqual(
            status, Cluster.STATUS_ACTIVE,
            "an UNREACHABLE node whose storage network still serves must not "
            "degrade or suspend the cluster")

    def test_unreachable_with_lost_data_plane_counts(self):
        self.nodes[0].status = StorageNode.STATUS_UNREACHABLE
        self.probe.return_value = True  # peer majority lost its data plane
        status = self.mon.get_next_cluster_status("c1")
        self.assertEqual(
            status, Cluster.STATUS_DEGRADED,
            "once the data plane is confirmed lost, the node counts toward "
            "the FTT bucket (affected == k → degraded)")

    def test_unreachable_data_plane_loss_plus_device_outage_suspends(self):
        self.nodes[0].status = StorageNode.STATUS_UNREACHABLE
        self.probe.return_value = True
        # Second, unrelated node with an offline device → affected 2 > k 1.
        self.nodes[1].nvme_devices[0].status = NVMeDevice.STATUS_UNAVAILABLE
        status = self.mon.get_next_cluster_status("c1")
        self.assertEqual(status, Cluster.STATUS_SUSPENDED)

    def test_incident_shape_mgmt_blip_during_device_outage_stays_degraded(self):
        # The false-suspend shape: a device outage at exactly design tolerance
        # (affected == k) plus a third node that is merely mgmt-unreachable.
        # Before the gate this suspended the cluster; now it must not.
        self.nodes[1].nvme_devices[0].status = NVMeDevice.STATUS_UNAVAILABLE
        self.nodes[0].status = StorageNode.STATUS_UNREACHABLE
        self.probe.return_value = False
        status = self.mon.get_next_cluster_status("c1")
        self.assertEqual(status, Cluster.STATUS_DEGRADED)


class TestStrandedDeviceReconciler(unittest.TestCase):
    """The backstop sweep: no UNAVAILABLE (io_error=False) device may survive
    on an ONLINE node past the grace window, regardless of HOW it got
    stranded. This closes the entry points nobody has hit yet, and one-shot
    failures of the targeted fixes."""

    def setUp(self):
        self.node = _node("node-a", StorageNode.STATUS_ONLINE)
        self.dev = _device("dev-1", "node-a", NVMeDevice.STATUS_UNAVAILABLE)
        self.node.nvme_devices = [self.dev]
        self.db = _FakeDB([self.node])

        from simplyblock_core.services import storage_node_monitor as mon
        self.mon = mon
        mon._stranded_first_seen.clear()
        self.addCleanup(mon._stranded_first_seen.clear)

        self.set_online = MagicMock(return_value=True)
        for target, kw in [
            ("db", {"new": self.db}),
            ("device_controller", {"new": MagicMock(
                device_set_online=self.set_online)}),
            # Grace elapsed by default; individual tests raise it again.
            ("STRANDED_DEVICE_READMIT_GRACE_SEC", {"new": 0}),
        ]:
            p = patch.object(mon, target, **kw)
            p.start()
            self.addCleanup(p.stop)

    def test_stranded_device_is_readmitted_after_grace(self):
        self.mon._readmit_stranded_devices("c1")
        self.set_online.assert_called_once_with("dev-1")

    def test_not_readmitted_before_grace(self):
        with patch.object(self.mon, "STRANDED_DEVICE_READMIT_GRACE_SEC", 3600):
            self.mon._readmit_stranded_devices("c1")
        self.set_online.assert_not_called()
        # ...but the sighting is recorded so a later tick can act on it.
        self.assertIn(
            ("c1", "node-a", "dev-1"), self.mon._stranded_first_seen)

    def test_io_error_and_exhausted_devices_left_to_their_owners(self):
        self.dev.io_error = True
        self.mon._readmit_stranded_devices("c1")
        self.set_online.assert_not_called()
        self.dev.io_error = False
        self.dev.retries_exhausted = True
        self.mon._readmit_stranded_devices("c1")
        self.set_online.assert_not_called()

    def test_devices_on_non_online_nodes_untouched(self):
        # Mid-outage nodes are not the reconciler's business.
        self.node.status = StorageNode.STATUS_UNREACHABLE
        self.mon._readmit_stranded_devices("c1")
        self.set_online.assert_not_called()

    def test_refused_readmit_retries_next_tick(self):
        self.set_online.return_value = False
        with self.assertLogs(level="ERROR"):
            self.mon._readmit_stranded_devices("c1")
        self.assertIn(
            ("c1", "node-a", "dev-1"), self.mon._stranded_first_seen,
            "a refused re-admit must stay tracked so the next tick retries")

    def test_recovered_device_forgotten_for_fresh_grace(self):
        with patch.object(self.mon, "STRANDED_DEVICE_READMIT_GRACE_SEC", 3600):
            self.mon._readmit_stranded_devices("c1")
        self.dev.status = NVMeDevice.STATUS_ONLINE
        self.mon._readmit_stranded_devices("c1")
        self.assertNotIn(
            ("c1", "node-a", "dev-1"), self.mon._stranded_first_seen,
            "a recovered device must get a fresh grace window if re-stranded")

    def test_sweep_wired_into_update_cluster_status(self):
        import inspect
        src = inspect.getsource(self.mon.update_cluster_status)
        self.assertIn(
            "_readmit_stranded_devices", src,
            "update_cluster_status must run the reconciler sweep each tick")


if __name__ == "__main__":
    unittest.main()
