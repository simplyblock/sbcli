"""Unit tests for the erasure-coding resource floors (simplyblock_core.floors)
and the places that enforce them.

  - cluster_floors: the five floors for 2+1, 1+1, 2+2 and 4+2, with and
    without raid_devices; the single-node and two-node topology overrides.
  - count_data_devices: journal devices never count (floor 5).
  - floor_violations: every violated floor is named.
  - evaluate: online resources, the node left out of a planned operation.
  - Activation: fresh activation refuses below a floor; the failure-domain
    minimum is max(k+2, n+k).
  - Monitor: DEGRADED (never SUSPENDED) while online resources are below a
    floor, named in the cluster event.
  - Shutdown preconditions: refused below a floor, force overrides.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops, floors, storage_node_ops
from simplyblock_core.controllers.cluster_expansion import planner
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _cluster(ndcs, npcs, raid_devices=False, is_single_node=False, two_node_arbitration=False,
             enable_failure_domain=False, ha_type="ha"):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.distr_ndcs = ndcs
    cl.distr_npcs = npcs
    cl.raid_devices = raid_devices
    cl.is_single_node = is_single_node
    cl.two_node_arbitration = two_node_arbitration
    cl.enable_failure_domain = enable_failure_domain
    cl.ha_type = ha_type
    cl.max_fault_tolerance = max(npcs, 1)
    cl.status = Cluster.STATUS_ACTIVE
    return cl


def _dev(status=NVMeDevice.STATUS_ONLINE, uuid="dev"):
    d = NVMeDevice()
    d.uuid = uuid
    d.status = status
    return d


def _node(uuid, devices=1, status=StorageNode.STATUS_ONLINE, failure_domain=-1,
          device_statuses=None, with_jm=False):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "cluster-1"
    n.status = status
    n.mgmt_ip = f"10.0.0.{abs(hash(uuid)) % 250 + 1}"
    n.failure_domain = failure_domain
    statuses = device_statuses if device_statuses is not None else [NVMeDevice.STATUS_ONLINE] * devices
    n.nvme_devices = [_dev(s, f"{uuid}-dev-{i}") for i, s in enumerate(statuses)]
    if with_jm:
        jm = JMDevice()
        jm.uuid = f"{uuid}-jm"
        jm.status = NVMeDevice.STATUS_ONLINE
        n.jm_device = jm
    return n


# ---------------------------------------------------------------------------
# cluster_floors
# ---------------------------------------------------------------------------

class TestClusterFloors(unittest.TestCase):

    def _floors(self, ndcs, npcs, members=8, **kw):
        cl = _cluster(ndcs, npcs, **kw)
        return floors.cluster_floors(cl, uses_failure_domains=kw.get("enable_failure_domain", False),
                                     member_nodes=members)

    def test_2_1(self):
        f = self._floors(2, 1)
        self.assertEqual((f.nodes, f.devices_per_node, f.data_devices, f.failure_domains), (4, 1, 4, None))
        f = self._floors(2, 1, raid_devices=True)
        self.assertEqual((f.nodes, f.data_devices), (4, 3))
        self.assertIn("raid_devices", f.data_devices_rule)

    def test_1_1(self):
        f = self._floors(1, 1)
        self.assertEqual((f.nodes, f.data_devices), (3, 3))
        self.assertEqual(self._floors(1, 1, raid_devices=True).data_devices, 2)

    def test_2_2(self):
        f = self._floors(2, 2)
        self.assertEqual((f.nodes, f.data_devices), (6, 6))
        self.assertEqual(self._floors(2, 2, raid_devices=True).data_devices, 4)

    def test_4_2(self):
        f = self._floors(4, 2)
        self.assertEqual((f.nodes, f.data_devices), (8, 8))
        self.assertEqual(self._floors(4, 2, raid_devices=True).data_devices, 6)

    def test_failure_domain_floor_is_n_plus_k_only_when_used(self):
        self.assertEqual(self._floors(4, 2, enable_failure_domain=True).failure_domains, 6)
        self.assertEqual(self._floors(2, 1, enable_failure_domain=True).failure_domains, 3)
        self.assertIsNone(self._floors(2, 1).failure_domains)

    def test_single_node_override(self):
        for kw in ({"is_single_node": True, "members": 3}, {"members": 1}):
            f = self._floors(2, 1, **kw)
            self.assertEqual((f.nodes, f.data_devices), (1, 3), kw)
            self.assertEqual(f.nodes_rule, "single-node cluster")
        cl = _cluster(1, 0)
        cl.activated_single_node = True
        f = floors.cluster_floors(cl, False, member_nodes=1)
        self.assertEqual((f.nodes, f.data_devices), (1, 1))

    def test_two_node_override(self):
        for kw in ({"two_node_arbitration": True, "members": 2}, {"members": 2}):
            f = self._floors(1, 1, **kw)
            # Two nodes, but the data-device floor stays the cluster rule.
            self.assertEqual((f.nodes, f.data_devices), (2, 3), kw)
            self.assertEqual(f.nodes_rule, "two-node cluster")
        self.assertEqual(self._floors(1, 1, raid_devices=True, members=2).data_devices, 2)

    def test_volume_scheme_override(self):
        cl = _cluster(4, 2)
        f = floors.cluster_floors(cl, False, member_nodes=8, ndcs=2, npcs=1)
        self.assertEqual((f.nodes, f.data_devices), (4, 4))

    def test_mock_flags_never_read_truthy(self):
        """A MagicMock stand-in for the cluster (spec'd or not) must not turn a
        child mock into raid_devices / two_node / single-node."""
        cl = MagicMock(spec=Cluster)
        cl.distr_ndcs, cl.distr_npcs = 2, 1
        f = floors.cluster_floors(cl, False, member_nodes=5)
        self.assertEqual((f.nodes, f.data_devices), (4, 4))


# ---------------------------------------------------------------------------
# count_data_devices (floor 5: journal devices never count)
# ---------------------------------------------------------------------------

class TestCountDataDevices(unittest.TestCase):

    def test_journal_is_not_a_data_device(self):
        # A dedicated journal device lives in jm_device, outside nvme_devices;
        # a device still flagged STATUS_JM inside the list is skipped as well.
        n1 = _node("n1", device_statuses=[NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_JM], with_jm=True)
        n2 = _node("n2", devices=2, with_jm=True)
        total, per_node = floors.count_data_devices([n1, n2])
        self.assertEqual(total, 3)
        self.assertEqual(per_node, {"n1": 1, "n2": 2})

    def test_only_serving_statuses_count(self):
        n = _node("n1", device_statuses=[
            NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE,
            NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_FAILED,
            NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED_AND_MIGRATED])
        total, per_node = floors.count_data_devices([n])
        self.assertEqual((total, per_node["n1"]), (3, 3))

    def test_a_node_without_devices_is_listed_with_zero(self):
        total, per_node = floors.count_data_devices([_node("n1", devices=0), _node("n2")])
        self.assertEqual((total, per_node), (1, {"n1": 0, "n2": 1}))

    def test_secondary_only_nodes_are_skipped(self):
        sec = _node("sec", devices=0)
        sec.is_secondary_node = True
        total, per_node = floors.count_data_devices([sec, _node("n1")])
        self.assertEqual((total, per_node), (1, {"n1": 1}))


# ---------------------------------------------------------------------------
# floor_violations / evaluate
# ---------------------------------------------------------------------------

class TestViolations(unittest.TestCase):

    def test_every_violated_floor_is_named(self):
        f = floors.Floors(nodes=4, devices_per_node=1, data_devices=4, failure_domains=3)
        v = floors.floor_violations(f, online_nodes=3, data_devices=2, per_node={"a": 2, "b": 0}, online_fds=2)
        self.assertEqual(v, [
            "nodes online 3 < floor 4 (n+2k)",
            "node b has 0 online data devices < floor 1 (data devices per node)",
            "data devices online 2 < floor 4 (n+2k)",
            "failure domains online 2 < floor 3 (n+k)",
        ])

    def test_no_violation_is_empty(self):
        f = floors.Floors(nodes=4, devices_per_node=1, data_devices=4, failure_domains=None)
        self.assertEqual(floors.floor_violations(f, 4, 4, {"a": 1, "b": 1, "c": 1, "d": 1}, None), [])

    def test_evaluate_judges_online_resources(self):
        cl = _cluster(2, 1)
        nodes = [_node(f"n{i}") for i in range(4)] + [_node("n4", status=StorageNode.STATUS_OFFLINE)]
        self.assertEqual(floors.evaluate(cl, nodes), [])
        nodes[3].status = StorageNode.STATUS_OFFLINE
        self.assertEqual(floors.evaluate(cl, nodes), [
            "nodes online 3 < floor 4 (n+2k)",
            "data devices online 3 < floor 4 (n+2k)",
        ])

    def test_evaluate_without_a_node_keeps_the_membership(self):
        """Shutting one node of three down does not turn a 1+1 cluster into a
        two-node cluster: the excluded node still counts as a member."""
        cl = _cluster(1, 1)
        nodes = [_node("n0", devices=2), _node("n1", devices=2), _node("n2", devices=2)]
        self.assertEqual(floors.evaluate(cl, nodes), [])
        self.assertEqual(floors.evaluate(cl, nodes, exclude_node_id="n2"),
                         ["nodes online 2 < floor 3 (n+2k)"])

    def test_evaluate_failure_domains(self):
        cl = _cluster(2, 1, enable_failure_domain=True)
        nodes = [_node(f"n{i}", failure_domain=i % 3) for i in range(6)]
        self.assertEqual(floors.evaluate(cl, nodes), [])
        for n in nodes:
            n.failure_domain = n.failure_domain % 2
        self.assertEqual(floors.evaluate(cl, nodes), ["failure domains online 2 < floor 3 (n+k)"])

    def test_evaluate_volume_override(self):
        cl = _cluster(1, 1)
        nodes = [_node(f"n{i}") for i in range(3)]
        self.assertEqual(floors.evaluate(cl, nodes), [])
        self.assertEqual(floors.evaluate(cl, nodes, ndcs=2, npcs=1),
                         ["nodes online 3 < floor 4 (n+2k)", "data devices online 3 < floor 4 (n+2k)"])

    def test_migration_affinity_floor(self):
        self.assertEqual(floors.migration_affinity_floor(_cluster(2, 1)), "device")
        self.assertEqual(floors.migration_affinity_floor(_cluster(2, 1, raid_devices=True)), "relaxed")


# ---------------------------------------------------------------------------
# Activation
# ---------------------------------------------------------------------------

class TestActivation(unittest.TestCase):

    def test_fresh_activation_floor_check(self):
        cl = _cluster(2, 1)
        nodes = [_node(f"n{i}") for i in range(4)]
        self.assertEqual(cluster_ops.activation_floor_violations(cl, nodes, nodes), [])
        nodes.append(_node("n4", devices=0))
        self.assertEqual(cluster_ops.activation_floor_violations(cl, nodes, nodes),
                         ["node n4 has 0 online data devices < floor 1 (data devices per node)"])

    def test_single_node_activation_floor(self):
        cl = _cluster(1, 0, is_single_node=True)
        nodes = [_node("n0")]
        self.assertEqual(cluster_ops.activation_floor_violations(cl, nodes, nodes), [])
        cl = _cluster(2, 1, is_single_node=True)
        nodes = [_node("n0", devices=3)]
        self.assertEqual(cluster_ops.activation_floor_violations(cl, nodes, nodes), [])
        nodes = [_node("n0", devices=2)]
        self.assertEqual(cluster_ops.activation_floor_violations(cl, nodes, nodes),
                         ["data devices online 2 < floor 3 (n+k, single-node cluster)"])

    def test_reactivation_gate_is_the_old_device_count(self):
        self.assertEqual(cluster_ops.activation_minimum_devices(_cluster(2, 1), False), 4)
        self.assertEqual(cluster_ops.activation_minimum_devices(_cluster(2, 1), True), 3)

    def test_fd_activation_minimum_is_max_of_spare_rule_and_floor(self):
        self.assertEqual(planner.fd_activation_min_domains(1, 2), 3)   # 2+1: k+2 == n+k
        self.assertEqual(planner.fd_activation_min_domains(2, 4), 6)   # 4+2: n+k binds
        self.assertEqual(planner.fd_activation_min_domains(1, 1), 3)   # 1+1: k+2 binds
        self.assertIsNone(planner.fd_activation_domain_count_violation(2, 6, ndcs=4))
        msg = planner.fd_activation_domain_count_violation(2, 5, ndcs=4)
        self.assertIn("at least 6 distinct failure domains (n+k", msg)
        # Without ndcs the former k+2 rule is unchanged.
        self.assertIsNone(planner.fd_activation_domain_count_violation(1, 3))
        self.assertIsNotNone(planner.fd_activation_domain_count_violation(1, 2))


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class TestMonitorFloors(unittest.TestCase):

    def _status(self, cluster, nodes):
        from simplyblock_core.services import storage_node_monitor as mod
        mod._floor_violations_reported.clear()
        events = MagicMock()
        with patch.object(mod, "db") as mock_db, patch.object(mod, "cluster_events", events), \
                patch.object(mod, "is_new_migrated_node", return_value=False):
            mock_db.get_cluster_by_id.return_value = cluster
            mock_db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
            mock_db.get_job_tasks.return_value = []
            return mod.get_next_cluster_status("cluster-1"), events

    def test_healthy_cluster_at_the_floor_is_active(self):
        status, events = self._status(_cluster(2, 1), [_node(f"n{i}") for i in range(4)])
        self.assertEqual(status, Cluster.STATUS_ACTIVE)
        events.cluster_floor_violation.assert_not_called()

    def test_below_the_node_floor_is_degraded_and_named(self):
        status, events = self._status(_cluster(2, 1), [_node(f"n{i}", devices=2) for i in range(3)])
        self.assertEqual(status, Cluster.STATUS_DEGRADED)
        _, violations = events.cluster_floor_violation.call_args.args
        self.assertEqual(violations, ["nodes online 3 < floor 4 (n+2k)"])

    def test_below_the_device_floor_is_degraded_not_suspended(self):
        # Every node online, the data devices below n+2k: the old rules say
        # ACTIVE (n+k devices are there), the floors say DEGRADED.
        nodes = [_node(f"n{i}") for i in range(4)]
        nodes[3].nvme_devices = []
        status, _ = self._status(_cluster(2, 1), nodes)
        self.assertEqual(status, Cluster.STATUS_DEGRADED)

    def test_raid_devices_lowers_the_device_floor_for_the_monitor(self):
        nodes = [_node(f"n{i}") for i in range(4)]
        nodes[3].nvme_devices = [_dev(NVMeDevice.STATUS_ONLINE)]
        nodes[2].nvme_devices = []
        cl = _cluster(2, 1, raid_devices=True)
        status, events = self._status(cl, nodes)
        # The per-node floor still degrades; the cluster-wide one no longer does.
        self.assertEqual(status, Cluster.STATUS_DEGRADED)
        _, violations = events.cluster_floor_violation.call_args.args
        self.assertEqual(violations, ["node n2 has 0 online data devices < floor 1 (data devices per node)"])

    def test_suspension_rules_are_untouched(self):
        # k=1 with two affected nodes: SUSPENDED by the existing rules, and
        # the floors never turn that into DEGRADED.
        nodes = [_node(f"n{i}") for i in range(5)]
        nodes[0].status = StorageNode.STATUS_OFFLINE
        nodes[1].status = StorageNode.STATUS_OFFLINE
        status, _ = self._status(_cluster(2, 1), nodes)
        self.assertEqual(status, Cluster.STATUS_SUSPENDED)

    def test_event_once_per_change_and_restored(self):
        from simplyblock_core.services import storage_node_monitor as mod
        cl = _cluster(2, 1)
        short = [_node(f"n{i}", devices=2) for i in range(3)]
        whole = short + [_node("n3", devices=2)]
        mod._floor_violations_reported.clear()
        events = MagicMock()
        with patch.object(mod, "db") as mock_db, patch.object(mod, "cluster_events", events), \
                patch.object(mod, "is_new_migrated_node", return_value=False):
            mock_db.get_cluster_by_id.return_value = cl
            mock_db.get_job_tasks.return_value = []
            mock_db.get_primary_storage_nodes_by_cluster_id.return_value = short
            mod.get_next_cluster_status("cluster-1")
            mod.get_next_cluster_status("cluster-1")
            self.assertEqual(events.cluster_floor_violation.call_count, 1)
            mock_db.get_primary_storage_nodes_by_cluster_id.return_value = whole
            self.assertEqual(mod.get_next_cluster_status("cluster-1"), Cluster.STATUS_ACTIVE)
            events.cluster_floor_restored.assert_called_once()


# ---------------------------------------------------------------------------
# Shutdown preconditions
# ---------------------------------------------------------------------------

class TestShutdownFloors(unittest.TestCase):

    def _db(self, cluster, nodes):
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        db.get_storage_node_by_id.side_effect = lambda nid: next(n for n in nodes if n.get_id() == nid)
        return db

    def test_shutdown_below_a_floor_is_refused(self):
        nodes = [_node(f"n{i}", devices=2) for i in range(4)]
        with patch.object(storage_node_ops, "DBController", return_value=self._db(_cluster(2, 1), nodes)):
            allowed, reason = storage_node_ops.check_node_shutdown_preconditions("n0")
        self.assertFalse(allowed)
        self.assertIn("nodes online 3 < floor 4 (n+2k)", reason)

    def test_force_overrides_the_floor(self):
        nodes = [_node(f"n{i}", devices=2) for i in range(4)]
        tc = MagicMock()
        tc.get_active_node_tasks.return_value = []
        tc.get_active_node_restart_task.return_value = []
        with patch.object(storage_node_ops, "DBController", return_value=self._db(_cluster(2, 1), nodes)), \
                patch.object(storage_node_ops, "tasks_controller", tc):
            allowed, reason = storage_node_ops.check_node_shutdown_preconditions("n0", force=True)
        self.assertTrue(allowed, reason)

    def test_shutdown_with_headroom_passes_the_floor(self):
        nodes = [_node(f"n{i}", devices=2) for i in range(5)]
        tc = MagicMock()
        tc.get_active_node_tasks.return_value = []
        tc.get_active_node_restart_task.return_value = []
        with patch.object(storage_node_ops, "DBController", return_value=self._db(_cluster(2, 1), nodes)), \
                patch.object(storage_node_ops, "tasks_controller", tc):
            allowed, reason = storage_node_ops.check_node_shutdown_preconditions("n0")
        self.assertTrue(allowed, reason)


if __name__ == "__main__":
    unittest.main()
