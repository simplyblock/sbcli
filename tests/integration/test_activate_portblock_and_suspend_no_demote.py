# coding=utf-8
"""
Regression tests for two related fixes:

1. cluster_activate (Pass 2): on re-activation (cluster.status != UNREADY),
   the configured primary's LVS port is firewall-blocked before
   recreate_lvstore_on_non_leader runs and unblocked afterwards. On a fresh
   activation (cluster.status == UNREADY) NO port-block is issued — peers
   aren't serving yet, so there is nothing to quiesce.
   Bug: a JCERR-driven cluster suspend followed by re-activation looped
   forever with bs_load_cur_extent_page_valid CRC mismatch on the
   secondary's examine because the live primary kept writing into the LVS
   blob metadata. Observed 2026-05-11, LVS_6769 on node 8084.

2. suspend_storage_node: after blocking the lvs+hublvol ports, the
   explicit bdev_lvol_set_leader(leader=False) and
   bdev_distrib_force_to_non_leader RPCs are NOT issued. With both ports
   blocked the surviving peer auto-promotes; demoting now races pre-block
   in-flight IO still completing on the local distrib → writer conflict.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(uuid, status=StorageNode.STATUS_ONLINE, lvstore="LVS_A",
          jm_vuid=1, primary_secondary=None, primary_tertiary=None,
          mgmt_ip="10.0.0.1", rpc_port=8080, n_devices=4):
    from simplyblock_core.models.nvme_device import NVMeDevice
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "cluster-1"
    n.status = status
    n.hostname = f"host-{uuid}"
    n.mgmt_ip = mgmt_ip
    n.rpc_port = rpc_port
    n.rpc_username = "u"
    n.rpc_password = "p"
    n.lvstore = lvstore
    n.lvstore_status = "ready"
    n.jm_vuid = jm_vuid
    n.is_secondary_node = False
    n.lvstore_stack_secondary = ""
    n.lvstore_stack_tertiary = ""
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420, "hublvol_port": 4427}}
    devs = []
    for i in range(n_devices):
        d = NVMeDevice()
        d.uuid = f"dev-{uuid}-{i}"
        d.status = NVMeDevice.STATUS_ONLINE
        devs.append(d)
    n.nvme_devices = devs
    n.remote_devices = []
    n.remote_jm_devices = []
    n.physical_label = 0
    n.secondary_node_id = primary_secondary or ""
    n.tertiary_node_id = primary_tertiary or ""
    n.data_nics = [IFace()]
    n.data_nics[0].ip4_address = mgmt_ip
    n.data_nics[0].trtype = "TCP"
    n.active_tcp = True
    n.active_rdma = False
    return n


def _cluster(status=Cluster.STATUS_SUSPENDED, ha_type="ha", ftt=1):
    c = Cluster()
    c.uuid = "cluster-1"
    c.status = status
    c.ha_type = ha_type
    c.max_fault_tolerance = ftt
    c.distr_ndcs = 4
    c.distr_npcs = 2
    c.distr_bs = 4096
    c.distr_chunk_bs = 4096
    c.page_size_in_blocks = 128
    c.nqn = "nqn.cluster"
    c.is_single_node = False
    c.enable_node_affinity = False
    c.backup_config = None
    return c


# ===========================================================================
# 1. cluster_activate Pass 2 port-block wrapper
# ===========================================================================


class TestActivatePortBlockWrapper(unittest.TestCase):
    """The Pass 2 firewall block runs only on re-activation, not on first
    activation, and wraps recreate_lvstore_on_non_leader exactly once per
    (snode, primary_node) pair."""

    def _patch_cluster_activate_environment(
        self, cluster, primary, secondary,
        recreate_lvstore_ret=True,
        recreate_non_leader_ret=True,
        recreate_non_leader_exc=None,
        firewall_block_exc=None,
        firewall_allow_exc=None,
    ):
        """Returns a tuple of (patches_started, recorded_calls).

        Caller is responsible for stopping the patches (via the returned
        contextmanager-like list) — done in the test's tearDown via addCleanup.
        """
        from simplyblock_core import cluster_ops

        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = [primary, secondary]
        db.get_storage_node_by_id.side_effect = lambda nid: (
            primary if nid == primary.get_id() else secondary)
        db.get_cluster_capacity.return_value = [{"size_total": 1 << 40}]
        db.get_qos.return_value = []

        def _primary_for(node_id):
            return [primary] if node_id == secondary.get_id() else []
        db.get_primary_storage_nodes_by_secondary_node_id.side_effect = _primary_for

        # port_block.set_port: record block/allow calls. (Port blocking moved
        # off the directly-imported FirewallClient onto port_block.set_port.)
        fw_calls = []

        def _fake_set_port(node, port, block, is_reject=False, timeout=5, retry=2):
            action = "block" if block else "allow"
            fw_calls.append((node.get_id(), port, action))
            if action == "block" and firewall_block_exc:
                raise firewall_block_exc
            if action == "allow" and firewall_allow_exc:
                raise firewall_allow_exc

        # storage_node_ops.recreate_lvstore* are heavy — replace with stubs.
        recreate_calls = []

        def _recreate_primary(snode, activation_mode=False, **kw):
            recreate_calls.append(("primary", snode.get_id(), activation_mode))
            return recreate_lvstore_ret

        def _recreate_non_leader(snode, leader, primary_node,
                                  activation_mode=False, **kw):
            recreate_calls.append(("non_leader", snode.get_id(),
                                   primary_node.get_id(), activation_mode))
            if recreate_non_leader_exc:
                raise recreate_non_leader_exc
            return recreate_non_leader_ret

        # tasks_controller: drop schedule fallback (the wrapper falls back to
        # add_port_allow_task only when unblock RPC fails — we don't trigger
        # that in the green-path tests below; still need a no-op shim).
        scheduled_port_allow = []

        def _add_port_allow_task(cluster_id, node_id, port):
            scheduled_port_allow.append((cluster_id, node_id, port))

        port_events = []

        def _port_deny(node, port):
            port_events.append(("deny", node.get_id(), port))

        def _port_allowed(node, port):
            port_events.append(("allow", node.get_id(), port))

        patches = [
            patch.object(cluster_ops, "db_controller", db),
            patch.object(cluster_ops, "DBController", return_value=db),
            patch("simplyblock_core.port_block.set_port", _fake_set_port),
            patch.object(cluster_ops.tcp_ports_events, "port_deny", _port_deny),
            patch.object(cluster_ops.tcp_ports_events, "port_allowed", _port_allowed),
            patch.object(cluster_ops.tasks_controller, "add_port_allow_task",
                         _add_port_allow_task),
            patch.object(cluster_ops.storage_node_ops, "recreate_lvstore",
                         _recreate_primary),
            patch.object(cluster_ops.storage_node_ops, "recreate_lvstore_on_non_leader",
                         _recreate_non_leader),
            patch.object(cluster_ops.storage_node_ops, "get_next_physical_device_order",
                         lambda *a, **kw: 0),
            patch.object(cluster_ops.storage_node_ops, "get_secondary_nodes",
                         lambda *a, **kw: [secondary.get_id()]),
            patch.object(cluster_ops.storage_node_ops, "get_secondary_nodes_2",
                         lambda *a, **kw: []),
            patch.object(cluster_ops, "set_cluster_status", lambda *a, **kw: None),
            patch.object(cluster_ops, "time", MagicMock()),
            # qos: prevent FDB writes
            patch.object(cluster_ops.qos_controller, "get_qos_weights_list",
                         lambda *a, **kw: []),
        ]
        # Ensure each node's rpc_client and snode.recreate_hublvol / connect /
        # write_to_db are no-ops (Pass 3 + post-loop work). The model objects
        # already exist; we patch their methods inline.
        for n in (primary, secondary):
            n.write_to_db = MagicMock(return_value=True)
            n.rpc_client = MagicMock()
            n.recreate_hublvol = MagicMock(return_value=True)
            n.create_secondary_hublvol = MagicMock(return_value=True)
            n.connect_to_hublvol = MagicMock(return_value=True)
            n.client = MagicMock()
        # Primary's per-LVS port lookup — already populated in _node().
        # Make is_qos_set on the cluster return False so the QOS branch is skipped.
        cluster.is_qos_set = lambda: False

        for p in patches:
            p.start()
        return patches, fw_calls, recreate_calls, port_events, scheduled_port_allow

    def _run_activate(self, cluster, primary, secondary, **kw):
        from simplyblock_core import cluster_ops
        patches, fw_calls, recreate_calls, port_events, scheduled = \
            self._patch_cluster_activate_environment(cluster, primary,
                                                     secondary, **kw)
        self.addCleanup(lambda: [p.stop() for p in patches])
        try:
            cluster_ops.cluster_activate("cluster-1", force=True)
        except ValueError:
            # cluster_activate may raise on the LVSRestartRequiredError path;
            # the tests below assert on it explicitly.
            pass
        return fw_calls, recreate_calls, port_events, scheduled

    # ----- tests -----

    def test_reactivation_blocks_and_unblocks_leader_port(self):
        cluster = _cluster(status=Cluster.STATUS_SUSPENDED)
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", mgmt_ip="10.0.0.2", rpc_port=8081)

        fw_calls, recreate_calls, port_events, _ = self._run_activate(
            cluster, primary, secondary)

        # The wrapper must have issued block on primary:4420, then allow on
        # primary:4420 — once each, in that order, and surrounding the
        # non_leader recreate call.
        fw_for_primary = [c for c in fw_calls if c[0] == "primary-1"]
        self.assertEqual(
            fw_for_primary,
            [("primary-1", 4420, "block"), ("primary-1", 4420, "allow")],
            f"unexpected firewall sequence: {fw_calls}")
        # Recreate on the non-leader ran with activation_mode=True (we deliberately
        # do NOT switch the helper out of activation_mode — only add the firewall).
        non_leader_runs = [c for c in recreate_calls if c[0] == "non_leader"]
        self.assertEqual(len(non_leader_runs), 1, recreate_calls)
        _, snode_id, primary_id, activation_mode = non_leader_runs[0]
        self.assertEqual(snode_id, "secondary-1")
        self.assertEqual(primary_id, "primary-1")
        self.assertTrue(activation_mode,
                        "Pass 2 must still call helper with activation_mode=True "
                        "— the firewall wrapper provides the only added op")
        # tcp_ports_events emitted deny + allowed events on the primary.
        self.assertIn(("deny", "primary-1", 4420), port_events)
        self.assertIn(("allow", "primary-1", 4420), port_events)

    def test_fresh_activation_does_not_block_leader_port(self):
        cluster = _cluster(status=Cluster.STATUS_UNREADY)
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", mgmt_ip="10.0.0.2", rpc_port=8081)

        fw_calls, recreate_calls, port_events, _ = self._run_activate(
            cluster, primary, secondary)

        # On fresh activation NO port-block is issued — peers aren't serving
        # yet and the existing activation_mode=True short-circuit handles
        # everything.
        self.assertEqual(
            [c for c in fw_calls if c[0] == "primary-1"], [],
            f"fresh activation must not block primary's port; got {fw_calls}")
        self.assertEqual(
            [e for e in port_events if e[1] == "primary-1"], [],
            f"fresh activation must not emit port deny/allow events; got {port_events}")

    def test_reactivation_unblocks_when_recreate_raises(self):
        """LVSRestartRequiredError out of recreate_lvstore_on_non_leader must
        not leak a stuck-blocked leader port — the finally clause unblocks."""
        from simplyblock_core import storage_node_ops
        cluster = _cluster(status=Cluster.STATUS_SUSPENDED)
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", mgmt_ip="10.0.0.2", rpc_port=8081)

        err = storage_node_ops.LVSRestartRequiredError(
            "secondary-1", "LVS_A", detail="examine did not produce lvstore")
        fw_calls, _, _, scheduled = self._run_activate(
            cluster, primary, secondary, recreate_non_leader_exc=err)

        fw_for_primary = [c for c in fw_calls if c[0] == "primary-1"]
        # block then allow — even on the exception path.
        self.assertEqual(
            fw_for_primary,
            [("primary-1", 4420, "block"), ("primary-1", 4420, "allow")],
            f"finally-unblock missing on exception path: {fw_calls}")
        # No port_allow_task scheduled — the unblock RPC itself succeeded.
        self.assertEqual(scheduled, [])

    def test_reactivation_schedules_port_allow_task_on_unblock_failure(self):
        cluster = _cluster(status=Cluster.STATUS_SUSPENDED)
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", mgmt_ip="10.0.0.2", rpc_port=8081)

        fw_calls, _, _, scheduled = self._run_activate(
            cluster, primary, secondary,
            firewall_allow_exc=RuntimeError("network down"))

        # block recorded, allow attempted (and raised), so it is in fw_calls.
        self.assertEqual(
            [c for c in fw_calls if c[0] == "primary-1"],
            [("primary-1", 4420, "block"), ("primary-1", 4420, "allow")])
        # Fallback task scheduled.
        self.assertEqual(scheduled, [("cluster-1", "primary-1", 4420)],
                         f"add_port_allow_task fallback missing: {scheduled}")


# ===========================================================================
# 2. suspend_storage_node — no leadership drop after port block
# ===========================================================================


class TestSuspendIsDeprecatedNoop(unittest.TestCase):
    """The suspension phase (port-block on sec/tert + own-primary LVS
    ports) was removed entirely after the 2026-05-19 jm_vuid=4818
    incident: an iptables-only fence cannot stop SPDK's lvol layer
    from resubmitting failed-redirect IO as if it were new host IO,
    which races the surviving peer's auto-promotion. See the new
    `shutdown_storage_node` docstring for the replacement flow
    (Loop 1 device-unavailable + Loop 2 detach-remote-controllers).

    Here we just verify that `suspend_storage_node` is a noop
    (returns True without invoking FirewallClient). The detailed
    coverage for the new shutdown flow lives in
    tests/test_shutdown_no_suspension.py.
    """

    def test_suspend_is_noop_and_does_not_touch_firewall(self):
        from simplyblock_core import storage_node_ops

        snode = _node("node-A", lvstore="LVS_A")

        with patch.object(storage_node_ops, "port_block") as pb, \
                patch.object(storage_node_ops, "DBController") as _db:
            self.assertTrue(
                storage_node_ops.suspend_storage_node(snode.get_id()))
            pb.set_port.assert_not_called()

    def test_resume_is_noop_and_does_not_touch_firewall(self):
        from simplyblock_core import storage_node_ops

        snode = _node("node-A", lvstore="LVS_A")

        with patch.object(storage_node_ops, "port_block") as pb, \
                patch.object(storage_node_ops, "DBController") as _db:
            self.assertTrue(
                storage_node_ops.resume_storage_node(snode.get_id()))
            pb.set_port.assert_not_called()


if __name__ == "__main__":
    unittest.main()
