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

import contextlib
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode
from tests._mocks import patch_fences

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_device_mesh(nodes):
    """Give every node a connected remote-device record for every other node's
    devices.

    ``cluster_activate`` gates on ``_wait_for_full_device_connectivity``, which
    requires the full cross-node mesh before it will build distribs on top of
    it. Without this the activation under test aborts at that gate and never
    reaches pass 2, so the port-block assertions see no calls at all.
    """
    for node in nodes:
        node.remote_devices = [
            _remote_of(peer, dev)
            for peer in nodes if peer.get_id() != node.get_id()
            for dev in peer.nvme_devices
        ]


def _remote_of(owner, dev):
    """A connected remote-device record on a peer for ``owner``'s ``dev``. The
    uuid must match the owning device's — the mesh is keyed on device id."""
    from simplyblock_core.models.nvme_device import RemoteDevice

    rd = RemoteDevice()
    rd.uuid = dev.get_id()
    rd.remote_bdev = f"remote_{dev.get_id()}"
    rd.node_id = owner.get_id()
    return rd


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


def _two_node_ring():
    """A two-node HA cluster: each node owns an LVS and is the other's
    secondary. ``primary-1``/``LVS_A`` is the pair the tests assert on;
    ``secondary-1`` also owns ``LVS_B`` because an HA node without a secondary
    of its own is not a state activation accepts — it assigns one."""
    primary = _node("primary-1", primary_secondary="secondary-1")
    secondary = _node("secondary-1", lvstore="LVS_B", primary_secondary="primary-1",
                      mgmt_ip="10.0.0.2", rpc_port=8081)
    return primary, secondary


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
        """Seed ``cluster``, ``primary`` and ``secondary`` into FDB and patch
        everything above the database. Returns the started patchers and the
        recorded calls; the caller stops the patchers.
        """
        from simplyblock_core import cluster_ops

        _seed_device_mesh([primary, secondary])
        db = DBController()
        for record in (cluster, primary, secondary):
            record.write_to_db(db.kv_store)

        # Fences (nvmf_port_block / nvmf_port_unblock on node.rpc_client):
        # record block/allow calls per node.
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

        # The wrapper falls back to add_port_allow_task only when the unblock
        # RPC fails; record it rather than queueing a real task.
        scheduled_port_allow = []

        def _add_port_allow_task(cluster_id, node_id, port):
            scheduled_port_allow.append((cluster_id, node_id, port))

        port_events = []

        def _port_deny(node, port):
            port_events.append(("deny", node.get_id(), port))

        def _port_allowed(node, port):
            port_events.append(("allow", node.get_id(), port))

        patches = [
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
            # Only sleep. Replacing the whole module makes time.time() return a
            # MagicMock, so the activation path's `time.time() >= deadline`
            # comparisons raise TypeError instead of running.
            patch.object(cluster_ops.time, "sleep", MagicMock()),
            # Storage nodes: the activation reads its nodes back from FDB, so
            # the node-side calls are patched on the class, not the instances.
            patch_fences(_fake_set_port),
            patch.object(StorageNode, "recreate_hublvol", return_value=True),
            patch.object(StorageNode, "create_secondary_hublvol", return_value=True),
            patch.object(StorageNode, "connect_to_hublvol", return_value=True),
            patch.object(StorageNode, "client", MagicMock()),
        ]
        for p in patches:
            p.start()
        return patches, fw_calls, recreate_calls, port_events, scheduled_port_allow

    def _run_activate(self, cluster, primary, secondary, **kw):
        from simplyblock_core import cluster_ops
        patches, fw_calls, recreate_calls, port_events, scheduled = \
            self._patch_cluster_activate_environment(cluster, primary,
                                                     secondary, **kw)
        self.addCleanup(lambda: [p.stop() for p in patches])
        # cluster_activate may raise on the LVSRestartRequiredError path;
        # the tests below assert on its effects explicitly.
        with contextlib.suppress(ValueError):
            cluster_ops.cluster_activate("cluster-1", force=True)
        return fw_calls, recreate_calls, port_events, scheduled

    # ----- tests -----

    def test_reactivation_blocks_and_unblocks_leader_port(self):
        cluster = _cluster(status=Cluster.STATUS_SUSPENDED)
        primary, secondary = _two_node_ring()

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
        non_leader_runs = [c for c in recreate_calls
                           if c[0] == "non_leader" and c[2] == "primary-1"]
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
        primary, secondary = _two_node_ring()

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
        primary, secondary = _two_node_ring()

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
        primary, secondary = _two_node_ring()

        fw_calls, _, _, scheduled = self._run_activate(
            cluster, primary, secondary,
            firewall_allow_exc=RuntimeError("network down"))

        # block recorded, allow attempted (and raised), so it is in fw_calls.
        self.assertEqual(
            [c for c in fw_calls if c[0] == "primary-1"],
            [("primary-1", 4420, "block"), ("primary-1", 4420, "allow")])
        # Fallback task scheduled.
        self.assertEqual([t for t in scheduled if t[1] == "primary-1"],
                         [("cluster-1", "primary-1", 4420)],
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
    (returns True without fencing any port). The detailed
    coverage for the new shutdown flow lives in
    tests/test_shutdown_no_suspension.py.
    """

    def test_suspend_is_noop_and_does_not_touch_firewall(self):
        from simplyblock_core import storage_node_ops

        snode = _node("node-A", lvstore="LVS_A", status=StorageNode.STATUS_ONLINE)
        snode.write_to_db(DBController().kv_store)

        fence = MagicMock()
        with patch.object(storage_node_ops, "set_node_status"), patch_fences(fence):
            self.assertTrue(
                storage_node_ops.suspend_storage_node(snode.get_id()))
            fence.assert_not_called()

    def test_resume_is_noop_and_does_not_touch_firewall(self):
        from simplyblock_core import storage_node_ops

        snode = _node("node-A", lvstore="LVS_A", status=StorageNode.STATUS_SUSPENDED)
        snode.write_to_db(DBController().kv_store)

        fence = MagicMock()
        with patch.object(storage_node_ops, "set_node_status"), patch_fences(fence):
            self.assertTrue(
                storage_node_ops.resume_storage_node(snode.get_id()))
            fence.assert_not_called()


if __name__ == "__main__":
    unittest.main()
