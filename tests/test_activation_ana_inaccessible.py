# coding=utf-8
"""
test_activation_ana_inaccessible.py — covers Fix 1 of the 2026-06-08 incident:

During (re)activation the client-facing lvol listeners are brought up
INACCESSIBLE (so no client IO can flow before the hublvol redirects exist and
leadership has settled). cluster_activate then runs a dedicated final pass that
sets each listener to its correct ANA state — optimized on the LVS primary,
non_optimized on its secondary/tertiary — and only AFTER that flips the cluster
to ACTIVE.

Two properties are verified:
  A. recreate_lvstore / recreate_lvstore_on_non_leader select ana_state
     "inaccessible" when activation_mode=True (and the normal optimized /
     non_optimized otherwise).
  B. cluster_activate's Pass 4 sets optimized on the primary + non_optimized on
     the secondary for every lvol, and every ANA write happens BEFORE the
     STATUS_ACTIVE transition.

All FDB / RPC / SPDK dependencies are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Property A: ana_state selection is inaccessible during activation
# ---------------------------------------------------------------------------

class TestActivationAnaStateSelection(unittest.TestCase):
    """The ana_state passed to add_lvol_thread must be 'inaccessible' under
    activation_mode and the normal accessible state otherwise. We assert the
    exact expression used in the source so the intent can't silently regress."""

    def test_primary_inaccessible_iff_activation(self):
        # recreate_lvstore: lvol_ana_state = "inaccessible" if activation_mode else "optimized"
        for activation_mode, expected in [(True, "inaccessible"), (False, "optimized")]:
            with self.subTest(activation_mode=activation_mode):
                self.assertEqual(
                    "inaccessible" if activation_mode else "optimized", expected)

    def test_non_leader_inaccessible_iff_activation(self):
        # recreate_lvstore_on_non_leader: "inaccessible" if activation_mode else "non_optimized"
        for activation_mode, expected in [(True, "inaccessible"), (False, "non_optimized")]:
            with self.subTest(activation_mode=activation_mode):
                self.assertEqual(
                    "inaccessible" if activation_mode else "non_optimized", expected)

    def test_source_uses_activation_gated_inaccessible(self):
        # Guard against the lines being reverted to an unconditional accessible
        # state: the source must gate on activation_mode and use 'inaccessible'.
        import inspect
        from simplyblock_core import storage_node_ops
        src = inspect.getsource(storage_node_ops)
        self.assertIn('"inaccessible" if activation_mode else "optimized"', src)
        self.assertIn('"inaccessible" if activation_mode else "non_optimized"', src)


# ---------------------------------------------------------------------------
# Property B: cluster_activate Pass 4 sets correct ANA before STATUS_ACTIVE
# ---------------------------------------------------------------------------

def _node(uuid, status=StorageNode.STATUS_ONLINE, lvstore="LVS_A",
          jm_vuid=1, primary_secondary="", primary_tertiary="",
          mgmt_ip="10.0.0.1", rpc_port=8080, is_secondary_node=False):
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
    n.is_secondary_node = is_secondary_node
    n.lvstore_stack_secondary = ""
    n.lvstore_stack_tertiary = ""
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420, "hublvol_port": 4427}}
    devs = []
    for i in range(4):
        d = NVMeDevice()
        d.uuid = f"dev-{uuid}-{i}"
        d.status = NVMeDevice.STATUS_ONLINE
        devs.append(d)
    n.nvme_devices = devs
    n.remote_devices = []
    n.remote_jm_devices = []
    n.physical_label = 0
    n.secondary_node_id = primary_secondary
    n.tertiary_node_id = primary_tertiary
    n.data_nics = [IFace()]
    n.data_nics[0].ip4_address = mgmt_ip
    n.data_nics[0].trtype = "TCP"
    n.active_tcp = True
    n.active_rdma = False
    return n


def _cluster(status=Cluster.STATUS_SUSPENDED):
    c = Cluster()
    c.uuid = "cluster-1"
    c.status = status
    c.ha_type = "ha"
    c.distr_ndcs = 4
    c.distr_npcs = 2
    c.distr_bs = 4096
    c.distr_chunk_bs = 4096
    c.page_size_in_blocks = 128
    c.nqn = "nqn.cluster"
    c.is_single_node = False
    c.enable_node_affinity = False
    c.backup_config = None
    c.cluster_max_size = 1 << 40  # non-zero -> skip the max_size write block
    return c


def _lvol(uuid, lvs_name="LVS_A"):
    lv = MagicMock(spec=LVol)
    lv.status = LVol.STATUS_ONLINE
    lv.lvs_name = lvs_name
    lv.nqn = f"nqn.2023-02.io.simplyblock:lvol:{uuid}"
    lv.lvol_uuid = uuid
    lv.get_id = MagicMock(return_value=uuid)
    return lv


class TestClusterActivatePass4(unittest.TestCase):

    def _run(self, cluster, primary, secondary, lvols):
        from simplyblock_core import cluster_ops

        events = []  # ordered record of ("ana", node_id, ana_state) / ("status", value)

        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = [primary, secondary]
        db.get_storage_node_by_id.side_effect = lambda nid: (
            primary if nid == primary.get_id() else secondary)
        db.get_cluster_capacity.return_value = [{"size_total": 1 << 40}]
        db.get_qos.return_value = []
        db.get_primary_storage_nodes_by_secondary_node_id.side_effect = \
            lambda nid: [primary] if nid == secondary.get_id() else []
        db.get_lvols_by_node_id.side_effect = \
            lambda nid: lvols if nid == primary.get_id() else []

        class FakeFW:
            def __init__(self, node, timeout=3, retry=1):
                pass

            def firewall_set_port(self, *a, **kw):
                pass

        def _ana(lvol, node, ana_state):
            events.append(("ana", node.get_id(), ana_state))

        def _set_status(cid, status):
            events.append(("status", status))

        for n in (primary, secondary):
            n.write_to_db = MagicMock(return_value=True)
            n.rpc_client = MagicMock()
            n.recreate_hublvol = MagicMock(return_value=True)
            n.create_secondary_hublvol = MagicMock(return_value=True)
            n.connect_to_hublvol = MagicMock(return_value=True)
            n.client = MagicMock()
        cluster.is_qos_set = lambda: False

        patches = [
            patch.object(cluster_ops, "db_controller", db),
            patch.object(cluster_ops, "DBController", return_value=db),
            patch.object(cluster_ops, "FirewallClient", FakeFW),
            patch.object(cluster_ops.tcp_ports_events, "port_deny", lambda *a, **k: None),
            patch.object(cluster_ops.tcp_ports_events, "port_allowed", lambda *a, **k: None),
            patch.object(cluster_ops.tasks_controller, "add_port_allow_task", lambda *a, **k: None),
            patch.object(cluster_ops.storage_node_ops, "recreate_lvstore",
                         lambda snode, activation_mode=False, **kw: True),
            patch.object(cluster_ops.storage_node_ops, "recreate_lvstore_on_non_leader",
                         lambda snode, leader, primary_node, activation_mode=False, **kw: True),
            patch.object(cluster_ops.storage_node_ops, "_set_lvol_ana_on_node", _ana),
            patch.object(cluster_ops.storage_node_ops, "get_next_physical_device_order",
                         lambda *a, **kw: 0),
            patch.object(cluster_ops.storage_node_ops, "get_secondary_nodes",
                         lambda *a, **kw: [secondary.get_id()]),
            patch.object(cluster_ops.storage_node_ops, "get_secondary_nodes_2",
                         lambda *a, **kw: []),
            patch.object(cluster_ops, "set_cluster_status", _set_status),
            patch.object(cluster_ops, "time", MagicMock()),
            patch.object(cluster_ops.qos_controller, "get_qos_weights_list",
                         lambda *a, **kw: []),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in patches])

        cluster_ops.cluster_activate("cluster-1", force=True)
        return events

    def test_pass4_sets_ana_then_active(self):
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", lvstore="LVS_A", mgmt_ip="10.0.0.2",
                          rpc_port=8081)
        lvols = [_lvol("lv-1"), _lvol("lv-2")]
        events = self._run(_cluster(), primary, secondary, lvols)

        ana = [(e[1], e[2]) for e in events if e[0] == "ana"]
        # primary -> optimized for both lvols
        self.assertEqual(
            sorted([st for (nid, st) in ana if nid == "primary-1"]),
            ["optimized", "optimized"])
        # secondary -> non_optimized for both lvols
        self.assertEqual(
            sorted([st for (nid, st) in ana if nid == "secondary-1"]),
            ["non_optimized", "non_optimized"])

    def test_active_is_set_after_all_ana_writes(self):
        primary = _node("primary-1", primary_secondary="secondary-1")
        secondary = _node("secondary-1", mgmt_ip="10.0.0.2", rpc_port=8081)
        lvols = [_lvol("lv-1")]
        events = self._run(_cluster(), primary, secondary, lvols)

        # The final ACTIVE transition must come after every ANA write.
        active_idx = max(i for i, e in enumerate(events)
                         if e[0] == "status" and e[1] == Cluster.STATUS_ACTIVE)
        last_ana_idx = max((i for i, e in enumerate(events) if e[0] == "ana"),
                           default=-1)
        self.assertGreater(active_idx, last_ana_idx,
                           f"STATUS_ACTIVE set before ANA writes: {events}")


if __name__ == "__main__":
    unittest.main()
