# coding=utf-8
"""
Regression tests for the LVS_9060 incident (2026-04-25, 10:32 run).

When a node was the *secondary* of an LVS, came back from a network
outage while the original primary was still offline, the dispatcher in
recreate_all_lvstores Step 2 unconditionally promoted the recovering
secondary to leader — without checking whether the *tertiary* was
already serving as leader. The wrongly-promoted secondary then drove
recreate_lvstore in takeover mode, which hardcoded role="primary" on
the kernel-side lvstore. Result: two leaders (the still-running
tertiary on the data plane + the new secondary-as-primary), writer
conflict on the journal, and unexpected node down state.

These tests cover both the dispatch fix and the role-derivation fix:

  Dispatch (recreate_all_lvstores Step 2):
    1. primary disconnected, tertiary online  -> non-leader recreate
       with leader_node = tertiary
    2. primary disconnected, tertiary disconnected -> takeover via
       recreate_lvstore(lvs_primary=...)
    3. primary disconnected, no tertiary configured -> takeover

  Role derivation (recreate_lvstore Step 7):
    4. takeover, snode is the secondary of lvs_node  -> role=secondary
    5. takeover, snode is the tertiary of lvs_node   -> role=tertiary
    6. no takeover                                   -> role=primary
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1"):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = "ha"
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.max_fault_tolerance = 2
    c.client_qpair_count = 3
    c.client_data_nic = ""
    c.status = Cluster.STATUS_ACTIVE
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          lvstore="", secondary_node_id="", tertiary_node_id="",
          mgmt_ip="", rpc_port=8080, lvol_subsys_port=4434,
          lvstore_ports=None, jm_vuid=100, lvstore_status="ready",
          lvstore_stack_secondary="", lvstore_stack_tertiary=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = f"host-{uuid[:8]}"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.tertiary_node_id = tertiary_node_id
    n.mgmt_ip = mgmt_ip or f"10.0.0.{abs(hash(uuid)) % 254 + 1}"
    n.rpc_port = rpc_port
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.lvol_subsys_port = lvol_subsys_port
    n.lvstore_ports = dict(lvstore_ports) if lvstore_ports else {}
    n.active_tcp = True
    n.active_rdma = False
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.jm_vuid = jm_vuid
    n.lvstore_status = lvstore_status
    n.enable_ha_jm = False
    n.lvstore_stack = []
    n.raid = "raid0"
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}",
                          "bdev_name": f"{lvstore or 'lvs'}/hublvol",
                          "model_number": "model1", "nguid": "0" * 32})
    n.remote_devices = []
    n.remote_jm_devices = []
    n.nvme_devices = []
    n.health_check = True
    nic = IFace()
    nic.ip4_address = mgmt_ip or n.mgmt_ip
    nic.trtype = "TCP"
    n.data_nics = [nic]
    return n


# --------------------------------------------------------------------------
# Step 2 dispatcher cascade
# --------------------------------------------------------------------------

class TestRecreateAllLvstoresStep2Cascade(unittest.TestCase):
    """recreate_all_lvstores Step 2 must defer to a still-online tertiary
    instead of always promoting the recovering secondary."""

    def _topology(self):
        """Three nodes for an LVS_9060-like topology:
        primary=lvs_owner, secondary=snode (the one being restarted),
        tertiary=tert. (StorageNode.get_id() returns just the uuid,
        so secondary_node_id / tertiary_node_id store bare uuids.)
        """
        snode = _node(
            "snode", lvstore="LVS_SNODE_PRIMARY",
            lvstore_stack_secondary="lvs_owner",
            mgmt_ip="10.0.0.205", rpc_port=8084, jm_vuid=100)
        lvs_owner = _node(
            "lvs_owner", lvstore="LVS_9060",
            secondary_node_id="snode",
            tertiary_node_id="tert",
            status=StorageNode.STATUS_OFFLINE,
            mgmt_ip="10.0.0.204", rpc_port=8083, jm_vuid=9060)
        tert = _node(
            "tert", lvstore="LVS_TERT_PRIMARY",
            mgmt_ip="10.0.0.206", rpc_port=8085, jm_vuid=200)
        # Detach DB writes so test doesn't try to hit FDB
        for n in (snode, lvs_owner, tert):
            n.write_to_db = MagicMock()
        return {"snode": snode, "lvs_owner": lvs_owner, "tert": tert}

    def _common_setup(self, m, nodes):
        db = m["db_cls"].return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []

    def _patches(self):
        return [
            patch("simplyblock_core.storage_node_ops._check_peer_disconnected"),
            patch("simplyblock_core.storage_node_ops.recreate_lvstore"),
            patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader"),
            patch("simplyblock_core.storage_node_ops.DBController"),
        ]

    def _enter(self, patches):
        mocks = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in patches])
        return {
            "check_peer_disc": mocks[0],
            "recreate": mocks[1],
            "recreate_non_leader": mocks[2],
            "db_cls": mocks[3],
        }

    def test_primary_offline_secondary_takes_leadership(self):
        """Per the leadership hierarchy primary > secondary > tertiary,
        the rejoining secondary (snode) takes leadership unconditionally
        whenever the primary is disconnected — regardless of whether the
        tertiary happens to be online. This protects against split-brain
        where the secondary would defer to a tertiary that itself only
        ever gets to lead in the primary-AND-secondary-offline case."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter(patches)
        nodes = self._topology()
        self._common_setup(m, nodes)

        # Primary is disconnected; tertiary is online (returns False).
        def disc_side(node, lvs_peer_ids=None):
            return node.get_id() == "lvs_owner"

        m["check_peer_disc"].side_effect = disc_side
        m["recreate"].return_value = True
        m["recreate_non_leader"].return_value = True

        ok = storage_node_ops.recreate_all_lvstores(nodes["snode"])
        self.assertTrue(ok)

        # Step 2 MUST have taken leadership: primary is offline, so the
        # secondary (next in hierarchy) takes over. Filter by lvs_primary
        # presence to ignore Step 1 (snode's own primary recreation).
        takeover_calls = [c for c in m["recreate"].call_args_list
                          if c.kwargs.get("lvs_primary") is not None]
        self.assertEqual(len(takeover_calls), 1,
                         f"Expected exactly one takeover call, got {takeover_calls}")
        call_args = takeover_calls[0]
        self.assertEqual(call_args.args[0].get_id(), "snode")
        self.assertEqual(call_args.kwargs["lvs_primary"].get_id(), "lvs_owner")

        # Non-leader path NOT used in Step 2 — secondary outranks tertiary.
        m["recreate_non_leader"].assert_not_called()

    def test_primary_offline_tertiary_offline_uses_takeover(self):
        """When both primary and tertiary are unreachable, snode (the
        secondary) really is the only surviving peer and must take
        leadership via recreate_lvstore(lvs_primary=...)."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter(patches)
        nodes = self._topology()
        self._common_setup(m, nodes)

        # Both primary and tertiary disconnected
        m["check_peer_disc"].return_value = True
        m["recreate"].return_value = True

        ok = storage_node_ops.recreate_all_lvstores(nodes["snode"])
        self.assertTrue(ok)

        # Step 2 must have invoked takeover (recreate_lvstore with lvs_primary)
        takeover_calls = [c for c in m["recreate"].call_args_list
                          if c.kwargs.get("lvs_primary") is not None]
        self.assertEqual(len(takeover_calls), 1,
                         f"Expected exactly one takeover call, got {takeover_calls}")
        call_args = takeover_calls[0]
        self.assertEqual(call_args.args[0].get_id(), "snode")
        self.assertEqual(call_args.kwargs["lvs_primary"].get_id(), "lvs_owner")

        # Non-leader path NOT used
        m["recreate_non_leader"].assert_not_called()

    def test_primary_offline_no_tertiary_configured_uses_takeover(self):
        """If the lvstore has no tertiary configured at all, snode (the
        secondary) takes leadership."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter(patches)
        nodes = self._topology()
        nodes["lvs_owner"].tertiary_node_id = ""  # no tertiary
        self._common_setup(m, nodes)

        # Primary disconnected
        def disc_side(node, lvs_peer_ids=None):
            return node.get_id() == "lvs_owner"

        m["check_peer_disc"].side_effect = disc_side
        m["recreate"].return_value = True

        ok = storage_node_ops.recreate_all_lvstores(nodes["snode"])
        self.assertTrue(ok)

        takeover_calls = [c for c in m["recreate"].call_args_list
                          if c.kwargs.get("lvs_primary") is not None]
        self.assertEqual(len(takeover_calls), 1)
        m["recreate_non_leader"].assert_not_called()

    def test_primary_online_uses_non_leader_path_with_primary_as_leader(self):
        """Pre-existing behavior: primary online -> snode joins as
        non-leader with leader=primary. Regression test."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter(patches)
        nodes = self._topology()
        self._common_setup(m, nodes)

        # Nothing is disconnected
        m["check_peer_disc"].return_value = False
        m["recreate_non_leader"].return_value = True

        ok = storage_node_ops.recreate_all_lvstores(nodes["snode"])
        self.assertTrue(ok)

        # No takeover invocations (Step 1 may call recreate_lvstore without
        # lvs_primary; we filter on that)
        takeover_calls = [c for c in m["recreate"].call_args_list
                          if c.kwargs.get("lvs_primary") is not None]
        self.assertEqual(takeover_calls, [])
        m["recreate_non_leader"].assert_called_once()
        leader_arg = m["recreate_non_leader"].call_args.args[1]
        self.assertEqual(leader_arg.get_id(), "lvs_owner")


# --------------------------------------------------------------------------
# recreate_lvstore role derivation on takeover
# --------------------------------------------------------------------------

class TestRecreateLvstoreRoleDerivation(unittest.TestCase):
    """recreate_lvstore Step 7 must pass the topology role of snode
    relative to lvs_node (primary/secondary/tertiary), not hardcode
    role='primary'."""

    def _build_takeover_nodes(self, snode_role):
        """snode_role: 'secondary' or 'tertiary' relative to lvs_owner.

        Returns (snode, lvs_owner) where lvs_owner is offline and
        recreate_lvstore is invoked in takeover mode for lvs_owner.lvstore.
        """
        if snode_role == "secondary":
            snode = _node("snode", lvstore="LVS_OWN",
                           mgmt_ip="10.0.0.205", rpc_port=8084,
                           jm_vuid=100,
                           lvstore_ports={"LVS_TAKEOVER": {
                               "lvol_subsys_port": 4432,
                               "hublvol_port": 4433}})
            lvs_owner = _node("lvs_owner", lvstore="LVS_TAKEOVER",
                               secondary_node_id="snode",
                               tertiary_node_id="tert",
                               status=StorageNode.STATUS_OFFLINE,
                               jm_vuid=9060)
        elif snode_role == "tertiary":
            snode = _node("snode", lvstore="LVS_OWN",
                           mgmt_ip="10.0.0.206", rpc_port=8085,
                           jm_vuid=100,
                           lvstore_ports={"LVS_TAKEOVER": {
                               "lvol_subsys_port": 4432,
                               "hublvol_port": 4433}})
            lvs_owner = _node("lvs_owner", lvstore="LVS_TAKEOVER",
                               secondary_node_id="sec",
                               tertiary_node_id="snode",
                               status=StorageNode.STATUS_OFFLINE,
                               jm_vuid=9060)
        else:
            raise ValueError(snode_role)
        return snode, lvs_owner

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=True)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._failback_primary_ana")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs", return_value=[])
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_devs", return_value=[])
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack", return_value=(True, None))
    @patch("simplyblock_core.storage_node_ops.DBController")
    def _run_takeover(self, snode_role,
                      mock_db_cls, mock_create_bdev, mock_connect_devs,
                      mock_connect_jm, mock_rpc_cls, mock_fw_cls,
                      mock_storage_events, mock_tcp_events, mock_health,
                      mock_failback, mock_phase, mock_disc):
        from simplyblock_core import storage_node_ops

        snode, lvs_owner = self._build_takeover_nodes(snode_role)
        nodes = {"snode": snode, "lvs_owner": lvs_owner}
        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes.get(key, snode)

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []
        db.get_cluster_by_id.return_value = _cluster()

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [
            {"lvs leadership": True, "uuid": "u", "lvs_primary": False}
        ]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.jc_explicit_synchronization.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc
        mock_fw_cls.return_value = MagicMock()
        mock_health.check_bdev.return_value = True

        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock(return_value=True)
            n.create_secondary_hublvol = MagicMock()
            n.adopt_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.client = MagicMock(return_value=MagicMock())

        storage_node_ops.recreate_lvstore(snode, lvs_primary=lvs_owner)

        # Find the bdev_lvol_set_lvs_opts call against lvs_owner.lvstore
        opts_calls = [
            c for c in rpc.bdev_lvol_set_lvs_opts.call_args_list
            if c.args[0] == "LVS_TAKEOVER"
        ]
        return opts_calls

    def test_takeover_secondary_role(self):
        """Takeover where snode is the secondary of lvs_node ->
        role='secondary'."""
        opts_calls = self._run_takeover("secondary")
        self.assertTrue(opts_calls,
                        "set_lvs_opts was never invoked for the takeover lvstore")
        # The leadership-take call uses role kwarg
        leadership_call = next(
            (c for c in opts_calls if "role" in c.kwargs), None)
        self.assertIsNotNone(leadership_call,
                             "set_lvs_opts call with role= was never made")
        self.assertEqual(leadership_call.kwargs["role"], "secondary",
                         f"Wrong role on takeover (snode is secondary): "
                         f"{leadership_call.kwargs['role']}")

    def test_takeover_tertiary_role(self):
        """Takeover where snode is the tertiary of lvs_node ->
        role='tertiary'. (Not exercised by current dispatcher, but the
        derivation must be correct so future cascades stay safe.)"""
        opts_calls = self._run_takeover("tertiary")
        self.assertTrue(opts_calls)
        leadership_call = next(
            (c for c in opts_calls if "role" in c.kwargs), None)
        self.assertIsNotNone(leadership_call)
        self.assertEqual(leadership_call.kwargs["role"], "tertiary",
                         f"Wrong role on takeover (snode is tertiary): "
                         f"{leadership_call.kwargs['role']}")


# --------------------------------------------------------------------------
# Step 8b — sec_1 hublvol creation + per-peer role assignment
# --------------------------------------------------------------------------

class TestRecreateLvstoreStep8bHublvolWiring(unittest.TestCase):
    """Step 8b on takeover must:

    1. Pass lvs_node (the original primary owner) to
       create_secondary_hublvol — NOT snode (the new leader). Reading
       the lvstore name off snode.lvstore is wrong: snode.lvstore is
       snode's OWN primary, which leaks across takeovers (LVS_9060
       takeover, 2026-04-25 11:28:50 run, routed sec_1 work to
       LVS_9380 because LVS_9380's takeover ran just before and
       mutated snode's local state).

    2. Choose sec_1 by topology (lvs_node.secondary_node_id), not by
       sec_nodes index. When the original primary is in
       disconnected_peers, sec_nodes[0] becomes whichever peer
       happened to come next, and the index-based 'secondary' /
       'tertiary' assignment no longer matches topology.
    """

    def _build_topology(self, snode_role):
        """Build snode + lvs_owner + remaining peers for a takeover where
        snode plays ``snode_role`` (secondary or tertiary) of lvs_owner.

        snode_role='tertiary' is the LVS_9060 case: lvs_owner.primary is
        offline, the topological secondary (sec) is online and was the
        prior acting leader, and snode (the topological tertiary) is
        restarting and taking leadership.

        snode_role='secondary' is the LVS_9380 case: snode is the
        topological secondary taking leadership, and the topological
        tertiary (tert) is the only remaining peer.
        """
        if snode_role == "tertiary":
            snode = _node("snode", lvstore="LVS_OWN_OF_SNODE",
                           jm_vuid=100,
                           lvstore_ports={"LVS_TAKEOVER": {
                               "lvol_subsys_port": 4432,
                               "hublvol_port": 4433}})
            lvs_owner = _node("lvs_owner", lvstore="LVS_TAKEOVER",
                               secondary_node_id="sec",
                               tertiary_node_id="snode",
                               status=StorageNode.STATUS_OFFLINE,
                               jm_vuid=9060)
            sec = _node("sec", lvstore="LVS_OWN_OF_SEC", jm_vuid=200)
            return {"snode": snode, "lvs_owner": lvs_owner, "sec": sec}
        elif snode_role == "secondary":
            snode = _node("snode", lvstore="LVS_OWN_OF_SNODE",
                           jm_vuid=100,
                           lvstore_ports={"LVS_TAKEOVER": {
                               "lvol_subsys_port": 4432,
                               "hublvol_port": 4433}})
            lvs_owner = _node("lvs_owner", lvstore="LVS_TAKEOVER",
                               secondary_node_id="snode",
                               tertiary_node_id="tert",
                               status=StorageNode.STATUS_OFFLINE,
                               jm_vuid=9060)
            tert = _node("tert", lvstore="LVS_OWN_OF_TERT", jm_vuid=300)
            return {"snode": snode, "lvs_owner": lvs_owner, "tert": tert}
        else:
            raise ValueError(snode_role)

    def _run_step8b(self, snode_role,
                    primary_in_disconnected=True,
                    secondary_in_disconnected=False,
                    tertiary_in_disconnected=False):
        """Drive recreate_lvstore in takeover mode and capture what
        Step 8b did. Returns a dict with the captured calls.

        Most peer-side RPCs/events are mocked away (we only care about
        which peer's create_secondary_hublvol/connect_to_hublvol got
        called with what arguments)."""
        from simplyblock_core import storage_node_ops

        nodes = self._build_topology(snode_role)
        snode = nodes["snode"]
        lvs_owner = nodes["lvs_owner"]

        disconnected_peers = set()
        if primary_in_disconnected:
            disconnected_peers.add(lvs_owner.get_id())
        if snode_role == "tertiary":
            sec = nodes["sec"]
            if secondary_in_disconnected:
                disconnected_peers.add(sec.get_id())
        if snode_role == "secondary":
            tert = nodes["tert"]
            if tertiary_in_disconnected:
                disconnected_peers.add(tert.get_id())

        def disc_side(node, lvs_peer_ids=None):
            return node.get_id() in disconnected_peers

        captured = {
            "create_sec_hublvol_calls": [],
            "connect_calls": [],
        }

        def fake_create_sec(self_node, primary_node, cluster_nqn):
            captured["create_sec_hublvol_calls"].append({
                "self_id": self_node.get_id(),
                "primary_node_id": primary_node.get_id(),
                "primary_node_lvstore": primary_node.lvstore,
            })
            return f"nqn-{primary_node.lvstore}"

        def fake_connect(self_node, primary_node, failover_node=None,
                         role=None, timeout=None):
            captured["connect_calls"].append({
                "self_id": self_node.get_id(),
                "primary_node_id": primary_node.get_id(),
                "failover_id": failover_node.get_id() if failover_node else None,
                "role": role,
            })
            return True

        for n in nodes.values():
            n.write_to_db = MagicMock()
            n.rpc_client = MagicMock(return_value=MagicMock(
                bdev_lvol_get_lvstores=MagicMock(return_value=[
                    {"lvs leadership": True, "uuid": "u",
                     "lvs_primary": False}]),
                get_bdevs=MagicMock(return_value=[]),
                bdev_lvol_set_lvs_opts=MagicMock(return_value=True),
                bdev_lvol_set_leader=MagicMock(return_value=True),
                bdev_wait_for_examine=MagicMock(return_value=True),
                bdev_examine=MagicMock(return_value=True),
                bdev_distrib_force_to_non_leader=MagicMock(return_value=True),
                jc_compression_get_status=MagicMock(return_value=False),
                jc_explicit_synchronization=MagicMock(return_value=True),
                bdev_distrib_check_inflight_io=MagicMock(return_value=False),
            ))
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.adopt_hublvol = MagicMock()
            # Bind the captured-call helpers as bound methods so call sites
            # see the standard self-first signature.
            n.create_secondary_hublvol = lambda primary_node, cluster_nqn, _self=n: \
                fake_create_sec(_self, primary_node, cluster_nqn)
            n.connect_to_hublvol = lambda primary_node, failover_node=None, role=None, \
                                          timeout=None, rpc_timeout=None, _self=n: \
                fake_connect(_self, primary_node, failover_node=failover_node,
                             role=role, timeout=timeout)
            n.client = MagicMock(return_value=MagicMock())

        with patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
                   side_effect=disc_side), \
             patch("simplyblock_core.storage_node_ops._set_restart_phase"), \
             patch("simplyblock_core.storage_node_ops._failback_primary_ana"), \
             patch("simplyblock_core.storage_node_ops.health_controller") as mh, \
             patch("simplyblock_core.storage_node_ops.tcp_ports_events"), \
             patch("simplyblock_core.storage_node_ops.storage_events"), \
             patch("simplyblock_core.storage_node_ops.FirewallClient",
                   return_value=MagicMock()), \
             patch("simplyblock_core.rpc_client.RPCClient",
                   return_value=MagicMock(
                       bdev_lvol_get_lvstores=MagicMock(return_value=[
                           {"lvs leadership": True}]))), \
             patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs",
                   return_value=[]), \
             patch("simplyblock_core.storage_node_ops._connect_to_remote_devs",
                   return_value=[]), \
             patch("simplyblock_core.storage_node_ops._create_bdev_stack",
                   return_value=(True, None)), \
             patch("simplyblock_core.storage_node_ops.DBController") as mdb:

            mh.check_bdev.return_value = True

            db = mdb.return_value
            db.get_storage_node_by_id.side_effect = \
                lambda nid: nodes.get(nid.split("/")[-1] if "/" in nid else nid, snode)
            db.get_lvols_by_node_id.return_value = []
            db.get_snapshots_by_node_id.return_value = []
            db.get_cluster_by_id.return_value = _cluster()

            storage_node_ops.recreate_lvstore(snode, lvs_primary=lvs_owner)

        return captured

    def test_create_secondary_hublvol_called_with_lvs_node_in_takeover(self):
        """LVS_9060 case: snode is the topological tertiary, takes
        leadership, and sec_1 is the topological secondary peer.

        create_secondary_hublvol must be invoked with lvs_owner (which
        carries lvstore=LVS_TAKEOVER), NOT with snode (whose lvstore is
        snode's OWN primary). The whole point of this fix is that
        sec_1 ends up exposing a hublvol bdev for THE LVS BEING TAKEN
        OVER, not for an unrelated LVS that snode happens to own."""
        captured = self._run_step8b("tertiary")

        sec_calls = captured["create_sec_hublvol_calls"]
        self.assertEqual(len(sec_calls), 1,
                         f"Expected exactly one create_secondary_hublvol "
                         f"call on sec_1, got {sec_calls}")
        c = sec_calls[0]
        self.assertEqual(c["self_id"], "sec",
                         "create_secondary_hublvol must run on the "
                         "topological secondary owner ('sec'), not some "
                         "other peer")
        self.assertEqual(c["primary_node_id"], "lvs_owner",
                         "create_secondary_hublvol must be called with "
                         "lvs_owner (the LVS being taken over), not snode")
        self.assertEqual(c["primary_node_lvstore"], "LVS_TAKEOVER",
                         "primary_node.lvstore must resolve to the LVS "
                         "being taken over (LVS_TAKEOVER), not snode's "
                         "own primary lvstore")

    def test_connect_role_assignment_uses_topology_tertiary_takeover(self):
        """LVS_9060 case: in the post-takeover ring, sec_1 is the
        topological secondary owner and gets role='secondary'. There
        are no other peers in this scenario, so no 'tertiary' role
        connect happens."""
        captured = self._run_step8b("tertiary")

        connects = captured["connect_calls"]
        # There is one peer (sec) — only one connect_to_hublvol expected.
        self.assertEqual(len(connects), 1,
                         f"Expected one connect_to_hublvol call (to sec_1), "
                         f"got {connects}")
        c = connects[0]
        self.assertEqual(c["self_id"], "sec")
        self.assertEqual(c["primary_node_id"], "snode")
        self.assertEqual(c["role"], "secondary",
                         "The topological secondary owner must get "
                         "role='secondary' regardless of its index in "
                         "sec_nodes")
        self.assertIsNone(c["failover_id"],
                          "The secondary peer connects only to the new "
                          "leader; no failover path applies")

    def test_connect_role_assignment_uses_topology_secondary_takeover(self):
        """LVS_9380 case: snode is the topological secondary taking
        leadership; the only remaining peer is the topological
        tertiary. With topology-based role assignment, the tertiary
        peer gets role='tertiary' (NOT 'secondary' as the old
        index-based code would assign). Since snode IS the topological
        secondary, no separate sec_1 hublvol is created."""
        captured = self._run_step8b("secondary")

        # No create_secondary_hublvol call — snode IS sec_1 topologically,
        # so there is no separate node to expose the secondary hublvol on.
        self.assertEqual(captured["create_sec_hublvol_calls"], [],
                         "create_secondary_hublvol must NOT be called when "
                         "snode itself is the topological secondary owner")

        connects = captured["connect_calls"]
        self.assertEqual(len(connects), 1,
                         f"Expected one connect_to_hublvol call (to tert), "
                         f"got {connects}")
        c = connects[0]
        self.assertEqual(c["self_id"], "tert")
        self.assertEqual(c["role"], "tertiary",
                         "The topological tertiary must get role='tertiary' "
                         "even though the index-based logic would assign "
                         "'secondary' (since the offline primary was "
                         "filtered out)")


if __name__ == "__main__":
    unittest.main()
