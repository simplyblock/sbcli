# coding=utf-8
"""
test_connect_hublvol_lvs_node.py — pins the ``lvs_node`` parameter on
``StorageNode.connect_to_hublvol`` and verifies it routes LVS metadata
from the configured primary, not from the (possibly peer) hublvol target.

Background — incident 2026-05-02 (k8s_native_failover_ha-20260502-101452,
worker1 at 15:53:42):

  - worker1 is configured *secondary* of LVS_4729 (and *tertiary* of
    LVS_6207).
  - During worker1's restart of its LVS_6207 lvstore, the configured
    primary (worker2) was in a planned outage; worker5 had taken over
    leadership of LVS_6207. The recreate path on worker1 picked
    worker5 as ``sync_target`` (acting leader) and called
    ``connect_to_hublvol(worker5, role='tertiary')``.
  - The OLD code used ``primary_node.lvstore`` / ``primary_node.jm_vuid``
    / ``primary_node.hublvol.bdev_name`` for everything. Since
    worker5's OWN primary lvstore is LVS_4729 (not LVS_6207), the
    ``bdev_lvol_set_lvs_opts`` RPC fired with ``groupid=4729`` and
    ``port=4434`` while we were trying to wire up LVS_6207 — and the
    remote bdev name became ``LVS_4729/hublvoln1`` rather than the
    expected ``LVS_6207/hublvoln1``.

The fix: ``connect_to_hublvol`` accepts a separate ``lvs_node``
parameter (defaulting to ``primary_node`` for backward-compat) that
sources the LVS-side metadata. ``primary_node`` is now strictly the
nvme-of attach target.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


class FakeHubLvol:
    def __init__(self, bdev_name, nqn, port):
        self.bdev_name = bdev_name
        self.nqn = nqn
        self.nvmf_port = port
        self.uuid = "fake-uuid"
        self.nguid = "fake-nguid"
        self.model_number = "fake-model"


def _make_node(uuid, lvstore, jm_vuid, hub_bdev_name=None, hub_nqn=None,
               hub_port=4435, lvol_subsys_port=4434):
    n = StorageNode()
    n.uuid = uuid
    n.lvstore = lvstore
    n.jm_vuid = jm_vuid
    n.cluster_id = "c1"
    n.hublvol = FakeHubLvol(
        bdev_name=hub_bdev_name or f"{lvstore}/hublvol",
        nqn=hub_nqn or f"nqn.test:hublvol:{lvstore}",
        port=hub_port,
    )
    n.get_lvol_subsys_port = MagicMock(return_value=lvol_subsys_port)
    n.write_to_db = MagicMock(return_value=True)
    n.rpc_client = MagicMock()
    return n


class TestConnectToHublvolLvsNode(unittest.TestCase):
    """When lvs_node differs from primary_node, the LVS-side RPCs use
    lvs_node's metadata, not primary_node's."""

    def setUp(self):
        # The recovering node (snode) — the one calling connect_to_hublvol
        self.snode = _make_node("snode", "LVS_640", jm_vuid=640)

        # primary_node = the *peer* that took over leadership for
        # an LVS not its own (e.g. worker5 acting as leader of LVS_6207
        # despite owning LVS_4729 as its own primary).
        self.peer = _make_node(
            "peer-uuid", lvstore="LVS_4729", jm_vuid=4729,
            hub_bdev_name="LVS_4729/hublvol",
            hub_nqn="nqn.test:hublvol:LVS_4729",
            hub_port=4435,
            lvol_subsys_port=4434,
        )

        # lvs_node = the configured primary of the LVS we're connecting
        # for (e.g. worker2 for LVS_6207). The node may be offline; what
        # matters is its DB record carries the right LVS metadata.
        self.lvs_node = _make_node(
            "lvs-node-uuid", lvstore="LVS_6207", jm_vuid=6207,
            hub_bdev_name="LVS_6207/hublvol",
            hub_nqn="nqn.test:hublvol:LVS_6207",
            hub_port=4433,
            lvol_subsys_port=4432,
        )

        # snode.rpc_client() returns a MagicMock that records calls
        self.rpc = MagicMock()
        self.rpc.get_bdevs.return_value = [{"name": "LVS_6207/hublvoln1"}]
        self.rpc.bdev_lvol_set_lvs_opts.return_value = True
        self.rpc.bdev_lvol_connect_hublvol.return_value = True
        self.snode.rpc_client = MagicMock(return_value=self.rpc)

    def test_lvs_node_routes_set_lvs_opts(self):
        """bdev_lvol_set_lvs_opts must use lvs_node's lvstore + jm_vuid + port."""
        ok = self.snode.connect_to_hublvol(
            self.peer, failover_node=None, role="tertiary",
            lvs_node=self.lvs_node,
        )
        self.assertTrue(ok)
        # The set_lvs_opts call must use lvs_node's lvstore / jm_vuid / port,
        # NOT the peer's.
        self.rpc.bdev_lvol_set_lvs_opts.assert_called_once()
        args, kwargs = self.rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(args[0], "LVS_6207",
                         "lvstore positional arg should be lvs_node.lvstore "
                         "(LVS_6207), not peer.lvstore (LVS_4729)")
        self.assertEqual(kwargs.get("groupid"), 6207,
                         "groupid should be lvs_node.jm_vuid (6207)")
        self.assertEqual(kwargs.get("subsystem_port"), 4432,
                         "subsystem_port should be lvs_node's port (4432)")
        self.assertEqual(kwargs.get("role"), "tertiary")

    def test_lvs_node_routes_connect_hublvol(self):
        """bdev_lvol_connect_hublvol must use lvs_node.lvstore for both args."""
        ok = self.snode.connect_to_hublvol(
            self.peer, failover_node=None, role="tertiary",
            lvs_node=self.lvs_node,
        )
        self.assertTrue(ok)
        self.rpc.bdev_lvol_connect_hublvol.assert_called_once()
        args, kwargs = self.rpc.bdev_lvol_connect_hublvol.call_args
        self.assertEqual(args[0], "LVS_6207",
                         "first arg (lvstore) should be lvs_node.lvstore")
        # remote_bdev should be derived from lvs_node, not peer
        self.assertEqual(args[1], "LVS_6207/hublvoln1",
                         "remote bdev name should encode lvs_node's "
                         "lvstore (LVS_6207), not peer's (LVS_4729)")

    def test_lvs_node_routes_remote_bdev_lookup(self):
        """The pre-attach bdev existence check must look up by lvs_node's bdev name."""
        # Force the get_bdevs branch to confirm what name is checked.
        self.rpc.get_bdevs.return_value = []  # bdev not present → triggers coordinator
        with patch(
            "simplyblock_core.utils.hublvol_reconnect.HublvolReconnectCoordinator"
        ) as mock_coord_cls:
            mock_coord = MagicMock()
            mock_coord.reconcile.return_value = True
            mock_coord_cls.return_value = mock_coord
            self.snode.connect_to_hublvol(
                self.peer, failover_node=None, role="tertiary",
                lvs_node=self.lvs_node,
            )
        # The first get_bdevs call was the existence check on remote_bdev
        first_call = self.rpc.get_bdevs.call_args_list[0]
        args, _kw = first_call
        self.assertEqual(
            args[0], "LVS_6207/hublvoln1",
            "remote bdev existence check should use lvs_node's lvstore",
        )
        # The coordinator should be invoked with lvs_node, not peer,
        # so it derives ctrl_name / nqn / port from lvs_node.hublvol.
        mock_coord.reconcile.assert_called_once()
        c_args, _c_kw = mock_coord.reconcile.call_args
        # Signature: (node, primary_node_or_lvs_node, peers, role=...)
        self.assertIs(c_args[1], self.lvs_node,
                      "coordinator must receive lvs_node so its hublvol "
                      "metadata (NQN/bdev_name/port) matches the LVS we're "
                      "connecting for, not the peer's own primary LVS")
        self.assertIn(self.peer, c_args[2],
                      "peer must still appear in peer_nodes — it remains "
                      "the actual NVMe-oF attach target (its IPs)")


class TestBackwardCompat(unittest.TestCase):
    """Without lvs_node, behavior is identical to the prior
    primary_node-only contract."""

    def test_lvs_node_defaults_to_primary_node(self):
        node = _make_node("self", "LVS_640", jm_vuid=640)
        primary = _make_node("primary", "LVS_3261", jm_vuid=3261,
                             hub_bdev_name="LVS_3261/hublvol",
                             hub_nqn="nqn.test:hublvol:LVS_3261",
                             hub_port=4427,
                             lvol_subsys_port=4420)
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": "LVS_3261/hublvoln1"}]
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_connect_hublvol.return_value = True
        node.rpc_client = MagicMock(return_value=rpc)

        ok = node.connect_to_hublvol(primary, failover_node=None, role="secondary")
        self.assertTrue(ok)
        # Without lvs_node, all metadata comes from primary_node — backward compat.
        args, kwargs = rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(args[0], "LVS_3261")
        self.assertEqual(kwargs.get("groupid"), 3261)
        self.assertEqual(kwargs.get("subsystem_port"), 4420)


class TestSourceCallSites(unittest.TestCase):
    """Pin the call sites in recreate_lvstore_on_non_leader use lvs_node."""

    @classmethod
    def setUpClass(cls):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "storage_node_ops.py",
        )
        with open(path, "r") as f:
            cls.src = f.read()

    def test_recreate_on_non_leader_passes_lvs_node_for_tertiary_branch(self):
        # The tertiary branch resolves its hublvol attach target into
        # ``attach_target = sync_target`` (which may be a peer acting as
        # leader for an LVS that primary_node owns but is offline for).
        # The subsequent connect_to_hublvol on that target must pass
        # lvs_node=primary_node so LVS metadata comes from the configured
        # primary, not the peer.
        start = self.src.index("def recreate_lvstore_on_non_leader(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        # Tertiary branch maps sync_target onto attach_target.
        tertiary_idx = body.index("attach_target = sync_target")
        tertiary_window = body[tertiary_idx:tertiary_idx + 2000]
        self.assertIn(
            "lvs_node=primary_node",
            tertiary_window,
            "tertiary branch in recreate_lvstore_on_non_leader must pass "
            "lvs_node=primary_node so connect_to_hublvol uses the right "
            "LVS metadata when the attach target is a peer",
        )

    def test_recreate_on_non_leader_passes_lvs_node_for_secondary_branch(self):
        start = self.src.index("def recreate_lvstore_on_non_leader(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        # Secondary branch sets attach_target = leader_node, then the
        # shared connect call (snode.connect_to_hublvol(attach_target, ...))
        # must route LVS metadata via lvs_node=primary_node since the
        # leader may be a peer.
        sec_idx = body.index("attach_target = leader_node")
        sec_window = body[sec_idx:sec_idx + 2000]
        self.assertIn(
            "lvs_node=primary_node",
            sec_window,
            "secondary branch must also route via lvs_node=primary_node "
            "(leader_node may be a peer)",
        )

    def test_recreate_lvstore_takeover_passes_lvs_node(self):
        # The takeover branch of recreate_lvstore (snode is taking over
        # leadership for an LVS whose configured primary, lvs_primary, is
        # offline) must pass lvs_node=lvs_node so the peer iteration's
        # connect_to_hublvol routes LVS metadata from the configured
        # primary, not from snode's OWN primary-LVS. Without this, the
        # peer is reconfigured for snode's own LVS (the wrong one) and
        # the LVS being taken over never gets its hublvol wired up on
        # the peer — but the peer-port unblock fires anyway, and the
        # peer's existing tertiary path then re-promotes on the next
        # client write, producing a dual-leader writer conflict.
        # (incident 2026-05-21 05:38:14 k8s_native_resilient_failover-
        # 20260520-231822, LVS_270 takeover by worker-4.)
        start = self.src.index("def recreate_lvstore(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        # The peer-loop connect call uses sec_node.connect_to_hublvol(snode, ...)
        idx = body.index("sec_node.connect_to_hublvol(snode")
        window = body[idx:idx + 800]
        self.assertIn(
            "lvs_node=lvs_node",
            window,
            "recreate_lvstore takeover peer-loop must pass "
            "lvs_node=lvs_node so connect_to_hublvol uses the metadata "
            "of the LVS being taken over, not snode's own primary LVS",
        )

    def test_no_naked_connect_to_hublvol_in_takeover_path(self):
        """Stricter pin: every connect_to_hublvol call inside
        recreate_lvstore must pass lvs_node= explicitly. Guards against
        a regression that adds a new call site in the takeover path
        without re-applying the metadata-routing arg."""
        start = self.src.index("def recreate_lvstore(")
        end = self.src.index("\ndef ", start + 1)
        body = self.src[start:end]
        cursor = 0
        call_token = "connect_to_hublvol("
        offenders = []
        while True:
            i = body.find(call_token, cursor)
            if i < 0:
                break
            depth = 1
            j = i + len(call_token)
            while j < len(body) and depth:
                ch = body[j]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                j += 1
            arglist = body[i + len(call_token):j - 1]
            if "lvs_node=" not in arglist:
                offenders.append(body[i:i + 120].replace("\n", " "))
            cursor = j
        self.assertEqual(
            offenders, [],
            "Every connect_to_hublvol call inside recreate_lvstore must "
            "pass lvs_node= explicitly (takeover paths break otherwise). "
            f"Offenders: {offenders}",
        )


# --------------------------------------------------------------------------
# Behavioral test: drive recreate_lvstore in takeover mode and assert that
# connect_to_hublvol on the surviving peer receives lvs_node=lvs_primary,
# not snode. Reproduces the LVS_270 incident topology:
#   worker-3 = primary (offline)
#   worker-4 = secondary, snode (restarting, taking leadership)
#   worker-1 = tertiary (was acting leader, now non-leader)
# --------------------------------------------------------------------------

class TestRecreateLvstoreTakeoverBehavioral(unittest.TestCase):

    def _node(self, uuid, lvstore, jm_vuid, secondary_node_id="",
              tertiary_node_id="", status="online", lvstore_ports=None):
        from simplyblock_core.models.iface import IFace
        from simplyblock_core.models.hublvol import HubLVol
        n = StorageNode()
        n.uuid = uuid
        n.status = status
        n.cluster_id = "c1"
        n.hostname = f"host-{uuid[:8]}"
        n.lvstore = lvstore
        n.jm_vuid = jm_vuid
        n.secondary_node_id = secondary_node_id
        n.tertiary_node_id = tertiary_node_id
        n.mgmt_ip = "10.0.0.1"
        n.rpc_port = 8080
        n.rpc_username = "u"
        n.rpc_password = "p"
        n.lvol_subsys_port = 4434
        n.lvstore_ports = dict(lvstore_ports) if lvstore_ports else {}
        n.active_tcp = True
        n.active_rdma = False
        n.lvstore_stack_secondary = ""
        n.lvstore_stack_tertiary = ""
        n.lvstore_status = "ready"
        n.enable_ha_jm = False
        n.lvstore_stack = []
        n.raid = "raid0"
        n.hublvol = HubLVol({"nvmf_port": 4433, "uuid": f"hub-{uuid}",
                              "nqn": f"nqn.hub.{uuid}",
                              "bdev_name": f"{lvstore}/hublvol",
                              "model_number": "m", "nguid": "0" * 32})
        n.remote_devices = []
        n.remote_jm_devices = []
        n.nvme_devices = []
        n.health_check = True
        nic = IFace()
        nic.ip4_address = n.mgmt_ip
        nic.trtype = "TCP"
        n.data_nics = [nic]
        return n

    def test_takeover_passes_lvs_primary_as_lvs_node(self):
        """Reproduce the LVS_270 incident topology: worker-3 (primary)
        offline, worker-4 (secondary, snode) restarting, worker-1
        (tertiary) online. connect_to_hublvol on worker-1 must receive
        lvs_node=worker-3, not lvs_node=worker-4 (the default that
        triggered the 2026-05-21 incident)."""
        from simplyblock_core import storage_node_ops
        from simplyblock_core.models.cluster import Cluster

        snode = self._node(
            "worker-4", lvstore="LVS_9915", jm_vuid=9915,
            lvstore_ports={"LVS_270": {"lvol_subsys_port": 4432,
                                        "hublvol_port": 4433}},
        )
        lvs_owner = self._node(
            "worker-3", lvstore="LVS_270", jm_vuid=270,
            secondary_node_id="worker-4",
            tertiary_node_id="worker-1",
            status="offline",
            lvstore_ports={"LVS_270": {"lvol_subsys_port": 4432,
                                        "hublvol_port": 4433}},
        )
        tertiary = self._node(
            "worker-1", lvstore="LVS_TERT_OWN", jm_vuid=1111,
            lvstore_ports={"LVS_270": {"lvol_subsys_port": 4432,
                                        "hublvol_port": 4433}},
        )
        nodes = {n.get_id(): n for n in (snode, lvs_owner, tertiary)}

        captured = []

        def make_capture(self_node):
            def fake(primary_node, failover_node=None, role=None,
                     timeout=None, rpc_timeout=None, lvs_node=None):
                captured.append({
                    "self_id": self_node.get_id(),
                    "primary_id": primary_node.get_id(),
                    "lvs_node_id": lvs_node.get_id() if lvs_node else None,
                    "role": role,
                })
                return True
            return fake

        cluster = Cluster()
        cluster.uuid = "c1"
        cluster.ha_type = "ha"
        cluster.distr_ndcs = 2
        cluster.distr_npcs = 2
        cluster.max_fault_tolerance = 2
        cluster.client_qpair_count = 3
        cluster.client_data_nic = ""
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.nqn = "nqn.cluster.c1"

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

        for n in nodes.values():
            n.write_to_db = MagicMock()
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock(return_value=True)
            n.adopt_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock(return_value="nqn-sec")
            n.connect_to_hublvol = make_capture(n)
            n.client = MagicMock(return_value=MagicMock())

        def disc(node, lvs_peer_ids=None):
            return node.get_id() == "worker-3"

        with patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
                   side_effect=disc), \
             patch("simplyblock_core.storage_node_ops._set_restart_phase"), \
             patch("simplyblock_core.storage_node_ops._failback_primary_ana"), \
             patch("simplyblock_core.storage_node_ops.health_controller"), \
             patch("simplyblock_core.storage_node_ops.tcp_ports_events"), \
             patch("simplyblock_core.storage_node_ops.storage_events"), \
             patch("simplyblock_core.port_block.set_port",
                   return_value=MagicMock()), \
             patch("simplyblock_core.rpc_client.RPCClient", return_value=rpc), \
             patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs",
                   return_value=[]), \
             patch("simplyblock_core.storage_node_ops._connect_to_remote_devs",
                   return_value=[]), \
             patch("simplyblock_core.storage_node_ops._create_bdev_stack",
                   return_value=(True, None)), \
             patch("simplyblock_core.storage_node_ops.DBController") as mdb:

            db = mdb.return_value
            db.get_storage_node_by_id.side_effect = lambda nid: nodes.get(
                nid.split("/")[-1] if "/" in nid else nid, snode)
            db.get_lvols_by_node_id.return_value = []
            db.get_snapshots_by_node_id.return_value = []
            db.get_cluster_by_id.return_value = cluster

            ok = storage_node_ops.recreate_lvstore(snode, lvs_primary=lvs_owner)
            self.assertTrue(ok)

        # Exactly one connect_to_hublvol on the tertiary peer.
        self.assertEqual(
            len(captured), 1,
            f"Expected one connect_to_hublvol call on tertiary, got {captured}")
        c = captured[0]
        self.assertEqual(c["self_id"], "worker-1",
                         "connect_to_hublvol should run on the tertiary peer")
        self.assertEqual(c["primary_id"], "worker-4",
                         "primary_node arg must be snode (the new acting "
                         "leader / hublvol attach target)")
        # THE assertion that pins the bug fix:
        self.assertEqual(
            c["lvs_node_id"], "worker-3",
            "lvs_node MUST be the configured primary (worker-3), not snode "
            "(worker-4). With lvs_node defaulting to primary_node=snode, "
            "the peer is reconfigured for snode's own LVS_9915 instead of "
            "LVS_270, the LVS_270 hublvol on the peer never gets wired up, "
            "and the peer-port unblock exposes LVS_270 in a pre-takeover "
            "state — producing the dual-leader writer conflict observed "
            "in the 2026-05-21 incident.")
        self.assertEqual(c["role"], "tertiary",
                         "worker-1 is the topological tertiary of LVS_270")


if __name__ == "__main__":
    unittest.main()
