# coding=utf-8
"""
test_recreate_lvstore_abort_on_peer_rpc_failure.py

Regression tests for the LVS_6609 writer-conflict incident (2026-04-25).

In recreate_lvstore, RPC failures to peers in the pre-block / port-block
window used to be routed through _handle_rpc_failure_on_peer, whose
_check_hublvol_connected probe is structurally meaningless on a freshly
restarting snode (no peer hublvol controller bdevs exist yet -> probe
always returns False -> "skip" path). That silently dropped the current
leader from tracking and let snode promote itself while the old leader
was still serving IO -> writer conflict on the journal.

Fix: any peer-RPC failure in this window now aborts the recreate attempt.
The retry that follows re-evaluates peer state via the data-plane check
(_check_peer_disconnected) at the top of recreate_lvstore.

These tests verify that:
  - leader-port-block FW raise     -> abort, snode NOT promoted, SPDK killed
  - non-leader port-block FW raise -> abort, leader port unblocked, SPDK killed
  - leader-detection RPC raise     -> abort, no _create_bdev_stack ran
  - replication-wait raise         -> abort
  - jc_compression check raise     -> abort

External dependencies (FDB, RPC, SPDK, FirewallClient) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol


# --------------------------------------------------------------------------
# Helpers (kept aligned with tests/test_dual_ft_secondary_fixes.py)
# --------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1", ha_type="ha", max_fault_tolerance=2):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = ha_type
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.max_fault_tolerance = max_fault_tolerance
    c.client_qpair_count = 3
    c.client_data_nic = ""
    c.status = Cluster.STATUS_ACTIVE
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          lvstore="", secondary_node_id="", tertiary_node_id="",
          mgmt_ip="", rpc_port=8080, lvol_subsys_port=4434,
          lvstore_ports=None, active_rdma=False, jm_vuid=6609,
          lvstore_status="ready"):
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
    n.active_rdma = active_rdma
    n.lvstore_stack_secondary = ""
    n.lvstore_stack_tertiary = ""
    n.jm_vuid = jm_vuid
    n.lvstore_status = lvstore_status
    n.enable_ha_jm = False
    n.lvstore_stack = []
    n.raid = "raid0_6609"
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


def _build_lvs6609_topology():
    """Replicates the topology from the LVS_6609 incident:
    - .205/snode (c3914196) primary for LVS_6609
    - .206 (951ffc7a) secondary
    - .201 (ea5eb8ef) tertiary
    """
    nodes = {
        "snode": _node(
            "c3914196", lvstore="LVS_6609",
            secondary_node_id="cluster-1/sec",
            tertiary_node_id="cluster-1/tert",
            lvstore_ports={"LVS_6609": {"lvol_subsys_port": 4434, "hublvol_port": 4435}},
            mgmt_ip="10.0.0.205", rpc_port=8084, jm_vuid=6609),
        "sec": _node(
            "951ffc7a", lvstore="LVS_7995",
            secondary_node_id="",
            tertiary_node_id="",
            lvstore_ports={"LVS_6609": {"lvol_subsys_port": 4434, "hublvol_port": 4435}},
            mgmt_ip="10.0.0.206", rpc_port=8085),
        "tert": _node(
            "ea5eb8ef", lvstore="LVS_1789",
            secondary_node_id="",
            tertiary_node_id="",
            lvstore_ports={"LVS_6609": {"lvol_subsys_port": 4434, "hublvol_port": 4435}},
            mgmt_ip="10.0.0.201", rpc_port=8080),
    }
    return nodes


def _attach_node_helpers(nodes, rpc):
    for n in nodes.values():
        n.rpc_client = MagicMock(return_value=rpc)
        n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        n.recreate_hublvol = MagicMock()
        n.connect_to_hublvol = MagicMock()
        n.write_to_db = MagicMock()


def _make_snode_api():
    """Returns a snode_api MagicMock that reports SPDK gone after kill so
    _kill_app() doesn't loop on time.sleep."""
    api = MagicMock()
    api.spdk_process_kill.return_value = True
    api.spdk_process_is_up.return_value = False
    return api


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

class TestRecreateLvstoreAbortsOnPeerRPCFailure(unittest.TestCase):
    """The bug: _check_hublvol_connected on a fresh snode always says
    "disconnected" because the peer hublvol controller bdevs haven't been
    re-attached yet. Fix: any peer-RPC failure in this window aborts."""

    def _patches(self):
        """Common patches for recreate_lvstore tests."""
        return [
            patch("simplyblock_core.storage_node_ops.set_node_status"),
            patch("simplyblock_core.storage_node_ops.time.sleep"),
            patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False),
            patch("simplyblock_core.storage_node_ops._set_restart_phase"),
            patch("simplyblock_core.storage_node_ops.health_controller"),
            patch("simplyblock_core.storage_node_ops.tcp_ports_events"),
            patch("simplyblock_core.storage_node_ops.storage_events"),
            patch("simplyblock_core.storage_node_ops.FirewallClient"),
            patch("simplyblock_core.models.storage_node.RPCClient"),
            patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs"),
            patch("simplyblock_core.storage_node_ops._connect_to_remote_devs"),
            patch("simplyblock_core.storage_node_ops._create_bdev_stack"),
            patch("simplyblock_core.storage_node_ops._failback_primary_ana"),
            patch("simplyblock_core.storage_node_ops.DBController"),
        ]

    def _enter_patches(self, patches):
        mocks = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in patches])
        # Order matches _patches(): bottom-up = unwind order
        return {
            "set_node_status": mocks[0],
            "sleep": mocks[1],
            "check_peer_disc": mocks[2],
            "set_phase": mocks[3],
            "health": mocks[4],
            "tcp_events": mocks[5],
            "storage_events": mocks[6],
            "fw_cls": mocks[7],
            "rpc_cls": mocks[8],
            "connect_jm": mocks[9],
            "connect_devs": mocks[10],
            "create_bdev": mocks[11],
            "failback_ana": mocks[12],
            "db_cls": mocks[13],
        }

    def _setup_common(self, m):
        """Wire common mocks so a happy-path recreate_lvstore would succeed."""
        m["create_bdev"].return_value = (True, None)
        m["connect_jm"].return_value = []
        m["connect_devs"].return_value = []

        nodes = _build_lvs6609_topology()
        db = m["db_cls"].return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            mapping = {"c3914196": "snode", "951ffc7a": "sec", "ea5eb8ef": "tert"}
            return nodes[mapping.get(key, key)]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []

        rpc = MagicMock()
        # On snode: leader=False (snode just restarted, has not promoted yet)
        rpc.bdev_lvol_get_lvstores.return_value = [{
            "lvs leadership": True,  # default: peer reports as leader
            "lvs_primary": False,
            "uuid": "lvs-uuid",
        }]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.jc_explicit_synchronization.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        m["rpc_cls"].return_value = rpc
        _attach_node_helpers(nodes, rpc)

        # snode.client() returns the SnodeAPI used by _kill_app
        snode_api = _make_snode_api()
        nodes["snode"].client = MagicMock(return_value=snode_api)

        m["health"].check_bdev.return_value = True
        return nodes, rpc, snode_api

    # ----------------------------------------------------------------------
    # Bug repro: leader port-block FW timeout → abort, no leader promotion
    # ----------------------------------------------------------------------
    def test_leader_port_block_fw_failure_aborts_and_does_not_promote(self):
        """LVS_6609 incident: FirewallClient.firewall_set_port raises on the
        leader. recreate_lvstore must abort (not silent-skip), so snode is
        NOT promoted and SPDK on snode is killed."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter_patches(patches)
        nodes, rpc, snode_api = self._setup_common(m)

        # FirewallClient: raises only when called against the secondary (leader)
        fw_calls = []

        def make_fw(node, **kwargs):
            fw = MagicMock()
            peer_id = getattr(node, "uuid", None)

            def _set_port(port, ptype, action, rpc_port):
                fw_calls.append((peer_id, port, action, rpc_port))
                if peer_id == "951ffc7a":  # the current leader (.206)
                    raise Exception("simulated SnodeAPI/firewall timeout")
                return True

            fw.firewall_set_port.side_effect = _set_port
            return fw

        m["fw_cls"].side_effect = make_fw

        snode = nodes["snode"]
        with self.assertRaises(Exception) as ctx:
            storage_node_ops.recreate_lvstore(snode)

        self.assertIn("port-block leader", str(ctx.exception).lower())

        # SPDK must have been killed on snode
        self.assertGreaterEqual(snode_api.spdk_process_kill.call_count, 1)

        # snode must NOT have been promoted to primary leader
        promote_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if (c.kwargs.get("leader") is True or
                (len(c.args) > 1 and c.args[1] is True))
        ]
        self.assertEqual(promote_calls, [],
                         f"snode was promoted despite leader port-block failure: {promote_calls}")

        # Old leader must NOT have been demoted by mgmt RPC (set_leader leader=False)
        demote_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if (c.kwargs.get("leader") is False or
                (len(c.args) > 1 and c.args[1] is False))
        ]
        self.assertEqual(demote_calls, [],
                         "Demote leader RPC was issued despite port-block failure on leader")

    # ----------------------------------------------------------------------
    # Non-leader port-block fails AFTER leader was blocked → leader unblocked
    # ----------------------------------------------------------------------
    def test_non_leader_port_block_fw_failure_aborts_and_unblocks_leader(self):
        """If the non-leader port-block fails after the leader was blocked,
        abort must unwind by unblocking the leader (so client IO isn't
        stranded)."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter_patches(patches)
        nodes, rpc, snode_api = self._setup_common(m)

        # First peer reachable for leader detection: it returns "lvs leadership: True"
        # (sec=.206 is current leader). Tertiary block call (target=ea5eb8ef) raises.
        fw_calls = []

        def make_fw(node, **kwargs):
            fw = MagicMock()
            peer_id = getattr(node, "uuid", None)

            def _set_port(port, ptype, action, rpc_port):
                fw_calls.append((peer_id, port, action, rpc_port))
                if peer_id == "ea5eb8ef" and action == "block":
                    raise Exception("simulated tertiary firewall timeout")
                return True

            fw.firewall_set_port.side_effect = _set_port
            return fw

        m["fw_cls"].side_effect = make_fw

        snode = nodes["snode"]
        with self.assertRaises(Exception) as ctx:
            storage_node_ops.recreate_lvstore(snode)

        self.assertIn("non-leader peer", str(ctx.exception).lower())

        # SPDK on snode killed
        self.assertGreaterEqual(snode_api.spdk_process_kill.call_count, 1)

        # The leader (which was successfully blocked first) must have an "allow"
        # call on the same port — that's _abort_restart_and_unblock unwinding it.
        leader_actions = [c for c in fw_calls if c[0] == "951ffc7a"]
        self.assertTrue(any(c[2] == "block" for c in leader_actions),
                        f"Leader was never blocked: {leader_actions}")
        self.assertTrue(any(c[2] == "allow" for c in leader_actions),
                        f"Leader was blocked but never unblocked on abort: {leader_actions}")

        # snode NOT promoted
        promote_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if (c.kwargs.get("leader") is True or
                (len(c.args) > 1 and c.args[1] is True))
        ]
        self.assertEqual(promote_calls, [])

    # ----------------------------------------------------------------------
    # Leader-detection RPC raises → abort (early, before _create_bdev_stack)
    # ----------------------------------------------------------------------
    def test_leader_detection_rpc_failure_aborts_before_bdev_stack(self):
        """get_lvstores on the only peer raises (mgmt slow). Must abort
        without creating the bdev stack on snode."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter_patches(patches)
        nodes, rpc, snode_api = self._setup_common(m)

        # rpc returned by RPCClient(sec_node...) raises on get_lvstores
        rpc.bdev_lvol_get_lvstores.side_effect = Exception("simulated peer mgmt timeout")

        snode = nodes["snode"]
        with self.assertRaises(Exception) as ctx:
            storage_node_ops.recreate_lvstore(snode)

        self.assertIn("leader detection", str(ctx.exception).lower())
        # Abort happens BEFORE _create_bdev_stack
        self.assertEqual(m["create_bdev"].call_count, 0,
                         "bdev stack created despite early abort")

    # ----------------------------------------------------------------------
    # Replication-wait raises → abort
    # ----------------------------------------------------------------------
    def test_replication_wait_failure_aborts(self):
        """wait_for_jm_rep_tasks_to_finish raises on leader. Must abort
        instead of silently dropping current_leader."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter_patches(patches)
        nodes, rpc, snode_api = self._setup_common(m)

        nodes["sec"].wait_for_jm_rep_tasks_to_finish = MagicMock(
            side_effect=Exception("simulated rep wait RPC timeout"))

        snode = nodes["snode"]
        with self.assertRaises(Exception) as ctx:
            storage_node_ops.recreate_lvstore(snode)

        self.assertIn("replication-wait", str(ctx.exception).lower())

        # snode NOT promoted
        promote_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if (c.kwargs.get("leader") is True or
                (len(c.args) > 1 and c.args[1] is True))
        ]
        self.assertEqual(promote_calls, [])

    # ----------------------------------------------------------------------
    # jc_compression check raises → abort
    # ----------------------------------------------------------------------
    def test_jc_compression_check_failure_aborts(self):
        """jc_compression_get_status on leader raises. Must abort."""
        from simplyblock_core import storage_node_ops

        patches = self._patches()
        m = self._enter_patches(patches)
        nodes, rpc, snode_api = self._setup_common(m)

        # snode.rpc_client() is reused for both snode and peers in our
        # _attach_node_helpers wiring; route the failure via the leader's
        # bound rpc_client mock instead. Mirror the leader-detection
        # response on leader_rpc so sec is still selected as leader.
        leader_rpc = MagicMock()
        leader_rpc.bdev_lvol_get_lvstores.return_value = [{
            "lvs leadership": True,
            "lvs_primary": False,
            "uuid": "lvs-uuid",
        }]
        leader_rpc.jc_compression_get_status.side_effect = Exception(
            "simulated jc_compression timeout")
        nodes["sec"].rpc_client = MagicMock(return_value=leader_rpc)

        snode = nodes["snode"]
        with self.assertRaises(Exception) as ctx:
            storage_node_ops.recreate_lvstore(snode)

        self.assertIn("jc_compression", str(ctx.exception).lower())

        # _create_bdev_stack should not have run yet (this check is pre-block)
        self.assertEqual(m["create_bdev"].call_count, 0)


if __name__ == "__main__":
    unittest.main()
