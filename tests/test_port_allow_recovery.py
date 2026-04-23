# coding=utf-8
"""
test_port_allow_recovery.py — unit tests for network-outage recovery via
``tasks_runner_port_allow.exec_port_allow_task``.

Two invariants are covered:

  1. Before the recovering node's port is unblocked, the full cluster map
     must be pushed via ``distr_controller.send_cluster_map_to_node`` so
     that every distrib on the recovering node has fresh per-device state
     (not only the node's own devices). Otherwise the first IO through
     the unblocked port hits ``status_device=48 / is_device_available_read=0``
     and raises a DISTRIBD "Unable to read stripe" error.

  2. When the current LVS leader is a *peer* (typical outage-recovery
     case — a secondary/tertiary took over while this node was out), the
     peer must be demoted (port blocked, ``bdev_lvol_set_leader(leader=False)``
     + ``bdev_distrib_force_to_non_leader``, inflight drain) before the
     recovering node's port is unblocked. Previously the task logged
     "skipping peer demotion" and unblocked directly, which allowed
     dual-leader IO and writer conflicts.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode


def _make_node(uuid, mgmt_ip, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = uuid
    n.mgmt_ip = mgmt_ip
    n.rpc_port = 8080
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.status = status
    n.cluster_id = "c1"
    n.active_rdma = False
    n.nvme_devices = []
    n.remote_devices = []
    n.lvstore = "LVS_TEST"
    n.jm_vuid = 1234
    n.lvstore_status = "ready"
    n.api_endpoint = f"{mgmt_ip}:5000"
    # Stub FDB writes so MagicMock fields never get JSON-serialized.
    n.write_to_db = MagicMock(return_value=True)
    return n


def _make_task(node_id, port_number):
    t = JobSchedule()
    t.uuid = "task-1"
    t.cluster_id = "c1"
    t.node_id = node_id
    t.status = JobSchedule.STATUS_NEW
    t.function_name = JobSchedule.FN_PORT_ALLOW
    t.function_params = {"port_number": port_number}
    t.canceled = False
    return t


class _BasePortAllowTest(unittest.TestCase):
    """Shared plumbing: patches every boundary so exec_port_allow_task
    runs start-to-finish against in-memory mocks."""

    def setUp(self):
        # Recovering node (was out, now back)
        self.node = _make_node("node-a", "10.0.0.10")
        # Its secondary — the one that took leadership during the outage
        self.sec = _make_node("node-b", "10.0.0.11")
        self.node.secondary_node_id = self.sec.uuid
        self.node.tertiary_node_id = None
        self.node.lvstore_ports = {self.node.lvstore: {"lvol_port": 4430}}
        # Task asks us to unblock the primary LVS port
        self.port = 4430
        self.task = _make_task(self.node.uuid, self.port)

        # One RPC client mock per node — both exposed via node.rpc_client()
        # and via the explicit RPCClient constructor used for peer demote.
        self.node_rpc = MagicMock(name="node_rpc")
        self.node_rpc.bdev_lvol_set_lvs_opts.return_value = True
        self.node_rpc.bdev_lvol_set_leader.return_value = True
        self.sec_rpc = MagicMock(name="sec_rpc")
        self.sec_rpc.bdev_lvol_set_leader.return_value = True
        self.sec_rpc.bdev_distrib_force_to_non_leader.return_value = True
        self.sec_rpc.bdev_distrib_check_inflight_io.return_value = False
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self.sec_rpc.jc_compression_get_status.return_value = False

        self.node.rpc_client = MagicMock(return_value=self.node_rpc)
        self.sec.rpc_client = MagicMock(return_value=self.sec_rpc)
        self.sec.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        # get_lvol_subsys_port(lvstore) used for both node and sec
        self.node.get_lvol_subsys_port = MagicMock(return_value=self.port)
        self.sec.get_lvol_subsys_port = MagicMock(return_value=self.port)

        # Call log — anything the task does touches one of these; the test
        # inspects ordering + presence of specific calls.
        self.calls = []

        # Record call ordering via side_effect on each method we care about,
        # preserving whatever return_value was configured. Tests that need a
        # method to raise simply overwrite side_effect on that method to a
        # function that records the call and then raises.
        def _record(method_name, owner_label, return_value):
            def _side(*a, **kw):
                self.calls.append((method_name, owner_label))
                return return_value
            return _side

        for m, rv in [("bdev_lvol_set_leader", True),
                      ("bdev_distrib_force_to_non_leader", True),
                      ("bdev_distrib_check_inflight_io", False),
                      ("bdev_lvol_set_lvs_opts", True)]:
            getattr(self.sec_rpc, m).side_effect = _record(m, "sec", rv)
            getattr(self.node_rpc, m).side_effect = _record(m, "node", rv)
        # Expose the recorder helper so individual tests can install raising
        # side_effects that still log the attempted call.
        self._record = _record

        # FirewallClient instances we return from the patched factory
        fw_node = MagicMock(name="fw_node")
        fw_sec = MagicMock(name="fw_sec")

        def _firewall_side_effect(target, *a, **kw):
            fw = fw_node if target is self.node else fw_sec
            target_uuid = target.uuid

            def _spy(port, ptype, action, *a, **kw):
                self.calls.append(("firewall_set_port", target_uuid, port, action))
                return True
            fw.firewall_set_port = _spy
            return fw

        self.fw_node = fw_node
        self.fw_sec = fw_sec

        # RPCClient(...) constructor is called explicitly for peer demote
        # — always return self.sec_rpc for the peer's ip, otherwise a fresh
        # MagicMock.
        def _rpc_ctor(ip, port, *a, **kw):
            if ip == self.sec.mgmt_ip:
                return self.sec_rpc
            return self.node_rpc

        # Shared patches applied to every test
        self._patches = [
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.db",
                MagicMock(
                    get_task_by_id=MagicMock(return_value=self.task),
                    get_storage_node_by_id=MagicMock(side_effect=self._get_node),
                    kv_store=MagicMock(),
                ),
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.health_controller._check_node_ping",
                return_value=True,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.storage_node_ops._connect_to_remote_devs",
                return_value=[MagicMock()],
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.storage_node_ops._connect_to_remote_jm_devs",
                return_value=[MagicMock(), MagicMock()],
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.distr_controller.send_cluster_map_to_node",
                side_effect=self._send_cluster_map,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.distr_controller.send_dev_status_event",
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.health_controller._check_node_lvstore",
                return_value=True,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.health_controller._check_node_hublvol",
                return_value=True,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.health_controller._check_sec_node_hublvol",
                return_value=True,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.tasks_controller.get_lvol_sync_del_task",
                return_value=None,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.FirewallClient",
                side_effect=_firewall_side_effect,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.RPCClient",
                side_effect=_rpc_ctor,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.tcp_ports_events.port_deny",
                side_effect=lambda n, p: self.calls.append(("port_deny", n.uuid, p)),
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.tcp_ports_events.port_allowed",
                side_effect=lambda n, p: self.calls.append(("port_allowed", n.uuid, p)),
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.time.sleep",
                return_value=None,
            ),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def _get_node(self, uuid):
        if uuid == self.node.uuid:
            return self.node
        if uuid == self.sec.uuid:
            return self.sec
        raise KeyError(uuid)

    def _send_cluster_map(self, n):
        self.calls.append(("send_cluster_map_to_node", n.uuid))
        return True


class TestClusterMapBeforeUnblock(_BasePortAllowTest):
    """Bug #2: full cluster map must be pushed to the recovering node
    before the firewall allow is issued."""

    def test_cluster_map_sent_before_port_allow(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        # No-leader on first look; then the local-restore takes leadership
        # and _get_lvs_leader returns self.node on the confirmation call.
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._get_lvs_leader",
            side_effect=[None, self.node],
        ):
            exec_port_allow_task(self.task)

        map_send_idx = next(
            i for i, c in enumerate(self.calls) if c[0] == "send_cluster_map_to_node"
        )
        node_allow_idx = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        )
        self.assertLess(
            map_send_idx, node_allow_idx,
            "send_cluster_map_to_node must fire BEFORE the recovering node's "
            "firewall allow; otherwise the distribs have stale remote-device "
            "state and DISTRIBD raises 'Unable to read stripe'."
        )

    def test_cluster_map_failure_suspends_task(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        # Make send_cluster_map_to_node fail; task must suspend before any
        # firewall allow is issued.
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.distr_controller.send_cluster_map_to_node",
            return_value=False,
        ):
            exec_port_allow_task(self.task)

        allow_calls = [c for c in self.calls
                       if c[0] == "firewall_set_port" and c[3] == "allow"]
        self.assertEqual(allow_calls, [],
                         "No firewall allow should be issued if the cluster map send failed.")
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)


class TestPeerLeaderDemoted(_BasePortAllowTest):
    """Bug #1: if the current leader is a peer (outage-recovery case),
    demote it (with port block, set_leader=False, drain) before unblocking
    the recovering node's port. The old code skipped this entirely."""

    def test_peer_leader_is_demoted_before_node_port_unblock(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        # Peer is the current leader.
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._get_lvs_leader",
            side_effect=[self.sec, self.node],
        ):
            exec_port_allow_task(self.task)

        # A) peer port was blocked before any demote RPC to the peer
        peer_block_idx = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "block"
        )
        sec_demote_idx = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
        )
        self.assertLess(peer_block_idx, sec_demote_idx,
                        "peer port must be blocked before demote RPC is issued")

        # B) demote + inflight drain happened on the peer
        self.assertTrue(
            any(c[0] == "bdev_distrib_force_to_non_leader" and c[1] == "sec"
                for c in self.calls),
            "expected bdev_distrib_force_to_non_leader on the peer leader",
        )
        self.assertTrue(
            any(c[0] == "bdev_distrib_check_inflight_io" and c[1] == "sec"
                for c in self.calls),
            "expected inflight IO drain on the peer leader",
        )

        # C) leadership taken locally AFTER the peer was demoted
        local_take_idx = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "bdev_lvol_set_leader" and c[1] == "node"
        )
        self.assertLess(sec_demote_idx, local_take_idx,
                        "local take-leadership must follow the peer demote")

        # D) node port only unblocked AFTER the peer was demoted
        node_allow_idx = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        )
        self.assertLess(sec_demote_idx, node_allow_idx,
                        "recovering node port must not be unblocked before the "
                        "peer leader has been demoted")

        # E) peer port eventually re-allowed at the end
        peer_allow_calls = [c for c in self.calls
                            if c[0] == "firewall_set_port"
                            and c[1] == self.sec.uuid and c[3] == "allow"]
        self.assertTrue(peer_allow_calls,
                        "peer port must be re-allowed after its demote")

        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_peer_demote_failure_unblocks_peer_and_suspends_task(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        # Install a raising side_effect that still logs the attempt.
        def _raise(*a, **kw):
            self.calls.append(("bdev_lvol_set_leader", "sec"))
            raise RuntimeError("rpc fail")
        self.sec_rpc.bdev_lvol_set_leader.side_effect = _raise

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._get_lvs_leader",
            return_value=self.sec,
        ):
            exec_port_allow_task(self.task)

        # Peer port was blocked and then re-allowed (rollback) — not left
        # blocked.
        peer_evts = [c for c in self.calls
                     if c[0] == "firewall_set_port" and c[1] == self.sec.uuid]
        actions = [c[3] for c in peer_evts]
        self.assertIn("block", actions)
        self.assertIn("allow", actions)
        # Recovering node port was NOT unblocked.
        self.assertFalse(
            any(c[0] == "firewall_set_port" and c[1] == self.node.uuid
                and c[3] == "allow" for c in self.calls),
            "node port must not be unblocked if peer demote fails",
        )
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)


if __name__ == "__main__":
    unittest.main()
