# coding=utf-8
"""
test_port_allow_recovery.py — unit tests for
``tasks_runner_port_allow.exec_port_allow_task``.

Invariants covered:

  1. **Cluster map sent before unblock.** Before the recovering node's
     port is unblocked, the full cluster map must be pushed via
     ``distr_controller.send_cluster_map_to_node`` so every distrib on
     the recovering node has fresh per-device state. Otherwise the
     first IO through the unblocked port hits
     ``status_device=48 / is_device_available_read=0`` and raises a
     DISTRIBD "Unable to read stripe" error.

  2. **No leadership manipulation.** The runner must not block any
     peer's port, must not call ``bdev_lvol_set_leader`` /
     ``bdev_distrib_force_to_non_leader`` on a peer, and must not call
     ``bdev_lvol_set_lvs_opts`` to "take leadership locally" on the
     recovering node. Leadership belongs to the JM heartbeat /
     writer-conflict resolution; ``port_allow`` only allows the port.

     Background: an earlier implementation did force-failback —
     when ``_get_lvs_leader`` returned a peer (typical after a
     failover), the runner would block the peer's port, demote the
     peer, take leadership locally on the recovering node, and
     additionally walk every secondary and block + demote them too.
     That was wrong: a writer conflict only ever blocks the *primary*,
     and once a failover succeeded the peer is the legitimate new
     leader — blocking it cuts client IO and opens a fresh writer
     conflict. See incident 2026-05-02 (k8s_native_failover_ha-
     20260502-101452): worker5's port_allow at 15:51:44.818 blocked
     worker1 (the new primary), producing client IO errors.

  3. **Only the recovering node's port is firewall-allowed.** Exactly
     one ``firewall_set_port(..., "allow", ...)`` call, on the
     recovering node, on the requested port number.
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
    runs start-to-finish against in-memory mocks and records every
    ordered call into ``self.calls``."""

    def setUp(self):
        # Recovering node (was DOWN, now coming back)
        self.node = _make_node("node-a", "10.0.0.10")
        # A peer that took leadership during the outage / conflict.
        self.sec = _make_node("node-b", "10.0.0.11")
        self.node.secondary_node_id = self.sec.uuid
        self.node.tertiary_node_id = None
        self.node.lvstore_ports = {self.node.lvstore: {"lvol_port": 4430}}
        self.port = 4430
        self.task = _make_task(self.node.uuid, self.port)

        self.node_rpc = MagicMock(name="node_rpc")
        self.sec_rpc = MagicMock(name="sec_rpc")
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        self.sec_rpc.jc_compression_get_status.return_value = False

        self.node.rpc_client = MagicMock(return_value=self.node_rpc)
        self.sec.rpc_client = MagicMock(return_value=self.sec_rpc)
        self.sec.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        self.node.get_lvol_subsys_port = MagicMock(return_value=self.port)
        self.sec.get_lvol_subsys_port = MagicMock(return_value=self.port)

        self.calls = []

        # Record any RPC that would represent leadership manipulation.
        for rpc in (self.node_rpc, self.sec_rpc):
            owner = "node" if rpc is self.node_rpc else "sec"
            for method in ("bdev_lvol_set_leader",
                           "bdev_distrib_force_to_non_leader",
                           "bdev_lvol_set_lvs_opts"):
                def _make_side(name=method, who=owner):
                    def _side(*a, **kw):
                        self.calls.append((name, who))
                        return True
                    return _side
                getattr(rpc, method).side_effect = _make_side()

        # FirewallClient factory + spy
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
    """Cluster map must reach the recovering node before the firewall
    allow, so distribs see fresh per-device state on the first IO."""

    def test_cluster_map_sent_before_port_allow(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
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
            "firewall allow; otherwise distribs have stale remote-device state.",
        )

    def test_cluster_map_failure_suspends_task(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        # Stop the side-effect-recording patch and re-patch with a failing
        # send_cluster_map_to_node.
        self._patches[4].stop()  # the send_cluster_map patch
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.distr_controller.send_cluster_map_to_node",
            return_value=False,
        ):
            exec_port_allow_task(self.task)

        # Task suspended, no firewall allow on the node.
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allow = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allow, [],
                         "no firewall allow may fire when cluster map push failed")


class TestNoLeadershipManipulation(_BasePortAllowTest):
    """Regression for incident 2026-05-02: port_allow must not touch
    leadership at all (no peer demote, no local take-leadership, no
    secondary block)."""

    def test_no_peer_port_block(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        peer_blocks = [
            c for c in self.calls
            if c[0] == "firewall_set_port"
            and c[1] == self.sec.uuid
            and c[3] == "block"
        ]
        self.assertEqual(
            peer_blocks, [],
            "port_allow must not block any peer's port — once a failover "
            "succeeded the peer is the legitimate new leader and blocking "
            "it cuts client IO (incident 2026-05-02)",
        )

    def test_no_peer_demote(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        peer_demote_calls = [
            c for c in self.calls
            if c[1] == "sec" and c[0] in (
                "bdev_lvol_set_leader",
                "bdev_distrib_force_to_non_leader",
            )
        ]
        self.assertEqual(
            peer_demote_calls, [],
            "port_allow must not call bdev_lvol_set_leader or "
            "bdev_distrib_force_to_non_leader on a peer",
        )

    def test_no_local_take_leadership(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        local_leader_calls = [
            c for c in self.calls
            if c[1] == "node" and c[0] in (
                "bdev_lvol_set_leader",
                "bdev_lvol_set_lvs_opts",
            )
        ]
        self.assertEqual(
            local_leader_calls, [],
            "port_allow must not 'take leadership locally' on the recovering "
            "node — leadership belongs to the JM heartbeat, not this task",
        )


class TestOnlyNodePortAllowed(_BasePortAllowTest):
    """The runner must firewall-allow exactly one port on exactly one node:
    the requested port on the recovering node."""

    def test_exactly_one_firewall_allow_on_recovering_node(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        allow_calls = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[3] == "allow"
        ]
        self.assertEqual(
            len(allow_calls), 1,
            f"expected exactly 1 firewall_set_port allow; got {allow_calls}",
        )
        self.assertEqual(allow_calls[0][1], self.node.uuid)
        self.assertEqual(allow_calls[0][2], self.port)

    def test_no_block_calls_at_all(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        block_calls = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[3] == "block"
        ]
        self.assertEqual(
            block_calls, [],
            "port_allow must not block any port — only allow on the "
            "recovering node",
        )


class TestSourceShape(unittest.TestCase):
    """Source-level guards that the removed code paths cannot be silently
    reintroduced."""

    @classmethod
    def setUpClass(cls):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "services", "tasks_runner_port_allow.py",
        )
        with open(path, "r") as f:
            cls.src = f.read()

    def test_no_get_lvs_leader_helper(self):
        # The helper that resolved current_leader is no longer needed and
        # was deleted along with the leadership-manipulation block.
        self.assertNotIn("def _get_lvs_leader", self.src)

    def test_no_peer_leader_demote_branch(self):
        self.assertNotIn("Demoting before port_allow", self.src)
        self.assertNotIn("current_leader.get_id() != node.get_id()", self.src)
        self.assertNotIn("bdev_distrib_force_to_non_leader", self.src)
        self.assertNotIn("bs_nonleadership=True", self.src)

    def test_no_unconditional_secondary_loop(self):
        # The loop iterated `for sid in sec_ids` and called
        # firewall_set_port(..., "block", ...) on every secondary. That
        # entire pattern must be gone.
        # Multiple `for sid in sec_ids` loops still exist for legitimate
        # purposes (hublvol checks, JC compression), but none of them may
        # contain a firewall block call.
        import re
        for m in re.finditer(r"for sid in [\w_]+", self.src):
            window = self.src[m.start():m.start() + 1500]
            self.assertNotIn(
                'firewall_set_port(port_number, sn_port_type, "block"',
                window,
                "no firewall block may appear inside any sec_ids loop",
            )

    def test_rationale_documented_in_source(self):
        self.assertIn("incident 2026-05-02", self.src)
        self.assertIn("port_allow's correct scope", self.src)


if __name__ == "__main__":
    unittest.main()
