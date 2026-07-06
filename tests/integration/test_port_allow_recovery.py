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

  2. **Fenced leadership failback.** When a peer holds LVS leadership at
     port_allow time (it became acting leader during the outage), the
     runner must fail leadership back to the recovering primary BEFORE
     the node's port is unblocked: block the peer's port, drain
     in-flight IO (bounded), demote the peer
     (``bdev_lvol_set_leader(leader=False, bs_nonleadership=True)`` +
     ``bdev_distrib_force_to_non_leader``), take leadership on the
     primary, re-commit the peer's follower role, and only then unblock
     the peer's port and allow the node's port.

     Background: an unfenced force-failback here caused incident
     2026-05-02 (k8s_native_failover_ha-20260502-101452) — it demoted
     the legitimately-elected new leader with unverified hublvols and
     no drain, cutting client IO. It was removed; but leaving
     leadership on the peer turned out equally wrong: nothing on the
     no-restart recovery path reconciles the ex-leader afterwards, its
     redirect stays broken and the monitor flips it DOWN. The failback
     is therefore reinstated WITH fencing: it runs only after the peer
     hublvol gate has proven every online follower can redirect.

  3. **Own follower-side hublvols wired before unblock.** The recovering
     node's hublvol connections for lvstores it serves as secondary /
     tertiary (reverse refs ``lvstore_stack_secondary`` /
     ``lvstore_stack_tertiary``) — outbound to the primary, plus the
     inbound secondary-hublvol exposure the tertiary multipaths to —
     must be (re)established before the port opens; the periodic health
     loop repaired them only after.

  4. **The recovering node's port is firewall-allowed exactly once**, on
     the requested port number, as the last step.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


def _make_device(uuid, status, io_error=False, retries_exhausted=False, order=0):
    d = NVMeDevice()
    d.uuid = uuid
    d.status = status
    d.io_error = io_error
    d.retries_exhausted = retries_exhausted
    d.cluster_device_order = order
    return d


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
        # Default scenario: the secondary became acting leader during the
        # outage; the recovering primary reads back leadership after the
        # failback's take (poll on bdev_lvol_get_lvstores).
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        self.sec_rpc.jc_compression_get_status.return_value = False
        self.sec_rpc.jc_disable_replication.return_value = True
        self.sec_rpc.bdev_distrib_check_inflight_io.return_value = False
        self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]

        self.node.rpc_client = MagicMock(return_value=self.node_rpc)
        self.sec.rpc_client = MagicMock(return_value=self.sec_rpc)
        self.sec.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        # Follower re-commit after the demote drives connect_to_hublvol on
        # the ex-leader (set_lvs_opts + connect_hublvol re-commit).
        self.sec.connect_to_hublvol = MagicMock(return_value=True)

        self.node.get_lvol_subsys_port = MagicMock(return_value=self.port)
        self.sec.get_lvol_subsys_port = MagicMock(return_value=self.port)

        self.calls = []

        # Record any RPC that represents leadership manipulation, with its
        # kwargs so tests can distinguish demote from promote.
        for rpc in (self.node_rpc, self.sec_rpc):
            owner = "node" if rpc is self.node_rpc else "sec"
            for method in ("bdev_lvol_set_leader",
                           "bdev_distrib_force_to_non_leader",
                           "bdev_lvol_set_lvs_opts"):
                def _make_side(name=method, who=owner):
                    def _side(*a, **kw):
                        self.calls.append((name, who, kw))
                        return True
                    return _side
                getattr(rpc, method).side_effect = _make_side()

        # port_block.set_port spy. Port (un)blocking moved off the
        # directly-imported FirewallClient onto port_block.set_port(node,
        # port, block=...). Record each call in the legacy
        # ("firewall_set_port", target_uuid, port, action) shape so the
        # assertions below keep working unchanged.
        def _set_port_side_effect(node, port, block, *a, **kw):
            action = "block" if block else "allow"
            self.calls.append(("firewall_set_port", node.uuid, port, action))
            return True

        self.db_mock = MagicMock(
            get_task_by_id=MagicMock(return_value=self.task),
            get_storage_node_by_id=MagicMock(side_effect=self._get_node),
            get_lvols_by_node_id=MagicMock(return_value=[]),
            kv_store=MagicMock(),
        )

        self._patches = [
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.db",
                self.db_mock,
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
                "simplyblock_core.services.tasks_runner_port_allow.tasks_controller.get_active_node_restart_task",
                return_value=None,
            ),
            patch(
                "simplyblock_core.port_block.set_port",
                side_effect=_set_port_side_effect,
            ),
            patch(
                "simplyblock_core.port_block.is_port_blocked",
                return_value=False,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.storage_node_ops._failback_primary_ana",
                side_effect=lambda n: self.calls.append(("ana_failback", n.uuid)),
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.storage_node_ops._set_lvol_ana_on_node",
                side_effect=lambda lvol, n, ana: self.calls.append(
                    ("ana_set", lvol.nqn, n.uuid, ana)),
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


class TestDataNicGate(_BasePortAllowTest):
    """Mgmt reachability alone must not drive recovery: the node itself
    must positively confirm at least one data NIC via SnodeAPI ping_ip
    (partial partitions often restore the mgmt plane first, and a dark
    data NIC would otherwise surface as peer-gate exhaustion -> node
    abort). False AND inconclusive (None) both suspend — recovery needs
    positive confirmation, unlike the monitor's DOWN-flip which ignores
    inconclusive results."""

    def setUp(self):
        super().setUp()
        nic = MagicMock(name="data_nic")
        nic.ip4_address = "10.10.0.10"
        nic.if_name = "eth1"
        self.node.data_nics = [nic]

    def _run_with_data_ping(self, result):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "health_controller._check_ping_from_node",
            return_value=result,
        ):
            exec_port_allow_task(self.task)

    def _node_allows(self):
        return [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]

    def test_data_nic_down_suspends(self):
        self._run_with_data_ping(False)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(self._node_allows(), [],
                         "the port must not open while the node's data NIC is down")

    def test_data_nic_inconclusive_suspends(self):
        self._run_with_data_ping(None)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(self._node_allows(), [],
                         "an inconclusive data-NIC ping must not allow the port — "
                         "recovery requires positive confirmation")

    def test_data_nic_up_proceeds(self):
        self._run_with_data_ping(True)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(len(self._node_allows()), 1)


class TestLeadershipFailback(_BasePortAllowTest):
    """Fenced leadership failback (reinstated 2026-07-06): when a peer is
    acting leader at port_allow time, demote it and hand leadership back
    to the recovering primary BEFORE the node's port opens — with the
    port-block window, bounded drain, follower re-commit, and
    zero-leader protection that the pre-2026-05-02 version lacked."""

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def _idx(self, pred):
        return next(i for i, c in enumerate(self.calls) if pred(c))

    def test_peer_leader_demoted_and_primary_takes_over(self):
        self._run()
        sec_demotes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
            and c[2].get("leader") is False and c[2].get("bs_nonleadership") is True
        ]
        self.assertEqual(len(sec_demotes), 1, f"expected one peer demote; calls={self.calls}")
        force_calls = [
            c for c in self.calls
            if c[0] == "bdev_distrib_force_to_non_leader" and c[1] == "sec"
        ]
        self.assertEqual(len(force_calls), 1)
        node_takes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "node"
            and c[2].get("leader") is True
        ]
        self.assertEqual(len(node_takes), 1, "recovering primary must take leadership back")

    def test_failback_ordering_block_drain_demote_recommit_unblock_allow(self):
        self._run()
        i_block_sec = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "block")
        i_demote = self._idx(
            lambda c: c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
            and c[2].get("leader") is False)
        i_take = self._idx(
            lambda c: c[0] == "bdev_lvol_set_leader" and c[1] == "node"
            and c[2].get("leader") is True)
        i_unblock_sec = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "allow")
        i_allow_node = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow")
        self.assertLess(i_block_sec, i_demote,
                        "peer port must be blocked before the demote")
        self.assertLess(i_demote, i_take, "demote precedes the primary's take")
        self.assertLess(i_take, i_unblock_sec,
                        "the peer's port reopens only after leadership moved "
                        "and the follower role was re-committed")
        self.assertLess(i_unblock_sec, i_allow_node,
                        "the recovering node's port opens last")
        # Follower re-commit happened before the peer port reopened.
        self.sec.connect_to_hublvol.assert_called()

    def test_ana_failback_fired_for_secondary_only(self):
        self._run()
        self.assertIn(("ana_failback", self.node.uuid), self.calls,
                      "successful failback must reconcile the secondary's ANA "
                      "listeners back to non_optimized")

    def test_no_failback_when_no_peer_leader(self):
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        leadership_calls = [
            c for c in self.calls
            if c[0] in ("bdev_lvol_set_leader", "bdev_distrib_force_to_non_leader")
        ]
        self.assertEqual(leadership_calls, [],
                         "no demote and no take when the primary already leads")
        sec_blocks = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "block"
        ]
        self.assertEqual(sec_blocks, [], "no peer port-block without a failback")
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(len(node_allows), 1)

    def test_zero_leader_is_healed_by_taking_leadership(self):
        # Nobody holds leadership (no-abort outage aftermath): the primary
        # must take it before the port opens.
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]

        # Once set_leader(leader=True) fires on the node, leadership reads True.
        def _set_leader_side(*a, **kw):
            self.calls.append(("bdev_lvol_set_leader", "node", kw))
            self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
            return True
        self.node_rpc.bdev_lvol_set_leader.side_effect = _set_leader_side

        self._run()
        node_takes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "node" and c[2].get("leader") is True
        ]
        self.assertEqual(len(node_takes), 1,
                         "zero-leader LVS must be healed by the primary taking leadership")
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(len(node_allows), 1)

    def test_drain_timeout_suspends_and_releases_peer(self):
        self.sec_rpc.bdev_distrib_check_inflight_io.return_value = True
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._FAILBACK_DRAIN_BOUND_SEC",
            0.05,
        ):
            self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        demotes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
        ]
        self.assertEqual(demotes, [],
                         "no demote may fire against a non-drained distrib pipeline")
        # The peer's port must have been released again.
        i_block = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "block")
        i_release = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.sec.uuid and c[3] == "allow")
        self.assertLess(i_block, i_release)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [], "node port must stay blocked on drain failure")

    def test_failed_take_repromotes_old_leader(self):
        # The primary never reads back leadership after take.
        self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        # Old leader was demoted, then re-promoted (leader=True on sec).
        i_demote = self._idx(
            lambda c: c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
            and c[2].get("leader") is False)
        i_repromote = self._idx(
            lambda c: c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
            and c[2].get("leader") is True)
        self.assertLess(i_demote, i_repromote,
                        "a failed take must re-promote the old leader — the LVS "
                        "may never be left with zero leaders")
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [])


class TestOnlyNodePortAllowed(_BasePortAllowTest):
    """The recovering node's own port is allowed exactly once, as the last
    step; ports may only ever be blocked on peers (the failback window),
    never on the recovering node, and every peer block is paired with a
    release."""

    def test_exactly_one_allow_on_recovering_node_and_it_is_last(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(
            len(node_allows), 1,
            f"expected exactly 1 allow on the recovering node; got {node_allows}",
        )
        self.assertEqual(node_allows[0][2], self.port)
        fw_calls = [c for c in self.calls if c[0] == "firewall_set_port"]
        self.assertEqual(fw_calls[-1][1], self.node.uuid)
        self.assertEqual(fw_calls[-1][3], "allow")

    def test_recovering_node_never_blocked_and_peer_blocks_released(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        node_blocks = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "block"
        ]
        self.assertEqual(node_blocks, [],
                         "the recovering node's own port must never be blocked here")
        for c in [c for c in self.calls
                  if c[0] == "firewall_set_port" and c[3] == "block"]:
            releases = [
                r for r in self.calls
                if r[0] == "firewall_set_port" and r[1] == c[1]
                and r[2] == c[2] and r[3] == "allow"
            ]
            self.assertTrue(
                releases,
                f"peer block {c} has no matching release — a failback must "
                f"never leave a peer port permanently blocked on success")


class TestSourceShape(unittest.TestCase):
    """Source-level guards that the removed code paths cannot be silently
    reintroduced."""

    @classmethod
    def setUpClass(cls):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "..",
            "simplyblock_core", "services", "tasks_runner_port_allow.py",
        )
        with open(path, "r") as f:
            cls.src = f.read()

    def test_no_get_lvs_leader_helper(self):
        # The old pre-2026-05-02 helper stays gone; leader detection lives
        # inline in exec_port_allow_task with explicit failure handling.
        self.assertNotIn("def _get_lvs_leader", self.src)

    def test_fenced_failback_present_and_ordered(self):
        # The failback is reinstated WITH fencing: helper present, demote
        # semantics present, and — in exec — the failback call sits after
        # the peer hublvol gate and before the node's port unblock.
        self.assertIn("def _failback_leadership_to_primary", self.src)
        self.assertIn("bs_nonleadership=True", self.src)
        self.assertIn("bdev_distrib_force_to_non_leader", self.src)
        i_gate = self.src.find("_verify_or_reconnect_peer_hublvol(sec_node, node)")
        i_failback_call = self.src.find(
            "failback_ok, failback_msg = _failback_leadership_to_primary(")
        i_unblock = self.src.find(
            "port_block.set_port(node, port_number, block=False")
        self.assertGreater(i_gate, 0)
        self.assertGreater(i_failback_call, i_gate,
                           "failback must run only after the peer hublvol gate")
        self.assertGreater(i_unblock, i_failback_call,
                           "the node's port opens only after the failback")

    def test_zero_leader_protection_present(self):
        # A failed take must re-promote the old leader, and a zero-leader
        # LVS must be healed by the primary taking leadership.
        self.assertIn("def _take_leadership_on_primary", self.src)
        self.assertIn("re-promote", self.src)

    def test_no_unconditional_secondary_loop(self):
        # The old loop iterated `for sid in sec_ids` and called
        # firewall_set_port(..., "block", ...) on every secondary
        # unconditionally. Peer blocks now exist only inside the fenced
        # failback helper (with paired releases), never in a bare sec_ids
        # loop.
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
        self.assertIn("verified-open", self.src)


class _StrictGateBase(_BasePortAllowTest):
    """Specialised setup for the retry-then-abort strict-gate tests.

    On top of the base test plumbing this attaches a ``hublvol`` object
    to the recovering primary so the strict ``_hublvol_verified_open``
    branch becomes active. The base ``primary_node.hublvol`` is None;
    without metadata our helper falls back to delegating to the existing
    ``_check_sec_node_hublvol`` (already mocked True in the base) — which
    is the correct behavior but bypasses the strict-verify code path we
    want to cover here.
    """

    def setUp(self):
        super().setUp()
        # Synthesize a minimal HubLVol-like object: only ``bdev_name`` is
        # touched by ``_hublvol_verified_open`` / ``_reconnect_peer_hublvol_once``.
        hub = MagicMock(name="hublvol")
        hub.bdev_name = "LVS_TEST/hublvol"
        self.node.hublvol = hub
        # Keep these tests focused on the strict gate: no peer holds
        # leadership and the primary already does, so the leadership
        # failback block is a no-op (its own coverage lives in
        # TestLeadershipFailback).
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]


class TestStrictHublvolGate(_StrictGateBase):
    """Per port-allow design (2026-05-21 incident): the strict gate must
    confirm the peer's hublvol is *verified-open* before unblocking, not
    just that ``bdev_nvme_controller_list`` is non-empty."""

    def test_strict_verify_passes_unblocks(self):
        """Happy path: ``_check_sec_node_hublvol`` returns True AND the
        strict ``_hublvol_verified_open`` returns True on the first
        attempt → port allowed."""
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._hublvol_verified_open",
            return_value=True,
        ) as verify_mock, patch(
            "simplyblock_core.services.tasks_runner_port_allow._reconnect_peer_hublvol_once",
        ) as reconnect_mock:
            exec_port_allow_task(self.task)

        verify_mock.assert_called()  # strict check was applied
        reconnect_mock.assert_not_called()  # no forced reconnect needed
        allow_calls = [c for c in self.calls
                       if c[0] == "firewall_set_port" and c[3] == "allow"]
        self.assertEqual(len(allow_calls), 1)
        self.assertEqual(allow_calls[0][1], self.node.uuid)

    def test_strict_verify_failing_then_succeeding_via_reconnect_unblocks(self):
        """Strict check fails first, forced reconnect runs, second strict
        check succeeds → port allowed within the retry budget."""
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        # _hublvol_verified_open is consulted twice per attempt (pre + post-
        # reconnect); make it False, False, True, True so the first attempt
        # fails the pre-check, the post-reconnect check succeeds.
        verify_results = iter([False, True])
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._hublvol_verified_open",
            side_effect=lambda *a, **kw: next(verify_results),
        ), patch(
            "simplyblock_core.services.tasks_runner_port_allow._reconnect_peer_hublvol_once",
            return_value=True,
        ) as reconnect_mock:
            exec_port_allow_task(self.task)

        reconnect_mock.assert_called_once()
        allow_calls = [c for c in self.calls
                       if c[0] == "firewall_set_port" and c[3] == "allow"]
        self.assertEqual(
            len(allow_calls), 1,
            "port must be allowed once the strict gate confirms the peer "
            "hublvol verified-open via forced reconnect",
        )

    def test_strict_verify_exhausts_retries_aborts_recovering_node(self):
        """Strict check never succeeds → after 5 attempts the recovering
        node is aborted (SPDK kill + OFFLINE) and the port is NOT allowed."""
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._hublvol_verified_open",
            return_value=False,
        ), patch(
            "simplyblock_core.services.tasks_runner_port_allow._reconnect_peer_hublvol_once",
            return_value=False,
        ) as reconnect_mock, patch(
            "simplyblock_core.services.tasks_runner_port_allow._abort_recovering_node",
            side_effect=lambda n, r: self.calls.append(("abort_recovering_node", n.uuid, r)),
        ) as abort_mock:
            exec_port_allow_task(self.task)

        abort_mock.assert_called_once()
        self.assertEqual(abort_mock.call_args.args[0].uuid, self.node.uuid)

        # No port_allowed event must have been emitted.
        allow_events = [c for c in self.calls if c[0] == "port_allowed"]
        self.assertEqual(allow_events, [],
                         "port_allowed must NOT fire when peers fail the gate")

        # No firewall allow on the recovering node either.
        allow_calls = [c for c in self.calls
                       if c[0] == "firewall_set_port" and c[3] == "allow"]
        self.assertEqual(allow_calls, [])

        # Task ended in DONE (not SUSPENDED — the retries already ran).
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

        # 5 reconnect attempts were issued (one per retry iteration).
        self.assertEqual(reconnect_mock.call_count, 5)

    def test_no_metadata_falls_back_to_existing_check(self):
        """If ``primary_node.hublvol`` is None (no metadata to drive a strict
        verify), the helper must delegate to the existing
        ``_check_sec_node_hublvol`` (mocked True in the base setup) and not
        attempt the strict path. This is the safety net for callers that
        haven't populated hublvol metadata yet."""
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        # Strip the hublvol metadata we added in _StrictGateBase.setUp.
        self.node.hublvol = None

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._hublvol_verified_open",
        ) as verify_mock, patch(
            "simplyblock_core.services.tasks_runner_port_allow._reconnect_peer_hublvol_once",
        ) as reconnect_mock:
            exec_port_allow_task(self.task)

        verify_mock.assert_not_called()
        reconnect_mock.assert_not_called()

        allow_calls = [c for c in self.calls
                       if c[0] == "firewall_set_port" and c[3] == "allow"]
        self.assertEqual(len(allow_calls), 1)


class TestAbortRecoveringNode(_StrictGateBase):
    """The abort helper itself must kill SPDK, set OFFLINE, and emit a
    restart-failed event — same shape as the existing
    ``storage_node_ops._abort_and_unblock`` non-leader-restart abort."""

    def test_abort_kills_spdk_and_marks_offline(self):
        from simplyblock_core.services.tasks_runner_port_allow import _abort_recovering_node

        snode_api = MagicMock()
        self.node.client = MagicMock(return_value=snode_api)

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.storage_node_ops.set_node_status",
        ) as set_status_mock, patch(
            "simplyblock_core.services.tasks_runner_port_allow.storage_events.snode_restart_failed",
        ) as event_mock:
            _abort_recovering_node(self.node, "test reason")

        # SPDK must be killed
        snode_api.spdk_process_kill.assert_called_once_with(
            self.node.rpc_port, self.node.cluster_id)

        # Node must be flipped to OFFLINE with the restart-cleanup tag (the
        # tag that's whitelisted by the RESTARTING→OFFLINE FSM guard in
        # storage_node_ops.set_node_status).
        set_status_mock.assert_called_once()
        args, kwargs = set_status_mock.call_args
        self.assertEqual(args[0], self.node.get_id())
        self.assertEqual(args[1], StorageNode.STATUS_OFFLINE)
        self.assertEqual(kwargs.get("caused_by"), "restart_cleanup")

        # The restart-failed event must fire (best-effort, before the kill,
        # so monitoring sees the abort decision even if the kill RPC stalls).
        event_mock.assert_called_once_with(self.node)


class TestSourceShapeStrictGate(unittest.TestCase):
    """Source-level guards for the strict-gate change: the retry+abort
    rationale must stay in the source so a future refactor doesn't quietly
    re-introduce the stale-controller-list bug."""

    @classmethod
    def setUpClass(cls):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "..",
            "simplyblock_core", "services", "tasks_runner_port_allow.py",
        )
        with open(path, "r") as f:
            cls.src = f.read()

    def test_retry_constants_present(self):
        self.assertIn("_HUBLVOL_RETRY_DELAYS_SEC", self.src)
        self.assertIn("_HUBLVOL_MAX_ATTEMPTS", self.src)
        # 1+2+4+8+16 backoff
        self.assertIn("(1, 2, 4, 8, 16)", self.src)

    def test_strict_verify_helper_present(self):
        self.assertIn("def _hublvol_verified_open", self.src)
        self.assertIn("bdev_nvme_controller_list", self.src)
        # Two-condition strict check: enabled path AND namespace bdev.
        # The enabled-path guard is expressed as a skip-if-not-enabled
        # (`if ct.get("state") != "enabled": continue`), so match that form.
        self.assertIn('state") != "enabled"', self.src)
        self.assertIn('+ "n1"', self.src)

    def test_abort_helper_present_and_used(self):
        self.assertIn("def _abort_recovering_node", self.src)
        # Helper must be invoked from exec_port_allow_task on exhaustion.
        self.assertIn("_abort_recovering_node(node, reason)", self.src)

    def test_no_port_allowed_after_abort(self):
        # Source-level invariant: when the abort path runs the task returns
        # without falling through to firewall_set_port/port_allowed.
        # We assert that abort sets task status DONE and the function
        # returns before the firewall/port_allowed lines.
        i_abort = self.src.find("_abort_recovering_node(node, reason)")
        i_done = self.src.find("STATUS_DONE", i_abort)
        i_return = self.src.find("return", i_done)
        # port_allowed also appears earlier in the failback helpers (peer
        # port releases); the invariant here is about the occurrence in
        # exec_port_allow_task AFTER the abort path's return.
        i_allow_event = self.src.find("tcp_ports_events.port_allowed", i_abort)
        self.assertGreater(i_abort, 0)
        self.assertGreater(i_done, i_abort)
        self.assertGreater(i_return, i_done)
        self.assertGreater(i_allow_event, i_return,
                           "tcp_ports_events.port_allowed must appear after "
                           "the abort path's return so it cannot fire on the "
                           "abort code path")


class TestDeviceReadmitOnPortAllow(_BasePortAllowTest):
    """Regression: the device re-admit at the end of port_allow must NOT be
    gated on ``node.status == ONLINE``.

    port_allow runs as the last step of node recovery and completes a couple of
    seconds BEFORE the storage-node monitor flips the node's status to ONLINE.
    The original self-heal guarded on ``node.status == ONLINE``, so it was False
    every time and the re-admit was skipped -- a device the remote-IO quorum had
    force-marked UNAVAILABLE during the outage stayed down with no other recovery
    path (device_monitor auto-restart only touches io_error devices), leaving a
    phantom offline-device baseline that a later node outage turned into a cluster
    suspension.

    The re-admit must run regardless of node status and regardless of prior device
    state; REMOVED is the one terminal state we never resurrect.
    """

    def setUp(self):
        super().setUp()
        # The node is NOT yet ONLINE when port_allow runs -- this is the bug.
        self.node.status = StorageNode.STATUS_DOWN
        self.dev_unavail = _make_device(
            "dev-unavail", NVMeDevice.STATUS_UNAVAILABLE, order=0)
        self.dev_ioerr = _make_device(
            "dev-ioerr", NVMeDevice.STATUS_UNAVAILABLE, io_error=True, order=1)
        self.dev_removed = _make_device(
            "dev-removed", NVMeDevice.STATUS_REMOVED, order=2)
        self.dev_online = _make_device(
            "dev-online", NVMeDevice.STATUS_ONLINE, order=3)
        self.node.nvme_devices = [
            self.dev_unavail, self.dev_ioerr, self.dev_removed, self.dev_online]

        self.readmitted = []

        def _record(dev_id, *a, **kw):
            self.readmitted.append(dev_id)
            return True

        self._readmit_patch = patch(
            "simplyblock_core.services.tasks_runner_port_allow.device_controller.device_set_online",
            side_effect=_record,
        )
        self._readmit_patch.start()
        self.addCleanup(self._readmit_patch.stop)

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def test_unavailable_device_readmitted_though_node_not_online(self):
        self._run()
        self.assertIn(
            "dev-unavail", self.readmitted,
            "an UNAVAILABLE device must be re-admitted on port_allow even while "
            "node.status is still DOWN (port_allow precedes the ONLINE flip)")

    def test_readmit_regardless_of_prior_device_state(self):
        self._run()
        self.assertIn(
            "dev-ioerr", self.readmitted,
            "port_allow runs only on node recovery, so every non-terminal device "
            "is flipped back online regardless of io_error/retries_exhausted")

    def test_removed_device_not_resurrected(self):
        self._run()
        self.assertNotIn(
            "dev-removed", self.readmitted,
            "a deliberately REMOVED device must never be resurrected by port_allow")

    def test_online_device_not_retouched(self):
        self._run()
        self.assertNotIn(
            "dev-online", self.readmitted,
            "an already-ONLINE device needs no re-admit")

    def test_refused_readmit_is_logged_and_does_not_fail_task(self):
        # device_set_state refuses ONLINE while the node is not ONLINE and
        # returns False (it does not raise). The 2026-07-02 suspend series
        # went unnoticed for five iterations because this refusal was silent:
        # port_allow logged "Re-admitting …" and moved on. It must now warn
        # loudly and still complete the task (the monitor's node-ONLINE clear
        # owns the actual re-admit). Nested patch overrides the recording one.
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "device_controller.device_set_online",
            return_value=False,
        ):
            with self.assertLogs(level="WARNING") as logs:
                self._run()
        self.assertTrue(
            any("refused" in line for line in logs.output),
            "a refused re-admit must be logged, never silent")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class _SecRoleReconnectBase(_BasePortAllowTest):
    """Shared topology for the own-hublvol tests: the recovering node is
    ALSO the secondary of primary P, which has tertiary T."""

    def setUp(self):
        super().setUp()
        # node is ALSO the secondary of primary P, which has tertiary T.
        self.prim = _make_node("node-p", "10.0.0.12")
        self.prim.hublvol = MagicMock(name="p_hublvol")
        self.prim.hublvol.nqn = "nqn-p-hub"
        self.prim.secondary_node_id = self.node.uuid
        self.prim.tertiary_node_id = "node-t"
        self.tert = _make_node("node-t", "10.0.0.13")
        self.tert.add_hublvol_failover_path = MagicMock(return_value=True)
        self.node.lvstore_stack_secondary = self.prim.uuid
        self.node.create_secondary_hublvol = MagicMock(return_value="nqn-p-hub")
        # Focus on the reconnect phase: no leadership failback in play.
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]

        # P owns a DIFFERENT lvstore than the node's own, and leadership
        # reads are per-lvstore: the node leads its own LVS_TEST (so the
        # own-lvstore failback is a no-op) and by default does NOT claim
        # P's LVS_P; P itself reports leading LVS_P.
        self.prim.lvstore = "LVS_P"
        self.lvs_leadership_on_node = {"LVS_TEST": True, "LVS_P": False}
        self.node_rpc.bdev_lvol_get_lvstores.side_effect = (
            lambda lvs=None, *a, **kw: [
                {"lvs leadership": self.lvs_leadership_on_node.get(lvs, False)}])
        self.prim_rpc = MagicMock(name="prim_rpc")
        self.prim_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        self.prim.rpc_client = MagicMock(return_value=self.prim_rpc)

    def _get_node(self, uuid):
        if uuid == self.prim.uuid:
            return self.prim
        if uuid == self.tert.uuid:
            return self.tert
        return super()._get_node(uuid)

    def _run_with_verify(self, verify_side_effect):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._verify_or_reconnect_peer_hublvol",
            side_effect=verify_side_effect,
        ) as verify_mock:
            exec_port_allow_task(self.task)
        return verify_mock


class TestOwnSecTertHublvolReconnect(_SecRoleReconnectBase):
    """The recovering node's OWN follower-side hublvols (lvstores it serves
    as secondary/tertiary of OTHER primaries) must be wired before the
    port opens — outbound to the primary, plus (secondary role) the
    inbound shared-NQN exposure the tertiary multipaths to."""

    def test_outbound_reconnect_driven_before_port_allow(self):
        def _record(peer, primary):
            self.calls.append(("verify_hublvol", peer.uuid, primary.uuid))
            return True
        self._run_with_verify(_record)

        i_outbound = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "verify_hublvol" and c[1] == self.node.uuid
            and c[2] == self.prim.uuid)
        i_allow = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow")
        self.assertLess(
            i_outbound, i_allow,
            "the node's own hublvol to its primary must be verified/reconnected "
            "BEFORE the port opens — not left to the 30s health loop after")

    def test_inbound_secondary_hublvol_recreated_when_missing(self):
        self.node_rpc.subsystem_list.return_value = []
        self._run_with_verify(lambda *a: True)
        self.node.create_secondary_hublvol.assert_called_once()
        self.assertIs(self.node.create_secondary_hublvol.call_args.args[0], self.prim)

    def test_inbound_exposure_skipped_when_present(self):
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self._run_with_verify(lambda *a: True)
        self.node.create_secondary_hublvol.assert_not_called()

    def test_tertiary_failover_path_topped_up(self):
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self._run_with_verify(lambda *a: True)
        self.tert.add_hublvol_failover_path.assert_called_once()
        args = self.tert.add_hublvol_failover_path.call_args.args
        self.assertIs(args[0], self.prim)
        self.assertIs(args[1], self.node)

    def test_tertiary_path_failure_is_best_effort(self):
        # A failed tertiary top-up must not gate the node's recovery.
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self.tert.add_hublvol_failover_path = MagicMock(return_value=False)
        self._run_with_verify(lambda *a: True)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(len(node_allows), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_outbound_failure_suspends_task(self):
        def _fail_only_own(peer, primary):
            # Fail the node->prim direction; pass the peer-gate direction.
            return not (peer is self.node and primary is self.prim)
        self._run_with_verify(_fail_only_own)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [],
                         "the port must not open with a broken follower redirect")

    def test_offline_primary_outbound_skipped(self):
        self.prim.status = StorageNode.STATUS_OFFLINE
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        verify_mock = self._run_with_verify(lambda *a: True)
        own_calls = [
            c for c in verify_mock.call_args_list
            if c.args and c.args[0] is self.node and c.args[1] is self.prim
        ]
        self.assertEqual(own_calls, [],
                         "no OUTBOUND reconnect toward a non-ONLINE primary — "
                         "its own recovery path re-drives that leg")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class TestStaleLeaderConvergence(_SecRoleReconnectBase):
    """Gap-B fix: a recovering follower that still claims leadership for a
    lvstore whose ONLINE primary actually leads (the primary failed back
    while this node was partitioned) must be demoted (leader=False,
    bs_nonleadership) + follower-re-committed before the port opens —
    covering the short-blip case where the surviving hublvol controller
    lets the verify pass without a re-commit."""

    def setUp(self):
        super().setUp()
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self.lvs_leadership_on_node["LVS_P"] = True  # the stale claim

        # The demote flips the node's claim, mirroring the data plane;
        # keep recording into self.calls like the base recorder does.
        def _set_leader(lvs, *a, **kw):
            self.calls.append(("bdev_lvol_set_leader", "node", kw))
            if kw.get("leader") is False:
                self.lvs_leadership_on_node[lvs] = False
            return True
        self.node_rpc.bdev_lvol_set_leader.side_effect = _set_leader

    def _run(self, recommit=True):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow._verify_or_reconnect_peer_hublvol",
            return_value=True,
        ), patch(
            "simplyblock_core.services.tasks_runner_port_allow._reconnect_peer_hublvol_once",
            return_value=recommit,
        ) as recommit_mock:
            exec_port_allow_task(self.task)
        return recommit_mock

    def _node_demotes(self):
        return [c for c in self.calls
                if c[0] == "bdev_lvol_set_leader" and c[1] == "node"
                and c[2].get("leader") is False
                and c[2].get("bs_nonleadership") is True]

    def test_stale_claim_demoted_and_recommitted(self):
        recommit_mock = self._run()
        self.assertEqual(len(self._node_demotes()), 1,
                         f"expected one stale-leader demote; calls={self.calls}")
        forces = [c for c in self.calls
                  if c[0] == "bdev_distrib_force_to_non_leader" and c[1] == "node"]
        self.assertEqual(len(forces), 1)
        recommit_mock.assert_called_once()
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_no_demote_when_primary_not_leading(self):
        # The node is then the legitimate acting leader; demoting it would
        # leave the LVS with zero writers. The primary's own recovery
        # performs that failback.
        self.prim_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        self.assertEqual(self._node_demotes(), [])
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_persisting_claim_suspends(self):
        # Demote RPC "succeeds" but the claim does not clear -> the port
        # must not open on a contested writer.
        def _set_leader_noop(lvs, *a, **kw):
            self.calls.append(("bdev_lvol_set_leader", "node", kw))
            return True
        self.node_rpc.bdev_lvol_set_leader.side_effect = _set_leader_noop
        self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [])

    def test_no_action_without_stale_claim(self):
        self.lvs_leadership_on_node["LVS_P"] = False
        self._run()
        self.assertEqual(self._node_demotes(), [])
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class TestFenceOnFirstContact(_SecRoleReconnectBase):
    """Gap-C fix: a DOWN node's follower redirect-listener ports must be
    nvmf-blocked at first contact (before any reconnect work) and released
    only after every gate passed, strictly before the node's own client
    port opens. Ports blocked by someone else are never claimed."""

    def setUp(self):
        super().setUp()
        self.node.status = StorageNode.STATUS_DOWN
        self.fport = 4444
        self.prim.get_lvol_subsys_port = MagicMock(return_value=self.fport)
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]

    def _fence_blocks(self):
        return [c for c in self.calls
                if c[0] == "firewall_set_port" and c[1] == self.node.uuid
                and c[2] == self.fport and c[3] == "block"]

    def _fence_releases(self):
        return [c for c in self.calls
                if c[0] == "firewall_set_port" and c[1] == self.node.uuid
                and c[2] == self.fport and c[3] == "allow"]

    def test_fenced_before_work_released_before_own_port(self):
        def _record(peer, primary):
            self.calls.append(("verify_hublvol", peer.uuid, primary.uuid))
            return True
        self._run_with_verify(_record)

        i_fence = next(i for i, c in enumerate(self.calls)
                       if c[0] == "firewall_set_port" and c[1] == self.node.uuid
                       and c[2] == self.fport and c[3] == "block")
        i_work = next(i for i, c in enumerate(self.calls)
                      if c[0] == "verify_hublvol")
        i_release = next(i for i, c in enumerate(self.calls)
                         if c[0] == "firewall_set_port" and c[1] == self.node.uuid
                         and c[2] == self.fport and c[3] == "allow")
        i_own = next(i for i, c in enumerate(self.calls)
                     if c[0] == "firewall_set_port" and c[1] == self.node.uuid
                     and c[2] == self.port and c[3] == "allow")
        self.assertLess(i_fence, i_work,
                        "follower port must be fenced before any reconnect work")
        self.assertLess(i_release, i_own,
                        "fenced ports release before the node's own client port")
        self.assertEqual(self.task.function_params.get("fenced_ports"), [],
                         "the fence record must be cleared after release")

    def test_online_node_not_fenced(self):
        self.node.status = StorageNode.STATUS_ONLINE
        self._run_with_verify(lambda *a: True)
        self.assertEqual(self._fence_blocks(), [],
                         "an ONLINE node's serving listeners must never be fenced")

    def test_preblocked_port_not_claimed(self):
        with patch("simplyblock_core.port_block.is_port_blocked", return_value=True):
            self._run_with_verify(lambda *a: True)
        self.assertEqual(self._fence_blocks(), [])
        self.assertEqual(self._fence_releases(), [],
                         "a port blocked by another flow must not be released here")

    def test_suspension_keeps_the_fence(self):
        def _fail_own(peer, primary):
            return not (peer is self.node and primary is self.prim)
        self._run_with_verify(_fail_own)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(len(self._fence_blocks()), 1)
        self.assertEqual(self._fence_releases(), [],
                         "a suspended task keeps its fence — the safe state")
        self.assertEqual(self.task.function_params.get("fenced_ports"), [self.fport],
                         "the fence record must survive for the retry pass")


class TestOfflinePrimarySwitchback(_SecRoleReconnectBase):
    """Primary went OFFLINE while the secondary was partitioned: the
    tertiary is acting leader and the secondary's ANA promotion was
    skipped (trigger_ana_failover_for_node requires first_sec ONLINE).
    The recovering secondary's port_allow must complete the deferred
    failover — tertiary->secondary hublvol FIRST (hard gate), THEN
    promote the secondary's listeners to optimized — so clients switch
    back from the tertiary. Tertiary ANA is never touched."""

    def setUp(self):
        super().setUp()
        self.prim.status = StorageNode.STATUS_OFFLINE
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self.lvol = MagicMock(name="lvol")
        self.lvol.nqn = "nqn-lvol-1"
        from simplyblock_core.models.lvol_model import LVol
        self.lvol.status = LVol.STATUS_ONLINE
        self.db_mock.get_lvols_by_node_id.return_value = [self.lvol]

        def _tert_path(primary, failover):
            self.calls.append(("tert_path", primary.uuid, failover.uuid))
            return True
        self.tert.add_hublvol_failover_path = MagicMock(side_effect=_tert_path)

    def test_tertiary_path_gated_then_ana_promoted_then_allow(self):
        self._run_with_verify(lambda *a: True)
        i_tert_path = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "tert_path" and c[1] == self.prim.uuid and c[2] == self.node.uuid)
        i_promote = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "ana_set" and c[1] == self.lvol.nqn
            and c[2] == self.node.uuid and c[3] == "optimized")
        i_allow = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow")
        self.assertLess(i_tert_path, i_promote,
                        "the tertiary->secondary hublvol must be connected "
                        "BEFORE the ANA switch-back moves clients here")
        self.assertLess(i_promote, i_allow,
                        "the promotion happens before the port opens so clients "
                        "switch back the moment it is allowed")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_tertiary_path_failure_blocks_switchback(self):
        self.tert.add_hublvol_failover_path = MagicMock(return_value=False)
        self._run_with_verify(lambda *a: True)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        promotes = [c for c in self.calls if c[0] == "ana_set"]
        self.assertEqual(promotes, [],
                         "no ANA switch-back while the tertiary cannot redirect "
                         "to this node — that would open a dual-writer window")
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [])

    def test_down_primary_gates_tertiary_path_but_no_promote(self):
        # DOWN is not OFFLINE: the primary may still be serving, so the
        # deferred ANA failover must NOT fire (dual-optimized risk); the
        # tertiary path is still a hard gate while the primary is out.
        self.prim.status = StorageNode.STATUS_DOWN
        self._run_with_verify(lambda *a: True)
        promotes = [c for c in self.calls if c[0] == "ana_set"]
        self.assertEqual(promotes, [],
                         "ANA promotion is tied to primary OFFLINE only")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class TestTertiaryFollowsActingLeader(_BasePortAllowTest):
    """Recovering TERTIARY with its primary out and the secondary acting
    as leader: the tertiary's redirect must be re-wired toward the
    secondary (attach target sec_1, LVS metadata from the configured
    primary) before the port opens."""

    def setUp(self):
        super().setUp()
        self.prim2 = _make_node("node-p2", "10.0.0.14",
                                status=StorageNode.STATUS_OFFLINE)
        self.prim2.hublvol = MagicMock(name="p2_hublvol")
        self.prim2.hublvol.nqn = "nqn-p2-hub"
        self.sec1 = _make_node("node-s1", "10.0.0.15")
        self.prim2.secondary_node_id = self.sec1.uuid
        self.prim2.tertiary_node_id = self.node.uuid
        self.node.lvstore_stack_tertiary = self.prim2.uuid
        self.node.connect_to_hublvol = MagicMock(return_value=True)
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]

    def _get_node(self, uuid):
        if uuid == self.prim2.uuid:
            return self.prim2
        if uuid == self.sec1.uuid:
            return self.sec1
        return super()._get_node(uuid)

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def test_redirect_rewired_toward_acting_leader(self):
        self._run()
        self.node.connect_to_hublvol.assert_called_once()
        call = self.node.connect_to_hublvol.call_args
        self.assertIs(call.args[0], self.sec1)
        self.assertEqual(call.kwargs.get("role"), "tertiary")
        self.assertIs(call.kwargs.get("lvs_node"), self.prim2)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_failure_suspends_before_port_opens(self):
        self.node.connect_to_hublvol = MagicMock(return_value=False)
        self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [])


if __name__ == "__main__":
    unittest.main()
