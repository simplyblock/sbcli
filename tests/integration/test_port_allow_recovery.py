# coding=utf-8
"""
test_port_allow_recovery.py — tests for
``tasks_runner_port_allow.exec_port_allow_task`` (network-outage recovery).

Reworked 2026-07-07 after the 2026-07-06 failback incident: the management
plane prepares redirection and then steps away. Invariants covered:

  1. **No port is ever blocked by this runner.** The 2026-07-06 design
     fenced a DOWN follower's redirect-listener ports at first contact and
     port-blocked the acting leader during failback; blocking LVS ports
     mid-IO severed redirect chains (DISTRIBD n_unavail_read) and JC paths
     (history_append n_success=0 -> node abort). The runner's ONLY
     ``port_block.set_port`` call is the final unblock of the recovering
     node's own port.

  2. **Device-state refresh before unblock** (replaces the old full
     ``send_cluster_map_to_node`` push, which is GONE): re-admit the
     recovering node's non-ONLINE/non-REMOVED/non-JM/non-NEW devices via
     ``device_set_online(cause=CAUSE_NODE_RECOVERY)`` (passes the stale
     re-online guard while the node is still DOWN), broadcast the node's
     local device status to ALL nodes, and send every other node's device
     status to the recovering node only — all BEFORE the port opens, so no
     distrib works from a stale device view on the first IO (2026-07-06
     18:23: 3 of 4 sids online=0, "Failed to find available location").

  3. **Minimal leadership failback.** When a peer holds LVS leadership at
     port_allow time: quiesce + ``jc_disable_replication`` on the acting
     leader (retried in a bounded LOCAL loop, not via task suspension),
     verified-open hublvols from every reachable peer to the primary, then
     exactly ONE plain ``bdev_lvol_set_leader(leader=False)`` demote of the
     acting leader. The CP never assigns leadership — a management-forced
     ``leader=True`` skips the primary's LVS update and serves stale blob
     metadata (2026-07-06 18:23 extent-metadata corruption); the primary
     takes leadership itself on the first redirected IO. If NO peer leads,
     the runner does nothing leadership-wise. No port blocks, no in-flight
     drain window anywhere.

  4. **Own follower-side hublvols wired before unblock.** Outbound to
     ONLINE primaries, inbound secondary-hublvol NQN exposure, the
     tertiary->secondary hublvol HARD GATE when the primary is out, and
     the tertiary re-wired to the acting secondary. No ANA calls inside
     the hublvol wiring.

  5. **The recovering node's port is firewall-allowed exactly once**, on
     the requested port number — the runner's only firewall action.

  6. **Deferred ANA promotion, strictly POST-unblock.** A secondary
     recovering while its primary is OFFLINE returns with non-optimized
     listeners (trigger_ana_failover_for_node skipped it while the node
     wasn't ONLINE). After the port opens, the runner promotes this
     node's listeners for the primary's lvols to "optimized" per-lvol
     via ``storage_node_ops._set_lvol_ana_on_node`` — best-effort and
     non-gating, so IO drains back from the tertiary gradually. No
     promotion when the primary is ONLINE (or merely DOWN).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import device_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.lvol_model import LVol


def _make_lvol(uuid, status=None):
    lv = LVol()
    lv.uuid = uuid
    lv.status = status or LVol.STATUS_ONLINE
    lv.nqn = f"nqn.test:lvol:{uuid}"
    return lv


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
        # outage. The recovering primary does NOT lead — and never will
        # through this runner: it promotes itself on the first redirected
        # IO after running its LVS update.
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        self.sec_rpc.jc_compression_get_status.return_value = False
        self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]

        self.node.rpc_client = MagicMock(return_value=self.node_rpc)
        self.sec.rpc_client = MagicMock(return_value=self.sec_rpc)
        self.sec.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        self.sec.connect_to_hublvol = MagicMock(return_value=True)

        self.node.get_lvol_subsys_port = MagicMock(return_value=self.port)
        self.sec.get_lvol_subsys_port = MagicMock(return_value=self.port)

        self.calls = []

        # jc_disable_replication is the failback's quiesce gate; record it
        # so ordering assertions can see it.
        def _jc_disable_side(vuid):
            self.calls.append(("jc_disable_replication", "sec", vuid))
            return True
        self.sec_rpc.jc_disable_replication.side_effect = _jc_disable_side

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

        # port_block.set_port spy. Record each call in the legacy
        # ("firewall_set_port", target_uuid, port, action) shape so the
        # assertions below keep working unchanged.
        def _set_port_side_effect(node, port, block, *a, **kw):
            action = "block" if block else "allow"
            self.calls.append(("firewall_set_port", node.uuid, port, action))
            return True

        self.db_mock = MagicMock(
            get_task_by_id=MagicMock(return_value=self.task),
            get_storage_node_by_id=MagicMock(side_effect=self._get_node),
            get_storage_nodes_by_cluster_id=MagicMock(
                side_effect=lambda cluster_id: self._cluster_nodes()),
            get_lvols_by_node_id=MagicMock(return_value=[]),
            kv_store=MagicMock(),
        )

        # Spy for the REMOVED full-cluster-map push: the targeted
        # device-state refresh replaced it and the runner must never call
        # it again.
        self.cluster_map_push = MagicMock(name="send_cluster_map_to_node")

        def _dev_event_side(dev, status, target_node=None):
            self.calls.append((
                "send_dev_status_event", dev.get_id(), status,
                target_node.uuid if target_node is not None else None))
            return True

        def _dev_set_online_side(dev_id, *a, **kw):
            self.calls.append(("device_set_online", dev_id, kw.get("cause")))
            return True

        def _check_sec_hublvol_side(peer_node, auto_fix=True, primary_node_id=None):
            self.calls.append(("check_sec_hublvol", peer_node.uuid, primary_node_id))
            return True

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
                self.cluster_map_push,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.distr_controller.send_dev_status_event",
                side_effect=_dev_event_side,
            ),
            patch(
                "simplyblock_core.services.tasks_runner_port_allow.device_controller.device_set_online",
                side_effect=_dev_set_online_side,
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
                side_effect=_check_sec_hublvol_side,
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

    def _cluster_nodes(self):
        """Nodes returned by get_storage_nodes_by_cluster_id (the targeted
        device-event loop iterates it)."""
        return [self.node, self.sec]

    def _idx(self, pred):
        return next(i for i, c in enumerate(self.calls) if pred(c))

    def _node_allows(self):
        return [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]


class TestDeviceRefreshBeforeUnblock(_BasePortAllowTest):
    """The targeted device-state refresh (which REPLACED the full
    ``send_cluster_map_to_node`` push) must complete before the firewall
    allow: (a) re-admit the recovering node's non-ONLINE devices in FDB
    with CAUSE_NODE_RECOVERY, (b) broadcast the node's local device status
    to all nodes, (c) send the other nodes' device status to the
    recovering node only. Otherwise the first IO through the unblocked
    port works from a stale device view (2026-07-06 18:23 incident:
    3 of 4 sids online=0, "Failed to find available location")."""

    def setUp(self):
        super().setUp()
        self.dev_unavail = _make_device(
            "dev-a-unavail", NVMeDevice.STATUS_UNAVAILABLE, order=0)
        self.dev_online = _make_device(
            "dev-a-online", NVMeDevice.STATUS_ONLINE, order=1)
        self.dev_jm = _make_device("dev-a-jm", NVMeDevice.STATUS_JM, order=2)
        self.node.nvme_devices = [self.dev_unavail, self.dev_online, self.dev_jm]
        self.sec_dev = _make_device("dev-b-1", NVMeDevice.STATUS_ONLINE, order=0)
        self.sec.nvme_devices = [self.sec_dev]

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def test_full_cluster_map_push_is_gone(self):
        self._run()
        self.cluster_map_push.assert_not_called()
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_readmit_uses_node_recovery_cause_and_precedes_unblock(self):
        self._run()
        readmits = [c for c in self.calls
                    if c[0] == "device_set_online"
                    and c[2] == device_controller.CAUSE_NODE_RECOVERY]
        self.assertEqual(
            [c[1] for c in readmits], ["dev-a-unavail"],
            "only non-ONLINE/non-REMOVED/non-JM/non-NEW devices are "
            "re-admitted before the unblock, with the node-recovery cause "
            "(it passes the stale re-online guard while the node is DOWN)")
        i_readmit = self._idx(
            lambda c: c[0] == "device_set_online"
            and c[2] == device_controller.CAUSE_NODE_RECOVERY)
        i_allow = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow")
        self.assertLess(i_readmit, i_allow,
                        "the FDB re-admit must land before the port opens")

    def test_local_broadcast_then_targeted_events_before_unblock(self):
        self._run()
        broadcasts = [c for c in self.calls
                      if c[0] == "send_dev_status_event" and c[3] is None]
        self.assertEqual(
            sorted(c[1] for c in broadcasts), ["dev-a-online", "dev-a-unavail"],
            "ALL local non-JM/non-NEW devices are broadcast (no target) — "
            "including the ones that stayed ONLINE through the outage; "
            "JM devices are skipped")
        targeted = [c for c in self.calls
                    if c[0] == "send_dev_status_event" and c[3] is not None]
        self.assertEqual(
            [(c[1], c[3]) for c in targeted],
            [("dev-b-1", self.node.uuid)],
            "other nodes' devices are sent to the recovering node only "
            "(targeted events, not a full cluster-map push)")
        i_readmit = self._idx(
            lambda c: c[0] == "device_set_online"
            and c[2] == device_controller.CAUSE_NODE_RECOVERY)
        i_bcast = self._idx(
            lambda c: c[0] == "send_dev_status_event" and c[3] is None)
        i_targeted = self._idx(
            lambda c: c[0] == "send_dev_status_event" and c[3] is not None)
        i_allow = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow")
        self.assertLess(i_readmit, i_bcast,
                        "FDB re-admit before the local broadcast")
        self.assertLess(i_bcast, i_targeted,
                        "local broadcast before the targeted refresh")
        self.assertLess(i_targeted, i_allow,
                        "every device event lands before the port opens")

    def test_readmit_refused_suspends_task(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "device_controller.device_set_online",
            return_value=False,
        ):
            exec_port_allow_task(self.task)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(self._node_allows(), [],
                         "no firewall allow when the pre-unblock re-admit "
                         "was refused")
        events = [c for c in self.calls if c[0] == "send_dev_status_event"]
        self.assertEqual(events, [],
                         "no device events are sent when the re-admit failed")

    def test_broadcast_failure_suspends_task(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "distr_controller.send_dev_status_event",
            side_effect=Exception("distrib send failed"),
        ):
            exec_port_allow_task(self.task)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(self._node_allows(), [],
                         "no firewall allow when the device-status refresh "
                         "failed")


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
    """MINIMAL leadership failback (2026-07-07 design, after the 2026-07-06
    failback incident): when a peer is acting leader at port_allow time the
    CP prepares redirection and steps away — quiesce + jc_disable_replication
    on the acting leader (bounded LOCAL retry loop), verified-open hublvols
    from every reachable peer to the primary, then exactly ONE plain demote
    (``bdev_lvol_set_leader(leader=False)``). The primary takes leadership
    itself on the first redirected IO; the CP never assigns it, never blocks
    a port, never drains in a blocked window."""

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def test_acting_leader_demoted_exactly_once_and_plain(self):
        self._run()
        sec_demotes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
        ]
        self.assertEqual(len(sec_demotes), 1,
                         f"expected exactly one peer demote; calls={self.calls}")
        kw = sec_demotes[0][2]
        self.assertIs(kw.get("leader"), False)
        self.assertNotIn(
            "bs_nonleadership", kw,
            "the failback demote is a PLAIN leader=False — no bs_nonleadership")
        force_calls = [
            c for c in self.calls
            if c[0] == "bdev_distrib_force_to_non_leader"
        ]
        self.assertEqual(force_calls, [],
                         "no force_to_non_leader in the minimal failback")

    def test_primary_never_takes_leadership(self):
        self._run()
        takes = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[2].get("leader") is True
        ]
        self.assertEqual(
            takes, [],
            "the runner must NEVER set leader=True: a management-forced take "
            "skips the primary's LVS update and serves stale blob metadata "
            "(2026-07-06 18:23 extent-metadata corruption)")
        node_side = [
            c for c in self.calls
            if c[0] == "bdev_lvol_set_leader" and c[1] == "node"
        ]
        self.assertEqual(node_side, [],
                         "no leadership mutation on the recovering primary at all")

    def test_failback_order_quiesce_verify_demote_unblock(self):
        self._run()
        i_jc = self._idx(lambda c: c[0] == "jc_disable_replication")
        i_demote = self._idx(
            lambda c: c[0] == "bdev_lvol_set_leader" and c[1] == "sec"
            and c[2].get("leader") is False)
        i_allow = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow")
        self.assertLess(i_jc, i_demote,
                        "journal replication is suspended before the demote")
        verifies_between = [
            c for c in self.calls[i_jc:i_demote]
            if c[0] == "check_sec_hublvol" and c[1] == self.sec.uuid
        ]
        self.assertTrue(
            verifies_between,
            "the acting leader's hublvol to the primary must be verified "
            "between the quiesce and the demote — leadership can only move "
            "if the demoted leader can redirect")
        self.assertLess(i_demote, i_allow,
                        "the recovering node's port opens only after the demote")
        self.sec.wait_for_jm_rep_tasks_to_finish.assert_called()

    def test_no_port_blocks_and_no_drain_window(self):
        self._run()
        blocks = [c for c in self.calls
                  if c[0] == "firewall_set_port" and c[3] == "block"]
        self.assertEqual(
            blocks, [],
            "the failback must NOT port-block the acting leader or any peer: "
            "LVS ports carry hublvol/redirect and journal traffic; blocking "
            "them mid-IO severed redirect chains and JC paths (2026-07-06)")
        self.sec_rpc.bdev_distrib_check_inflight_io.assert_not_called()

    def test_no_failback_when_no_peer_leader(self):
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        leadership_calls = [
            c for c in self.calls
            if c[0] in ("bdev_lvol_set_leader", "bdev_distrib_force_to_non_leader")
        ]
        self.assertEqual(leadership_calls, [],
                         "no leadership action when no peer holds leadership")
        jc_calls = [c for c in self.calls if c[0] == "jc_disable_replication"]
        self.assertEqual(jc_calls, [], "no quiesce without an acting leader")
        self.assertEqual(len(self._node_allows()), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_zero_leader_left_alone(self):
        # Nobody holds leadership (no-abort outage aftermath): the runner
        # does NOTHING leadership-wise — the primary promotes itself on the
        # first arriving IO, after running its LVS update. The old
        # _take_leadership_on_primary healing is gone (it caused the
        # 2026-07-06 stale-metadata corruption).
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self.node_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        leader_calls = [c for c in self.calls if c[0] == "bdev_lvol_set_leader"]
        self.assertEqual(leader_calls, [],
                         "a zero-leader LVS is left for the primary's "
                         "promotion-on-first-IO — no CP-side healing")
        self.assertEqual(len(self._node_allows()), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_quiesce_loop_retries_locally_then_succeeds(self):
        # jc_disable_replication False = active replication -> re-quiesce
        # and retry IN the loop (not via task suspension).
        results = iter([False, True])

        def _jc(vuid):
            self.calls.append(("jc_disable_replication", "sec", vuid))
            return next(results)
        self.sec_rpc.jc_disable_replication.side_effect = _jc

        self._run()
        jc_calls = [c for c in self.calls if c[0] == "jc_disable_replication"]
        self.assertEqual(len(jc_calls), 2,
                         "the disable retries locally inside the failback")
        self.assertEqual(self.sec.wait_for_jm_rep_tasks_to_finish.call_count, 2,
                         "each retry re-quiesces first")
        demotes = [c for c in self.calls
                   if c[0] == "bdev_lvol_set_leader" and c[1] == "sec"]
        self.assertEqual(len(demotes), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_quiesce_loop_exhaustion_suspends(self):
        from simplyblock_core.services.tasks_runner_port_allow import (
            _REPL_SUSPEND_MAX_ATTEMPTS,
        )

        def _jc(vuid):
            self.calls.append(("jc_disable_replication", "sec", vuid))
            return False
        self.sec_rpc.jc_disable_replication.side_effect = _jc

        self._run()
        jc_calls = [c for c in self.calls if c[0] == "jc_disable_replication"]
        self.assertEqual(len(jc_calls), _REPL_SUSPEND_MAX_ATTEMPTS,
                         "the quiesce+disable loop is bounded locally")
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        demotes = [c for c in self.calls if c[0] == "bdev_lvol_set_leader"]
        self.assertEqual(demotes, [],
                         "no demote against active journal replication")
        self.assertEqual(self._node_allows(), [],
                         "the port stays blocked on quiesce exhaustion")

    def test_jc_disable_raise_suspends(self):
        self.sec_rpc.jc_disable_replication.side_effect = Exception("jc rpc timeout")
        self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        demotes = [c for c in self.calls if c[0] == "bdev_lvol_set_leader"]
        self.assertEqual(demotes, [])
        self.assertEqual(self._node_allows(), [])

    def test_down_acting_leader_hublvol_is_still_a_hard_gate(self):
        # The acting leader was flipped DOWN during the outage: the peer
        # hublvol gate skips non-ONLINE peers, but the failback must still
        # re-verify the acting leader's hublvol — leadership can only move
        # if the demoted leader can redirect its in-flight IO.
        self.sec.status = StorageNode.STATUS_DOWN
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "_verify_or_reconnect_peer_hublvol",
            return_value=False,
        ):
            self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        demotes = [c for c in self.calls if c[0] == "bdev_lvol_set_leader"]
        self.assertEqual(demotes, [],
                         "no demote while the acting leader's hublvol to the "
                         "primary is not verified-open")
        self.assertEqual(self._node_allows(), [])


class TestLeadershipFailbackTertiaryActingLeader(_BasePortAllowTest):
    """Failback when the TERTIARY (not the secondary) is the acting leader
    at port_allow time: leader detection probes [secondary, tertiary]; the
    quiesce + jc_disable_replication and the single plain demote must land
    on the tertiary — never on the secondary or the recovering primary —
    and every reachable peer's hublvol to the primary is verified between
    the quiesce and the demote."""

    def setUp(self):
        super().setUp()
        self.tert = _make_node("node-c", "10.0.0.16")
        self.node.tertiary_node_id = self.tert.uuid
        self.tert_rpc = MagicMock(name="tert_rpc")
        self.tert.rpc_client = MagicMock(return_value=self.tert_rpc)
        self.tert.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        self.tert.connect_to_hublvol = MagicMock(return_value=True)
        self.tert.get_lvol_subsys_port = MagicMock(return_value=self.port)
        # The secondary does NOT lead; the tertiary took over during the
        # outage (e.g. the secondary was also briefly out).
        self.sec_rpc.bdev_lvol_get_lvstores.return_value = [
            {"lvs leadership": False}]
        self.tert_rpc.bdev_lvol_get_lvstores.return_value = [
            {"lvs leadership": True}]
        self.tert_rpc.jc_compression_get_status.return_value = False

        def _jc_disable_tert(vuid):
            self.calls.append(("jc_disable_replication", "tert", vuid))
            return True
        self.tert_rpc.jc_disable_replication.side_effect = _jc_disable_tert

        for method in ("bdev_lvol_set_leader",
                       "bdev_distrib_force_to_non_leader"):
            def _make_side(name=method):
                def _side(*a, **kw):
                    self.calls.append((name, "tert", kw))
                    return True
                return _side
            getattr(self.tert_rpc, method).side_effect = _make_side()

    def _get_node(self, uuid):
        if uuid == self.tert.uuid:
            return self.tert
        return super()._get_node(uuid)

    def _cluster_nodes(self):
        return [self.node, self.sec, self.tert]

    def _run(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

    def test_demote_lands_on_tertiary_only_and_is_plain(self):
        self._run()
        tert_demotes = [c for c in self.calls
                        if c[0] == "bdev_lvol_set_leader" and c[1] == "tert"]
        self.assertEqual(len(tert_demotes), 1,
                         f"exactly one demote, on the tertiary acting "
                         f"leader; calls={self.calls}")
        kw = tert_demotes[0][2]
        self.assertIs(kw.get("leader"), False)
        self.assertNotIn("bs_nonleadership", kw,
                         "the failback demote is a PLAIN leader=False")
        other_leader_calls = [
            c for c in self.calls
            if c[0] in ("bdev_lvol_set_leader",
                        "bdev_distrib_force_to_non_leader")
            and c[1] != "tert"]
        self.assertEqual(other_leader_calls, [],
                         "no leadership mutation on the secondary or the "
                         "recovering primary")
        forces = [c for c in self.calls
                  if c[0] == "bdev_distrib_force_to_non_leader"]
        self.assertEqual(forces, [],
                         "no force_to_non_leader in the minimal failback")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_quiesce_verify_demote_order_with_tertiary_leader(self):
        self._run()
        i_jc = self._idx(lambda c: c[0] == "jc_disable_replication"
                         and c[1] == "tert")
        i_demote = self._idx(lambda c: c[0] == "bdev_lvol_set_leader"
                             and c[1] == "tert")
        i_allow = self._idx(lambda c: c[0] == "firewall_set_port"
                            and c[1] == self.node.uuid and c[3] == "allow")
        self.assertLess(i_jc, i_demote,
                        "journal replication suspended on the TERTIARY "
                        "before its demote")
        self.assertLess(i_demote, i_allow,
                        "the recovering node's port opens only after the "
                        "tertiary is demoted")
        verified_between = {
            c[1] for c in self.calls[i_jc:i_demote]
            if c[0] == "check_sec_hublvol"}
        self.assertEqual(
            verified_between, {self.sec.uuid, self.tert.uuid},
            "BOTH reachable peers' hublvols to the primary are verified "
            "between the quiesce and the demote")
        self.tert.wait_for_jm_rep_tasks_to_finish.assert_called()
        # No jc quiesce landed on the non-leading secondary.
        jc_on_sec = [c for c in self.calls
                     if c[0] == "jc_disable_replication" and c[1] == "sec"]
        self.assertEqual(jc_on_sec, [])
        self.assertEqual(len(self._node_allows()), 1)


class TestOnlyNodePortAllowed(_BasePortAllowTest):
    """The recovering node's own port is allowed exactly once, as the last
    step — and NO port is ever blocked by this runner, on any node: peer
    port-block windows and follower fencing were removed on 2026-07-07."""

    def test_exactly_one_allow_on_recovering_node_and_it_is_last(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        node_allows = self._node_allows()
        self.assertEqual(
            len(node_allows), 1,
            f"expected exactly 1 allow on the recovering node; got {node_allows}",
        )
        self.assertEqual(node_allows[0][2], self.port)
        fw_calls = [c for c in self.calls if c[0] == "firewall_set_port"]
        self.assertEqual(fw_calls[-1][1], self.node.uuid)
        self.assertEqual(fw_calls[-1][3], "allow")
        # The unblock is the ONLY firewall action of the whole run.
        self.assertEqual(len(fw_calls), 1)

    def test_no_port_is_ever_blocked(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task
        exec_port_allow_task(self.task)

        blocks = [c for c in self.calls
                  if c[0] == "firewall_set_port" and c[3] == "block"]
        self.assertEqual(
            blocks, [],
            "the runner must NEVER call port_block.set_port with block=True — "
            "on any node, any port, any phase. LVS ports carry hublvol/"
            "redirect and journal traffic; blocking them mid-IO produced "
            "DISTRIBD n_unavail_read errors and JC history_append failures "
            "(incident 2026-07-06)")


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

    def _failback_fn(self):
        start = self.src.find("def _failback_leadership_to_primary")
        self.assertGreater(start, 0, "the minimal failback helper must exist")
        end = self.src.find("\ndef ", start + 1)
        return self.src[start:end]

    def test_no_get_lvs_leader_helper(self):
        # The old pre-2026-05-02 helper stays gone; leader detection lives
        # inline in exec_port_allow_task with explicit failure handling.
        self.assertNotIn("def _get_lvs_leader", self.src)

    def test_failback_present_and_ordered(self):
        # The minimal failback: helper present, and — in exec — the failback
        # call sits after the peer hublvol gate and before the node's port
        # unblock.
        self.assertIn("def _failback_leadership_to_primary", self.src)
        i_gate = self.src.find("_verify_or_reconnect_peer_hublvol(sec_node, node)")
        i_failback_call = self.src.find(
            "failback_ok, failback_msg = _failback_leadership_to_primary(")
        i_unblock = self.src.find(
            "port_block.set_port(node, p, block=False")
        self.assertGreater(i_gate, 0)
        self.assertGreater(i_failback_call, i_gate,
                           "failback must run only after the peer hublvol gate")
        self.assertGreater(i_unblock, i_failback_call,
                           "the node's port opens only after the failback")

    def test_failback_is_minimal(self):
        # Inside _failback_leadership_to_primary: no port blocks, no drain
        # poll, no bs_nonleadership / force_to_non_leader — exactly one
        # plain demote of the acting leader.
        fn = self._failback_fn()
        self.assertNotIn("block=True", fn)
        self.assertNotIn("bdev_distrib_check_inflight_io", fn)
        self.assertNotIn("bs_nonleadership", fn)
        self.assertNotIn("bdev_distrib_force_to_non_leader", fn)
        self.assertEqual(
            fn.count(".bdev_lvol_set_leader("), 1,
            "exactly one leadership mutation in the failback: the plain "
            "demote of the acting leader")
        # Order: quiesce/disable -> hublvol verify -> demote.
        i_jc = fn.find(".jc_disable_replication(")
        i_hub = fn.find("_verify_or_reconnect_peer_hublvol(")
        i_demote = fn.find(".bdev_lvol_set_leader(")
        self.assertTrue(
            0 < i_jc < i_hub < i_demote,
            f"failback order must be jc_disable -> hublvol verify -> demote "
            f"(got jc={i_jc}, hub={i_hub}, demote={i_demote})")
        # The quiesce loop is bounded locally, not via task suspension.
        self.assertIn("_REPL_SUSPEND_MAX_ATTEMPTS", fn)

    def test_runner_never_takes_leadership_and_never_blocks(self):
        # A management-forced leader=True caused the 2026-07-06
        # stale-blob-metadata corruption; _take_leadership_on_primary is
        # deleted. And the runner never blocks any port: exactly one
        # port_block.set_port callsite, the final block=False unblock.
        self.assertNotIn("def _take_leadership_on_primary", self.src)
        self.assertNotIn("leader=True)", self.src)
        self.assertNotIn("block=True", self.src)
        import re
        set_port_calls = re.findall(r"port_block\.set_port\(", self.src)
        self.assertEqual(len(set_port_calls), 1,
                         "exactly one set_port callsite: the final unblock")
        self.assertIn("port_block.set_port(node, p, block=False", self.src)

    def test_fencing_recommit_and_map_push_are_gone(self):
        for gone in (
            "_fence_follower_ports_on_first_contact",
            "_release_fenced_ports",
            "_recommit_follower_and_unblock",
            "_recommit_followers_for_leader",
            "send_cluster_map_to_node",
            "_failback_primary_ana",
        ):
            self.assertNotIn(
                gone, self.src,
                f"{gone} was removed in the 2026-07-07 rework and must not "
                f"be silently reintroduced")

    def test_deferred_ana_promotion_is_post_unblock_and_offline_gated(self):
        # The ONLY ANA call left in the runner is the deferred promotion of
        # a recovered secondary whose primary is OFFLINE — it must sit AFTER
        # the unblock (non-gating, gradual drain-back) and behind the
        # primary-OFFLINE check. No ANA anywhere before the port opens.
        self.assertEqual(
            self.src.count("_set_lvol_ana_on_node"), 1,
            "exactly one ANA callsite in the runner: the deferred "
            "post-unblock promotion")
        i_unblock = self.src.find(
            "port_block.set_port(node, p, block=False")
        i_event = self.src.find("tcp_ports_events.port_allowed")
        i_gate = self.src.find("ana_primary.status == StorageNode.STATUS_OFFLINE")
        i_ana = self.src.find("storage_node_ops._set_lvol_ana_on_node(")
        self.assertGreater(i_ana, i_unblock,
                           "the deferred ANA promotion runs only after the "
                           "port unblock — it must never gate the recovery")
        self.assertGreater(i_ana, i_event,
                           "the promotion runs after the port_allowed event")
        self.assertTrue(i_unblock < i_gate < i_ana,
                        "the promotion is gated on the primary being OFFLINE")
        self.assertIn('_set_lvol_ana_on_node(lvol, node, "optimized")', self.src,
                      "the recovered secondary's listeners are promoted to "
                      "optimized, per-lvol")

    def test_device_refresh_present_and_ordered(self):
        # Re-admit (CAUSE_NODE_RECOVERY) -> local broadcast -> targeted
        # events to the recovering node -> unblock.
        i_readmit = self.src.find("cause=device_controller.CAUSE_NODE_RECOVERY")
        i_broadcast = self.src.find("distr_controller.send_dev_status_event(")
        i_targeted = self.src.find("target_node=node")
        i_unblock = self.src.find(
            "port_block.set_port(node, p, block=False")
        self.assertGreater(i_readmit, 0)
        self.assertGreater(i_broadcast, i_readmit,
                           "FDB re-admit before the broadcast")
        self.assertGreater(i_targeted, i_broadcast,
                           "local broadcast before the targeted refresh")
        self.assertGreater(i_unblock, i_targeted,
                           "device refresh completes before the unblock")

    def test_rationale_documented_in_source(self):
        self.assertIn("2026-05-02", self.src)
        self.assertIn("2026-07-06", self.src)
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
        # leadership, so the leadership failback block is a no-op (its own
        # coverage lives in TestLeadershipFailback).
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
        # The invariant is about the port_allowed occurrence in
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
    """The device re-admit around port_allow must NOT be gated on
    ``node.status == ONLINE``.

    port_allow runs as the last step of node recovery and completes a couple of
    seconds BEFORE the storage-node monitor flips the node's status to ONLINE.
    An earlier self-heal guarded on ``node.status == ONLINE``, so it was False
    every time and the re-admit was skipped -- a device the remote-IO quorum had
    force-marked UNAVAILABLE during the outage stayed down with no other recovery
    path (device_monitor auto-restart only touches io_error devices), leaving a
    phantom offline-device baseline that a later node outage turned into a cluster
    suspension.

    Since the 2026-07-07 rework there are two re-admit sites: the HARD pre-unblock
    re-admit (cause=CAUSE_NODE_RECOVERY, refusal suspends the task) and the
    best-effort post-unblock epilogue (kept as a safety net; refusal is logged,
    the node-online clear in storage_node_monitor owns that re-admit). Both run
    regardless of node status and prior device state; REMOVED is the one terminal
    state we never resurrect.
    """

    def setUp(self):
        super().setUp()
        # The node is NOT yet ONLINE when port_allow runs -- by design.
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
        self.readmit_calls = []

        def _record(dev_id, *a, **kw):
            self.readmitted.append(dev_id)
            self.readmit_calls.append((dev_id, kw.get("cause")))
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

    def test_pre_unblock_readmit_uses_node_recovery_cause(self):
        self._run()
        self.assertIn(
            ("dev-unavail", device_controller.CAUSE_NODE_RECOVERY),
            self.readmit_calls,
            "the pre-unblock re-admit must pass CAUSE_NODE_RECOVERY so "
            "device_set_state's stale re-online guard admits the device "
            "while the node is still DOWN")

    def test_pre_unblock_refusal_suspends_task(self):
        # Before the unblock, a refused re-admit is a HARD gate: the port
        # must not open against a stale device view — suspend and retry.
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "device_controller.device_set_online",
            return_value=False,
        ):
            self._run()
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [],
                         "the port must not open when the pre-unblock "
                         "re-admit was refused")

    def test_epilogue_refusal_is_best_effort_and_logged(self):
        # The post-unblock epilogue re-admit (kept as a safety net) stays
        # best-effort: device_set_state may still refuse ONLINE while the
        # monitor hasn't flipped the node yet; the node-online clear in
        # storage_node_monitor owns that re-admit. It must warn loudly
        # (the 2026-07-02 suspend series went unnoticed for five iterations
        # because the refusal was silent) and still complete the task.
        def _refuse_epilogue(dev_id, *a, **kw):
            # pre-unblock re-admit (node-recovery cause) passes; the
            # epilogue call (no cause) is refused.
            return kw.get("cause") == device_controller.CAUSE_NODE_RECOVERY

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "device_controller.device_set_online",
            side_effect=_refuse_epilogue,
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

    def _cluster_nodes(self):
        return [self.node, self.sec, self.prim, self.tert]

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

    def test_tertiary_path_failure_is_a_hard_gate(self):
        # A reachable tertiary whose inbound redirect path to this recovering
        # secondary cannot be connected is a HARD gate: reopening the secondary
        # listener with no tertiary redirect is a dual-writer risk once
        # leadership sits on the tertiary. The task suspends and the port stays
        # closed (ce892407: split inbound/outbound hublvols, inbound is gating).
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        self.tert.add_hublvol_failover_path = MagicMock(return_value=False)
        self._run_with_verify(lambda *a: True)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [],
                         "the port must not open with a broken tertiary redirect")
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)

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

    def test_follower_recovery_is_hublvol_wiring_only(self):
        # Scenario (iv): the recovering node is a plain follower of an
        # ONLINE, leading primary. Its recovery is hublvol wiring only —
        # no leadership mutation anywhere, no ANA call, one final unblock.
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "storage_node_ops._set_lvol_ana_on_node",
        ) as ana_mock:
            self._run_with_verify(lambda *a: True)
        leadership_calls = [
            c for c in self.calls
            if c[0] in ("bdev_lvol_set_leader",
                        "bdev_distrib_force_to_non_leader")
        ]
        self.assertEqual(leadership_calls, [],
                         "a follower recovering under a leading primary "
                         "triggers no leadership action at all")
        ana_mock.assert_not_called()
        self.assertEqual(len(self._node_allows()), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_no_ana_promotion_when_primary_online(self):
        # The deferred post-unblock ANA promotion is gated on the primary
        # being OFFLINE. With the primary ONLINE the recovered secondary's
        # listeners stay untouched even though the primary has lvols.
        self.db_mock.get_lvols_by_node_id.side_effect = (
            lambda nid: [_make_lvol("lv-on")] if nid == self.prim.uuid else [])
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "storage_node_ops._set_lvol_ana_on_node",
        ) as ana_mock:
            self._run_with_verify(lambda *a: True)
        ana_mock.assert_not_called()
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class TestStaleLeaderConvergence(_SecRoleReconnectBase):
    """After the 2026-07-07 rework (f98a73cb / ce892407) the port-allow runner
    NEVER assigns or mutates leadership for a lvstore where the recovering node
    is a follower: a management-forced leader flip skips the LVS
    update-on-first-IO and serves stale blob metadata (2026-07-06 corruption).
    The node-side stale-claim demote + follower-re-commit machinery of the
    earlier Gap-B fix was removed (documented open item — the takeover case is
    reconciled by the primary's own recovery, not here). These tests pin that
    the runner leaves a recovering follower's stale leadership claim alone and
    still opens the port."""

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

    def test_stale_claim_not_converged_by_runner(self):
        # The recovering follower still claims LVS_P while the ONLINE primary
        # leads it. The runner does NOT demote the node (no leader mutation,
        # no bs_nonleadership force) — that reconciliation is a documented open
        # item owned by the primary's own recovery. The port still opens.
        self._run()
        self.assertEqual(self._node_demotes(), [],
                         f"the runner must not demote a recovering follower; "
                         f"calls={self.calls}")
        forces = [c for c in self.calls
                  if c[0] == "bdev_distrib_force_to_non_leader" and c[1] == "node"]
        self.assertEqual(forces, [])
        self.assertEqual(len(self._node_allows()), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_no_demote_when_primary_not_leading(self):
        # The node is then the legitimate acting leader; demoting it would
        # leave the LVS with zero writers. The primary's own recovery
        # performs that failback.
        self.prim_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        self._run()
        self.assertEqual(self._node_demotes(), [])
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_no_action_without_stale_claim(self):
        self.lvs_leadership_on_node["LVS_P"] = False
        self._run()
        self.assertEqual(self._node_demotes(), [])
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


class TestNoFollowerPortFencing(_SecRoleReconnectBase):
    """The 2026-07-06 design fenced a DOWN node's follower redirect-listener
    ports at first contact and released them after the gates; the 2026-07-07
    rework REMOVED that fencing entirely — blocking LVS ports severs the
    hublvol/redirect and journal traffic that rides on them. The runner must
    never block any port, on any node, in any phase, and never write a
    fence record into the task params."""

    def setUp(self):
        super().setUp()
        self.node.status = StorageNode.STATUS_DOWN
        self.prim.get_lvol_subsys_port = MagicMock(return_value=4444)
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]

    def _blocks(self):
        return [c for c in self.calls
                if c[0] == "firewall_set_port" and c[3] == "block"]

    def test_down_follower_never_fenced(self):
        self._run_with_verify(lambda *a: True)
        self.assertEqual(
            self._blocks(), [],
            "a DOWN follower's redirect-listener ports must NOT be fenced "
            "at first contact — the fencing was removed on 2026-07-07")
        self.assertNotIn("fenced_ports", self.task.function_params,
                         "no fence record is ever written into the task")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_suspension_leaves_no_block_behind(self):
        def _fail_own(peer, primary):
            return not (peer is self.node and primary is self.prim)
        self._run_with_verify(_fail_own)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(
            self._blocks(), [],
            "a suspended recovery must not leave any port blocked — the "
            "runner never blocked one in the first place")

    @unittest.skip("pending design call 2026-07: follower-port fencing at "
                   "first contact (removed 2026-07-07 with the port-allow "
                   "rework — blocking LVS ports severed redirect/journal "
                   "traffic; an alternative quiet-window mechanism is TBD)")
    def test_fence_window_semantics(self):
        raise NotImplementedError


class TestOfflinePrimaryTertiaryGate(_SecRoleReconnectBase):
    """Primary went OFFLINE while the secondary was partitioned: the
    tertiary is the acting leader for that lvstore, and clients may land
    on the recovering secondary the moment its port opens. The
    tertiary->secondary hublvol path is therefore a HARD GATE while the
    primary is out — the tertiary must be able to redirect here first, or
    a dual-writer window opens. The ANA switch-back for this scenario is
    DEFERRED: strictly after the unblock, the runner promotes this
    secondary's listeners for the primary's lvols to optimized, per-lvol
    and best-effort (see the ANA tests below)."""

    def setUp(self):
        super().setUp()
        self.prim.status = StorageNode.STATUS_OFFLINE
        self.node_rpc.subsystem_list.return_value = [{"nqn": "nqn-p-hub"}]

        def _tert_path(primary, failover):
            self.calls.append(("tert_path", primary.uuid, failover.uuid))
            return True
        self.tert.add_hublvol_failover_path = MagicMock(side_effect=_tert_path)

    def test_tertiary_path_gated_before_allow(self):
        self._run_with_verify(lambda *a: True)
        i_tert_path = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "tert_path" and c[1] == self.prim.uuid and c[2] == self.node.uuid)
        i_allow = next(
            i for i, c in enumerate(self.calls)
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow")
        self.assertLess(i_tert_path, i_allow,
                        "the tertiary->secondary hublvol must be connected "
                        "BEFORE the port opens — the tertiary is the acting "
                        "leader and must be able to redirect here")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_tertiary_path_failure_suspends(self):
        self.tert.add_hublvol_failover_path = MagicMock(return_value=False)
        self._run_with_verify(lambda *a: True)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)
        node_allows = [
            c for c in self.calls
            if c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
        ]
        self.assertEqual(node_allows, [],
                         "the port must not open while the acting-leader "
                         "tertiary cannot redirect to this node — that would "
                         "open a dual-writer window")

    def test_down_primary_also_gates_tertiary_path(self):
        # DOWN is not OFFLINE, but either way the primary is out: the
        # tertiary path stays a hard gate.
        self.prim.status = StorageNode.STATUS_DOWN
        self._run_with_verify(lambda *a: True)
        tert_paths = [c for c in self.calls if c[0] == "tert_path"]
        self.assertEqual(len(tert_paths), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    # --- deferred post-unblock ANA promotion (primary OFFLINE) ----------

    def _with_prim_lvols(self):
        self.lvol_ok = _make_lvol("lv-ok")
        self.lvol_deleting = _make_lvol("lv-del", status=LVol.STATUS_IN_DELETION)
        self.db_mock.get_lvols_by_node_id.side_effect = (
            lambda nid: [self.lvol_ok, self.lvol_deleting]
            if nid == self.prim.uuid else [])

    def _run_with_ana(self, ana_side=None):
        def _record_ana(lvol, target, state):
            self.calls.append(("ana_set", lvol.uuid, target.uuid, state))
            return True
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow."
            "storage_node_ops._set_lvol_ana_on_node",
            side_effect=ana_side or _record_ana,
        ) as ana_mock:
            self._run_with_verify(lambda *a: True)
        return ana_mock

    def test_ana_switchback_promotes_secondary_listeners_post_unblock(self):
        # Primary OFFLINE: after (and only after) the port opens, this
        # recovered secondary's listeners for the primary's serviceable
        # lvols are promoted to optimized — per-lvol, so IO drains back
        # from the tertiary gradually. Lvols in terminal/transitional
        # states are skipped.
        self._with_prim_lvols()
        self._run_with_ana()
        ana_calls = [c for c in self.calls if c[0] == "ana_set"]
        self.assertEqual(
            ana_calls, [("ana_set", "lv-ok", self.node.uuid, "optimized")],
            "only ONLINE/OFFLINE lvols of the offline primary are promoted, "
            "on the recovering secondary, to optimized")
        i_allow = self._idx(
            lambda c: c[0] == "firewall_set_port" and c[1] == self.node.uuid
            and c[3] == "allow")
        i_event = self._idx(lambda c: c[0] == "port_allowed")
        i_ana = self._idx(lambda c: c[0] == "ana_set")
        self.assertLess(i_allow, i_ana,
                        "the ANA promotion is strictly POST-unblock — it "
                        "must never gate the recovery")
        self.assertLess(i_event, i_ana,
                        "the promotion runs after the port_allowed event")
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)

    def test_ana_promotion_failure_is_best_effort(self):
        # A failing per-lvol promotion must not fail the (already
        # completed) recovery: the port stays open and the task completes.
        self._with_prim_lvols()

        def _boom(lvol, target, state):
            self.calls.append(("ana_set", lvol.uuid, target.uuid, state))
            raise Exception("listener set-ana rpc failed")
        self._run_with_ana(ana_side=_boom)
        self.assertEqual(len(self._node_allows()), 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_DONE)


@unittest.skip("pending design call 2026-07: deferred ANA DEMOTE (a "
               "recovering secondary's still-optimized listeners once its "
               "ONLINE primary provably leads) — the 2026-07-07 rework kept "
               "only the post-unblock PROMOTION for the offline-primary "
               "case (covered in TestOfflinePrimaryTertiaryGate); the "
               "demote direction has no owner in the runner yet")
class TestDeferredAnaFailback(unittest.TestCase):
    """Placeholder keeping the coverage gap visible.

    Pre-2026-07-07 behavior (Gap-A fix): a secondary that missed the
    primary's failback while partitioned returned with its listeners still
    optimized; once the ONLINE primary provably led, the recovering
    secondary demoted its own listeners for that lvstore back to
    non_optimized via storage_node_ops._set_lvol_ana_on_node. The rework
    kept only the reverse direction (post-unblock promotion when the
    primary is OFFLINE); whether (and where) the deferred demote belongs
    now is an open design question."""

    def test_demoted_when_primary_leads(self):
        raise NotImplementedError


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
