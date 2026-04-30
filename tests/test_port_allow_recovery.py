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

        # B) demote happened on the peer. The previous code also drained via
        # bdev_distrib_check_inflight_io here; that loop was removed because
        # distrib-inflight includes migration IO that the port block does not
        # pause, so the drain could hold clients blocked beyond fio max_latency.
        # A fixed 0.5s quiesce in storage_node_ops now stands in for the drain.
        self.assertTrue(
            any(c[0] == "bdev_distrib_force_to_non_leader" and c[1] == "sec"
                for c in self.calls),
            "expected bdev_distrib_force_to_non_leader on the peer leader",
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


class TestGetLvsLeaderLocalNodeExemption(unittest.TestCase):
    """Unit test for ``_get_lvs_leader``'s ``local_node_id`` kwarg.

    The recovering node's DB status is ``STATUS_DOWN`` while its own
    port_allow task runs (``StorageNodeMonitor`` only flips it back to
    ``STATUS_ONLINE`` after recovery). The post-takeover verify call at
    line 302 must not treat that DB status as authoritative — SPDK is the
    ground truth for "did this node just become leader." Without
    ``local_node_id``, the STATUS_ONLINE filter would skip the recovering
    node and force the task into the spurious "No leader available, retry
    task" suspend path that leaks the peer port-block.
    """

    def test_down_node_skipped_when_local_node_id_not_set(self):
        from simplyblock_core.services.tasks_runner_port_allow import _get_lvs_leader
        n_down = _make_node("n-down", "10.0.0.10", status=StorageNode.STATUS_DOWN)
        n_online = _make_node("n-online", "10.0.0.11")
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.lvol_controller.is_node_leader",
            return_value=True,
        ) as m:
            result = _get_lvs_leader("LVS_TEST", [n_down, n_online])
        self.assertIs(result, n_online,
                      "DOWN node must be skipped when local_node_id is not set")
        m.assert_called_once_with(n_online, "LVS_TEST")

    def test_local_node_id_exempts_recovering_node_from_status_filter(self):
        from simplyblock_core.services.tasks_runner_port_allow import _get_lvs_leader
        n_down = _make_node("n-down", "10.0.0.10", status=StorageNode.STATUS_DOWN)
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.lvol_controller.is_node_leader",
            return_value=True,
        ) as m:
            result = _get_lvs_leader("LVS_TEST", [n_down], local_node_id="n-down")
        self.assertIs(result, n_down,
                      "with local_node_id set, the named DOWN node must be queried")
        m.assert_called_once_with(n_down, "LVS_TEST")

    def test_local_node_id_only_exempts_the_named_node(self):
        # Other DOWN peers are still filtered out — only the recovering node
        # is exempt. This keeps the original safety property: the controller
        # does not RPC nodes it already knows are unreachable.
        from simplyblock_core.services.tasks_runner_port_allow import _get_lvs_leader
        n_local = _make_node("n-local", "10.0.0.10", status=StorageNode.STATUS_DOWN)
        peer_down = _make_node("peer-down", "10.0.0.11", status=StorageNode.STATUS_DOWN)
        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.lvol_controller.is_node_leader",
            return_value=True,
        ) as m:
            result = _get_lvs_leader("LVS_TEST", [peer_down, n_local], local_node_id="n-local")
        self.assertIs(result, n_local)
        m.assert_called_once_with(n_local, "LVS_TEST")


class TestRecoveringNodeDownDoesNotSpuriouslySuspend(_BasePortAllowTest):
    """Regression for the network-outage cascade observed in the e2e
    (``n_plus_k_failover_multi_client_ha_all_nodes-20260424-183535``).

    Sequence the failing run produced:
      1. ``5475265e`` had a 30s NIC outage; secondary ``7b1d1d08`` took
         leadership of LVS_3636 in the meantime.
      2. NIC restored. ``exec_port_allow_task`` ran on ``5475265e``
         (DB status: STATUS_DOWN — only the monitor flips it later).
      3. The cluster-wide ``_get_lvs_leader`` correctly returned the peer
         (``7b1d1d08``); peer was port-blocked + demoted.
      4. Local ``bdev_lvol_set_leader(True)`` was issued on the recovering
         node — SPDK accepted it.
      5. The post-takeover verify ``_get_lvs_leader([node])`` filtered the
         recovering node out by ``status != STATUS_ONLINE`` and returned
         ``None``.
      6. Task suspended with "No leader available, retry task". The peer's
         firewall block was leaked (no rollback on suspend paths).
      7. On retry the failover protocol had promoted ``4da9be44`` to leader;
         that node got the same treatment, leaking its port-block too. Both
         peers ended up DOWN with port 4432 stuck blocked, and the
         recovering node never came back.

    With the fix, step (5) trusts ``is_node_leader`` (SPDK) for the
    recovering node and the verify succeeds. Task reaches STATUS_DONE; the
    peer's port is unblocked at the end of the happy path.
    """

    def test_recovering_node_db_down_completes_takeover_and_releases_peer(self):
        from simplyblock_core.services.tasks_runner_port_allow import exec_port_allow_task

        # Reproduce the actual mid-recovery DB state: the recovering node
        # is still DOWN while its own port_allow task is executing.
        self.node.status = StorageNode.STATUS_DOWN

        # SPDK leadership truth:
        #   - peer is the current leader (failover during outage)
        #   - after demote+local-takeover, the recovering node reports True
        # _get_lvs_leader is NOT patched in this test — we want the real
        # function (with the local_node_id exemption) to run end-to-end.
        def _is_node_leader(snode, lvs):
            if snode.uuid == self.sec.uuid:
                return True   # peer currently leads (pre-demote query)
            if snode.uuid == self.node.uuid:
                return True   # recovering node leads after local takeover
            return False

        with patch(
            "simplyblock_core.services.tasks_runner_port_allow.lvol_controller.is_node_leader",
            side_effect=_is_node_leader,
        ):
            exec_port_allow_task(self.task)

        # Task must complete — pre-fix this suspended with "No leader
        # available, retry task" because the post-takeover verify filtered
        # the DOWN-in-DB recovering node out of its own candidate list.
        self.assertEqual(
            self.task.status, JobSchedule.STATUS_DONE,
            "task must reach STATUS_DONE; pre-fix it suspended at the "
            "post-takeover verify because the recovering node's DB status "
            "was STATUS_DOWN, even though SPDK had accepted set_leader(True)",
        )

        # Recovering node port unblocked once leadership is verified.
        self.assertTrue(
            any(c[0] == "firewall_set_port" and c[1] == self.node.uuid and c[3] == "allow"
                for c in self.calls),
            "recovering node port must be unblocked after the verify succeeds",
        )

        # Peer port: blocked during demote, then re-allowed at the end.
        # The original cascade leaked the block; verify there is no leak by
        # asserting the peer ends up allowed (the unblock loop reached).
        peer_evts = [c for c in self.calls
                     if c[0] == "firewall_set_port" and c[1] == self.sec.uuid]
        actions = [c[3] for c in peer_evts]
        self.assertIn("block", actions,
                      "peer must be port-blocked during demote")
        self.assertIn("allow", actions,
                      "peer port must be re-allowed at the end — leaving it "
                      "blocked is the cascade that took down 7b1d1d08 / 4da9be44")
        # Block precedes allow.
        peer_block_idx = next(i for i, c in enumerate(self.calls)
                              if c[0] == "firewall_set_port" and c[1] == self.sec.uuid
                              and c[3] == "block")
        peer_allow_idx = next(i for i, c in enumerate(self.calls)
                              if c[0] == "firewall_set_port" and c[1] == self.sec.uuid
                              and c[3] == "allow")
        self.assertLess(peer_block_idx, peer_allow_idx)


if __name__ == "__main__":
    unittest.main()
