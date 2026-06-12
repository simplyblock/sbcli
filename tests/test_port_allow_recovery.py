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

        # port_block.set_port spy. Port (un)blocking moved off the
        # directly-imported FirewallClient onto port_block.set_port(node,
        # port, block=...). Record each call in the legacy
        # ("firewall_set_port", target_uuid, port, action) shape so the
        # assertions below keep working unchanged.
        def _set_port_side_effect(node, port, block, *a, **kw):
            action = "block" if block else "allow"
            self.calls.append(("firewall_set_port", node.uuid, port, action))
            return True

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
                "simplyblock_core.port_block.set_port",
                side_effect=_set_port_side_effect,
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
            os.path.dirname(__file__), "..",
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
        i_allow_event = self.src.find("tcp_ports_events.port_allowed")
        self.assertGreater(i_abort, 0)
        self.assertGreater(i_done, i_abort)
        self.assertGreater(i_return, i_done)
        self.assertGreater(i_allow_event, i_return,
                           "tcp_ports_events.port_allowed must appear after "
                           "the abort path's return so it cannot fire on the "
                           "abort code path")


if __name__ == "__main__":
    unittest.main()
