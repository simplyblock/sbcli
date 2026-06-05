# coding=utf-8
"""
Tests for the no-suspension graceful shutdown flow.

The legacy `suspend_storage_node()` precursor (iptables port-block on
sec/tert + own-primary LVS ports) was removed because it could not
stop SPDK's lvol layer from resubmitting failed-redirect IO as if it
were new host IO, which races the surviving sec/tert peer's
auto-promotion and produces a writer conflict (incident 2026-05-19,
jm_vuid=4818, fio EIO on v2_c15).

The new shutdown flow:

  1. set status -> in_shutdown
  2. cancel migration tasks
  3. Loop 1: device_set_unavailable() / set_jm_device_state() for
     every device on the dying node (these fire distr_status_events
     to peers under the hood)
  4. Loop 2: _detach_remote_controllers_from_peers() — for every
     peer in {online, down, in_restart}, bdev_nvme_detach_controller
     on each remote_alceml_<dev-uuid> / remote_jm_<node-uuid> that
     references the dying node
  5. spdk_process_kill
  6. set status -> offline

These tests verify each property using lightweight mocks.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_node(uuid, status=StorageNode.STATUS_ONLINE, n_devices=2,
               with_jm=True, mgmt_ip="10.0.0.1", rpc_port=8080):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "cluster-1"
    n.status = status
    n.hostname = f"host-{uuid}"
    n.mgmt_ip = mgmt_ip
    n.rpc_port = rpc_port
    n.rpc_username = "u"
    n.rpc_password = "p"
    n.lvstore = ""
    n.lvstore_status = ""
    n.is_secondary_node = False
    n.lvstore_stack_secondary = ""
    n.lvstore_stack_tertiary = ""

    devs = []
    for i in range(n_devices):
        d = NVMeDevice()
        d.uuid = f"dev-{uuid}-{i}"
        d.node_id = uuid
        d.status = NVMeDevice.STATUS_ONLINE
        d.pcie_address = f"0000:00:0{i}.0"
        d.alceml_bdev = f"alceml_{d.uuid}"
        d.alceml_name = d.alceml_bdev
        devs.append(d)
    n.nvme_devices = devs
    n.remote_devices = []
    n.remote_jm_devices = []

    if with_jm:
        jm = JMDevice()
        jm.uuid = f"jm-{uuid}"
        jm.node_id = uuid
        jm.status = JMDevice.STATUS_ONLINE
        jm.jm_bdev = f"jm_{uuid}"
        n.jm_device = jm
        n.jm_ids = [jm.uuid]

    n.data_nics = [IFace()]
    n.data_nics[0].ip4_address = mgmt_ip
    n.data_nics[0].trtype = "TCP"
    n.active_tcp = True
    n.active_rdma = False
    return n


def _make_remote_dev(home_node_id, idx):
    """A remote_alceml entry on some peer that points back to home_node_id."""
    d = NVMeDevice()
    d.uuid = f"remote-dev-{home_node_id}-{idx}"
    d.node_id = home_node_id
    d.status = NVMeDevice.STATUS_ONLINE
    d.alceml_name = f"alceml_dev-{home_node_id}-{idx}"
    d.remote_bdev = f"remote_alceml_dev-{home_node_id}-{idx}n1"
    return d


def _make_remote_jm(home_node_id):
    """A remote_jm entry on some peer that points back to home_node_id."""
    j = JMDevice()
    j.uuid = f"remote-jm-{home_node_id}"
    j.node_id = home_node_id
    j.status = JMDevice.STATUS_ONLINE
    j.remote_bdev = f"remote_jm_{home_node_id}n1"
    return j


# ---------------------------------------------------------------------------
# 1. _target_is_reconnect_eligible
# ---------------------------------------------------------------------------


class TestTargetIsReconnectEligible(unittest.TestCase):
    """The reconnect-eligibility gate accepts online/down/in_restart and
    rejects every other status — including in_shutdown (the entire reason
    this gate exists)."""

    def test_eligible_statuses(self):
        from simplyblock_core import storage_node_ops
        for s in (
            StorageNode.STATUS_ONLINE,
            StorageNode.STATUS_DOWN,
            StorageNode.STATUS_RESTARTING,
        ):
            n = _make_node("x", status=s)
            self.assertTrue(
                storage_node_ops._target_is_reconnect_eligible(n),
                f"{s} should be eligible",
            )

    def test_ineligible_statuses(self):
        from simplyblock_core import storage_node_ops
        for s in (
            StorageNode.STATUS_IN_SHUTDOWN,
            StorageNode.STATUS_OFFLINE,
            StorageNode.STATUS_UNREACHABLE,
            StorageNode.STATUS_REMOVED,
            StorageNode.STATUS_SUSPENDED,
        ):
            n = _make_node("x", status=s)
            self.assertFalse(
                storage_node_ops._target_is_reconnect_eligible(n),
                f"{s} should NOT be eligible",
            )

    def test_none_target(self):
        from simplyblock_core import storage_node_ops
        self.assertFalse(storage_node_ops._target_is_reconnect_eligible(None))


# ---------------------------------------------------------------------------
# 2. _detach_remote_controllers_from_peers
# ---------------------------------------------------------------------------


class TestDetachRemoteControllersFromPeers(unittest.TestCase):
    """Loop 2 of graceful shutdown — verify peer iteration, filtering,
    and that bdev_nvme_detach_controller is called with the right
    controller names."""

    def _setup(self, dying_id="dying", peer_statuses=None,
               peer_has_refs=True):
        """Build a cluster with one dying node and several peers in
        configurable statuses. Returns (dying_node, peers, rpc_clients,
        db_controller_mock, detach_calls_list)."""
        from simplyblock_core import storage_node_ops

        peer_statuses = peer_statuses or {
            "p_online": StorageNode.STATUS_ONLINE,
            "p_down": StorageNode.STATUS_DOWN,
            "p_in_restart": StorageNode.STATUS_RESTARTING,
            "p_offline": StorageNode.STATUS_OFFLINE,
        }

        dying = _make_node(dying_id, status=StorageNode.STATUS_IN_SHUTDOWN)
        peers = []
        detach_calls = []  # list of (peer_id, ctrl_name)
        rpc_clients = {}

        for pid, pstatus in peer_statuses.items():
            p = _make_node(pid, status=pstatus)
            if peer_has_refs:
                p.remote_devices = [
                    _make_remote_dev(dying_id, 0),
                    _make_remote_dev(dying_id, 1),
                    _make_remote_dev("other-node", 0),  # unrelated, must not be detached
                ]
                p.remote_jm_devices = [_make_remote_jm(dying_id)]

            class _FakeRPC:
                def __init__(self, peer_id):
                    self.peer_id = peer_id

                def bdev_nvme_detach_controller(self, name):
                    detach_calls.append((self.peer_id, name))
                    return True

            fake = _FakeRPC(pid)
            rpc_clients[pid] = fake
            p.rpc_client = lambda timeout=None, retry=None, _r=fake: _r
            peers.append(p)

        all_nodes = [dying] + peers
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = all_nodes
        return dying, peers, rpc_clients, db, detach_calls, storage_node_ops

    def test_detaches_from_eligible_peers_only(self):
        dying, peers, _rpcs, db, detach_calls, sno = self._setup()
        sno._detach_remote_controllers_from_peers(dying, db)

        peers_called = {pid for pid, _ in detach_calls}
        # Online, down, in_restart -> all three should have been called.
        self.assertIn("p_online", peers_called)
        self.assertIn("p_down", peers_called)
        self.assertIn("p_in_restart", peers_called)
        # Offline -> never called.
        self.assertNotIn("p_offline", peers_called)

    def test_detaches_only_controllers_referencing_dying_node(self):
        dying, peers, _rpcs, db, detach_calls, sno = self._setup()
        sno._detach_remote_controllers_from_peers(dying, db)

        # Every detach must be for a controller name that references
        # the dying node id. The unrelated "other-node" controller must
        # never be detached.
        for pid, ctrl_name in detach_calls:
            self.assertIn("dying", ctrl_name,
                          f"unexpected detach: peer={pid} ctrl={ctrl_name}")
            self.assertNotIn("other-node", ctrl_name,
                             f"unrelated detach: peer={pid} ctrl={ctrl_name}")

    def test_strips_n1_suffix_for_controller_name(self):
        dying, peers, _rpcs, db, detach_calls, sno = self._setup()
        sno._detach_remote_controllers_from_peers(dying, db)
        for _pid, ctrl_name in detach_calls:
            self.assertFalse(
                ctrl_name.endswith("n1"),
                f"controller name should not include n1 suffix: {ctrl_name}",
            )

    def test_silent_on_peer_rpc_failure(self):
        """One peer raises; the rest must still get detached, and the
        helper must return rather than re-raise."""
        from simplyblock_core import storage_node_ops as sno

        dying = _make_node("dying", status=StorageNode.STATUS_IN_SHUTDOWN)
        detach_calls = []

        class _GoodRPC:
            def bdev_nvme_detach_controller(self, name):
                detach_calls.append(name)
                return True

        class _BadRPC:
            def bdev_nvme_detach_controller(self, name):
                raise RuntimeError("simulated RPC failure")

        p_good = _make_node("p_good", status=StorageNode.STATUS_ONLINE)
        p_good.remote_devices = [_make_remote_dev("dying", 0)]
        p_good.remote_jm_devices = []
        good_rpc = _GoodRPC()
        p_good.rpc_client = lambda timeout=None, retry=None: good_rpc

        p_bad = _make_node("p_bad", status=StorageNode.STATUS_ONLINE)
        p_bad.remote_devices = [_make_remote_dev("dying", 0)]
        p_bad.remote_jm_devices = []
        bad_rpc = _BadRPC()
        p_bad.rpc_client = lambda timeout=None, retry=None: bad_rpc

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [dying, p_good, p_bad]

        # Must not raise.
        sno._detach_remote_controllers_from_peers(dying, db)

        # The good peer's detach went through (1 call).
        self.assertEqual(len(detach_calls), 1)

    def test_peer_with_no_refs_skipped_silently(self):
        """A peer in_restart that hasn't created the remote controllers
        yet has empty remote_devices/remote_jm_devices — must not be a
        problem."""
        from simplyblock_core import storage_node_ops as sno

        dying = _make_node("dying", status=StorageNode.STATUS_IN_SHUTDOWN)
        p = _make_node("p", status=StorageNode.STATUS_RESTARTING)
        # No remote refs at all (controller not yet attached during restart).
        p.remote_devices = []
        p.remote_jm_devices = []

        called = []

        class _RPC:
            def bdev_nvme_detach_controller(self, name):
                called.append(name)

        p.rpc_client = lambda timeout=None, retry=None: _RPC()

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [dying, p]
        count = sno._detach_remote_controllers_from_peers(dying, db)
        self.assertEqual(count, 0)
        self.assertEqual(called, [])

    def test_returns_zero_when_no_peers(self):
        from simplyblock_core import storage_node_ops as sno
        dying = _make_node("dying", status=StorageNode.STATUS_IN_SHUTDOWN)
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [dying]
        self.assertEqual(
            sno._detach_remote_controllers_from_peers(dying, db), 0)


# ---------------------------------------------------------------------------
# 3. suspend_storage_node / resume_storage_node are deprecated noops
# ---------------------------------------------------------------------------


class TestSuspendAndResumeAreNoops(unittest.TestCase):
    """Old `sn suspend` and `sn resume` callers must still get a True
    return so external automation doesn't break, but the body must do
    nothing — in particular it must not touch FirewallClient."""

    def test_suspend_is_noop(self):
        from simplyblock_core import storage_node_ops as sno
        with patch.object(sno, "FirewallClient") as fw_cls:
            self.assertTrue(sno.suspend_storage_node("any-node-id"))
            fw_cls.assert_not_called()

    def test_resume_is_noop(self):
        from simplyblock_core import storage_node_ops as sno
        with patch.object(sno, "FirewallClient") as fw_cls:
            self.assertTrue(sno.resume_storage_node("any-node-id"))
            fw_cls.assert_not_called()


# ---------------------------------------------------------------------------
# 4. shutdown_storage_node graceful path
# ---------------------------------------------------------------------------


class TestShutdownStorageNodeGraceful(unittest.TestCase):
    """Verify the graceful path runs Loop 1 (device-unavailable) and
    Loop 2 (detach), does NOT call FirewallClient, and finishes with
    a SPDK kill + offline transition."""

    def _patch(self, force=False):
        from simplyblock_core import storage_node_ops as sno

        snode = _make_node("dying", status=StorageNode.STATUS_ONLINE,
                            n_devices=2, with_jm=True)
        peer = _make_node("peer", status=StorageNode.STATUS_ONLINE)
        peer.remote_devices = [_make_remote_dev("dying", 0)]
        peer.remote_jm_devices = [_make_remote_jm("dying")]

        # peer detach RPC sink
        detach_calls = []
        class _PeerRPC:
            def bdev_nvme_detach_controller(self, name):
                detach_calls.append(name)
                return True
        peer.rpc_client = lambda timeout=None, retry=None: _PeerRPC()

        # dying-node SPDK kill + bind sink
        kill_calls = []
        class _DyingClient:
            def spdk_process_kill(self, rpc_port, cluster_id):
                kill_calls.append((rpc_port, cluster_id))
                return True
            def bind_device_to_nvme(self, pci):
                return True
        snode.client = lambda timeout=None, retry=None: _DyingClient()

        db = MagicMock()

        # get_storage_node_by_id is called several times in shutdown
        def _get_node(nid):
            return snode if nid == snode.get_id() else peer
        db.get_storage_node_by_id.side_effect = _get_node
        db.get_storage_nodes_by_cluster_id.return_value = [snode, peer]
        db.get_job_tasks.return_value = []
        db.kv_store = MagicMock()

        device_unavailable_calls = []
        def _dev_set_unavail(dev_id, cause=None):
            device_unavailable_calls.append(dev_id)
            return True

        set_jm_calls = []
        def _set_jm(dev_id, state):
            set_jm_calls.append((dev_id, state))
            return True

        status_changes = []
        def _set_status(nid, status, caused_by="monitor"):
            status_changes.append((nid, status))
            return True

        patches = [
            patch.object(sno, "DBController", return_value=db),
            # _check_ftt_allows_node_removal is no longer called from
            # shutdown_storage_node — shutdown does not consult the
            # removal-budget guard. The web API layer still calls it.
            patch.object(sno.tasks_controller, "get_active_node_restart_task",
                         return_value=None),
            patch.object(sno.tasks_controller, "get_active_node_tasks",
                         return_value=[]),
            patch.object(sno, "set_node_status", _set_status),
            patch.object(sno.device_controller, "device_set_unavailable",
                         _dev_set_unavail),
            patch.object(sno.device_controller, "set_jm_device_state", _set_jm),
            patch.object(sno, "trigger_ana_failover_for_node",
                         lambda *_a, **_kw: None),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        fw_patch = patch.object(sno, "FirewallClient")  # MUST NOT be used
        fw_mock = fw_patch.start()
        self.addCleanup(fw_patch.stop)

        return {
            "snode": snode, "peer": peer, "db": db,
            "detach_calls": detach_calls, "kill_calls": kill_calls,
            "dev_unavail": device_unavailable_calls,
            "set_jm": set_jm_calls, "status_changes": status_changes,
            "fw_mock": fw_mock,
        }

    def test_graceful_runs_both_loops_then_kills(self):
        from simplyblock_core import storage_node_ops as sno
        env = self._patch()
        self.assertTrue(sno.shutdown_storage_node(env["snode"].get_id(),
                                                  force=False))

        # Loop 1: every nvme device + the JM device were marked unavailable.
        self.assertEqual(len(env["dev_unavail"]), 2)
        self.assertEqual(len(env["set_jm"]), 1)

        # Loop 2: peer received a detach for each remote ctrlr referencing
        # the dying node (1 remote_alceml + 1 remote_jm).
        self.assertEqual(len(env["detach_calls"]), 2)

        # Step 5: SPDK kill exactly once.
        self.assertEqual(len(env["kill_calls"]), 1)

        # Step 6: status -> offline at the end. (in_shutdown was set in
        # step 1, offline in step 6.)
        statuses = [s for _, s in env["status_changes"]]
        self.assertIn(StorageNode.STATUS_IN_SHUTDOWN, statuses)
        self.assertIn(StorageNode.STATUS_OFFLINE, statuses)
        self.assertEqual(statuses[0], StorageNode.STATUS_IN_SHUTDOWN)
        self.assertEqual(statuses[-1], StorageNode.STATUS_OFFLINE)

    def test_graceful_never_invokes_firewall(self):
        """The suspension phase is removed; no FirewallClient call must
        happen during graceful shutdown."""
        from simplyblock_core import storage_node_ops as sno
        env = self._patch()
        sno.shutdown_storage_node(env["snode"].get_id(), force=False)
        env["fw_mock"].assert_not_called()

    def test_force_skips_loops(self):
        """--force keeps the legacy "go straight to kill" semantics:
        no Loop 1, no Loop 2 (peers find out via TCP drops)."""
        from simplyblock_core import storage_node_ops as sno
        env = self._patch()
        sno.shutdown_storage_node(env["snode"].get_id(), force=True)

        self.assertEqual(env["dev_unavail"], [])
        self.assertEqual(env["set_jm"], [])
        self.assertEqual(env["detach_calls"], [])
        # Kill still happens.
        self.assertEqual(len(env["kill_calls"]), 1)

    def test_shutdown_does_not_consult_ftt_removal_guard(self):
        # Regression guard: shutdown_storage_node must not call
        # _check_ftt_allows_node_removal. Removal and shutdown are
        # different operations; shutdown is supposed to proceed under
        # the FTT contract, not be blocked by a removal-budget check.
        # See commit removing the fbdffea3 guard from this path.
        from simplyblock_core import storage_node_ops as sno
        env = self._patch()
        with patch.object(sno, "_check_ftt_allows_node_removal") as guard:
            self.assertTrue(
                sno.shutdown_storage_node(env["snode"].get_id(), force=False))
            guard.assert_not_called()


if __name__ == '__main__':
    unittest.main()
