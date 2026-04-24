# coding=utf-8
"""Pin the hublvol reconnect coordinator invariants.

The coordinator is the single entry point for
``bdev_nvme_attach_controller`` / ``bdev_nvme_detach_controller`` on
hublvol subsystems. It enforces three things:

1. Only one attach/detach for a given ``(node_id, lvstore)`` at a time
   (serialization). In tests there is no FDB, so the coordinator falls
   back to a process-local per-key ``threading.Lock``; that path is
   still a strict mutex.
2. A cooldown between attach attempts — a second caller arriving inside
   the window either observes an already-enabled controller and
   returns immediately, or sleeps out the rest of the cooldown before
   acting. This removes the
   "``bdev_nvme_check_multipath: cntlid N are duplicated``" race where
   a second attach lands while SPDK's async
   ``nvme_ctrlr_destruct_poll_async`` is still running on the prior
   controller.
3. A detach-and-wait-gone before any re-attach when the currently-
   attached controller is in any non-enabled state — detach alone is
   asynchronous in SPDK, so issuing a new attach immediately after
   ``bdev_nvme_detach_controller`` is precisely the race we're closing.
"""
import threading
import time
import unittest
from unittest.mock import MagicMock

from simplyblock_core.utils import hublvol_reconnect


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_peer(node_id, ip):
    peer = MagicMock()
    peer.get_id.return_value = node_id
    peer.active_rdma = False
    peer.active_tcp = True
    iface = MagicMock()
    iface.trtype = "TCP"
    iface.ip4_address = ip
    peer.data_nics = [iface]
    return peer


def _make_primary(lvstore="LVS_1", bdev="LVS_1/hublvol",
                   nqn="nqn.test:hublvol:LVS_1", port=4437, ip="10.0.0.1"):
    primary = _make_peer("primary", ip)
    primary.lvstore = lvstore
    primary.hublvol = MagicMock()
    primary.hublvol.bdev_name = bdev
    primary.hublvol.nqn = nqn
    primary.hublvol.nvmf_port = port
    return primary


def _make_node(node_id="sec", rpc=None):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.rpc_client.return_value = rpc or MagicMock()
    return node


def _fresh_process_state():
    """Wipe the module-level in-process lock + state dicts so a test's
    (node,lvs) key doesn't inherit a cooldown stamp from a previous test."""
    hublvol_reconnect._process_local_locks.clear()
    hublvol_reconnect._process_local_state.clear()


def _coordinator(cooldown_sec=0.0):
    """A coordinator with no FDB (tests) and a configurable cooldown."""
    db = MagicMock()
    db.kv_store = None  # forces the process-local lock path
    return hublvol_reconnect.HublvolReconnectCoordinator(
        db, cooldown_sec=cooldown_sec)


# ---------------------------------------------------------------------------
# Basic attach / observe paths
# ---------------------------------------------------------------------------

class NoExistingController(unittest.TestCase):
    """If no controller exists, the coordinator does a fresh multipath
    attach of every expected peer path — no detach."""

    def setUp(self):
        _fresh_process_state()

    def test_fresh_attach_calls_attach_per_peer_ip_and_no_detach(self):
        rpc = MagicMock()
        # Two successive list calls: first (observe) returns empty,
        # second (verify after attach) returns enabled with the attached IP.
        rpc.bdev_nvme_controller_list.side_effect = [
            None,
            [{"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}],
        ]
        rpc.bdev_nvme_attach_controller.return_value = ["LVS_1/hublvoln1"]
        node = _make_node(rpc=rpc)

        coord = _coordinator()
        ok = coord.reconcile(node, _make_primary(), [_make_primary()],
                             role="secondary")

        self.assertTrue(ok)
        self.assertEqual(rpc.bdev_nvme_attach_controller.call_count, 1)
        self.assertEqual(rpc.bdev_nvme_detach_controller.call_count, 0,
                         "no detach when no prior controller exists")

    def test_attach_passes_hublvol_ctrlr_timeouts(self):
        """Hublvol attaches must carry bumped ctrlr_loss / reconnect_delay /
        fast_io_fail timeouts so a short peer blip is absorbed by the
        reset window rather than destroying the controller (which is
        what opens the cntlid-duplicated race)."""
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.side_effect = [
            None,
            [{"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}],
        ]
        rpc.bdev_nvme_attach_controller.return_value = ["x"]
        node = _make_node(rpc=rpc)

        coord = _coordinator()
        coord.reconcile(node, _make_primary(), [_make_primary()])

        kwargs = rpc.bdev_nvme_attach_controller.call_args.kwargs
        self.assertEqual(
            kwargs["ctrlr_loss_timeout_sec"],
            hublvol_reconnect.HUBLVOL_CTRLR_LOSS_TIMEOUT_SEC)
        self.assertEqual(
            kwargs["reconnect_delay_sec"],
            hublvol_reconnect.HUBLVOL_RECONNECT_DELAY_SEC)
        self.assertEqual(
            kwargs["fast_io_fail_timeout_sec"],
            hublvol_reconnect.HUBLVOL_FAST_IO_FAIL_TIMEOUT_SEC)


class ExistingEnabledController(unittest.TestCase):
    """An already-enabled controller with the expected paths is a no-op.
    A missing peer path is topped up without tearing the controller
    down."""

    def setUp(self):
        _fresh_process_state()

    def test_fully_enabled_is_noop(self):
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = [{
            "ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}],
        }]
        node = _make_node(rpc=rpc)

        coord = _coordinator()
        ok = coord.reconcile(node, _make_primary(), [_make_primary()])

        self.assertTrue(ok)
        self.assertEqual(rpc.bdev_nvme_attach_controller.call_count, 0,
                         "attach must not be called when state is enabled "
                         "and all expected paths are present")
        self.assertEqual(rpc.bdev_nvme_detach_controller.call_count, 0)

    def test_missing_peer_path_is_topped_up_not_rebuilt(self):
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = [{
            "ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}],
        }]
        rpc.bdev_nvme_attach_controller.return_value = ["x"]
        node = _make_node(rpc=rpc)

        primary = _make_primary(ip="10.0.0.1")
        failover = _make_peer("fo", "10.0.0.2")

        coord = _coordinator()
        ok = coord.reconcile(node, primary, [primary, failover])

        self.assertTrue(ok)
        self.assertEqual(rpc.bdev_nvme_detach_controller.call_count, 0,
                         "healthy controller must not be torn down to add "
                         "a missing peer path")
        self.assertEqual(rpc.bdev_nvme_attach_controller.call_count, 1,
                         "one attach call for the missing peer path")


# ---------------------------------------------------------------------------
# Non-enabled state → detach-and-wait-gone, then attach
# ---------------------------------------------------------------------------

class DetachAndWaitGoneBeforeReattach(unittest.TestCase):
    """Any non-enabled state on the existing controller forces a detach,
    and the coordinator must wait for ``bdev_nvme_controller_list`` to
    report the controller absent before issuing a new attach — otherwise
    we re-race SPDK's async destroy."""

    def setUp(self):
        _fresh_process_state()

    def test_failed_state_triggers_detach_and_polls_until_absent(self):
        rpc = MagicMock()
        # Observed state: failed (settled non-enabled).
        # Then detach-wait polls: first call still shows the ctrlr,
        # second shows it gone.
        # Final observation after attach: enabled.
        observed = [
            [{"ctrlrs": [{"state": "failed", "trid": {"traddr": "10.0.0.1"}}]}],
            [{"ctrlrs": [{"state": "failed", "trid": {"traddr": "10.0.0.1"}}]}],
            None,
            [{"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}],
        ]
        rpc.bdev_nvme_controller_list.side_effect = observed
        rpc.bdev_nvme_attach_controller.return_value = ["x"]
        node = _make_node(rpc=rpc)

        coord = _coordinator()
        ok = coord.reconcile(node, _make_primary(), [_make_primary()])

        self.assertTrue(ok)
        self.assertEqual(rpc.bdev_nvme_detach_controller.call_count, 1,
                         "failed state must trigger exactly one detach")
        # The attach must have happened AFTER the list showed the ctrlr absent.
        # We can check that by asserting >= 2 list calls before the attach
        # (one observe, one wait-gone poll that still sees it, one that sees
        # it gone). call_count on list should reach at least 3 before attach.
        self.assertGreaterEqual(rpc.bdev_nvme_controller_list.call_count, 3)
        self.assertEqual(rpc.bdev_nvme_attach_controller.call_count, 1)

    def test_detach_wait_timeout_returns_false_without_attaching(self):
        """If the controller never goes away, the coordinator must give up
        (return False) rather than issue an attach that will race SPDK's
        still-running destroy."""
        rpc = MagicMock()
        # Always observe "failed" — never absent.
        rpc.bdev_nvme_controller_list.return_value = [{
            "ctrlrs": [{"state": "failed", "trid": {"traddr": "10.0.0.1"}}],
        }]
        node = _make_node(rpc=rpc)

        coord = _coordinator()
        # Patch the wait-gone timeout to something tiny so the test runs fast.
        orig = hublvol_reconnect.DEFAULT_DETACH_WAIT_SEC
        try:
            hublvol_reconnect.DEFAULT_DETACH_WAIT_SEC = 0.2
            ok = coord.reconcile(node, _make_primary(), [_make_primary()])
        finally:
            hublvol_reconnect.DEFAULT_DETACH_WAIT_SEC = orig

        self.assertFalse(ok)
        self.assertEqual(rpc.bdev_nvme_attach_controller.call_count, 0,
                         "never attach while SPDK still has the old ctrlr")


# ---------------------------------------------------------------------------
# Serialization — the actual race-prevention contract
# ---------------------------------------------------------------------------

class SerializationAcrossThreads(unittest.TestCase):
    """Two threads calling reconcile for the same (node, lvs) must not
    overlap inside the attach/detach critical section."""

    def setUp(self):
        _fresh_process_state()

    def test_two_threads_serialize_on_same_subsystem(self):
        # Track overlap: ``inflight`` increments on entry, decrements on
        # exit; we assert the peak is 1.
        inflight = {"n": 0, "peak": 0}
        seen_lock = threading.Lock()

        def list_side_effect(*a, **kw):
            with seen_lock:
                inflight["n"] += 1
                inflight["peak"] = max(inflight["peak"], inflight["n"])
            # Let the other thread catch up so any concurrency shows up.
            time.sleep(0.02)
            with seen_lock:
                inflight["n"] -= 1
            return [{"ctrlrs": [
                {"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}]

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.side_effect = list_side_effect
        node = _make_node(rpc=rpc)

        coord = _coordinator()

        def _do():
            coord.reconcile(node, _make_primary(), [_make_primary()])

        t1 = threading.Thread(target=_do)
        t2 = threading.Thread(target=_do)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(
            inflight["peak"], 1,
            "two reconcile calls for the same (node, lvs) must serialize; "
            "a peak of 2 means both threads were inside the critical section "
            "at the same time — that's the race we're closing")


# ---------------------------------------------------------------------------
# Cooldown — coalesces a second caller arriving just after the first
# ---------------------------------------------------------------------------

class CooldownCoalescesRapidCalls(unittest.TestCase):
    """A second caller inside the cooldown window, seeing an enabled
    controller with all expected peer paths, must return immediately
    without issuing RPCs."""

    def setUp(self):
        _fresh_process_state()

    def test_second_call_is_noop_inside_cooldown(self):
        rpc = MagicMock()
        # First call: no ctrlr → attach → verify enabled.
        # Second call: the controller is already enabled with the peer path.
        rpc.bdev_nvme_controller_list.side_effect = [
            None,
            [{"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}],
            [{"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.1"}}]}],
        ]
        rpc.bdev_nvme_attach_controller.return_value = ["x"]
        node = _make_node(rpc=rpc)

        coord = _coordinator(cooldown_sec=10.0)
        coord.reconcile(node, _make_primary(), [_make_primary()])
        # Second call arrives immediately — within the cooldown.
        coord.reconcile(node, _make_primary(), [_make_primary()])

        self.assertEqual(
            rpc.bdev_nvme_attach_controller.call_count, 1,
            "second caller inside cooldown must coalesce to a no-op; "
            "issuing a second attach is how the duplicate-cntlid race "
            "opens")


if __name__ == "__main__":
    unittest.main()
