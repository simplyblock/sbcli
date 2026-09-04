"""test_dial_backoff.py — repeated refusals earn a peer address a hold.

Run mass_create_delete_docker-20260821: a node's SPDK was dead for hours while
its DB record said ONLINE, so status gates let every repair path keep dialling
its addresses. The connection-refused storm wedged a healthy peer's app thread
until its own RPC port stopped answering and the monitor declared *it* dead —
the cascade that suspended the cluster. The record was wrong; only the dial
failures themselves carried the truth, hence a per-address circuit breaker.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.utils import dial_backoff


class TestDialBackoff(unittest.TestCase):

    def setUp(self):
        dial_backoff._state.clear()

    def test_fresh_address_is_allowed(self):
        self.assertTrue(dial_backoff.allowed("10.0.0.1"))

    def test_first_failures_are_free(self):
        """Transient hiccups (a restart racing a reconnect) must not delay
        repair — only a streak earns a hold."""
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD - 1):
            dial_backoff.record_failure("10.0.0.1")
        self.assertTrue(dial_backoff.allowed("10.0.0.1"))

    def test_a_streak_earns_a_hold(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD):
            dial_backoff.record_failure("10.0.0.1")
        self.assertFalse(dial_backoff.allowed("10.0.0.1"))
        self.assertIn("10.0.0.1", dial_backoff.held_keys())

    def test_hold_expires(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD):
            dial_backoff.record_failure("10.0.0.1")
        with patch.object(dial_backoff.time, "monotonic",
                          return_value=dial_backoff.time.monotonic()
                          + dial_backoff.BASE_HOLD_SEC + 1):
            self.assertTrue(dial_backoff.allowed("10.0.0.1"))

    def test_hold_doubles_and_caps(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD + 20):
            dial_backoff.record_failure("10.0.0.1")
        remaining = dial_backoff._state["10.0.0.1"][1] - dial_backoff.time.monotonic()
        self.assertLessEqual(remaining, dial_backoff.MAX_HOLD_SEC + 1)
        self.assertGreater(remaining, dial_backoff.MAX_HOLD_SEC * 0.9,
                           "a long streak should sit at the ceiling")

    def test_one_success_clears_everything(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD + 5):
            dial_backoff.record_failure("10.0.0.1")
        dial_backoff.record_success("10.0.0.1")
        self.assertTrue(dial_backoff.allowed("10.0.0.1"))
        self.assertEqual(dial_backoff.held_keys(), [])

    def test_addresses_are_independent(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD):
            dial_backoff.record_failure("10.0.0.1")
        self.assertTrue(dial_backoff.allowed("10.0.0.2"),
                        "one dead peer must not hold dials to healthy peers")


class TestRepairHonoursTheHold(unittest.TestCase):
    """repair_multipath_controller must skip a held address without dialling."""

    def setUp(self):
        dial_backoff._state.clear()

    def _run_repair(self):
        from simplyblock_core import storage_node_ops as ops

        device = MagicMock()
        device.nvmf_multipath = True
        device.nvmf_ip = "10.0.0.1,10.0.0.2"
        device.nvmf_nqn = "nqn:test"
        device.nvmf_port = 4420
        device.node_id = "owner-1"

        node = MagicMock()
        rpc = MagicMock()
        # one path attached (10.0.0.2); 10.0.0.1 missing
        rpc.bdev_nvme_controller_list.return_value = [
            {"ctrlrs": [{"state": "enabled", "trid": {"traddr": "10.0.0.2"}}]}]
        rpc.bdev_nvme_attach_controller.return_value = None   # refused
        node.rpc_client.return_value = rpc

        owner = MagicMock()
        owner.active_rdma = False
        owner.active_tcp = True
        with patch.object(ops, "DBController") as db:
            db.return_value.get_storage_node_by_id.return_value = owner
            ops.repair_multipath_controller("remote_x", device, node)
        return rpc.bdev_nvme_attach_controller.call_count

    def test_failures_accumulate_then_dials_stop(self):
        dials = 0
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD):
            dials += self._run_repair()
        self.assertEqual(dials, dial_backoff.FAILURES_BEFORE_HOLD)
        # the streak is complete: the next repair must not dial at all
        self.assertEqual(self._run_repair(), 0,
                         "a held address was dialled anyway")

    def test_success_resets_the_breaker(self):
        for _ in range(dial_backoff.FAILURES_BEFORE_HOLD - 1):
            self._run_repair()
        dial_backoff.record_success("10.0.0.1")
        self.assertEqual(self._run_repair(), 1)


if __name__ == "__main__":
    unittest.main()
