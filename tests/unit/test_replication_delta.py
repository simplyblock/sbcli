"""Partial (delta) snapshot replication, control-plane side.

A delta transfer sends only the blocks the source's leader saw written since
the predecessor. These tests pin what the control plane must establish before
it asks for one, and that every doubt falls back to a full transfer.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import replication_delta
from simplyblock_core.services import snapshot_replication


def _snap(before=None, after=None, blobid=0x1001):
    return SimpleNamespace(local_writes_before=before or {},
                           local_writes_after=after or {}, blobid=blobid,
                           get_id=lambda: "snap")


CLEAN = {"n1": [0, "t0"], "n2": [3, "t0"]}


class TestDeltaBasis(unittest.TestCase):

    def test_unchanged_counts_allow_a_delta(self):
        self.assertEqual(replication_delta.delta_basis(_snap(before=CLEAN), _snap(after=CLEAN), "n1"),
                         (True, ""))

    def test_missing_records_rule_it_out(self):
        self.assertFalse(replication_delta.delta_basis(_snap(), _snap(after=CLEAN), "n1")[0])
        self.assertFalse(replication_delta.delta_basis(_snap(before=CLEAN), _snap(), "n1")[0])

    def test_the_transferring_nodes_own_writes_are_in_its_bitmap(self):
        self.assertEqual(replication_delta.delta_basis(
            _snap(before=CLEAN), _snap(after={"n1": [900, "t0"], "n2": [3, "t0"]}), "n1"), (True, ""))

    def test_a_transferring_node_outside_the_lvstore_rules_it_out(self):
        self.assertFalse(replication_delta.delta_basis(
            _snap(before=CLEAN), _snap(after=CLEAN), "n9")[0])

    def test_a_write_on_another_member_rules_it_out(self):
        ok, why = replication_delta.delta_basis(
            _snap(before=CLEAN), _snap(after={"n1": [0, "t0"], "n2": [4, "t0"]}), "n1")
        self.assertFalse(ok)
        self.assertIn("n2", why)

    def test_a_restart_rules_it_out_even_with_equal_counts(self):
        # a restarted member counts from zero again: an equal count says nothing
        ok, why = replication_delta.delta_basis(
            _snap(before=CLEAN), _snap(after={"n1": [0, "t1"], "n2": [3, "t0"]}), "n2")
        self.assertFalse(ok)
        self.assertIn("restarted", why)

    def test_a_membership_change_rules_it_out(self):
        self.assertFalse(replication_delta.delta_basis(
            _snap(before=CLEAN), _snap(after={"n1": [0, "t0"]}), "n1")[0])


class TestWriteCounts(unittest.TestCase):

    def _db(self, infos):
        nodes = {}
        for i, info in enumerate(infos):
            node = MagicMock()
            node.get_id.return_value = f"n{i}"
            node.online_since = "t0"
            if isinstance(info, Exception):
                node.rpc_client.return_value.bdev_lvol_dirty_bitmap_info.side_effect = info
            else:
                node.rpc_client.return_value.bdev_lvol_dirty_bitmap_info.return_value = info
            nodes[f"n{i}"] = node
        nodes["n0"].secondary_node_id = "n1" if len(infos) > 1 else ""
        nodes["n0"].tertiary_node_id = "n2" if len(infos) > 2 else ""
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = nodes.__getitem__
        return db

    def _counts(self, infos):
        lvol = SimpleNamespace(node_id="n0", top_bdev="LVS/LV", get_id=lambda: "lv")
        with patch.object(replication_delta, "DBController", return_value=self._db(infos)):
            return replication_delta.local_write_counts(lvol)

    def test_every_member_is_read(self):
        info = {"lvs_local_writes": 5}
        self.assertEqual(self._counts([info, info, info]),
                         {"n0": [5, "t0"], "n1": [5, "t0"], "n2": [5, "t0"]})

    def test_an_unreadable_member_yields_no_record(self):
        info = {"lvs_local_writes": 5}
        self.assertEqual(self._counts([info, RuntimeError("down")]), {})
        # an SPDK without the counter
        self.assertEqual(self._counts([info, {"tracking": True}]), {})


class TestTransferRpc(unittest.TestCase):

    def _client(self, side_effect):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(side_effect=side_effect)
        return client

    def test_delta_is_sent_with_its_base(self):
        client = self._client([True])
        client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", allow_partial=True,
                                  partial_base=0x1001, priority_class=7)
        kw = client._request.call_args.kwargs
        self.assertTrue(kw["allow_partial"])
        self.assertEqual(kw["partial_base"], 0x1001)
        self.assertEqual(kw["priority_class"], 7)

    def test_no_base_no_delta(self):
        client = self._client([True])
        client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", allow_partial=True)
        self.assertNotIn("allow_partial", client._request.call_args.kwargs)

    def test_older_spdk_falls_back_to_full_then_to_own_class(self):
        from simplyblock_core.rpc_client import RPCRemoteError
        decode = RPCRemoteError("spdk_json_decode_object failed", -32603)
        client = self._client([decode, decode, True])
        self.assertTrue(client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate",
                                                  allow_partial=True, partial_base=1,
                                                  priority_class=7))
        calls = [c.kwargs for c in client._request.call_args_list]
        self.assertTrue(calls[0]["allow_partial"])
        self.assertNotIn("allow_partial", calls[1])
        self.assertEqual(calls[1]["priority_class"], 7)
        self.assertNotIn("priority_class", calls[2])


class TestPartialDecision(unittest.TestCase):

    def _decide(self, prechained, prev, snap=None):
        task = SimpleNamespace(function_params={}, write_to_db=MagicMock(), uuid="t")
        remote_lv = SimpleNamespace(get_id=lambda: "landing")
        with patch.object(snapshot_replication, "_prechain_landing_volume", return_value=prechained), \
                patch.object(snapshot_replication, "_previous_replicated_snapshot", return_value=prev):
            first = snapshot_replication._partial_transfer_decision(
                task, snap or _snap(after=CLEAN), False, remote_lv, None, "n1")
            again = snapshot_replication._partial_transfer_decision(
                task, snap or _snap(after=CLEAN), False, remote_lv, None, "n1")
        self.assertEqual(first, again)      # cached for a resumed attempt
        return first

    def test_clean_basis_sends_a_delta_on_the_predecessor(self):
        self.assertEqual(self._decide(True, _snap(before=CLEAN, blobid=0x77)), (True, 0x77))

    def test_unchained_landing_is_full(self):
        self.assertEqual(self._decide(False, _snap(before=CLEAN)), (False, 0))

    def test_a_non_leader_write_makes_it_full(self):
        self.assertEqual(self._decide(True, _snap(before={"n1": [0, "t0"], "n2": [2, "t0"]})),
                         (False, 0))

    def test_a_predecessor_without_blob_id_is_full(self):
        self.assertEqual(self._decide(True, _snap(before=CLEAN, blobid=0)), (False, 0))


class TestLandingMerge(unittest.TestCase):
    """After a delta, the landing volume's pre-allocated clusters hold only the
    delta's blocks; the rest comes from the base before the convert, as a job
    on the target that the finish starts and then waits for across passes."""

    def _merge(self, params, start=None, stats=(), side_effect=None):
        task = SimpleNamespace(function_params=params, write_to_db=MagicMock(), uuid="t")
        lv = SimpleNamespace(get_id=lambda: "landing", top_bdev="LVS/LAND")
        node = MagicMock()
        node.get_id.return_value = "n1"
        rpc = node.rpc_client.return_value
        rpc.bdev_lvol_merge_base.return_value = start
        rpc.bdev_lvol_merge_base.side_effect = side_effect
        rpc.bdev_lvol_merge_base_stat.side_effect = list(stats)
        with patch.object(snapshot_replication.constants, "REPL_MERGE_INLINE_WAIT_SEC", 0.05),                 patch.object(snapshot_replication.constants, "REPL_MERGE_POLL_SEC", 0.01):
            return snapshot_replication._merge_landing(task, lv, node), rpc, task

    def test_a_full_transfer_needs_no_merge(self):
        res, rpc, _ = self._merge({})
        self.assertEqual(res, "ok")
        rpc.bdev_lvol_merge_base.assert_not_called()

    def test_a_delta_is_merged_once(self):
        res, rpc, task = self._merge({"partial_base": 7}, start={"state": "running"},
                                     stats=[{"state": "done", "rc": 0, "clusters": 3}])
        self.assertEqual(res, "ok")
        rpc.bdev_lvol_merge_base.assert_called_once_with("LVS/LAND")
        self.assertEqual(task.function_params["merged_landing"], "landing")
        res, rpc, _ = self._merge(dict(task.function_params))
        self.assertEqual(res, "ok")
        rpc.bdev_lvol_merge_base.assert_not_called()

    def test_a_long_merge_is_left_to_the_next_pass(self):
        res, _, task = self._merge({"partial_base": 7}, start={"state": "running"},
                                   stats=[{"state": "running"}] * 50)
        self.assertEqual(res, "pending")
        self.assertNotIn("merged_landing", task.function_params)

    def test_a_lost_record_repeats_the_transfer(self):
        res, _, _ = self._merge({"partial_base": 7}, start={"state": "failed", "rc": -116})
        self.assertEqual(res, "restart")

    def test_other_failures_retry_the_finish(self):
        from simplyblock_core.rpc_client import RPCRemoteError
        res, _, _ = self._merge({"partial_base": 7}, start={"state": "failed", "rc": -5})
        self.assertEqual(res, "retry")
        res, _, _ = self._merge({"partial_base": 7},
                                side_effect=RPCRemoteError("No such device", -19))
        self.assertEqual(res, "retry")


if __name__ == "__main__":
    unittest.main()
