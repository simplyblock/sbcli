"""Partial (delta) snapshot replication, control-plane side.

A delta transfer sends only the blocks the source's leader saw written since
the predecessor. These tests pin what the control plane must establish before
it asks for one, and that every doubt falls back to a full transfer.
"""
import unittest
from types import SimpleNamespace
from typing import ClassVar
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
                patch.object(snapshot_replication, "_finishing_predecessor", return_value=None), \
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


class TestOverlapWithMerge(unittest.TestCase):
    """The next transfer of a volume runs while the target still merges the
    previous copy: its delta is taken against that predecessor, and its
    landing volume is chained onto the predecessor's copy only in the finish,
    once that copy exists."""

    def _decide(self, pred, sec_status=None):
        task = SimpleNamespace(function_params={}, write_to_db=MagicMock(), uuid="t")
        remote_lv = SimpleNamespace(get_id=lambda: "landing")
        node = SimpleNamespace(secondary_node_id="sec" if sec_status else "",
                               get_id=lambda: "tgt")
        db = MagicMock()
        db.get_storage_node_by_id.return_value = SimpleNamespace(status=sec_status)
        prechain = MagicMock()
        with patch.object(snapshot_replication, "_finishing_predecessor", return_value=pred), \
                patch.object(snapshot_replication, "_prechain_landing_volume", prechain), \
                patch.object(snapshot_replication, "db", db):
            res = snapshot_replication._partial_transfer_decision(
                task, _snap(after=CLEAN), False, remote_lv, node, "n1")
        prechain.assert_not_called()        # nothing to chain onto yet
        return res, task.function_params

    def test_a_delta_on_the_merging_predecessor(self):
        (res, params) = self._decide(_snap(before=CLEAN, blobid=0x88), sec_status="online")
        self.assertEqual(res, (True, 0x88))
        self.assertEqual(params["chain_deferred_to"], "snap")

    def test_an_offline_secondary_makes_it_full_but_still_deferred(self):
        (res, params) = self._decide(_snap(before=CLEAN, blobid=0x88), sec_status="offline")
        self.assertEqual(res, (False, 0))
        self.assertEqual(params["chain_deferred_to"], "snap")

    def test_a_dirty_basis_makes_it_full(self):
        (res, _) = self._decide(_snap(before={"n1": [0, "t0"], "n2": [9, "t0"]}))
        self.assertEqual(res, (False, 0))

    def _gate(self, pred, chain=None, live=True):
        task = SimpleNamespace(function_params={"chain_deferred_to": "pred"}, uuid="t")
        db = MagicMock()
        if pred is None:
            db.get_snapshot_by_id.side_effect = KeyError("pred")
        else:
            db.get_snapshot_by_id.return_value = pred
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_previous_replicated_snapshot",
                             return_value=chain), \
                patch.object(snapshot_replication, "_live_replication_task",
                             return_value=object() if live else None):
            return snapshot_replication._deferred_chain_gate(task, _snap(), False)

    @staticmethod
    def _pred(copy="", status="online"):
        return SimpleNamespace(target_replicated_snap_uuid=copy, status=status,
                               cluster_id="cl", get_id=lambda: "pred")

    def test_the_finish_waits_for_the_predecessors_copy(self):
        self.assertEqual(self._gate(self._pred()), "pending")

    def test_the_finish_chains_once_the_copy_exists(self):
        self.assertEqual(self._gate(self._pred("copy"), chain=self._pred("copy")), "ok")

    def test_a_different_chain_base_starts_over(self):
        other = SimpleNamespace(get_id=lambda: "older")
        self.assertEqual(self._gate(self._pred("copy"), chain=other), "restart")

    def test_a_predecessor_that_will_not_replicate_starts_over(self):
        self.assertEqual(self._gate(self._pred(), live=False), "restart")
        self.assertEqual(self._gate(self._pred(status="in_deletion")), "restart")
        self.assertEqual(self._gate(None), "restart")

    def test_no_deferred_chain_passes(self):
        task = SimpleNamespace(function_params={}, uuid="t")
        self.assertEqual(snapshot_replication._deferred_chain_gate(task, _snap(), False), "ok")

    def _walk(self, completed):
        base = SimpleNamespace(snap_bdev="LVS/SNAP1", target_replicated_snap_uuid="",
                               status="online", get_id=lambda: "s1")
        snap = SimpleNamespace(snap_bdev="LVS/SNAP2", lvol=SimpleNamespace(node_id="n0"))
        node = MagicMock()
        node.rpc_client.return_value.bdev_get.side_effect = [
            {"driver_specific": {"lvol": {"base_snapshot": "SNAP1"}}},
            {"driver_specific": {"lvol": {}}}]
        db = MagicMock()
        db.get_snapshots_by_node_id.return_value = [base]
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_transfer_completed", return_value=completed):
            verdict, rec, _ = snapshot_replication._unreplicated_local_ancestor(node, snap, False)
        return verdict, rec

    def test_a_base_that_has_arrived_does_not_hold_the_transfer(self):
        self.assertEqual(self._walk(True), ("ok", None))

    def test_a_base_still_transferring_goes_first(self):
        verdict, rec = self._walk(False)
        self.assertEqual(verdict, "pending")
        self.assertEqual(rec.get_id(), "s1")


    def test_a_landing_chained_by_an_earlier_pass_is_recognised(self):
        """The finish chains, then waits for the merge over several passes;
        add_clone again fails with "File exists" (bed run 4)."""
        node = MagicMock()
        node.rpc_client.return_value.bdev_get.return_value = {
            "driver_specific": {"lvol": {"base_snapshot": "LVOL_59"}}}
        self.assertTrue(snapshot_replication._chained_onto(node, "LVS_25/LVOL_61", "LVS_25/LVOL_59"))
        self.assertFalse(snapshot_replication._chained_onto(node, "LVS_25/LVOL_61", "LVS_25/LVOL_55"))
        node.rpc_client.return_value.bdev_get.return_value = [{"driver_specific": {"lvol": {}}}]
        self.assertFalse(snapshot_replication._chained_onto(node, "LVS_25/LVOL_61", "LVS_25/LVOL_59"))

    def test_a_chain_is_recorded_once(self):
        task = SimpleNamespace(function_params={}, write_to_db=MagicMock())
        node = SimpleNamespace(get_id=lambda: "n1")
        snapshot_replication._record_chained(task, node)
        snapshot_replication._record_chained(task, node)
        self.assertEqual(task.function_params["prechained_node_ids"], ["n1"])
        task.write_to_db.assert_called_once()

class TestLandingMerge(unittest.TestCase):
    """After a delta, the landing volume's pre-allocated clusters hold only the
    delta's blocks; the rest comes from the base before the convert, as a job
    on the target that the finish starts and then waits for across passes."""

    #: what bdev_lvol_merge_base_stat answers before any job ran
    NONE: ClassVar[dict] = {"state": "none", "rc": 0}

    def _merge(self, params, start=None, stats=(NONE,), side_effect=None):
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
                                     stats=[self.NONE, {"state": "done", "rc": 0, "clusters": 3}])
        self.assertEqual(res, "ok")
        rpc.bdev_lvol_merge_base.assert_called_once_with("LVS/LAND")
        self.assertEqual(task.function_params["merged_landing"], "landing")
        res, rpc, _ = self._merge(dict(task.function_params))
        self.assertEqual(res, "ok")
        rpc.bdev_lvol_merge_base.assert_not_called()

    def test_a_long_merge_is_left_to_the_next_pass(self):
        res, _, task = self._merge({"partial_base": 7}, start={"state": "running"},
                                   stats=[self.NONE] + [{"state": "running"}] * 50)
        self.assertEqual(res, "pending")
        self.assertNotIn("merged_landing", task.function_params)

    def test_a_lost_record_repeats_the_transfer(self):
        res, _, _ = self._merge({"partial_base": 7}, start={"state": "failed", "rc": -116})
        self.assertEqual(res, "restart")

    def test_a_lost_record_is_seen_before_a_relaunch(self):
        # SPDK relaunches a failed job and answers "running"; the relaunch
        # fails again at once. Starting first hid the -ESTALE on every pass.
        res, rpc, _ = self._merge({"partial_base": 7}, start={"state": "running"},
                                  stats=[{"state": "failed", "rc": -116}])
        self.assertEqual(res, "restart")
        rpc.bdev_lvol_merge_base.assert_not_called()

    def test_a_running_merge_is_not_started_again(self):
        res, rpc, _ = self._merge({"partial_base": 7}, stats=[{"state": "running"}] * 50)
        self.assertEqual(res, "pending")
        rpc.bdev_lvol_merge_base.assert_not_called()

    def test_a_failed_merge_is_started_again(self):
        res, rpc, _ = self._merge({"partial_base": 7}, start={"state": "running"},
                                  stats=[{"state": "failed", "rc": -5}] + [{"state": "running"}] * 50)
        self.assertEqual(res, "pending")
        rpc.bdev_lvol_merge_base.assert_called_once_with("LVS/LAND")

    def test_other_failures_retry_the_finish(self):
        from simplyblock_core.rpc_client import RPCRemoteError
        res, _, _ = self._merge({"partial_base": 7}, start={"state": "failed", "rc": -5})
        self.assertEqual(res, "retry")
        res, _, _ = self._merge({"partial_base": 7},
                                side_effect=RPCRemoteError("No such device", -19))
        self.assertEqual(res, "retry")



class TestMergeHandOff(unittest.TestCase):
    """The merges of a volume run one after another; the runner must not
    leave the target idle between them (bed run 4: ~50 s per cycle)."""

    def _task(self, snap_id, **params):
        from simplyblock_core.models.job_schedule import JobSchedule
        return SimpleNamespace(function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                               status=JobSchedule.STATUS_RUNNING, canceled=False, uuid=snap_id,
                               cluster_id="cl", function_params={"snapshot_id": snap_id, **params})

    def test_the_successor_is_finished_as_soon_as_the_copy_exists(self):
        done = self._task("s1")
        waiting = self._task("s2", chain_deferred_to="s1", transfer_done=True)
        other = self._task("s3", chain_deferred_to="s0", transfer_done=True)
        db = MagicMock()
        db.get_job_tasks.return_value = [done, other, waiting]
        db.get_snapshot_by_id.side_effect = lambda i: SimpleNamespace(get_id=lambda: i)
        with patch.object(snapshot_replication, "db", db),                 patch.object(snapshot_replication, "_resume_finish") as resume:
            snapshot_replication._hand_off_to_successor(done, SimpleNamespace(get_id=lambda: "s1"))
        resume.assert_called_once()
        self.assertIs(resume.call_args.args[0], waiting)
        self.assertEqual(resume.call_args.args[1].get_id(), "s2")

    def test_a_successor_still_transferring_is_left_alone(self):
        done = self._task("s1")
        waiting = self._task("s2", chain_deferred_to="s1")
        db = MagicMock()
        db.get_job_tasks.return_value = [done, waiting]
        with patch.object(snapshot_replication, "db", db),                 patch.object(snapshot_replication, "_resume_finish") as resume:
            snapshot_replication._hand_off_to_successor(done, SimpleNamespace(get_id=lambda: "s1"))
        resume.assert_not_called()

    def test_the_runner_passes_fast_while_replication_is_in_flight(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        running = self._task("s1")
        idle = self._task("s2")
        idle.status = JobSchedule.STATUS_DONE
        self.assertTrue(snapshot_replication._replication_in_flight([idle, running]))
        self.assertFalse(snapshot_replication._replication_in_flight([idle]))
        c = snapshot_replication.constants
        self.assertLess(c.REPL_ACTIVE_PASS_SEC, c.TASK_EXEC_INTERVAL_SEC)
        self.assertEqual(c.REPL_MERGE_INLINE_WAIT_SEC, 0)

if __name__ == "__main__":
    unittest.main()
