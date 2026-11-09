"""Cutover endgame rounds sent straight into the cutover clone (direct rounds).

bf18 (2026-11-09): a round replicated into a landing volume counted as done
only after the target merged it with its base, whole 2 MiB cluster by
cluster: 9.2-9.8 GB, 35-44 s per round of a randomly written 10 GiB volume at
a 4 MB/s source cap. Direct rounds send each round's dirty pages into the
cutover clone (dest_clone / cow_landing), with no landing volume and no merge.
"""
import inspect
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import replication_delta, snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCClient, RPCRemoteError
from simplyblock_core.services import replication_final_step, snapshot_replication
from simplyblock_core.services import tasks_runner_replication_final as final
from simplyblock_core.services.task_runner_base import TaskDefer


def _task(**params):
    t = JobSchedule()
    t.uuid = "F1"
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.function_params = {"lvol_id": "LV1", "tgt_node_id": "TN", "shrink_round": 1,
                         "shrink_snap_id": "S1", **params}
    t.status = JobSchedule.STATUS_RUNNING
    t.retry = 0
    t.max_retry = 8
    return t


def _record_inline(task, **params):
    task.function_params.update(params)
    return task


def _drop_inline(task, *names):
    for n in names:
        task.function_params.pop(n, None)
    return task


def _snap(sid, blobid=0x10, name="repl_round_x"):
    s = MagicMock()
    s.get_id.return_value = sid
    s.blobid = blobid
    s.snap_bdev = f"LVS_1/SNAP_{sid}"
    s.snap_name = name
    s.cluster_id = "CL"
    s.target_replicated_snap_uuid = ""
    s.status = "online"
    return s


def _lvol():
    lv = MagicMock()
    lv.get_id.return_value = "LV1"
    return lv


PREPARED = dict(early_prepared=True, tgt_lvol_composite="LVS_9/LVOL_1", tgt_map_id=7,
                tgt_snap_id="COPY0")


class TestMode:

    def test_possible_with_an_early_clone_and_its_base(self):
        with patch.object(final, "_clone_base_source", return_value=_snap("S0")), \
                patch.object(constants, "REPL_CUTOVER_DIRECT_ROUNDS", True):
            assert final._direct_mode_possible(_lvol(), dict(PREPARED))

    def test_not_without_the_early_clone(self):
        with patch.object(final, "_clone_base_source", return_value=_snap("S0")):
            assert not final._direct_mode_possible(_lvol(), {})

    def test_not_when_switched_off(self):
        with patch.object(final, "_clone_base_source", return_value=_snap("S0")), \
                patch.object(constants, "REPL_CUTOVER_DIRECT_ROUNDS", False):
            assert not final._direct_mode_possible(_lvol(), dict(PREPARED))

    def test_not_without_a_known_clone_base(self):
        with patch.object(final, "_clone_base_source", return_value=None):
            assert not final._direct_mode_possible(_lvol(), dict(PREPARED))


def _submit_env(rpc, base, snap, delta_ok=True):
    src = MagicMock()
    src.get_id.return_value = "SRC"
    src.rpc_client.return_value = rpc
    db = MagicMock()
    db.get_snapshot_by_id.side_effect = lambda i: {"S1": snap, "S0": base}[i]
    return [
        patch.object(final, "_record", side_effect=_record_inline),
        patch.object(final, "drop_params", side_effect=_drop_inline),
        patch.object(final, "db", db),
        patch.object(final, "_clone_base_source", return_value=base),
        patch.object(final, "_cutover_source", return_value=(src, "")),
        patch.object(final, "_cutover_target", return_value=(MagicMock(), "")),
        patch.object(replication_final_step, "ensure_hub_attached",
                     return_value=("ctrl", "LVS_9/transferhubn1", None)),
        patch.object(replication_delta, "delta_basis",
                     return_value=(delta_ok, "" if delta_ok else "writes elsewhere")),
        patch.object(snapshot_replication, "_replication_class_on", return_value=True),
    ]


def _enter(patches):
    for p in patches:
        p.start()


def _leave(patches):
    for p in patches:
        p.stop()


class TestSubmit:

    def test_the_round_goes_into_the_clone_as_a_delta_on_the_clone_base(self):
        rpc = MagicMock()
        base, snap = _snap("S0", blobid=0x55), _snap("S1")
        env = _submit_env(rpc, base, snap)
        _enter(env)
        try:
            err, task = final._submit_direct_round(_task(**PREPARED), _lvol())
        finally:
            _leave(env)
        assert err is None
        kw = rpc.bdev_lvol_transfer.call_args.kwargs
        assert kw["name"] == snap.snap_bdev and kw["lvol_id"] == 7
        assert kw["bdev_name"] == "LVS_9/transferhubn1"
        assert kw["dest_clone"] is True
        assert kw["allow_partial"] is True and kw["partial_base"] == 0x55
        assert kw["priority_class"] == constants.QOS_REPLICATION_WIRE_CLASS
        assert task.function_params["direct_xfer_node"] == "SRC"
        assert task.function_params["direct_submits"] == 1

    def test_later_rounds_are_deltas_on_the_last_arrived_round(self):
        rpc = MagicMock()
        prev, snap = _snap("S0", blobid=0x77), _snap("S1")
        env = _submit_env(rpc, prev, snap)
        _enter(env)
        try:
            final._submit_direct_round(_task(direct_done_snap="S0", **PREPARED), _lvol())
        finally:
            _leave(env)
        assert rpc.bdev_lvol_transfer.call_args.kwargs["partial_base"] == 0x77

    def test_without_a_delta_basis_whole_clusters_still_go_into_the_clone(self):
        rpc = MagicMock()
        env = _submit_env(rpc, _snap("S0"), _snap("S1"), delta_ok=False)
        _enter(env)
        try:
            final._submit_direct_round(_task(**PREPARED), _lvol())
        finally:
            _leave(env)
        kw = rpc.bdev_lvol_transfer.call_args.kwargs
        assert kw["allow_partial"] is False and kw["partial_base"] == 0
        assert kw["dest_clone"] is True

    def test_an_spdk_without_dest_clone_falls_back_to_ordinary_rounds(self):
        rpc = MagicMock()
        err = RPCRemoteError.__new__(RPCRemoteError)
        err.code = -32602
        err.args = ("spdk_json_decode_object failed",)
        rpc.bdev_lvol_transfer.side_effect = err
        snap = _snap("S1")
        snap.lvol.node_id = "SRC"
        env = _submit_env(rpc, _snap("S0"), snap)
        _enter(env)
        try:
            with patch("simplyblock_core.controllers.tasks_controller."
                       "add_snapshot_replication_task") as add:
                e, task = final._submit_direct_round(
                    _task(direct_rounds=True, **PREPARED), _lvol())
        finally:
            _leave(env)
        assert e is None
        assert task.function_params["direct_rounds"] is False
        add.assert_called_once_with("CL", "SRC", "S1")

    def test_a_clone_holding_direct_rounds_is_never_rebased(self):
        src = inspect.getsource(final._direct_unsupported)
        assert src.index('get("direct_done_snap")') < src.index("_abandon_endgame")


class TestDone:

    def _stat(self, state):
        node = MagicMock()
        node.rpc_client.return_value.bdev_lvol_transfer_stat.return_value = {
            "transfer_state": state}
        db = MagicMock()
        db.get_snapshot_by_id.return_value = _snap("S1")
        db.get_storage_node_by_id.return_value = node
        return db

    def test_done_records_the_round_as_arrived(self):
        task = _task(direct_rounds=True, direct_xfer_node="SRC", direct_submits=1)
        with patch.object(final, "db", self._stat("Done")), \
                patch.object(final, "_record", side_effect=_record_inline):
            done, task = final._direct_round_done(task, _lvol())
        assert done is True and task.function_params["direct_done_snap"] == "S1"

    def test_an_arrived_round_is_not_asked_again(self):
        task = _task(direct_rounds=True, direct_done_snap="S1")
        with patch.object(final, "db") as db:
            done, _ = final._direct_round_done(task, _lvol())
        assert done is True
        db.get_storage_node_by_id.assert_not_called()

    def test_in_progress_waits(self):
        task = _task(direct_rounds=True, direct_xfer_node="SRC", direct_submits=1)
        with patch.object(final, "db", self._stat("In progress")), \
                patch.object(final, "_submit_direct_round") as sub:
            done, _ = final._direct_round_done(task, _lvol())
        assert done is False
        sub.assert_not_called()

    def test_a_failed_transfer_is_sent_again(self):
        task = _task(direct_rounds=True, direct_xfer_node="SRC", direct_submits=1)
        with patch.object(final, "db", self._stat("Failed")), \
                patch.object(final, "_submit_direct_round",
                             return_value=(None, task)) as sub:
            done, _ = final._direct_round_done(task, _lvol())
        assert done is False
        sub.assert_called_once()

    def test_a_round_that_keeps_failing_abandons_the_endgame(self):
        task = _task(direct_rounds=True, direct_xfer_node="SRC",
                     direct_submits=constants.REPL_CUTOVER_DIRECT_ROUND_SUBMITS)
        with patch.object(final, "db", self._stat("Failed")), \
                patch.object(final, "_abandon_endgame",
                             side_effect=TaskDefer("abandoned")) as ab:
            with pytest.raises(TaskDefer):
                final._direct_round_done(task, _lvol())
        ab.assert_called_once()

    def test_ordinary_rounds_keep_their_check(self):
        task = _task()
        with patch.object(final, "_shrink_round_done", return_value=True) as legacy:
            done, _ = final._round_done(task, _lvol())
        assert done is True
        legacy.assert_called_once_with("S1")


class TestTakeRound:

    def test_the_mode_is_decided_once_and_the_snapshot_has_no_replication(self):
        task = _task(**PREPARED)
        task.function_params.pop("shrink_snap_id")
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch.object(final, "_direct_mode_possible", return_value=True), \
                patch.object(final, "_take_shrink_snapshot",
                             return_value=("S1", None, task)) as take, \
                patch.object(final, "_submit_direct_round",
                             return_value=(None, task)) as sub:
            err, task = final._take_round(task, _lvol())
        assert err is None
        assert take.call_args.kwargs == {"direct": True}
        sub.assert_called_once()
        assert task.function_params["direct_rounds"] is True
        assert task.function_params["direct_rounds_used"] is True

    def test_ordinary_rounds_are_taken_as_before(self):
        task = _task(direct_rounds=False)
        with patch.object(final, "_take_shrink_snapshot",
                          return_value=("S2", None, task)) as take, \
                patch.object(final, "_submit_direct_round") as sub:
            final._take_round(task, _lvol())
        assert take.call_args.kwargs == {}
        sub.assert_not_called()

    def test_direct_round_snapshots_have_their_own_names(self):
        assert final._new_shrink_snap_name(True).startswith(final.DIRECT_ROUND_PREFIX)
        assert not final._new_shrink_snap_name().startswith(final.DIRECT_ROUND_PREFIX)


class TestEnds:

    def test_an_attempt_without_a_cutover_requeues_its_round_snapshots(self):
        snaps = [_snap("R1"), _snap("R2")]
        for s in snaps:
            s.lvol.node_id = "SRC"
        task = _task(direct_rounds_used=True, cutover_lvs="LVS_1")
        with patch.object(final, "_direct_round_snapshots", return_value=snaps), \
                patch.object(final, "drop_params", side_effect=_drop_inline), \
                patch("simplyblock_core.controllers.tasks_controller."
                      "add_snapshot_replication_task") as add:
            final._release_lvs_claim(task)
        assert [c.args[2] for c in add.call_args_list] == ["R1", "R2"]
        assert "direct_rounds_used" not in task.function_params

    def test_nothing_to_requeue_without_direct_rounds(self):
        task = _task(cutover_lvs="LVS_1")
        with patch.object(final, "_direct_round_snapshots") as rounds, \
                patch.object(final, "drop_params", side_effect=_drop_inline):
            final._release_lvs_claim(task)
        rounds.assert_not_called()

    def test_a_cutover_deletes_its_round_snapshots_before_releasing(self):
        src = inspect.getsource(final._record_cutover_done)
        assert src.index("_delete_direct_rounds(task)") < src.index("_release_lvs_claim(task)")

    def test_an_unfinished_cutover_requeues(self):
        assert "_requeue_direct_rounds(task)" in inspect.getsource(
            final._cleanup_unfinished_cutover)

    def test_the_freeze_takes_the_last_round_as_its_delta_base(self):
        src = inspect.getsource(final.task_runner)
        assert "_direct_last_round(params) if params.get(\"direct_rounds\")" in src
        i = src.index('if params.get("direct_rounds"):')
        assert src.index("_round_done(task, lvol)", i) < src.index("_rebase_on_last_round", i)

    def test_the_clone_exists_before_the_first_round(self):
        src = inspect.getsource(final.task_runner)
        assert src.index("_prepare_early(task, lvol, src_node, tgt_node)") < \
            src.index("snap_err, task = _take_round(task, lvol)")


class TestEarlyForDirect:
    """bf19 (2026-11-09): REPL_CUTOVER_EARLY_PREPARE was off, so no clone
    existed before round 1 and no endgame ever went direct -- silently."""

    def test_direct_rounds_prepare_the_clone_even_with_early_prepare_off(self):
        src = inspect.getsource(final.task_runner)
        assert "early_for_direct = (not constants.REPL_CUTOVER_EARLY_PREPARE" in src
        assert "(constants.REPL_CUTOVER_EARLY_PREPARE or early_for_direct)" in src

    def test_wanted_only_when_enabled_for_a_replication_not_found_unsupported(self):
        with patch.object(constants, "REPL_CUTOVER_DIRECT_ROUNDS", True):
            assert final._direct_rounds_wanted({})
            assert not final._direct_rounds_wanted({"operation": "migrate"})
            assert not final._direct_rounds_wanted({"direct_unsupported": True})
        with patch.object(constants, "REPL_CUTOVER_DIRECT_ROUNDS", False):
            assert not final._direct_rounds_wanted({})

    def test_unsupported_for_the_task_means_no_direct_mode(self):
        with patch.object(final, "_clone_base_source", return_value=_snap("S0")), \
                patch.object(constants, "REPL_CUTOVER_DIRECT_ROUNDS", True):
            assert not final._direct_mode_possible(
                _lvol(), dict(PREPARED, direct_unsupported=True))

    def test_a_clone_for_direct_rounds_is_discarded_not_rebased(self):
        # the mode decision finds direct impossible: the endgame and its clone
        # are given up, and the task goes on without direct rounds
        task = _task(early_for_direct=True, **PREPARED)
        task.function_params.pop("shrink_snap_id")
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch.object(final, "_direct_mode_possible", return_value=False), \
                patch.object(final, "_abandon_prepared_cutover",
                             side_effect=lambda t: t) as discard, \
                patch.object(final, "_abandon_endgame",
                             side_effect=TaskDefer("abandoned")) as ab, \
                patch.object(final, "_take_shrink_snapshot") as take:
            with pytest.raises(TaskDefer):
                final._take_round(task, _lvol())
        assert task.function_params["direct_unsupported"] is True
        discard.assert_called_once()
        ab.assert_called_once()
        take.assert_not_called()

    def test_an_spdk_without_dest_clone_discards_a_clone_for_direct_rounds(self):
        task = _task(direct_rounds=True, early_for_direct=True, **PREPARED)
        snap = _snap("S1")
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch.object(final, "_abandon_prepared_cutover",
                             side_effect=lambda t: t) as discard, \
                patch.object(final, "_abandon_endgame",
                             side_effect=TaskDefer("abandoned")) as ab, \
                patch("simplyblock_core.controllers.tasks_controller."
                      "add_snapshot_replication_task") as add:
            with pytest.raises(TaskDefer):
                final._direct_unsupported(task, _lvol(), snap, "decode")
        assert task.function_params["direct_unsupported"] is True
        discard.assert_called_once()
        ab.assert_called_once()
        add.assert_not_called()      # the abandon re-queues the round snapshots

    def test_the_mode_is_logged_either_way(self):
        task = _task(direct_rounds=None)
        task.function_params.pop("direct_rounds")
        with patch.object(final, "_record", side_effect=_record_inline), \
                patch.object(final, "_direct_mode_possible", return_value=False), \
                patch.object(final, "_take_shrink_snapshot", return_value=("S2", None, task)), \
                patch.object(final.logger, "info") as info:
            final._take_round(task, _lvol())
        assert any("endgame rounds replicate into landing volumes" in (c.args[0] % c.args[1:])
                   for c in info.call_args_list)

    def test_a_new_attempt_prepares_again(self):
        assert "early_for_direct" in final._PREPARED_CUTOVER_PARAMS


class TestPlumbing:

    def test_snapshot_add_can_skip_the_replication_task(self):
        src = inspect.getsource(snapshot_controller.add)
        assert "if lvol.do_replicate and replicate:" in src

    def test_dest_clone_is_never_dropped_on_a_retry(self):
        c = RPCClient.__new__(RPCClient)
        calls = []

        def req(method, **kw):
            calls.append(kw)
            e = RPCRemoteError.__new__(RPCRemoteError)
            e.code = -32602
            e.args = ("spdk_json_decode_object failed",)
            raise e
        c._request = req  # type: ignore[method-assign]
        with pytest.raises(RPCRemoteError):
            c.bdev_lvol_transfer("S", 0, 256, "hub", operation="replicate", lvol_id=7,
                                 allow_partial=True, partial_base=5, priority_class=7,
                                 dest_clone=True)
        assert calls and all(kw.get("dest_clone") is True for kw in calls)
