"""A delta transfer SPDK refuses with -ENOENT goes again as a full send.

simplyblock/spdk#108: when a delta against ``partial_base`` falls back to whole
clusters and the source's parent is not that base, SPDK also sends the
snapshots in between -- and when it cannot reach the base through snapshots
open on the sending node, ``bdev_lvol_transfer`` / ``bdev_lvol_transfer_final_step``
answer ``{"code": -32602, "message": "No such file or directory"}`` before
anything is sent (a final step's freeze is undone). Retrying the same delta is
refused every time: the control plane must send without a base -- the last
resort, so every such full send raises REPLICATION_FULL_SEND.
"""
import errno
import os
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import lvol_events
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCRemoteError, transfer_base_unreachable
from simplyblock_core.services import replication_final_step as rfs
from simplyblock_core.services import snapshot_replication
from simplyblock_core.services import tasks_runner_replication_final as final

# the real one, for the event's own test (the fixture below stubs it)
_REAL_FULL_SEND = lvol_events.replication_full_send


@pytest.fixture(autouse=True)
def full_send_event():
    with patch.object(snapshot_replication.lvol_events, "replication_full_send") as ev:
        yield ev


def _refused():
    # what rpc_bdev_lvol_transfer{,_final_step} send for rc == -ENOENT
    return RPCRemoteError(os.strerror(errno.ENOENT), -32602)


# ---- matching ------------------------------------------------------------------

def test_the_refusal_is_recognised():
    assert transfer_base_unreachable(_refused())


@pytest.mark.parametrize("err", [
    RPCRemoteError("No such device", -errno.ENODEV),      # bdev lookup / gateway open
    RPCRemoteError(os.strerror(errno.ENODEV), -32602),
    RPCRemoteError(os.strerror(errno.EEXIST), -32602),    # task already running
    RPCRemoteError("spdk_json_decode_object failed", -32602),
    RPCRemoteError(os.strerror(errno.ENOENT), -32603),    # not the transfer handlers' code
    ValueError("No such file or directory"),
])
def test_other_errors_are_not_the_refusal(err):
    assert not transfer_base_unreachable(err)


# ---- periodic snapshot replication ---------------------------------------------------

def _repl_task(**params):
    t = JobSchedule()
    t.uuid = "T1"
    t.function_params = dict(params)
    t.write_to_db = MagicMock()
    return t


_SNAP = MagicMock()
_KW = dict(name="LVS_1/SNAP_2", offset=0, batch_size=256, bdev_name="hub",
           operation="replicate", lvol_id=7, priority_class=7)


def test_periodic_refused_delta_is_sent_in_full(full_send_event):
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = [_refused(), True]
    task = _repl_task(allow_partial=True, partial_base=0x42, allow_partial_landing="REP_LV")
    applied: dict = {}

    snapshot_replication._submit_transfer(task, rpc, _SNAP, allow_partial=True,
                                          partial_base=0x42, cow_landing=True,
                                          applied=applied, **_KW)

    first, second = (c.kwargs for c in rpc.bdev_lvol_transfer.call_args_list)
    assert first["allow_partial"] is True and first["partial_base"] == 0x42
    assert second["allow_partial"] is False and second["partial_base"] == 0
    assert second["cow_landing"] is False
    assert {k: second[k] for k in _KW} == _KW
    # a resumed attempt keeps the full mode and the finish does not merge
    assert task.function_params["allow_partial"] is False
    assert task.function_params["partial_base"] == 0
    assert task.function_params["allow_partial_landing"] == "REP_LV"
    task.write_to_db.assert_called()
    full_send_event.assert_called_once()
    lvol, what, base, err = full_send_event.call_args.args
    assert lvol is _SNAP.lvol and "SNAP_2" in what and base == 0x42
    assert transfer_base_unreachable(err)


def test_periodic_a_failing_event_does_not_stop_the_full_send(full_send_event):
    full_send_event.side_effect = RuntimeError("fdb down")
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = [_refused(), True]
    snapshot_replication._submit_transfer(_repl_task(), rpc, _SNAP, allow_partial=True,
                                          partial_base=0x42, cow_landing=False,
                                          applied={}, **_KW)
    assert rpc.bdev_lvol_transfer.call_count == 2


def test_periodic_other_errors_still_raise(full_send_event):
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = RPCRemoteError(os.strerror(errno.EEXIST), -32602)
    task = _repl_task(allow_partial=True, partial_base=0x42)
    with pytest.raises(RPCRemoteError):
        snapshot_replication._submit_transfer(task, rpc, _SNAP, allow_partial=True,
                                              partial_base=0x42, cow_landing=False,
                                              applied={}, **_KW)
    assert rpc.bdev_lvol_transfer.call_count == 1
    assert task.function_params["partial_base"] == 0x42
    full_send_event.assert_not_called()


def test_periodic_full_send_is_not_retried():
    # without a base SPDK never refuses this way; an ENOENT here is something else
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = _refused()
    with pytest.raises(RPCRemoteError):
        snapshot_replication._submit_transfer(_repl_task(), rpc, _SNAP, allow_partial=False,
                                              partial_base=0, cow_landing=False,
                                              applied={}, **_KW)
    assert rpc.bdev_lvol_transfer.call_count == 1


# ---- cutover final step ------------------------------------------------------------

class _FinalRPC:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def bdev_lvol_transfer_final_step(self, lvol_name, lvol_id, snapshot_name, batch,
                                      gateway, operation, partial_base=0, cow_landing=False,
                                      applied=None):
        self.calls.append({"partial_base": partial_base, "cow_landing": cow_landing,
                           "batch": batch})
        r = self.results.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    def bdev_lvol_transfer_stat(self, name):
        return {"transfer_state": "No process"}


def _node(uuid, rpc, lvstore):
    n = MagicMock()
    n.get_id.return_value = uuid
    n.lvstore = lvstore
    n.rpc_client.return_value = rpc
    return n


def _lvol():
    lv = MagicMock()
    lv.uuid = "LV1"
    lv.get_id.return_value = "LV1"
    lv.lvol_bdev = "LVOL_1"
    lv.nqn = "nqn:LV1"
    lv.ns_id = 1
    return lv


def _run_cutover(rpc):
    src = _node("S1", rpc, "lvs_src")
    tgt = _node("T1", MagicMock(), "lvs_tgt")
    with patch.object(rfs, "ensure_hub_attached", return_value=("hub", "lvs_tgt/hubn1", None)), \
            patch.object(rfs, "fence_source_paths") as fence, \
            patch.object(rfs, "restore_source_paths") as restore, \
            patch.object(rfs, "enable_target_paths") as enable, \
            patch.object(rfs, "_lvs_members", return_value=[]), \
            patch.object(rfs, "_final_delta_base", return_value=0x31), \
            patch.object(rfs.constants, "REPL_FINAL_STEP_DELTA", True):
        ok, err = rfs.run_cutover(src, tgt, _lvol(), "lvs_tgt/LVOL_1", 7, "lvs_tgt/SNAP_1",
                                  operation="replicate", delta_base=MagicMock())
    return ok, err, fence, restore, enable


def test_final_step_refused_delta_goes_again_in_whole_clusters(full_send_event):
    rpc = _FinalRPC([_refused(), {"transfer_state": "Done"}])
    ok, err, fence, restore, enable = _run_cutover(rpc)
    assert ok is True and err is None
    assert rpc.calls == [
        {"partial_base": 0x31, "cow_landing": True, "batch": rfs._FINAL_STEP_DELTA_BATCH},
        {"partial_base": 0, "cow_landing": True, "batch": rfs._FINAL_STEP_BATCH},
    ]
    # fenced once, never restored in between: the full send runs inside the same fence
    fence.assert_called_once()
    restore.assert_not_called()
    enable.assert_called_once()
    full_send_event.assert_called_once()
    assert "final step" in full_send_event.call_args.args[1]
    assert full_send_event.call_args.args[2] == 0x31


def test_final_step_failing_full_send_restores_the_source():
    rpc = _FinalRPC([_refused(), {"transfer_state": "Failed"}])
    ok, err, _fence, restore, enable = _run_cutover(rpc)
    assert ok is False and "Failed" in err
    assert len(rpc.calls) == 2
    restore.assert_called_once()
    enable.assert_not_called()


def test_final_step_other_errors_are_not_retried_in_full(full_send_event):
    rpc = _FinalRPC([RPCRemoteError(os.strerror(errno.EEXIST), -32602)])
    ok, _err, _fence, restore, enable = _run_cutover(rpc)
    assert ok is False
    assert len(rpc.calls) == 1
    restore.assert_called_once()
    enable.assert_not_called()
    full_send_event.assert_not_called()


# ---- cutover direct rounds ---------------------------------------------------------

def _snap(sid, blobid):
    s = MagicMock()
    s.get_id.return_value = sid
    s.blobid = blobid
    s.snap_bdev = f"LVS_1/SNAP_{sid}"
    s.cluster_id = "CL"
    return s


def _final_task():
    t = JobSchedule()
    t.uuid = "F1"
    t.cluster_id = "CL"
    t.function_params = {"lvol_id": "LV1", "tgt_node_id": "TN", "shrink_round": 1,
                         "shrink_snap_id": "S1", "early_prepared": True,
                         "tgt_lvol_composite": "LVS_9/LVOL_1", "tgt_map_id": 7,
                         "tgt_snap_id": "COPY0"}
    return t


def _submit_round(rpc):
    base, snap = _snap("S0", 0x55), _snap("S1", 0x10)
    src = MagicMock()
    src.get_id.return_value = "SRC"
    src.rpc_client.return_value = rpc
    db = MagicMock()
    db.get_snapshot_by_id.side_effect = lambda i: {"S1": snap, "S0": base}[i]

    def _record(task, **params):
        task.function_params.update(params)
        return task

    with patch.object(final, "_record", side_effect=_record), \
            patch.object(final, "db", db), \
            patch.object(final, "_direct_round_base", return_value=base), \
            patch.object(final, "_cutover_source", return_value=(src, "")), \
            patch.object(final, "_cutover_target", return_value=(MagicMock(), "")), \
            patch.object(rfs, "ensure_hub_attached",
                         return_value=("ctrl", "LVS_9/transferhubn1", None)), \
            patch("simplyblock_core.controllers.replication_delta.delta_basis",
                  return_value=(True, "")), \
            patch.object(snapshot_replication, "_replication_class_on", return_value=False):
        return final._submit_direct_round(_final_task(), _lvol())


def test_direct_round_refused_delta_is_sent_in_whole_clusters(full_send_event):
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = [_refused(), True]
    err, task = _submit_round(rpc)
    assert err is None
    first, second = (c.kwargs for c in rpc.bdev_lvol_transfer.call_args_list)
    assert first["allow_partial"] is True and first["partial_base"] == 0x55
    assert second["allow_partial"] is False and second["partial_base"] == 0
    assert second["dest_clone"] is True and second["lvol_id"] == 7
    assert task.function_params["direct_submits"] == 1
    full_send_event.assert_called_once()
    assert "round" in full_send_event.call_args.args[1]
    assert full_send_event.call_args.args[2] == 0x55


def test_direct_round_other_errors_are_reported(full_send_event):
    rpc = MagicMock()
    rpc.bdev_lvol_transfer.side_effect = RPCRemoteError(os.strerror(errno.EEXIST), -32602)
    err, _task = _submit_round(rpc)
    assert "did not start" in err
    assert rpc.bdev_lvol_transfer.call_count == 1
    full_send_event.assert_not_called()


# ---- the event ---------------------------------------------------------------------

def test_the_full_send_event_is_a_warning_naming_base_and_cause():
    from simplyblock_core.controllers import events_controller as ec
    from simplyblock_core.models.events import EventObj
    lv = _lvol()
    lv.pool_uuid = "P1"
    dbc = MagicMock()
    dbc.return_value.get_pool_by_id.return_value.cluster_id = "CL"
    with patch.object(lvol_events, "DBController", dbc),             patch.object(lvol_events.ec, "log_event_cluster") as log:
        _REAL_FULL_SEND(lv, "snapshot LVS_1/SNAP_2", 0x42, _refused())
    kw = log.call_args.kwargs
    assert kw["event"] == ec.EVENT_REPLICATION_FULL_SEND
    assert kw["event_level"] == EventObj.LEVEL_WARN
    assert kw["cluster_id"] == "CL"
    assert "0x42" in kw["message"] and "in full" in kw["message"]
