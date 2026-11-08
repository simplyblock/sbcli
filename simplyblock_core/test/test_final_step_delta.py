"""The cutover's final step as a dirty-page delta, and the endgame's byte gate.

bf14 (2026-11-06): every final step of a randomly written volume copied
nearly every 2 MB cluster whole and hit SPDK's 12 s limit; five of eight
volumes kept pace with RPOs of minutes and never met the 50 s lag gate.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core import rpc_client as rpc_module
from simplyblock_core.controllers import replication_delta
from simplyblock_core.services import replication_final_step as rfs
from simplyblock_core.services import tasks_runner_replication_final as runner

# ---- RPC wrapper ------------------------------------------------------------

def _client(responses):
    client = rpc_module.RPCClient.__new__(rpc_module.RPCClient)
    sent = []

    def _request(method, **params):
        sent.append((method, params))
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r
    client._request = _request
    return client, sent


def test_final_step_without_base_sends_the_plain_request():
    client, sent = _client([{"transfer_state": "Done"}])
    client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate")
    assert sent == [("bdev_lvol_transfer_final_step",
                     {"lvol_name": "lvs/L", "lvol_id": 7, "snapshot_name": "tlvs/S",
                      "cluster_batch": 2, "gateway": "hub", "operation": "replicate"})]


def test_final_step_delta_always_lands_copy_on_write():
    client, sent = _client([{"transfer_state": "Done"}])
    applied = {}
    client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate",
                                         partial_base=0x100000031, applied=applied)
    params = sent[0][1]
    assert params["allow_partial"] is True
    assert params["partial_base"] == 0x100000031
    assert params["cow_landing"] is True
    assert applied == {"partial": True, "cow_landing": True}


def test_final_step_delta_falls_back_to_whole_clusters_on_an_old_spdk():
    err = rpc_module.RPCRemoteError("spdk_json_decode_object failed", -32602)
    client, sent = _client([err, {"transfer_state": "Done"}])
    applied = {}
    ret = client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate",
                                               partial_base=5, applied=applied)
    assert ret == {"transfer_state": "Done"}
    assert len(sent) == 2
    assert "allow_partial" not in sent[1][1] and "cow_landing" not in sent[1][1]
    assert applied == {"partial": False, "cow_landing": False}


def test_final_step_delta_other_errors_are_not_swallowed():
    client, _ = _client([rpc_module.RPCRemoteError("No such device", -19)])
    with pytest.raises(rpc_module.RPCRemoteError):
        client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate",
                                             partial_base=5)


# ---- write-count basis --------------------------------------------------------

def _snap(before, blobid=0x10, sid="S1"):
    return SimpleNamespace(local_writes_before=before, blobid=blobid, get_id=lambda: sid)


def test_live_basis_holds_when_only_the_freezing_member_wrote():
    base = _snap({"A": [10, 1], "B": [3, 1]})
    ok, why = replication_delta.live_delta_basis(base, {"A": [900, 1], "B": [3, 1]}, "A")
    assert ok, why


def test_live_basis_fails_when_another_member_wrote():
    base = _snap({"A": [10, 1], "B": [3, 1]})
    ok, why = replication_delta.live_delta_basis(base, {"A": [900, 1], "B": [4, 1]}, "A")
    assert not ok and "B" in why


def test_live_basis_fails_after_a_restart_or_without_counts():
    base = _snap({"A": [10, 1], "B": [3, 1]})
    assert not replication_delta.live_delta_basis(base, {"A": [10, 2], "B": [3, 1]}, "A")[0]
    assert not replication_delta.live_delta_basis(base, {}, "A")[0]
    assert not replication_delta.live_delta_basis(_snap({}), {"A": [1, 1]}, "A")[0]


def test_final_delta_base(monkeypatch):
    lvol = SimpleNamespace(get_id=lambda: "LV1")
    src = SimpleNamespace(get_id=lambda: "A")
    base = _snap({"A": [10, 1], "B": [3, 1]}, blobid=0x100000031)
    monkeypatch.setattr(replication_delta, "local_write_counts",
                        lambda lv, timeout=5, retry=1: {"A": [50, 1], "B": [3, 1]})
    assert rfs._final_delta_base(lvol, base, src, "replicate") == 0x100000031
    assert rfs._final_delta_base(lvol, base, src, "migrate") == 0
    assert rfs._final_delta_base(lvol, None, src, "replicate") == 0
    assert rfs._final_delta_base(lvol, _snap(base.local_writes_before, blobid=0), src,
                                 "replicate") == 0
    monkeypatch.setattr(replication_delta, "local_write_counts",
                        lambda lv, timeout=5, retry=1: {"A": [50, 1], "B": [4, 1]})
    assert rfs._final_delta_base(lvol, base, src, "replicate") == 0


# ---- runner: clone base and backlog gate ------------------------------------

def _isnap(sid, created, copy="", bdev=None):
    return SimpleNamespace(get_id=lambda: sid, created_at=created,
                           target_replicated_snap_uuid=copy, snap_bdev=bdev or f"lvs/{sid}",
                           lvol=SimpleNamespace(get_id=lambda: "LV1"),
                           snap_type=runner.SnapShot.TYPE_INTERNAL)


def _lvol():
    return SimpleNamespace(get_id=lambda: "LV1", node_id="N1", top_bdev="lvs/LVOL_1")


def test_clone_base_source_maps_the_copy_back_to_its_source(monkeypatch):
    snaps = [_isnap("S1", 1, copy="C1"), _isnap("S2", 2, copy="C2"), _isnap("S3", 3)]
    monkeypatch.setattr(runner.db, "get_snapshots_by_node_id", lambda nid: snaps)
    assert runner._clone_base_source(_lvol(), {"tgt_snap_id": "C2"}).get_id() == "S2"
    assert runner._clone_base_source(_lvol(), {"tgt_snap_id": "C9"}) is None
    assert runner._clone_base_source(_lvol(), {}) is None


class _Leader:
    def __init__(self, infos):
        self.infos = infos

    def rpc_client(self, **_kw):
        return SimpleNamespace(bdev_lvol_dirty_bitmap_info=lambda b: self.infos[b])


def _gate(monkeypatch, snaps, infos):
    monkeypatch.setattr(runner.db, "get_snapshots_by_node_id", lambda nid: snaps)
    monkeypatch.setattr(runner, "_cutover_source", lambda lv: (_Leader(infos), ""))
    return runner._endgame_backlog_bytes(_lvol())


def _info(n, complete=True):
    return {"tracking": True, "complete": complete, "dirty_bytes": n}


def test_backlog_counts_the_snapshot_in_flight_and_the_live_volume(monkeypatch):
    snaps = [_isnap("S1", 1, copy="C1"), _isnap("S2", 2)]
    total, why = _gate(monkeypatch, snaps, {"lvs/S2": _info(3 << 30), "lvs/LVOL_1": _info(1 << 30)})
    assert total == 4 << 30, why


def test_backlog_gate_closed_while_snapshots_queue(monkeypatch):
    snaps = [_isnap("S1", 1, copy="C1"), _isnap("S2", 2), _isnap("S3", 3)]
    total, why = _gate(monkeypatch, snaps, {})
    assert total is None and "falling behind" in why


def test_backlog_gate_closed_without_a_complete_count(monkeypatch):
    snaps = [_isnap("S1", 1, copy="C1")]
    total, _ = _gate(monkeypatch, snaps, {"lvs/LVOL_1": _info(1, complete=False)})
    assert total is None
    total, _ = _gate(monkeypatch, [_isnap("S1", 1)], {})
    assert total is None


def test_final_step_without_delta_still_lands_on_the_clone():
    client, sent = _client([{"transfer_state": "Done"}])
    applied = {}
    client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate",
                                         cow_landing=True, applied=applied)
    params = sent[0][1]
    assert params["cow_landing"] is True and "allow_partial" not in params
    assert applied == {"partial": False, "cow_landing": True}


# ---- bf15: no cadence snapshot in an endgame; yield to a ready cutover --------

def _task(tid, lvol_id, **params):
    return SimpleNamespace(get_id=lambda: "C1/0/" + tid, uuid=tid, function_name=runner.JobSchedule.FN_REPLICATION_FINAL,
                           status="running", canceled=False,
                           function_params=dict(lvol_id=lvol_id, **params))


def test_cadence_skips_volumes_in_their_endgame(monkeypatch):
    from simplyblock_core.services import snapshot_monitor
    tasks = [_task("t1", "LV1", shrink_snap_id="S9"), _task("t2", "LV2", cutover_lvs="lvs"),
             _task("t3", "LV3"), _task("t4", "LV4", shrink_snap_id="S1")]
    tasks[3].status = runner.JobSchedule.STATUS_DONE
    monkeypatch.setattr(snapshot_monitor.db, "get_job_tasks", lambda cid: tasks)
    assert snapshot_monitor._cutover_endgame_lvol_ids("C1") == {"LV1", "LV2"}


def test_a_converging_volume_yields_to_a_cutover_ready_to_freeze(monkeypatch):
    me = _task("t1", "LV1", shrink_snap_id="S9")
    waiting = _task("t2", "LV2", tgt_lvol_composite="lvs/X", replication_id="R2")
    reps = {"R2": SimpleNamespace(cutover_proceed=False)}
    monkeypatch.setattr(runner.db, "get_lvol_replication_by_id", lambda rid: reps[rid])
    fresh = {"t1": me, "t2": waiting}
    monkeypatch.setattr(runner.db, "get_task_by_id", lambda tid: fresh[tid])
    assert runner._other_cutover_ready(me, [me, waiting]) == ""
    reps["R2"].cutover_proceed = True
    assert runner._other_cutover_ready(me, [me, waiting]) == "LV2"
    # never to itself
    assert runner._other_cutover_ready(waiting, [waiting]) == ""


def test_yield_reads_the_other_task_fresh(monkeypatch):
    """bf16: the pass's task list predates the other task's clone."""
    me = _task("t1", "LV1", shrink_snap_id="S9")
    stale = _task("t2", "LV2")
    prepared = _task("t2", "LV2", tgt_lvol_composite="lvs/X", replication_id="R2")
    monkeypatch.setattr(runner.db, "get_lvol_replication_by_id",
                        lambda rid: SimpleNamespace(cutover_proceed=True))
    monkeypatch.setattr(runner.db, "get_task_by_id", lambda tid: prepared)
    assert runner._other_cutover_ready(me, [me, stale]) == "LV2"


# ---- between cutover attempts: prune to one replicated snapshot first ----------

def _osnap(sid, created):
    s = _isnap(sid, created, copy="C" + sid)
    s.status = runner.SnapShot.STATUS_ONLINE
    return s


def test_a_retry_waits_until_the_old_chain_is_pruned_to_one(monkeypatch):
    from simplyblock_core.services import snapshot_replication as sr
    snaps = [_osnap("S1", 1), _osnap("S2", 2), _osnap("S3", 3)]
    monkeypatch.setattr(runner.db, "get_snapshots_by_node_id", lambda nid: snaps)
    kicked = []
    monkeypatch.setattr(sr, "prune_async", lambda lv, keep=None: kicked.append(keep) or True)
    task = SimpleNamespace(function_params={"prune_gate_since": 10.0})
    with pytest.raises(runner.TaskDefer):
        runner._prune_gate(task, _lvol())
    assert kicked == [1]
    # pruned down to one; a snapshot replicated after the gate opened is fine
    snaps[:] = [_osnap("S3", 3), _osnap("S4", 20)]
    monkeypatch.setattr(runner, "drop_params", lambda t, *k: t)
    assert runner._prune_gate(task, _lvol()) is task


def test_the_source_throttle_tightens_with_every_failed_attempt():
    caps = [runner._throttle_cap(SimpleNamespace(retry=r)) for r in range(6)]
    assert caps == [8, 4, 2, 1, 1, 1]
