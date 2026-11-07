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
    assert applied == {"partial": True}


def test_final_step_delta_falls_back_to_whole_clusters_on_an_old_spdk():
    err = rpc_module.RPCRemoteError("spdk_json_decode_object failed", -32602)
    client, sent = _client([err, {"transfer_state": "Done"}])
    applied = {}
    ret = client.bdev_lvol_transfer_final_step("lvs/L", 7, "tlvs/S", 2, "hub", "replicate",
                                               partial_base=5, applied=applied)
    assert ret == {"transfer_state": "Done"}
    assert len(sent) == 2
    assert "allow_partial" not in sent[1][1] and "cow_landing" not in sent[1][1]
    assert applied == {"partial": False}


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
