"""A snapshot's record is written before the snapshot exists in SPDK.

bf8 (2026-11-02): a control-plane restart at 10:47:48 killed the snapshot
monitor between the SPDK create (and replica registration) of an internal
snapshot and the record write. The blob sat in the volume's chain with no
record, three more snapshots were stacked on it, and the volume's replication
was blocked for good ("chain blob ... has no snapshot record").

Now: intent record (in_creation) first, online once the snapshot exists
everywhere; no new snapshot of a volume while one is in_creation; an intent
older than any create is rolled back by reap_snapshot_intents.
"""
import ast
import inspect
import textwrap
from types import SimpleNamespace
from typing import Any

import pytest

from simplyblock_core.controllers import snapshot_controller as sc
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.services import snapshot_replication as sr

# --- create order -----------------------------------------------------------

def _add_src():
    return textwrap.dedent(inspect.getsource(sc.add))


def test_the_intent_is_written_before_the_spdk_create():
    src = _add_src()
    intent = src.index("snap.status = SnapShot.STATUS_IN_CREATION")
    write = src.index("snap.write_to_db", intent)
    create_call = src.index("failed = _create_in_spdk()")
    assert intent < write < create_call
    # every SPDK create of the snapshot happens inside _create_in_spdk
    inner = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef)
             and n.name == "_create_in_spdk"][0]
    creates = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call)
               and getattr(n.func, "attr", "") in ("lvol_create_snapshot", "lvol_create_snapshot2")]
    end = inner.end_lineno or inner.lineno
    assert creates and all(inner.lineno <= c.lineno <= end for c in creates)


def test_a_failed_or_raising_create_settles_the_intent():
    src = _add_src()
    tail = src[src.index("failed = _create_in_spdk()") - 40:]
    tail = tail[:tail.index("snap.status = SnapShot.STATUS_ONLINE")]
    assert tail.count("_settle_failed_intent(snap, lvol)") == 2
    assert "_drop_snapshot_intent(snap)" not in tail
    assert "except BaseException" in tail and "raise" in tail


class _Member:
    def __init__(self, nid, held=None, err=None):
        self.nid, self.held, self.err = nid, held, err

    def rpc_client(self, **kw):
        def bdev_get(name):
            if self.err:
                raise self.err
            return self.held
        return SimpleNamespace(bdev_get=bdev_get)


@pytest.fixture
def settle(monkeypatch):
    st: dict[str, Any] = {"members": {}}
    monkeypatch.setattr(sc.db_controller, "get_storage_node_by_id", lambda nid: st["members"][nid])
    return st


def _settle_case(settle, members):
    settle["members"] = {m.nid: m for m in members}
    snap = _Snap()
    lvol = SimpleNamespace(nodes=[m.nid for m in members], node_id=members[0].nid)
    return sc._settle_failed_intent(snap, lvol), snap


def test_a_failed_create_with_no_blob_anywhere_drops_the_intent(settle):
    dropped, snap = _settle_case(settle, [_Member("P"), _Member("S"), _Member("T")])
    assert dropped and snap.removed


def test_a_blob_left_on_the_primary_keeps_the_intent(settle):
    # bf9 SNAP_160: the rollback's delete was refused on a primary that had lost leadership
    dropped, snap = _settle_case(settle, [_Member("P", held={"name": "LVS_16/SNAP_2027"}),
                                          _Member("S"), _Member("T")])
    assert not dropped and not snap.removed


def test_an_unreachable_member_keeps_the_intent(settle):
    dropped, snap = _settle_case(settle, [_Member("P"), _Member("S", err=RuntimeError("timeout"))])
    assert not dropped and not snap.removed


def test_success_completes_the_same_record():
    src = _add_src()
    done = src[src.index("snap.status = SnapShot.STATUS_ONLINE") - 200:]
    assert "snap.blobid = blobid" in done and "snap.snap_uuid = snap_uuid" in done
    assert src.count("snap = SnapShot()") == 1


# --- reaper ------------------------------------------------------------------

class _Snap:
    def __init__(self, sid="S1", status=SnapShot.STATUS_IN_CREATION, age=10_000, lvol="L1"):
        self._id, self.status, self.cluster_id = sid, status, "CL"
        self.created_at, self.snap_bdev = 100_000 - age, "LVS_16/SNAP_2027"
        self.lvol = SimpleNamespace(get_id=lambda: lvol)
        self.removed = False

    def get_id(self):
        return self._id

    def remove(self, kv):
        self.removed = True


@pytest.fixture
def reap(monkeypatch):
    st: dict[str, Any] = {"snaps": [], "leader_bdev": None, "leader": True, "rollbacks": []}
    leader = SimpleNamespace(get_id=lambda: "N1",
                             rpc_client=lambda **kw: SimpleNamespace(bdev_get=lambda n: st["leader_bdev"]))
    lvol = SimpleNamespace(nodes=["N1", "N2"], node_id="N1", lvs_name="LVS_16")

    def get_lvol(lid):
        if lid != "L1":
            raise KeyError(lid)
        return lvol

    db = SimpleNamespace(
        get_snapshots=lambda cid: st["snaps"], get_lvol_by_id=get_lvol,
        get_storage_node_by_id=lambda nid: SimpleNamespace(get_id=lambda: nid), kv_store=None)
    monkeypatch.setattr(sc, "db_controller", db)
    monkeypatch.setattr(sc, "_find_lvs_leader", lambda cid, lvs, nodes: leader if st["leader"] else None)
    monkeypatch.setattr(sc, "_rollback_snapshot_bdev",
                        lambda cid, lvs, ldr, bdev, members: st["rollbacks"].append(bdev))
    sc._intent_reap_attempts.clear()
    return st


def test_an_old_intent_is_rolled_back_and_dropped(reap):
    s = _Snap()
    reap["snaps"] = [s]
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 1
    assert reap["rollbacks"] == ["SNAP_2027"] and s.removed


def test_a_young_intent_is_left_to_its_create(reap):
    s = _Snap(age=60)
    reap["snaps"] = [s]
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 0
    assert reap["rollbacks"] == [] and not s.removed


def test_the_record_stays_while_the_bdev_is_still_there(reap):
    s = _Snap()
    reap["snaps"], reap["leader_bdev"] = [s], {"name": "LVS_16/SNAP_2027"}
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 0
    assert not s.removed
    # and is not hammered: the next attempt waits max_age
    assert sc.reap_snapshot_intents("CL", now_ts=100_010, max_age=600) == 0
    assert reap["rollbacks"] == ["SNAP_2027"]


def test_no_leader_no_rollback(reap):
    s = _Snap()
    reap["snaps"], reap["leader"] = [s], False
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 0
    assert reap["rollbacks"] == [] and not s.removed


def test_the_intent_of_a_vanished_volume_is_dropped(reap):
    s = _Snap(lvol="GONE")
    reap["snaps"] = [s]
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 1
    assert s.removed


def test_online_snapshots_are_not_touched(reap):
    s = _Snap(status=SnapShot.STATUS_ONLINE)
    reap["snaps"] = [s]
    assert sc.reap_snapshot_intents("CL", now_ts=100_000, max_age=600) == 0
    assert not s.removed


# --- readers -----------------------------------------------------------------

def test_no_new_snapshot_while_one_of_the_volume_is_in_creation(monkeypatch):
    monkeypatch.setattr(sc.db_controller, "get_snapshots_by_lvol_id",
                        lambda lid: [_Snap(), _Snap("S2", SnapShot.STATUS_ONLINE)])
    assert [s.get_id() for s in sc._snapshots_in_creation("L1")] == ["S1"]
    src = _add_src()
    gate = src.index("pending = _snapshots_in_creation(lvol.get_id())")
    assert gate < src.index("snap.status = SnapShot.STATUS_IN_CREATION")


def test_a_snapshot_in_creation_cannot_be_deleted(monkeypatch):
    monkeypatch.setattr(sc.db_controller, "get_snapshot_by_id", lambda sid: _Snap())
    monkeypatch.setattr(sc.ops_gate, "assert_object_ops_allowed", lambda *a, **k: None)
    assert sc.delete("S1") is False


def test_the_replication_chain_treats_an_intent_as_blocked(monkeypatch):
    rec = _Snap(sid="ANC")
    rec.snap_bdev = "LVS_16/SNAP_1"
    snapshot = SimpleNamespace(snap_bdev="LVS_16/SNAP_2", lvol=SimpleNamespace(node_id="N1"),
                               get_id=lambda: "S")
    bdevs = {"LVS_16/SNAP_2": {"driver_specific": {"lvol": {"base_snapshot": "SNAP_1"}}}}
    node = SimpleNamespace(get_id=lambda: "N1",
                           rpc_client=lambda **kw: SimpleNamespace(bdev_get=lambda n: bdevs.get(n)))
    monkeypatch.setattr(sr.db, "get_snapshots_by_node_id", lambda nid: [rec])
    verdict, got, why = sr._unreplicated_local_ancestor(node, snapshot, False)
    assert verdict == "blocked" and got is rec and "still being created" in why


def test_the_cadence_holds_while_the_newest_snapshot_is_an_intent(monkeypatch):
    lvol = SimpleNamespace(get_id=lambda: "L1")
    mini = SimpleNamespace(lvol=lvol, snap_type=SnapShot.TYPE_INTERNAL, status=SnapShot.STATUS_IN_CREATION,
                           created_at=5, get_id=lambda: "S1")
    full = _Snap()
    monkeypatch.setattr(sm.db, "get_snapshot_by_id", lambda sid: full)
    assert sm._outstanding_internal_snapshot(lvol, [mini]) is full
