"""Listing, deleting, and cloning one generation of a consistency group, against
real FoundationDB.

Moved from simplyblock_core/test/test_consistency_groups.py, whose fake database
stood in for the snapshot lookups (tests/AGENTS.md): the generation's snapshots
are now read through the (group_id, group_seq) index, which the fake did not
implement. The groups, volumes, and snapshots are real records; only the
snapshot delete and clone, which reach storage nodes, are patched.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

CLUSTER = "cgg-cluster"


@pytest.fixture
def db():
    return DBController()


def _lvol(db, uuid, node="NODE_A", lvs="LVS_1"):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.cluster_id = CLUSTER
    lvol.node_id = node
    lvol.lvs_name = lvs
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db.kv_store)
    return lvol


def _snap(db, uuid, group, seq, lvol):
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER
    snap.lvol = lvol
    snap.group_id = group.get_id()
    snap.group_seq = seq
    snap.status = SnapShot.STATUS_ONLINE
    snap.write_to_db(db.kv_store)
    return snap


def test_list_generations_reports_incomplete(db):
    g = cgc.ensure_group(CLUSTER, "db-group")
    g.members = {"a": {"joined_seq": 1, "removed_seq": 0},
                 "b": {"joined_seq": 1, "removed_seq": 0}}
    g.write_to_db(db.kv_store)
    la, lb = _lvol(db, "a"), _lvol(db, "b")
    # generation 1 complete (both members), generation 2 missing member b
    _snap(db, "s1a", g, 1, la)
    _snap(db, "s1b", g, 1, lb)
    _snap(db, "s2a", g, 2, la)

    rows = {r["group_seq"]: r for r in cgc.list_generations(g)}
    assert rows[1]["expected"] == 2 and rows[1]["present"] == 2 and rows[1]["complete"]
    assert rows[2]["expected"] == 2 and rows[2]["present"] == 1 and not rows[2]["complete"]


def test_delete_generation_removes_every_member_snapshot(db, monkeypatch):
    g = cgc.ensure_group(CLUSTER, "db-group")
    la = _lvol(db, "a")
    s2a, s2b = _snap(db, "s2a", g, 2, la), _snap(db, "s2b", g, 2, la)
    _snap(db, "s3a", g, 3, la)
    deleted_calls = []

    def _fake_delete(sid, *a, **k):
        deleted_calls.append(sid)
        return True

    monkeypatch.setattr(snapshot_controller, "delete", _fake_delete)
    deleted, err = cgc.delete_generation(g, 2)
    assert err is None
    assert set(deleted_calls) == {s2a.get_id(), s2b.get_id()}   # generation 3 untouched


def test_delete_generation_unknown_seq_errors(db):
    g = cgc.ensure_group(CLUSTER, "db-group")
    deleted, err = cgc.delete_generation(g, 9)
    assert deleted is None and "not found" in err


def test_clone_generation_forms_new_group(db, monkeypatch):
    g = cgc.ensure_group(CLUSTER, "db-group")
    g.node_id, g.lvs_name = "NODE_A", "LVS_1"
    g.write_to_db(db.kv_store)
    _snap(db, "s1a", g, 1, _lvol(db, "a"))
    _snap(db, "s1b", g, 1, _lvol(db, "b"))
    # each clone lands on the pinned node so the new group colocates
    _lvol(db, "clone-a")
    _lvol(db, "clone-b")
    seq = iter(["clone-a", "clone-b"])
    monkeypatch.setattr(snapshot_controller, "clone",
                        lambda sid, name, *a, **k: (next(seq), False))

    created, err = cgc.clone_generation(g, 1, into_name="restored")
    assert err is None
    assert set(created) == {"clone-a", "clone-b"}
    new_group = db.get_consistency_group_by_name(CLUSTER, "restored")
    assert new_group is not None
    assert set(new_group.members) == {"clone-a", "clone-b"}
