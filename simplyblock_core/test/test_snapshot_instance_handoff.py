"""A multi-copy snapshot delete has to terminate.

A snapshot that lvol migration copied to another node carries those copies in
``snap.instances``. Phase-2 hands the delete on to the next copy -- which is a
DIFFERENT record, with its own uuid, node and bdev, inheriting the rest of the
chain -- but it never retired the record it had just finished with.

So the record stayed in_deletion with its instances list intact and every
monitor cycle re-ran phase-2 for it, logged "Snapshot deleted successfully",
and rewrote the successor to in_deletion again, resurrecting a copy that had
already been deleted. Lab 2026-08-20: 104 snapshots frozen for 40+ minutes with
no errors logged at all (869 "Snapshot has instances" per 2 minutes), while the
104 that had no instances drained normally.
"""
from typing import Any

import pytest

from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.models.snapshot import SnapShot


class _LvolRef:
    def __init__(self, node_id="NODE_A"):
        self.node_id = node_id
        self.lvs_name = "LVS_1"
        self.uuid = "LV1"
        self.ha_type = "ha"

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid, instances=(), node_id="NODE_A"):
        self.uuid = uuid
        self.snap_bdev = f"LVS_1/{uuid}"
        self.cluster_id = "CL1"
        self.instances = list(instances)
        self.status = SnapShot.STATUS_IN_DELETION
        self.deletion_status = ""
        self.lvol = _LvolRef(node_id)
        self.removed = False
        self.unindexed = False

    def get_id(self):
        return self.uuid

    def write_to_db(self, kv=None):
        return True

    def remove(self, kv=None):
        self.removed = True


class _Node:
    def __init__(self, uuid="NODE_A"):
        self.uuid = uuid
        self.status = "online"
        self.secondary_node_id = ""
        self.tertiary_node_id = ""

    def get_id(self):
        return self.uuid

    def lvol_del_sync_lock(self):
        return True

    def lvol_del_sync_lock_reset(self):
        return True

    def rpc_client(self):
        raise AssertionError("no RPC expected in this test")


@pytest.fixture
def harness(monkeypatch):
    """Stub everything phase-2 does to the cluster; keep the record bookkeeping."""
    state: dict[str, Any] = {"handed_off": [], "unindexed": [], "events": []}

    successor = _Snap("SUCCESSOR", node_id="NODE_B")

    class _DB:
        kv_store = "KV"

        def get_storage_node_by_id(self, nid):
            return _Node(nid)

        def get_snapshot_by_id(self, uuid):
            return successor

        def unindex_snapshot(self, snap):
            state["unindexed"].append(snap.get_id())
            snap.unindexed = True

    class _SnapShotFactory:
        """Callable stand-in that still carries the STATUS_* constants."""
        STATUS_IN_DELETION = SnapShot.STATUS_IN_DELETION
        STATUS_ONLINE = SnapShot.STATUS_ONLINE
        TYPE_INTERNAL = SnapShot.TYPE_INTERNAL

        def __call__(self, payload):
            return successor

    monkeypatch.setattr(sm, "db", _DB())
    monkeypatch.setattr(sm, "SnapShot", _SnapShotFactory())
    monkeypatch.setattr(sm.snapshot_controller, "lvstore_op_lock",
                        lambda *a, **kw: _NullCtx())
    monkeypatch.setattr(sm.snapshot_controller, "delete_bdev_absent_ok",
                        lambda *a, **kw: True)
    monkeypatch.setattr(sm.snapshot_controller, "sync_delete_on_peer",
                        lambda *a, **kw: True)
    monkeypatch.setattr(sm.snapshot_events, "snapshot_delete",
                        lambda s: state["events"].append(s.get_id()))
    monkeypatch.setattr(sm, "process_snap_delete",
                        lambda s, node: state["handed_off"].append(s.get_id()))
    state["successor"] = successor
    return state


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_record_with_instances_is_retired_after_handoff(harness):
    """The regression: the finished record must not survive the hand-off."""
    snap = _Snap("ORIGINAL", instances=[{"uuid": "SUCCESSOR"}])
    sm.process_snap_delete_finish(snap, _Node("NODE_A"))

    assert harness["handed_off"] == ["SUCCESSOR"], "successor must be processed"
    assert snap.removed is True, "the handed-off record must be removed"
    assert "ORIGINAL" in harness["unindexed"]


def test_handoff_emits_no_delete_event(harness):
    """The snapshot is not gone until its LAST copy is."""
    snap = _Snap("ORIGINAL", instances=[{"uuid": "SUCCESSOR"}])
    sm.process_snap_delete_finish(snap, _Node("NODE_A"))
    assert harness["events"] == []


def test_last_copy_is_removed_and_announced(harness):
    snap = _Snap("LAST", instances=[])
    sm.process_snap_delete_finish(snap, _Node("NODE_A"))

    assert harness["handed_off"] == [], "nothing left to hand off"
    assert snap.removed is True
    assert harness["events"] == ["LAST"]


def test_remaining_chain_is_carried_to_the_successor(harness):
    snap = _Snap("ORIGINAL", instances=[{"uuid": "SUCCESSOR"}, {"uuid": "THIRD"}])
    sm.process_snap_delete_finish(snap, _Node("NODE_A"))

    assert harness["successor"].instances == [{"uuid": "THIRD"}]
    assert snap.removed is True
