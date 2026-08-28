# coding=utf-8
"""Retention for replication-driven internal snapshots (D2), against real FDB.

``snapshot_replication._prune_internal_snapshots`` decides which internal
snapshots may be deleted by *reading model state*: the source snapshot chain of
a volume (``get_snapshots_by_node_id``), each snapshot's replicated copy on the
target cluster (``get_snapshot_by_id``), that copy's chain link
(``prev_snap_uuid``), the volumes cloned from it (``get_mini_lvols``), the
owning storage node, and the volume's replication policy
(``get_replication_policy_for_lvol``). Every one of those is a DBController
accessor, so the tests belong to the FDB-backed tier: the state under test IS
database state.

These cases previously ran against a hand-written ``_FakeDB`` that implemented
the four accessors the code used at the time. They broke the moment retention
started consulting the replication policy — the fake had no
``get_replication_policy_for_lvol``, so twelve tests failed on an
``AttributeError`` rather than on anything about retention. A real
``DBController`` cannot drift out of sync with the code it serves.

Mocked here — everything *above* the database, per the tier's rule:

- ``snapshot_controller.delete``, the data-plane delete (cluster-wide object
  lock, ``bdev_lvol_delete`` over JSON-RPC, reaped by snapshot_monitor). It is
  replaced by a recorder that applies the DB effect the real delete eventually
  has — the record is removed — because that is what the prune loop reads back.
- ``StorageNode.rpc_client``, the SPDK chain query on the target node. The
  integration tier never talks to a storage node.
"""

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr

CLUSTER_ID = "cluster-1"
POOL_ID = "pool-1"
SOURCE_NODE_ID = "N1"
TARGET_NODE_ID = "TN1"
TARGET_LVOL_ID = "T_LV1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _write_lvol(db, uuid, node_id, cloned_from_snap="", status=LVol.STATUS_ONLINE):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.cluster_id = CLUSTER_ID
    lvol.pool_uuid = POOL_ID
    lvol.node_id = node_id
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.lvol_bdev = f"LVOL_{uuid}"
    lvol.lvs_name = "LVS_S" if node_id == SOURCE_NODE_ID else "LVS_T"
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.size = 1024 ** 3
    lvol.status = status
    lvol.cloned_from_snap = cloned_from_snap
    lvol.write_to_db(db.kv_store)
    return lvol


def _write_node(db, uuid, status):
    node = StorageNode()
    node.uuid = uuid
    node.cluster_id = CLUSTER_ID
    node.hostname = uuid
    node.status = status
    node.lvstore = "LVS_T"
    node.lvstore_status = "ready"
    node.write_to_db(db.kv_store)
    return node


def _write_snapshot(db, uuid, created_at, snap_type, lvol, target="",
                    status=SnapShot.STATUS_ONLINE):
    """A snapshot on the SOURCE cluster, optionally already replicated."""
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER_ID
    snap.pool_uuid = POOL_ID
    snap.created_at = created_at
    snap.snap_type = snap_type
    snap.status = status
    snap.target_replicated_snap_uuid = target
    snap.snap_name = f"SNAP_{uuid}"
    snap.snap_bdev = f"LVS_S/{uuid}"
    snap.snap_uuid = f"uuid-{uuid}"
    snap.size = lvol.size
    snap.lvol = lvol
    snap.write_to_db(db.kv_store)
    return snap


def _write_target_copy(db, uuid, target_lvol, prev_snap_uuid=""):
    """A replicated snapshot as it exists on the remote cluster.

    ``prev_snap_uuid`` is the chain link retention checks before it deletes a
    predecessor: it is only written once bdev_lvol_add_clone + convert
    succeeded.
    """
    copy = SnapShot()
    copy.uuid = uuid
    copy.cluster_id = CLUSTER_ID
    copy.pool_uuid = POOL_ID
    copy.status = SnapShot.STATUS_ONLINE
    copy.snap_type = SnapShot.TYPE_INTERNAL
    copy.snap_name = f"SNAP_{uuid}"
    copy.snap_bdev = f"LVS_T/{uuid}"
    copy.snap_uuid = f"uuid-{uuid}"
    copy.prev_snap_uuid = prev_snap_uuid
    copy.size = target_lvol.size
    copy.lvol = target_lvol
    copy.write_to_db(db.kv_store)
    return copy


def _healthy_chain(source_snaps):
    """Link each replicated internal target copy onto its predecessor's.

    This is the state a converged replication leaves behind, so it is the
    default for the existing cases: they assert on retention, not on chaining.
    """
    chain: dict = {}
    per_lvol: dict = {}
    for s in source_snaps:
        if s.snap_type != SnapShot.TYPE_INTERNAL or not s.target_replicated_snap_uuid:
            continue
        per_lvol.setdefault(s.lvol.get_id(), []).append(s)
    for snaps in per_lvol.values():
        snaps.sort(key=lambda s: s.created_at)
        for prev, nxt in zip(snaps, snaps[1:]):
            chain[nxt.target_replicated_snap_uuid] = prev.target_replicated_snap_uuid
    return chain


class _RecordingSnapshotController:
    """Records what retention decided to delete, and applies it to the real DB.

    The real ``snapshot_controller.delete`` is a data-plane path; what matters
    to the prune loop is the state it leaves behind, since the loop reads each
    target copy back before acting on it.
    """

    def __init__(self, db):
        self._db = db
        self.deleted = []

    def delete(self, uuid, force_delete=False):
        self.deleted.append(uuid)
        try:
            snap = self._db.get_snapshot_by_id(uuid)
        except KeyError:
            return True
        snap.remove(self._db.kv_store)
        return True


def _seed(db, monkeypatch, source_snaps, existing_targets, chain=None,
          target_node_status=StorageNode.STATUS_OFFLINE, target_bdevs=()):
    """Materialize the target-side state and stub the layers above the DB.

    The target node is offline by default so the SPDK chain fallback stays out
    of the way unless a test explicitly opts into it.
    """
    if chain is None:
        chain = _healthy_chain(source_snaps)

    target_lvol = _write_lvol(db, TARGET_LVOL_ID, TARGET_NODE_ID)
    for uuid in existing_targets:
        _write_target_copy(db, uuid, target_lvol, chain.get(uuid, ""))
    _write_node(db, TARGET_NODE_ID, target_node_status)

    class _RPC:
        def get_bdevs(self, name=None):
            return [b for b in target_bdevs if b.get("name") == name]

    monkeypatch.setattr(StorageNode, "rpc_client", lambda self, **kwargs: _RPC())

    snapctl = _RecordingSnapshotController(db)
    monkeypatch.setattr(sr, "snapshot_controller", snapctl)
    return snapctl


def test_prunes_older_internal_keeps_newest_and_users(db, monkeypatch):
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "user_mid", 150, SnapShot.TYPE_USER, source_lvol, target="T_user"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_user", "T_mid", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    # Target copy deleted before the source snapshot; the newest PAIR is kept
    # so an arriving snapshot always has a predecessor to chain onto.
    assert snapctl.deleted == ["T_old", "int_old"]
    for kept in ("int_mid", "T_mid", "int_new", "T_new", "user_mid", "T_user"):
        assert kept not in snapctl.deleted


def test_single_internal_not_pruned(db, monkeypatch):
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [_write_snapshot(db, "int_only", 100, SnapShot.TYPE_INTERNAL,
                             source_lvol, target="T_only")]
    snapctl = _seed(db, monkeypatch, snaps, {"T_only"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []


def test_unreplicated_internal_ignored(db, monkeypatch):
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    # Newest internal not yet replicated (no target) -> excluded; the only
    # replicated internal is the single oldest, so nothing is pruned.
    snaps = [
        _write_snapshot(db, "int_repl", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_repl"),
        _write_snapshot(db, "int_pending", 200, SnapShot.TYPE_INTERNAL, source_lvol, target=""),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_repl"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []


def test_missing_target_still_cleans_source(db, monkeypatch):
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_gone"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    # T_gone already deleted on target -> only source snapshot is cleaned up.
    snapctl = _seed(db, monkeypatch, snaps, {"T_mid", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["int_old"]


def test_other_lvol_snapshots_untouched(db, monkeypatch):
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)
    other_lvol = _write_lvol(db, "LV2", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
        # Different lvol on same node — must never be pruned.
        _write_snapshot(db, "other_old", 50, SnapShot.TYPE_INTERNAL, other_lvol, target="TO_old"),
        _write_snapshot(db, "other_new", 250, SnapShot.TYPE_INTERNAL, other_lvol, target="TO_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps,
                    {"T_old", "T_mid", "T_new", "TO_old", "TO_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_never_prunes_snapshot_a_failed_over_volume_is_cloned_from(db, monkeypatch):
    """Root cause of the all-zeros DR fail-over (labs 2026-08-10/11).

    Fail-over clones the volume from the last replicated TARGET snapshot; the
    prune, keyed only on the SOURCE snapshot age, then deleted that target copy.
    The delete reaches SPDK as bdev_lvol_delete(sync=False) and frees the blocks
    immediately, so no DB-level guard downstream can save the clone. Retention
    must skip a target snapshot with a live dependent clone.

    Three snapshots, not two: with only a pair, the newest-``keep`` rule returns
    before the clone guard is ever consulted, and the case passes no matter what
    that guard does. ``int_old`` has to be a genuine prune candidate — chained,
    past the cushion — so that the dependent clone is the only thing saving it.
    """
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"})
    # A failed-over volume lives on the OLD target snapshot.
    _write_lvol(db, "FO_VOL", TARGET_NODE_ID, cloned_from_snap="T_old")

    sr._prune_internal_snapshots(source_lvol)

    assert "T_old" not in snapctl.deleted, (
        "pruned the target snapshot a failed-over volume is cloned from — "
        "its blocks are freed by SPDK immediately (sync=False), the volume "
        "reads zeros from then on")
    # The source-side copy must survive too (it pairs with the kept target).
    assert "int_old" not in snapctl.deleted


def test_in_deletion_clone_does_not_pin_the_snapshot(db, monkeypatch):
    """A clone that is itself going away must not block retention forever."""
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"})
    _write_lvol(db, "DYING", TARGET_NODE_ID, cloned_from_snap="T_old",
                status=LVol.STATUS_IN_DELETION)

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_newest_pair_is_kept_so_arrivals_have_a_chain_parent(db, monkeypatch):
    """Chain continuity: a replicated snapshot holds only its own clusters, and
    deleting one swap-merges its segments into the successor CHAINED to it.
    Keeping just the newest pruned the predecessor the instant a replication
    finished, so the next arrival had nothing to chain onto and kept only its
    delta — the target then held the last delta over holes (labs run 15 vs 19,
    same case passing then failing on timing alone)."""
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_prev", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_prev"),
        _write_snapshot(db, "int_new", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_prev", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == [], (
        "pruned the predecessor the next arrival must chain onto")


def test_defers_prune_until_the_successor_is_actually_chained(db, monkeypatch):
    """The count cushion is not the precondition — the chain link is.

    Keeping the newest N only widens the window in which chaining is expected to
    have happened. If it lagged or failed for one snapshot while newer ones kept
    arriving, the predecessor was still pruned, and because the delete reaches
    SPDK as bdev_lvol_delete(sync=False) its segments were freed instead of
    swap-merged into the successor. The target then holds the newest delta over
    holes and a fail-over clone reads zeros (labs 2026-08-10..17). Retention must
    verify the link and defer while it is absent.
    """
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    # T_mid arrived but was never chained onto T_old.
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"}, chain={})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == [], (
        "pruned a predecessor whose successor is not chained onto it — SPDK frees "
        "the blocks immediately, so those segments are lost rather than merged")


def test_prunes_once_the_chain_is_established(db, monkeypatch):
    """The deferral must release as soon as chaining catches up (no livelock)."""
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"},
                    chain={"T_mid": "T_old", "T_new": "T_mid"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_spdk_verdict_releases_a_missing_db_link(db, monkeypatch):
    """A missing link must not pin the pair for ever.

    The link write is best-effort, and snapshots replicated before chaining was
    implemented have none at all. SPDK is the real authority, so when the DB has
    no link we ask the target node before giving up — otherwise retention would
    never release those snapshots and both chains would grow without bound.
    """
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    # SPDK reports T_mid as a clone whose base is T_old, while the DB link is absent.
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"}, chain={},
                    target_node_status=StorageNode.STATUS_ONLINE,
                    target_bdevs=[{
                        "name": "LVS_T/T_mid",
                        "driver_specific": {
                            "lvol": {"clone": True, "base_snapshot": "LVS_T/T_old"}},
                    }])

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_unchained_in_spdk_is_not_pruned_even_when_node_is_reachable(db, monkeypatch):
    """An online target that reports a standalone blob must still defer."""
    source_lvol = _write_lvol(db, "LV1", SOURCE_NODE_ID)

    snaps = [
        _write_snapshot(db, "int_old", 100, SnapShot.TYPE_INTERNAL, source_lvol, target="T_old"),
        _write_snapshot(db, "int_mid", 200, SnapShot.TYPE_INTERNAL, source_lvol, target="T_mid"),
        _write_snapshot(db, "int_new", 300, SnapShot.TYPE_INTERNAL, source_lvol, target="T_new"),
    ]
    snapctl = _seed(db, monkeypatch, snaps, {"T_old", "T_mid", "T_new"}, chain={},
                    target_node_status=StorageNode.STATUS_ONLINE,
                    target_bdevs=[{
                        "name": "LVS_T/T_mid",
                        "driver_specific": {
                            "lvol": {"clone": False, "base_snapshot": None}},
                    }])

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []
