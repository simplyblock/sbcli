"""A predecessor that was never replicated is not a chain base.

Lab 2026-08-20, case 4: 299 occurrences in 75 minutes of

    ERROR: Predecessor snapshot <uuid> has remote copy  but it cannot be
    resolved ('Snapshot lookup with a blank id'); refusing to finalize an
    unchained snapshot

Note the empty value between "remote copy" and "but": the id was BLANK.
``_previous_replicated_snapshot`` returned a predecessor via the
``snap_ref_id`` shortcut without checking that it had actually been
replicated -- the precondition its age-ordered branch has always enforced and
its own name states. ``_resolve_chain_target`` then read the blank id as "a
remote copy exists but cannot be resolved" and refused to finalize, so the
transfer retried for ever and the snapshot never got its replicated marker.

That is what starved case 4's data gate, and (with the cadence back-pressure)
what froze the volume's snapshots entirely.
"""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr


class _LvolRef:
    def __init__(self, uuid="LV1", node_id="NODE_A"):
        self.uuid = uuid
        self.node_id = node_id

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid, created_at=100, target="", source="",
                 snap_ref_id="", status=SnapShot.STATUS_ONLINE, lvol=None,
                 snap_bdev="LVS_1/SNAP_X"):
        self.uuid = uuid
        self.created_at = created_at
        self.target_replicated_snap_uuid = target
        self.source_replicated_snap_uuid = source
        self.snap_ref_id = snap_ref_id
        self.status = status
        self.lvol = lvol or _LvolRef()
        self.snap_bdev = snap_bdev

    def get_id(self):
        return self.uuid


class _Node:
    def __init__(self, uuid="NODE_B"):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


class _DB:
    def __init__(self, by_id, by_node=()):
        self._by_id = by_id
        self._by_node = list(by_node)

    def get_snapshot_by_id(self, uuid):
        if not uuid:
            raise KeyError("Snapshot lookup with a blank id")
        return self._by_id[uuid]

    def get_snapshots_by_node_id(self, node_id):
        return self._by_node


# --- _previous_replicated_snapshot ---------------------------------------


def test_referenced_predecessor_must_itself_be_replicated(monkeypatch):
    """The regression: snap_ref_id pointed at an unreplicated snapshot."""
    unreplicated = _Snap("PREV", created_at=100)          # no remote copy
    newest = _Snap("NEW", created_at=200, snap_ref_id="PREV")
    monkeypatch.setattr(sr, "db", _DB({"PREV": unreplicated, "NEW": newest}))

    assert sr._previous_replicated_snapshot(newest, False) is None


def test_referenced_predecessor_is_used_when_replicated(monkeypatch):
    replicated = _Snap("PREV", created_at=100, target="REMOTE_PREV")
    newest = _Snap("NEW", created_at=200, snap_ref_id="PREV")
    monkeypatch.setattr(sr, "db", _DB({"PREV": replicated, "NEW": newest}))

    assert sr._previous_replicated_snapshot(newest, False) is replicated


def test_unreplicated_reference_falls_back_to_a_replicated_sibling(monkeypatch):
    """The reference is not a dead end: an older replicated sibling still wins."""
    unreplicated = _Snap("PREV", created_at=150)
    sibling = _Snap("SIB", created_at=120, target="REMOTE_SIB")
    newest = _Snap("NEW", created_at=200, snap_ref_id="PREV")
    monkeypatch.setattr(sr, "db", _DB({"PREV": unreplicated, "NEW": newest},
                                      by_node=[sibling, newest]))

    assert sr._previous_replicated_snapshot(newest, False) is sibling


def test_failback_direction_uses_the_source_side_marker(monkeypatch):
    replicated_back = _Snap("PREV", created_at=100, source="REMOTE_PREV")
    newest = _Snap("NEW", created_at=200, snap_ref_id="PREV")
    monkeypatch.setattr(sr, "db", _DB({"PREV": replicated_back, "NEW": newest}))

    assert sr._previous_replicated_snapshot(newest, True) is replicated_back
    # ...and the same record is NOT a chain base in the forward direction.
    assert sr._previous_replicated_snapshot(newest, False) is None


# --- _resolve_chain_target ------------------------------------------------


def test_blank_remote_copy_starts_a_chain_instead_of_failing(monkeypatch):
    """Defence in depth: a blank id means NO copy, not a broken reference."""
    prev = _Snap("PREV", created_at=100)                  # blank target uuid
    newest = _Snap("NEW", created_at=200)
    monkeypatch.setattr(sr, "db", _DB({"PREV": prev, "NEW": newest}))
    monkeypatch.setattr(sr, "_previous_replicated_snapshot",
                        lambda snapshot, to_source: prev)

    target, prev_for_db, ok = sr._resolve_chain_target(newest, False, _Node())
    assert ok is True, "a missing predecessor copy must not fail the transfer"
    assert (target, prev_for_db) == (None, None)


def test_a_genuinely_unresolvable_copy_still_fails(monkeypatch):
    """A non-blank id that does not resolve is still a broken chain."""
    prev = _Snap("PREV", created_at=100, target="GONE")
    newest = _Snap("NEW", created_at=200)
    monkeypatch.setattr(sr, "db", _DB({"PREV": prev, "NEW": newest}))
    monkeypatch.setattr(sr, "_previous_replicated_snapshot",
                        lambda snapshot, to_source: prev)

    _target, _prev, ok = sr._resolve_chain_target(newest, False, _Node())
    assert ok is False


def test_chaining_across_lvstores_still_refused(monkeypatch):
    prev = _Snap("PREV", created_at=100, target="REMOTE_PREV")
    remote = _Snap("REMOTE_PREV", lvol=_LvolRef("LV2", node_id="OTHER_NODE"))
    newest = _Snap("NEW", created_at=200)
    monkeypatch.setattr(sr, "db", _DB({"PREV": prev, "REMOTE_PREV": remote,
                                       "NEW": newest}))
    monkeypatch.setattr(sr, "_previous_replicated_snapshot",
                        lambda snapshot, to_source: prev)

    _target, _prev, ok = sr._resolve_chain_target(newest, False, _Node("NODE_B"))
    assert ok is False


def test_no_predecessor_at_all_is_fine(monkeypatch):
    newest = _Snap("NEW", created_at=200)
    monkeypatch.setattr(sr, "db", _DB({"NEW": newest}))
    monkeypatch.setattr(sr, "_previous_replicated_snapshot",
                        lambda snapshot, to_source: None)

    assert sr._resolve_chain_target(newest, False, _Node()) == (None, None, True)
