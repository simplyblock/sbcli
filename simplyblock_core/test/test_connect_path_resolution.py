"""Which volume's paths `volume connect` hands out.

Resolution is driven by the replication relationship and NEVER by Cluster.status.
The old redirect fired only while the source cluster was SUSPENDED, so it stopped
redirecting the moment that cluster auto-recovered — which it does within minutes
when its SPDK containers restart — while the volume was still living on the
target. It also consulted the single cluster-scoped target field and never fired
for a planned migration, where the source is healthy throughout.
"""
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol, LVolReplication


class _FakeDB:
    def __init__(self, lvols, replications=()):
        self._lvols = {lv.get_id(): lv for lv in lvols}
        self._replications = list(replications)

    def get_lvol_by_id(self, lvol_id):
        if lvol_id not in self._lvols:
            raise KeyError(f'LVol {lvol_id} not found')
        return self._lvols[lvol_id]

    def get_lvol_replication_objects(self):
        return self._replications


def _lvol(uuid, status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uuid
    lv.status = status
    return lv


def _rep(source, target, state, direction=LVolReplication.DIRECTION_TO_TARGET):
    rep = LVolReplication()
    rep.source_lvol = source
    rep.target_lvol = target
    rep.state = state
    rep.direction = direction
    return rep


def _ids(volumes):
    return [v.get_id() for v in volumes]


def test_no_relationship_returns_the_volume_itself():
    src = _lvol("LV_SRC")
    db = _FakeDB([src])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_SRC"]


def test_replicating_returns_only_the_source():
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_REPLICATING)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_SRC"]


def test_cutover_pending_returns_both_sides():
    """The client must already hold the target paths when ANA flips; that is what
    makes a planned cutover non-disruptive. Target first, so its paths are
    established before the source's are given up."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_CUTOVER_PENDING)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_TGT", "LV_SRC"]


def test_failed_over_returns_the_target_unconditionally():
    """No dependency on source-cluster status: a recovered source must not pull
    the client back to paths that no longer serve the volume."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_TGT"]


def test_cutover_done_returns_only_the_post_move_volume():
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_CUTOVER_DONE)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_TGT"]


def test_querying_the_target_side_also_resolves_to_the_target():
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    assert _ids(lvol_controller._connect_path_volumes(db, tgt)) == ["LV_TGT"]


def test_deleted_target_falls_back_to_the_source():
    """A stale relationship must not strand the caller with no paths at all."""
    src = _lvol("LV_SRC")
    tgt = _lvol("LV_TGT", status=LVol.STATUS_DELETED)
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_SRC"]


def test_missing_target_record_falls_back_to_the_source():
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_SRC"]


def test_newest_relationship_wins_after_a_failback():
    """Fail over, then fail back: the latest relationship decides."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    back = _lvol("LV_BACK")
    db = _FakeDB([src, tgt, back], [
        _rep(src, tgt, LVolReplication.STATE_FAILED_OVER),
        _rep(tgt, back, LVolReplication.STATE_CUTOVER_DONE,
             direction=LVolReplication.DIRECTION_TO_SOURCE),
    ])
    assert _ids(lvol_controller._connect_path_volumes(db, tgt)) == ["LV_BACK"]


def test_resolution_never_consults_cluster_status():
    """Guard rail: the helper must not need clusters at all — a DB without
    get_cluster_by_id still resolves, so no future edit can sneak a status check
    back in without breaking this."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    assert not hasattr(db, "get_cluster_by_id")
    assert _ids(lvol_controller._connect_path_volumes(db, src)) == ["LV_TGT"]


# ---- device-lookup identity (entry.target_lvol_id) ------------------------ #
#
# A clone's wire NSUUID may be borrowed from another volume (the kernel merges
# multipath paths only on matching NSUUIDs), so the client's /dev/disk/by-id
# links can carry an id that is not the record's own. The record's ns_uuid
# persists that wire identity; connect reports it whenever it differs from the
# requested id. Records predating ns_uuid fall back to the relationship's
# other end (the only fail-back shape that existed then inherited the DR
# source's NSUUID).


class _FakeEntry:
    target_lvol_id = None


def _connect(monkeypatch, db, requested_id):
    """Run connect_lvol against the fake DB with the entry builder stubbed."""
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    monkeypatch.setattr(lvol_controller.HostConnectAuth, "resolve",
                        classmethod(lambda cls, lvol, host_nqn, db_controller: None))
    monkeypatch.setattr(lvol_controller, "_connect_entries_for_volume",
                        lambda *a, **kw: [_FakeEntry()])
    entries, err = lvol_controller.connect_lvol(requested_id)
    assert err is None
    return entries


def test_legacy_failback_record_falls_back_to_the_relationship(monkeypatch):
    """cutover_done with the requested volume on the TARGET end and no ns_uuid
    persisted (records from before the field existed): those clones inherited
    the DR SOURCE's NSUUID, so connect must hand that id out."""
    dr, back = _lvol("LV_DR"), _lvol("LV_BACK")
    db = _FakeDB([dr, back], [_rep(dr, back, LVolReplication.STATE_CUTOVER_DONE)])
    entries = _connect(monkeypatch, db, "LV_BACK")
    assert [e.target_lvol_id for e in entries] == ["LV_DR"]


def test_wire_identity_equal_to_own_id_needs_no_override(monkeypatch):
    """A volume whose persisted wire identity equals its own id needs no
    redirect: the CSI's own-id lookup already matches the device, and any
    emission would point it elsewhere."""
    dr, back = _lvol("LV_DR"), _lvol("LV_BACK")
    back.ns_uuid = "LV_BACK"
    db = _FakeDB([dr, back], [_rep(dr, back, LVolReplication.STATE_CUTOVER_DONE)])
    entries = _connect(monkeypatch, db, "LV_BACK")
    assert [e.target_lvol_id for e in entries] == [None]


def test_failback_reports_the_persisted_wire_identity(monkeypatch):
    """After a fail-back the UUID swap gives the record its original id while
    the namespace keeps advertising the DR source's wire identity (that is
    what kept the client's multipath head alive through the cutover). Connect
    must report the persisted ns_uuid — no relationship walk needed."""
    back = _lvol("LV_BACK")
    back.ns_uuid = "LV_DR"
    db = _FakeDB([back])
    entries = _connect(monkeypatch, db, "LV_BACK")
    assert [e.target_lvol_id for e in entries] == ["LV_DR"]


def test_forward_migration_keeps_the_target_id_for_device_lookup(monkeypatch):
    """Requested volume on the SOURCE end: the redirect loop already reports the
    target id; the fail-back emission must not overwrite it."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_CUTOVER_DONE)])
    entries = _connect(monkeypatch, db, "LV_SRC")
    assert [e.target_lvol_id for e in entries] == ["LV_TGT"]


def test_failed_over_target_end_reports_no_device_lookup_id(monkeypatch):
    """A fail-over clone's NSUUID is its own uuid — connecting it by its own id
    needs no redirect, so nothing must be emitted."""
    src, tgt = _lvol("LV_SRC"), _lvol("LV_TGT")
    db = _FakeDB([src, tgt], [_rep(src, tgt, LVolReplication.STATE_FAILED_OVER)])
    entries = _connect(monkeypatch, db, "LV_TGT")
    assert [e.target_lvol_id for e in entries] == [None]
