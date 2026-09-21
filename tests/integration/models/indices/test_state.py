"""Index state: the scan fallback, the kill switch, and converging on a switch."""
from simplyblock_core import index_ops
from simplyblock_core.models import indices
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.utils import ttl_cache

from .helpers import CLUSTER, POOL, index_keys, make_lvol, ready


def test_state_defaults_to_building(db):
    assert db.index_state(LVol, 'pool_uuid') == indices.STATE_BUILDING


def test_building_falls_back_to_the_scan_and_agrees_with_ready(db):
    """The strongest single assertion available: index and scan pinned to the
    same answer, which is what makes the rollout switchable at all."""
    for name in ("vol-a", "vol-b", "vol-c"):
        make_lvol(name).write_to_db(db.kv_store)
    make_lvol("elsewhere", pool="pool-other").write_to_db(db.kv_store)

    scanned = [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)]
    ready(LVol)
    indexed = [v.get_id() for v in db.query(LVol, 'pool_uuid', POOL)]

    assert scanned == indexed
    assert len(indexed) == 3


def test_ordered_index_agrees_between_scan_and_index(db):
    """Ordering, not just membership: `limit`/`reverse` must mean the same on
    both paths or the chain tail moves when the index flips."""
    from simplyblock_core.models.snapshot import SnapShot

    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    for seq in (30, 10, 20):
        snap = SnapShot()
        snap.uuid = f"snap-{seq}"
        snap.snap_name = f"snap-{seq}"
        snap.cluster_id = CLUSTER
        snap.pool_uuid = POOL
        snap.created_at = seq
        snap.vuid = seq
        snap.lvol = lvol
        snap.write_to_db(db.kv_store)

    scanned = db.get_lvol_latest_snapshot(lvol.get_id())
    ready(SnapShot)
    indexed = db.get_lvol_latest_snapshot(lvol.get_id())

    assert scanned.get_id() == indexed.get_id() == "snap-30"


def test_disabled_is_a_kill_switch_for_reads_and_writes(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    index_ops.disable_index(LVol, 'node_id')

    # The read still answers — from the scan.
    assert len(db.query(LVol, 'node_id', "node-1")) == 1

    # And the write no longer touches that index's keys.
    before = {k for k in index_keys(db, LVol) if b'/node_id/' in k}
    make_lvol("vol-b").write_to_db(db.kv_store)
    after = {k for k in index_keys(db, LVol) if b'/node_id/' in k}
    assert before == after


def test_disabling_leaves_the_entries_in_place(db):
    """Deliberate: another process may hold `ready` from its TTL cache for a
    few seconds yet, and an emptied index would answer its reads with no rows
    instead of sending them to the scan."""
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    before = {k for k in index_keys(db, LVol) if b'/node_id/' in k}

    index_ops.disable_index(LVol, 'node_id')

    assert {k for k in index_keys(db, LVol) if b'/node_id/' in k} == before


def _record_waits(db, monkeypatch, model_cls, index_name):
    """Make the convergence wait observable: what the switch looks like from
    another process at the moment it starts waiting."""
    monkeypatch.setattr(ttl_cache, 'INDEX_STATE_CONVERGENCE_SEC', 30)
    waits = []
    monkeypatch.setattr(index_ops.time, 'sleep', lambda delay: waits.append((
        delay,
        db.index_meta(model_cls, index_name).get('state'),
        {k for k in index_keys(db, model_cls) if f'/{index_name}/'.encode() in k},
    )))
    return waits


def test_disabling_waits_until_every_process_has_seen_it(db, monkeypatch):
    """The state is TTL-cached per process, so a kill switch that returned on
    the commit would return before it was a kill switch."""
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.disable_index(LVol, 'node_id')

    assert [(delay, state) for delay, state, _keys in waits] == [
        (30, indices.STATE_DISABLED)]


def test_enabling_clears_only_after_the_disable_has_converged(db, monkeypatch):
    """A process still holding `ready` from its cache would read the emptied
    index as "no such records" — the answer the kill switch exists to avoid."""
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    index_ops.disable_index(LVol, 'node_id')
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    assert len(waits) == 1
    _delay, state, keys = waits[0]
    assert state == indices.STATE_DISABLED
    assert keys, "the index was emptied before the readers had converged"
    assert not any(b'/node_id/' in k for k in index_keys(db, LVol))


def test_enabling_a_ready_index_takes_it_out_of_service_first(db, monkeypatch):
    """`--set building` on an index that was never disabled is the same race:
    the clear must not run while readers still trust it."""
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    waits = _record_waits(db, monkeypatch, LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    assert [state for _delay, state, _keys in waits] == [indices.STATE_DISABLED]
    assert db.index_state(LVol, 'node_id') == indices.STATE_BUILDING


def test_enabling_clears_only_its_own_index(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    index_ops.disable_index(LVol, 'node_id')

    index_ops.enable_index(LVol, 'node_id')

    keys = index_keys(db, LVol)
    assert not any(b'/node_id/' in k for k in keys)
    assert any(b'/pool_uuid/' in k for k in keys)


def test_enabling_pool_uuid_leaves_the_composite_index_alone(db):
    """`index/LVol/pool_uuid/` must not reach `index/LVol/pool_uuid+lvol_name/`."""
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    index_ops.disable_index(LVol, 'pool_uuid')

    index_ops.enable_index(LVol, 'pool_uuid')

    keys = index_keys(db, LVol)
    assert not any(b'/pool_uuid/' in k for k in keys)
    assert any(b'/pool_uuid+lvol_name/' in k for k in keys)


def test_reenabling_after_a_change_does_not_leave_a_stale_entry(db):
    """The reason enabling clears: while an index is out of service nothing
    maintains it, and the backfill that follows only ever *adds*."""
    lvol = make_lvol("vol-a", node="node-1")
    lvol.write_to_db(db.kv_store)
    ready(LVol)

    index_ops.disable_index(LVol, 'node_id')
    lvol.node_id = "node-2"
    lvol.write_to_db(db.kv_store)

    index_ops.enable_index(LVol, 'node_id')
    assert db.index_state(LVol, 'node_id') == indices.STATE_BUILDING
    index_ops.build_indices([LVol])

    assert db.index_state(LVol, 'node_id') == indices.STATE_READY
    assert db.query(LVol, 'node_id', "node-1") == []
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-2")] == [lvol.get_id()]
    assert index_ops.check_indices([LVol])['stale'] == []


def test_index_states_reports_every_declared_index(db):
    ready(LVol)
    index_ops.disable_index(LVol, 'node_id')

    states = {row['index']: row['state'] for row in index_ops.index_states(LVol)}

    assert states['LVol.node_id'] == indices.STATE_DISABLED
    assert states['LVol.pool_uuid'] == indices.STATE_READY
    assert set(states) == {f'LVol.{index.name}' for index in indices.indexes_of(LVol)}
