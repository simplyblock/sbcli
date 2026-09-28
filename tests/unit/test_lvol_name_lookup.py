"""A volume name lookup must not answer with a tombstone by default.

``status == deleted`` records were written by the pre-2026-05 force-delete path
(lvol_controller before 4467d716d) and never removed; a delete now removes the
record outright, so they only exist on upgraded clusters. They stay invisible to
every list helper via ``DBController._live_lvols``, and ``get_lvol_by_name``
inherited that filter by routing through ``get_lvols()`` — until it was
re-expressed over ``query()``, which has no such filter.

A tombstone still occupies the pool+name uniqueness slot, though, so the two
callers that are about to create under the name have to see it. These tests pin
both halves, and the ambiguity in between: a tombstone must not turn a name that
resolves into ``ValueError('Multiple values present')``.

The fake store holds no ``index_meta/`` records, so every index reads as still
building and the query layer takes its scan fallback. The filter sits above
``query``, so both sides of the flip are filtered identically.
"""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol


LIVE_POOL = 'pool-1'
TOMB_POOL = 'pool-2'
SHARED_NAME = 'vol-a'
TOMB_ONLY_NAME = 'vol-gone'


class _FakeKV:
    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data.get(key)

    def clear(self, key):
        self._data.pop(key, None)

    def add(self, key, value):
        self._data[key] = value

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        rows = sorted((k, v) for k, v in self._data.items() if k.startswith(prefix))
        if reverse:
            rows.reverse()
        if limit:
            rows = rows[:limit]
        return rows


def _lvol(uuid, name, pool, status):
    return LVol({
        'uuid': uuid,
        'lvol_name': name,
        'pool_uuid': pool,
        'node_id': 'node-1',
        'status': status,
    })


@pytest.fixture
def db():
    instance = DBController()
    instance.kv_store = _FakeKV()

    for lvol in (
        _lvol('lv-live', SHARED_NAME, LIVE_POOL, LVol.STATUS_ONLINE),
        _lvol('lv-tomb', SHARED_NAME, TOMB_POOL, LVol.STATUS_DELETED),
        _lvol('lv-tomb-only', TOMB_ONLY_NAME, TOMB_POOL, LVol.STATUS_DELETED),
    ):
        lvol.write_to_db(instance.kv_store)

    return instance


def test_tombstone_is_not_found_by_name(db):
    with pytest.raises(KeyError):
        db.get_lvol_by_name(TOMB_ONLY_NAME)


def test_tombstone_is_not_found_within_its_pool(db):
    with pytest.raises(KeyError):
        db.get_lvol_by_name(TOMB_ONLY_NAME, TOMB_POOL)


def test_include_deleted_returns_the_tombstone(db):
    assert db.get_lvol_by_name(TOMB_ONLY_NAME, include_deleted=True).get_id() == 'lv-tomb-only'
    assert db.get_lvol_by_name(
        TOMB_ONLY_NAME, TOMB_POOL, include_deleted=True).get_id() == 'lv-tomb-only'


def test_tombstone_does_not_make_a_shared_name_ambiguous(db):
    """The filter runs before ``single_or_none``, not after it."""
    assert db.get_lvol_by_name(SHARED_NAME).get_id() == 'lv-live'


def test_include_deleted_still_reports_a_genuinely_ambiguous_name(db):
    with pytest.raises(ValueError):
        db.get_lvol_by_name(SHARED_NAME, include_deleted=True)


def test_live_volume_resolves_within_its_pool(db):
    assert db.get_lvol_by_name(SHARED_NAME, LIVE_POOL).get_id() == 'lv-live'
