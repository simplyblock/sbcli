# coding=utf-8
"""Core watch primitive: diff_by_id, ScopeWatch index-diff, and watch()."""

import asyncio
import json
import struct

import pytest

from simplyblock_core import watch as watchmod
from simplyblock_core import watches
from simplyblock_core.models.pool import Pool
from simplyblock_core.watch import (
    ScopeWatch, WatchUnavailable, _Subscription, diff_by_id, watch,
)


class FakeModel:
    def __init__(self, entity_id, **fields):
        self._id = entity_id
        self._fields = fields

    def get_id(self):
        return self._id

    def to_dict(self):
        return {'uuid': self._id, **self._fields}


def _m(entity_id, **fields):
    return FakeModel(entity_id, **fields)


# ---- diff_by_id (pure function) ----

def test_diff_created_updated_deleted():
    prev = {'a': _m('a', status='x').to_dict(), 'b': _m('b', status='x').to_dict()}
    b2, c = _m('b', status='y'), _m('c', status='x')
    new = {'b': b2, 'c': c}
    events, new_dicts = diff_by_id(prev, new, dict(new))
    by_kind = {e.kind: e for e in events}
    assert set(by_kind) == {'created', 'updated', 'deleted'}
    assert by_kind['created'].id == 'c'
    assert by_kind['updated'].id == 'b' and by_kind['updated'].model is b2
    assert by_kind['deleted'].id == 'a'
    assert by_kind['deleted'].model is None  # 'a' absent from full: physically gone
    assert set(new_dicts) == {'b', 'c'}


def test_diff_unchanged_emits_nothing():
    prev = {'a': _m('a', status='x').to_dict()}
    events, _ = diff_by_id(prev, {'a': _m('a', status='x')}, {'a': _m('a', status='x')})
    assert events == []


def test_diff_scope_leave_carries_current_state():
    prev = {'a': _m('a', status='online').to_dict()}
    current = _m('a', status='deleted')
    events, _ = diff_by_id(prev, {}, {'a': current})
    [event] = events
    assert event.kind == 'deleted' and event.model is current


def test_diff_created_order_follows_new():
    events, _ = diff_by_id({}, {'a': _m('a'), 'b': _m('b')}, {'a': _m('a'), 'b': _m('b')})
    assert [e.id for e in events] == ['a', 'b']


# ---- ScopeWatch._read (version-map diff + point-read), no FDB ----

class FakeKV:
    def __init__(self):
        self.data = {}

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        prefix = bytes(prefix)
        items = [(k, v) for k, v in self.data.items() if k.startswith(prefix)]
        items.sort(key=lambda kv: kv[0], reverse=reverse)
        return items[:limit] if limit else items


def _write_obj(kv, model):
    kv.data[model.get_db_id().encode()] = json.dumps(model.to_dict(unwrap_secrets=True)).encode()


def _set_version(kv, cls, scope, leaf, n):
    kv.data[watches.watch_index_version_key(cls, scope, leaf)] = struct.pack('<q', n)


def _clear_version(kv, cls, scope, leaf):
    kv.data.pop(watches.watch_index_version_key(cls, scope, leaf), None)


def test_scopewatch_list_index_diff():
    kv = FakeKV()
    sw = ScopeWatch(Pool, ('cl-1',), None)

    assert sw._read(kv) == ([], True)     # first read, empty scope
    assert sw._read(kv)[1] is False        # nothing changed

    pool = Pool({'uuid': 'p1', 'cluster_id': 'cl-1', 'pool_name': 'x'})
    _write_obj(kv, pool)
    _set_version(kv, Pool, ('cl-1',), 'p1', 1)
    entities, changed = sw._read(kv)
    assert changed and [p.get_id() for p in entities] == ['p1']
    assert sw._read(kv)[1] is False        # version unchanged -> no re-publish

    pool.status = Pool.STATUS_INACTIVE
    _write_obj(kv, pool)
    _set_version(kv, Pool, ('cl-1',), 'p1', 2)  # a write bumps the version
    entities, changed = sw._read(kv)
    assert changed and entities[0].status == Pool.STATUS_INACTIVE  # re-read the changed entity

    _clear_version(kv, Pool, ('cl-1',), 'p1')   # remove clears the version key
    entities, changed = sw._read(kv)
    assert changed and entities == []


def test_scopewatch_scoped_to_its_subtree():
    kv = FakeKV()
    other = Pool({'uuid': 'p2', 'cluster_id': 'cl-2', 'pool_name': 'y'})
    _write_obj(kv, other)
    _set_version(kv, Pool, ('cl-2',), 'p2', 1)  # a pool in a different cluster
    sw = ScopeWatch(Pool, ('cl-1',), None)
    entities, _ = sw._read(kv)
    assert entities == []  # out-of-scope entity is invisible


def test_scopewatch_detail_read():
    kv = FakeKV()
    pool = Pool({'uuid': 'p1', 'cluster_id': 'cl-1', 'pool_name': 'x'})
    sw = ScopeWatch(Pool, ('cl-1',), 'p1')

    assert sw._read(kv) == ([], True)  # not present yet
    _write_obj(kv, pool)
    _set_version(kv, Pool, ('cl-1',), 'p1', 1)
    entities, changed = sw._read(kv)
    assert changed and [p.get_id() for p in entities] == ['p1']
    _clear_version(kv, Pool, ('cl-1',), 'p1')
    entities, changed = sw._read(kv)
    assert changed and entities == []


# ---- watch() generator with a fake scope watch ----

class FakeScopeWatch:
    def __init__(self):
        self.subs = []
        self.cache = None

    def subscribe(self, loop, notify):
        sub = _Subscription(loop, notify)
        self.subs.append(sub)
        if self.cache is not None:
            sub._deliver(self.cache)
        return sub

    def unsubscribe(self, sub):
        self.subs.remove(sub)

    def publish(self, models):
        self.cache = models
        for sub in list(self.subs):
            sub._deliver(models)

    def fail(self):
        for sub in list(self.subs):
            sub._fail()


class Thing:
    pass


class Parent:
    pass


def _patch(monkeypatch, watches_map):
    monkeypatch.setattr(
        watchmod, 'get_scope_watch',
        lambda cls, scope=(), entity_id=None: watches_map[(cls, tuple(scope), entity_id)])


async def _next(gen, timeout=2.0):
    return await asyncio.wait_for(gen.__anext__(), timeout)


def test_watch_snapshot_update_delete(monkeypatch):
    sw = FakeScopeWatch()
    _patch(monkeypatch, {(Thing, (), None): sw})
    sw.publish([_m('a', status='online')])

    async def scenario():
        gen = watch(Thing)
        [event] = await _next(gen)
        assert event.kind == 'created' and event.id == 'a'

        sw.publish([_m('a', status='offline')])
        [event] = await _next(gen)
        assert event.kind == 'updated' and event.model.to_dict()['status'] == 'offline'

        sw.publish([])
        [event] = await _next(gen)
        assert event.kind == 'deleted' and event.id == 'a'

        await gen.aclose()
        assert sw.subs == []

    asyncio.run(scenario())


def test_watch_coalesces_bursts(monkeypatch):
    sw = FakeScopeWatch()
    _patch(monkeypatch, {(Thing, (), None): sw})
    sw.publish([_m('a', status='s0')])

    async def scenario():
        gen = watch(Thing)
        assert (await _next(gen))[0].kind == 'created'

        sw.publish([_m('a', status='s1')])
        sw.publish([_m('a', status='s2')])
        [event] = await _next(gen)
        assert event.kind == 'updated' and event.model.to_dict()['status'] == 's2'

        with pytest.raises(asyncio.TimeoutError):
            await _next(gen, timeout=0.2)

    asyncio.run(scenario())


def test_watch_scope_select(monkeypatch):
    sw = FakeScopeWatch()
    _patch(monkeypatch, {(Thing, (), None): sw})
    sw.publish([_m('a', keep=True), _m('b', keep=False)])

    async def scenario():
        gen = watch(Thing, select=lambda models: [m for m in models if m.to_dict()['keep']])
        events = await _next(gen)
        assert [e.id for e in events] == ['a']

    asyncio.run(scenario())


def test_watch_ancestor_deletion_closes(monkeypatch):
    sw, parent = FakeScopeWatch(), FakeScopeWatch()
    _patch(monkeypatch, {(Thing, (), None): sw, (Parent, (), 'p'): parent})
    sw.publish([_m('a')])
    parent.publish([_m('p')])

    async def scenario():
        gen = watch(Thing, ancestors=[(Parent, (), 'p')])
        assert (await _next(gen))[0].id == 'a'
        parent.publish([])  # parent gone
        with pytest.raises(StopAsyncIteration):
            await _next(gen)
        assert sw.subs == [] and parent.subs == []

    asyncio.run(scenario())


def test_watch_failure_raises(monkeypatch):
    sw = FakeScopeWatch()
    _patch(monkeypatch, {(Thing, (), None): sw})
    sw.publish([_m('a')])

    async def scenario():
        gen = watch(Thing)
        await _next(gen)
        sw.fail()
        with pytest.raises(WatchUnavailable):
            await _next(gen)

    asyncio.run(scenario())
