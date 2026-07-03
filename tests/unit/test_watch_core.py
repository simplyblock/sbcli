# coding=utf-8
"""Core watch primitive: diff_by_id and the async watch() generator."""

import asyncio

import pytest

from simplyblock_core import watch as watchmod
from simplyblock_core.watch import (
    WatchUnavailable, _Subscription, diff_by_id, watch,
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
    """Left the scoped set but still present in full -> deleted carries the
    current (unscoped) model, not a stale one."""
    prev = {'a': _m('a', status='online').to_dict()}
    current = _m('a', status='deleted')
    events, _ = diff_by_id(prev, {}, {'a': current})
    [event] = events
    assert event.kind == 'deleted' and event.model is current


def test_diff_created_order_follows_new():
    events, _ = diff_by_id({}, {'a': _m('a'), 'b': _m('b')}, {'a': _m('a'), 'b': _m('b')})
    assert [e.id for e in events] == ['a', 'b']


# ---- watch() generator with a fake hub ----

class FakeHub:
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


def _patch(monkeypatch, hubs):
    monkeypatch.setattr(watchmod, 'get_hub', lambda cls: hubs[cls])


async def _next(gen, timeout=2.0):
    return await asyncio.wait_for(gen.__anext__(), timeout)


def test_watch_snapshot_update_delete(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish([_m('a', status='online')])

    async def scenario():
        gen = watch(Thing)
        [event] = await _next(gen)
        assert event.kind == 'created' and event.id == 'a'

        hub.publish([_m('a', status='offline')])
        [event] = await _next(gen)
        assert event.kind == 'updated' and event.model.to_dict()['status'] == 'offline'

        hub.publish([])
        [event] = await _next(gen)
        assert event.kind == 'deleted' and event.id == 'a'

        await gen.aclose()
        assert hub.subs == []

    asyncio.run(scenario())


def test_watch_coalesces_bursts(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish([_m('a', status='s0')])

    async def scenario():
        gen = watch(Thing)
        assert (await _next(gen))[0].kind == 'created'

        # Two publications before the generator runs: only the latest diffs.
        hub.publish([_m('a', status='s1')])
        hub.publish([_m('a', status='s2')])
        [event] = await _next(gen)
        assert event.kind == 'updated' and event.model.to_dict()['status'] == 's2'

        with pytest.raises(asyncio.TimeoutError):
            await _next(gen, timeout=0.2)

    asyncio.run(scenario())


def test_watch_scope_select(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish([_m('a', keep=True), _m('b', keep=False)])

    async def scenario():
        gen = watch(Thing, select=lambda models: [m for m in models if m.to_dict()['keep']])
        events = await _next(gen)
        assert [e.id for e in events] == ['a']

    asyncio.run(scenario())


def test_watch_ancestor_deletion_closes(monkeypatch):
    hub, parent = FakeHub(), FakeHub()
    _patch(monkeypatch, {Thing: hub, Parent: parent})
    hub.publish([_m('a')])
    parent.publish([_m('p')])

    async def scenario():
        gen = watch(Thing, ancestors=[(Parent, 'p')])
        assert (await _next(gen))[0].id == 'a'
        parent.publish([])
        with pytest.raises(StopAsyncIteration):
            await _next(gen)
        assert hub.subs == [] and parent.subs == []

    asyncio.run(scenario())


def test_watch_failure_raises(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish([_m('a')])

    async def scenario():
        gen = watch(Thing)
        await _next(gen)
        hub.fail()
        with pytest.raises(WatchUnavailable):
            await _next(gen)

    asyncio.run(scenario())
