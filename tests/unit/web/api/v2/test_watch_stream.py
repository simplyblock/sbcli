# coding=utf-8
"""Watch stream builder: diffing, event ordering, ancestor close, secrets."""

import asyncio
import logging
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import BaseModel

from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_web.api.v2 import _watch
from simplyblock_web.api.v2._dtos import ClusterDTO, TaskDTO
from simplyblock_web.api.v2._watch import _Subscription, _build_diff, _build_snapshot, watch_response


class _DTO(BaseModel):
    uuid: str
    status: str = ''


def _dto(entity: dict) -> _DTO:
    return _DTO(uuid=entity['uuid'], status=entity.get('status', ''))


# ---- _build_snapshot / _build_diff (pure functions) ----

def test_snapshot_collection_is_json_array():
    events, cache = _build_snapshot(
        {b'k1': {'uuid': 'a'}, b'k2': {'uuid': 'b'}}, _dto, single=False)
    [event] = events
    assert event.event == 'snapshot'
    assert event.data.startswith('[') and event.data.endswith(']')
    assert '"a"' in event.data and '"b"' in event.data
    assert set(cache) == {b'k1', b'k2'}


def test_snapshot_single_is_bare_object():
    events, _ = _build_snapshot({b'k1': {'uuid': 'a'}}, _dto, single=True)
    [event] = events
    assert event.event == 'snapshot'
    assert event.data.startswith('{')


def test_diff_created_updated_deleted():
    prev = {b'k1': {'uuid': 'a', 'status': 'x'}, b'k2': {'uuid': 'b', 'status': 'x'}}
    _, cache = _build_snapshot(prev, _dto, single=False)
    new = {b'k2': {'uuid': 'b', 'status': 'y'}, b'k3': {'uuid': 'c', 'status': 'x'}}
    events, new_cache = _build_diff(prev, new, cache, _dto, single=False)
    by_name = {e.event: e.data for e in events}
    assert set(by_name) == {'created', 'updated', 'deleted'}
    assert '"c"' in by_name['created']
    assert '"b"' in by_name['updated'] and '"y"' in by_name['updated']
    assert '"a"' in by_name['deleted']  # last-known DTO
    assert set(new_cache) == {b'k2', b'k3'}


def test_diff_suppresses_changes_invisible_in_dto():
    """Raw dict changed (lease heartbeat) but the DTO is identical -> no event."""
    task = {
        'uuid': str(uuid4()), 'cluster_id': str(uuid4()), 'date': 1,
        'function_name': 'node_restart', 'status': 'running',
        'updated_at': '2026-01-01T00:00:00',
    }
    build = lambda d: TaskDTO.from_model(JobSchedule(d))  # noqa: E731
    prev = {b'k1': task}
    _, cache = _build_snapshot(prev, build, single=False)
    new = {b'k1': {**task, 'updated_at': '2026-01-01T00:00:05'}}
    events, _ = _build_diff(prev, new, cache, build, single=False)
    assert events == []


def test_diff_compound_keys():
    """JobSchedule FDB keys share a cluster prefix; diff must key exactly."""
    cluster_id = str(uuid4())
    t1 = {'uuid': str(uuid4()), 'cluster_id': cluster_id, 'date': 1,
          'function_name': 'node_restart', 'status': 'running'}
    t2 = {'uuid': str(uuid4()), 'cluster_id': cluster_id, 'date': 2,
          'function_name': 'node_restart', 'status': 'running'}
    build = lambda d: TaskDTO.from_model(JobSchedule(d))  # noqa: E731
    k1 = f'object/JobSchedule/{cluster_id}/1/{t1["uuid"]}'.encode()
    k2 = f'object/JobSchedule/{cluster_id}/2/{t2["uuid"]}'.encode()
    prev = {k1: t1, k2: t2}
    _, cache = _build_snapshot(prev, build, single=False)
    new = {k1: t1, k2: {**t2, 'status': 'done'}}
    events, _ = _build_diff(prev, new, cache, build, single=False)
    [event] = events
    assert event.event == 'updated'
    assert t2['uuid'] in event.data


def test_diff_single_mode_maps_created_to_updated():
    events, _ = _build_diff({}, {b'k1': {'uuid': 'a'}}, {}, _dto, single=True)
    [event] = events
    assert event.event == 'updated'


def test_diff_scope_leave_emits_deleted():
    """An entity leaving the projected set (filtered out) emits deleted."""
    project = lambda state: {  # noqa: E731
        k: d for k, d in state.items() if d.get('status') != 'deleted'}
    s1 = project({b'k1': {'uuid': 'a', 'status': 'online'}})
    _, cache = _build_snapshot(s1, _dto, single=False)
    s2 = project({b'k1': {'uuid': 'a', 'status': 'deleted'}})
    events, _ = _build_diff(s1, s2, cache, _dto, single=False)
    [event] = events
    assert event.event == 'deleted'


# ---- generator behavior with a fake hub ----

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

    def publish(self, state):
        self.cache = state
        for sub in list(self.subs):
            sub._deliver(state)

    def fail(self):
        for sub in list(self.subs):
            sub._fail()


class Thing:
    pass


class Parent:
    pass


def _patch(monkeypatch, hubs):
    monkeypatch.setattr(_watch, 'DBController', lambda: SimpleNamespace(kv_store=object()))
    monkeypatch.setattr(_watch, 'get_hub', lambda cls: hubs[cls])


async def _next_event(gen, timeout=2.0):
    return await asyncio.wait_for(gen.__anext__(), timeout)


def test_stream_snapshot_update_delete(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish({b'k1': {'uuid': 'a', 'status': 'online'}})

    async def scenario():
        response = watch_response(Thing, lambda s: s, _dto)
        gen = response.body_iterator
        event = await _next_event(gen)
        assert event.event == 'snapshot'

        hub.publish({b'k1': {'uuid': 'a', 'status': 'offline'}})
        event = await _next_event(gen)
        assert event.event == 'updated' and 'offline' in event.data

        hub.publish({})
        event = await _next_event(gen)
        assert event.event == 'deleted'

        await gen.aclose()
        assert hub.subs == []

    asyncio.run(scenario())


def test_stream_coalesces_bursts(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish({b'k1': {'uuid': 'a', 'status': 's0'}})

    async def scenario():
        gen = watch_response(Thing, lambda s: s, _dto).body_iterator
        assert (await _next_event(gen)).event == 'snapshot'

        # Two publications without yielding to the generator: only the
        # latest state is diffed.
        hub.publish({b'k1': {'uuid': 'a', 'status': 's1'}})
        hub.publish({b'k1': {'uuid': 'a', 'status': 's2'}})
        event = await _next_event(gen)
        assert event.event == 'updated' and 's2' in event.data

        with pytest.raises(asyncio.TimeoutError):
            await _next_event(gen, timeout=0.2)

    asyncio.run(scenario())


def test_detail_stream_closes_after_delete(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish({b'k1': {'uuid': 'a', 'status': 'online'}})

    async def scenario():
        gen = watch_response(
            Thing,
            lambda s: {k: d for k, d in s.items() if d['uuid'] == 'a'},
            _dto,
            single_id='a',
        ).body_iterator
        assert (await _next_event(gen)).event == 'snapshot'
        hub.publish({})
        assert (await _next_event(gen)).event == 'deleted'
        with pytest.raises(StopAsyncIteration):
            await _next_event(gen)
        assert hub.subs == []

    asyncio.run(scenario())


def test_ancestor_deletion_closes_stream(monkeypatch):
    hub, parent_hub = FakeHub(), FakeHub()
    _patch(monkeypatch, {Thing: hub, Parent: parent_hub})
    hub.publish({b'k1': {'uuid': 'a'}})
    parent_hub.publish({b'p1': {'uuid': 'p'}})

    async def scenario():
        gen = watch_response(
            Thing, lambda s: s, _dto, ancestors=[(Parent, 'p')],
        ).body_iterator
        assert (await _next_event(gen)).event == 'snapshot'
        parent_hub.publish({})
        with pytest.raises(StopAsyncIteration):
            await _next_event(gen)
        assert hub.subs == [] and parent_hub.subs == []

    asyncio.run(scenario())


def test_hub_failure_emits_error_and_closes(monkeypatch):
    hub = FakeHub()
    _patch(monkeypatch, {Thing: hub})
    hub.publish({b'k1': {'uuid': 'a'}})

    async def scenario():
        gen = watch_response(Thing, lambda s: s, _dto).body_iterator
        assert (await _next_event(gen)).event == 'snapshot'
        hub.fail()
        event = await _next_event(gen)
        assert event.event == 'error'
        assert 'backend unavailable' in event.data
        with pytest.raises(StopAsyncIteration):
            await _next_event(gen)

    asyncio.run(scenario())


def test_kv_store_unavailable_yields_503(monkeypatch):
    from fastapi import HTTPException
    monkeypatch.setattr(_watch, 'DBController', lambda: SimpleNamespace(kv_store=None))
    with pytest.raises(HTTPException) as exc_info:
        watch_response(Thing, lambda s: s, _dto)
    assert exc_info.value.status_code == 503


# ---- device projection (importable helper) ----

def test_device_projection_explodes_node():
    from simplyblock_web.api.v2.cluster.storage_node.device import _make_device_projection
    state = {
        b'n1': {'uuid': 'node-1', 'nvme_devices': [
            {'uuid': 'dev-1', 'status': 'online'},
            {'uuid': 'dev-2', 'status': 'unavailable'},
        ]},
        b'n2': {'uuid': 'node-2', 'nvme_devices': [{'uuid': 'dev-3'}]},
    }
    assert set(_make_device_projection('node-1')(state)) == {'dev-1', 'dev-2'}
    assert set(_make_device_projection('node-1', 'dev-2')(state)) == {'dev-2'}
    assert _make_device_projection('node-404')(state) == {}


# ---- secrets on the stream (ClusterDTO carries `secret`) ----

def _cluster_dict(secret='hunter2'):
    return {'uuid': str(uuid4()), 'secret': secret, 'status': 'active'}


def _cluster_dto(data: dict) -> ClusterDTO:
    return ClusterDTO.from_model(ClusterModel(data), None)


def test_stream_payload_unwraps_secret_on_wire(caplog):
    """Same behavior as the plain GET: JSON wire carries plaintext."""
    with caplog.at_level(logging.DEBUG):
        events, _ = _build_snapshot({b'k1': _cluster_dict()}, _cluster_dto, single=False)
    [event] = events
    assert 'hunter2' in event.data
    assert 'hunter2' not in caplog.text


def test_dto_python_mode_masks_secret():
    dto = _cluster_dto(_cluster_dict())
    assert 'hunter2' not in repr(dto)
    assert str(dto.model_dump()['secret']) == '**********'
