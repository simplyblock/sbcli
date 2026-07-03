# coding=utf-8
"""SSE framing (_sse.py): snapshot/diff rendering, suppression, secrets."""

import asyncio
import logging
from uuid import uuid4

import pytest
from pydantic import BaseModel

from simplyblock_core.watch import ChangeEvent, WatchUnavailable
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_web.api.v2 import _sse
from simplyblock_web.api.v2._sse import _build_events, _build_snapshot, sse_response
from simplyblock_web.api.v2._dtos import ClusterDTO, TaskDTO


class FakeModel:
    def __init__(self, entity_id, **fields):
        self._id = entity_id
        self._fields = fields

    def get_id(self):
        return self._id

    def to_dict(self):
        return {'uuid': self._id, **self._fields}


class _DTO(BaseModel):
    id: str
    status: str = ''


def _dto(model) -> _DTO:
    return _DTO(id=model.get_id(), status=model.to_dict().get('status', ''))


def _created(entity_id, **f):
    return ChangeEvent('created', entity_id, FakeModel(entity_id, **f))


def _updated(entity_id, **f):
    return ChangeEvent('updated', entity_id, FakeModel(entity_id, **f))


def _deleted(entity_id, model=None):
    return ChangeEvent('deleted', entity_id, model)


# ---- _build_snapshot ----

def test_snapshot_collection_is_json_array():
    events, cache = _build_snapshot([_created('a'), _created('b')], _dto, single=False)
    [event] = events
    assert event.event == 'snapshot'
    assert event.data.startswith('[') and event.data.endswith(']')
    assert '"a"' in event.data and '"b"' in event.data
    assert set(cache) == {'a', 'b'}


def test_snapshot_single_is_bare_object():
    events, _ = _build_snapshot([_created('a')], _dto, single=True)
    [event] = events
    assert event.event == 'snapshot'
    assert event.data.startswith('{')


def test_snapshot_single_empty_emits_nothing():
    events, cache = _build_snapshot([], _dto, single=True)
    assert events == [] and cache == {}


# ---- _build_events ----

def test_events_created_updated_deleted():
    _, cache = _build_snapshot([_created('a', status='x')], _dto, single=False)
    events, new_cache, deleted = _build_events(
        [_updated('a', status='y'), _created('c', status='x'), _deleted('b')],
        cache, _dto, single=False)
    by_name = {e.event: e.data for e in events}
    assert set(by_name) == {'updated', 'created', 'deleted'}
    assert '"y"' in by_name['updated']
    assert '"c"' in by_name['created']
    assert by_name['deleted'] == '{}'  # physically gone: no representation
    assert deleted is True
    assert set(new_cache) == {'a', 'c'}


def test_events_suppress_dto_invisible_update():
    """Raw model changed (lease heartbeat) but the DTO is identical -> no event."""
    task = {
        'uuid': str(uuid4()), 'cluster_id': str(uuid4()), 'date': 1,
        'function_name': 'node_restart', 'status': 'running',
        'updated_at': '2026-01-01T00:00:00',
    }
    build = lambda m: TaskDTO.from_model(m)  # noqa: E731
    js1 = JobSchedule(task)
    _, cache, _ = _build_events([ChangeEvent('created', js1.get_id(), js1)], {}, build, single=False)
    js2 = JobSchedule({**task, 'updated_at': '2026-01-01T00:00:05'})
    events, _, _ = _build_events([ChangeEvent('updated', js2.get_id(), js2)], cache, build, single=False)
    assert events == []


def test_events_single_maps_created_to_updated():
    events, _, _ = _build_events([_created('a')], {}, _dto, single=True)
    [event] = events
    assert event.event == 'updated'


def test_events_deleted_carries_current_state():
    events, _, _ = _build_events(
        [_deleted('a', FakeModel('a', status='deleted'))], {}, _dto, single=False)
    [event] = events
    assert event.event == 'deleted'
    assert '"deleted"' in event.data


# ---- sse_response generator ----

class FakeStream:
    def __init__(self, batches):
        self._it = iter(batches)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self):
        self.closed = True


class FailingStream:
    def __init__(self):
        self.n = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.n += 1
        if self.n == 1:
            return [_created('a')]
        raise WatchUnavailable()

    async def aclose(self):
        pass


async def _event(gen, timeout=2.0):
    return await asyncio.wait_for(gen.__anext__(), timeout)


def test_stream_snapshot_update_delete(monkeypatch):
    monkeypatch.setattr(_sse, 'backend_available', lambda: True)

    async def scenario():
        stream = FakeStream([[_created('a', status='online')],
                             [_updated('a', status='offline')],
                             [_deleted('a')]])
        gen = sse_response(stream, _dto).body_iterator
        assert (await _event(gen)).event == 'snapshot'
        event = await _event(gen)
        assert event.event == 'updated' and 'offline' in event.data
        assert (await _event(gen)).event == 'deleted'
        with pytest.raises(StopAsyncIteration):
            await _event(gen)
        assert stream.closed

    asyncio.run(scenario())


def test_detail_stream_closes_after_delete(monkeypatch):
    monkeypatch.setattr(_sse, 'backend_available', lambda: True)

    async def scenario():
        stream = FakeStream([[_created('a')], [_deleted('a')]])
        gen = sse_response(stream, _dto, single=True).body_iterator
        assert (await _event(gen)).event == 'snapshot'
        assert (await _event(gen)).event == 'deleted'
        with pytest.raises(StopAsyncIteration):
            await _event(gen)

    asyncio.run(scenario())


def test_stream_failure_emits_error_and_closes(monkeypatch):
    monkeypatch.setattr(_sse, 'backend_available', lambda: True)

    async def scenario():
        gen = sse_response(FailingStream(), _dto).body_iterator
        assert (await _event(gen)).event == 'snapshot'
        event = await _event(gen)
        assert event.event == 'error' and 'backend unavailable' in event.data
        with pytest.raises(StopAsyncIteration):
            await _event(gen)

    asyncio.run(scenario())


def test_kv_store_unavailable_yields_503(monkeypatch):
    from fastapi import HTTPException
    monkeypatch.setattr(_sse, 'backend_available', lambda: False)
    with pytest.raises(HTTPException) as exc_info:
        sse_response(FakeStream([]), _dto)
    assert exc_info.value.status_code == 503


# ---- secrets on the stream (ClusterDTO carries `secret`) ----

def test_stream_payload_unwraps_secret_on_wire(caplog):
    """Same behavior as the plain GET: JSON wire carries plaintext."""
    cluster = ClusterModel({'uuid': str(uuid4()), 'secret': 'hunter2', 'status': 'active'})
    build = lambda m: ClusterDTO.from_model(m, None)  # noqa: E731
    with caplog.at_level(logging.DEBUG):
        events, _ = _build_snapshot(
            [ChangeEvent('created', cluster.get_id(), cluster)], build, single=False)
    [event] = events
    assert 'hunter2' in event.data
    assert 'hunter2' not in caplog.text


def test_dto_python_mode_masks_secret():
    cluster = ClusterModel({'uuid': str(uuid4()), 'secret': 'hunter2', 'status': 'active'})
    dto = ClusterDTO.from_model(cluster, None)
    assert 'hunter2' not in repr(dto)
    assert str(dto.model_dump()['secret']) == '**********'
