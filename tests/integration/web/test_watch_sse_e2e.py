# coding=utf-8
"""End-to-end SSE watch: real app in a real uvicorn server on a real FDB.

Runs the actual ``simplyblock_web.app:app`` (including AccessLogMiddleware —
the BaseHTTPMiddleware whose interaction with streaming responses this pins)
in a background uvicorn thread and consumes the stream over a real socket
with ``requests``, so client-disconnect semantics are exercised for real.
"""

import json
import threading
import time
from uuid import uuid4

import pytest
import requests
import uvicorn

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_web.api.v2._watch import get_hub


@pytest.fixture(scope='module')
def base_url():
    from simplyblock_web.app import app
    from simplyblock_web.api.v2._auth import verify_api_token

    app.dependency_overrides[verify_api_token] = lambda: None
    config = uvicorn.Config(app, host='127.0.0.1', port=0, log_level='warning')
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        assert time.monotonic() < deadline, 'uvicorn did not start'
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f'http://127.0.0.1:{port}'
    server.should_exit = True
    thread.join(timeout=10)
    app.dependency_overrides.clear()


class SSEReader:
    """Minimal SSE parser over a requests streaming response."""

    def __init__(self, response):
        # requests' iter_lines mangles \r\n-terminated SSE lines (spurious
        # empty lines that are indistinguishable from event separators), so
        # buffer raw content and split on \n ourselves.
        self._content = response.iter_content(chunk_size=None, decode_unicode=True)
        self._buffer = ''

    def _readline(self):
        while '\n' not in self._buffer:
            try:
                self._buffer += next(self._content)
            except StopIteration:
                return None
        line, self._buffer = self._buffer.split('\n', 1)
        return line.rstrip('\r')

    def next_event(self):
        """Return (event, data) for the next event, or (None, None) on EOF."""
        name, data = None, []
        while True:
            line = self._readline()
            if line is None:
                return None, None
            if line == '':
                if name is not None or data:
                    return name, '\n'.join(data)
                continue
            if line.startswith(':'):
                continue
            field, _, value = line.partition(':')
            value = value.removeprefix(' ')
            if field == 'event':
                name = value
            elif field == 'data':
                data.append(value)


def _seed_cluster(db):
    cluster = Cluster({'uuid': str(uuid4()), 'status': Cluster.STATUS_ACTIVE})
    cluster.write_to_db(db.kv_store)
    return cluster


def _seed_pool(db, cluster):
    pool = Pool({
        'uuid': str(uuid4()),
        'cluster_id': cluster.get_id(),
        'pool_name': f'pool-{uuid4().hex[:8]}',
        'status': Pool.STATUS_ACTIVE,
    })
    pool.write_to_db(db.kv_store)
    return pool


def _seed_lvol(db, pool, name):
    lvol = LVol({
        'uuid': str(uuid4()),
        'pool_uuid': pool.get_id(),
        'pool_name': pool.pool_name,
        'lvol_name': name,
        'node_id': str(uuid4()),
        'status': LVol.STATUS_ONLINE,
        'size': 1024,
    })
    lvol.write_to_db(db.kv_store)
    return lvol


def _volumes_watch_url(base_url, cluster, pool):
    return (f'{base_url}/api/v2/clusters/{cluster.get_id()}'
            f'/storage-pools/{pool.get_id()}/volumes/?watch=true')


@pytest.mark.timeout(120)
def test_volume_watch_stream_flow(base_url):
    db = DBController()
    cluster = _seed_cluster(db)
    pool = _seed_pool(db, cluster)
    other_pool = _seed_pool(db, cluster)
    lvol = _seed_lvol(db, pool, 'watched-1')

    with requests.get(_volumes_watch_url(base_url, cluster, pool),
                      stream=True, timeout=(5, 30)) as response:
        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        reader = SSEReader(response)

        name, data = reader.next_event()
        assert name == 'snapshot'
        snapshot = json.loads(data)
        assert [v['id'] for v in snapshot] == [lvol.get_id()]

        # A volume in another pool must not produce an event; the next event
        # observed has to be the update of the watched volume.
        _seed_lvol(db, other_pool, 'other-pool-volume')
        lvol.status = LVol.STATUS_OFFLINE
        lvol.write_to_db(db.kv_store)
        name, data = reader.next_event()
        assert name == 'updated'
        assert json.loads(data)['id'] == lvol.get_id()
        assert json.loads(data)['status'] == LVol.STATUS_OFFLINE

        second = _seed_lvol(db, pool, 'watched-2')
        name, data = reader.next_event()
        assert name == 'created'
        assert json.loads(data)['id'] == second.get_id()

        # Soft delete: the entity leaves the filtered set but its key is
        # still retrievable — the event carries the final representation.
        second.status = LVol.STATUS_DELETED
        second.write_to_db(db.kv_store)
        name, data = reader.next_event()
        assert name == 'deleted'
        payload = json.loads(data)
        assert payload['id'] == second.get_id()
        assert payload['status'] == LVol.STATUS_DELETED

        # Physical removal: no representation left to return.
        lvol.remove(db.kv_store)
        name, data = reader.next_event()
        assert name == 'deleted'
        assert json.loads(data) == {}


@pytest.mark.timeout(120)
def test_parent_pool_deletion_closes_stream(base_url):
    db = DBController()
    cluster = _seed_cluster(db)
    pool = _seed_pool(db, cluster)
    _seed_lvol(db, pool, 'orphan-to-be')

    with requests.get(_volumes_watch_url(base_url, cluster, pool),
                      stream=True, timeout=(5, 30)) as response:
        reader = SSEReader(response)
        name, _ = reader.next_event()
        assert name == 'snapshot'

        pool.remove(db.kv_store)
        name, data = reader.next_event()
        assert (name, data) == (None, None)  # stream closed

    # Reconnect gets the endpoint's regular 404.
    response = requests.get(_volumes_watch_url(base_url, cluster, pool), timeout=5)
    assert response.status_code == 404


@pytest.mark.timeout(120)
def test_client_disconnect_unsubscribes(base_url):
    db = DBController()
    cluster = _seed_cluster(db)
    pool = _seed_pool(db, cluster)
    _seed_lvol(db, pool, 'disconnect-test')

    response = requests.get(_volumes_watch_url(base_url, cluster, pool),
                            stream=True, timeout=(5, 30))
    reader = SSEReader(response)
    name, _ = reader.next_event()
    assert name == 'snapshot'
    assert get_hub(LVol).subscriber_count() > 0

    response.close()

    deadline = time.monotonic() + 20
    while get_hub(LVol).subscriber_count() > 0 and time.monotonic() < deadline:
        time.sleep(0.2)
    assert get_hub(LVol).subscriber_count() == 0
