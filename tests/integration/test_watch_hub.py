# coding=utf-8
"""WatchHub against real FDB: watch firing, reconcile poll, teardown."""

import asyncio
import json
import time
from uuid import uuid4

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core import watch
from simplyblock_core.watch import _UNSET, WatchHub


def _pool_dict(pool_id=None):
    return {
        'uuid': pool_id or str(uuid4()),
        'cluster_id': str(uuid4()),
        'pool_name': 'hub-test-pool',
        'status': Pool.STATUS_ACTIVE,
    }


async def _await_publication(sub, notify, timeout=10.0):
    """Wait until the subscription has a fresh state and return it."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'no publication within timeout'
        await asyncio.wait_for(notify.wait(), remaining)
        notify.clear()
        state = sub.take()
        if state is not _UNSET:
            return state


@pytest.mark.timeout(60)
def test_hub_publishes_writes_and_stops_on_unsubscribe():
    db = DBController()

    async def scenario():
        loop = asyncio.get_running_loop()
        notify = asyncio.Event()
        hub = WatchHub(Pool)
        sub = hub.subscribe(loop, notify)
        try:
            # Initial publication (whatever the range currently holds).
            await _await_publication(sub, notify)

            pool = Pool(_pool_dict())
            started = time.monotonic()
            await asyncio.to_thread(pool.write_to_db, db.kv_store)
            state = await _await_publication(sub, notify, timeout=5.0)
            assert time.monotonic() - started < 5.0
            assert any(m.get_id() == pool.get_id() for m in state)

            await asyncio.to_thread(pool.remove, db.kv_store)
            state = await _await_publication(sub, notify, timeout=5.0)
            assert not any(m.get_id() == pool.get_id() for m in state)
        finally:
            hub.unsubscribe(sub)

        thread = hub._thread
        deadline = time.monotonic() + 10
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        assert not thread.is_alive()

    asyncio.run(scenario())


@pytest.mark.timeout(60)
def test_reconcile_catches_writes_without_counter_bump(monkeypatch):
    """Old-version writers don't bump counters; the reconcile poll must
    still pick their writes up."""
    monkeypatch.setattr(watch, 'WATCH_RECONCILE_SEC', 0.5)
    db = DBController()

    async def scenario():
        loop = asyncio.get_running_loop()
        notify = asyncio.Event()
        hub = WatchHub(Pool)
        sub = hub.subscribe(loop, notify)
        try:
            await _await_publication(sub, notify)

            # Simulate a pre-upgrade writer: raw set, no counter bump.
            legacy = _pool_dict()
            key = f'object/Pool/{legacy["uuid"]}'.encode()
            await asyncio.to_thread(
                db.kv_store.set, key, json.dumps(legacy).encode())

            state = await _await_publication(sub, notify, timeout=10.0)
            assert any(m.get_id() == legacy['uuid'] for m in state)
        finally:
            hub.unsubscribe(sub)

    asyncio.run(scenario())
