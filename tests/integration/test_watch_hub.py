# coding=utf-8
"""ScopeWatch against real FDB: scoped delivery, scoped wakeup, teardown."""

import asyncio
import time
from uuid import uuid4

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.watch import _UNSET, ScopeWatch


def _pool_dict(cluster_id):
    return {
        'uuid': str(uuid4()),
        'cluster_id': cluster_id,
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
def test_scopewatch_delivers_scoped_writes_and_stops_on_unsubscribe():
    db = DBController()
    cluster_id = str(uuid4())

    async def scenario():
        loop = asyncio.get_running_loop()
        notify = asyncio.Event()
        sw = ScopeWatch(Pool, (cluster_id,), None)
        sub = sw.subscribe(loop, notify)
        try:
            # Initial publication for this (empty) scope.
            assert await _await_publication(sub, notify) == []

            pool = Pool(_pool_dict(cluster_id))
            started = time.monotonic()
            await asyncio.to_thread(pool.write_to_db, db.kv_store)
            state = await _await_publication(sub, notify, timeout=5.0)
            assert time.monotonic() - started < 5.0
            assert any(m.get_id() == pool.get_id() for m in state)

            await asyncio.to_thread(pool.remove, db.kv_store)
            state = await _await_publication(sub, notify, timeout=5.0)
            assert not any(m.get_id() == pool.get_id() for m in state)
        finally:
            sw.unsubscribe(sub)

        thread = sw._thread
        deadline = time.monotonic() + 10
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        assert not thread.is_alive()

    asyncio.run(scenario())


@pytest.mark.timeout(60)
def test_scopewatch_ignores_other_scope():
    """A write in a different cluster's scope must not wake this watch."""
    db = DBController()
    cluster_a = str(uuid4())
    cluster_b = str(uuid4())

    async def scenario():
        loop = asyncio.get_running_loop()
        notify = asyncio.Event()
        sw = ScopeWatch(Pool, (cluster_a,), None)
        sub = sw.subscribe(loop, notify)
        try:
            assert await _await_publication(sub, notify) == []

            other = Pool(_pool_dict(cluster_b))
            await asyncio.to_thread(other.write_to_db, db.kv_store)

            # cluster_a's rollup key never bumped -> no publication (< reconcile).
            with pytest.raises(asyncio.TimeoutError):
                await _await_publication(sub, notify, timeout=3.0)
        finally:
            sw.unsubscribe(sub)

    asyncio.run(scenario())
