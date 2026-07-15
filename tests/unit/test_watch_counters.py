# coding=utf-8
"""Version-index keys and the transactional write+index maintenance in BaseModel."""

import struct
from unittest.mock import MagicMock

import pytest

from simplyblock_core import watches
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.cluster import Cluster, ClusterAddNodeLock, PortReservation
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolMini
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.stats import LVolStatObject, StatsObject
from simplyblock_core.models.storage_node import StorageNode


WATCHED_CLASSES = [Cluster, StorageNode, Pool, LVol, SnapShot, JobSchedule]
UNWATCHED_CLASSES = [
    EventObj, LVolMini, SnapShotMini, StatsObject, LVolStatObject,
    ClusterAddNodeLock, PortReservation,
]


def test_index_key_format():
    assert watches.watch_index_rollup_key(Cluster, ()) == b'watch_index/Cluster'
    assert watches.watch_index_rollup_key(Pool, ('cl-1',)) == b'watch_index/Pool/cl-1'
    assert watches.watch_index_version_key(LVol, ('p1',), 'v1') == b'watch_index/LVol/p1/v1'
    assert watches.watch_index_version_prefix(LVol, ('p1',)) == b'watch_index/LVol/p1/'


def test_version_prefix_excludes_rollup_key():
    """Scanning the version prefix must not pick up the rollup key itself."""
    rollup = watches.watch_index_rollup_key(LVol, ('p1',))
    prefix = watches.watch_index_version_prefix(LVol, ('p1',))
    assert not rollup.startswith(prefix)
    assert prefix.startswith(rollup)


def test_scope_derivation_per_class():
    assert Cluster({'uuid': 'c'}).watch_scope() == ()
    assert Pool({'uuid': 'p', 'cluster_id': 'c'}).watch_scope() == ('c',)
    assert StorageNode({'uuid': 'n', 'cluster_id': 'c'}).watch_scope() == ('c',)
    assert LVol({'uuid': 'l', 'pool_uuid': 'p'}).watch_scope() == ('p',)
    assert SnapShot({'uuid': 's', 'pool_uuid': 'p'}).watch_scope() == ('p',)
    assert JobSchedule({'uuid': 't', 'cluster_id': 'c', 'date': 1}).watch_scope() == ('c',)


@pytest.mark.parametrize('model_cls', WATCHED_CLASSES)
def test_index_keys_disjoint_from_entity_scans(model_cls):
    """read_from_db scans '<object_type>/<ClassName>/' prefixes; index keys must
    never be picked up by any such scan."""
    keys = [
        watches.watch_index_rollup_key(model_cls, ('x',)),
        watches.watch_index_version_key(model_cls, ('x',), 'y'),
    ]
    for other_cls in WATCHED_CLASSES + UNWATCHED_CLASSES:
        instance = other_cls()
        prefix = f'{instance.object_type}/{instance.name}/'.encode()
        for key in keys:
            assert not key.startswith(prefix)


@pytest.mark.parametrize('model_cls', WATCHED_CLASSES)
def test_watched_flag_set(model_cls):
    assert model_cls._WATCHED is True


@pytest.mark.parametrize('model_cls', UNWATCHED_CLASSES)
def test_unwatched_flag_not_set(model_cls):
    assert model_cls._WATCHED is False


def test_watched_write_bumps_rollup_and_version():
    kv = MagicMock()
    pool = Pool({'uuid': 'pool-1', 'cluster_id': 'cl-1'})
    pool.write_to_db(kv)
    kv.set.assert_called_once()
    key, _value = kv.set.call_args[0]
    assert key == b'object/Pool/pool-1'
    assert kv.add.call_count == 2
    kv.add.assert_any_call(b'watch_index/Pool/cl-1', watches.ONE_LE64)
    kv.add.assert_any_call(b'watch_index/Pool/cl-1/pool-1', watches.ONE_LE64)


def test_unwatched_write_does_not_index():
    kv = MagicMock()
    event = EventObj({'uuid': 'ev-1'})
    event.write_to_db(kv)
    kv.set.assert_called_once()
    kv.add.assert_not_called()


def test_watched_remove_bumps_rollup_and_clears_version():
    kv = MagicMock()
    pool = Pool({'uuid': 'pool-1', 'cluster_id': 'cl-1'})
    pool.remove(kv)
    assert kv.clear.call_count == 2
    kv.clear.assert_any_call(b'object/Pool/pool-1')
    kv.clear.assert_any_call(b'watch_index/Pool/cl-1/pool-1')
    kv.add.assert_called_once_with(b'watch_index/Pool/cl-1', watches.ONE_LE64)


def test_unwatched_remove_does_not_index():
    kv = MagicMock()
    event = EventObj({'uuid': 'ev-1', 'cluster_uuid': 'c', 'date': 5})
    event.remove(kv)
    kv.clear.assert_called_once()
    kv.add.assert_not_called()


def test_compound_key_write_indexes_by_get_id():
    """JobSchedule's compound get_id() is the version leaf so a watcher can
    reconstruct the object key and point-read the task."""
    kv = MagicMock()
    task = JobSchedule({'uuid': 'task-1', 'cluster_id': 'cl-1', 'date': 42})
    task.write_to_db(kv)
    key, _value = kv.set.call_args[0]
    assert key == b'object/JobSchedule/cl-1/42/task-1'
    kv.add.assert_any_call(b'watch_index/JobSchedule/cl-1', watches.ONE_LE64)
    kv.add.assert_any_call(b'watch_index/JobSchedule/cl-1/cl-1/42/task-1', watches.ONE_LE64)


def test_base_model_default_unwatched():
    assert BaseModel._WATCHED is False


def test_base_model_default_scope_empty():
    assert BaseModel().watch_scope() == ()


def test_unpack_counter():
    assert watches.unpack_counter(None) == 0
    assert watches.unpack_counter(b'') == 0
    assert watches.unpack_counter(struct.pack('<q', 7)) == 7
    assert watches.unpack_counter(struct.pack('<q', 2 ** 40)) == 2 ** 40
