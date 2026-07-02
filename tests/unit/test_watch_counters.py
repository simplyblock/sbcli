# coding=utf-8
"""Change-counter keys and the transactional write+bump in BaseModel."""

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


def test_counter_key_format():
    assert watches.watch_counter_key(StorageNode) == b'watch_seq/StorageNode'
    assert watches.watch_counter_key(LVol) == b'watch_seq/LVol'


@pytest.mark.parametrize('model_cls', WATCHED_CLASSES)
def test_counter_keys_disjoint_from_entity_scans(model_cls):
    """read_from_db scans '<object_type>/<ClassName>/' prefixes; counter keys
    must never be picked up by any such scan."""
    counter_key = watches.watch_counter_key(model_cls)
    for other_cls in WATCHED_CLASSES + UNWATCHED_CLASSES:
        instance = other_cls()
        prefix = f'{instance.object_type}/{instance.name}/'.encode()
        assert not counter_key.startswith(prefix)


@pytest.mark.parametrize('model_cls', WATCHED_CLASSES)
def test_watched_flag_set(model_cls):
    assert model_cls._WATCHED is True


@pytest.mark.parametrize('model_cls', UNWATCHED_CLASSES)
def test_unwatched_flag_not_set(model_cls):
    assert model_cls._WATCHED is False


def test_watched_write_bumps_counter():
    kv = MagicMock()
    pool = Pool({'uuid': 'pool-1'})
    pool.write_to_db(kv)
    kv.set.assert_called_once()
    key, _value = kv.set.call_args[0]
    assert key == b'object/Pool/pool-1'
    kv.add.assert_called_once_with(b'watch_seq/Pool', watches.ONE_LE64)


def test_unwatched_write_does_not_bump():
    kv = MagicMock()
    event = EventObj({'uuid': 'ev-1'})
    event.write_to_db(kv)
    kv.set.assert_called_once()
    kv.add.assert_not_called()


def test_watched_remove_bumps_counter():
    kv = MagicMock()
    pool = Pool({'uuid': 'pool-1'})
    pool.remove(kv)
    kv.clear.assert_called_once_with(b'object/Pool/pool-1')
    kv.add.assert_called_once_with(b'watch_seq/Pool', watches.ONE_LE64)


def test_unwatched_remove_does_not_bump():
    kv = MagicMock()
    event = EventObj({'uuid': 'ev-1', 'cluster_uuid': 'c', 'date': 5})
    event.remove(kv)
    kv.clear.assert_called_once()
    kv.add.assert_not_called()


def test_compound_key_write_bumps_class_counter():
    """JobSchedule's compound get_id() must not leak into the counter key."""
    kv = MagicMock()
    task = JobSchedule({'uuid': 'task-1', 'cluster_id': 'cl-1', 'date': 42})
    task.write_to_db(kv)
    key, _value = kv.set.call_args[0]
    assert key == b'object/JobSchedule/cl-1/42/task-1'
    kv.add.assert_called_once_with(b'watch_seq/JobSchedule', watches.ONE_LE64)


def test_base_model_default_unwatched():
    assert BaseModel._WATCHED is False


def test_unpack_counter():
    assert watches.unpack_counter(None) == 0
    assert watches.unpack_counter(b'') == 0
    assert watches.unpack_counter(struct.pack('<q', 7)) == 7
    assert watches.unpack_counter(struct.pack('<q', 2 ** 40)) == 2 ** 40
