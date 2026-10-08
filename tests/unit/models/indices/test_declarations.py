"""The declarations this release ships, checked against their models."""
import importlib
import inspect
import pkgutil

from simplyblock_core import models
from simplyblock_core.models import indices
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


def _model_classes():
    for module_info in pkgutil.iter_modules(models.__path__):
        module = importlib.import_module(f'simplyblock_core.models.{module_info.name}')
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if issubclass(cls, BaseModel) and cls.__module__ == module.__name__:
                yield cls


def _declared():
    for cls in _model_classes():
        for index in indices.indexes_of(cls):
            yield cls, index


def test_every_declared_index_names_fields_the_model_has():
    """The guard that makes a renamed field a test failure rather than an
    index that silently stops matching anything."""
    offenders = [
        f'{cls.__name__}.{index.name}: no field {field!r}'
        for cls, index in _declared()
        for field in (index.fields or ())
        if field not in cls().keys()
    ]
    assert offenders == []


def test_every_callable_extractor_works_on_a_default_instance():
    """A default instance is the shape the backfill meets on a partially
    populated record, so an extractor must handle it rather than raise."""
    for cls, index in _declared():
        if index.fields is not None:
            continue
        for values in index.tuples(cls()):
            assert isinstance(values, tuple), f'{cls.__name__}.{index.name}'


def test_every_declared_arity_matches_the_tuples_its_extractor_returns():
    """A declared arity that disagrees with the extractor would have the read
    path take a value segment for the entity id."""
    for cls, index in _declared():
        for values in index.tuples(cls()):
            assert len(values) == index.arity, f'{cls.__name__}.{index.name}'


def test_index_names_are_unique_per_class():
    for cls in _model_classes():
        names = [index.name for index in indices.indexes_of(cls)]
        assert len(names) == len(set(names)), cls.__name__


def test_index_keys_are_disjoint_from_entity_scans():
    """``read_from_db`` scans ``<object_type>/<ClassName>/``; no index or state
    key may be picked up by one."""
    scan_prefixes = [
        f'{cls().object_type}/{cls.__name__}/'.encode() for cls in _model_classes()
    ]
    for cls, index in _declared():
        keys = [index.base(cls).encode(), indices.index_meta_key(cls, index)]
        for key in keys:
            assert not any(key.startswith(prefix) for prefix in scan_prefixes), key


def test_index_and_index_meta_namespaces_are_disjoint():
    assert not indices.INDEX_META_PREFIX.startswith(indices.INDEX_PREFIX)
    assert not indices.INDEX_PREFIX.startswith(indices.INDEX_META_PREFIX)


def test_no_index_is_maintained_against_a_store_that_cannot_be_read():
    """Index maintenance has to read the record it replaces, so it only runs
    against a real FoundationDB handle. A mock store gets the plain write it
    always got, instead of a read that returns whatever the mock invents."""
    from unittest.mock import MagicMock

    assert LVol.active_indexes(MagicMock()) == ()
    assert LVol.active_indexes(None) == ()


def test_the_mini_models_are_not_indexed():
    """They are scheduled for deletion; indexing them would entrench them and
    add write cost to a model on its way out."""
    from simplyblock_core.models.lvol_model import LVolMini
    from simplyblock_core.models.snapshot import SnapShotMini

    assert indices.indexes_of(LVolMini) == ()
    assert indices.indexes_of(SnapShotMini) == ()


def test_node_device_index_covers_nvme_and_journal_devices():
    node = StorageNode({
        'uuid': 'n1', 'cluster_id': 'c1',
        'nvme_devices': [{'uuid': 'd1'}, {'uuid': 'd2'}],
        'jm_device': {'uuid': 'jm1'},
    })

    index = indices.get_index(StorageNode, 'device_id')

    assert index.keys(StorageNode, node) == {
        b'index/StorageNode/device_id/d1/nvme/n1',
        b'index/StorageNode/device_id/d2/nvme/n1',
        b'index/StorageNode/device_id/jm1/jm/n1',
    }


def test_node_device_index_discriminates_by_kind():
    """A JM built on a whole device carries that device's uuid, so the id alone
    does not say which device is meant."""
    node = StorageNode({
        'uuid': 'n1', 'cluster_id': 'c1',
        'nvme_devices': [{'uuid': 'd1'}],
        'jm_device': {'uuid': 'jm1'},
    })

    index = indices.get_index(StorageNode, 'device_id')

    assert index.match_paths(node, ('jm1',)) == ['jm1/jm']
    assert index.match_paths(node, ('jm1', StorageNode.DEVICE_KIND_JM)) == ['jm1/jm']
    assert index.match_paths(node, ('jm1', StorageNode.DEVICE_KIND_NVME)) == []
    assert index.match_paths(node, ('d1', StorageNode.DEVICE_KIND_JM)) == []


def test_node_failover_index_covers_both_peer_slots():
    node = StorageNode({'uuid': 'n1', 'secondary_node_id': 'n2', 'tertiary_node_id': 'n3'})

    index = indices.get_index(StorageNode, 'failover_for')

    assert index.keys(StorageNode, node) == {
        b'index/StorageNode/failover_for/n2/n1',
        b'index/StorageNode/failover_for/n3/n1',
    }


def test_composite_key_classes_index_the_bare_uuid():
    """The point of the `uuid` index on a composite-keyed class: the key's tail
    is the composite id, so a caller holding only the uuid can point-read."""
    task = JobSchedule({'uuid': 't1', 'cluster_id': 'c1', 'date': 42})

    assert indices.get_index(JobSchedule, 'uuid').keys(JobSchedule, task) == {
        b'index/JobSchedule/uuid/t1/c1/42/t1',
    }


def test_snapshot_chain_index_is_ordered_by_creation():
    def snap(uuid, created_at):
        return SnapShot({'uuid': uuid, 'created_at': created_at,
                         'lvol': {'uuid': 'lv1'}, 'vuid': 1})

    index = indices.get_index(SnapShot, 'lvol_snaps')
    keys = [next(iter(index.keys(SnapShot, snap(f's{n}', n)))) for n in (2, 10, 100)]

    assert keys == sorted(keys)


def test_lvol_replication_policy_is_indexed_by_the_bare_uuid():
    lvol = LVol({'uuid': 'v1', 'replication_policy_id': 'cluster-1/policy-9'})

    assert indices.get_index(LVol, 'replication_policy_id').keys(LVol, lvol) == {
        b'index/LVol/replication_policy_id/policy-9/v1',
    }
