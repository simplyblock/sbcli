"""Declared secondary indices: key derivation, encoding, and the declarations.

Everything here is a pure function of a model instance — no database. The
transactional maintenance, the backfill and the read path are exercised against
a real FoundationDB in ``tests/integration/test_database_indices.py``.
"""
import importlib
import inspect
import pkgutil

import pytest

from simplyblock_core import models
from simplyblock_core.models import indices
from simplyblock_core.models.indices import Index, Unique
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


class Sample(BaseModel):
    pool: str = ""
    label: str = ""
    seq: int = 0
    children: list = None  # type: ignore[assignment]


# --- value encoding ---------------------------------------------------------

@pytest.mark.parametrize('value', [
    'plain', 'with/slash', 'with%percent', '%2F', '%25', 'a/b%c/d', '', '//', '%%',
])
def test_escaping_round_trips(value):
    assert indices.unescape(indices.escape(value)) == value


@pytest.mark.parametrize('value', ['with/slash', 'a/b%c/d', '%2F'])
def test_escaped_segment_carries_no_separator(value):
    """A value containing the separator must not be able to forge one."""
    assert '/' not in indices.escape(value)


def test_distinct_values_never_collide_after_escaping():
    """Escaping is injective, which is what keeps two values off one key."""
    values = ['a/b', 'a%2Fb', 'a%b', 'a%25b', 'a', 'b']
    assert len({indices.escape(v) for v in values}) == len(values)


def test_ordered_encoding_is_lexicographic_for_integers():
    encoded = [indices.encode_value(n, ordered=True) for n in (0, 1, 9, 10, 1000, 2**63 - 1)]
    assert encoded == sorted(encoded)
    assert len({len(e) for e in encoded}) == 1


@pytest.mark.parametrize('value', ['has/slash', 'has%percent'])
def test_ordered_encoding_refuses_values_it_cannot_order(value):
    with pytest.raises(ValueError, match='ordered index'):
        indices.encode_value(value, ordered=True)


def test_ordered_encoding_refuses_negative_integers():
    with pytest.raises(ValueError, match='negative'):
        indices.encode_value(-1, ordered=True)


# --- key derivation ---------------------------------------------------------

def test_field_extractor():
    index = Index('pool')
    obj = Sample({'uuid': 'u1', 'pool': 'p1'})
    assert index.keys(Sample, obj) == {b'index/Sample/pool/p1/u1'}


def test_tuple_extractor_names_itself():
    index = Index(('pool', 'label'))
    obj = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'vol'})
    assert index.name == 'pool+label'
    assert index.keys(Sample, obj) == {b'index/Sample/pool+label/p1/vol/u1'}


def test_callable_extractor_yields_many_keys_per_record():
    index = Index('child', extract=lambda obj: [(child,) for child in (obj.children or [])])
    obj = Sample({'uuid': 'u1', 'children': ['a', 'b']})
    assert index.keys(Sample, obj) == {
        b'index/Sample/child/a/u1',
        b'index/Sample/child/b/u1',
    }


def test_unique_key_omits_the_entity_id():
    """The key IS the constraint: two entities with one value collide on it."""
    index = Unique(('pool', 'label'))
    first = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'vol'})
    second = Sample({'uuid': 'u2', 'pool': 'p1', 'label': 'vol'})
    assert index.keys(Sample, first) == index.keys(Sample, second)
    assert index.keys(Sample, first) == {b'index/Sample/pool+label/p1/vol'}


def test_blank_values_are_not_indexed():
    index = Index('pool')
    assert index.keys(Sample, Sample({'uuid': 'u1'})) == set()


def test_no_keys_for_a_missing_record():
    assert Index('pool').keys(Sample, None) == set()


def test_prefix_is_exact_at_the_separator():
    """Without the trailing separator a lookup for 'abc' would also return
    the entries of 'abcd'."""
    index = Index('pool')
    prefix = index.prefix(Sample, ('abc',))
    assert prefix == b'index/Sample/pool/abc/'
    key, = index.keys(Sample, Sample({'uuid': 'u', 'pool': 'abcd'}))
    assert not key.startswith(prefix)


def test_partial_prefix_of_a_composite_index():
    index = Index(('pool', 'label'))
    assert index.prefix(Sample, ('p1',)) == b'index/Sample/pool+label/p1/'


# --- the write diff ---------------------------------------------------------

def test_changing_an_indexed_field_moves_exactly_one_key():
    index = Index('pool')
    before = Sample({'uuid': 'u1', 'pool': 'p1'})
    after = Sample({'uuid': 'u1', 'pool': 'p2'})

    old, new = index.keys(Sample, before), index.keys(Sample, after)

    assert old - new == {b'index/Sample/pool/p1/u1'}
    assert new - old == {b'index/Sample/pool/p2/u1'}


def test_changing_an_unindexed_field_moves_nothing():
    index = Index('pool')
    before = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'a'})
    after = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'b'})

    assert index.keys(Sample, before) == index.keys(Sample, after)


def test_multi_valued_diff_keeps_the_unchanged_entries():
    index = Index('child', extract=lambda obj: [(child,) for child in (obj.children or [])])
    before = Sample({'uuid': 'u1', 'children': ['a', 'b']})
    after = Sample({'uuid': 'u1', 'children': ['b', 'c']})

    old, new = index.keys(Sample, before), index.keys(Sample, after)

    assert old - new == {b'index/Sample/child/a/u1'}
    assert new - old == {b'index/Sample/child/c/u1'}
    assert old & new == {b'index/Sample/child/b/u1'}


# --- the scan fallback answers what the index answers ------------------------

def test_match_paths_selects_what_the_prefix_would():
    index = Index(('pool', 'label'))
    obj = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'vol'})

    assert index.match_paths(obj, ('p1',)) == ['p1/vol']
    assert index.match_paths(obj, ('p1', 'vol')) == ['p1/vol']
    assert index.match_paths(obj, ('p',)) == []
    assert index.match_paths(obj, ('p2',)) == []


def test_match_paths_agrees_with_the_key_the_index_would_store():
    index = Index(('pool', 'label'))
    obj = Sample({'uuid': 'u1', 'pool': 'p/1', 'label': 'vol'})

    [path] = index.match_paths(obj, ('p/1',))

    assert index.keys(Sample, obj) == {f'index/Sample/pool+label/{path}/u1'.encode()}


# --- the shipped declarations -----------------------------------------------

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
        b'index/StorageNode/device_id/d1/n1',
        b'index/StorageNode/device_id/d2/n1',
        b'index/StorageNode/device_id/jm1/n1',
    }


def test_node_failover_index_covers_both_peer_slots():
    node = StorageNode({'uuid': 'n1', 'secondary_node_id': 'n2', 'tertiary_node_id': 'n3'})

    index = indices.get_index(StorageNode, 'failover_for')

    assert index.keys(StorageNode, node) == {
        b'index/StorageNode/failover_for/n2/n1',
        b'index/StorageNode/failover_for/n3/n1',
    }


def test_composite_key_classes_index_the_bare_uuid():
    """The point of the `uuid` index on a composite-keyed class: the value is
    the composite id, so a caller holding only the uuid can point-read."""
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
