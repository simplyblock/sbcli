"""Deriving a record's keys: extractors, prefixes, arity, and the write diff."""
import pytest

from simplyblock_core.models.indices import Index, Unique

from .sample import Sample


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
    index = Index('child', arity=1, extract=lambda obj: [(child,) for child in (obj.children or [])])
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


def test_a_key_needs_the_full_value_tuple():
    """An entry whose key carries the wrong number of value segments cannot be
    read back: `entry_id` would return a value where the id belongs."""
    index = Index(('pool', 'label'))
    with pytest.raises(ValueError, match='2 value'):
        index.key(Sample, ('p1',), 'u1')


# --- arity ------------------------------------------------------------------

def test_arity_is_derived_from_the_fields():
    assert Index('pool').arity == 1
    assert Index(('pool', 'label')).arity == 2


def test_a_callable_extractor_must_declare_its_arity():
    with pytest.raises(ValueError, match='arity'):
        Index('child', extract=lambda obj: [(obj.pool,)])


def test_a_field_extractor_may_not_restate_its_arity():
    with pytest.raises(ValueError, match='derived from the fields'):
        Index('pool', arity=1)


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
    index = Index('child', arity=1, extract=lambda obj: [(child,) for child in (obj.children or [])])
    before = Sample({'uuid': 'u1', 'children': ['a', 'b']})
    after = Sample({'uuid': 'u1', 'children': ['b', 'c']})

    old, new = index.keys(Sample, before), index.keys(Sample, after)

    assert old - new == {b'index/Sample/child/a/u1'}
    assert new - old == {b'index/Sample/child/c/u1'}
    assert old & new == {b'index/Sample/child/b/u1'}
