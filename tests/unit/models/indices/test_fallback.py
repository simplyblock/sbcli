"""The scan fallback answers what the index answers.

``match_paths`` selects over a record what ``prefix`` selects over the stored
keys; the two disagreeing is what would make the flip to ``ready`` visible.
"""
import pytest

from simplyblock_core.models.indices import Index
from simplyblock_core.models.job_schedule import JobSchedule

from .sample import Sample


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


@pytest.mark.parametrize('values', [('',), (None,)])
def test_a_blank_lookup_value_is_refused_on_both_read_paths(values):
    """A blank names no stored key, and an empty value path is also the empty
    prefix — so encoding one reads back as "nothing" through the index and as
    "everything" through the scan."""
    index = Index('pool')
    obj = Sample({'uuid': 'u1', 'pool': 'p1'})

    with pytest.raises(ValueError, match='blank component'):
        index.prefix(Sample, values)
    with pytest.raises(ValueError, match='blank component'):
        index.match_paths(obj, values)


def test_no_values_still_means_the_whole_index():
    index = Index('pool')
    obj = Sample({'uuid': 'u1', 'pool': 'p1'})

    assert index.prefix(Sample, ()) == b'index/Sample/pool/'
    assert index.match_paths(obj, ()) == ['p1']


def test_a_lookup_wider_than_the_index_is_refused():
    """An id is appended to an entry key raw, so an over-long prefix can land
    inside the id of a class whose `get_id()` embeds separators and select
    entries `match_paths` would never return."""
    index = Index('uuid')
    job = JobSchedule({'uuid': 'u1', 'cluster_id': 'c1', 'date': '2026'})
    assert index.keys(JobSchedule, job) == {b'index/JobSchedule/uuid/u1/c1/2026/u1'}

    with pytest.raises(ValueError, match='1 value'):
        index.prefix(JobSchedule, ('u1', 'c1'))
    with pytest.raises(ValueError, match='1 value'):
        index.match_paths(job, ('u1', 'c1'))
