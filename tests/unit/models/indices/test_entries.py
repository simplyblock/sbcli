"""Reading an entry back: the id a stored key and value resolve to."""
import pytest

from simplyblock_core.models import indices
from simplyblock_core.models.indices import Index, Unique
from simplyblock_core.models.job_schedule import JobSchedule

from .sample import Sample


def test_a_non_unique_entry_stores_nothing():
    """The id is the key's own tail. A second copy in the value could only ever
    disagree with it — which is the drift this design does not have."""
    assert Index('pool').entry_value('u1') == b''


def test_the_id_comes_back_off_the_key():
    index = Index('pool')
    key, = index.keys(Sample, Sample({'uuid': 'u1', 'pool': 'p1'}))

    assert index.entry_id(Sample, key, b'') == 'u1'


def test_the_id_comes_back_whole_for_a_composite_primary_key():
    """`JobSchedule.get_id()` is `<cluster>/<date>/<uuid>`, appended raw while
    every value segment is escaped — so the id is the only part of a key that
    may contain the separator, and skipping `arity` of them lands on all of it."""
    task = JobSchedule({'uuid': 't1', 'cluster_id': 'c1', 'date': 42})
    index = indices.get_index(JobSchedule, 'uuid')
    key, = index.keys(JobSchedule, task)

    assert index.entry_id(JobSchedule, key, b'') == 'c1/42/t1'
    assert task.get_id() == 'c1/42/t1'


def test_the_id_comes_back_through_a_value_carrying_the_separator():
    index = Index('pool')
    key, = index.keys(Sample, Sample({'uuid': 'u/1', 'pool': 'p/1'}))

    assert index.entry_id(Sample, key, b'') == 'u/1'


@pytest.mark.parametrize('arity', [1, 2, 3])
def test_every_declared_index_round_trips_its_own_keys(arity):
    """The property the read path rests on, over every shape of declaration:
    what `keys()` writes, `entry_id()` reads back."""
    index = Index('child', arity=arity, extract=lambda obj: [
        tuple(f'v{n}' for n in range(arity))])
    obj = Sample({'uuid': 'c1/c2'})
    key, = index.keys(Sample, obj)

    assert index.entry_id(Sample, key, b'') == 'c1/c2'


def test_a_unique_entry_records_its_holder_in_the_value():
    """Its key omits the id by design, so the value is the only record of it."""
    index = Unique(('pool', 'label'))
    obj = Sample({'uuid': 'u1', 'pool': 'p1', 'label': 'vol'})
    key, = index.keys(Sample, obj)

    assert index.entry_value('u1') == b'u1'
    assert index.entry_id(Sample, key, b'u1') == 'u1'


def test_entry_id_refuses_a_key_of_another_index():
    index = Index('pool')
    with pytest.raises(ValueError, match='not an entry'):
        index.entry_id(Sample, b'index/Sample/label/x/u1', b'')


def test_entry_id_refuses_a_key_with_no_room_for_an_id():
    index = Index(('pool', 'label'))
    with pytest.raises(ValueError, match='no entity id'):
        index.entry_id(Sample, b'index/Sample/pool+label/p1/vol', b'')
