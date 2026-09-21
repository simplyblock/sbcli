"""``check-indices``: what it finds, what it repairs, and what it refuses."""
from simplyblock_core import index_ops
from simplyblock_core.models import indices
from simplyblock_core.models.lvol_model import LVol

from .helpers import POOL, index_keys, indexed_ids, make_lvol, ready, seed_raw


def test_verifier_is_quiet_on_a_healthy_index(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)

    findings = index_ops.check_indices([LVol])

    assert findings['missing'] == []
    assert findings['stale'] == []
    assert findings['orphaned'] == []


def test_verifier_detects_and_repairs_a_missing_entry(db):
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store.clear(victim)

    assert index_ops.check_indices([LVol])['missing']

    index_ops.check_indices([LVol], repair=True)

    assert index_ops.check_indices([LVol])['missing'] == []


def test_a_non_unique_entry_cannot_name_the_wrong_record(db):
    """The drift the design does not have: a non-unique entry stores nothing, so
    there is no second copy of the id that could disagree with the key's own
    tail. Junk in the value changes no answer and is no finding."""
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    victim = next(iter(indices.get_index(LVol, 'node_id').keys(LVol, lvol)))
    db.kv_store[victim] = b'some-other-entity'

    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-1")] == [lvol.get_id()]
    assert index_ops.check_indices([LVol])['stale'] == []


def test_verifier_names_the_duplicate_behind_a_unique_violation(db):
    """Two live records deriving one unique key is the condition a
    UniqueIndexViolation reports at write time; this is what says so afterwards."""
    ready(LVol)
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)
    # Seeded raw: the write path is exactly what refuses this.
    second = seed_raw(db, make_lvol("same-name"))

    findings = index_ops.check_indices([LVol])

    duplicates = [entry for entry in findings['duplicate']
                  if 'pool_uuid+lvol_name' in entry[0]]
    assert len(duplicates) == 1
    assert set(duplicates[0][1:]) == {first.get_id(), second.get_id()}


def _unique_key(lvol):
    return next(iter(
        indices.get_index(LVol, 'pool_uuid+lvol_name').keys(LVol, lvol)))


def test_verifier_repairs_a_unique_key_its_holder_no_longer_derives(db):
    """The repair the `unique` flag used to refuse wholesale. A unique key names
    one of several claimants only while the one it names still derives it; once
    that record has moved on, the candidate is the only claimant left and the
    key has exactly one right answer — as a non-unique key always does."""
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    moved_on = make_lvol("vol-b")
    moved_on.write_to_db(db.kv_store)
    ready(LVol)
    db.kv_store[_unique_key(lvol)] = moved_on.get_id().encode()

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['unresolved'] == 0
    assert db.lvol_name_lookup(POOL, "vol-a").get_id() == lvol.get_id()


def test_verifier_refuses_a_unique_key_two_live_records_derive(db):
    """The other half of the same question, and the one case a repair cannot
    settle: both claimants still carry the value, and overwriting the key would
    take whichever loses out of every lookup the index serves."""
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)
    ready(LVol)
    second = seed_raw(db, make_lvol("same-name"))

    findings = index_ops.check_indices([LVol], repair=True)

    # The seeded record's non-unique entries are written back...
    assert findings['repaired'] > 0
    assert second.get_id() in indexed_ids(db, LVol, 'node_id')
    # ...while the name they collide on is left exactly as it was.
    assert findings['duplicate']
    assert findings['unresolved'] > 0
    assert bytes(db.kv_store.get(_unique_key(first))).decode() == first.get_id()


def test_every_finding_ends_in_exactly_one_counter(db):
    """What makes `unresolved` answerable at all: a finding is repaired, or it
    evaporated, or it is still outstanding — never none of the three."""
    first = make_lvol("same-name")
    first.write_to_db(db.kv_store)
    ready(LVol)
    seed_raw(db, make_lvol("same-name"))
    db.kv_store[b'index/LVol/node_id/ghost-node/ghost-lvol'] = b''

    findings = index_ops.check_indices([LVol], repair=True)

    total = sum(len(findings[kind])
                for kind in ('missing', 'stale', 'orphaned', 'duplicate'))
    assert total == (findings['repaired'] + findings['vanished']
                     + findings['unresolved'])


def test_verifier_detects_and_repairs_an_orphan(db):
    make_lvol("vol-a").write_to_db(db.kv_store)
    ready(LVol)
    orphan = b'index/LVol/node_id/ghost-node/ghost-lvol'
    db.kv_store[orphan] = b'ghost-lvol'

    assert index_ops.check_indices([LVol])['orphaned'] == [orphan.decode()]

    index_ops.check_indices([LVol], repair=True)

    assert db.kv_store.get(orphan) is None


def _between_the_walks(monkeypatch, action):
    """Run ``action`` after ``check_indices`` walks the entities and before it
    walks the index keyspace — the window its findings can go stale in.

    Hooked on the index-keyspace walk itself rather than on the range reader
    underneath it, which both walks share."""
    real = index_ops._stored_entries

    def stored_entries(*args, **kwargs):
        action()
        return real(*args, **kwargs)

    monkeypatch.setattr(index_ops, '_stored_entries', stored_entries)


def test_repair_leaves_a_record_deleted_mid_check_alone(db, monkeypatch):
    """A record deleted mid-check is indistinguishable from a missing entry.
    Writing the entry back would leave a key for a record that is gone — an
    orphan, and on a unique index one that blocks the name from then on."""
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    _between_the_walks(monkeypatch, lambda: lvol.remove(db.kv_store))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['missing']
    assert findings['repaired'] == 0
    assert findings['unresolved'] == 0
    assert findings['vanished'] == len(findings['missing'])
    assert index_keys(db, LVol) == {}


def test_repair_leaves_a_record_created_mid_check_alone(db, monkeypatch):
    """A record created after the entity walk passed its key has live index
    entries the walk never saw. Clearing those as orphans would take a healthy
    volume out of every lookup."""
    ready(LVol)
    newcomer = make_lvol("vol-b")
    _between_the_walks(monkeypatch, lambda: newcomer.write_to_db(db.kv_store))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['orphaned']
    assert findings['repaired'] == 0
    assert findings['unresolved'] == 0
    assert findings['vanished'] == len(findings['orphaned'])
    assert [v.get_id() for v in db.query(LVol, 'node_id', "node-1")] == [newcomer.get_id()]


def _after_the_walks(monkeypatch, action):
    """Run ``action`` once ``check_indices`` has walked the index keyspace —
    the window between a finding and the repair that acts on it."""
    real = index_ops._stored_entries

    def stored_entries(*args, **kwargs):
        entries = real(*args, **kwargs)
        action()
        return entries

    monkeypatch.setattr(index_ops, '_stored_entries', stored_entries)


def test_repair_does_not_steal_a_unique_key_claimed_since_the_walk(db, monkeypatch):
    """A unique key free during the walk and held by a live claimant by the time
    the repair runs is a duplicate for the operator to resolve, not drift to
    overwrite."""
    # Two records derive the one key, and the entity walk keeps the last it
    # meets as the candidate to repair towards — so the ids are pinned, or which
    # of the two that is would be left to uuid order.
    claimant = seed_raw(db, make_lvol("vol-a", uuid_="aaaa-walked-first"))
    lvol = make_lvol("vol-a", uuid_="zzzz-walked-last")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    unique_key = _unique_key(lvol)
    db.kv_store.clear(unique_key)
    _after_the_walks(
        monkeypatch, lambda: db.kv_store.__setitem__(unique_key, claimant.get_id().encode()))

    findings = index_ops.check_indices([LVol], repair=True)

    assert (unique_key.decode(), lvol.get_id()) in findings['missing']
    assert findings['unresolved'] > 0
    assert bytes(db.kv_store.get(unique_key)).decode() == claimant.get_id()


def test_repair_takes_back_a_unique_key_held_by_a_record_that_is_gone(db, monkeypatch):
    """The converse, and why the refusal cannot be keyed on `unique` alone: a
    key naming a record that does not exist blocks the name for the one record
    that does derive it — forever, since nothing else ever clears it."""
    lvol = make_lvol("vol-a")
    lvol.write_to_db(db.kv_store)
    ready(LVol)
    unique_key = _unique_key(lvol)
    db.kv_store.clear(unique_key)
    _after_the_walks(
        monkeypatch, lambda: db.kv_store.__setitem__(unique_key, b'ghost-lvol'))

    findings = index_ops.check_indices([LVol], repair=True)

    assert findings['unresolved'] == 0
    assert db.lvol_name_lookup(POOL, "vol-a").get_id() == lvol.get_id()
