"""A named consistency group must be readable back from the store.

Regression for the 2026-09-10 VolumeGroupSnapshot failure: ConsistencyGroup
declared a ``name`` data field, shadowing BaseModel.name — which doubles as
the class-name segment of every FDB key (get_db_id builds
``object/{self.name}/{id}``). ensure_group("vgs-group") therefore wrote the
record under ``object/vgs-group/...`` while every reader scans
``object/ConsistencyGroup/...``: each labeled volume minted a fresh, invisible
group (so members never shared a placement pin), and the CSI GroupController's
membership read failed with a 404 for a group that had just been created.

These tests drive the controller and DB APIs against a fake store and pin the
behavior that was broken: a group created by name is found again — by name,
by id, and in its cluster's listing.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.db_controller import DBController


class _FakeKV:
    """Minimal writable store: set/clear plus prefix scans, mirroring the
    surface read_from_db and write_to_db use when get_range is absent."""

    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def clear(self, key):
        self._data.pop(key, None)

    def get_range_startswith(self, prefix, limit=0, reverse=False):
        rows = sorted((k, v) for k, v in self._data.items()
                      if k.startswith(prefix))
        if reverse:
            rows.reverse()
        if limit:
            rows = rows[:limit]
        return rows


@pytest.fixture
def db(monkeypatch):
    """A DBController backed by a fresh fake store, wired into the controller.

    In the unit tier the fdb stub makes ``DBController()`` return uncached
    instances with ``kv_store=None``, so the controller module's ``db`` must be
    pointed at this instance explicitly.
    """
    instance = DBController()
    instance.kv_store = _FakeKV()
    monkeypatch.setattr(cgc, 'db', instance)
    return instance


CLUSTER = "aaaaaaaa-0000-0000-0000-000000000001"


def test_ensure_group_is_idempotent_by_name(db):
    """The second labeled volume must join the FIRST volume's group, not mint
    its own (the placement pin only exists if both resolve to one group)."""
    first = cgc.ensure_group(CLUSTER, "vgs-group")
    second = cgc.ensure_group(CLUSTER, "vgs-group")
    assert second.uuid == first.uuid


def test_named_group_is_found_by_id(db):
    """The CSI GroupController resolves a member's group_id and reads its
    membership; a just-created group must not 404."""
    group = cgc.ensure_group(CLUSTER, "vgs-group")
    assert db.get_consistency_group_by_id(group.get_id()).uuid == group.uuid


def test_named_group_is_listed_in_its_cluster(db):
    group = cgc.ensure_group(CLUSTER, "vgs-group")
    listed = db.get_consistency_groups(CLUSTER)
    assert [g.uuid for g in listed] == [group.uuid]
    by_name = db.get_consistency_group_by_name(CLUSTER, "vgs-group")
    assert by_name is not None and by_name.uuid == group.uuid


def test_group_name_is_persisted(db):
    """The group's own name survives the round trip — it is the identity a
    labeled volume joins by — without displacing the record's keyspace."""
    group = cgc.ensure_group(CLUSTER, "vgs-group")
    read_back = db.get_consistency_group_by_id(group.get_id())
    assert read_back.group_name == "vgs-group"
