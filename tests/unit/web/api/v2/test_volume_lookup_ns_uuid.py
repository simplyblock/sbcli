"""A migrated volume is still found under the id its PV names.

Bed bf28 (2026-11-19): migration gives the volume a new record id while its
namespace keeps the original one (LVol.ns_uuid). The PV - immutable - names the
original id, so every lookup the CSI made for a migrated volume answered 404 in
both clusters and the reconnect loop could not restore paths (vol-1: 1 of 6).
"""
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException

import simplyblock_web.api.v2._dependencies as deps
from simplyblock_core.models.lvol_model import LVol

ORIG = "731ff4bb-cf97-4bf2-9963-06cee01448c4"
NEW = "46a93411-0000-4000-8000-000000000001"
POOL_B = "446f40a9-52e9-4dcd-a9a3-45350672ac1b"
POOL_A = "baf07936-00b0-4f42-9934-b87e819910ce"


def _lv(uuid, pool, ns_uuid="", status=LVol.STATUS_ONLINE, create_dt="2026-11-19 13:40", deleted=False):
    return SimpleNamespace(uuid=uuid, get_id=lambda: uuid, pool_uuid=pool, ns_uuid=ns_uuid,
                           status=status, create_dt=create_dt, deleted=deleted)


class _DB:
    def __init__(self, *lvols):
        self.lvols = {lv.uuid: lv for lv in lvols}

    def get_lvol_by_id(self, uuid):
        if uuid not in self.lvols:
            raise KeyError(f"LVol {uuid} not found")
        return self.lvols[uuid]

    def get_lvols_by_pool_id(self, pool_id):
        return [lv for lv in self.lvols.values() if lv.pool_uuid == pool_id]


def _pool(pid):
    return SimpleNamespace(get_id=lambda: pid)


@pytest.fixture
def db(monkeypatch):
    def install(*lvols):
        d = _DB(*lvols)
        monkeypatch.setattr(deps, "_db", d)
        return d
    return install


def test_the_original_id_finds_the_migrated_volume(db):
    db(_lv(NEW, POOL_B, ns_uuid=ORIG))
    assert deps._lookup_volume(UUID(ORIG), _pool(POOL_B)).get_id() == NEW


def test_a_volume_found_by_its_own_id_is_unchanged(db):
    db(_lv(NEW, POOL_B, ns_uuid=ORIG))
    assert deps._lookup_volume(UUID(NEW), _pool(POOL_B)).get_id() == NEW


def test_another_pool_still_answers_404(db):
    db(_lv(NEW, POOL_B, ns_uuid=ORIG))
    with pytest.raises(HTTPException) as e:
        deps._lookup_volume(UUID(ORIG), _pool(POOL_A))
    assert e.value.status_code == 404


def test_the_kept_source_record_in_another_pool_does_not_hide_the_move(db):
    # The original record survives in the source pool; asked in the target
    # pool, the migrated copy answers instead of a 404.
    db(_lv(ORIG, POOL_A), _lv(NEW, POOL_B, ns_uuid=ORIG))
    assert deps._lookup_volume(UUID(ORIG), _pool(POOL_B)).get_id() == NEW
    assert deps._lookup_volume(UUID(ORIG), _pool(POOL_A)).get_id() == ORIG


def test_deleted_copies_are_skipped_and_online_wins(db):
    db(_lv("11111111-0000-4000-8000-000000000001", POOL_B, ns_uuid=ORIG, deleted=True),
       _lv("22222222-0000-4000-8000-000000000002", POOL_B, ns_uuid=ORIG, status=LVol.STATUS_IN_DELETION),
       _lv("33333333-0000-4000-8000-000000000003", POOL_B, ns_uuid=ORIG, status="offline",
           create_dt="2026-11-19 15:00"),
       _lv(NEW, POOL_B, ns_uuid=ORIG))
    assert deps._lookup_volume(UUID(ORIG), _pool(POOL_B)).get_id() == NEW


def test_unknown_id_is_404(db):
    db(_lv(NEW, POOL_B, ns_uuid=ORIG))
    with pytest.raises(HTTPException) as e:
        deps._lookup_volume(UUID("99999999-0000-4000-8000-000000000009"), _pool(POOL_B))
    assert e.value.status_code == 404
