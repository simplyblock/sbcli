"""A group generation taken with a request id is taken once.

A caller that takes a generation and loses the answer (a crash between the
call and recording its sequence) retries with the same request id. The take
must answer with the generation it already took rather than freeze the group
and take another, which would leave one unused generation behind per lost
answer. The id is stamped on every member snapshot of the generation, so the
lookup reads the snapshots the take wrote and needs no other record. A take
answers with its generation's sequence, the one identifier of what it took.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from tests.unit.test_consistency_group_store import _FakeKV

CLUSTER = "aaaaaaaa-0000-0000-0000-000000000001"


@pytest.fixture
def db(monkeypatch):
    instance = DBController()
    instance.kv_store = _FakeKV()
    monkeypatch.setattr(cgc, 'db', instance)
    return instance


def _member_snapshot(db, group, uuid, lvol_uuid, seq, request_id=""):
    lvol = LVol()
    lvol.uuid = lvol_uuid
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER
    snap.lvol = lvol
    snap.status = SnapShot.STATUS_ONLINE
    snap.group_id = group.get_id()
    snap.group_seq = seq
    snap.group_request_id = request_id
    snap.write_to_db(db.kv_store)
    return snap


@pytest.fixture
def group(db):
    group = cgc.ensure_group(CLUSTER, "orders")
    _member_snapshot(db, group, "s-3a", "lv-a", 3, request_id="req-1")
    _member_snapshot(db, group, "s-3b", "lv-b", 3, request_id="req-1")
    _member_snapshot(db, group, "s-4a", "lv-a", 4, request_id="req-2")
    _member_snapshot(db, group, "s-5a", "lv-a", 5)
    return group


def test_the_generation_a_request_id_took_is_found(group):
    assert cgc.generation_for_request(group, "req-1") == 3
    assert cgc.generation_for_request(group, "req-2") == 4


def test_an_unknown_or_empty_request_id_finds_nothing(group):
    assert cgc.generation_for_request(group, "req-9") is None
    # Generation 5 carries no id; an empty id must never match it.
    assert cgc.generation_for_request(group, "") is None


def test_a_retried_take_answers_with_the_generation_it_took(group, monkeypatch):
    """The retry returns before anything is frozen: no precheck, no RPC."""
    def no_take(_group):
        raise AssertionError("a retried take must not take another generation")
    monkeypatch.setattr(cgc, '_precheck_members', no_take)

    seq, err = cgc.create_group_snapshot_for_group(group, request_id="req-1", lock=False)

    assert err is None
    assert seq == 3
