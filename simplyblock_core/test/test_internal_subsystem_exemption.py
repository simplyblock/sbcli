"""The per-node subsystem cap is a user-admission limit, not a bound on
system-created volumes.

Lab 2026-08-20: replication into the fresh cluster stopped dead with
``Too many subsystems on node: 956a513f..., max subsystems reached: 75``. The
REP_* volumes a transfer lands in are created by the replication service, and
refusing them is self-defeating — the node only empties once transfers complete
and retention prunes the older generations, so the cap blocked the very work
that would have released the slots.

Internal volumes are still COUNTED, so a user create continues to see true
occupancy; they are only exempt from being refused.
"""
import pytest
from typing import ClassVar

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController, SubsystemCapacityError


NODE_ID = "NODE1"
NODE_MAX = 3


class _Node:
    uuid = NODE_ID
    cluster_id = "CL1"
    max_lvol = NODE_MAX

    def get_id(self):
        return NODE_ID


class _Mini:
    """Stand-in for an existing lvol record occupying a subsystem."""

    def __init__(self, i):
        self.uuid = f"LV{i}"
        self.node_id = NODE_ID
        self.nqn = f"nqn.test:lvol:LV{i}"
        self.namespace = ""
        self.status = "online"

    def get_id(self):
        return self.uuid


class _NewLvol:
    uuid = "NEW"
    nqn = ""
    namespace = ""
    max_namespace_per_subsys = 1
    allowed_hosts: ClassVar[list[dict]] = []

    def get_id(self):
        return "NEW"

    def write_to_db(self, *a, **kw):
        return True


class _Cluster:
    nqn = "nqn.test"


def _full_node_lvols():
    return [_Mini(i) for i in range(NODE_MAX)]


# --- advisory pre-check (lvol_controller._resolve_lvol_subsystem) ---------


def test_user_create_is_refused_on_a_full_node():
    ok, error = lvol_controller._resolve_lvol_subsystem(
        _NewLvol(), _Node(), _Cluster(), False, _full_node_lvols())
    assert ok is False
    assert "Too many subsystems" in error


def test_internal_create_is_admitted_on_a_full_node():
    ok, error = lvol_controller._resolve_lvol_subsystem(
        _NewLvol(), _Node(), _Cluster(), False, _full_node_lvols(), internal=True)
    assert ok is True
    assert error == ""


def test_internal_flag_does_not_change_an_uncrowded_node():
    ok, error = lvol_controller._resolve_lvol_subsystem(
        _NewLvol(), _Node(), _Cluster(), False, [_Mini(0)], internal=True)
    assert (ok, error) == (True, "")


# --- authoritative check (DBController._claim_lvol_ns_slot_tx) -----------


class _Txn:
    """Minimal transaction: the claim path reads the allocator key and takes a
    snapshot read of the lvol table."""

    def __init__(self):
        self.snapshot = self

    def get(self, key):
        return self

    def wait(self):
        return self

    def present(self):
        return False

    # The claim writes the record (and bumps the allocator key) once it is past
    # the cap check; nothing here needs to persist for these assertions.
    def set(self, key, value):
        return None

    def clear(self, key):
        return None


def _claim(monkeypatch, internal):
    db = DBController.__new__(DBController)
    monkeypatch.setattr(lvol_controller, "count_lvol_subsystems",
                        lambda node, minis: NODE_MAX)
    monkeypatch.setattr("simplyblock_core.models.lvol_model.LVolMini.read_from_db",
                        lambda self, store: _full_node_lvols(), raising=False)
    lvol = _NewLvol()
    return db._claim_lvol_ns_slot_tx(
        _Txn(), lvol, _Node(), False, "nqn.test:lvol:NEW", "", 1, None, None,
        internal=internal)


def test_claim_refuses_a_user_create_at_the_cap(monkeypatch):
    with pytest.raises(SubsystemCapacityError):
        _claim(monkeypatch, internal=False)


def test_claim_admits_an_internal_create_at_the_cap(monkeypatch):
    """The regression: replication must not be blocked by the admission cap."""
    _claim(monkeypatch, internal=True)      # must not raise
