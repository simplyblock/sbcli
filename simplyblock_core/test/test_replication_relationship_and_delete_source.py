"""Migration semantics around the relationship record.

Three contracts (2026-08-21):

  * ``replication-commit --delete-source`` retires the source volume once the
    cutover state is durable -- and only then, so a crash in between leaves a
    completed cutover with the source present, never the reverse.
  * the source->target mapping must resolve BY SOURCE UUID even after the
    source volume has been deleted: the relationship record embeds both
    volumes and is never removed with them.
  * the same look-up says which side is ACTIVE right now: the source until the
    cutover completes or a fail-over happens, the target from then on.
"""
from typing import Any

import pytest

from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVolReplication
from simplyblock_core.services import tasks_runner_replication_final as trf


class _LvolRef:
    def __init__(self, uuid):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


def _rep(state, src="SRC1", tgt="TGT1"):
    rep = LVolReplication()
    rep.uuid = "REP1"
    rep.source_lvol = _LvolRef(src)      # type: ignore[assignment]  # embedded copies: survive deletion
    rep.target_lvol = _LvolRef(tgt)      # type: ignore[assignment]
    rep.source_cluster_id = "CL_SRC"
    rep.target_cluster_id = "CL_TGT"
    rep.mode = "migration"
    rep.state = state
    rep.direction = LVolReplication.DIRECTION_TO_TARGET
    rep.target_nqn = "nqn.test"
    rep.target_ns_id = 1
    return rep


class _DB:
    """No lvol table at all: every volume-record look-up would raise, which is
    exactly the situation after --delete-source."""

    def __init__(self, reps):
        self._reps = reps

    def get_lvol_replication_objects(self):
        return self._reps

    def get_lvol_by_id(self, uuid):
        raise KeyError(uuid)


# --- look-up by source uuid, active side ----------------------------------


def test_lookup_by_source_uuid_survives_source_deletion(monkeypatch):
    monkeypatch.setattr(rpc, "db", _DB([_rep(LVolReplication.STATE_CUTOVER_DONE)]))
    rel = rpc.get_relationship("SRC1")
    assert rel is not None
    assert rel["target_lvol_id"] == "TGT1"
    assert rel["is_source"] is True


def test_active_is_source_while_replicating(monkeypatch):
    monkeypatch.setattr(rpc, "db", _DB([_rep(LVolReplication.STATE_REPLICATING)]))
    rel = rpc.get_relationship("SRC1")
    assert rel["active"] == "source"
    assert rel["active_lvol_id"] == "SRC1"


def test_active_is_source_while_cutover_pending(monkeypatch):
    monkeypatch.setattr(rpc, "db", _DB([_rep(LVolReplication.STATE_CUTOVER_PENDING)]))
    assert rpc.get_relationship("SRC1")["active"] == "source"


@pytest.mark.parametrize("state", [LVolReplication.STATE_CUTOVER_DONE,
                                   LVolReplication.STATE_FAILED_OVER])
def test_active_is_target_after_cutover_or_failover(monkeypatch, state):
    monkeypatch.setattr(rpc, "db", _DB([_rep(state)]))
    rel = rpc.get_relationship("SRC1")
    assert rel["active"] == "target"
    assert rel["active_lvol_id"] == "TGT1"


def test_lookup_by_target_uuid_still_works(monkeypatch):
    monkeypatch.setattr(rpc, "db", _DB([_rep(LVolReplication.STATE_CUTOVER_DONE)]))
    rel = rpc.get_relationship("TGT1")
    assert rel["source_lvol_id"] == "SRC1"
    assert rel["is_source"] is False


def _rep2(state, src, tgt):
    rep = _rep(state, src=src, tgt=tgt)
    rep.uuid = f"REP_{src}_{tgt}"
    return rep


def test_chained_migration_resolves_to_the_final_volume(monkeypatch):
    """T1 (target of A) later migrated on to T2: a caller holding only S1
    must learn that the data is served by T2, not stop one hop short."""
    a = _rep2(LVolReplication.STATE_CUTOVER_DONE, "S1", "T1")
    b = _rep2(LVolReplication.STATE_CUTOVER_DONE, "T1", "T2")
    monkeypatch.setattr(rpc, "db", _DB([a, b]))
    rel = rpc.get_relationship("S1")
    assert rel["target_lvol_id"] == "T1", "the direct relationship is A"
    assert rel["active_lvol_id"] == "T2", "the ACTIVE volume is chain-resolved"


def test_target_uuid_drives_the_next_forward_path(monkeypatch):
    """The target of one replication can be the source of the next."""
    a = _rep2(LVolReplication.STATE_CUTOVER_DONE, "S1", "T1")
    b = _rep2(LVolReplication.STATE_REPLICATING, "T1", "T2")
    monkeypatch.setattr(rpc, "db", _DB([a, b]))
    rel = rpc.get_relationship("T1")
    assert rel["is_source"] is True and rel["target_lvol_id"] == "T2"
    assert rel["active"] == "source", "B has not cut over: T1 still serves"
    assert rel["active_lvol_id"] == "T1"


def test_incomplete_next_hop_does_not_advance_the_chain(monkeypatch):
    a = _rep2(LVolReplication.STATE_CUTOVER_DONE, "S1", "T1")
    b = _rep2(LVolReplication.STATE_REPLICATING, "T1", "T2")
    monkeypatch.setattr(rpc, "db", _DB([a, b]))
    assert rpc.get_relationship("S1")["active_lvol_id"] == "T1"


def test_failback_cycle_terminates(monkeypatch):
    """S1 -> T1 (failed over), then T1 -> S1 (failed back): the walk must not
    loop, and the active volume is where the LAST completed hop landed."""
    a = _rep2(LVolReplication.STATE_FAILED_OVER, "S1", "T1")
    b = _rep2(LVolReplication.STATE_CUTOVER_DONE, "T1", "S1")
    monkeypatch.setattr(rpc, "db", _DB([a, b]))
    rel = rpc.get_relationship("S1")
    # newest record containing S1 is B (scan is newest-first)
    assert rel["active_lvol_id"] in ("S1",), "cycle must terminate, not loop"


def test_newest_relationship_wins(monkeypatch):
    """A volume that migrated twice resolves to its latest relationship."""
    older = _rep(LVolReplication.STATE_CUTOVER_DONE, tgt="TGT_OLD")
    newer = _rep(LVolReplication.STATE_REPLICATING, tgt="TGT_NEW")
    monkeypatch.setattr(rpc, "db", _DB([older, newer]))
    assert rpc.get_relationship("SRC1")["target_lvol_id"] == "TGT_NEW"


# --- --delete-source ordering ----------------------------------------------


class _Task:
    max_retry = 100

    def __init__(self, delete_source):
        self.function_params = {"lvol_id": "SRC1", "replication_id": "REP1",
                                "final_state": LVolReplication.STATE_CUTOVER_DONE,
                                "delete_source": delete_source}
        self.status = JobSchedule.STATUS_RUNNING
        self.function_result = ""
        self.retry = 0
        self.canceled = False
        self.writes = 0

    def write_to_db(self, kv=None):
        self.writes += 1


def _run_finalize(monkeypatch, delete_source, delete_raises=False):
    events: list[Any] = []
    rep = _rep(LVolReplication.STATE_CUTOVER_PENDING)

    class _SrcLvol(_LvolRef):
        def __init__(self, uuid):
            super().__init__(uuid)
            self.do_replicate = True
            self.replication_interval_min = 1
            self.replication_policy_id = "POL1"

        def write_to_db(self, kv=None):
            events.append(("src_config", self.do_replicate,
                           self.replication_interval_min,
                           self.replication_policy_id))

    src_state = {"lvol": _SrcLvol("SRC1")}

    class _DB2:
        kv_store = "KV"

        def get_lvol_replication_by_id(self, rid):
            return rep

        def get_lvol_by_id(self, uuid):
            events.append(("lookup", uuid))
            return src_state["lvol"]

    monkeypatch.setattr(trf, "db", _DB2())
    rep.write_to_db = lambda kv=None: events.append(("state", rep.state))

    from simplyblock_core.controllers import lvol_controller

    def _delete(lvol, **kw):
        if delete_raises:
            raise RuntimeError("delete blew up")
        events.append(("delete", lvol.get_id()))
    monkeypatch.setattr(lvol_controller, "delete_lvol", _delete)

    task = _Task(delete_source)
    ok = trf._finalize(task, True, "")
    return ok, task, events


def test_cutover_stops_replication_on_the_source(monkeypatch):
    """Observed 2026-08-21: after "cutover done" the source kept taking and
    replicating cadence snapshots. The hand-off must stop the source."""
    ok, _task, events = _run_finalize(monkeypatch, delete_source=False)
    assert ok is True
    assert ("src_config", False, 0, "") in events,         "the source's replication config must be cleared at cutover"


def test_source_deleted_only_after_cutover_state_is_durable(monkeypatch):
    ok, task, events = _run_finalize(monkeypatch, delete_source=True)
    assert ok is True
    assert ("delete", "SRC1") in events
    state_at = events.index(("state", LVolReplication.STATE_CUTOVER_DONE))
    delete_at = events.index(("delete", "SRC1"))
    assert state_at < delete_at, "the cutover state must be durable BEFORE the delete"
    assert task.status == JobSchedule.STATUS_DONE


def test_source_kept_without_the_flag(monkeypatch):
    ok, _task, events = _run_finalize(monkeypatch, delete_source=False)
    assert ok is True
    assert not any(e[0] == "delete" for e in events)


def test_failed_source_delete_does_not_unsucceed_the_cutover(monkeypatch):
    ok, task, _events = _run_finalize(monkeypatch, delete_source=True,
                                      delete_raises=True)
    assert ok is True
    assert task.status == JobSchedule.STATUS_DONE
