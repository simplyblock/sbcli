"""A migration back to a cluster the volume came from (A -> B, then B -> A).

_prepare_cutover recognises the return trip as a fail-back -- the source was
the TARGET of a completed relationship whose source cluster is the new
target -- and the success path swaps the new clone onto the original UUID,
which keeps the PV's volume handle valid. A DR fail-back also re-arms
replication on the restored volume, because it is the primary of a live DR
pairing again. A migration has no pairing to resume: re-arming copied the
B -> A migration policy onto the volume now on A, which then replicated
toward its own cluster.
"""
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.services import tasks_runner_replication_final as runner


class _Db:
    def __init__(self, lvols):
        self.lvols = {lv.uuid: lv for lv in lvols}
        self.kv_store = object()

    def get_lvol_by_id(self, lvol_id):
        return self.lvols[lvol_id]


def _lvol(uuid, **kw):
    lv = LVol()
    lv.uuid = uuid
    for k, v in kw.items():
        setattr(lv, k, v)
    return lv


def _install(monkeypatch, lvols):
    db = _Db(lvols)

    def write(self, kv_store=None):
        if isinstance(self, LVol):
            db.lvols[self.uuid] = self

    def remove(self, kv_store):
        db.lvols.pop(self.uuid, None)

    def write_replacing(self, old, kv_store):
        # One transaction on FDB; here the same effect on the fake store.
        remove(old, kv_store)
        write(self, kv_store)

    monkeypatch.setattr(LVol, "write_to_db", write)
    monkeypatch.setattr(LVol, "remove", remove)
    monkeypatch.setattr(LVol, "write_replacing", write_replacing)
    monkeypatch.setattr(LVolReplication, "write_to_db", lambda self, kv_store=None: None)
    monkeypatch.setattr(runner, "db", db)
    return db


def _swap(monkeypatch, mode):
    # B_lvol: the volume on B being moved back, following the B -> A policy.
    b_lvol = _lvol("b-lvol", do_replicate=True, replication_interval_min=1,
                   replication_policy_id="pol-b-to-a", replication_mode=mode)
    # The clone just cut over on A, built with replication off.
    clone = _lvol("a-clone", do_replicate=False, replication_interval_min=0,
                  replication_policy_id="")
    # The original A volume is gone: the first migration deleted its source.
    db = _install(monkeypatch, [b_lvol, clone])

    rep = LVolReplication()
    rep.uuid = "rep-2"
    rep.mode = mode
    rep.source_lvol = b_lvol
    rep.target_lvol = clone

    runner._swap_failback_lvol_uuid(rep, "a-original")
    return db


def test_migration_return_keeps_the_original_uuid_but_does_not_rearm(monkeypatch):
    db = _swap(monkeypatch, "migration")

    assert "a-clone" not in db.lvols
    restored = db.lvols["a-original"]
    assert restored.do_replicate is False
    assert restored.replication_policy_id == ""
    assert restored.replication_interval_min == 0


def test_dr_failback_still_rearms_replication(monkeypatch):
    db = _swap(monkeypatch, "failover")

    restored = db.lvols["a-original"]
    assert restored.do_replicate is True
    assert restored.replication_policy_id == "pol-b-to-a"
    assert restored.replication_interval_min == 1
