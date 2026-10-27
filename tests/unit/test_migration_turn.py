"""One migrating volume per source node at a time
(simplyblock_core/migration_turn.py)."""
from simplyblock_core import migration_turn
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol


def _lvol(uuid, node, queued=0, mode="migration", replicate=True, group=""):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node
    lv.do_replicate = replicate
    lv.replication_mode = mode
    lv.migration_queued_at = queued
    lv.group_id = group
    return lv


def _final(lvol_id, status=JobSchedule.STATUS_NEW, result="", canceled=False):
    t = JobSchedule()
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.function_params = {"lvol_id": lvol_id}
    t.status = status
    t.function_result = result
    t.canceled = canceled
    return t


class _DB:
    def __init__(self, lvols, tasks=()):
        self.lvols, self.tasks = list(lvols), list(tasks)

    def get_lvols(self, cluster_id=None):
        return self.lvols

    def get_job_tasks(self, cluster_id, reverse=True, limit=0):
        return self.tasks


def test_one_volume_per_node_in_queue_order():
    db = _DB([_lvol("n1-late", "N1", 20), _lvol("n1-first", "N1", 10), _lvol("n1-last", "N1", 30),
              _lvol("n2-only", "N2", 40)])
    assert migration_turn.waiting_lvol_ids(db, "C") == {"n1-late", "n1-last"}


def test_completed_cutover_hands_the_turn_to_the_next_volume():
    # A completed cutover clears the source's do_replicate.
    db = _DB([_lvol("first", "N1", 10, replicate=False), _lvol("second", "N1", 20),
              _lvol("third", "N1", 30)])
    assert migration_turn.waiting_lvol_ids(db, "C") == {"third"}


def test_failed_or_canceled_cutover_releases_the_turn():
    lvols = [_lvol("bad", "N1", 10), _lvol("next", "N1", 20)]
    failed = _DB(lvols, [_final("bad", JobSchedule.STATUS_DONE, "max retries reached")])
    assert migration_turn.waiting_lvol_ids(failed, "C") == set()
    canceled = _DB(lvols, [_final("bad", canceled=True)])
    assert migration_turn.waiting_lvol_ids(canceled, "C") == set()
    # A retried cutover running again takes the turn back.
    retried = _DB(lvols, [_final("bad", canceled=True), _final("bad")])
    assert migration_turn.waiting_lvol_ids(retried, "C") == {"next"}


def test_failover_replication_and_groups_are_not_serialized_apart():
    db = _DB([_lvol("dr1", "N1", mode="failover"), _lvol("dr2", "N1", mode="failover"),
              _lvol("g1", "N2", 10, group="G"), _lvol("g2", "N2", 11, group="G"),
              _lvol("loose", "N2", 12)])
    assert migration_turn.waiting_lvol_ids(db, "C") == {"loose"}


def test_a_waiting_volume_ships_no_backlog(monkeypatch):
    # Snapshots a volume had before migration started are queued at
    # replication_start; the transfer gate holds them while it waits its turn.
    from simplyblock_core.services import snapshot_replication as sr
    lv = _lvol("behind", "N1", 20)
    lv.lvs_name = "LVS_1"
    snap = type("Snap", (), {"lvol": lv})()
    task = type("Task", (), {"cluster_id": "C", "get_id": lambda self: "t1"})()
    monkeypatch.setattr(sr, "_group_id_for_lvol", lambda lvol: "")
    monkeypatch.setattr(sr, "db", _DB([_lvol("ahead", "N1", 10), lv]))
    assert "migration turn" in sr._lvs_transfer_hold(task, snap)
    monkeypatch.setattr(sr, "db", _DB([_lvol("ahead", "N1", 10, replicate=False), lv]))
    assert sr._lvs_transfer_hold(task, snap) == ""
