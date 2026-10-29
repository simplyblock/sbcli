"""The endgame's snapshots are held until the cutover completes; then one stays.

With copy-on-write snapshots (ultra #222, spdk #86) taking a snapshot is cheap
but deleting one is a merge that physically moves data. The cutover endgame
converges by taking snapshot after snapshot; pruning between those rounds put
merges on both clusters right when the convergence and the freeze need them
idle. Retention is therefore held from the first shrink snapshot until the
cutover task ends, and the completed cutover keeps only the newest pair.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_replication_final as runner

SRC = "src-lvol"


def _lvol(uuid=SRC):
    return SimpleNamespace(uuid=uuid, node_id="N1", get_id=lambda: uuid)


def _snap(uuid, created, lvol):
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created
    s.lvol = lvol
    s.snap_type = SnapShot.TYPE_INTERNAL
    s.status = SnapShot.STATUS_ONLINE
    s.target_replicated_snap_uuid = f"T{uuid}"
    return s


def _final_task(status=JobSchedule.STATUS_RUNNING, **params):
    return SimpleNamespace(function_name=JobSchedule.FN_REPLICATION_FINAL,
                           status=status, canceled=False,
                           function_params={"lvol_id": SRC, **params})


class _Db:
    def __init__(self, snaps, tasks):
        self.snaps = {s.uuid: s for s in snaps}
        self.tasks = tasks

    def get_storage_node_by_id(self, _id):
        return SimpleNamespace(cluster_id="CL")

    def get_job_tasks(self, _cluster_id):
        return self.tasks

    def get_snapshots_by_node_id(self, _node_id):
        return list(self.snaps.values())

    def get_snapshot_by_id(self, uuid):
        if uuid.startswith("T"):          # the target copies exist
            return SimpleNamespace(uuid=uuid)
        return self.snaps[uuid]


@pytest.fixture
def harness(monkeypatch):
    lvol = _lvol()
    snaps = [_snap(f"S{i}", 100 + i, lvol) for i in range(1, 6)]   # 5 replicated rounds
    db = _Db(snaps, [])
    deleted = []
    monkeypatch.setattr(sr, "db", db)
    monkeypatch.setattr(sr, "_keep_replicated_for", lambda lv: 2)
    monkeypatch.setattr(sr, "_retention_schedule_for", lambda lv: [])
    monkeypatch.setattr(sr, "_successor_is_chained_to", lambda a, b: True)
    monkeypatch.setattr(sr, "_has_dependent_clone", lambda sid: False)
    monkeypatch.setattr(sr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())

    def delete(uuid):
        deleted.append(uuid)
        return True

    monkeypatch.setattr(sr.snapshot_controller, "delete", delete)
    return SimpleNamespace(lvol=lvol, db=db, deleted=deleted)


def test_ordinary_retention_keeps_the_newest_two(harness):
    sr._prune_internal_snapshots(harness.lvol)
    assert set(harness.deleted) == {"S1", "TS1", "S2", "TS2", "S3", "TS3"}


@pytest.mark.parametrize("params", [{"shrink_snap_id": "S5"}, {"cutover_lvs": "LVS_1"}])
def test_nothing_is_pruned_while_the_endgame_runs(harness, params):
    harness.db.tasks = [_final_task(**params)]
    sr._prune_internal_snapshots(harness.lvol)
    assert harness.deleted == []


def test_a_queued_cutover_does_not_hold_retention(harness):
    """Before its first shrink snapshot a cutover holds nothing: the volume is
    still replicating on its cadence, and that needs ordinary retention."""
    harness.db.tasks = [_final_task(require_baseline=True)]
    sr._prune_internal_snapshots(harness.lvol)
    assert harness.deleted != []


@pytest.mark.parametrize("ended", ["done", "canceled"])
def test_an_ended_cutover_releases_the_hold(harness, ended):
    task = _final_task(shrink_snap_id="S5")
    if ended == "done":
        task.status = JobSchedule.STATUS_DONE
    else:
        task.canceled = True
    harness.db.tasks = [task]
    sr._prune_internal_snapshots(harness.lvol)
    assert harness.deleted != []


def test_another_volumes_endgame_does_not_hold_this_one(harness):
    other = _final_task(shrink_snap_id="X")
    other.function_params["lvol_id"] = "other-lvol"
    harness.db.tasks = [other]
    sr._prune_internal_snapshots(harness.lvol)
    assert harness.deleted != []


def test_after_the_cutover_all_but_the_newest_pair_go(harness):
    # The cutover task is still open while its success path runs.
    harness.db.tasks = [_final_task(shrink_snap_id="S5", cutover_lvs="LVS_1")]
    sr.prune_after_cutover(harness.lvol)
    assert set(harness.deleted) == {f"{p}S{i}" for i in range(1, 5) for p in ("", "T")}
    assert "S5" not in harness.deleted and "TS5" not in harness.deleted


@pytest.mark.parametrize("delete_source, pruned", [(False, True), (True, False)])
def test_the_cutover_prunes_a_kept_source_only(monkeypatch, delete_source, pruned):
    """With --delete-source the source's lineage is retired and reaped instead."""
    calls = []
    lvol = _lvol()
    monkeypatch.setattr(runner, "db", SimpleNamespace(get_lvol_by_id=lambda i: lvol))
    monkeypatch.setattr(sr, "prune_after_cutover", lambda lv: calls.append(lv.get_id()))
    task = SimpleNamespace(function_params={"lvol_id": SRC, "delete_source": delete_source})
    runner._prune_endgame_snapshots(task)
    assert calls == ([SRC] if pruned else [])
