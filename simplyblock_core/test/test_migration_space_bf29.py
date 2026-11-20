"""A migration must never fill its SOURCE cluster by itself (bed bf29).

Bed bf29 (2026-11-20): two 5-node clusters, 8 client volumes under sustained
fio overwrite, a cluster-scope migration A -> B. Five volumes ran their
cutover endgame at once; each shrink round snapshotted its volume every
15-30 s, every snapshot pinned what fio overwrote after it, and the
superseded snapshots were deleted more slowly than new ones were taken.
Cluster A went 41% -> 82% -> 99% in 16 minutes and ran out of space.

Three rules (simplyblock_core/migration_space.py):

1. at most REPL_CUTOVER_MAX_ENDGAMES_PER_CLUSTER endgames per source cluster;
   the others keep replicating on their ordinary cadence;
2. no next round snapshot while the volume's earlier deletions are pending;
3. no migration snapshot (round or cadence) while the source's free space is
   under the headroom estimate; an unreadable capacity holds only briefly.

None of them touches client IO: only the migration's own snapshots wait.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants, migration_space
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_monitor
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer

# The real functions: conftest neutralises both for every other test.
REAL_SPACE_HOLD = migration_space.space_hold
REAL_DELETIONS_PENDING = migration_space.deletions_pending

GiB = 1 << 30
NOW = 1_000_000.0


@pytest.fixture
def real_rules(monkeypatch):
    monkeypatch.setattr(migration_space, "space_hold", REAL_SPACE_HOLD)
    monkeypatch.setattr(migration_space, "deletions_pending", REAL_DELETIONS_PENDING)


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #

def _task(uuid, lvol_id, cluster="A", **params):
    t = JobSchedule()
    t.uuid = uuid
    t.cluster_id = cluster
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = JobSchedule.STATUS_RUNNING
    t.max_retry = 8
    t.date = 0
    t.function_params = {"lvol_id": lvol_id, **params}
    return t


def _lvol(uuid, lvs, node="N-A", size=10 * GiB, mode="migration"):
    lv = MagicMock()
    lv.get_id.return_value = uuid
    lv.uuid = uuid
    lv.lvs_name = lvs
    lv.node_id = node
    lv.size = size
    lv.top_bdev = f"LVS/{uuid}"
    lv.replication_policy_id = ""
    lv.replication_mode = mode
    lv.pool_uuid = "P"
    return lv


def _snap(snap_id, status=SnapShot.STATUS_ONLINE):
    return SimpleNamespace(get_id=lambda: snap_id, status=status)


class _DB:
    """The DB reads the rules make: nodes, tasks, capacity, snapshots."""

    def __init__(self, tasks=(), free=None, total=1000 * GiB, record_age=5.0,
                 snaps=None, dirty=None, nodes=None):
        self.tasks = list(tasks)
        self.total = total
        self.free = free
        self.record_age = record_age
        self.snaps = snaps or {}
        self.dirty = dirty
        self.rpc_calls = 0
        self.now = NOW                          # when the latest record was written + age
        self.nodes = nodes or {"N-A": self._node("N-A", "A"), "N-B": self._node("N-B", "B")}

    def _node(self, node_id, cluster):
        def info(bdev):
            self.rpc_calls += 1
            if self.dirty is None:
                raise RuntimeError("no count")
            return {"tracking": True, "complete": True, "dirty_bytes": self.dirty}
        rpc = SimpleNamespace(bdev_lvol_dirty_bitmap_info=info)
        return SimpleNamespace(get_id=lambda: node_id, cluster_id=cluster,
                               status=StorageNode.STATUS_ONLINE,
                               secondary_node_id="", tertiary_node_id="",
                               rpc_client=lambda **kw: rpc)

    def get_storage_node_by_id(self, node_id):
        return self.nodes[node_id]

    def get_job_tasks(self, cluster_id, **kw):
        return [t for t in self.tasks if t.cluster_id == cluster_id]

    def get_cluster_by_id(self, cluster_id):
        return SimpleNamespace(get_id=lambda: cluster_id)

    def get_cluster_capacity(self, cluster, limit=1):
        if self.free is None:
            return []
        return [SimpleNamespace(size_total=self.total, size_used=self.total - self.free,
                                date=int(self.now - self.record_age))]

    def get_snapshots_by_lvol_id(self, lvol_id):
        return list(self.snaps.get(lvol_id, ()))


@pytest.fixture
def clock(monkeypatch):
    now = {"t": NOW}
    monkeypatch.setattr(runner.time, "time", lambda: now["t"])
    return now


def _use(monkeypatch, fake):
    monkeypatch.setattr(runner, "db", fake)
    monkeypatch.setattr(snapshot_monitor, "db", fake)


# --------------------------------------------------------------------------- #
# Rule 1: one endgame per source cluster
# --------------------------------------------------------------------------- #

def test_bf29_a_second_lvstore_of_the_same_cluster_queues_for_the_endgame_slot(monkeypatch, clock):
    """bf29: five endgames on five lvstores of cluster A ran at once. The
    per-lvstore claim let them; the cluster slot does not."""
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_claimed_at=NOW - 60)
    mine = _task("T2", "LV2")
    _use(monkeypatch, _DB([holder, mine]))
    with pytest.raises(TaskDefer, match="endgame slot of source cluster A"):
        runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [holder, mine])
    assert "cutover_lvs" not in mine.function_params, "a queued volume holds no lvstore"


def test_bf29_the_queued_volume_keeps_its_ordinary_cadence(monkeypatch):
    """Waiting for the slot is not an endgame: the cadence snapshots of the
    queued volume go on (no RPO gap added by the wait)."""
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_snap_id="S1")
    queued = _task("T2", "LV2", shrink_deadline=NOW + 900)
    _use(monkeypatch, _DB([holder, queued]))
    assert snapshot_monitor._cutover_endgame_lvol_ids("A") == {"LV1"}


def test_bf29_an_endgame_of_another_cluster_does_not_take_the_slot(monkeypatch, clock):
    other = _task("T1", "LVB", cluster="B", cutover_lvs="LVS_1", cutover_cluster="B")
    mine = _task("T2", "LV2")
    _use(monkeypatch, _DB([other, mine]))
    out = runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [other, mine])
    assert out.function_params["cutover_lvs"] == "LVS_2"
    assert out.function_params["cutover_cluster"] == "A"


def test_bf29_a_consistency_group_sibling_joins_its_groups_slot(monkeypatch, clock):
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_group="G1",
                   cutover_claimed_at=NOW - 60)
    mine = _task("T2", "LV2")
    _use(monkeypatch, _DB([holder, mine]))
    monkeypatch.setattr(runner, "_group_id_for_lvol", lambda lv: "G1")
    out = runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_1"), [holder, mine])
    assert out.function_params["cutover_lvs"] == "LVS_1"


def test_bf29_the_limit_is_configurable(monkeypatch, clock):
    monkeypatch.setattr(constants, "REPL_CUTOVER_MAX_ENDGAMES_PER_CLUSTER", 2)
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_claimed_at=NOW - 60)
    second = _task("T2", "LV2")
    third = _task("T3", "LV3")
    fake = _DB([holder, second, third])
    _use(monkeypatch, fake)
    runner._acquire_lvs_claim(second.frozen_view(), _lvol("LV2", "LVS_2"), [holder, second, third])
    assert second.function_params["cutover_lvs"] == "LVS_2"
    with pytest.raises(TaskDefer, match="2 of 2 in use"):
        runner._acquire_lvs_claim(third.frozen_view(), _lvol("LV3", "LVS_3"),
                                  [holder, second, third])


def test_bf29_claims_racing_in_one_pass_leave_exactly_one_endgame(monkeypatch, clock):
    """All five caught up in the same runner pass: the pass's task list shows
    no claim to any of them. The claim is re-checked against a fresh read
    after it is written, and only the first claimant keeps it."""
    tasks = [_task(f"T{i}", f"LV{i}") for i in range(1, 6)]
    stale = [JobSchedule().from_dict(t.to_dict()) for t in tasks]   # the pass's view
    _use(monkeypatch, _DB(tasks))
    winners, queued = [], []
    for i, t in enumerate(tasks, start=1):
        clock["t"] = NOW + i
        try:
            runner._acquire_lvs_claim(t.frozen_view(), _lvol(f"LV{i}", f"LVS_{i}"), stale)
            winners.append(t.uuid)
        except TaskDefer as e:
            assert "lost to a concurrent claim" in str(e)
            queued.append(t.uuid)
    assert winners == ["T1"]
    assert queued == ["T2", "T3", "T4", "T5"]
    for t in tasks[1:]:
        assert not t.function_params.get("cutover_lvs"), "a loser withdraws its claim"


def test_bf29_a_later_claim_never_displaces_a_running_endgame(monkeypatch, clock):
    """Ranking is by claim time, not task age: an older task that claims
    late must not take the slot from the endgame already running."""
    running = _task("T9", "LV9", cutover_lvs="LVS_9", cutover_claimed_at=NOW - 300)
    older = _task("T0", "LV0")          # sorts first by id/creation
    _use(monkeypatch, _DB([running, older]))
    with pytest.raises(TaskDefer):
        runner._confirm_cluster_endgame_slot(
            runner._record(older.frozen_view(), cutover_lvs="LVS_0",
                           cutover_cluster="A", cutover_claimed_at=NOW), "", "A")
    assert not older.function_params.get("cutover_lvs")


def test_bf29_releasing_the_claim_frees_the_slot(monkeypatch, clock):
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_cluster="A",
                   cutover_claimed_at=NOW - 60)
    _use(monkeypatch, _DB([holder]))
    monkeypatch.setattr(runner, "_requeue_direct_rounds", lambda t: None)
    runner._release_lvs_claim(holder.frozen_view())
    for key in ("cutover_lvs", "cutover_cluster", "cutover_claimed_at"):
        assert key not in holder.function_params


# --------------------------------------------------------------------------- #
# Rule 2: the next round waits for the volume's pending deletions
# --------------------------------------------------------------------------- #

def _hold_db(**kw):
    kw.setdefault("free", 900 * GiB)
    return _DB(**kw)


def test_bf29_no_round_snapshot_while_an_earlier_one_is_still_being_deleted(
        monkeypatch, clock, real_rules):
    fake = _hold_db(snaps={"LV1": [_snap("S1", SnapShot.STATUS_IN_DELETION),
                                   _snap("S2")]})
    _use(monkeypatch, fake)
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=3, direct_rounds=False)
    taken = MagicMock()
    monkeypatch.setattr(runner, "_take_shrink_snapshot", taken)
    err, _ = runner._take_round(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert "round 4 snapshot waits for 1 deletion" in err
    taken.assert_not_called()
    assert task.function_params["deletion_wait_since"] == NOW


def test_bf29_the_round_is_taken_once_the_deletion_completed(monkeypatch, clock, real_rules):
    fake = _hold_db(snaps={"LV1": [_snap("S2")]})
    _use(monkeypatch, fake)
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=3, direct_rounds=False,
                 deletion_wait_since=NOW - 20)
    taken = MagicMock(return_value=("S3", None, task))
    monkeypatch.setattr(runner, "_take_shrink_snapshot", lambda t, lv, direct=False: taken(t))
    err, _ = runner._take_round(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert err is None
    taken.assert_called_once()
    assert "deletion_wait_since" not in task.function_params


def test_bf29_a_stuck_deletion_is_set_aside_after_the_bounded_wait(monkeypatch, clock, real_rules):
    """A deletion that never completes (no leader, a clone in deletion) must
    not stall the endgame on its own; a newer pending deletion is still
    waited for."""
    fake = _hold_db(snaps={"LV1": [_snap("STUCK", SnapShot.STATUS_IN_DELETION)]})
    _use(monkeypatch, fake)
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=3, direct_rounds=False,
                 deletion_wait_since=NOW - constants.REPL_ENDGAME_DELETION_WAIT_MAX_SEC)
    why, _ = runner._round_snapshot_hold(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert why == ""
    assert task.function_params["deletion_ignored"] == ["STUCK"]
    fake.snaps["LV1"].append(_snap("S4", SnapShot.STATUS_IN_DELETION))
    why, _ = runner._round_snapshot_hold(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert "S4" in why and "STUCK" not in why


def test_bf29_a_soft_deleted_snapshot_is_not_a_pending_deletion(monkeypatch, real_rules):
    soft = SimpleNamespace(get_id=lambda: "S1", status=SnapShot.STATUS_ONLINE, deleted=True)
    assert migration_space.deletions_pending(_DB(snaps={"LV1": [soft]}), _lvol("LV1", "L")) == []


def test_bf29_a_held_round_is_not_measured_twice(monkeypatch, clock, real_rules):
    """The round finished; the next snapshot waits for a deletion. The next
    pass must not book the waiting time as another, longer round."""
    fake = _hold_db(snaps={"LV1": [_snap("S0", SnapShot.STATUS_IN_DELETION)]})
    _use(monkeypatch, fake)
    monkeypatch.setattr(runner, "_shrink_round_done", lambda snap_id: True)
    monkeypatch.setattr(runner, "_take_shrink_snapshot", MagicMock())
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=2, shrink_snap_id="S1",
                 shrink_started_at=NOW - 10, shrink_deadline=NOW + 900,
                 shrink_round_times=[20.0], direct_rounds=False)
    lvol = _lvol("LV1", "LVS_1")
    with patch.object(runner, "_throttle_source", lambda t, lv: t):
        with pytest.raises(TaskDefer, match="waits for 1 deletion"):
            runner._shrink_step(task.frozen_view(), lvol)
        assert task.function_params["shrink_round_times"] == [20.0, 10.0]
        clock["t"] += 60
        with pytest.raises(TaskDefer, match="waits for 1 deletion"):
            runner._shrink_step(task.frozen_view(), lvol)
    assert task.function_params["shrink_round_times"] == [20.0, 10.0]
    runner._take_shrink_snapshot.assert_not_called()


def test_bf29_the_endgame_is_not_entered_while_its_first_round_would_wait(
        monkeypatch, clock, real_rules):
    """Holding the lvstore and the cluster slot through the wait would stop
    the other volumes' transfers for nothing."""
    fake = _hold_db(snaps={"LV1": [_snap("S0", SnapShot.STATUS_IN_DELETION)]})
    _use(monkeypatch, fake)
    held, _ = runner._round_snapshot_hold(_task("T1", "LV1").frozen_view(), _lvol("LV1", "L"))
    assert held
    import inspect
    src = inspect.getsource(runner.task_runner)
    assert src.index("_round_snapshot_hold") < src.index("_acquire_lvs_claim")


# --------------------------------------------------------------------------- #
# Rule 3: free space on the source cluster
# --------------------------------------------------------------------------- #

def test_bf29_no_round_snapshot_with_the_source_nearly_full(monkeypatch, clock, real_rules):
    """82% -> 99%: under the 5% floor no migration snapshot is taken, and an
    event says why -- once per wait, not once per pass."""
    fake = _hold_db(free=10 * GiB)              # 99% used of 1000 GiB
    _use(monkeypatch, fake)
    events = []
    monkeypatch.setattr(migration_space, "space_low_event",
                        lambda lv, why: events.append(why))
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=5, direct_rounds=False)
    monkeypatch.setattr(runner, "_take_shrink_snapshot", MagicMock())
    err, _ = runner._take_round(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert "deferred" in err and "floor" in err
    err, _ = runner._take_round(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert err
    assert len(events) == 1
    runner._take_shrink_snapshot.assert_not_called()
    assert fake.rpc_calls == 0, "under the floor there is nothing to ask SPDK"


def test_bf29_space_recovered_lets_the_round_go(monkeypatch, clock, real_rules):
    fake = _hold_db(free=900 * GiB)
    _use(monkeypatch, fake)
    task = _task("T1", "LV1", cutover_lvs="LVS_1", shrink_round=5, direct_rounds=False,
                 space_low_since=NOW - 120)
    monkeypatch.setattr(runner, "_take_shrink_snapshot",
                        lambda t, lv, direct=False: ("S6", None, t))
    err, _ = runner._take_round(task.frozen_view(), _lvol("LV1", "LVS_1"))
    assert err is None
    assert "space_low_since" not in task.function_params


def test_bf29_headroom_is_twice_the_bytes_written_since_the_last_snapshot(monkeypatch):
    lvol = _lvol("LV1", "L", size=400 * GiB)
    # 100 GiB free (> 5% floor of 50 GiB); 60 GiB dirty needs 120 GiB
    fake = _DB(free=100 * GiB, dirty=60 * GiB)
    verdict, why = migration_space.assess(fake, lvol, "A", now=NOW)
    assert verdict == "low" and "120 GiB headroom" in why
    fake.dirty = 30 * GiB                       # needs 60 GiB: fits
    assert migration_space.assess(fake, lvol, "A", now=NOW)[0] == "ok"


def test_bf29_a_small_volume_on_a_roomy_cluster_asks_spdk_nothing(monkeypatch):
    fake = _DB(free=500 * GiB, dirty=1 * GiB)
    assert migration_space.assess(fake, _lvol("LV1", "L", size=10 * GiB), "A", now=NOW)[0] == "ok"
    assert fake.rpc_calls == 0


def test_bf29_an_unknown_dirty_count_assumes_the_whole_volume(monkeypatch):
    fake = _DB(free=100 * GiB, dirty=None)      # RPC fails
    verdict, _ = migration_space.assess(fake, _lvol("LV1", "L", size=60 * GiB), "A", now=NOW)
    assert verdict == "low"                     # 2 x 60 GiB > 100 GiB


def test_bf29_unreadable_capacity_holds_only_for_a_bounded_time(monkeypatch, real_rules):
    """Fail-safe: no capacity record (collector down) holds the snapshot for
    REPL_MIGRATION_CAPACITY_UNKNOWN_WAIT_SEC, then lets it through."""
    fake = _DB(free=None)
    lvol = _lvol("LV1", "L")
    why, since = migration_space.space_hold(fake, lvol, "A", 0, now=NOW)
    assert why and since == NOW
    why, since = migration_space.space_hold(fake, lvol, "A", since, now=NOW + 60)
    assert why and since == NOW
    why, since = migration_space.space_hold(
        fake, lvol, "A", since, now=NOW + constants.REPL_MIGRATION_CAPACITY_UNKNOWN_WAIT_SEC)
    assert why == "" and since == NOW


def test_bf29_a_stale_capacity_record_is_no_reading(monkeypatch):
    fake = _DB(free=900 * GiB, record_age=constants.REPL_MIGRATION_CAPACITY_STALE_SEC + 1)
    verdict, why = migration_space.assess(fake, _lvol("LV1", "L"), "A", now=NOW)
    assert verdict == "unknown" and "old" in why


def test_bf29_known_low_space_is_never_waited_out(monkeypatch, real_rules):
    """Only an UNREADABLE capacity is bounded; a known-full source keeps the
    migration snapshot waiting (the endgame's shrink deadline bounds that)."""
    fake = _DB(free=1 * GiB)
    why, since = migration_space.space_hold(fake, _lvol("LV1", "L"), "A", NOW - 10 ** 6,
                                            now=NOW)
    assert why and since == 0


def test_bf29_cadence_of_a_migrating_volume_waits_for_space(monkeypatch, real_rules):
    fake = _DB(free=10 * GiB)
    _use(monkeypatch, fake)
    events = []
    monkeypatch.setattr(migration_space, "space_low_event",
                        lambda lv, why: events.append(why))
    snapshot_monitor._space_low.clear()
    snapshot_monitor._space_unknown_since.clear()
    lvol = _lvol("LV1", "L", mode="migration")
    assert snapshot_monitor._migration_space_hold(lvol, "A", NOW)
    assert snapshot_monitor._migration_space_hold(lvol, "A", NOW + 60)
    assert len(events) == 1
    fake.free = 900 * GiB
    fake.now = NOW + 120
    assert snapshot_monitor._migration_space_hold(lvol, "A", NOW + 120) == ""
    assert "LV1" not in snapshot_monitor._space_low


def test_bf29_dr_replication_cadence_is_never_held_for_space(monkeypatch, real_rules):
    """The rules bound a migration's own consumption only: a volume that
    replicates for DR is not a migration and keeps its cadence."""
    fake = _DB(free=1 * GiB)
    _use(monkeypatch, fake)
    lvol = _lvol("LV1", "L", mode="replicate")
    assert snapshot_monitor._migration_space_hold(lvol, "A", NOW) == ""


def test_bf29_client_volume_creation_does_not_consult_the_migration_rules():
    """Capacity thresholds stay alarms: nothing on the client path (volume
    create, IO) imports the migration space rules."""
    import inspect

    from simplyblock_core.controllers import lvol_controller
    assert "migration_space" not in inspect.getsource(lvol_controller)
