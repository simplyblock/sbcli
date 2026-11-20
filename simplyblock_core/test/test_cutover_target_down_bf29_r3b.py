"""An endgame whose target is down must not stop the volume's replication.

Bed bf29 round R3b (2026-11-20): the target primary node was shut down
between replication cycles of an A -> B migration. One volume was in its
cutover endgame; the endgame waited for the target primary (the cutover
clone is built through it), and while it held the volume nothing replicated
it -- the endgame takes no snapshot while it waits, and the cadence snapshot
skips a volume in its endgame. No cycle completed for ~20 min, RPO 1280 s.

After REPL_CUTOVER_TARGET_DOWN_GRACE_SEC the endgame is given up (no attempt
burned, no back-off): round state dropped, lvstore claim and cluster endgame
slot released, source write cap lifted. The volume replicates on its cadence
until the target is back, then enters a fresh endgame.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_monitor
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer

NOW = 2_000_000.0
GRACE = constants.REPL_CUTOVER_TARGET_DOWN_GRACE_SEC


def _node(node_id, status):
    return SimpleNamespace(get_id=lambda: node_id, status=status, cluster_id="A")


class _DB:
    def __init__(self, tasks, tgt_status=StorageNode.STATUS_OFFLINE):
        self.tasks = tasks
        self.nodes = {"SRC": _node("SRC", StorageNode.STATUS_ONLINE),
                      "TGT": _node("TGT", tgt_status)}
        self.lvol = MagicMock()
        self.lvol.get_id.return_value = "LV1"
        self.lvol.lvs_name = "LVS_1"
        self.lvol.node_id = "SRC"

    def get_lvol_by_id(self, lvol_id):
        return self.lvol

    def get_storage_node_by_id(self, node_id):
        return self.nodes[node_id]

    def get_job_tasks(self, cluster_id, **kw):
        return list(self.tasks)


def _endgame_task(**extra):
    t = JobSchedule()
    t.uuid = "T1"
    t.cluster_id = "A"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = JobSchedule.STATUS_RUNNING
    t.retry = 0
    t.max_retry = 8
    t.function_params = {"lvol_id": "LV1", "src_node_id": "SRC", "tgt_node_id": "TGT",
                         "start_time": int(NOW - 3600), **extra}
    return t


IN_ENDGAME = dict(cutover_lvs="LVS_1", cutover_cluster="A", cutover_claimed_at=NOW - 300,
                  shrink_snap_id="S5", shrink_round=5, shrink_round_times=[30.0] * 4,
                  shrink_started_at=NOW - 20, shrink_deadline=NOW + 600,
                  direct_rounds=False, throttle_restore=[0, 0, 0, 0])


@pytest.fixture
def env(monkeypatch):
    clock = {"t": NOW}
    monkeypatch.setattr(runner.time, "time", lambda: clock["t"])
    spies = SimpleNamespace(throttle=MagicMock(), requeue=MagicMock())
    monkeypatch.setattr(runner, "_restore_source_throttle", spies.throttle)
    monkeypatch.setattr(runner, "_requeue_direct_rounds", spies.requeue)
    monkeypatch.setattr(runner, "_discard_spare_landing", lambda t: t)

    def setup(task, **kw):
        fake = _DB([task], **kw)
        monkeypatch.setattr(runner, "db", fake)
        monkeypatch.setattr(snapshot_monitor, "db", fake)
        return fake
    return SimpleNamespace(clock=clock, spies=spies, setup=setup)


def _pass(task):
    with pytest.raises(TaskDefer) as e:
        runner.task_runner(task.frozen_view(), [task])
    return str(e.value)


def test_bf29_r3b_a_short_target_blip_keeps_the_endgame(env):
    task = _endgame_task(**IN_ENDGAME)
    env.setup(task)
    assert "target node not online" in _pass(task)
    assert task.function_params["target_down_since"] == NOW
    env.clock["t"] = NOW + GRACE - 1
    _pass(task)
    assert task.function_params["cutover_lvs"] == "LVS_1"
    assert task.function_params["shrink_snap_id"] == "S5"
    env.spies.throttle.assert_not_called()


def test_bf29_r3b_a_target_down_past_the_grace_returns_the_volume_to_its_cadence(env):
    task = _endgame_task(**IN_ENDGAME)
    env.setup(task)
    _pass(task)
    # the volume is held out of cadence while its endgame holds it ...
    assert snapshot_monitor._cutover_endgame_lvol_ids("A") == {"LV1"}
    env.clock["t"] = NOW + GRACE
    _pass(task)
    params = task.function_params
    for key in ("cutover_lvs", "cutover_cluster", "cutover_claimed_at", "shrink_snap_id",
                "shrink_round", "shrink_round_times"):
        assert key not in params, key
    # ... and is back on the ordinary cadence once the endgame is suspended
    assert snapshot_monitor._cutover_endgame_lvol_ids("A") == set()
    assert "endgame suspended" in params["last_error"]
    assert task.retry == 0, "a target outage burns no attempt"
    assert "endgame_retry_after" not in params, "no back-off: resume when the target is back"
    env.spies.throttle.assert_called_once()
    env.spies.requeue.assert_called_once()


def test_bf29_r3b_the_released_slot_is_free_for_another_volume(env, monkeypatch):
    task = _endgame_task(**IN_ENDGAME)
    env.setup(task)
    _pass(task)
    env.clock["t"] = NOW + GRACE
    _pass(task)
    other = _endgame_task()
    other.uuid = "T2"
    assert runner._cluster_endgame_slots_taken(other.frozen_view(), [task, other], "", "A") == ""


def test_bf29_r3b_a_volume_not_in_its_endgame_is_only_deferred(env):
    task = _endgame_task(shrink_deadline=NOW + 900)
    env.setup(task)
    _pass(task)
    env.clock["t"] = NOW + 10 * GRACE
    _pass(task)
    assert task.function_params["shrink_deadline"] == NOW + 900
    env.spies.throttle.assert_not_called()
    env.spies.requeue.assert_not_called()


def test_bf29_r3b_the_target_back_clears_the_outage_and_the_endgame_can_resume(env, monkeypatch):
    task = _endgame_task(**IN_ENDGAME)
    fake = env.setup(task)
    _pass(task)
    env.clock["t"] = NOW + GRACE
    _pass(task)
    fake.nodes["TGT"].status = StorageNode.STATUS_ONLINE

    class _Stop(Exception):
        pass

    def _lag(lvol):
        raise _Stop()
    monkeypatch.setattr(runner, "_replication_lag_sec", _lag)
    with pytest.raises(_Stop):
        # past the target check: the volume is on its way to the catch-up gate
        # that admits it to a fresh endgame
        runner.task_runner(task.frozen_view(), [task])
    assert "target_down_since" not in task.function_params
