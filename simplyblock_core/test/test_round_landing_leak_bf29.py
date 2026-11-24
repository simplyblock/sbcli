"""A round name that is replaced takes its pre-staged landing volume with it.

Bed bf29 (2026-11-20): three 10 GiB REP_repl_round_* landing volumes stayed
on cluster A for good. The cutover names its next round snapshot in advance
(next_shrink_snap_name) and the replication runner pre-stages REP_<name>.
_take_shrink_snapshot replaces that name when its kind does not match the
round (direct vs ordinary), when a snapshot of that name exists but is not
usable, and when the name turns out to be taken -- and left REP_<old name>
behind each time.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import tasks_runner_replication_final as runner

OLD = "repl_commit_old"
OLD_DIRECT = runner.DIRECT_ROUND_PREFIX + "old"


class _DB:
    def __init__(self, landings, tasks=()):
        self.landings = landings            # name -> lvol
        self.tasks = list(tasks)

    def get_lvol_by_name(self, name):
        return self.landings[name]

    def get_job_tasks(self, cluster_id, **kw):
        return list(self.tasks)


def _landing(lid, status=LVol.STATUS_ONLINE):
    return SimpleNamespace(get_id=lambda: lid, status=status)


def _task(name):
    t = JobSchedule()
    t.uuid = "T1"
    t.cluster_id = "A"
    t.status = JobSchedule.STATUS_RUNNING
    t.function_params = {"lvol_id": "LV1", "shrink_round": 2, "next_shrink_snap_name": name}
    return t


def _lvol():
    lv = MagicMock()
    lv.get_id.return_value = "LV1"
    return lv


@pytest.fixture
def env(monkeypatch):
    deleted = []
    from simplyblock_core.controllers import lvol_controller
    monkeypatch.setattr(lvol_controller, "delete_lvol",
                        lambda lv, force_delete=False: deleted.append(lv.get_id()))
    from simplyblock_core.controllers import replication_delta
    add = MagicMock(return_value=("SNEW", None))
    monkeypatch.setattr(replication_delta, "add_snapshot", add)
    monkeypatch.setattr(runner, "_volume_snapshots", lambda lv: [])

    def use(fake):
        monkeypatch.setattr(runner, "db", fake)
    return SimpleNamespace(deleted=deleted, add=add, use=use, monkeypatch=monkeypatch)


def test_bf29_a_direct_round_drops_the_landing_staged_for_an_ordinary_name(env):
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD")}))
    task = _task(OLD)
    _, err, _ = runner._take_shrink_snapshot(task.frozen_view(), _lvol(), direct=True)
    assert err is None
    assert env.deleted == ["L-OLD"]
    assert env.add.call_args.args[1].startswith(runner.DIRECT_ROUND_PREFIX)


def test_bf29_an_ordinary_round_drops_the_landing_staged_for_a_direct_name(env):
    env.use(_DB({f"REP_{OLD_DIRECT}": _landing("L-OLD")}))
    runner._take_shrink_snapshot(_task(OLD_DIRECT).frozen_view(), _lvol(), direct=False)
    assert env.deleted == ["L-OLD"]


def test_bf29_a_taken_name_drops_its_landing(env):
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD")}))
    env.add.return_value = (None, "Snapshot name must be unique")
    task = _task(OLD)
    _, err, _ = runner._take_shrink_snapshot(task.frozen_view(), _lvol())
    assert "must be unique" in err
    assert env.deleted == ["L-OLD"]
    assert task.function_params["next_shrink_snap_name"] != OLD


def test_bf29_an_unusable_snapshot_of_that_name_drops_its_landing(env):
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD")}))
    stale = SimpleNamespace(snap_name=OLD, status=SnapShot.STATUS_IN_DELETION)
    env.monkeypatch.setattr(runner, "_volume_snapshots", lambda lv: [stale])
    runner._take_shrink_snapshot(_task(OLD).frozen_view(), _lvol())
    assert env.deleted == ["L-OLD"]


def test_bf29_a_landing_a_transfer_lands_in_is_kept(env):
    busy = JobSchedule()
    busy.uuid = "R1"
    busy.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    busy.status = JobSchedule.STATUS_RUNNING
    busy.function_params = {"remote_lvol_id": "L-OLD"}
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD")}, tasks=[busy]))
    runner._take_shrink_snapshot(_task(OLD).frozen_view(), _lvol(), direct=True)
    assert env.deleted == []


def test_bf29_a_landing_not_online_is_left_alone(env):
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD", LVol.STATUS_IN_DELETION)}))
    runner._take_shrink_snapshot(_task(OLD).frozen_view(), _lvol(), direct=True)
    assert env.deleted == []


def test_bf29_a_matching_name_keeps_its_landing_for_adoption(env):
    env.use(_DB({f"REP_{OLD}": _landing("L-OLD")}))
    runner._take_shrink_snapshot(_task(OLD).frozen_view(), _lvol(), direct=False)
    assert env.deleted == []
    assert env.add.call_args.args[1] == OLD
