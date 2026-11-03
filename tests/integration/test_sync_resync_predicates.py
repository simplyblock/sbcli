"""The shared predicates of the sync-replication zone catch-up against the real
FoundationDB: why a catch-up cannot run now (resync_wait_reason, the resync
runner's start gate), which catch-up a not-yet-started re-balance of the LVS
waits for (resync_holding_rebalance), and the open device-migration tasks of a
cluster (DBController.get_active_device_migration_tasks). Zone desyncs are
recorded through the real event path, which also queues the LVS's
FN_SYNC_RESYNC task. The predicates read the database only; no storage node
is asked.
"""
import itertools
import uuid

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db

PRIMARY_ZONE = "primary_zone_unavailable"
SECONDARY_ZONE = "secondary_zone_unavailable"

_ts_seq = itertools.count()


def _desync(node, vuid=11, status=SECONDARY_ZONE):
    """A zone desync event of ``node``'s instance, through the real path."""
    return src.record_sync_event(node.get_id(), {
        "timestamp": f"2026-11-03T11:00:00.{next(_ts_seq):06d}Z", "event_type": "sync_replication_status",
        "status": status, "vuid": vuid})


def _layout(db):
    """a0 owns LVS_1: home triplet a0 a1 a2, remote triplet b0 b1 b2."""
    cluster = stack._seed_cluster(db)
    a, b = stack._sites(db, cluster)
    owner = stack._make_owner(db, a[0], a, b, lvs_id=1)
    return cluster, a, b, owner


def _resync_task(db, cluster, lvs_name="LVS_1"):
    tasks = [t for t in db.get_active_sync_resync_tasks(cluster.get_id())
             if t.function_params.get("lvs_name") == lvs_name]
    assert len(tasks) == 1, tasks
    return tasks[0]


def _set(db, task, **fields):
    """Commit ``fields`` onto the stored ``task`` (another actor's write)."""
    def _apply(fresh):
        for key, value in fields.items():
            setattr(fresh, key, value)
        return True
    return db.atomic_update(db.get_task_by_id(task.uuid), _apply)


def _with_params(db, task, **params):
    return _set(db, task, function_params={**db.get_task_by_id(task.uuid).function_params, **params})


def _lose_site(db, cluster, site):
    cluster = db.get_cluster_by_id(cluster.get_id())
    cluster.lost_site, cluster.lost_site_state = site, Cluster.LOST_SITE_DOWN
    cluster.write_to_db(db.kv_store)
    return cluster


def _dev_down(db, node, status=NVMeDevice.STATUS_UNAVAILABLE):
    fresh = db.get_storage_node_by_id(node.get_id())
    dev = NVMeDevice(fresh.nvme_devices[0].to_dict())
    dev.status = status
    return stack._update(db, fresh, nvme_devices=[dev]), dev


def _reason(db, cluster, owner, task):
    return src.resync_wait_reason(db.get_cluster_by_id(cluster.get_id()),
                                  db.get_storage_node_by_id(owner.get_id()),
                                  db.get_storage_nodes_by_cluster_id(cluster.get_id()),
                                  db.get_task_by_id(task.uuid))


def _holding(db, cluster, owner):
    return src.resync_holding_rebalance(db.get_cluster_by_id(cluster.get_id()),
                                        db.get_storage_node_by_id(owner.get_id()),
                                        db.get_storage_nodes_by_cluster_id(cluster.get_id()))


_STARTED = {"leader_id": "x", "started_at": 1.0, "distribs": ["distrib_11"]}


# ---------------------------------------------------------------------------
# resync_wait_reason
# ---------------------------------------------------------------------------

class TestResyncWaitReason:

    def test_a_lost_site_waits(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _resync_task(db, cluster)
        _lose_site(db, cluster, SITE_B)
        assert _reason(db, cluster, owner, task) == f"site {SITE_B} is lost, waiting for its return"

    def test_a_lagging_zone_node_offline_waits(self, db):
        cluster, a, b, owner = _layout(db)
        _desync(a[0])                                     # the replica zone (site B) is behind
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        reason = _reason(db, cluster, owner, _resync_task(db, cluster))
        assert reason == f"waiting for the lagging zone(s) ['{SITE_B}'] to be up: ['node:{b[0].get_id()}']"

    def test_a_lagging_zone_device_not_online_waits(self, db):
        cluster, a, b, owner = _layout(db)
        _desync(a[0])
        _, dev = _dev_down(db, b[1])
        reason = _reason(db, cluster, owner, _resync_task(db, cluster))
        assert reason == f"waiting for the lagging zone(s) ['{SITE_B}'] to be up: ['dev:{dev.get_id()}']"

    def test_the_home_zone_lagging_waits_for_the_home_site(self, db):
        cluster, a, b, owner = _layout(db)
        _desync(a[0], status=PRIMARY_ZONE)                # the primary zone (site A) is behind
        task = _resync_task(db, cluster)
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        assert _reason(db, cluster, owner, task) == ""    # site B is not the lagging zone
        stack._update(db, a[1], status=StorageNode.STATUS_OFFLINE)
        assert _reason(db, cluster, owner, task) == \
            f"waiting for the lagging zone(s) ['{SITE_A}'] to be up: ['node:{a[1].get_id()}']"

    @pytest.mark.parametrize("down_site", [SITE_A, SITE_B])
    def test_both_zones_lagging_wait_for_either_site(self, db, down_site):
        cluster, a, b, owner = _layout(db)
        _desync(a[0], status=PRIMARY_ZONE)
        _desync(a[0], status=SECONDARY_ZONE)
        down = (a if down_site == SITE_A else b)[1]
        stack._update(db, down, status=StorageNode.STATUS_OFFLINE)
        reason = _reason(db, cluster, owner, _resync_task(db, cluster))
        assert reason.startswith("waiting for the lagging zone(s) ")
        assert f"'{SITE_A}'" in reason and f"'{SITE_B}'" in reason
        assert reason.endswith(f"to be up: ['node:{down.get_id()}']")

    def test_the_zone_up_can_run(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        assert _reason(db, cluster, owner, _resync_task(db, cluster)) == ""

    def test_a_down_node_outside_the_lagging_zone_is_ignored(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])                                     # replica zone (B) behind
        stack._update(db, a[2], status=StorageNode.STATUS_OFFLINE)
        assert _reason(db, cluster, owner, _resync_task(db, cluster)) == ""

    def test_a_started_catch_up_runs_with_the_zone_down(self, db):
        cluster, a, b, owner = _layout(db)
        _desync(a[0])
        task = _with_params(db, _resync_task(db, cluster), resync=_STARTED)
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        assert _reason(db, cluster, owner, task) == ""

    def test_a_started_catch_up_waits_for_a_lost_site(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _with_params(db, _resync_task(db, cluster), resync=_STARTED)
        _lose_site(db, cluster, SITE_B)
        assert _reason(db, cluster, owner, task) == f"site {SITE_B} is lost, waiting for its return"


# ---------------------------------------------------------------------------
# resync_holding_rebalance
# ---------------------------------------------------------------------------

class TestResyncHoldingRebalance:

    def test_no_task_holds_nothing(self, db):
        cluster, _, _, owner = _layout(db)
        assert _holding(db, cluster, owner) == ""

    @pytest.mark.parametrize("status", [JobSchedule.STATUS_NEW, JobSchedule.STATUS_RUNNING,
                                        JobSchedule.STATUS_SUSPENDED])
    def test_an_active_task_with_the_zone_up_holds(self, db, status):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _set(db, _resync_task(db, cluster), status=status)
        assert _holding(db, cluster, owner) == task.uuid

    @pytest.mark.parametrize("fields", [{"status": JobSchedule.STATUS_DONE}, {"canceled": True}],
                             ids=["done", "canceled"])
    def test_a_done_or_canceled_task_holds_nothing(self, db, fields):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        _set(db, _resync_task(db, cluster), **fields)
        assert _holding(db, cluster, owner) == ""

    def test_a_task_waiting_for_its_zone_holds_nothing(self, db):
        cluster, a, b, owner = _layout(db)
        _desync(a[0])
        _resync_task(db, cluster)
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        assert _holding(db, cluster, owner) == ""

    def test_a_task_waiting_for_a_lost_site_holds_nothing(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        _with_params(db, _resync_task(db, cluster), resync=_STARTED)
        _lose_site(db, cluster, SITE_B)
        assert _holding(db, cluster, owner) == ""

    def test_the_alert_run_count_releases_the_hold(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _set(db, _resync_task(db, cluster), retry=constants.SYNC_RESYNC_ALERT_RUNS - 1)
        assert _holding(db, cluster, owner) == task.uuid
        _set(db, task, retry=constants.SYNC_RESYNC_ALERT_RUNS)
        assert _holding(db, cluster, owner) == ""

    def test_a_held_task_holds_nothing(self, db):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _resync_task(db, cluster)
        assert _holding(db, cluster, owner) == task.uuid
        _with_params(db, task, held_by={"task": "t", "function": JobSchedule.FN_DEV_MIG,
                                        "node": a[0].get_id(), "since": 1.0})
        assert _holding(db, cluster, owner) == ""

    def test_a_task_of_another_lvs_holds_nothing(self, db):
        cluster, a, b, owner = _layout(db)
        other = stack._make_owner(db, b[0], b, a, lvs_id=2)
        _desync(b[0], vuid=21)                            # LVS_2 only
        task = _resync_task(db, cluster, "LVS_2")
        assert _holding(db, cluster, owner) == ""
        assert _holding(db, cluster, other) == task.uuid


# ---------------------------------------------------------------------------
# DBController.get_active_device_migration_tasks
# ---------------------------------------------------------------------------

def _write_task(db, cluster, function_name, date, *, status=JobSchedule.STATUS_NEW, canceled=False):
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = "node"
    task.function_name = function_name
    task.function_params = {}
    task.status = status
    task.canceled = canceled
    task.date = date
    task.max_retry = -1
    task.write_to_db(db.kv_store)
    return task


class TestActiveDeviceMigrationTasks:

    def test_the_three_families_oldest_first(self, db):
        cluster = stack._seed_cluster(db)
        other_cluster = stack._seed_cluster(db)
        # Dates interleave the families, so only a sort over all of them
        # yields this order.
        expected = [
            _write_task(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, 100),
            _write_task(db, cluster, JobSchedule.FN_DEV_MIG, 200, status=JobSchedule.STATUS_RUNNING,
                        canceled=True),
            _write_task(db, cluster, JobSchedule.FN_NEW_DEV_MIG, 300, status=JobSchedule.STATUS_SUSPENDED),
            _write_task(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, 400, status=JobSchedule.STATUS_SUSPENDED),
            _write_task(db, cluster, JobSchedule.FN_DEV_MIG, 500),
            _write_task(db, cluster, JobSchedule.FN_NEW_DEV_MIG, 600),
        ]
        _write_task(db, cluster, JobSchedule.FN_DEV_MIG, 50, status=JobSchedule.STATUS_DONE)
        _write_task(db, cluster, JobSchedule.FN_LVOL_MIG, 150)
        _write_task(db, cluster, JobSchedule.FN_SYNC_RESYNC, 250)
        _write_task(db, other_cluster, JobSchedule.FN_DEV_MIG, 350)
        found = db.get_active_device_migration_tasks(cluster.get_id())
        assert [t.uuid for t in found] == [t.uuid for t in expected]
