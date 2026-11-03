"""Tests for the device-migration runners on a sync-replication cluster: the
restart re-balance, the failed-device migration and the new-device
migration. The database is real FoundationDB. The storage node RPCs are
mocked, and they answer the leadership question for each LVS name.

- A task that has not started first moves to the primary of its LVS's active
  triplet (tasks_controller.migration_role_node).
- It waits while the LVS is being moved, and it is skipped when that primary
  is departing.
- It then runs on that primary. The leadership check uses the OWNER's LVS.
- A started task is polled on its node until it ends.
- The checks that use the task's node (eligibility, the shutdown guard, the
  cancellation at a shutdown) use the node that runs the task.
- On a cluster without sync replication, the migration runs on the owner and
  the leadership check uses the owner's own LVS.
- A re-balance or new-device migration that has not started waits while the
  zone catch-up of its LVS can run (failure > resync > re-balance). A
  failed-device migration never waits for it.
- A start answered EBUSY (the distrib's migration slot is busy) is a wait for a
  sync-replication task, and a failed attempt for any other task.
"""
import errno
import types
import uuid
from unittest.mock import patch

import pytest

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.controllers import (
    migration_controller,
    sync_replication_controller,
    tasks_controller,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError
from simplyblock_core.services import migration_task_common as mig
from simplyblock_core.services import (
    storage_node_monitor,
    tasks_runner_failed_migration,
    tasks_runner_migration,
    tasks_runner_new_dev_migration,
)
from simplyblock_core.utils import ttl_cache
from tests.integration import test_migration_placement_sync as placement
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration import test_sync_resync_predicates as predicates
from tests.integration.task_driver import drive, handler_reached

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs

DONE, SUSPENDED, RUNNING = JobSchedule.STATUS_DONE, JobSchedule.STATUS_SUSPENDED, JobSchedule.STATUS_RUNNING
COMPLETED = [{"status": "completed", "error": 0, "progress": 100}]
IN_PROGRESS = [{"status": "running", "error": 0, "progress": 10}]

RUNNERS = {
    JobSchedule.FN_DEV_MIG: tasks_runner_migration,
    JobSchedule.FN_FAILED_DEV_MIG: tasks_runner_failed_migration,
    JobSchedule.FN_NEW_DEV_MIG: tasks_runner_new_dev_migration,
}
START_RPC = {
    JobSchedule.FN_DEV_MIG: "distr_migration_expansion_start",
    JobSchedule.FN_FAILED_DEV_MIG: "distr_migration_failure_start",
    JobSchedule.FN_NEW_DEV_MIG: "distr_migration_expansion_start",
}
_MIGRATION_RPCS = ("distr_migration_expansion_start", "distr_migration_failure_start", "distr_migration_status")


@pytest.fixture(autouse=True)
def _runners():
    """Make the runners' sleeps return at once, and record the calls that tag
    a device as migrated."""
    ttl_cache.invalidate_all()
    no_sleep = types.SimpleNamespace(sleep=lambda _seconds: None)
    with patch.object(tasks_runner_failed_migration, "time", no_sleep), \
            patch.object(tasks_runner_new_dev_migration, "time", no_sleep), \
            patch.object(tasks_runner_failed_migration.device_controller, "device_set_failed_and_migrated"):
        yield
    ttl_cache.invalidate_all()


# ---------------------------------------------------------------------------
# seeding and the mocked data plane
# ---------------------------------------------------------------------------

def _layout(db, *, active_site=SITE_B, **kwargs):
    """a0 owns LVS_1, with the remote triplet b0, b1, b2. b0 owns LVS_2, with
    the remote triplet a0, a1, a2. ``active_site`` is the site that leads
    LVS_1; the default is SITE_B, so the other site leads LVS_1."""
    return placement._layout(db, active_site=active_site, **kwargs)


def _non_sync(db):
    """The same two owners on a cluster without sync replication: no sites,
    no remote triplets."""
    cluster, a, b, _, _ = stack._two_owner_layout(db)
    cluster = db.get_cluster_by_id(cluster.get_id())
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    nodes = [stack._update(db, n, site="", remote_primary_node_id="", remote_secondary_node_id="",
                           remote_tertiary_node_id="") for n in (*a, *b)]
    return cluster, nodes[:3], nodes[3:]


def _leads(rpcs, node, *lvs_names):
    """``node`` reports the leadership of ``lvs_names`` only."""
    names = set(lvs_names)
    rpcs[node.get_id()].get_lvstore.side_effect = lambda name, *a, **k: {"lvs leadership": name in names}


def _data_plane(rpcs, nodes, status=COMPLETED):
    """Every migration start on ``nodes`` succeeds; a poll answers ``status``."""
    for node in nodes:
        rpc = rpcs[node.get_id()]
        rpc.distr_migration_expansion_start.return_value = True
        rpc.distr_migration_failure_start.return_value = True
        rpc.distr_migration_status.return_value = status


def _started(rpcs, node, function_name):
    """The distribs whose migration ``node`` was asked to start."""
    return sorted(c.args[0] for c in getattr(rpcs[node.get_id()], START_RPC[function_name]).call_args_list)


def _no_migration_rpc(rpcs, *nodes):
    for node in nodes:
        for method in _MIGRATION_RPCS:
            getattr(rpcs[node.get_id()], method).assert_not_called()


def _open(db, cluster, function_name, owner=None):
    """The open ``function_name`` tasks, of ``owner``'s LVS only when given."""
    tasks = placement._tasks(db, cluster, function_name)
    if owner is not None:
        tasks = [t for t in tasks if t.function_params.get("lvs_owner") == owner.get_id()]
    return sorted(tasks, key=lambda t: t.function_params["distr_name"])


def _queue(db, cluster, function_name, node):
    """Queue ``function_name`` through its creation path: the restart of
    ``node`` for the re-balance, ``node``'s device for the others."""
    if function_name == JobSchedule.FN_DEV_MIG:
        assert tasks_controller.add_device_mig_task_for_node(node.get_id())
        return None
    dev_id = placement._device(db, cluster, node)
    if function_name == JobSchedule.FN_FAILED_DEV_MIG:
        tasks_controller.add_device_failed_mig_task(dev_id)
    else:
        tasks_controller.add_new_device_mig_task(dev_id)
    return dev_id


def _write_task(db, cluster, function_name, node_id, params, status=JobSchedule.STATUS_NEW, device_id=""):
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = node_id
    task.device_id = device_id
    task.function_name = function_name
    task.function_params = params
    task.status = status
    task.max_retry = -1
    task.write_to_db(db.kv_store)
    return task


def _set(db, task, **fields):
    """Commit ``fields`` onto the stored ``task`` (another actor's write)."""
    def _apply(fresh):
        for key, value in fields.items():
            setattr(fresh, key, value)
        return True
    return db.atomic_update(db.get_task_by_id(task.uuid), _apply)


def _drive_all(db, tasks):
    return [drive(RUNNERS[t.function_name].SPEC, t) for t in tasks]


# ---------------------------------------------------------------------------
# a switched LVS: the remote primary runs its migrations
# ---------------------------------------------------------------------------

class TestSwitchedLvs:

    def test_a_serving_site_restart_rebalances_on_the_remote_primary(self, db, rpcs):
        """Site B leads LVS_1, and b1 restarts. The sub-tasks of LVS_1 run on
        b0, which leads LVS_1. They finish when the status is ``completed``,
        and then the restart's master task closes. a0 is the passive owner and
        gets no RPC."""
        cluster, a, b, owner_a, owner_b = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])
        tasks = _open(db, cluster, JobSchedule.FN_DEV_MIG)
        assert {t.node_id for t in tasks} == {b[0].get_id()}

        assert [t.status for t in _drive_all(db, tasks)] == [DONE] * 4
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == placement.LVS_1_DISTRIBS + placement.LVS_2_DISTRIBS
        _no_migration_rpc(rpcs, *a, b[1], b[2])

        tasks_runner_migration.update_master_tasks(cluster.get_id())
        [master] = [t for t in db.get_job_tasks(cluster.get_id())
                    if t.function_name == JobSchedule.FN_BALANCING_AFTER_NODE_RESTART]
        assert master.status == DONE

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_every_family_runs_on_the_remote_primary(self, db, rpcs, function_name):
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, b[1])
        tasks = _open(db, cluster, function_name, owner_a)
        assert [t.node_id for t in tasks] == [b[0].get_id()] * 2

        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, b[0], function_name) == placement.LVS_1_DISTRIBS
        _no_migration_rpc(rpcs, *a)

    def test_the_owners_lvs_led_by_an_acting_secondary_waits(self, db, rpcs):
        """b0 leads its own LVS_2, but b1 leads LVS_1 (an in-site failover).
        The migration of LVS_1 does not start. The check asks about LVS_1,
        not about the LVS in b0's own lvstore field."""
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_2")
        _leads(rpcs, b[1], "LVS_1")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])

        for task in _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a):
            fresh = drive(tasks_runner_migration.SPEC, task)
            assert (fresh.status, fresh.retry, fresh.node_id) == (SUSPENDED, 1, b[0].get_id())
            assert fresh.function_result == f"Node {b[0].get_id()} is not the leader for LVS_1, deferring migration"
        _no_migration_rpc(rpcs, *a, *b)

    def test_it_runs_while_its_own_lvs_is_switched_away(self, db, rpcs):
        """Site A leads LVS_2, the LVS of b0. b0 leads only LVS_1. The
        migration of LVS_1 starts on b0 and stays there on every runner pass."""
        cluster, a, b, owner_a, owner_b = _layout(db)
        stack._update(db, owner_b, lvs_active_site=SITE_A)
        _leads(rpcs, b[0], "LVS_1")
        _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])
        [task, _] = _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a)

        for _ in range(3):
            fresh = drive(tasks_runner_migration.SPEC, task)
            assert (fresh.status, fresh.node_id) == (RUNNING, b[0].get_id())
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == ["distrib_11"]
        rpcs[b[0].get_id()].distr_migration_status.assert_called_with(name="distrib_11")
        # LVS_2's sub-tasks follow LVS_2 to a0, LVS_1's stay on b0.
        assert {t.function_params["distr_name"]: t.node_id
                for t in _open(db, cluster, JobSchedule.FN_DEV_MIG)}["distrib_11"] == b[0].get_id()

    def test_the_passive_owner_returning_rebalances_on_the_serving_primary(self, db, rpcs):
        """Site A is lost, and LVS_1 is promoted to site B. When a0 returns,
        its restart queues the re-balance of LVS_1 on b0, and b0 runs it.
        Nothing runs on site A."""
        cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db, lost_site=SITE_A)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, a[0])
        tasks = _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a)
        assert [t.node_id for t in tasks] == [b[0].get_id()] * 2

        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == placement.LVS_1_DISTRIBS
        _no_migration_rpc(rpcs, *a)


# ---------------------------------------------------------------------------
# a promote between the queueing and the end
# ---------------------------------------------------------------------------

class TestPromote:

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_task_not_started_follows_the_promote(self, db, rpcs, function_name):
        """A task is queued on b0, and then LVS_1 is promoted back to the home
        site. The first runner pass moves the task to a0 and starts nothing.
        The next pass runs it on a0."""
        cluster, a, b, owner_a, _ = _layout(db)
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, b[1])
        task = _open(db, cluster, function_name, owner_a)[0]
        stack._update(db, owner_a, lvs_active_site="")
        _leads(rpcs, a[0], "LVS_1")
        _leads(rpcs, b[0], "LVS_2")

        moved = drive(RUNNERS[function_name].SPEC, task)
        assert (moved.status, moved.retry, moved.node_id) == (SUSPENDED, 0, a[0].get_id())
        assert moved.function_result == (
            f"moved {function_name} of distrib_11 (LVS_1) from {b[0].get_id()} to {a[0].get_id()}, "
            f"the primary of its active triplet")
        _no_migration_rpc(rpcs, *a, *b)

        assert drive(RUNNERS[function_name].SPEC, task).status == DONE
        assert _started(rpcs, a[0], function_name) == ["distrib_11"]
        _no_migration_rpc(rpcs, *b)

    def test_a_started_task_is_polled_on_its_node_to_the_end(self, db, rpcs):
        """A migration starts on b0, and then LVS_1 is promoted back to the
        home site. b0 does not lead LVS_1 now, but the runner still polls the
        task on b0 until the status is ``completed``. The task does not move,
        and nothing starts on a0."""
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1")
        _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])
        task = _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a)[0]
        assert drive(tasks_runner_migration.SPEC, task).status == RUNNING

        stack._update(db, owner_a, lvs_active_site="")
        _leads(rpcs, b[0])
        _leads(rpcs, a[0], "LVS_1")
        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        fresh = drive(tasks_runner_migration.SPEC, task)

        assert (fresh.status, fresh.node_id) == (DONE, b[0].get_id())
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == ["distrib_11"]
        _no_migration_rpc(rpcs, *a)

    @pytest.mark.parametrize("target", [SITE_A, SITE_B])
    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_move_in_flight_waits(self, db, rpcs, function_name, target):
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, b[1])
        task = _open(db, cluster, function_name, owner_a)[0]
        stack._update(db, owner_a, lvs_active_site=f"moving:{target}")

        fresh = drive(RUNNERS[function_name].SPEC, task)
        assert (fresh.status, fresh.retry, fresh.node_id) == (SUSPENDED, 0, b[0].get_id())
        assert fresh.function_result == (
            f"LVS_1 (owner {owner_a.get_id()}) is being moved (moving:{target}); "
            f"placing {function_name} of distrib_11 once the move settles")
        _no_migration_rpc(rpcs, *a, *b)


# ---------------------------------------------------------------------------
# the home site leads, or the cluster has no sync replication: the owner runs it
# ---------------------------------------------------------------------------

class TestUnswitched:

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_home_led_runs_on_the_owner(self, db, rpcs, function_name):
        cluster, a, b, owner_a, _ = _layout(db, active_site="")
        _leads(rpcs, a[0], "LVS_1")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, a[1])
        tasks = _open(db, cluster, function_name, owner_a)
        assert [t.node_id for t in tasks] == [a[0].get_id()] * 2

        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, a[0], function_name) == placement.LVS_1_DISTRIBS
        _no_migration_rpc(rpcs, *b)

    def test_without_sync_replication_the_owner_runs_under_its_own_lvs(self, db, rpcs):
        cluster, a, b = _non_sync(db)
        _leads(rpcs, a[0], "LVS_1")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, a[1])
        tasks = [t for t in _open(db, cluster, JobSchedule.FN_DEV_MIG) if t.node_id == a[0].get_id()]
        assert [t.function_params for t in tasks] == [{"distr_name": d} for d in placement.LVS_1_DISTRIBS]

        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, a[0], JobSchedule.FN_DEV_MIG) == placement.LVS_1_DISTRIBS

    def test_without_sync_replication_a_non_leader_owner_waits(self, db, rpcs):
        cluster, a, b = _non_sync(db)
        _leads(rpcs, a[0], "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, a[1])
        task = [t for t in _open(db, cluster, JobSchedule.FN_DEV_MIG) if t.node_id == a[0].get_id()][0]

        fresh = drive(tasks_runner_migration.SPEC, task)
        assert (fresh.status, fresh.retry) == (SUSPENDED, 1)
        assert fresh.function_result == f"Node {a[0].get_id()} is not the leader for LVS_1, deferring migration"
        _no_migration_rpc(rpcs, *a)


# ---------------------------------------------------------------------------
# boundaries
# ---------------------------------------------------------------------------

class TestBoundaries:

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_sync_task_without_its_owner_is_aborted(self, db, rpcs, function_name):
        cluster, a, b, _, _ = _layout(db)
        dev_id = placement._device(db, cluster, a[1])
        task = _write_task(db, cluster, function_name, b[0].get_id(), {"distr_name": "distrib_11"},
                           device_id=dev_id)

        fresh = drive(RUNNERS[function_name].SPEC, task)
        assert fresh.status == DONE
        assert fresh.function_result == (f"{function_name} of distrib_11 on {b[0].get_id()} has no lvs_owner: "
                                         f"a sync-replication migration task must name its LVS owner")
        _no_migration_rpc(rpcs, *a, *b)

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_missing_owner_is_aborted(self, db, rpcs, function_name):
        cluster, a, b, _, _ = _layout(db)
        dev_id = placement._device(db, cluster, a[1])
        task = _write_task(db, cluster, function_name, b[0].get_id(),
                           {"distr_name": "distrib_11", "lvs_name": "LVS_1", "lvs_owner": "gone"},
                           device_id=dev_id)

        fresh = drive(RUNNERS[function_name].SPEC, task)
        assert (fresh.status, fresh.function_result) == (DONE, "Owner gone of LVS_1 not found")
        _no_migration_rpc(rpcs, *a, *b)

    @pytest.mark.parametrize("slot", placement.DANGLING_PEERS)
    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_missing_peer_does_not_stop_a_queued_task(self, db, rpcs, function_name, slot):
        """The tasks of LVS_1 are queued on b0. Then the secondary or tertiary
        id of its remote triplet names a node that does not exist. b0 is online
        and leads LVS_1, so it still starts and completes the tasks."""
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, b[1])
        tasks = _open(db, cluster, function_name, owner_a)
        assert [t.node_id for t in tasks] == [b[0].get_id()] * 2
        stack._update(db, owner_a, **{slot: "no-such-node"})

        finished = _drive_all(db, tasks)
        assert [(t.status, t.node_id) for t in finished] == [(DONE, b[0].get_id())] * 2
        assert _started(rpcs, b[0], function_name) == placement.LVS_1_DISTRIBS
        _no_migration_rpc(rpcs, *a)

    @pytest.mark.parametrize("status", list(StorageNode.DEPARTING_STATUSES))
    @pytest.mark.parametrize("function_name", [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_a_departing_primary_skips_the_rebalance(self, db, rpcs, function_name, status):
        """b0 is the primary of the active triplets of both LVSs, and its
        removal starts after the tasks are queued. Every sub-task ends DONE
        with "skipped" and sends no RPC. No open task is left that would block
        the cluster's volume drain."""
        cluster, a, b, _, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, function_name, b[1])
        tasks = _open(db, cluster, function_name)
        assert len(tasks) == 4
        placement._status(db, b[0], status)

        for fresh in _drive_all(db, tasks):
            lvs = fresh.function_params["lvs_name"]
            assert (fresh.status, fresh.function_result) == (
                DONE, f"skipped: primary {b[0].get_id()} of {lvs} is departing")
        _no_migration_rpc(rpcs, *a, *b)
        tasks_runner_migration.update_master_tasks(cluster.get_id())
        assert migration_controller._can_add_lvol_migration(cluster.get_id())

    def test_no_new_device_migration_is_queued_on_a_shut_down_primary(self, db):
        cluster, a, b, owner_a, _ = _layout(db)
        placement._status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        _queue(db, cluster, JobSchedule.FN_NEW_DEV_MIG, b[1])
        assert _open(db, cluster, JobSchedule.FN_NEW_DEV_MIG) == []

    @pytest.mark.parametrize("status", list(StorageNode.REMOVAL_SHUT_DOWN_STATUSES))
    def test_a_failed_device_migration_of_a_shut_down_primary_moves_to_the_next_member(self, db, rpcs, status):
        cluster, a, b, owner_a, _ = _layout(db)
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[1])
        task = _open(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, owner_a)[0]
        assert task.node_id == b[0].get_id()
        placement._status(db, b[0], status)

        moved = drive(tasks_runner_failed_migration.SPEC, task)
        assert (moved.status, moved.node_id) == (SUSPENDED, b[1].get_id())
        assert drive(tasks_runner_failed_migration.SPEC, task).status == DONE
        assert _started(rpcs, b[1], JobSchedule.FN_FAILED_DEV_MIG) == ["distrib_11"]
        _no_migration_rpc(rpcs, *a, b[0], b[2])

    def test_a_failed_device_migration_waits_while_the_whole_active_triplet_departs(self, db, rpcs):
        cluster, a, b, owner_a, _ = _layout(db)
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[1])
        task = _open(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, owner_a)[0]
        for node in b:
            placement._status(db, node, StorageNode.STATUS_IN_REMOVAL)

        fresh = drive(tasks_runner_failed_migration.SPEC, task)
        assert (fresh.status, fresh.retry, fresh.node_id) == (SUSPENDED, 0, b[0].get_id())
        members = ", ".join(n.get_id() for n in b)
        assert fresh.function_result == (f"every member of the active triplet of LVS_1 ({members}) "
                                         f"is departing; waiting")
        _no_migration_rpc(rpcs, *a, *b)


# ---------------------------------------------------------------------------
# the move is one atomic update; then the new node's eligibility applies
# ---------------------------------------------------------------------------

def _queued_on_b0(db):
    """LVS_1 led from B: its first restart sub-task, queued on b0."""
    cluster, a, b, owner_a, owner_b = _layout(db)
    _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])
    return cluster, a, b, owner_a, _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a)[0]


class TestMove:

    def test_the_move_wins_on_an_unchanged_row(self, db):
        _, a, b, _, task = _queued_on_b0(db)
        assert mig.move_unstarted_task(task, a[0].get_id()) is True
        assert db.get_task_by_id(task.uuid).node_id == a[0].get_id()

    @pytest.mark.parametrize("change", ["started", "canceled", "done", "moved"])
    def test_the_move_loses_to_a_change_since_its_read(self, db, change):
        """The move uses an old copy of the task. If the stored task was
        started, canceled, finished or moved since then, that change stays, and
        the task stays on the node of the stored task."""
        _, a, b, _, stale = _queued_on_b0(db)
        node_id = b[0].get_id()
        if change == "started":
            _set(db, stale, function_params=dict(stale.function_params, migration={"name": "distrib_11"}))
        elif change == "canceled":
            _set(db, stale, canceled=True)
        elif change == "done":
            _set(db, stale, status=DONE)
        else:
            node_id = b[2].get_id()
            _set(db, stale, node_id=node_id)
        before = db.get_task_by_id(stale.uuid)

        assert mig.move_unstarted_task(stale, a[0].get_id()) is False
        after = db.get_task_by_id(stale.uuid)
        assert after.node_id == node_id
        assert after.to_dict() == before.to_dict()

    def test_a_moved_task_waits_behind_a_sibling_on_either_node(self, db, rpcs):
        """The task has moved to a0. It waits while another migration of its
        distrib runs on a0, and also while one runs on b0. Only one task at a
        time migrates a sync-replication distrib, on all nodes together."""
        cluster, a, b, owner_a, task = _queued_on_b0(db)
        stack._update(db, owner_a, lvs_active_site="")
        assert drive(tasks_runner_migration.SPEC, task).node_id == a[0].get_id()

        on_a0 = _write_task(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[0].get_id(),
                            {"distr_name": "distrib_11"}, status=RUNNING)
        on_b0 = _write_task(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, b[0].get_id(),
                            {"distr_name": "distrib_11"}, status=RUNNING)
        assert not handler_reached(tasks_runner_migration.SPEC, task)

        _set(db, on_a0, status=DONE)
        assert not handler_reached(tasks_runner_migration.SPEC, task)

        _set(db, on_b0, status=DONE)
        assert handler_reached(tasks_runner_migration.SPEC, task)


# ---------------------------------------------------------------------------
# a migration still running on the old primary blocks its distrib on the new one
# ---------------------------------------------------------------------------

def _started_on_b0_then_promoted(db, rpcs, function_name):
    """Site B leads LVS_1. A ``function_name`` migration of distrib_11 is
    queued through b1 and starts on b0. Then LVS_1 is promoted back to the
    home site, so a0 leads it while the migration still runs on b0."""
    cluster, a, b, owner_a, _ = _layout(db)
    _leads(rpcs, b[0], "LVS_1")
    _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
    _queue(db, cluster, function_name, b[1])
    started = _open(db, cluster, function_name, owner_a)[0]
    assert started.function_params["distr_name"] == "distrib_11"
    assert drive(RUNNERS[function_name].SPEC, started).status == RUNNING

    stack._update(db, owner_a, lvs_active_site="")
    _leads(rpcs, b[0])
    _leads(rpcs, a[0], "LVS_1")
    _data_plane(rpcs, a)
    return cluster, a, b, owner_a, started


def _queued_on(db, cluster, function_name, owner, via, node):
    """Queue ``function_name`` tasks through ``via``, and return the tasks of
    ``owner`` that are on ``node`` as ``{distr_name: task}``."""
    dev_id = _queue(db, cluster, function_name, via)
    tasks = {t.function_params["distr_name"]: t for t in _open(db, cluster, function_name, owner)
             if t.node_id == node.get_id() and (dev_id is None or t.device_id == dev_id)}
    assert sorted(tasks) == placement.LVS_1_DISTRIBS
    return tasks


def _eligible(db, spec, task, cluster):
    return spec.is_eligible(db.get_task_by_id(task.uuid), cluster)


#: Each entry is (the task type that runs on b0, the task type queued on a0
#: after it, the index of the b node that queues it). The two tasks have
#: different types, or they are failed-device migrations of different devices.
HELD_BACK = [
    (JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, 1),
    (JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_DEV_MIG, 1),
    (JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, 2),
]

#: Each entry is (the type of the waiting task, the type of the other task,
#: the status of the other task). In each case the other task, a migration of
#: the same distrib, stops the waiting task from starting.
SIBLINGS = [
    (JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, RUNNING),
    (JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG, SUSPENDED),
    (JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_DEV_MIG, RUNNING),
    (JobSchedule.FN_NEW_DEV_MIG, JobSchedule.FN_DEV_MIG, RUNNING),
]


class TestStartedSiblingOnTheFormerPrimary:

    @pytest.mark.parametrize("started_fn, waiting_fn, via", HELD_BACK)
    def test_the_new_primary_waits_for_the_run_on_the_former_one(self, db, rpcs, started_fn, waiting_fn, via):
        """While the migration of distrib_11 runs on b0, another migration of
        distrib_11 on a0 does not start. The other distrib of LVS_1 is not
        blocked, and the runner keeps polling the migration on b0. When it
        ends, the migration on a0 starts."""
        cluster, a, b, owner_a, started = _started_on_b0_then_promoted(db, rpcs, started_fn)
        queued = _queued_on(db, cluster, waiting_fn, owner_a, b[via], a[0])
        spec = RUNNERS[waiting_fn].SPEC

        assert not _eligible(db, spec, queued["distrib_11"], cluster)
        assert not handler_reached(spec, queued["distrib_11"])
        assert _eligible(db, spec, queued["distrib_12"], cluster)
        assert _eligible(db, RUNNERS[started_fn].SPEC, started, cluster)
        _no_migration_rpc(rpcs, *a)

        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        assert drive(RUNNERS[started_fn].SPEC, started).status == DONE
        assert _eligible(db, spec, queued["distrib_11"], cluster)
        fresh = drive(spec, queued["distrib_11"])
        assert (fresh.status, fresh.node_id) == (DONE, a[0].get_id())
        assert _started(rpcs, a[0], waiting_fn) == ["distrib_11"]

    def test_a_new_device_migration_waits_for_the_run_on_the_former_primary(self, db, rpcs):
        """The test checks eligibility only. The new-device handler also waits
        while any recovery migration is open, so the handler alone cannot show
        which rule blocks the task."""
        cluster, a, b, owner_a, started = _started_on_b0_then_promoted(db, rpcs, JobSchedule.FN_DEV_MIG)
        queued = _queued_on(db, cluster, JobSchedule.FN_NEW_DEV_MIG, owner_a, b[1], a[0])
        spec = tasks_runner_new_dev_migration.SPEC

        assert not _eligible(db, spec, queued["distrib_11"], cluster)
        assert _eligible(db, spec, queued["distrib_12"], cluster)

        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        assert drive(tasks_runner_migration.SPEC, started).status == DONE
        assert _eligible(db, spec, queued["distrib_11"], cluster)

    @pytest.mark.parametrize("waiting_fn, sibling_fn, status", SIBLINGS)
    def test_a_sibling_on_another_node_holds_back_a_sync_task(self, db, waiting_fn, sibling_fn, status):
        cluster, a, b, owner_a, _ = _layout(db, active_site="")
        params = tasks_controller._sync_mig_params(owner_a, "distrib_11")
        task = _write_task(db, cluster, waiting_fn, a[0].get_id(), params)
        sibling = _write_task(db, cluster, sibling_fn, b[0].get_id(),
                              dict(params, migration={"name": "distrib_11"}), status=status)
        spec = RUNNERS[waiting_fn].SPEC

        assert not _eligible(db, spec, task, cluster)
        if status == RUNNING:
            assert tasks_controller.get_active_node_mig_task(cluster.get_id(), None, "distrib_11") == sibling.uuid
            assert not tasks_controller.get_active_node_mig_task(cluster.get_id(), a[0].get_id(), "distrib_11")

        _set(db, sibling, canceled=True)
        assert _eligible(db, spec, task, cluster)
        _set(db, sibling, canceled=False, status=DONE)
        assert _eligible(db, spec, task, cluster)

    @pytest.mark.parametrize("waiting_fn, sibling_fn, status", SIBLINGS)
    def test_without_sync_replication_only_a_sibling_on_the_node_holds_back(self, db, waiting_fn, sibling_fn,
                                                                           status):
        cluster, a, b = _non_sync(db)
        task = _write_task(db, cluster, waiting_fn, a[0].get_id(), {"distr_name": "distrib_11"})
        params = {"distr_name": "distrib_11", "migration": {"name": "distrib_11"}}
        spec = RUNNERS[waiting_fn].SPEC

        elsewhere = _write_task(db, cluster, sibling_fn, b[0].get_id(), params, status=status)
        assert _eligible(db, spec, task, cluster)

        _set(db, elsewhere, node_id=a[0].get_id())
        assert not _eligible(db, spec, task, cluster)


# ---------------------------------------------------------------------------
# the checks that use the task's node use the node that runs the task
# ---------------------------------------------------------------------------

SHUTDOWN_CANCELS = [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG]


def _running_on_b0(db, rpcs):
    """LVS_1 led from B: its first restart sub-task started on b0."""
    cluster, a, b, owner_a, task = _queued_on_b0(db)
    _leads(rpcs, b[0], "LVS_1")
    _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
    assert drive(tasks_runner_migration.SPEC, task).status == RUNNING
    return cluster, a, b, owner_a, task


class TestNodeKeyed:

    def test_the_running_node_refuses_its_shutdown(self, db, rpcs):
        _, a, b, _, _ = _running_on_b0(db, rpcs)
        allowed, reason = storage_node_ops.check_node_shutdown_preconditions(b[0].get_id())
        assert not allowed
        # The running one and the three sub-tasks queued beside it on b0.
        assert reason.startswith("Migration task found: 4 (device_migration)")

    def test_the_owner_shuts_down_and_the_run_goes_on(self, db, rpcs):
        cluster, a, b, _, task = _running_on_b0(db, rpcs)
        assert storage_node_ops.check_node_shutdown_preconditions(a[0].get_id()) == (True, "")
        assert tasks_controller.cancel_node_tasks(cluster.get_id(), a[0].get_id(), SHUTDOWN_CANCELS) == 0

        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        fresh = drive(tasks_runner_migration.SPEC, task)
        assert (fresh.status, fresh.canceled, fresh.node_id) == (DONE, False, b[0].get_id())

    def test_the_remote_primarys_removal_cancels_what_is_placed_on_it(self, db):
        """The removal shutdown of b0 cancels the sub-tasks on b0, including
        the tasks of a0's LVS. While the removal runs, the re-balance is not
        queued again. A cluster without sync replication does the same."""
        cluster, a, b, owner_a, _ = _layout(db)
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, b[1])
        on_b0 = [t for t in _open(db, cluster, JobSchedule.FN_DEV_MIG) if t.node_id == b[0].get_id()]
        assert len(on_b0) == 4

        placement._status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        assert tasks_controller.cancel_node_tasks(cluster.get_id(), b[0].get_id(), SHUTDOWN_CANCELS) == 4
        assert all(db.get_task_by_id(t.uuid).canceled for t in on_b0)
        assert not tasks_controller.add_device_mig_task_for_node(b[1].get_id())
        assert _open(db, cluster, JobSchedule.FN_DEV_MIG) == []

    def test_a_failed_device_requeue_after_the_removal_lands_on_the_next_member(self, db):
        cluster, a, b, owner_a, _ = _layout(db)
        placement._status(db, b[0], StorageNode.STATUS_IN_REMOVAL)
        _queue(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[1])
        assert {t.node_id for t in _open(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, owner_a)} == {b[1].get_id()}

    def test_the_owner_is_left_out_of_the_status_count_while_its_expansion_runs_remotely(self, db, rpcs):
        cluster, a, b, owner_a, _ = _layout(db)
        _leads(rpcs, b[0], "LVS_1", "LVS_2")
        _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
        _queue(db, cluster, JobSchedule.FN_NEW_DEV_MIG, a[0])
        tasks = _open(db, cluster, JobSchedule.FN_NEW_DEV_MIG, owner_a)
        assert {t.node_id for t in tasks} == {b[0].get_id()}
        assert [t.status for t in _drive_all(db, tasks)] == [RUNNING, RUNNING]

        cluster_id = cluster.get_id()
        assert storage_node_monitor.is_new_migrated_node(cluster_id, stack._fresh(db, a[0]))
        assert not storage_node_monitor.is_new_migrated_node(cluster_id, stack._fresh(db, b[0]))

        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert not storage_node_monitor.is_new_migrated_node(cluster_id, stack._fresh(db, a[0]))

    def test_without_sync_replication_the_status_count_keeps_to_the_executor(self, db):
        """Without sync replication, a new-device task for a device and distrib
        of a0 removes a0 from the node status count only when the task is on
        a0."""
        cluster, a, b = _non_sync(db)
        dev_id = placement._device(db, cluster, a[0])
        task = _write_task(db, cluster, JobSchedule.FN_NEW_DEV_MIG, b[0].get_id(),
                           {"distr_name": "distrib_11"}, device_id=dev_id)
        assert not storage_node_monitor.is_new_migrated_node(cluster.get_id(), stack._fresh(db, a[0]))
        _set(db, task, node_id=a[0].get_id())
        assert storage_node_monitor.is_new_migrated_node(cluster.get_id(), stack._fresh(db, a[0]))


# ---------------------------------------------------------------------------
# a shutdown's cancellation racing the move
# ---------------------------------------------------------------------------

class TestCancelRacesTheMove:

    def test_a_move_between_the_scan_and_the_cancel_is_not_canceled(self, db):
        """The shutdown of b0 finds the task on b0, and then the runner moves
        the task to a0. The cancel sees that the task is not on b0 now, and
        does not cancel it."""
        cluster, a, b, owner_a, task = _queued_on_b0(db)
        real = tasks_controller._cancel_atomically
        moved = []

        def _move_first(scanned, mutate):
            if scanned.uuid == task.uuid:
                moved.append(mig.move_unstarted_task(scanned, a[0].get_id()))
            return real(scanned, mutate)

        with patch.object(tasks_controller, "_cancel_atomically", side_effect=_move_first):
            canceled = tasks_controller.cancel_node_tasks(cluster.get_id(), b[0].get_id(), SHUTDOWN_CANCELS)

        assert moved == [True]
        assert canceled == 3
        fresh = db.get_task_by_id(task.uuid)
        assert (fresh.node_id, fresh.canceled) == (a[0].get_id(), False)

    def test_a_cancel_before_the_move_stops_it(self, db):
        cluster, a, b, owner_a, task = _queued_on_b0(db)
        assert tasks_controller.cancel_node_tasks(cluster.get_id(), b[0].get_id(), SHUTDOWN_CANCELS) == 4

        assert mig.move_unstarted_task(task, a[0].get_id()) is False
        fresh = db.get_task_by_id(task.uuid)
        assert (fresh.node_id, fresh.canceled) == (b[0].get_id(), True)


# ---------------------------------------------------------------------------
# the migration slot: a re-balance waits behind a zone catch-up that can run
# ---------------------------------------------------------------------------

REBALANCE = [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG]


def _resync(db, cluster, owner):
    """Queue the zone catch-up of ``owner``'s LVS. No zone event names a
    lagging zone, so it can run."""
    task_id = tasks_controller.add_sync_resync_task(cluster.get_id(), owner.get_id(), owner.lvstore)
    assert task_id
    return db.get_task_by_id(task_id)


def _holding(db, cluster, owner):
    return sync_replication_controller.resync_holding_rebalance(
        db.get_cluster_by_id(cluster.get_id()), db.get_storage_node_by_id(owner.get_id()),
        db.get_storage_nodes_by_cluster_id(cluster.get_id()))


def _catch_up_wait(task, resync):
    params = task.function_params
    return (f"{params['distr_name']} ({params['lvs_name']}): "
            f"waiting for the zone catch-up {resync.uuid} to finish first")


def _switched(db, rpcs, function_name):
    """Site B leads LVS_1; ``function_name`` tasks queued through b1."""
    cluster, a, b, owner_a, owner_b = _layout(db)
    _leads(rpcs, b[0], "LVS_1", "LVS_2")
    _data_plane(rpcs, (*a, *b))
    _queue(db, cluster, function_name, b[1])
    return cluster, a, b, owner_a, owner_b


class TestResyncGate:

    @pytest.mark.parametrize("function_name", REBALANCE)
    def test_a_rebalance_waits_for_a_catch_up_that_can_run(self, db, rpcs, function_name):
        """The wait consumes no retry and starts nothing. Once the catch-up is
        DONE, the tasks run."""
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        resync = _resync(db, cluster, owner_a)
        tasks = _open(db, cluster, function_name, owner_a)

        for _ in range(3):
            for fresh in _drive_all(db, tasks):
                assert (fresh.status, fresh.retry, fresh.node_id) == (SUSPENDED, 0, b[0].get_id())
                assert fresh.function_result == _catch_up_wait(fresh, resync)
                assert not mig.migration_started(fresh)
        _no_migration_rpc(rpcs, *a, *b)

        _set(db, resync, status=DONE)
        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, b[0], function_name) == placement.LVS_1_DISTRIBS

    @pytest.mark.parametrize("function_name", REBALANCE)
    def test_a_catch_up_waiting_for_a_lost_site_holds_nothing(self, db, rpcs, function_name):
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        predicates._desync(a[0])
        resync = predicates._resync_task(db, cluster)
        predicates._lose_site(db, cluster, SITE_A)
        assert predicates._reason(db, cluster, owner_a, resync) == f"site {SITE_A} is lost, waiting for its return"
        assert _holding(db, cluster, owner_a) == ""

        assert [t.status for t in _drive_all(db, _open(db, cluster, function_name, owner_a))] == [DONE, DONE]
        assert _started(rpcs, b[0], function_name) == placement.LVS_1_DISTRIBS

    @pytest.mark.parametrize("function_name, retry, result", [
        (JobSchedule.FN_DEV_MIG, 0,
         "waiting for unavailable nodes/devices to recover before restarting migration: ['dev:{dev}']"),
        (JobSchedule.FN_NEW_DEV_MIG, 1, "Some devs are offline, retrying"),
    ])
    def test_a_catch_up_waiting_for_its_lagging_zone_holds_nothing(self, db, rpcs, function_name, retry, result):
        """Site B lags, and a device of b2 is not online. The tasks pass the
        catch-up gate and stop at their own wait for the device."""
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        predicates._desync(a[0])
        resync = predicates._resync_task(db, cluster)
        _, dev = predicates._dev_down(db, b[2])
        assert predicates._reason(db, cluster, owner_a, resync) == (
            f"waiting for the lagging zone(s) ['{SITE_B}'] to be up: ['dev:{dev.get_id()}']")
        assert _holding(db, cluster, owner_a) == ""

        for fresh in _drive_all(db, _open(db, cluster, function_name, owner_a)):
            assert (fresh.status, fresh.retry) == (SUSPENDED, retry)
            assert fresh.function_result == result.format(dev=dev.get_id())
        _no_migration_rpc(rpcs, *a, *b)

    @pytest.mark.parametrize("release", ["alert_runs", "held"])
    @pytest.mark.parametrize("function_name", REBALANCE)
    def test_a_catch_up_that_cannot_take_the_slot_holds_nothing(self, db, rpcs, function_name, release):
        """A catch-up that has failed SYNC_RESYNC_ALERT_RUNS runs, or one the
        resync runner records as held behind a device migration, does not hold
        the re-balance."""
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        resync = _resync(db, cluster, owner_a)
        assert _holding(db, cluster, owner_a) == resync.uuid
        if release == "alert_runs":
            _set(db, resync, retry=constants.SYNC_RESYNC_ALERT_RUNS)
        else:
            _set(db, resync, function_params={**resync.function_params, "held_by": {
                "task": "t", "function": JobSchedule.FN_DEV_MIG, "node": b[0].get_id(), "since": 1.0}})
        assert _holding(db, cluster, owner_a) == ""

        assert [t.status for t in _drive_all(db, _open(db, cluster, function_name, owner_a))] == [DONE, DONE]
        assert _started(rpcs, b[0], function_name) == placement.LVS_1_DISTRIBS

    def test_a_catch_up_holds_only_its_own_lvs(self, db, rpcs):
        """The restart of b1 queues the re-balance of LVS_1 and of LVS_2 on b0.
        Only LVS_2 has a catch-up: LVS_1 runs, LVS_2 waits."""
        cluster, a, b, owner_a, owner_b = _switched(db, rpcs, JobSchedule.FN_DEV_MIG)
        resync = _resync(db, cluster, owner_b)

        assert [t.status for t in _drive_all(db, _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a))] == [DONE, DONE]
        for fresh in _drive_all(db, _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_b)):
            assert (fresh.status, fresh.retry) == (SUSPENDED, 0)
            assert fresh.function_result == _catch_up_wait(fresh, resync)
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == placement.LVS_1_DISTRIBS

    def test_a_failed_device_migration_never_waits(self, db, rpcs):
        cluster, a, b, owner_a, _ = _switched(db, rpcs, JobSchedule.FN_FAILED_DEV_MIG)
        resync = _resync(db, cluster, owner_a)
        assert _holding(db, cluster, owner_a) == resync.uuid

        tasks = _open(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, owner_a)
        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, b[0], JobSchedule.FN_FAILED_DEV_MIG) == placement.LVS_1_DISTRIBS

    def test_a_started_rebalance_keeps_polling(self, db, rpcs):
        """The catch-up is queued after the re-balance of distrib_11 started.
        The re-balance is polled on to the end and not started again."""
        cluster, a, b, owner_a, _ = _switched(db, rpcs, JobSchedule.FN_DEV_MIG)
        _data_plane(rpcs, (*a, *b), status=IN_PROGRESS)
        task = _open(db, cluster, JobSchedule.FN_DEV_MIG, owner_a)[0]
        assert drive(tasks_runner_migration.SPEC, task).status == RUNNING
        _resync(db, cluster, owner_a)

        fresh = drive(tasks_runner_migration.SPEC, task)
        assert (fresh.status, fresh.retry, fresh.function_result) == (RUNNING, 0, "Status: running, progress:10")
        rpcs[b[0].get_id()].distr_migration_status.return_value = COMPLETED
        assert drive(tasks_runner_migration.SPEC, task).status == DONE
        assert _started(rpcs, b[0], JobSchedule.FN_DEV_MIG) == ["distrib_11"]

    def test_without_sync_replication_nothing_waits(self, db, rpcs):
        """A catch-up record of LVS_1 that would hold a sync re-balance of a0's
        LVS does not hold a re-balance without sync replication."""
        cluster, a, b = _non_sync(db)
        _leads(rpcs, a[0], "LVS_1")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, a[1])
        resync = _resync(db, cluster, a[0])
        assert _holding(db, cluster, a[0]) == resync.uuid

        tasks = [t for t in _open(db, cluster, JobSchedule.FN_DEV_MIG) if t.node_id == a[0].get_id()]
        assert [t.status for t in _drive_all(db, tasks)] == [DONE, DONE]
        assert _started(rpcs, a[0], JobSchedule.FN_DEV_MIG) == placement.LVS_1_DISTRIBS


# ---------------------------------------------------------------------------
# a start answered EBUSY: the distrib's migration slot is busy
# ---------------------------------------------------------------------------

def _busy():
    return RPCRemoteError("Migration is already in progress", code=errno.EBUSY)


#: Start answers that are a failed attempt also for a sync-replication task.
OTHER_START_FAILURES = {
    "eagain": lambda start: setattr(start, "side_effect", RPCRemoteError("try again", code=errno.EAGAIN)),
    "negative_ebusy": lambda start: setattr(start, "side_effect", RPCRemoteError("busy", code=-errno.EBUSY)),
    "connection": lambda start: setattr(start, "side_effect", RPCConnectionError("Could not reach remote")),
    "false": lambda start: setattr(start, "return_value", False),
}


class TestBusySlot:

    @pytest.mark.parametrize("function_name", list(RUNNERS))
    def test_a_busy_slot_is_a_wait(self, db, rpcs, function_name):
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        task = _open(db, cluster, function_name, owner_a)[0]
        start = getattr(rpcs[b[0].get_id()], START_RPC[function_name])
        start.side_effect = _busy()

        for _ in range(2):
            fresh = drive(RUNNERS[function_name].SPEC, task)
            assert (fresh.status, fresh.retry) == (SUSPENDED, 0)
            assert fresh.function_result == f"migration slot of distrib_11 busy on {b[0].get_id()}, waiting"
            assert "migration" not in fresh.function_params
            assert "migration_started" not in fresh.function_params

        start.side_effect = None
        assert drive(RUNNERS[function_name].SPEC, task).status == DONE
        assert start.call_count == 3
        assert _started(rpcs, b[0], function_name) == ["distrib_11"] * 3

    def test_a_busy_slot_without_sync_replication_is_a_failed_attempt(self, db, rpcs):
        cluster, a, b = _non_sync(db)
        _leads(rpcs, a[0], "LVS_1")
        _data_plane(rpcs, (*a, *b))
        _queue(db, cluster, JobSchedule.FN_DEV_MIG, a[1])
        task = [t for t in _open(db, cluster, JobSchedule.FN_DEV_MIG) if t.node_id == a[0].get_id()][0]
        rpcs[a[0].get_id()].distr_migration_expansion_start.side_effect = _busy()

        fresh = drive(tasks_runner_migration.SPEC, task)
        assert (fresh.status, fresh.retry) == (SUSPENDED, 1)
        assert fresh.function_result == "Failed to start device migration task, retry later"
        assert "migration" not in fresh.function_params

    @pytest.mark.parametrize("answer", list(OTHER_START_FAILURES))
    @pytest.mark.parametrize("function_name", [JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG])
    def test_any_other_start_failure_is_a_failed_attempt(self, db, rpcs, function_name, answer):
        cluster, a, b, owner_a, _ = _switched(db, rpcs, function_name)
        task = _open(db, cluster, function_name, owner_a)[0]
        OTHER_START_FAILURES[answer](getattr(rpcs[b[0].get_id()], START_RPC[function_name]))

        fresh = drive(RUNNERS[function_name].SPEC, task)
        assert (fresh.status, fresh.retry) == (SUSPENDED, 1)
        assert fresh.function_result == "Failed to start device migration task, retry later"
        assert "migration" not in fresh.function_params
