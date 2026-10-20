"""What the flag of a running site activation holds, against the real
FoundationDB: the restart of any node, node add / removal, expansion, a full
activation, the migrations and the resync wait as under main's IN_ACTIVATION;
object operations and their task runners wait only on an LVS led from the
activating site; restart phases of a node of that site are honoured; an
operation on an LVS led from the other site reaches a node of the activating
site once its instance is rebuilt, and a group snapshot waits while one is
being rebuilt; the promote's disaster gate of that site fails.

3 + 3 nodes, site A recorded and being activated, four LVS shapes: a0 owns
LVS_1 (homed and led on A), b0 owns LVS_2 (homed on B, led from A), b1 owns
LVS_3 (homed and led on B), a1 owns LVS_4 (homed on A, led from B). A's nodes
are ONLINE without lvstores. The flag is set by the real
``begin_site_activation``; the window after the record is cleared (the flag
still set) is a second state. The step right after each gate raises
``_Passed``: an operation that reaches it got past the gate. Storage nodes are
mocked at the RPC boundary; the database never is.
"""
import types
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import cluster_ops
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import (
    consistency_group_controller,
    lvol_controller,
    snapshot_controller,
    tasks_controller,
)
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError
from simplyblock_core.services import (
    backup_merge_service,
    tasks_runner_backup,
    tasks_runner_batch_migration,
    tasks_runner_failed_migration,
    tasks_runner_jc_comp,
    tasks_runner_lvol_migration,
    tasks_runner_migration,
    tasks_runner_new_dev_migration,
    tasks_runner_node_add,
    tasks_runner_node_removal,
    tasks_runner_sync_lvol_del,
    tasks_runner_sync_resync,
)
from simplyblock_core.services import tasks_runner_sync_promote as promote
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack
from tests.integration import test_sync_replication_site_restart as site
from tests.integration.task_driver import drive, one_cycle

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
DOWN, DONE = Cluster.LOST_SITE_DOWN, Cluster.LOST_SITE_DONE
ONLINE = StorageNode.STATUS_ONLINE
HELD = f"site {SITE_A} is being activated; retry"
GIB = 1024 ** 3

#: The record still standing, and cleared with the flag still set (the window
#: between the record's clear and the end of the activation).
WINDOWS = ["record", "cleared"]

db = stack.db
rpcs = stack.rpcs
links = site.links
_fresh, _update = stack._fresh, stack._update
_update_cluster = site._update_cluster


class _Passed(BaseException):
    """Raised by the step right after a gate: the operation got past it."""


class _StopLoop(BaseException):
    """Ends a runner's main loop after one pass (its sleep)."""


@pytest.fixture(autouse=True)
def _clean_caches():
    ttl_cache.invalidate_all()
    yield
    ttl_cache.invalidate_all()


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _four(db, state=DOWN, **kwargs):
    """site._layout plus LVS_4 owned by a1 (home a1 a2 a0, remote b2 b0 b1,
    led from B), with what a volume needs on every node."""
    cluster, a, b = site._layout(db, state, **kwargs)
    stack._make_owner(db, a[1], [a[1], a[2], a[0]], [b[2], b[0], b[1]], lvs_id=4)
    _update(db, a[1], lvs_active_site=SITE_B, lvstore_status="")
    for node in (*a, *b):
        _update(db, node, data_nics=[publish._nic(node.mgmt_ip)], max_lvol=100)
    for node in (*a, *b):
        _update(db, node, lvstore_ports=ops._derive_lvstore_ports(_fresh(db, node), None, db))
    return (db.get_cluster_by_id(cluster.get_id()),
            [_fresh(db, n) for n in a], [_fresh(db, n) for n in b])


def _activating(db, window="record", state=DOWN):
    """_four with a site activation of A running; with ``window`` "cleared" the
    record is gone and the flag stays."""
    cluster, a, b = _four(db, state)
    assert src.begin_site_activation(cluster.get_id(), SITE_A)
    if window == "cleared":
        _update_cluster(db, cluster, lost_site="", lost_site_state="")
    return db.get_cluster_by_id(cluster.get_id()), a, b


def _not_activating(db, case):
    """_four where no site activation holds anything: "no flag" (A recorded,
    no activation), "ended" (one ran and ended) or "non-sync" (a cluster
    without sync replication with a stray flag)."""
    if case == "non-sync":
        cluster, a, b = _four(db, "", sync=False)
        _update_cluster(db, cluster, site_activation_site=SITE_A)
    else:
        cluster, a, b = _four(db, DOWN)
        if case == "ended":
            token = src.begin_site_activation(cluster.get_id(), SITE_A)
            src.end_site_activation(cluster.get_id(), token)
    return db.get_cluster_by_id(cluster.get_id()), a, b


def _owners(a, b):
    """LVS name -> owner."""
    return {"LVS_1": a[0], "LVS_2": b[0], "LVS_3": b[1], "LVS_4": a[1]}


LED_FROM_A, LED_FROM_B = ["LVS_1", "LVS_2"], ["LVS_3", "LVS_4"]


def _volume(db, cluster, owner):
    vol = publish._volume(db, cluster, owner, publish._pool(db, cluster),
                          active_site=owner.lvs_active_site or owner.site)
    vol.max_size = 10 * GIB
    vol.write_to_db(db.kv_store)
    return db.get_lvol_by_id(vol.get_id())


def _snapshot(db, cluster, owner):
    vol = _volume(db, cluster, owner)
    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.cluster_id = cluster.get_id()
    snap.pool_uuid = vol.pool_uuid
    snap.lvol = vol
    snap.snap_name = f"snap-{snap.uuid[:8]}"
    snap.status = SnapShot.STATUS_ONLINE
    snap.size = vol.size
    snap.snap_bdev = f"{owner.lvstore}/SNAP_{snap.uuid[:8]}"
    snap.fabric = "tcp"
    snap.write_to_db(db.kv_store)
    return snap


def _task(db, cluster, function_name, node_id, params=None, **fields):
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = node_id
    task.function_name = function_name
    task.function_params = params or {}
    task.status = JobSchedule.STATUS_NEW
    task.max_retry = 10
    for key, value in fields.items():
        setattr(task, key, value)
    task.write_to_db(db.kv_store)
    return task


def _untouched(db, task):
    """``task`` as it was written: status, result, retry, canceled, and not
    claimed (owner, updated_at)."""
    fresh = db.get_task_by_id(task.uuid)
    assert (fresh.status, fresh.function_result, fresh.retry, fresh.canceled, fresh.owner, fresh.updated_at) == (
        task.status, task.function_result, task.retry, task.canceled, task.owner, task.updated_at)


def _no_rpc(rpcs):
    for node_id, rpc in rpcs.items():
        assert rpc.method_calls == [], node_id


def _one_pass(runner):
    """Run the own loop of ``runner.main()`` for one pass: its sleep ends the
    loop."""
    clock = types.SimpleNamespace(sleep=MagicMock(side_effect=_StopLoop), time=lambda: 0)
    with patch.object(runner, "time", clock), pytest.raises(_StopLoop):
        runner.main()


# ---------------------------------------------------------------------------
# object operations (lvol and snapshot controllers)
# ---------------------------------------------------------------------------

#: name -> (module, the step right after the gate, the operation on an owner).
OPS = {
    "lvol create": (lvol_controller, "check_lvstore_object_limit", lambda db, c, o: lvol_controller.add_lvol_ha(
        "vol-new", GIB, o.get_id(), "ha", publish._pool(db, c).get_id())),
    "lvol resize": (lvol_controller, "_resize_lvol_on_all_nodes", lambda db, c, o: lvol_controller.resize_lvol(
        _volume(db, c, o).get_id(), 2 * GIB)),
    "lvol delete": (lvol_controller, "_delete_lvol_from_all_nodes", lambda db, c, o: lvol_controller.delete_lvol(
        _volume(db, c, o))),
    "lvol clone": (snapshot_controller, "check_snapshot_capacity", lambda db, c, o: lvol_controller.clone_lvol(
        _volume(db, c, o).get_id(), "clone-new")),
    "snapshot create": (snapshot_controller, "check_snapshot_capacity", lambda db, c, o: snapshot_controller.add(
        _volume(db, c, o).get_id(), "snap-new")),
    "snapshot clone": (lvol_controller, "check_lvstore_object_limit", lambda db, c, o: snapshot_controller.clone(
        _snapshot(db, c, o).get_id(), "clone-new")),
    "snapshot delete": (snapshot_controller.migration_controller, "get_active_migration_for_lvol",
                        lambda db, c, o: snapshot_controller.delete(_snapshot(db, c, o).get_id())),
}


def _attempt(db, cluster, owner, op):
    """Run ``op`` on ``owner``'s LVS: (passed the gate, what it answered or
    logged)."""
    module, step, run = OPS[op]
    said = []
    with patch.object(module, step, side_effect=_Passed), \
            patch.object(lvol_controller.logger, "error", side_effect=lambda m, *a: said.append(str(m))), \
            patch.object(snapshot_controller.logger, "error", side_effect=lambda m, *a: said.append(str(m))):
        try:
            said.append(str(run(db, cluster, owner)))
        except _Passed:
            return True, said
        except PreconditionError as e:
            said.append(str(e))
    return False, said


class TestObjectOperations:

    @pytest.mark.parametrize("op", OPS)
    @pytest.mark.parametrize("lvs", LED_FROM_A)
    @pytest.mark.parametrize("window", WINDOWS)
    def test_an_lvs_led_from_the_site_is_refused(self, db, rpcs, window, lvs, op):
        """Refused with the predicate's text before the step after the gate;
        no RPC, the volume as it was."""
        cluster, a, b = _activating(db, window)
        passed, said = _attempt(db, cluster, _owners(a, b)[lvs], op)
        assert passed is False
        assert any(HELD in s for s in said), said
        _no_rpc(rpcs)
        for vol in db.get_lvols(cluster.get_id()):
            assert vol.status == LVol.STATUS_ONLINE

    @pytest.mark.parametrize("op", OPS)
    @pytest.mark.parametrize("lvs", LED_FROM_B)
    @pytest.mark.parametrize("window", WINDOWS)
    def test_an_lvs_led_from_the_other_site_passes(self, db, rpcs, window, lvs, op):
        cluster, a, b = _activating(db, window)
        passed, said = _attempt(db, cluster, _owners(a, b)[lvs], op)
        assert passed is True, said

    @pytest.mark.parametrize("op", OPS)
    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_without_a_site_activation_each_passes(self, db, rpcs, case, op):
        """An LVS led from A goes on when no site activation runs."""
        cluster, a, b = _not_activating(db, case)
        for lvs in LED_FROM_A:
            passed, said = _attempt(db, cluster, _owners(a, b)[lvs], op)
            assert passed is True, (lvs, said)


# ---------------------------------------------------------------------------
# node-level operations
# ---------------------------------------------------------------------------

class TestNodeLevelOperations:

    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_full_activation_is_refused(self, db, window):
        cluster, a, b = _activating(db, window)
        with patch.object(cluster_ops, "set_cluster_status", side_effect=_Passed) as status:
            with pytest.raises(ValueError, match=HELD):
                cluster_ops._cluster_activate(cluster.get_id(), force=True)
        status.assert_not_called()
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_DEGRADED

    @pytest.mark.parametrize("window", WINDOWS)
    def test_an_expansion_is_refused(self, db, window):
        cluster, a, b = _activating(db, window)
        with patch.object(cluster_ops, "set_cluster_status", side_effect=_Passed) as status:
            with pytest.raises(ValueError, match=HELD):
                cluster_ops.cluster_expand(cluster.get_id())
        status.assert_not_called()
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_DEGRADED

    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_activation_and_expansion_without_a_flag(self, db, case):
        """Both reach their first status write."""
        cluster, a, b = _not_activating(db, case)
        with patch.object(cluster_ops, "set_cluster_status", side_effect=_Passed):
            with pytest.raises(_Passed):
                cluster_ops._cluster_activate(cluster.get_id(), force=True)
            with pytest.raises(_Passed):
                cluster_ops.cluster_expand(cluster.get_id())

    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_node_add_waits(self, db, window):
        """Not eligible: left untouched, as under IN_ACTIVATION."""
        cluster, a, b = _activating(db, window)
        task = _task(db, cluster, JobSchedule.FN_NODE_ADD, "", {"cluster_id": cluster.get_id()})
        with patch.object(tasks_runner_node_add.storage_node_ops, "add_node") as add:
            drive(tasks_runner_node_add.SPEC, task)
        add.assert_not_called()
        _untouched(db, task)

    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_node_removal_waits(self, db, window):
        cluster, a, b = _activating(db, window)
        task = _task(db, cluster, JobSchedule.FN_NODE_REMOVAL, b[2].get_id())
        with patch.object(tasks_runner_node_removal.storage_node_ops, "node_removal_orchestrate") as remove, \
                patch.object(tasks_runner_node_removal.storage_node_ops, "set_node_status") as status:
            fresh = drive(tasks_runner_node_removal.SPEC, task)
        remove.assert_not_called()
        status.assert_called_once_with(b[2].get_id(), StorageNode.STATUS_PENDING_REMOVAL, caused_by="remove")
        assert (fresh.status, fresh.function_result, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, HELD, 0)

    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_node_add_and_removal_run_without_a_flag(self, db, case):
        cluster, a, b = _not_activating(db, case)
        add_task = _task(db, cluster, JobSchedule.FN_NODE_ADD, "", {"cluster_id": cluster.get_id()})
        removal = _task(db, cluster, JobSchedule.FN_NODE_REMOVAL, b[2].get_id())
        with patch.object(tasks_runner_node_add.storage_node_ops, "add_node", return_value=True) as add, \
                patch.object(tasks_runner_node_removal.storage_node_ops, "node_removal_orchestrate",
                             return_value=True) as remove:
            added = drive(tasks_runner_node_add.SPEC, add_task)
            removed = drive(tasks_runner_node_removal.SPEC, removal)
        add.assert_called_once()
        remove.assert_called_once()
        assert (added.status, removed.status) == (JobSchedule.STATUS_DONE, JobSchedule.STATUS_DONE)


# ---------------------------------------------------------------------------
# migrations and the resync
# ---------------------------------------------------------------------------

DEVICE_MIGRATIONS = {
    "device": (tasks_runner_migration, JobSchedule.FN_DEV_MIG),
    "new device": (tasks_runner_new_dev_migration, JobSchedule.FN_NEW_DEV_MIG),
    "failed device": (tasks_runner_failed_migration, JobSchedule.FN_FAILED_DEV_MIG),
}


class TestMigrations:

    @pytest.mark.parametrize("kind", DEVICE_MIGRATIONS)
    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_device_migration_waits(self, db, rpcs, window, kind):
        """Cluster-wide: a task on b1 (serving site) is held before the step
        after the gate - suspended with no retry counted, so it resumes on the
        first pass after the activation."""
        runner, function_name = DEVICE_MIGRATIONS[kind]
        cluster, a, b = _activating(db, window)
        task = _task(db, cluster, function_name, b[1].get_id())
        with patch.object(tasks_controller, "get_active_cluster_expand_task", side_effect=_Passed):
            fresh = drive(runner.SPEC, task)
        assert (fresh.status, fresh.function_result, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, HELD, 0)
        _no_rpc(rpcs)

    @pytest.mark.parametrize("kind", DEVICE_MIGRATIONS)
    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_a_device_migration_runs_without_a_flag(self, db, rpcs, case, kind):
        runner, function_name = DEVICE_MIGRATIONS[kind]
        cluster, a, b = _not_activating(db, case)
        task = _task(db, cluster, function_name, b[1].get_id())
        with patch.object(tasks_controller, "get_active_cluster_expand_task", side_effect=_Passed):
            with pytest.raises(_Passed):
                drive(runner.SPEC, task)

    def test_a_volume_migration_ends_unsupported(self, db):
        """A volume migration never runs on a sync-replication cluster, the
        flag set or not - it ends "not supported" before any status check."""
        cluster, a, b = _activating(db)
        migration = LVolMigration()
        migration.uuid = str(uuid.uuid4())
        migration.cluster_id = cluster.get_id()
        migration.status = LVolMigration.STATUS_RUNNING
        migration.write_to_db(db.kv_store)
        task = _task(db, cluster, JobSchedule.FN_LVOL_MIG, b[1].get_id(), {"migration_id": migration.uuid})
        assert tasks_runner_lvol_migration.task_runner(task) is True
        fresh = db.get_task_by_id(task.uuid)
        assert fresh.status == JobSchedule.STATUS_DONE
        assert "not supported on a sync-replication cluster" in fresh.function_result

    def test_a_batch_migration_ends_unsupported(self, db):
        cluster, a, b = _activating(db)
        group = LVolMigrationGroup()
        group.uuid = str(uuid.uuid4())
        group.cluster_id = cluster.get_id()
        group.source_node_id, group.target_node_id = b[1].get_id(), b[2].get_id()
        group.status = LVolMigrationGroup.STATUS_RUNNING
        group.write_to_db(db.kv_store)
        task = _task(db, cluster, JobSchedule.FN_LVOL_BATCH_MIG, b[1].get_id(), {"group_id": group.uuid})
        assert tasks_runner_batch_migration.task_runner(task) is True
        fresh = db.get_task_by_id(task.uuid)
        assert fresh.status == JobSchedule.STATUS_DONE
        assert "not supported on a sync-replication cluster" in fresh.function_result


class TestResync:

    def _resync(self, db, cluster, b):
        task_id = tasks_controller.add_sync_resync_task(cluster.get_id(), b[1].get_id(), "LVS_3")
        assert task_id
        return db.get_task_by_id(task_id)

    def test_the_resync_waits_after_the_record(self, db, rpcs):
        """The record cleared, the flag still set: the catch-up of LVS_3 (led
        from B) waits for the activation to end, nothing started."""
        cluster, a, b = _activating(db, "cleared")
        task = self._resync(db, cluster, b)
        fresh = drive(tasks_runner_sync_resync.SPEC, task)
        assert (fresh.status, fresh.function_result, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, HELD, 0)
        _no_rpc(rpcs)

    def test_the_resync_waits_on_the_record_first(self, db):
        cluster, a, b = _activating(db, "record")
        task = self._resync(db, cluster, b)
        fresh = drive(tasks_runner_sync_resync.SPEC, task)
        assert (fresh.status, fresh.function_result, fresh.retry) == (
            JobSchedule.STATUS_SUSPENDED, f"site {SITE_A} is lost, waiting for its return", 0)

    def test_the_resync_goes_on_after_the_flag(self, db):
        """The flag ended: the runner goes on to its next check (here the
        leadership of LVS_3 is moving)."""
        cluster, a, b = _activating(db, "cleared")
        task = self._resync(db, cluster, b)
        src.clear_site_activation(cluster.get_id())
        _update(db, b[1], lvs_active_site=ops.lvs_moving_value(SITE_A))
        fresh = drive(tasks_runner_sync_resync.SPEC, task)
        assert (fresh.status, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert "is moving" in fresh.function_result


# ---------------------------------------------------------------------------
# the task runners of object operations
# ---------------------------------------------------------------------------

class TestBackupRunner:

    @pytest.mark.parametrize("lvs", LED_FROM_A)
    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_backup_on_an_lvs_led_from_a_waits(self, db, window, lvs):
        cluster, a, b = _activating(db, window)
        owner = _owners(a, b)[lvs]
        tasks = [_task(db, cluster, JobSchedule.FN_BACKUP, owner.get_id()),
                 _task(db, cluster, JobSchedule.FN_BACKUP, owner.get_id(), canceled=True)]
        run = MagicMock()
        with patch.dict(tasks_runner_backup._HANDLERS, {JobSchedule.FN_BACKUP: run}):
            for task in tasks:
                drive(tasks_runner_backup.SPEC, task)
        run.assert_not_called()
        for task in tasks:
            _untouched(db, task)

    @pytest.mark.parametrize("lvs", LED_FROM_B)
    def test_a_backup_on_an_lvs_led_from_b_runs(self, db, lvs):
        cluster, a, b = _activating(db)
        task = _task(db, cluster, JobSchedule.FN_BACKUP, _owners(a, b)[lvs].get_id())
        run = MagicMock()
        with patch.dict(tasks_runner_backup._HANDLERS, {JobSchedule.FN_BACKUP: run}):
            drive(tasks_runner_backup.SPEC, task)
        run.assert_called_once()

    @pytest.mark.parametrize("case", ["no flag", "non-sync"])
    def test_a_backup_runs_without_a_flag(self, db, case):
        cluster, a, b = _not_activating(db, case)
        task = _task(db, cluster, JobSchedule.FN_BACKUP, a[0].get_id())
        run = MagicMock()
        with patch.dict(tasks_runner_backup._HANDLERS, {JobSchedule.FN_BACKUP: run}):
            drive(tasks_runner_backup.SPEC, task)
        run.assert_called_once()


class TestBackupMerge:

    def _evaluated(self, db, cluster, a, b, removed=()):
        vols = {lvs: _volume(db, cluster, owner).get_id() for lvs, owner in _owners(a, b).items()}
        for node in removed:
            _fresh(db, node).remove(db.kv_store)
        with patch.object(backup_merge_service.backup_policy, "evaluate_policy") as policy, \
                patch.object(backup_merge_service.backup_policy, "evaluate_schedule") as schedule:
            _one_pass(backup_merge_service)
        by_id = {v: k for k, v in vols.items()}
        evaluated = sorted(by_id[c.args[0].get_id()] for c in policy.call_args_list)
        assert evaluated == sorted(by_id[c.args[0].get_id()] for c in schedule.call_args_list)
        return evaluated

    @pytest.mark.parametrize("window", WINDOWS)
    def test_volumes_led_from_a_are_left_for_later(self, db, window):
        cluster, a, b = _activating(db, window)
        assert self._evaluated(db, cluster, a, b) == LED_FROM_B

    @pytest.mark.parametrize("case", ["no flag", "non-sync"])
    def test_every_volume_is_evaluated_without_a_flag(self, db, case):
        cluster, a, b = _not_activating(db, case)
        assert self._evaluated(db, cluster, a, b) == sorted(LED_FROM_A + LED_FROM_B)

    def test_a_volume_without_its_owner_waits(self, db):
        """b1's record is gone: its volume (LVS_3) is held like a node-level
        operation; a1's (LVS_4) is evaluated."""
        cluster, a, b = _activating(db)
        evaluated = self._evaluated(db, cluster, a, b, removed=[b[1]])
        assert evaluated == ["LVS_4"]

    def test_no_volume_is_evaluated_without_nodes(self, db):
        """Every node record is gone, the pool and the volumes stay: nothing
        is evaluated while the flag is set."""
        cluster, a, b = _activating(db)
        assert self._evaluated(db, cluster, a, b, removed=[*a, *b]) == []


class TestJcCompaction:
    """The JM group a resume works on is ``function_params["jm_vuid"]`` (else
    the JM of its node); its LVS is the owner's with that jm_vuid, whatever
    node the RPC goes to."""

    def _resume(self, db, cluster, node, jm_vuid=None, **fields):
        params = {} if jm_vuid is None else {"jm_vuid": jm_vuid}
        return _task(db, cluster, JobSchedule.FN_JC_COMP_RESUME, node.get_id(), params, **fields)

    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_resume_of_a_group_led_from_a_waits(self, db, rpcs, window):
        """b1 (its own LVS_3 led from B) resuming b0's JM group (LVS_2, led
        from A); a0's own group without a jm_vuid key; and a canceled, a
        retry-exhausted and an upgrade-held one - all untouched, no RPC."""
        cluster, a, b = _activating(db, window)
        tasks = [self._resume(db, cluster, b[1], 2),
                 self._resume(db, cluster, a[0]),
                 self._resume(db, cluster, b[2], 1, canceled=True),
                 self._resume(db, cluster, a[2], 2, retry=10),
                 self._resume(db, cluster, b[0], 2)]
        with patch.object(tasks_runner_jc_comp.jc_compression_upgrade, "resume_is_held", return_value=True):
            one_cycle(tasks_runner_jc_comp.SPEC)
        for task in tasks:
            _untouched(db, task)
        _no_rpc(rpcs)

    def test_a_resume_of_a_group_led_from_b_runs(self, db, rpcs):
        """b0 (its own LVS_2 led from A) resuming b1's group (LVS_3), b2 the
        group of a1 (LVS_4, led from B) and b1 its own without a key."""
        cluster, a, b = _activating(db)
        tasks = [self._resume(db, cluster, b[0], 3), self._resume(db, cluster, b[2], 4),
                 self._resume(db, cluster, b[1])]
        one_cycle(tasks_runner_jc_comp.SPEC)
        for task in tasks:
            assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE
        rpcs[b[0].get_id()].jc_suspend_compression.assert_called_once_with(jm_vuid=3, suspend=False)
        rpcs[b[2].get_id()].jc_suspend_compression.assert_called_once_with(jm_vuid=4, suspend=False)
        rpcs[b[1].get_id()].jc_suspend_compression.assert_called_once_with(jm_vuid=3, suspend=False)

    @pytest.mark.parametrize("case", ["no flag", "non-sync"])
    def test_a_resume_runs_without_a_flag(self, db, rpcs, case):
        cluster, a, b = _not_activating(db, case)
        task = self._resume(db, cluster, b[1], 2)
        one_cycle(tasks_runner_jc_comp.SPEC)
        assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE
        rpcs[b[1].get_id()].jc_suspend_compression.assert_called_once_with(jm_vuid=2, suspend=False)


class TestSyncTasks:
    """The sync-delete and sync-op tasks of a volume, on a node of B. The LVS
    of a sync delete is the one its bdev belongs to; ``primary_node`` is the
    leader that acted on the delete, which may own another LVS."""

    def _delete(self, db, cluster, owner, dest, primary=None, **fields):
        return _task(db, cluster, JobSchedule.FN_LVOL_SYNC_DEL, dest.get_id(),
                     {"lvol_bdev_name": f"{owner.lvstore}/LVOL_x",
                      "primary_node": (primary or owner).get_id()}, **fields)

    def _queued_delete(self, db, rpcs, cluster, owner, dest, primary):
        """The task the real producer queues when the sync delete of a bdev of
        ``owner``'s LVS fails on ``dest``, acted on by ``primary``."""
        rpcs[dest.get_id()].delete_lvol.side_effect = RPCRemoteError(message="busy", code=-16)
        bdev = f"{owner.lvstore}/LVOL_q"
        assert snapshot_controller.sync_delete_on_peer(dest, bdev, primary.get_id()) is False
        rpcs[dest.get_id()].reset_mock()
        rpcs[dest.get_id()].delete_lvol.side_effect = None
        [task] = [t for t in db.get_job_tasks(cluster.get_id())
                  if t.function_name == JobSchedule.FN_LVOL_SYNC_DEL and t.function_params["lvol_bdev_name"] == bdev]
        assert task.function_params["primary_node"] == primary.get_id()
        return task

    def _op(self, db, cluster, owner, dest, op):
        vol = _volume(db, cluster, owner)
        assert dest.get_id() in vol.nodes
        return _task(db, cluster, JobSchedule.FN_LVOL_SYNC_OP, dest.get_id(),
                     {"lvol_id": vol.get_id(), "op": op, "secondary_index": 1})

    @pytest.mark.parametrize("window", WINDOWS)
    def test_tasks_on_an_lvs_led_from_a_wait(self, db, rpcs, window):
        """LVS_2 (b0, led from A): a delete on b2, a canceled delete (the
        owner's delete lock kept), a delete acted on by a1 (its own LVS_4 led
        from B) as the producer queues it, a register and a resize on b1."""
        cluster, a, b = _activating(db, window)
        b[0].lvol_del_sync_lock()
        tasks = [self._delete(db, cluster, b[0], b[2]),
                 self._delete(db, cluster, b[0], b[2], canceled=True),
                 self._queued_delete(db, rpcs, cluster, b[0], b[2], primary=a[1]),
                 self._op(db, cluster, b[0], b[1], "register"),
                 self._op(db, cluster, b[0], b[1], "resize")]
        with patch.object(ops, "repair_lvol_registration_on_non_leader") as register:
            one_cycle(tasks_runner_sync_lvol_del.SPEC)
        register.assert_not_called()
        for task in tasks:
            _untouched(db, task)
        assert db.get_lvol_del_lock(b[0].get_id())
        _no_rpc(rpcs)

    @pytest.mark.parametrize("window", WINDOWS)
    def test_tasks_on_an_lvs_led_from_b_run(self, db, rpcs, window):
        """LVS_3 (b1) and LVS_4 (a1, led from B): a delete, a register and a
        resize on b2 each run; so does a delete of LVS_4 acted on by b0 (its
        own LVS_2 led from A)."""
        cluster, a, b = _activating(db, window)
        rpcs[b[2].get_id()].bdev_lvol_resize.return_value = True
        queued = self._queued_delete(db, rpcs, cluster, a[1], b[2], primary=b[0])
        rpcs[b[2].get_id()].delete_lvol.return_value = True
        tasks = [self._delete(db, cluster, b[1], b[2]), self._delete(db, cluster, a[1], b[2]), queued,
                 self._op(db, cluster, b[1], b[2], "register"), self._op(db, cluster, a[1], b[2], "register"),
                 self._op(db, cluster, b[1], b[2], "resize")]
        with patch.object(ops, "repair_lvol_registration_on_non_leader", return_value=(True, None)) as register:
            one_cycle(tasks_runner_sync_lvol_del.SPEC)
        assert register.call_count == 2
        for task in tasks:
            assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE, task.function_name
        assert rpcs[b[2].get_id()].delete_lvol.call_count == 3
        rpcs[b[2].get_id()].bdev_lvol_resize.assert_called_once()

    def test_a_delete_of_an_unowned_lvs_waits(self, db, rpcs):
        """No node carries LVS_9: its delete is held like a node-level
        operation, whatever LVS its acting leader b1 owns."""
        cluster, a, b = _activating(db)
        task = _task(db, cluster, JobSchedule.FN_LVOL_SYNC_DEL, b[2].get_id(),
                     {"lvol_bdev_name": "LVS_9/LVOL_x", "primary_node": b[1].get_id()})
        one_cycle(tasks_runner_sync_lvol_del.SPEC)
        _untouched(db, task)
        _no_rpc(rpcs)

    @pytest.mark.parametrize("case", ["no flag", "non-sync"])
    def test_sync_tasks_run_without_a_flag(self, db, rpcs, case):
        cluster, a, b = _not_activating(db, case)
        rpcs[b[2].get_id()].delete_lvol.return_value = True
        tasks = [self._delete(db, cluster, b[0], b[2]), self._op(db, cluster, b[0], b[1], "register")]
        with patch.object(ops, "repair_lvol_registration_on_non_leader", return_value=(True, None)) as register:
            one_cycle(tasks_runner_sync_lvol_del.SPEC)
        register.assert_called_once()
        for task in tasks:
            assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE, task.function_name


# ---------------------------------------------------------------------------
# restart phases of a node of the activating site
# ---------------------------------------------------------------------------

PHASE = StorageNode.RESTART_PHASE_PRE_BLOCK


class TestRestartPhase:

    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_phase_on_the_site_is_honoured(self, db, window):
        """a1 is ONLINE under a DEGRADED cluster; its phase of LVS_2 belongs to
        the activation's rebuild of it."""
        cluster, a, b = _activating(db, window)
        _update(db, a[1], restart_phases={"LVS_2": PHASE})
        assert ops.get_restart_phase(a[1].get_id(), "LVS_2") == PHASE
        assert _fresh(db, a[1]).restart_phases == {"LVS_2": PHASE}

    def test_a_phase_on_the_other_site_is_cleared(self, db):
        cluster, a, b = _activating(db)
        _update(db, b[1], restart_phases={"LVS_2": PHASE})
        assert ops.get_restart_phase(b[1].get_id(), "LVS_2") == ""
        assert not _fresh(db, b[1]).restart_phases

    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_a_phase_is_cleared_without_a_flag(self, db, case):
        cluster, a, b = _not_activating(db, case)
        _update(db, a[1], restart_phases={"LVS_2": PHASE})
        assert ops.get_restart_phase(a[1].get_id(), "LVS_2") == ""
        assert not _fresh(db, a[1]).restart_phases

    @pytest.mark.parametrize("window", WINDOWS)
    def test_an_operation_queues_on_a_phase(self, db, rpcs, links, window):
        """check_non_leader_for_operation on a1 in a phase of LVS_2: "queue",
        before the absence of a node of the recorded site is judged."""
        cluster, a, b = _activating(db, window)
        _update(db, a[1], restart_phases={"LVS_2": PHASE})
        assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_2") == "queue"

    def test_an_operation_skips_a_node_not_rebuilt(self, db, rpcs, links):
        """a1 with no phase and no lvstore LVS_4 in its SPDK: absent, skipped."""
        cluster, a, b = _activating(db, "record")
        rpcs[a[1].get_id()].get_lvstore.return_value = None
        assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_4") == "skip"
        rpcs[a[1].get_id()].get_lvstore.assert_called_with("LVS_4")
        links.votes.assert_not_called()

    def test_an_operation_skips_a_node_whose_lvstore_cannot_be_told(self, db, rpcs, links):
        """a1's SPDK does not answer the lvstore probe: not proof that it holds
        LVS_4, skipped."""
        cluster, a, b = _activating(db, "record")
        rpcs[a[1].get_id()].get_lvstore.side_effect = RPCRemoteError(message="busy", code=-16)
        assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_4") == "skip"
        links.votes.assert_not_called()

    def test_an_operation_reaches_a_rebuilt_node(self, db, rpcs, links):
        """a1 with no phase whose SPDK holds LVS_4 (rebuilt by the site
        activation) takes the operation while the record stands."""
        cluster, a, b = _activating(db, "record")
        rpcs[a[1].get_id()].get_lvstore.return_value = {"name": "LVS_4", "lvs leadership": False}
        assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_4") == "proceed"
        rpcs[a[1].get_id()].get_lvstore.assert_called_with("LVS_4")
        links.votes.assert_not_called()

    def test_a_rebuilt_node_is_skipped_without_a_flag(self, db, rpcs, links):
        """With the record and no flag a1 is absent whatever its SPDK holds:
        skipped, its SPDK never asked."""
        cluster, a, b = _not_activating(db, "no flag")
        assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_4") == "skip"
        rpcs[a[1].get_id()].get_lvstore.assert_not_called()

    def test_after_the_record_the_node_is_asked(self, db, rpcs, links):
        """The record cleared, no phase: a1 counts as connected (the vote) and
        its RPC decides."""
        cluster, a, b = _activating(db, "cleared")
        with patch.object(ops, "_is_node_rpc_responsive", return_value=True) as asked:
            assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_2") == "proceed"
        asked.assert_called_once()
        assert links.votes.called

    def test_without_a_flag_absence_comes_first(self, db, rpcs, links):
        """With the record and no flag, a1 in a phase is skipped as absent -
        its phase is never read."""
        cluster, a, b = _not_activating(db, "no flag")
        _update(db, a[1], restart_phases={"LVS_2": PHASE})
        with patch.object(ops, "get_restart_phase", wraps=ops.get_restart_phase) as phase:
            assert ops.check_non_leader_for_operation(a[1].get_id(), "LVS_2") == "skip"
        phase.assert_not_called()


# ---------------------------------------------------------------------------
# snapshots on an LVS led from B with members on the activating site
# ---------------------------------------------------------------------------

def _snapshot_bdev(name):
    """bdev_get of the leader: the snapshot it was asked for exists."""
    if "/SNAP_" not in name:
        return None
    return {"name": name, "uuid": str(uuid.uuid4()),
            "driver_specific": {"lvol": {"blobid": 7, "num_allocated_clusters": 1}}}


def _leader(rpcs, node):
    """``node`` leads: its snapshot RPCs succeed and read back."""
    rpc = rpcs[node.get_id()]
    rpc.lvol_create_snapshot2.return_value = True
    rpc.bdev_lvol_snapshot_group.return_value = True
    rpc.bdev_get.side_effect = _snapshot_bdev
    return rpc


def _group(db, cluster, owner):
    """A consistency group pinned to ``owner``'s LVS with one volume."""
    vol = _volume(db, cluster, owner)
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = f"cg-{group.uuid[:8]}"
    group.node_id = owner.get_id()
    group.lvs_name = owner.lvstore
    group.members = {vol.get_id(): {"joined_seq": 1, "removed_seq": 0}}
    group.write_to_db(db.kv_store)
    vol.group_id = group.get_id()
    vol.write_to_db(db.kv_store)
    return db.get_consistency_group_by_id(group.get_id())


def _registered(rpcs, nodes):
    """Node ids among ``nodes`` that got bdev_lvol_snapshot_register."""
    return {n.get_id() for n in nodes if rpcs[n.get_id()].bdev_lvol_snapshot_register.called}


class TestRebuiltMembers:
    """LVS_4 (owner a1, home a1 a2 a0, remote b2 b0 b1) is led from B by b2.
    a1 and a0 hold their rebuilt instances, a2 none yet."""

    @staticmethod
    def _rebuilt(rpcs, a):
        rpcs[a[2].get_id()].get_lvstore.return_value = None

    def test_a_snapshot_registers_on_rebuilt_members(self, db, rpcs, links):
        cluster, a, b = _activating(db, "record")
        self._rebuilt(rpcs, a)
        _leader(rpcs, b[2])
        vol = _volume(db, cluster, a[1])
        with patch.object(snapshot_controller, "_find_lvs_leader", return_value=_fresh(db, b[2])):
            ok, said = snapshot_controller.add(vol.get_id(), "snap-new")
        assert ok, said
        assert _registered(rpcs, (*a, b[0], b[1])) == {a[1].get_id(), a[0].get_id(), b[0].get_id(),
                                                      b[1].get_id()}

    @pytest.mark.parametrize("lvs", LED_FROM_A)
    @pytest.mark.parametrize("window", WINDOWS)
    def test_a_group_led_from_the_site_is_refused(self, db, rpcs, window, lvs):
        """Refused with the predicate's text before the leader lookup; no RPC."""
        cluster, a, b = _activating(db, window)
        group = _group(db, cluster, _owners(a, b)[lvs])
        with patch.object(consistency_group_controller, "_find_lvs_leader") as leader:
            ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
        assert ids is None and HELD in err
        leader.assert_not_called()
        _no_rpc(rpcs)

    @pytest.mark.parametrize("case", ["no flag", "ended", "non-sync"])
    def test_a_group_passes_the_gate_without_a_flag(self, db, rpcs, case):
        """A group on an LVS led from A reaches the leader lookup."""
        cluster, a, b = _not_activating(db, case)
        group = _group(db, cluster, a[0])
        with patch.object(consistency_group_controller, "_find_lvs_leader", return_value=None) as leader:
            ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
        assert ids is None and "No leader available" in err
        leader.assert_called_once()

    def test_a_group_registers_on_rebuilt_members(self, db, rpcs, links):
        cluster, a, b = _activating(db, "record")
        self._rebuilt(rpcs, a)
        leader = _leader(rpcs, b[2])
        group = _group(db, cluster, a[1])
        with patch.object(consistency_group_controller, "_find_lvs_leader", return_value=_fresh(db, b[2])):
            ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
        assert err is None and len(ids) == 1
        leader.bdev_lvol_snapshot_group.assert_called_once()
        assert _registered(rpcs, a) == {a[1].get_id(), a[0].get_id()}

    def test_a_group_waits_for_a_member_in_rebuild(self, db, rpcs, links):
        """a0 in a restart phase of LVS_4: the generation is refused before the
        group RPC, nothing is deferred to the restart drain."""
        cluster, a, b = _activating(db, "record")
        self._rebuilt(rpcs, a)
        leader = _leader(rpcs, b[2])
        _update(db, a[0], restart_phases={"LVS_4": PHASE})
        group = _group(db, cluster, a[1])
        with patch.object(consistency_group_controller, "_find_lvs_leader", return_value=_fresh(db, b[2])), \
                patch.object(ops, "queue_for_restart_drain") as drain:
            ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
        assert ids is None and "is being rebuilt; retry" in err and a[0].get_id()[:8] in err
        leader.bdev_lvol_snapshot_group.assert_not_called()
        drain.assert_not_called()
        assert not _registered(rpcs, a)
        assert not db.get_snapshots(cluster.get_id())

    def test_a_group_skips_the_recorded_site(self, db, rpcs, links):
        """With the record and no flag A's members hold no instance: the
        generation is taken on the leader alone, A's SPDK never asked."""
        cluster, a, b = _not_activating(db, "no flag")
        leader = _leader(rpcs, b[2])
        group = _group(db, cluster, a[1])
        with patch.object(consistency_group_controller, "_find_lvs_leader", return_value=_fresh(db, b[2])):
            ids, err = consistency_group_controller.create_group_snapshot_for_group(group)
        assert err is None and len(ids) == 1
        leader.bdev_lvol_snapshot_group.assert_called_once()
        for node in a:
            assert rpcs[node.get_id()].method_calls == [], node.get_id()


# ---------------------------------------------------------------------------
# the promote's disaster gate
# ---------------------------------------------------------------------------

#: The checkpoint -> the step right before it, which begins the activation.
CHECKPOINTS = {
    "before the site steps": None,
    "before the move": "_classify",
    "before the transfer of LVS_2": "_mark_moving",
    "before opening the volumes": "_transfer",
}


class TestPromoteGate:

    def _task(self, db, cluster, b):
        return _task(db, cluster, JobSchedule.FN_SYNC_PROMOTE, "", {
            "site": SITE_B, "lost_site": SITE_A, "fence": True, "owners": {"LVS_2": b[0].get_id()},
            "lvol_ids": ["vol-1"]})

    @pytest.mark.parametrize("checkpoint", CHECKPOINTS)
    @pytest.mark.parametrize("state", [DOWN, DONE])
    def test_the_gate_fails_on_the_activating_site(self, db, state, checkpoint):
        """The site activation of A begins right before ``checkpoint``: the
        promote fails there, and the step after it never runs."""
        cluster, a, b = _four(db, state)
        begin_in = CHECKPOINTS[checkpoint]

        def _begin(*args, **kwargs):
            assert src.begin_site_activation(cluster.get_id(), SITE_A)

        steps = {name: MagicMock(name=name) for name in
                 ("_site_steps", "_classify", "_mark_moving", "checkpoint", "_lost_members_not_leading",
                  "_transfer", "_open")}
        steps["_classify"].return_value = (["LVS_2"], [])
        steps["checkpoint"].side_effect = lambda task: task
        if begin_in is None:
            _begin()
        else:
            steps[begin_in].side_effect = (
                lambda *a, **k: (_begin(), (["LVS_2"], []))[1]) if begin_in == "_classify" else _begin
        with patch.multiple(promote, **steps), patch.object(promote.sync_ctl, "check_disaster_gate"):
            with pytest.raises(promote._PromoteFailed, match=f"gate failed {checkpoint}: {HELD}"):
                promote._run(self._task(db, cluster, b))
        after = {"before the site steps": "_site_steps", "before the move": "_mark_moving",
                 "before the transfer of LVS_2": "_transfer", "before opening the volumes": "_open"}[checkpoint]
        steps[after].assert_not_called()

    @pytest.mark.parametrize("state", [DOWN, DONE])
    def test_the_gate_holds_without_a_flag(self, db, state):
        """No site activation: every step runs."""
        cluster, a, b = _four(db, state)
        steps = {name: MagicMock(name=name) for name in
                 ("_site_steps", "_classify", "_mark_moving", "checkpoint", "_lost_members_not_leading",
                  "_transfer", "_open")}
        steps["_classify"].return_value = (["LVS_2"], [])
        steps["checkpoint"].side_effect = lambda task: task
        with patch.multiple(promote, **steps), patch.object(promote.sync_ctl, "check_disaster_gate"):
            promote._run(self._task(db, cluster, b))
        steps["_open"].assert_called_once()
