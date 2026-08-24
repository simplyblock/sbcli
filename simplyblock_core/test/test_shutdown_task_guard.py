"""Only data-movement tasks may block a node shutdown.

The guard called get_active_node_tasks, which returns EVERY non-done task on
the node, and then reported them as "Migration task found". On a replicating
cluster a snapshot_replication task is in flight almost every minute, so
graceful shutdown was effectively impossible (lab run 18: "Migration task
found: 2" was one running replication task; the node never left `online`).

Replication and sync-delete work is durable and re-driven when the node
returns — surviving a node outage is precisely what it is built for. A
migration in progress is not.
"""
from simplyblock_core.models.job_schedule import JobSchedule


MIGRATION_FNS = {
    JobSchedule.FN_DEV_MIG,
    JobSchedule.FN_FAILED_DEV_MIG,
    JobSchedule.FN_NEW_DEV_MIG,
    JobSchedule.FN_LVOL_MIG,
    JobSchedule.FN_LVOL_BATCH_MIG,
}


class _Task:
    def __init__(self, fn):
        self.function_name = fn


def _blocking(tasks):
    """Mirror of the guard's filter in storage_node_ops.shutdown_storage_node."""
    return [t for t in tasks if t.function_name in MIGRATION_FNS]


def test_replication_task_does_not_block_shutdown():
    assert _blocking([_Task(JobSchedule.FN_SNAPSHOT_REPLICATION)]) == []


def test_sync_delete_task_does_not_block_shutdown():
    assert _blocking([_Task(JobSchedule.FN_LVOL_SYNC_DEL)]) == []


def test_device_migration_still_blocks():
    assert len(_blocking([_Task(JobSchedule.FN_DEV_MIG)])) == 1


def test_lvol_migration_still_blocks():
    assert len(_blocking([_Task(JobSchedule.FN_LVOL_MIG)])) == 1


def test_mixed_set_keeps_only_migrations():
    tasks = [_Task(JobSchedule.FN_SNAPSHOT_REPLICATION),
             _Task(JobSchedule.FN_LVOL_SYNC_DEL),
             _Task(JobSchedule.FN_NEW_DEV_MIG)]
    blocking = _blocking(tasks)
    assert [t.function_name for t in blocking] == [JobSchedule.FN_NEW_DEV_MIG]


def test_guard_uses_the_same_set_as_this_test():
    """Keep the production filter and this expectation from drifting apart.

    The filter lives in check_node_shutdown_preconditions, which
    shutdown_storage_node delegates to.
    """
    import inspect
    from simplyblock_core import storage_node_ops
    src = inspect.getsource(storage_node_ops.check_node_shutdown_preconditions)
    for fn_const in ("FN_DEV_MIG", "FN_FAILED_DEV_MIG", "FN_NEW_DEV_MIG",
                     "FN_LVOL_MIG", "FN_LVOL_BATCH_MIG"):
        assert fn_const in src, f"{fn_const} missing from the shutdown guard"
    assert "FN_SNAPSHOT_REPLICATION" not in src
    assert "check_node_shutdown_preconditions(" in inspect.getsource(
        storage_node_ops.shutdown_storage_node), "shutdown no longer uses the guard"
