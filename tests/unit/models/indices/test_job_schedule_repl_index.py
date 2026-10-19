"""The repl_snapshot_id index on JobSchedule.

A snapshot_replication task is indexed by the snapshot it ships, so a volume's
replication-status read finds its tasks through the index (one point/range read
per snapshot) instead of walking the whole never-pruned task table -- the scan
that made a single volume's status read ~30s on a cluster with a day of tasks
(6245), over the csi-addons status deadline (2026-10-03).

Regression: 2026-10-03-replication-status-per-task-snapshot-scan
"""
from simplyblock_core.models import indices
from simplyblock_core.models.job_schedule import JobSchedule


def _repl_index():
    return next(i for i in indices.indexes_of(JobSchedule)
                if i.name == 'repl_snapshot_id')


def _task(uuid, function_name, params):
    task = JobSchedule()
    task.uuid = uuid
    task.function_name = function_name
    task.function_params = params
    return task


def test_a_replication_task_is_keyed_by_its_snapshot_id():
    # JobSchedule.get_id() is composite (cluster/date/uuid), so the entity-id
    # segments at the tail are encoding detail -- assert the indexed VALUE is the
    # snapshot_id and the entry resolves back to this task.
    task = _task('task-1', JobSchedule.FN_SNAPSHOT_REPLICATION,
                 {'snapshot_id': 'snap-9'})
    task.cluster_id = 'c1'
    task.date = 123
    keys = _repl_index().keys(JobSchedule, task)
    assert len(keys) == 1
    (key,) = keys
    assert key.startswith(b'index/JobSchedule/repl_snapshot_id/snap-9/')
    assert key.endswith(b'task-1')


def test_a_non_replication_task_is_not_indexed():
    # replication_final carries lvol_id, not snapshot_id -- it must not land in
    # this index, or the snapshot lookup would return the wrong task kind.
    task = _task('task-2', JobSchedule.FN_REPLICATION_FINAL, {'lvol_id': 'lv-1'})
    assert _repl_index().keys(JobSchedule, task) == set()


def test_a_replication_task_without_a_snapshot_id_is_not_indexed():
    task = _task('task-3', JobSchedule.FN_SNAPSHOT_REPLICATION, {})
    assert _repl_index().keys(JobSchedule, task) == set()


def test_a_default_instance_yields_no_key():
    # The shape the backfill meets on a partially populated record: the
    # extractor must return nothing rather than raise.
    assert _repl_index().keys(JobSchedule, JobSchedule()) == set()
