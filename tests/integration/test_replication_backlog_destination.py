"""A forward replication task for a volume with no destination ends; it is not
retried (see simplyblock_core/test/test_replication_backlog_destination.py for
the enqueue side and the history).

replication_backlog walks the clone ancestry across volumes, into the
target-side REP_* receiving volumes, which carry no replication node and
do_replicate=False. A task queued for one can never resolve a destination; the
runner retried them for ever, which starved replication and wedged every
volume delete behind their snapshots (lab 2026-08-19).

Real FDB: the task's volume is looked up again before it is dropped (a volume
replicating to a peer is placed by its first task), so the volume, snapshot and
task are seeded. The source node's chain walk is mocked.
"""
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr
from tests.integration import test_deferred_peer_placement as placement

db = placement.db
source_side = placement.source_side


def test_undeliverable_forward_task_is_ended_not_retried(db, source_side):
    cluster, _, _, (vol,) = placement._site_a(db)
    receiving = placement._with(db, vol, lvol_name=f"REP_{vol.lvol_name}", do_replicate=False,
                                replication_node_id="")
    task, snap = placement._task(db, cluster, receiving)

    sr.process_snap_replicate_start(task, snap)

    stored = db.get_task_by_id(task.uuid)
    assert stored.status == JobSchedule.STATUS_DONE, "the task must end, not retry"
    assert "no replication destination" in stored.function_result
    assert stored.retry == 0, "ending a doomed task must not consume the retry budget"
