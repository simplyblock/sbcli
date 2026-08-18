"""One replication task must never take the runner down with it.

Lab run 19: the SnapshotReplication service crash-looped. Two unhandled
exceptions escaped into main() — KeyError('remote_lvol_id') from the max-retry
branch of a task that never got as far as creating a receiving lvol, and an
RPCException from a node the test had deliberately taken offline. Each killed
the process, so replication stopped for EVERY volume, and no snapshot was ever
chained or pruned. The visible symptom was a fail-over target full of zeros —
five layers away from the actual cause.
"""
from typing import cast
import inspect

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr


class _Snap:
    def __init__(self):
        self.uuid = "SNAP1"
        self.status = "online"
        self.snap_bdev = "LVS_1/SNAP1"
        self.lvol = type("L", (), {"node_id": "N1", "nodes": ["N1"],
                                   "lvs_name": "LVS_1",
                                   "get_id": lambda self: "LV1"})()

    def get_id(self):
        return self.uuid

    def write_to_db(self, *a, **kw):
        pass


# Duck-typed stand-in for JobSchedule: task_runner only touches the handful
# of attributes set below, and building a real JobSchedule would pull in FDB.
# cast() at the call sites keeps that explicit for the type checker.
class _Task:
    def __init__(self, params, retry=99, max_retry=5):
        self.uuid = "T1"
        self.function_params = params
        self.retry = retry
        self.max_retry = max_retry
        self.canceled = False
        self.status = JobSchedule.STATUS_SUSPENDED
        self.function_result = ""
        self.cluster_id = "C1"

    def get_id(self):
        return self.uuid

    def write_to_db(self, *a, **kw):
        pass


def test_max_retry_without_a_receiving_lvol_does_not_raise(monkeypatch):
    """The crash: params never carried remote_lvol_id."""
    snap = _Snap()
    node = type("N", (), {"status": "online", "get_id": lambda self: "N1"})()

    class _DB:
        def get_snapshot_by_id(self, uuid):
            return snap

        def get_storage_node_by_id(self, uuid):
            return node

        kv_store = None

    monkeypatch.setattr(sr, "db", _DB())
    monkeypatch.setattr(sr, "_source_leader_node", lambda s: node)

    task = _Task({"snapshot_id": "SNAP1", "replicate_to_source": False})
    assert sr.task_runner(cast(JobSchedule, task)) is True          # must not raise KeyError
    assert task.status == JobSchedule.STATUS_DONE


def test_max_retry_with_a_vanished_receiving_lvol_does_not_raise(monkeypatch):
    snap = _Snap()
    node = type("N", (), {"status": "online", "get_id": lambda self: "N1"})()

    class _DB:
        def get_snapshot_by_id(self, uuid):
            return snap

        def get_storage_node_by_id(self, uuid):
            return node

        def get_lvol_by_id(self, uuid):
            raise KeyError(uuid)

        kv_store = None

    monkeypatch.setattr(sr, "db", _DB())
    monkeypatch.setattr(sr, "_source_leader_node", lambda s: node)

    task = _Task({"snapshot_id": "SNAP1", "replicate_to_source": False,
                  "remote_lvol_id": "GONE"})
    assert sr.task_runner(cast(JobSchedule, task)) is True


def test_runner_loop_catches_task_exceptions():
    """A failing task is logged and skipped, not fatal to the service."""
    src = inspect.getsource(sr.main)
    assert "try:" in src and "task_runner(task)" in src
    assert "except Exception" in src, (
        "an RPC error from one node would stop replication cluster-wide")
