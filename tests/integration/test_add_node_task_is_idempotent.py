"""``tasks_controller.add_node_add_task`` answers a repeat with the task it has.

The dedup below it exists for a real incident: without it a retried HTTP
request creates a second independent FN_NODE_ADD for the same host, both get
dispatched, and two threads race the same host's config-slot logic (2026-07-23,
six nodes created for a four-slot host). It is correct and it stays.

What it reported was the problem. `_add_task` answered the duplicate with
False, and the v2 endpoint turns False into `ValueError('Failed to create
add-node task')`, so the guard working as designed reached the caller as a 500.
The operator re-posts an add whenever the task window it polls comes back empty
-- which it does whenever the control plane cannot be read -- so every one of
those retries was answered with a server error, on a host whose add was already
queued and progressing (live 2026-09-20).

An add-node task that already exists for a host is the answer to "add this
host", not a failure to produce one. ``ensure_node_restart_task`` in the same
module already answers its repeat that way.

The dedup check-then-create is itself a plain read-then-write, so a second race
sits behind the first one: two concurrent posts for the same host can both
pass `_validate_new_task_node_add` before either commits, producing two
FN_NODE_ADD tasks for one host. `add_node_add_task` now serializes
check-then-create per (cluster, node_addr) behind a DbLock, so this suite runs
against the real FoundationDB provisioned by ``tests/integration/conftest.py``
instead of mocking the DB layer -- `DbLock` itself talks to FDB, so a mocked
`db` module can no longer stand in for it.
"""

import threading

import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule

CLUSTER = "c1"
NODE_ADDR = "worker-5.storage-node-api.simplyblock.svc.cluster.local:5000"


@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return controller


@pytest.fixture(autouse=True)
def _clean_keyspace(db):
    db.kv_store.clear_range(b"\x00", b"\xff")
    yield


def _seed_task(db, node_addr, status=JobSchedule.STATUS_NEW, canceled=False,
               cluster_id=CLUSTER, uuid="6b8563cd-8237-4136-9b37-7b767c15bfb8"):
    task = JobSchedule()
    task.uuid = uuid
    task.cluster_id = cluster_id
    task.function_name = JobSchedule.FN_NODE_ADD
    task.function_params = {"node_addr": node_addr}
    task.status = status
    task.canceled = canceled
    task.write_to_db(db.kv_store)
    return task


class TestAddNodeTaskIsIdempotent:

    def test_a_repeat_answers_with_the_task_already_queued(self, db):
        existing = _seed_task(db, NODE_ADDR)

        result = tasks_controller.add_node_add_task(
            CLUSTER, {"node_addr": NODE_ADDR})

        assert result == existing.uuid, (
            "a host whose add is already queued answered falsy, "
            "which the v2 endpoint raises as a 500")
        matching = [t for t in db.get_job_tasks(CLUSTER)
                    if t.function_name == JobSchedule.FN_NODE_ADD]
        assert len(matching) == 1, "a second task was created for a host that already has one"

    def test_a_first_add_creates_one(self, db):
        result = tasks_controller.add_node_add_task(
            CLUSTER, {"node_addr": NODE_ADDR})

        assert result
        created = db.get_task_by_id(result)
        assert created.function_name == JobSchedule.FN_NODE_ADD
        assert created.function_params["node_addr"] == NODE_ADDR

    def test_another_host_is_not_mistaken_for_this_one(self, db):
        _seed_task(db, "worker-9:5000")

        result = tasks_controller.add_node_add_task(
            CLUSTER, {"node_addr": NODE_ADDR})

        assert result, "another host's queued add suppressed this host's"
        created = db.get_task_by_id(result)
        assert created.function_params["node_addr"] == NODE_ADDR

    def test_a_finished_add_does_not_suppress_a_new_one(self, db):
        _seed_task(db, NODE_ADDR, status=JobSchedule.STATUS_DONE)

        result = tasks_controller.add_node_add_task(
            CLUSTER, {"node_addr": NODE_ADDR})

        assert result, "a host whose previous add is done can be added again"
        created = db.get_task_by_id(result)
        assert created.status == JobSchedule.STATUS_NEW

    def test_concurrent_calls_for_same_host_create_only_one_task(self, db):
        # Reproduces the create-time race directly: several concurrent posts
        # for a host with no task yet all read "no existing task" before any
        # of them writes one, absent the DbLock serializing them. Assert
        # every caller is answered with the SAME task -- not an error, since
        # an add-node task that already exists is the correct answer to "add
        # this host", not a failure.
        results = []
        errors = []

        def worker():
            try:
                results.append(tasks_controller.add_node_add_task(
                    CLUSTER, {"node_addr": NODE_ADDR}))
            except Exception as e:  # noqa: BLE001 - captured for the assertion below
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"expected no errors, got: {errors}"
        assert len(results) == 5
        assert len(set(results)) == 1, f"expected every caller to get the same task, got: {results}"

        matching = [t for t in db.get_job_tasks(CLUSTER)
                    if t.function_name == JobSchedule.FN_NODE_ADD]
        assert len(matching) == 1, f"expected exactly 1 task, got {len(matching)}"
        assert matching[0].uuid == results[0]
