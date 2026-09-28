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
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule

CLUSTER = "c1"
NODE_ADDR = "worker-5.storage-node-api.simplyblock.svc.cluster.local:5000"
EXISTING = "6b8563cd-8237-4136-9b37-7b767c15bfb8"


def _add_task(uuid, node_addr, status=JobSchedule.STATUS_NEW, canceled=False):
    task = MagicMock(spec=JobSchedule)
    task.uuid = uuid
    task.function_name = JobSchedule.FN_NODE_ADD
    task.function_params = {"node_addr": node_addr}
    task.status = status
    task.canceled = canceled
    task.get_id = MagicMock(return_value=f"{CLUSTER}/{uuid}")
    return task


class TestAddNodeTaskIsIdempotent(unittest.TestCase):

    def _call(self, existing_tasks):
        with patch.object(tasks_controller, "db") as db:
            db.get_job_tasks.return_value = existing_tasks
            with patch.object(tasks_controller, "_add_task") as created:
                created.return_value = "a-new-task"
                result = tasks_controller.add_node_add_task(
                    CLUSTER, {"node_addr": NODE_ADDR})
        return result, created

    def test_a_repeat_answers_with_the_task_already_queued(self):
        result, created = self._call([_add_task(EXISTING, NODE_ADDR)])

        self.assertEqual(result, EXISTING,
                         "a host whose add is already queued answered falsy, "
                         "which the v2 endpoint raises as a 500")
        self.assertFalse(created.called,
                         "a second task was created for a host that already has one")

    def test_a_first_add_creates_one(self):
        result, created = self._call([])

        self.assertEqual(result, "a-new-task")
        self.assertTrue(created.called)

    def test_another_host_is_not_mistaken_for_this_one(self):
        result, created = self._call([_add_task(EXISTING, "worker-9:5000")])

        self.assertEqual(result, "a-new-task",
                         "another host's queued add suppressed this host's")
        self.assertTrue(created.called)

    def test_a_finished_add_does_not_suppress_a_new_one(self):
        result, created = self._call(
            [_add_task(EXISTING, NODE_ADDR, status=JobSchedule.STATUS_DONE)])

        self.assertEqual(result, "a-new-task",
                         "a host whose previous add is done can be added again")
        self.assertTrue(created.called)


if __name__ == "__main__":
    unittest.main()
