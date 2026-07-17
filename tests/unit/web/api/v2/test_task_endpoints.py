# coding=utf-8
"""Unit tests for /api/v2/clusters/{id}/tasks endpoints."""

from simplyblock_core.models.job_schedule import JobSchedule

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, TASK_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/tasks'


class TestListTasks:

    def test_returns_tasks_of_cluster(self, client, db, task):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == TASK_ID
        assert body['cluster_id'] == CLUSTER_ID
        assert body['function_name'] == 'node_restart'
        db.get_job_tasks.assert_called_once_with(CLUSTER_ID, limit=0)

    def test_filters_device_migration_tasks(self, client, db, task):
        migration_task = factories.make_task(
            uuid='77777777-7777-7777-7777-777777777778',
            function_name=JobSchedule.FN_DEV_MIG,
        )
        db.get_job_tasks.return_value = [task, migration_task]

        response = client.get(f'{BASE}/')

        assert [entry['id'] for entry in response.json()] == [TASK_ID]


class TestGetTask:

    def test_returns_task(self, client, db, task):
        response = client.get(f'{BASE}/{TASK_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == TASK_ID
        db.get_task_by_id.assert_called_once_with(TASK_ID)

    def test_task_of_other_cluster_returns_404(self, client, db, task):
        task.cluster_id = '11111111-1111-1111-1111-111111111112'

        response = client.get(f'{BASE}/{TASK_ID}/')

        assert response.status_code == 404

    def test_unknown_task_returns_404(self, client, db, cluster):
        db.get_task_by_id.return_value = None

        response = client.get(f'{BASE}/{TASK_ID}/')

        assert response.status_code == 404


class TestWatchTasks:

    def test_list_dispatches_watch_tasks(self, client, task, tasks_controller, watch_stream):
        tasks_controller.watch_tasks.return_value = watch_stream([task])

        response = client.get(f'{BASE}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert TASK_ID in response.text
        tasks_controller.watch_tasks.assert_called_once_with(CLUSTER_ID)

    def test_detail_dispatches_watch_task(self, client, task, tasks_controller, watch_stream):
        tasks_controller.watch_task.return_value = watch_stream([task])

        response = client.get(f'{BASE}/{TASK_ID}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert TASK_ID in response.text
        tasks_controller.watch_task.assert_called_once_with(CLUSTER_ID, TASK_ID)
