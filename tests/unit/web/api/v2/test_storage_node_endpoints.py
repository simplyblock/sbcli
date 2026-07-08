# coding=utf-8
"""Unit tests for /api/v2/clusters/{id}/storage-nodes endpoints (storage_node_ops mocked)."""

from tests.unit.web.api.v2._factories import CLUSTER_ID, STORAGE_NODE_ID, TASK_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-nodes'


class TestListStorageNodes:

    def test_returns_nodes_of_cluster(self, client, db, storage_node):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == STORAGE_NODE_ID
        assert body['cluster_id'] == CLUSTER_ID
        assert body['hostname'] == 'snode-1'
        assert body['status'] == 'online'
        db.get_storage_nodes_by_cluster_id.assert_called_once_with(CLUSTER_ID)


class TestCreateStorageNode:

    def test_creates_add_node_task(self, client, db, cluster, tasks_controller):
        tasks_controller.add_node_add_task.return_value = TASK_ID

        response = client.post(f'{BASE}/', json={
            'node_address': '10.0.0.10:5000',
            'interface_name': 'eth0',
        })

        assert response.status_code == 201
        tasks_controller.add_node_add_task.assert_called_once_with(CLUSTER_ID, {
            'cluster_id': CLUSTER_ID,
            'node_addr': '10.0.0.10:5000',
            'iface_name': 'eth0',
            'data_nics_list': [],
            'max_snap': 500,
            'spdk_image': '',
            'spdk_debug': False,
            'small_bufsize': 0,
            'large_bufsize': 0,
            'num_partitions_per_dev': 1,
            'jm_percent': 3,
            'enable_test_device': False,
            'namespace': 'default',
            'enable_ha_jm': True,
            'id_device_by_nqn': False,
            'cr_name': '',
            'cr_namespace': '',
            'cr_plural': '',
            'ha_jm_count': None,
            'format_4k': False,
            'spdk_proxy_image': None,
            'spdk_sys_mem': None,
            'failure_domain': None,
        })
        # Default response format is 'identifier': body is the task id
        assert response.json() == TASK_ID
        assert response.headers['Location'].endswith(f'/tasks/{TASK_ID}/')


class TestGetStorageNode:

    def test_returns_node(self, client, db, storage_node):
        response = client.get(f'{BASE}/{STORAGE_NODE_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == STORAGE_NODE_ID
        db.get_storage_node_by_id.assert_called_once_with(STORAGE_NODE_ID)

    def test_node_of_other_cluster_returns_404(self, client, db, storage_node):
        storage_node.cluster_id = '11111111-1111-1111-1111-111111111112'

        response = client.get(f'{BASE}/{STORAGE_NODE_ID}/')

        assert response.status_code == 404


class TestDeleteStorageNode:

    def test_removes_node(self, client, storage_node, storage_node_ops):
        response = client.delete(f'{BASE}/{STORAGE_NODE_ID}/')

        assert response.status_code == 204
        storage_node_ops.remove_storage_node.assert_called_once_with(
            STORAGE_NODE_ID, force_remove=False, force_migrate=False)
        storage_node_ops.delete_storage_node.assert_not_called()

    def test_force_delete_also_deletes_node(self, client, storage_node, storage_node_ops):
        response = client.delete(
            f'{BASE}/{STORAGE_NODE_ID}/', params={'force_remove': True, 'force_delete': True})

        assert response.status_code == 204
        storage_node_ops.remove_storage_node.assert_called_once_with(
            STORAGE_NODE_ID, force_remove=True, force_migrate=False)
        storage_node_ops.delete_storage_node.assert_called_once_with(
            STORAGE_NODE_ID, force=True)


class TestStorageNodeLifecycle:

    def test_suspend(self, client, storage_node, storage_node_ops):
        storage_node_ops.suspend_storage_node.return_value = True

        response = client.post(f'{BASE}/{STORAGE_NODE_ID}/suspend', params={'force': True})

        assert response.status_code == 204
        storage_node_ops.suspend_storage_node.assert_called_once_with(STORAGE_NODE_ID, True)

    def test_resume(self, client, storage_node, storage_node_ops):
        storage_node_ops.resume_storage_node.return_value = True

        response = client.post(f'{BASE}/{STORAGE_NODE_ID}/resume')

        assert response.status_code == 204
        storage_node_ops.resume_storage_node.assert_called_once_with(STORAGE_NODE_ID)

    def test_forced_shutdown(self, client, storage_node, storage_node_ops):
        response = client.post(f'{BASE}/{STORAGE_NODE_ID}/shutdown', params={'force': True})

        assert response.status_code == 202
        storage_node_ops.shutdown_storage_node.assert_called_once_with(STORAGE_NODE_ID, True)

    def test_restart(self, client, storage_node, storage_node_ops):
        response = client.post(
            f'{BASE}/{STORAGE_NODE_ID}/restart',
            json={'force': True, 'node_address': '10.0.0.11:5000'},
        )

        assert response.status_code == 202
        storage_node_ops.restart_storage_node.assert_called_once_with(
            node_id=STORAGE_NODE_ID,
            force=True,
            node_address='10.0.0.11:5000',
            reattach_volume=False,
        )


class TestStorageNodeStats:

    def test_capacity_passes_history(self, client, storage_node, storage_node_ops):
        storage_node_ops.get_node_iostats_history.return_value = [{'date': 1}]

        response = client.get(
            f'{BASE}/{STORAGE_NODE_ID}/capacity', params={'history': '10'})

        assert response.status_code == 200
        storage_node_ops.get_node_iostats_history.assert_called_once_with(
            STORAGE_NODE_ID, '10', parse_sizes=False, with_sizes=True)
