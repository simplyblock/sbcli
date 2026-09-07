"""Unit tests for /api/v2/clusters/{id}/storage-nodes endpoints (storage_node_ops mocked)."""

import pytest

from simplyblock_core.models.storage_node import StorageNode
from tests.unit.web.api.v2._factories import CLUSTER_ID, STORAGE_NODE_ID, TASK_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-nodes'

# Every status a StorageNode can actually be persisted with (BaseNodeObject's
# STATUS_* constants, inherited). StorageNodeDTO.status is a Pydantic Literal
# hand-listing the subset it accepts -- nothing keeps the two in sync, so a
# core status added without updating the DTO passes silently until a node
# actually reaches it, at which point every v2 storage-nodes list/get 500s
# for as long as that node stays in it (caught live: STATUS_IN_REMOVAL and
# STATUS_PENDING_REMOVAL existed and were actively set by node removal, but
# were missing from the DTO's Literal, breaking /storage-nodes/ for the
# entire removal window -- 2026-08-26).
ALL_STORAGE_NODE_STATUSES = [
    getattr(StorageNode, name) for name in dir(StorageNode)
    if name.startswith('STATUS_') and isinstance(getattr(StorageNode, name), str)
]


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
            'expansion': False,
        })
        # Default response format is 'identifier': body is the task id
        assert response.json() == TASK_ID
        assert response.headers['Location'].endswith(f'/tasks/{TASK_ID}/')

    def test_expand_flag_forwarded(self, client, db, cluster, tasks_controller):
        tasks_controller.add_node_add_task.return_value = TASK_ID

        response = client.post(f'{BASE}/', json={
            'node_address': '10.0.0.10:5000',
            'interface_name': 'eth0',
            'expand': True,
        })

        assert response.status_code == 201
        (_, submitted_task), _ = tasks_controller.add_node_add_task.call_args
        assert submitted_task['expansion'] is True


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

    @pytest.mark.parametrize('status', ALL_STORAGE_NODE_STATUSES)
    def test_every_core_status_serializes(self, client, db, storage_node, status):
        """StorageNodeDTO.status must accept every status the core model can
        actually set -- a status missing from its Literal 500s the endpoint
        for as long as any node holds it (see ALL_STORAGE_NODE_STATUSES)."""
        storage_node.status = status

        response = client.get(f'{BASE}/{STORAGE_NODE_ID}/')

        assert response.status_code == 200
        assert response.json()['status'] == status


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

    def test_refused_removal_returns_400_not_500(self, client, storage_node, storage_node_ops):
        # remove_storage_node's precondition gates (FTT, failure-domain
        # balance, replica-relocation feasibility, ...) signal refusal via
        # `return False`. An unhandled exception with no registered FastAPI
        # handler becomes a 500 -- and 500 is on the operator's *retryable*
        # list, so a permanently-infeasible removal (e.g. would leave a
        # failure domain unbalanced) got retried forever instead of the
        # operator resuming the node it had already suspended and failing
        # cleanly (2026-08-13 incident). Must be a 400: non-retryable there.
        storage_node_ops.remove_storage_node.return_value = False

        response = client.delete(f'{BASE}/{STORAGE_NODE_ID}/')

        assert response.status_code == 400
        storage_node_ops.delete_storage_node.assert_not_called()

    def test_refused_delete_after_successful_remove_returns_400(
            self, client, storage_node, storage_node_ops):
        storage_node_ops.remove_storage_node.return_value = 'task-uuid-1'
        storage_node_ops.delete_storage_node.return_value = False

        response = client.delete(
            f'{BASE}/{STORAGE_NODE_ID}/', params={'force_delete': True})

        assert response.status_code == 400


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

    def test_promote(self, client, storage_node, storage_node_ops):
        response = client.post(f'{BASE}/{STORAGE_NODE_ID}/promote')

        assert response.status_code == 204
        storage_node_ops.make_sec_new_primary.assert_called_once_with(STORAGE_NODE_ID)

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
            new_ssd_pcie=[],
        )

    def test_restart_passes_new_ssd_pcie(self, client, storage_node, storage_node_ops):
        response = client.post(
            f'{BASE}/{STORAGE_NODE_ID}/restart',
            json={'new_ssd_pcie': ['0000:00:1e.0', '0000:00:1f.0']},
        )

        assert response.status_code == 202
        storage_node_ops.restart_storage_node.assert_called_once_with(
            node_id=STORAGE_NODE_ID,
            force=False,
            node_address=None,
            reattach_volume=False,
            new_ssd_pcie=['0000:00:1e.0', '0000:00:1f.0'],
        )


class TestStorageNodeStats:

    def test_capacity_passes_history(self, client, storage_node, storage_node_ops):
        storage_node_ops.get_node_iostats_history.return_value = [{'date': 1}]

        response = client.get(
            f'{BASE}/{STORAGE_NODE_ID}/capacity', params={'history': '10'})

        assert response.status_code == 200
        storage_node_ops.get_node_iostats_history.assert_called_once_with(
            STORAGE_NODE_ID, '10', parse_sizes=False, with_sizes=True)
