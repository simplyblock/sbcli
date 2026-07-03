# coding=utf-8
"""Unit tests for /api/v2/clusters/{id}/storage-pools endpoints (pool_controller mocked)."""

from simplyblock_core.models.pool import Pool

from tests.unit.web.api.v2._factories import CLUSTER_ID, POOL_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools'
HOST_NQN = 'nqn.2014-08.org.nvmexpress:host-1'


class TestListStoragePools:

    def test_returns_pools_of_cluster(self, client, db, pool):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == POOL_ID
        assert body['cluster_id'] == CLUSTER_ID
        assert body['name'] == 'pool-1'
        db.get_pools.assert_called_with(CLUSTER_ID)


class TestCreateStoragePool:

    def test_calls_add_pool(self, client, db, pool, pool_controller):
        pool_controller.add_pool.return_value = POOL_ID

        response = client.post(f'{BASE}/', json={'name': 'pool-2', 'pool_max': 1024})

        assert response.status_code == 201
        pool_controller.add_pool.assert_called_once_with(
            'pool-2', 1024, 0, 0, 0, 0, 0, CLUSTER_ID, '', '', '', dhchap=False,
        )
        assert response.json()['id'] == POOL_ID
        assert response.headers['Location'].endswith(f'/storage-pools/{POOL_ID}/')

    def test_duplicate_name_returns_409(self, client, db, pool, pool_controller):
        response = client.post(f'{BASE}/', json={'name': pool.pool_name})

        assert response.status_code == 409
        pool_controller.add_pool.assert_not_called()


class TestGetStoragePool:

    def test_returns_pool(self, client, db, pool):
        response = client.get(f'{BASE}/{POOL_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == POOL_ID
        db.get_pool_by_id.assert_called_once_with(POOL_ID)

    def test_pool_of_other_cluster_returns_404(self, client, db, pool):
        pool.cluster_id = '11111111-1111-1111-1111-111111111112'

        response = client.get(f'{BASE}/{POOL_ID}/')

        assert response.status_code == 404

    def test_unknown_pool_returns_404(self, client, db, cluster):
        db.get_pool_by_id.side_effect = KeyError('Pool not found')

        response = client.get(f'{BASE}/{POOL_ID}/')

        assert response.status_code == 404


class TestUpdateStoragePool:

    def test_passes_only_set_fields_with_renames(self, client, pool, pool_controller):
        pool_controller.set_pool.return_value = (True, None)

        response = client.put(f'{BASE}/{POOL_ID}/', json={'name': 'renamed', 'max_size': 2048})

        assert response.status_code == 204
        pool_controller.set_pool.assert_called_once_with(POOL_ID, name='renamed', pool_max=2048)


class TestDeleteStoragePool:

    def test_deletes_pool(self, client, pool, pool_controller):
        pool_controller.delete_pool.return_value = True

        response = client.delete(f'{BASE}/{POOL_ID}/')

        assert response.status_code == 204
        pool_controller.delete_pool.assert_called_once_with(POOL_ID)

    def test_inactive_pool_returns_400(self, client, pool, pool_controller):
        pool.status = Pool.STATUS_INACTIVE

        response = client.delete(f'{BASE}/{POOL_ID}/')

        assert response.status_code == 400
        pool_controller.delete_pool.assert_not_called()


class TestStoragePoolHosts:

    def test_add_host(self, client, pool, pool_controller):
        pool_controller.add_host_to_pool.return_value = (True, None)

        response = client.post(f'{BASE}/{POOL_ID}/host', json={'host_nqn': HOST_NQN})

        assert response.status_code == 204
        pool_controller.add_host_to_pool.assert_called_once_with(POOL_ID, HOST_NQN)

    def test_add_host_error_returns_400(self, client, pool, pool_controller):
        pool_controller.add_host_to_pool.return_value = (False, 'host already added')

        response = client.post(f'{BASE}/{POOL_ID}/host', json={'host_nqn': HOST_NQN})

        assert response.status_code == 400

    def test_remove_host(self, client, pool, pool_controller):
        pool_controller.remove_host_from_pool.return_value = (True, None)

        response = client.request('DELETE', f'{BASE}/{POOL_ID}/host', json={'host_nqn': HOST_NQN})

        assert response.status_code == 204
        pool_controller.remove_host_from_pool.assert_called_once_with(POOL_ID, HOST_NQN)


class TestWatchStoragePools:

    def test_list_dispatches_watch_pools(self, client, pool, pool_controller, watch_stream):
        pool_controller.watch_pools.return_value = watch_stream([pool])

        response = client.get(f'{BASE}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert POOL_ID in response.text
        pool_controller.watch_pools.assert_called_once_with(CLUSTER_ID)

    def test_detail_dispatches_watch_pool(self, client, pool, pool_controller, watch_stream):
        pool_controller.watch_pool.return_value = watch_stream([pool])

        response = client.get(f'{BASE}/{POOL_ID}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert POOL_ID in response.text
        pool_controller.watch_pool.assert_called_once_with(CLUSTER_ID, POOL_ID)
