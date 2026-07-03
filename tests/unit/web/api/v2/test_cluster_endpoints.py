# coding=utf-8
"""Unit tests for /api/v2/clusters endpoints (cluster_ops mocked)."""

import pytest

from tests.unit.web.api.v2._factories import CLUSTER_ID


class TestListClusters:

    def test_returns_clusters_from_db(self, client, db, cluster):
        response = client.get('/api/v2/clusters/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == CLUSTER_ID
        assert body['name'] == 'cluster-1'
        assert body['status'] == 'active'
        assert body['ha'] is True
        db.get_clusters.assert_called_once_with()

    def test_unwraps_secret_in_wire_response(self, client, db, cluster):
        (body,) = client.get('/api/v2/clusters/').json()

        assert body['secret'] == 'cluster-secret'


class TestCreateCluster:

    def test_calls_add_cluster_with_parameters(self, client, db, cluster, cluster_ops):
        cluster_ops.add_cluster.return_value = CLUSTER_ID

        response = client.post('/api/v2/clusters/', json={'name': 'cluster-1', 'distr_npcs': 2})

        assert response.status_code == 201
        kwargs = cluster_ops.add_cluster.call_args.kwargs
        assert kwargs['name'] == 'cluster-1'
        assert kwargs['distr_npcs'] == 2
        assert kwargs['max_fault_tolerance'] == 2
        assert kwargs['blk_size'] == 512
        assert kwargs['ha_type'] == 'ha'
        assert response.json()['id'] == CLUSTER_ID
        assert response.headers['Location'].endswith(f'/clusters/{CLUSTER_ID}/')
        db.get_cluster_by_id.assert_called_once_with(CLUSTER_ID)

    def test_caps_max_fault_tolerance_at_two(self, client, db, cluster, cluster_ops):
        cluster_ops.add_cluster.return_value = CLUSTER_ID

        client.post('/api/v2/clusters/', json={'distr_npcs': 4})

        assert cluster_ops.add_cluster.call_args.kwargs['max_fault_tolerance'] == 2

    def test_conflict_maps_to_409(self, client, db, cluster_ops):
        cluster_ops.add_cluster.side_effect = ValueError('cluster exists')

        response = client.post('/api/v2/clusters/', json={'name': 'cluster-1'})

        assert response.status_code == 409


class TestGetCluster:

    def test_returns_cluster(self, client, db, cluster):
        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == CLUSTER_ID
        db.get_cluster_by_id.assert_called_once_with(CLUSTER_ID)

    def test_unknown_cluster_returns_404(self, client, db):
        db.get_cluster_by_id.side_effect = KeyError('Cluster not found')

        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/')

        assert response.status_code == 404


class TestUpdateCluster:

    def test_sets_name(self, client, cluster, cluster_ops):
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/', json={'name': 'renamed'})

        assert response.status_code == 204
        cluster_ops.set_name.assert_called_once_with(CLUSTER_ID, 'renamed')

    def test_omitted_name_is_not_set(self, client, cluster, cluster_ops):
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/', json={})

        assert response.status_code == 204
        cluster_ops.set_name.assert_not_called()


class TestDeleteCluster:

    def test_deletes_cluster(self, client, cluster, cluster_ops):
        response = client.delete(f'/api/v2/clusters/{CLUSTER_ID}/')

        assert response.status_code == 204
        cluster_ops.delete_cluster.assert_called_once_with(CLUSTER_ID)

    def test_conflict_maps_to_409(self, client, cluster, cluster_ops):
        cluster_ops.delete_cluster.side_effect = ValueError('cluster is not empty')

        response = client.delete(f'/api/v2/clusters/{CLUSTER_ID}/')

        assert response.status_code == 409


class TestClusterLifecycleActions:

    @pytest.mark.parametrize('action,operation', [
        ('start', 'cluster_grace_startup'),
        ('shutdown', 'cluster_grace_shutdown'),
        ('activate', 'cluster_activate'),
        ('expand', 'cluster_expand'),
    ])
    def test_runs_operation_on_cluster(self, client, cluster, cluster_ops, action, operation):
        response = client.post(f'/api/v2/clusters/{CLUSTER_ID}/{action}')

        assert response.status_code == 202
        getattr(cluster_ops, operation).assert_called_once_with(CLUSTER_ID)


class TestClusterStats:

    def test_capacity_passes_history(self, client, cluster, cluster_ops):
        cluster_ops.get_capacity.return_value = [{'date': 1}]

        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/capacity', params={'history': '10'})

        assert response.status_code == 200
        assert response.json() == [{'date': 1}]
        cluster_ops.get_capacity.assert_called_once_with(CLUSTER_ID, '10')

    def test_iostats(self, client, cluster, cluster_ops):
        cluster_ops.get_iostats_history.return_value = [{'date': 1}]

        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/iostats')

        assert response.status_code == 200
        cluster_ops.get_iostats_history.assert_called_once_with(CLUSTER_ID, None, with_sizes=True)

    def test_logs_pass_limit(self, client, cluster, cluster_ops):
        cluster_ops.get_logs.return_value = [{'message': 'started'}]

        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/logs', params={'limit': 10})

        assert response.status_code == 200
        cluster_ops.get_logs.assert_called_once_with(CLUSTER_ID, is_json=True, limit=10)


class TestUpgradeCluster:

    def test_management_only_update(self, client, cluster, cluster_ops):
        response = client.post(
            f'/api/v2/clusters/{CLUSTER_ID}/update',
            json={'management_image': 'simplyblock/mgmt:2', 'spdk_image': None},
        )

        assert response.status_code == 204
        cluster_ops.update_cluster.assert_called_once_with(
            cluster_id=CLUSTER_ID,
            mgmt_image='simplyblock/mgmt:2',
            mgmt_only=True,
            spdk_image=None,
            restart=False,
        )

    def test_spdk_update_disables_mgmt_only(self, client, cluster, cluster_ops):
        client.post(
            f'/api/v2/clusters/{CLUSTER_ID}/update',
            json={'management_image': None, 'spdk_image': 'simplyblock/spdk:2', 'restart': True},
        )

        cluster_ops.update_cluster.assert_called_once_with(
            cluster_id=CLUSTER_ID,
            mgmt_image=None,
            mgmt_only=False,
            spdk_image='simplyblock/spdk:2',
            restart=True,
        )


class TestAddReplication:

    def test_calls_add_replication(self, client, cluster, cluster_ops):
        target = '11111111-1111-1111-1111-111111111112'

        response = client.post(
            f'/api/v2/clusters/{CLUSTER_ID}/addreplication',
            json={
                'snapshot_replication_target_cluster': target,
                'snapshot_replication_timeout': 30,
                'target_pool': 'pool-1',
            },
        )

        assert response.status_code == 202
        cluster_ops.add_replication.assert_called_once_with(
            source_cl_id=CLUSTER_ID,
            target_cl_id=target,
            timeout=30,
            target_pool='pool-1',
        )


class TestWatchClusters:

    def test_list_dispatches_watch_clusters(self, client, cluster, cluster_ops, watch_stream):
        cluster_ops.watch_clusters.return_value = watch_stream([cluster])

        response = client.get('/api/v2/clusters/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert CLUSTER_ID in response.text
        cluster_ops.watch_clusters.assert_called_once_with()

    def test_detail_dispatches_watch_cluster(self, client, cluster, cluster_ops, watch_stream):
        cluster_ops.watch_cluster.return_value = watch_stream([cluster])

        response = client.get(f'/api/v2/clusters/{CLUSTER_ID}/?watch=true')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/event-stream')
        assert 'event: snapshot' in response.text
        assert CLUSTER_ID in response.text
        cluster_ops.watch_cluster.assert_called_once_with(CLUSTER_ID)
