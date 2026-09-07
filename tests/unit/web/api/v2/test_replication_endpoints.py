"""Unit tests for /api/v2/clusters/{id}/replication endpoints."""

from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError
from simplyblock_core.utils.nvme import NvmeConnectEntry

from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    REPLICATION_POLICY_ID,
    REPLICATION_TARGET_ID,
    TARGET_CLUSTER_ID,
    TARGET_POOL_ID,
    VOLUME_ID,
)


TARGETS_URL = f'/api/v2/clusters/{CLUSTER_ID}/replication/targets/'
POLICIES_URL = f'/api/v2/clusters/{CLUSTER_ID}/replication/policies/'


class TestListTargets:

    def test_returns_targets_from_controller(self, client, db, cluster, replication_target,
                                             replication_policy_controller):
        replication_policy_controller.list_targets.return_value = [replication_target]

        response = client.get(TARGETS_URL)

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == REPLICATION_TARGET_ID
        assert body['cluster_id'] == CLUSTER_ID
        assert body['target_name'] == 'site-b'
        assert body['target_cluster_id'] == TARGET_CLUSTER_ID
        assert body['target_pool_uuid'] == TARGET_POOL_ID
        replication_policy_controller.list_targets.assert_called_once_with(CLUSTER_ID)

    def test_unset_target_pool_serializes_as_null(self, client, db, cluster, replication_target,
                                                  replication_policy_controller):
        replication_target.target_pool_uuid = ''
        replication_policy_controller.list_targets.return_value = [replication_target]

        (body,) = client.get(TARGETS_URL).json()

        assert body['target_pool_uuid'] is None


class TestCreateTarget:

    def test_creates_target_and_links_to_it(self, client, db, cluster, replication_target,
                                            replication_policy_controller):
        replication_policy_controller.add_target.return_value = \
            f'{CLUSTER_ID}/{REPLICATION_TARGET_ID}'

        response = client.post(TARGETS_URL, json={
            'target_name': 'site-b',
            'target_cluster_id': TARGET_CLUSTER_ID,
            'target_pool_id': TARGET_POOL_ID,
            'timeout_sec': 600,
        })

        assert response.status_code == 201
        assert response.json()['id'] == REPLICATION_TARGET_ID
        assert response.headers['Location'].endswith(
            f'/clusters/{CLUSTER_ID}/replication/targets/{REPLICATION_TARGET_ID}/')
        args, kwargs = replication_policy_controller.add_target.call_args
        assert args == (CLUSTER_ID, 'site-b', TARGET_CLUSTER_ID)
        assert kwargs == {'target_pool': TARGET_POOL_ID, 'timeout_sec': 600}

    def test_identifier_response_format(self, client, db, cluster, replication_target,
                                        replication_policy_controller):
        replication_policy_controller.add_target.return_value = \
            f'{CLUSTER_ID}/{REPLICATION_TARGET_ID}'

        response = client.post(TARGETS_URL + '?response-format=identifier', json={
            'target_name': 'site-b',
            'target_cluster_id': TARGET_CLUSTER_ID,
        })

        assert response.status_code == 201
        assert response.json() == REPLICATION_TARGET_ID

    def test_non_uuid_cluster_rejected(self, client, db, cluster, replication_policy_controller):
        response = client.post(TARGETS_URL, json={
            'target_name': 'site-b',
            'target_cluster_id': 'not-a-uuid',
        })

        assert response.status_code == 422
        replication_policy_controller.add_target.assert_not_called()

    def test_config_error_maps_to_400(self, client, db, cluster, replication_policy_controller):
        replication_policy_controller.add_target.side_effect = \
            ReplicationConfigError('A cluster cannot replicate to itself')

        response = client.post(TARGETS_URL, json={
            'target_name': 'site-b',
            'target_cluster_id': TARGET_CLUSTER_ID,
        })

        assert response.status_code == 400

    def test_unknown_cluster_maps_to_404(self, client, db, cluster, replication_policy_controller):
        replication_policy_controller.add_target.side_effect = KeyError('Cluster not found')

        response = client.post(TARGETS_URL, json={
            'target_name': 'site-b',
            'target_cluster_id': TARGET_CLUSTER_ID,
        })

        assert response.status_code == 404


class TestTargetInstance:

    def test_detail(self, client, db, cluster, replication_target):
        response = client.get(TARGETS_URL + f'{REPLICATION_TARGET_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == REPLICATION_TARGET_ID

    def test_target_of_another_cluster_is_not_found(self, client, db, cluster, replication_target):
        replication_target.cluster_id = TARGET_CLUSTER_ID

        response = client.get(TARGETS_URL + f'{REPLICATION_TARGET_ID}/')

        assert response.status_code == 404

    def test_delete(self, client, db, cluster, replication_target,
                    replication_policy_controller):
        response = client.delete(TARGETS_URL + f'{REPLICATION_TARGET_ID}/')

        assert response.status_code == 204
        replication_policy_controller.remove_target.assert_called_once_with(
            f'{CLUSTER_ID}/{REPLICATION_TARGET_ID}')

    def test_delete_of_used_target_maps_to_400(self, client, db, cluster, replication_target,
                                               replication_policy_controller):
        replication_policy_controller.remove_target.side_effect = \
            ReplicationConfigError('still used by 1 policy(ies)')

        response = client.delete(TARGETS_URL + f'{REPLICATION_TARGET_ID}/')

        assert response.status_code == 400

    def test_failover_reports_per_volume_results(self, client, db, cluster, replication_target,
                                                 replication_policy_controller):
        entry = NvmeConnectEntry(
            transport='tcp', ip='10.0.0.9', port=4420, nqn=f'nqn.orig:lvol:{VOLUME_ID}',
            reconnect_delay=2, ctrl_loss_tmo=60, fast_io_fail_tmo=15, nr_io_queues=8,
            keep_alive_tmo=5, connect='sudo nvme connect …', ns_id=7,
            target_lvol_id=VOLUME_ID,
        ).model_dump(by_alias=True)
        replication_policy_controller.failover_target.return_value = [
            {'lvol_id': VOLUME_ID, 'status': 'failed_over',
             'target_lvol_id': VOLUME_ID, 'connection_strings': [entry]},
            {'lvol_id': VOLUME_ID, 'status': 'skipped',
             'detail': 'already failed_over', 'target_lvol_id': ''},
        ]

        response = client.post(TARGETS_URL + f'{REPLICATION_TARGET_ID}/failover')

        assert response.status_code == 200
        done, skipped = response.json()
        assert done['status'] == 'failed_over'
        assert done['connection_strings'] == [entry]
        assert done['connection_strings'][0]['target-lvol-id'] == VOLUME_ID
        assert skipped['status'] == 'skipped'
        assert skipped['target_lvol_id'] is None


class TestListPolicies:

    def test_returns_policies_from_controller(self, client, db, cluster, replication_policy,
                                              replication_policy_controller):
        replication_policy_controller.list_policies.return_value = [replication_policy]

        response = client.get(POLICIES_URL)

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == REPLICATION_POLICY_ID
        assert body['policy_name'] == 'nightly'
        assert body['target_id'] == REPLICATION_TARGET_ID
        assert body['mode'] == 'failover'
        replication_policy_controller.list_policies.assert_called_once_with(CLUSTER_ID)


class TestCreatePolicy:

    def test_creates_policy_and_links_to_it(self, client, db, cluster, replication_policy,
                                            replication_policy_controller):
        replication_policy_controller.add_policy.return_value = \
            f'{CLUSTER_ID}/{REPLICATION_POLICY_ID}'

        response = client.post(POLICIES_URL, json={
            'policy_name': 'nightly',
            'target_id': REPLICATION_TARGET_ID,
            'interval_min': 5,
            'mode': 'failover',
            'keep_replicated': 3,
        })

        assert response.status_code == 201
        assert response.json()['id'] == REPLICATION_POLICY_ID
        assert response.headers['Location'].endswith(
            f'/clusters/{CLUSTER_ID}/replication/policies/{REPLICATION_POLICY_ID}/')
        args, kwargs = replication_policy_controller.add_policy.call_args
        assert args == (CLUSTER_ID, 'nightly', REPLICATION_TARGET_ID)
        assert kwargs == {'interval_min': 5, 'mode': 'failover', 'keep_replicated': 3}

    def test_unknown_mode_rejected(self, client, db, cluster, replication_policy_controller):
        response = client.post(POLICIES_URL, json={
            'policy_name': 'nightly',
            'target_id': REPLICATION_TARGET_ID,
            'mode': 'sideways',
        })

        assert response.status_code == 422
        replication_policy_controller.add_policy.assert_not_called()

    def test_keep_replicated_below_minimum_rejected(self, client, db, cluster,
                                                    replication_policy_controller):
        response = client.post(POLICIES_URL, json={
            'policy_name': 'nightly',
            'target_id': REPLICATION_TARGET_ID,
            'keep_replicated': 1,
        })

        assert response.status_code == 422
        replication_policy_controller.add_policy.assert_not_called()


class TestPolicyInstance:

    def test_detail(self, client, db, cluster, replication_policy):
        response = client.get(POLICIES_URL + f'{REPLICATION_POLICY_ID}/')

        assert response.status_code == 200
        assert response.json()['id'] == REPLICATION_POLICY_ID

    def test_policy_of_another_cluster_is_not_found(self, client, db, cluster, replication_policy):
        replication_policy.cluster_id = TARGET_CLUSTER_ID

        response = client.get(POLICIES_URL + f'{REPLICATION_POLICY_ID}/')

        assert response.status_code == 404

    def test_delete(self, client, db, cluster, replication_policy,
                    replication_policy_controller):
        response = client.delete(POLICIES_URL + f'{REPLICATION_POLICY_ID}/')

        assert response.status_code == 204
        replication_policy_controller.remove_policy.assert_called_once_with(
            f'{CLUSTER_ID}/{REPLICATION_POLICY_ID}')

    def test_failover(self, client, db, cluster, replication_policy,
                      replication_policy_controller):
        replication_policy_controller.failover_policy.return_value = [
            {'lvol_id': VOLUME_ID, 'status': 'failed', 'detail': 'boom'},
        ]

        response = client.post(POLICIES_URL + f'{REPLICATION_POLICY_ID}/failover')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['status'] == 'failed'
        assert body['detail'] == 'boom'
        replication_policy_controller.failover_policy.assert_called_once_with(
            f'{CLUSTER_ID}/{REPLICATION_POLICY_ID}')
