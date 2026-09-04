# coding=utf-8
"""Unit tests for the volume replication endpoints and the policy assignment
folded into the volume PUT."""

from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import (
    CLUSTER_ID,
    POOL_ID,
    REPLICATION_POLICY_ID,
    TARGET_CLUSTER_ID,
    TASK_ID,
    VOLUME_ID,
)


VOLUME_URL = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/{VOLUME_ID}/'
REPLICATION_URL = VOLUME_URL + 'replication/'
TARGET_VOLUME_ID = '33333333-3333-3333-3333-333333333334'
REPLICATION_ID = 'abababab-abab-abab-abab-abababababab'


def _relationship(**overrides):
    relationship = {
        'replication_id': REPLICATION_ID,
        'source_lvol_id': VOLUME_ID,
        'target_lvol_id': TARGET_VOLUME_ID,
        'source_cluster_id': CLUSTER_ID,
        'target_cluster_id': TARGET_CLUSTER_ID,
        'mode': 'failover',
        'state': 'replicating',
        'direction': 'to_target',
        'target_nqn': 'nqn.2023-02.io.simplyblock:volume-1',
        'target_ns_id': 1,
        'is_source': True,
    }
    relationship.update(overrides)
    return relationship


class TestAssignPolicyThroughUpdate:

    def test_setting_the_policy_attaches_it(self, client, db, volume, lvol_controller,
                                            replication_policy_controller):
        response = client.put(VOLUME_URL, json={'replication_policy_id': REPLICATION_POLICY_ID})

        assert response.status_code == 204
        replication_policy_controller.attach_policy.assert_called_once_with(
            VOLUME_ID, REPLICATION_POLICY_ID)

    def test_null_policy_detaches_it(self, client, db, volume, lvol_controller,
                                     replication_policy_controller):
        response = client.put(VOLUME_URL, json={'replication_policy_id': None})

        assert response.status_code == 204
        replication_policy_controller.detach_policy.assert_called_once_with(VOLUME_ID)

    def test_omitting_the_policy_leaves_it_alone(self, client, db, volume, lvol_controller,
                                                 replication_policy_controller):
        response = client.put(VOLUME_URL, json={'name': 'volume-renamed'})

        assert response.status_code == 204
        replication_policy_controller.attach_policy.assert_not_called()
        replication_policy_controller.detach_policy.assert_not_called()

    def test_non_uuid_policy_rejected(self, client, db, volume, lvol_controller,
                                      replication_policy_controller):
        response = client.put(VOLUME_URL, json={'replication_policy_id': 'nightly'})

        assert response.status_code == 422
        replication_policy_controller.attach_policy.assert_not_called()

    def test_attach_config_error_maps_to_400(self, client, db, volume, lvol_controller,
                                             replication_policy_controller):
        replication_policy_controller.attach_policy.side_effect = \
            ReplicationConfigError('Replication policy nightly is not active')

        response = client.put(VOLUME_URL, json={'replication_policy_id': REPLICATION_POLICY_ID})

        assert response.status_code == 400

    def test_unknown_policy_maps_to_404(self, client, db, volume, lvol_controller,
                                        replication_policy_controller):
        replication_policy_controller.attach_policy.side_effect = KeyError('policy not found')

        response = client.put(VOLUME_URL, json={'replication_policy_id': REPLICATION_POLICY_ID})

        assert response.status_code == 404

    def test_detach_during_cutover_maps_to_409(self, client, db, volume, lvol_controller,
                                               replication_policy_controller):
        replication_policy_controller.detach_policy.side_effect = \
            ReplicationConfigError('cutover in flight')

        response = client.put(VOLUME_URL, json={'replication_policy_id': None})

        assert response.status_code == 409

    def test_size_and_policy_are_applied_together(self, client, db, volume, lvol_controller,
                                                  replication_policy_controller):
        response = client.put(VOLUME_URL, json={
            'size': '20G',
            'replication_policy_id': REPLICATION_POLICY_ID,
        })

        assert response.status_code == 204
        lvol_controller.resize_lvol.assert_called_once()
        replication_policy_controller.attach_policy.assert_called_once()


class TestRelationship:

    def test_returns_the_relationship(self, client, db, volume, replication_policy_controller):
        replication_policy_controller.get_relationship.return_value = _relationship()

        response = client.get(REPLICATION_URL)

        assert response.status_code == 200
        body = response.json()
        assert body['replication_id'] == REPLICATION_ID
        assert body['source_lvol_id'] == VOLUME_ID
        assert body['target_lvol_id'] == TARGET_VOLUME_ID
        assert body['state'] == 'replicating'
        assert body['direction'] == 'to_target'
        assert body['is_source'] is True

    def test_blank_counterpart_serializes_as_null(self, client, db, volume,
                                                  replication_policy_controller):
        replication_policy_controller.get_relationship.return_value = \
            _relationship(target_lvol_id='')

        body = client.get(REPLICATION_URL).json()

        assert body['target_lvol_id'] is None

    def test_missing_relationship_maps_to_404(self, client, db, volume,
                                              replication_policy_controller):
        replication_policy_controller.get_relationship.return_value = None

        assert client.get(REPLICATION_URL).status_code == 404


class TestStartStopTrigger:

    def test_start_passes_parameters(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'start', json={
            'replication_cluster_id': TARGET_CLUSTER_ID,
            'mode': 'migration',
            'interval_min': 15,
        })

        assert response.status_code == 204
        args, kwargs = lvol_controller.replication_start.call_args
        assert args == (VOLUME_ID,)
        assert kwargs == {
            'replication_cluster_id': TARGET_CLUSTER_ID,
            'mode': 'migration',
            'interval_min': 15,
        }

    def test_start_without_body_uses_cluster_default(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'start')

        assert response.status_code == 204
        assert lvol_controller.replication_start.call_args.kwargs == {
            'replication_cluster_id': None, 'mode': None, 'interval_min': None,
        }

    def test_unknown_mode_rejected(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'start', json={'mode': 'sideways'})

        assert response.status_code == 422
        lvol_controller.replication_start.assert_not_called()

    def test_negative_interval_rejected(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'start', json={'interval_min': -1})

        assert response.status_code == 422
        lvol_controller.replication_start.assert_not_called()

    def test_stop(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'stop')

        assert response.status_code == 204
        lvol_controller.replication_stop.assert_called_once_with(VOLUME_ID)

    def test_trigger(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'trigger')

        assert response.status_code == 204
        lvol_controller.replication_trigger.assert_called_once_with(VOLUME_ID)


class TestCutover:

    def test_failover(self, client, db, volume, lvol_controller):
        lvol_controller.replicate_lvol_on_target_cluster.return_value = {
            'lvol_id': TARGET_VOLUME_ID, 'nqn': 'nqn.x', 'ns_id': 1, 'connection_strings': [],
        }

        response = client.post(REPLICATION_URL + 'failover')

        assert response.status_code == 204
        assert response.content == b''
        lvol_controller.replicate_lvol_on_target_cluster.assert_called_once_with(
            VOLUME_ID, generation=0)

    def test_failover_forwards_the_requested_generation(self, client, db, volume,
                                                        lvol_controller):
        """``generation`` selects which retained point-in-time to come up on.

        It has to reach the controller: dropping it silently fails the volume
        over to the NEWEST copy, which in the case this parameter exists for —
        recovering from a logical corruption — is the copy that faithfully
        replicated the corruption.
        """
        lvol_controller.replicate_lvol_on_target_cluster.return_value = {
            'lvol_id': TARGET_VOLUME_ID, 'nqn': 'nqn.x', 'ns_id': 1, 'connection_strings': [],
        }

        response = client.post(REPLICATION_URL + 'failover?generation=2')

        assert response.status_code == 204
        lvol_controller.replicate_lvol_on_target_cluster.assert_called_once_with(
            VOLUME_ID, generation=2)

    def test_negative_generation_rejected(self, client, db, volume, lvol_controller):
        response = client.post(REPLICATION_URL + 'failover?generation=-1')

        assert response.status_code == 400
        lvol_controller.replicate_lvol_on_target_cluster.assert_not_called()

    def test_failed_failover_is_an_error(self, client, db, volume, lvol_controller):
        lvol_controller.replicate_lvol_on_target_cluster.return_value = (False, 'node is not online')

        response = client.post(REPLICATION_URL + 'failover')

        assert response.status_code == 500
        assert response.json()['detail'] == 'node is not online'

    def test_commit_points_at_the_cutover_task(self, client, db, volume, lvol_controller):
        lvol_controller.replication_commit.return_value = {
            'cutover_task_queued': True, 'task_id': TASK_ID,
        }

        response = client.post(REPLICATION_URL + 'commit')

        assert response.status_code == 202
        assert response.content == b''
        assert response.headers['Location'].endswith(f'/clusters/{CLUSTER_ID}/tasks/{TASK_ID}/')
        lvol_controller.replication_commit.assert_called_once_with(VOLUME_ID, delete_source=False)

    def test_unqueued_cutover_is_an_error(self, client, db, volume, lvol_controller):
        lvol_controller.replication_commit.return_value = False

        assert client.post(REPLICATION_URL + 'commit').status_code == 500

    def test_failback(self, client, db, volume, lvol_controller):
        lvol_controller.replication_failback.return_value = True

        response = client.post(REPLICATION_URL + 'failback',
                               json={'source_cluster_id': TARGET_CLUSTER_ID})

        assert response.status_code == 204
        lvol_controller.replication_failback.assert_called_once_with(
            VOLUME_ID, source_cluster_id=TARGET_CLUSTER_ID)

    def test_failback_without_source_cluster(self, client, db, volume, lvol_controller):
        lvol_controller.replication_failback.return_value = True

        response = client.post(REPLICATION_URL + 'failback', json={})

        assert response.status_code == 204
        lvol_controller.replication_failback.assert_called_once_with(
            VOLUME_ID, source_cluster_id=None)

    def test_failed_failback_is_an_error(self, client, db, volume, lvol_controller):
        lvol_controller.replication_failback.return_value = False

        assert client.post(REPLICATION_URL + 'failback', json={}).status_code == 500


class TestTasks:

    def test_lists_replication_tasks(self, client, db, volume, lvol_controller):
        lvol_controller.list_replication_tasks.return_value = [factories.make_task()]

        response = client.get(REPLICATION_URL + 'tasks')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['status'] == 'new'
        lvol_controller.list_replication_tasks.assert_called_once_with(VOLUME_ID)


class TestReplicateOnSourceCluster:

    def test_passes_the_volume_id(self, client, db, pool, lvol_controller):
        url = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/replicate_lvol_on_source_cluster'

        response = client.post(url, json={'lvol_id': VOLUME_ID})

        assert response.status_code == 204
        lvol_controller.replicate_lvol_on_source_cluster.assert_called_once_with(
            VOLUME_ID, CLUSTER_ID, POOL_ID)

    def test_missing_volume_id_rejected(self, client, db, pool, lvol_controller):
        url = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/replicate_lvol_on_source_cluster'

        response = client.post(url, json={})

        assert response.status_code == 422
        lvol_controller.replicate_lvol_on_source_cluster.assert_not_called()
