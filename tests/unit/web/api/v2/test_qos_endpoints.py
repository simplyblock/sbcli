"""Unit tests for /api/v2/clusters/{id}/qos and the service class on volume create.

The configuration logic lives in ``qos_controller`` and is tested in
``tests/unit/test_qos_replication_class.py``; what is pinned here is the HTTP
contract: the body a consumer sends and parses, and the status codes.
"""

from unittest.mock import patch

import pytest

from simplyblock_core.exceptions import PreconditionError
from tests.unit.web.api.v2._factories import CLUSTER_ID, POOL_ID, VOLUME_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/qos'


@pytest.fixture()
def qos():
    with patch('simplyblock_web.api.v2.cluster.qos.qos_controller') as m:
        yield m


class TestGetQOS:

    def test_returns_the_configuration(self, client, db, cluster, qos):
        qos.get_config.return_value = {
            'default_weight': 50,
            'classes': [{'id': 1, 'name': 'gold', 'weight': 30}],
            'replication_weight': 10,
            'replication_class_available': True,
            'weights': [43, 25, 22, 0, 0, 0, 0, 10],
        }

        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        body = response.json()
        assert body['classes'] == [{'id': 1, 'name': 'gold', 'weight': 30}]
        assert body['replication_weight'] == 10
        assert body['weights'][7] == 10
        qos.get_config.assert_called_once_with(CLUSTER_ID)


class TestSetQOS:

    def test_applies_the_configuration(self, client, db, cluster, qos):
        qos.set_config.return_value = []

        response = client.put(f'{BASE}/', json={
            'default_weight': 50,
            'classes': [{'name': 'gold', 'weight': 30}],
            'replication_weight': 15,
        })

        assert response.status_code == 204
        qos.set_config.assert_called_once_with(
            CLUSTER_ID, 50, [{'name': 'gold', 'weight': 30}], 15)

    def test_replication_weight_is_optional(self, client, db, cluster, qos):
        qos.set_config.return_value = []

        response = client.put(f'{BASE}/', json={'default_weight': 1})

        assert response.status_code == 204
        qos.set_config.assert_called_once_with(CLUSTER_ID, 1, [], None)

    @pytest.mark.parametrize('body', [
        {'default_weight': 0},
        {'default_weight': 1, 'replication_weight': 75},
        {'default_weight': 1, 'classes': [{'name': f'c{i}', 'weight': 1} for i in range(6)]},
        {'default_weight': 1, 'classes': [{'name': '7', 'weight': 1}]},
    ])
    def test_rejects_invalid_input(self, client, db, cluster, qos, body):
        response = client.put(f'{BASE}/', json=body)

        assert response.status_code == 422
        qos.set_config.assert_not_called()

    def test_a_precondition_error_is_not_mapped_here(self, client, db, cluster, qos):
        """What the caller could have checked past the static validation (422)
        is a PreconditionError, which app.py answers with 400 for the whole
        API. The endpoint lets it through instead of turning it into a 422.

        (This test app mounts only the routers, so an unhandled exception
        surfaces here instead of reaching that handler.)
        """
        qos.set_config.side_effect = PreconditionError('class names must be unique')

        with pytest.raises(PreconditionError):
            client.put(f'{BASE}/', json={'default_weight': 1, 'classes': [
                {'name': 'a', 'weight': 1}, {'name': 'a', 'weight': 2}]})

    def test_class_in_use_is_a_conflict(self, client, db, cluster, qos):
        qos.set_config.side_effect = RuntimeError('classes still used by volumes: gold')

        response = client.put(f'{BASE}/', json={'default_weight': 1})

        assert response.status_code == 409

    def test_unreached_nodes_are_reported(self, client, db, cluster, qos):
        qos.set_config.return_value = ['node-1']

        response = client.put(f'{BASE}/', json={'default_weight': 1})

        assert response.status_code == 503
        assert 'node-1' in response.json()['detail']


class TestVolumeCreateClass:

    URL = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/'

    def test_class_by_name_is_resolved(self, client, db, pool, lvol_controller):
        db.get_lvol_by_name.side_effect = KeyError('LVol not found')
        lvol_controller.add_lvol_ha.return_value = (VOLUME_ID, None)
        with patch('simplyblock_web.api.v2.cluster.storage_pool.volume.qos_controller') as q:
            q.resolve_class.return_value = 2
            response = client.post(self.URL, json={'name': 'v', 'size': '1G', 'priority_class': 'gold'})

        assert response.status_code == 201
        q.resolve_class.assert_called_once_with(CLUSTER_ID, 'gold')
        assert lvol_controller.add_lvol_ha.call_args.kwargs['lvol_priority_class'] == 2

    def test_unknown_class_is_rejected(self, client, db, pool, lvol_controller):
        db.get_lvol_by_name.side_effect = KeyError('LVol not found')
        with patch('simplyblock_web.api.v2.cluster.storage_pool.volume.qos_controller') as q:
            q.resolve_class.side_effect = ValueError('QoS class not found: x')
            response = client.post(self.URL, json={'name': 'v', 'size': '1G', 'priority_class': 'x'})

        assert response.status_code == 422
        lvol_controller.add_lvol_ha.assert_not_called()
