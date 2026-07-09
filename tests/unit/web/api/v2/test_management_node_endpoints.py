# coding=utf-8
"""Unit tests for /api/v2/management-nodes endpoints."""

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, MANAGEMENT_NODE_ID

BASE = '/api/v2/management-nodes'


class TestListManagementNodes:

    def test_returns_only_nodes_of_cluster(self, client, db, management_node):
        other = factories.make_management_node(
            uuid='88888888-8888-8888-8888-888888888889',
            cluster_id='11111111-1111-1111-1111-111111111112',
        )
        db.get_mgmt_nodes.return_value = [management_node, other]

        response = client.get(f'{BASE}/', params={'cluster_id': CLUSTER_ID})

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == MANAGEMENT_NODE_ID
        assert body['hostname'] == 'mgmt-1'
        assert body['ip'] == '10.0.0.5'


class TestGetManagementNode:

    def test_returns_node(self, client, db, management_node):
        response = client.get(
            f'{BASE}/{MANAGEMENT_NODE_ID}/', params={'cluster_id': CLUSTER_ID})

        assert response.status_code == 200
        assert response.json()['id'] == MANAGEMENT_NODE_ID
        db.get_mgmt_node_by_id.assert_called_once_with(MANAGEMENT_NODE_ID)

    def test_unknown_node_returns_404(self, client, db, cluster):
        db.get_mgmt_node_by_id.return_value = None

        response = client.get(
            f'{BASE}/{MANAGEMENT_NODE_ID}/', params={'cluster_id': CLUSTER_ID})

        assert response.status_code == 404
