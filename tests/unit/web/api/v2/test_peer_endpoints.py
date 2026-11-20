"""Unit tests for the internal /api/v2/_peer/clusters/{id} endpoints."""
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2.peer as peer_module
from simplyblock_core.peer.protocol import PEER_API_VERSION, PeerInfo
from tests.unit.web.api.v2._factories import CLUSTER_ID

INFO_URL = f'/api/v2/_peer/clusters/{CLUSTER_ID}/info'


@pytest.fixture()
def peer_service(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(peer_module, 'service', mock)
    return mock


class TestInfo:

    def test_returns_the_service_answer_for_the_path_cluster(self, client, db, cluster, peer_service):
        peer_service.info.return_value = PeerInfo(
            cluster_id=UUID(CLUSTER_ID), api_version=PEER_API_VERSION, capabilities=['info'])

        response = client.get(INFO_URL)

        assert response.status_code == 200
        assert response.json() == {
            'cluster_id': CLUSTER_ID, 'api_version': PEER_API_VERSION, 'capabilities': ['info']}
        peer_service.info.assert_called_once_with(CLUSTER_ID)

    def test_unknown_cluster_is_404(self, client, db, peer_service):
        db.get_cluster_by_id.side_effect = KeyError('Cluster not found')

        response = client.get(INFO_URL)

        assert response.status_code == 404
        peer_service.info.assert_not_called()


def test_requires_a_bearer_token(peer_service):
    """Without the test app's auth override, the router refuses an anonymous call."""
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')

    response = TestClient(app).get(INFO_URL)

    assert response.status_code in (401, 403)
    peer_service.info.assert_not_called()
