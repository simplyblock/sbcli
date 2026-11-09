"""Unit tests for the internal /api/v2/_peer/clusters/{id} endpoints."""
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2.peer as peer_module
from simplyblock_core.peer.protocol import (
    PEER_API_VERSION,
    DescribeRequest,
    DescribeResponse,
    PeerInfo,
)
from simplyblock_core.peer.service import NotALandingVolumeError
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


LVOL_ID = '0b6f4a8e-1c2d-4e3f-9a0b-1c2d3e4f5a99'
DISCARD_URL = f'/api/v2/_peer/clusters/{CLUSTER_ID}/landing-volumes/{LVOL_ID}'


class TestDiscardLanding:

    def test_deletes_through_the_service(self, client, db, cluster, peer_service):
        response = client.delete(DISCARD_URL)

        assert response.status_code == 204
        assert response.content == b''
        peer_service.discard_landing.assert_called_once_with(CLUSTER_ID, LVOL_ID)

    def test_a_volume_on_another_cluster_is_404(self, client, db, cluster, peer_service):
        peer_service.discard_landing.side_effect = KeyError('LVol is not on cluster')

        assert client.delete(DISCARD_URL).status_code == 404

    def test_a_refused_delete_is_409(self, client, db, cluster, peer_service):
        peer_service.discard_landing.side_effect = NotALandingVolumeError('not a landing volume')

        response = client.delete(DISCARD_URL)

        assert response.status_code == 409
        assert 'not a landing volume' in response.json()['detail']

    def test_a_malformed_volume_id_is_rejected(self, client, db, cluster, peer_service):
        response = client.delete(f'/api/v2/_peer/clusters/{CLUSTER_ID}/landing-volumes/not-a-uuid')

        assert response.status_code == 422
        peer_service.discard_landing.assert_not_called()


DESCRIBE_URL = f'/api/v2/_peer/clusters/{CLUSTER_ID}/describe'


class TestDescribe:

    def test_answers_through_the_service(self, client, db, cluster, peer_service):
        peer_service.describe.return_value = DescribeResponse(landing_leaders=['TS'])

        response = client.post(DESCRIBE_URL, json={'landing_lvol_id': 'R'})

        assert response.status_code == 200
        assert response.json()['landing_leaders'] == ['TS']
        cluster_id, request = peer_service.describe.call_args.args
        assert cluster_id == CLUSTER_ID
        assert request == DescribeRequest(landing_lvol_id='R')

    def test_a_malformed_question_is_rejected(self, client, db, cluster, peer_service):
        response = client.post(DESCRIBE_URL, json={'chain_checks': [{'successor_id': 'T2'}]})

        assert response.status_code == 422
        peer_service.describe.assert_not_called()
