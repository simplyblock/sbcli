# coding=utf-8
"""Unit tests for v2 router wiring: auth enforcement and _meta probes."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2

from tests.unit.web.api.v2._factories import CLUSTER_ID


@pytest.fixture()
def unauthenticated_client():
    """Client against the v2 router *without* the auth override."""
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    return TestClient(app)


class TestAuthWiring:

    @pytest.mark.parametrize('path', [
        '/api/v2/clusters/',
        f'/api/v2/clusters/{CLUSTER_ID}/',
        '/api/v2/management-nodes/',
    ])
    def test_rejects_requests_without_bearer_token(self, unauthenticated_client, path):
        response = unauthenticated_client.get(path)

        assert response.status_code == 401


class TestMeta:

    def test_health_needs_no_auth(self, unauthenticated_client):
        response = unauthenticated_client.get('/api/v2/_meta/health')

        assert response.status_code == 204

    def test_ready_reports_missing_fdb(self, unauthenticated_client):
        # The unit tier stubs fdb.open to return None, so the readiness
        # probe must report 503.
        response = unauthenticated_client.get('/api/v2/_meta/ready')

        assert response.status_code == 503
