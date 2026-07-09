# coding=utf-8
"""Unit tests for the unauthenticated v2 ``/_meta`` probe endpoints."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2


@pytest.fixture()
def unauthenticated_client():
    """Client against the v2 router *without* the auth override.

    The ``_meta`` probes are mounted without the ``verify_api_token``
    dependency, so they must answer even with no bearer token.
    """
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    return TestClient(app)


class TestMeta:

    def test_health_needs_no_auth(self, unauthenticated_client):
        response = unauthenticated_client.get('/api/v2/_meta/health')

        assert response.status_code == 204

    def test_ready_reports_missing_fdb(self, unauthenticated_client):
        # The unit tier stubs fdb.open to return None, so the readiness
        # probe must report 503.
        response = unauthenticated_client.get('/api/v2/_meta/ready')

        assert response.status_code == 503
