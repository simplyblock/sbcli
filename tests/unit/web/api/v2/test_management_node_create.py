"""``POST /management-nodes``: a stretched cluster's second control plane
registering itself with its site (docs/stretched-control-plane.md). The core
is mocked at the router module; these tests pin the HTTP contract.
"""
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2.management_node as mn_module
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, MANAGEMENT_NODE_ID

URL = f'/api/v2/management-nodes/?cluster_id={CLUSTER_ID}'


@pytest.fixture()
def cluster():
    return factories.make_cluster(stretched_control_plane=True, leader_site="a")


@pytest.fixture()
def client(cluster):
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    app.dependency_overrides[dependencies_module._lookup_cluster] = lambda: cluster
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def ops(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(mn_module, 'mgmt_node_ops', mock)
    return mock


@pytest.fixture()
def db(monkeypatch):
    mock = MagicMock()
    mock.get_mgmt_node_by_id.return_value = factories.make_management_node(site="b")
    monkeypatch.setattr(mn_module, 'db', mock)
    return mock


def test_registers_the_site_and_endpoint(client, ops, db):
    ops.add_mgmt_node.return_value = MANAGEMENT_NODE_ID
    response = client.post(URL, json={'site': 'b', 'api_endpoint': 'https://cp-b:443'})
    assert response.status_code == 201, response.text
    assert response.json()['site'] == 'b'
    ops.add_mgmt_node.assert_called_once_with(
        '0.0.0.0', 'kubernetes', CLUSTER_ID, site='b', site_api_endpoint='https://cp-b:443')


def test_a_refused_site_is_400_with_a_message(client, ops, db):
    ops.add_mgmt_node.side_effect = ValueError('this cluster has a stretched control plane; --site is required')
    response = client.post(URL, json={})
    assert response.status_code == 400
    assert 'site is required' in response.json()['detail']['message']


def test_a_duplicate_address_is_409(client, ops, db):
    ops.add_mgmt_node.return_value = False
    assert client.post(URL, json={'site': 'b'}).status_code == 409
