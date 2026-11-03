"""The leader site of a sync-replication (stretched) cluster on the v2 cluster
routes (design-stretched-sync-replication-ramen §5.1, §11.1, §13): the create
body's ``leader_site``, ``ClusterDTO.leader_site``, and the PUT that moves it.

The routes run in their own FastAPI app: the cluster lookup is overridden
with an in-memory model and the core modules are mocked at the router module,
so no database API is touched - these tests pin the HTTP contract (status
codes, ``detail.message``, the order of check / apply). The behavior against
the real database is in tests/integration/test_sync_replication_leader_site.py.
"""
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2.cluster as cluster_module
from simplyblock_core.exceptions import (
    SyncLeaderSiteRefusedError,
    SyncReplicationSiteError,
    SyncReplicationUnsupportedError,
)
from simplyblock_web.api.v2._dtos import ClusterDTO
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID

URL = f'/api/v2/clusters/{CLUSTER_ID}/'
SIZING = {'max_subsys': 40, 'spdk_vcpu_count': 4}


@pytest.fixture()
def cluster():
    return factories.make_cluster(sync_replication=True)


@pytest.fixture()
def client(cluster):
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    app.dependency_overrides[dependencies_module._lookup_cluster] = lambda: cluster
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def src(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(cluster_module, 'sync_replication_controller', mock)
    return mock


@pytest.fixture()
def ops(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(cluster_module, 'cluster_ops', mock)
    return mock


def _message(response):
    return response.json()['detail']['message']


class TestDTO:

    def test_exposes_the_leader_site(self):
        dto = ClusterDTO.from_model(factories.make_cluster(sync_replication=True, leader_site='site-a'))
        assert dto.leader_site == 'site-a'

    def test_defaults_to_unset(self):
        assert ClusterDTO.from_model(factories.make_cluster()).leader_site == ''


class TestCreate:

    def test_a_refused_leader_site_is_400_with_a_message(self, client, ops):
        ops.add_cluster.side_effect = SyncReplicationSiteError(
            'leader_site is only allowed together with sync_replication')

        response = client.post('/api/v2/clusters/', json={'leader_site': 'site-a', **SIZING})

        assert response.status_code == 400
        assert 'sync_replication' in _message(response)

    def test_the_leader_site_reaches_add_cluster(self, client, ops):
        ops.add_cluster.side_effect = SyncReplicationSiteError('stop here')

        client.post('/api/v2/clusters/', json={'sync_replication': True, 'leader_site': 'site-a', **SIZING})

        kwargs = ops.add_cluster.call_args.kwargs
        assert kwargs['sync_replication'] is True
        assert kwargs['leader_site'] == 'site-a'

    def test_a_name_conflict_is_still_409(self, client, ops):
        ops.add_cluster.side_effect = ValueError('cluster exists')
        response = client.post('/api/v2/clusters/', json=SIZING)
        assert response.status_code == 409


class TestUpdate:

    def test_sets_the_leader_site(self, client, cluster, src, ops):
        response = client.put(URL, json={'leader_site': 'site-b'})

        assert response.status_code == 204
        src.check_leader_site.assert_called_once_with(cluster, 'site-b')
        src.set_leader_site.assert_called_once_with(CLUSTER_ID, 'site-b')

    @pytest.mark.parametrize('error', [
        SyncReplicationUnsupportedError(f'cluster {CLUSTER_ID} is not a sync-replication cluster'),
        SyncReplicationSiteError('leader_site must not be empty'),
        SyncReplicationSiteError("site 'site-x' is not a site of cluster"),
    ])
    def test_a_refused_check_is_400_and_changes_nothing(self, client, src, ops, error):
        src.check_leader_site.side_effect = error

        response = client.put(URL, json={'name': 'renamed', 'leader_site': 'site-x'})

        assert response.status_code == 400
        assert _message(response) == str(error)
        ops.set_name.assert_not_called()
        src.set_leader_site.assert_not_called()

    def test_a_lost_site_is_409_and_changes_nothing(self, client, src, ops):
        src.check_leader_site.side_effect = SyncLeaderSiteRefusedError(
            "site 'site-a' is recorded lost; a lost site cannot lead")

        response = client.put(URL, json={'name': 'renamed', 'leader_site': 'site-a'})

        assert response.status_code == 409
        assert 'cannot lead' in _message(response)
        ops.set_name.assert_not_called()
        src.set_leader_site.assert_not_called()

    def test_a_site_lost_after_the_check_is_409(self, client, src, ops):
        """set_leader_site judges the record its transaction writes: a site
        recorded lost between the check and the write is still refused."""
        src.set_leader_site.side_effect = SyncLeaderSiteRefusedError("site 'site-a' is recorded lost")

        response = client.put(URL, json={'leader_site': 'site-a'})

        assert response.status_code == 409

    def test_validation_runs_before_the_arbitration_flag_and_the_rename(self, client, src, ops):
        src.check_leader_site.side_effect = SyncReplicationSiteError('nope')

        response = client.put(URL, json={'name': 'n', 'two_node_arbitration': False, 'leader_site': 'x'})

        assert response.status_code == 400
        ops.set_name.assert_not_called()
        ops.set_two_node_arbitration.assert_not_called()

    def test_a_rename_alone_does_not_touch_the_leader_site(self, client, src, ops):
        response = client.put(URL, json={'name': 'renamed'})

        assert response.status_code == 204
        src.check_leader_site.assert_not_called()
        src.set_leader_site.assert_not_called()
