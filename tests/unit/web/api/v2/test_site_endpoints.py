"""The v2 site routes of a sync-replication cluster
(``/clusters/{id}/sites/{site}/fence|unfence|status``, design-stretched
§10.2) and the reachability read (``/clusters/{id}/reachability``, §5.4).

The routes run in their own FastAPI app: the cluster lookup is overridden
with an in-memory model, and the core functions are mocked at the router
modules, so no database API is touched - these tests pin the HTTP contract
(status codes, bodies, which core function is called with what). The
behavior against the real database is in
tests/integration/test_sync_replication_site_fence.py and
tests/integration/test_node_reachability.py.
"""
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2.cluster as cluster_module
import simplyblock_web.api.v2.cluster.site as site_module
from simplyblock_core.controllers.health_controller import NodeReachability
from simplyblock_core.controllers.sync_replication_controller import (
    SiteFenceResult,
    SiteNode,
    SiteRecord,
)
from simplyblock_core.exceptions import (
    ReachabilityAddressError,
    SyncReplicationSiteError,
    SyncReplicationUnsupportedError,
    SyncSiteFenceRefusedError,
)
from simplyblock_web.api.v2._dtos import TaskDTO
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID, TASK_ID

SITE = 'site-a'
SITE_URL = f'/api/v2/clusters/{CLUSTER_ID}/sites/{SITE}/'
REACHABILITY_URL = f'/api/v2/clusters/{CLUSTER_ID}/reachability'
NODE_ID = '44444444-4444-4444-4444-444444444444'


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
def sync_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(site_module, 'sync_replication_controller', mock)
    return mock


@pytest.fixture()
def health_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(cluster_module, 'health_controller', mock)
    return mock


def _record(lost=True, state='done', fence_task_id=TASK_ID):
    return SiteRecord(site=SITE, lost=lost, state=state, fence_task_id=fence_task_id,
                      nodes=(SiteNode(NODE_ID, 'offline', '10.0.0.10'),))


def _detail(response):
    return response.json()['detail']


# ---------------------------------------------------------------------------
# fence
# ---------------------------------------------------------------------------

class TestFence:

    def test_in_progress_is_202_with_the_task(self, client, sync_controller):
        sync_controller.fence_site.return_value = SiteFenceResult(done=False, task_id=TASK_ID)

        response = client.post(SITE_URL + 'fence')

        assert response.status_code == 202
        assert response.json() == {'task_id': TASK_ID}
        assert response.headers['Location'] == f'/api/v2/clusters/{CLUSTER_ID}/tasks/{TASK_ID}/'
        sync_controller.fence_site.assert_called_once_with(CLUSTER_ID, SITE)

    def test_done_is_200_with_the_site_record(self, client, sync_controller):
        sync_controller.fence_site.return_value = SiteFenceResult(done=True)
        sync_controller.site_record.return_value = _record()

        response = client.post(SITE_URL + 'fence')

        assert response.status_code == 200
        assert response.json() == {
            'site': SITE, 'lost': True, 'state': 'done', 'fence_task_id': TASK_ID,
            'nodes': [{'id': NODE_ID, 'status': 'offline', 'mgmt_ip': '10.0.0.10'}]}
        sync_controller.site_record.assert_called_once_with(CLUSTER_ID, SITE)

    def test_a_refusal_is_409(self, client, sync_controller):
        sync_controller.fence_site.side_effect = SyncSiteFenceRefusedError(
            f'site {SITE} is not lost: nodes of it may still run: [n1 (online)]')

        response = client.post(SITE_URL + 'fence')

        assert response.status_code == 409
        assert _detail(response) == {
            'message': f'site {SITE} is not lost: nodes of it may still run: [n1 (online)]',
            'task_id': None}

    def test_a_failed_fence_is_409_with_its_task(self, client, sync_controller):
        sync_controller.fence_site.side_effect = SyncSiteFenceRefusedError(
            f'the last fence of site {SITE} failed', task_id=TASK_ID)

        response = client.post(SITE_URL + 'fence')

        assert response.status_code == 409
        assert _detail(response)['task_id'] == TASK_ID

    @pytest.mark.parametrize('error', [
        SyncReplicationSiteError("site 'site-x' is not a site of cluster c"),
        SyncReplicationUnsupportedError('cluster c is not a sync-replication cluster'),
    ], ids=['unknown-site', 'not-a-sync-cluster'])
    def test_a_bad_site_or_cluster_is_400(self, client, sync_controller, error):
        sync_controller.fence_site.side_effect = error

        response = client.post(SITE_URL + 'fence')

        assert response.status_code == 400
        assert _detail(response)['message'] == str(error)


# ---------------------------------------------------------------------------
# unfence and status
# ---------------------------------------------------------------------------

class TestUnfenceAndStatus:

    def test_unfence_confirmed_is_200_with_the_record(self, client, sync_controller):
        sync_controller.confirm_site_unfenced.return_value = _record(lost=False, state='')

        response = client.post(SITE_URL + 'unfence')

        assert response.status_code == 200
        assert (response.json()['lost'], response.json()['state']) == (False, '')
        sync_controller.confirm_site_unfenced.assert_called_once_with(CLUSTER_ID, SITE)

    def test_unfence_while_recorded_is_409(self, client, sync_controller):
        sync_controller.confirm_site_unfenced.side_effect = SyncSiteFenceRefusedError(
            f'the control plane still records site {SITE} as lost (done)')

        response = client.post(SITE_URL + 'unfence')

        assert response.status_code == 409
        assert 'still records' in _detail(response)['message']

    def test_status(self, client, sync_controller):
        sync_controller.site_record.return_value = _record(state='down', fence_task_id='')

        response = client.get(SITE_URL + 'status')

        assert response.status_code == 200
        assert response.json()['state'] == 'down'
        assert response.json()['fence_task_id'] is None
        sync_controller.site_record.assert_called_once_with(CLUSTER_ID, SITE)

    def test_status_of_an_unknown_site_is_400(self, client, sync_controller):
        sync_controller.site_record.side_effect = SyncReplicationSiteError("site 'site-x' is not a site")

        assert client.get(SITE_URL + 'status').status_code == 400


# ---------------------------------------------------------------------------
# reachability
# ---------------------------------------------------------------------------

class TestReachability:

    def test_the_nodes_of_a_site(self, client, health_controller):
        health_controller.cluster_reachability.return_value = [
            NodeReachability(NODE_ID, '10.0.0.10', SITE, True),
            NodeReachability('n2', '10.0.0.11', SITE, False)]

        response = client.get(REACHABILITY_URL + f'?site={SITE}')

        assert response.status_code == 200
        assert response.json() == [
            {'id': NODE_ID, 'mgmt_ip': '10.0.0.10', 'site': SITE, 'reachable': True},
            {'id': 'n2', 'mgmt_ip': '10.0.0.11', 'site': SITE, 'reachable': False}]
        health_controller.cluster_reachability.assert_called_once_with(CLUSTER_ID, SITE)

    @pytest.mark.parametrize('query', ['', '?site='])
    def test_without_a_site_every_node(self, client, health_controller, query):
        health_controller.cluster_reachability.return_value = []

        assert client.get(REACHABILITY_URL + query).status_code == 200
        health_controller.cluster_reachability.assert_called_once_with(CLUSTER_ID, None)

    @pytest.mark.parametrize('error', [
        SyncReplicationSiteError("site 'site-x' is not a site of cluster c"),
        SyncReplicationUnsupportedError('cluster c is not a sync-replication cluster; it has no sites'),
    ], ids=['unknown-site', 'not-a-sync-cluster'])
    def test_a_bad_site_is_400(self, client, health_controller, error):
        health_controller.cluster_reachability.side_effect = error

        response = client.get(REACHABILITY_URL + '?site=site-x')

        assert response.status_code == 400
        assert _detail(response)['message'] == str(error)


class TestReachabilityOfAddresses:
    """``?address=`` (repeatable): probe exactly those management addresses -
    workers that are not storage nodes yet (the follower's preflight, §5.4)."""

    def test_probes_the_addresses_in_request_order(self, client, health_controller):
        health_controller.address_reachability.return_value = [
            NodeReachability('', '192.0.2.2', '', True),
            NodeReachability('', '192.0.2.1', '', False)]

        response = client.get(REACHABILITY_URL + '?address=192.0.2.2&address=192.0.2.1')

        assert response.status_code == 200
        assert response.json() == [
            {'id': '', 'mgmt_ip': '192.0.2.2', 'site': '', 'reachable': True},
            {'id': '', 'mgmt_ip': '192.0.2.1', 'site': '', 'reachable': False}]
        health_controller.address_reachability.assert_called_once_with(['192.0.2.2', '192.0.2.1'])
        health_controller.cluster_reachability.assert_not_called()

    def test_site_and_address_together_are_400(self, client, health_controller):
        response = client.get(REACHABILITY_URL + f'?site={SITE}&address=192.0.2.1')

        assert response.status_code == 400
        assert 'site' in _detail(response)['message'] and 'address' in _detail(response)['message']
        health_controller.address_reachability.assert_not_called()
        health_controller.cluster_reachability.assert_not_called()

    def test_a_refused_address_is_400(self, client, health_controller):
        error = ReachabilityAddressError("'node-1' is not an IP address literal")
        health_controller.address_reachability.side_effect = error

        response = client.get(REACHABILITY_URL + '?address=node-1')

        assert response.status_code == 400
        assert _detail(response) == {'message': str(error)}

    def test_without_an_address_the_nodes_are_probed(self, client, health_controller):
        health_controller.cluster_reachability.return_value = []

        assert client.get(REACHABILITY_URL).status_code == 200
        health_controller.address_reachability.assert_not_called()


def test_a_fence_task_is_a_valid_task_dto():
    task = factories.make_task(function_name='sync_site_fence', node_id='',
                               function_params={'site': SITE, 'lvol_ids': []})

    assert TaskDTO.from_model(task).function_name == 'sync_site_fence'
