"""The stretched control plane's v2 routes (docs/stretched-control-plane.md):
status, the planned leadership move with its gates, the takeover with its
probe. The controller is mocked at the router module; these tests pin the
HTTP contract.
"""
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2.cluster.control_plane as cp_module
from simplyblock_core.controllers import control_plane_controller as cpc
from simplyblock_core.exceptions import (
    ControlPlaneError,
    ControlPlaneNotReadyError,
    ControlPlaneTakeoverRefusedError,
)
from simplyblock_core.models.control_plane_lease import ControlPlaneLease
from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import CLUSTER_ID

URL = f'/api/v2/clusters/{CLUSTER_ID}/control-plane/'


def _status(mode="leader"):
    lease = ControlPlaneLease()
    lease.uuid = lease.cluster_id = CLUSTER_ID
    lease.holder_site, lease.holder_host, lease.epoch = "a", "mgmt-a", 3
    lease.state, lease.expires_at = ControlPlaneLease.STATE_HELD, "2026-11-09T12:00:10+00:00"
    sites = [
        cpc.SiteReadiness(site="a", role="control_plane", mgmt_nodes_online=1, mgmt_nodes_total=1,
                          storage_nodes_online=3, storage_nodes_total=3, holds_lease=True),
        cpc.SiteReadiness(site="b", role="control_plane", mgmt_nodes_online=1, mgmt_nodes_total=1,
                          storage_nodes_online=2, storage_nodes_total=3, holds_lease=False,
                          fdb_lag_seconds=0.1, fdb_data_state="healthy",
                          problems=["storage nodes of site b not online: deadbeef=offline"]),
    ]
    return cpc.ControlPlaneStatus(
        cluster_id=CLUSTER_ID, stretched=True, local_site="a", mode=mode, mode_reason="holds",
        leader_site="a", lease=lease, sites=sites, fdb_lag_seconds=0.1, fdb_data_state="healthy")


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
def controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(cp_module, 'control_plane_controller', mock)
    return mock


class TestStatus:

    def test_reports_mode_lease_and_site_readiness(self, client, controller):
        controller.control_plane_status.return_value = _status()
        response = client.get(URL)
        assert response.status_code == 200
        body = response.json()
        assert body['mode'] == 'leader'
        assert body['lease']['holder_site'] == 'a'
        assert body['lease']['epoch'] == 3
        a, b = body['sites']
        assert a['failback_ready'] is True and a['problems'] == []
        assert b['failback_ready'] is False
        assert 'not online' in b['problems'][0]
        assert body['fdb_region_lag_seconds'] == 0.1


class TestLeaderSite:

    def test_a_ready_site_takes_the_lead(self, client, controller):
        controller.move_leader_site.return_value = cpc.SiteReadiness(site="b", role="control_plane")
        response = client.post(URL + 'leader-site', json={'site': 'b'})
        assert response.status_code == 200
        assert response.json()['failback_ready'] is True
        controller.move_leader_site.assert_called_once_with(CLUSTER_ID, 'b', force=False)

    def test_a_red_gate_is_409_with_the_problems(self, client, controller):
        controller.move_leader_site.side_effect = ControlPlaneNotReadyError(
            'site b is not ready', ['FoundationDB region lag 4.0s exceeds 1s'])
        response = client.post(URL + 'leader-site', json={'site': 'b'})
        assert response.status_code == 409
        assert response.json()['detail']['problems'] == ['FoundationDB region lag 4.0s exceeds 1s']

    def test_force_is_passed_through(self, client, controller):
        controller.move_leader_site.return_value = cpc.SiteReadiness(site="b", role="control_plane")
        client.post(URL + 'leader-site', json={'site': 'b', 'force': True})
        controller.move_leader_site.assert_called_once_with(CLUSTER_ID, 'b', force=True)

    def test_a_site_without_a_control_plane_is_400(self, client, controller):
        controller.move_leader_site.side_effect = ControlPlaneError("site 'c' has no control plane")
        response = client.post(URL + 'leader-site', json={'site': 'c'})
        assert response.status_code == 400
        assert 'no control plane' in response.json()['detail']['message']

    def test_an_empty_site_is_422(self, client, controller):
        assert client.post(URL + 'leader-site', json={'site': ''}).status_code == 422
        controller.move_leader_site.assert_not_called()


class TestTakeover:

    def test_a_confirmed_dead_site_is_taken_over(self, client, controller):
        controller.takeover.return_value = _status(mode="standby")
        response = client.post(URL + 'takeover', json={
            'site': 'b', 'confirm_site_down': 'a', 'operator': 'michael'})
        assert response.status_code == 200
        controller.takeover.assert_called_once_with(CLUSTER_ID, 'b', 'a', operator='michael')

    def test_a_site_that_still_answers_is_409_with_what_answered(self, client, controller):
        controller.takeover.side_effect = ControlPlaneTakeoverRefusedError(
            'site a still answers', ['management node mgmt-a (10.0.0.1) answers ping'])
        response = client.post(URL + 'takeover', json={'site': 'b', 'confirm_site_down': 'a'})
        assert response.status_code == 409
        assert response.json()['detail']['alive'] == ['management node mgmt-a (10.0.0.1) answers ping']

    def test_both_sites_are_required(self, client, controller):
        assert client.post(URL + 'takeover', json={'site': 'b'}).status_code == 422
        controller.takeover.assert_not_called()
