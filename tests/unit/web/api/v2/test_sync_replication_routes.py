"""The v2 replication routes on a sync-replication cluster, and unchanged on an
async one.

The routes run in their own FastAPI app: the resource lookups (cluster, pool,
volume, consistency group) are overridden with in-memory models, so no
database API is touched on any tested path, and the controllers are mocked at
the router modules - these tests pin the HTTP contract (status codes, the
``site`` parameter, DTO field names and values, which controller is called
with what). The controller guards and the group status are tested against the
real database in tests/integration/test_sync_replication_api.py.
"""
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
import simplyblock_web.api.v2._dependencies as dependencies_module
import simplyblock_web.api.v2.cluster.consistency_group as consistency_group_module
import simplyblock_web.api.v2.cluster.replication as replication_module
import simplyblock_web.api.v2.cluster.storage_pool.volume as volume_module
import simplyblock_web.api.v2.cluster.storage_pool.volume.replication as volume_replication_module
from simplyblock_core.controllers.sync_replication_controller import (
    ClusterSyncStatus, GroupSyncStatus, SyncPromoteResult, VolumeSyncStatus,
)
from simplyblock_core.exceptions import (
    SyncAnaError, SyncGateError, SyncGroupMemberError, SyncPromoteFailedError, SyncPromoteRefusedError,
    SyncReplicationSiteError, SyncReplicationUnsupportedError, SyncSiteOfflineError,
)
from simplyblock_core.utils.nvme import NvmeConnectEntry

from tests.unit.web.api.v2 import _factories as factories
from tests.unit.web.api.v2._factories import (
    CLUSTER_ID, CONSISTENCY_GROUP_ID, POOL_ID, REPLICATION_POLICY_ID, TASK_ID, VOLUME_ID,
)

SITE = 'site-b'
OTHER_VOLUME_ID = '33333333-3333-3333-3333-333333333334'
VOLUME_URL = f'/api/v2/clusters/{CLUSTER_ID}/storage-pools/{POOL_ID}/volumes/{VOLUME_ID}/'
REPLICATION_URL = VOLUME_URL + 'replication/'
GROUP_URL = f'/api/v2/clusters/{CLUSTER_ID}/consistency-groups/{CONSISTENCY_GROUP_ID}/'
RELATIONSHIP_URL = f'/api/v2/clusters/{CLUSTER_ID}/replication/relationships/{VOLUME_ID}'
#: ConsistencyGroup.get_id(): the ``cluster_id/uuid`` composite the routes pass on.
GROUP_ID = f'{CLUSTER_ID}/{CONSISTENCY_GROUP_ID}'
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# the app and its fixtures
# ---------------------------------------------------------------------------

def _production_runtime_error_handler():
    """The production app's RuntimeError handler (a failed ANA RPC answers
    through it). Importing the app module sets the root logger's level; keep
    the test session's."""
    root = logging.getLogger()
    level = root.level
    try:
        from simplyblock_web.app import runtime_error_handler
    finally:
        root.setLevel(level)
    return runtime_error_handler


class _Resources:
    """The models the overridden lookups answer with."""

    def __init__(self):
        self.cluster = factories.make_cluster(sync_replication=True)
        self.pool = factories.make_pool()
        self.volume = factories.make_volume()
        self.group = factories.make_consistency_group()


@pytest.fixture()
def resources():
    return _Resources()


@pytest.fixture()
def client(resources):
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.add_exception_handler(RuntimeError, _production_runtime_error_handler())
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    app.dependency_overrides[dependencies_module._lookup_cluster] = lambda: resources.cluster
    app.dependency_overrides[dependencies_module._lookup_storage_pool] = lambda: resources.pool
    app.dependency_overrides[dependencies_module._lookup_volume] = lambda: resources.volume
    app.dependency_overrides[dependencies_module._lookup_consistency_group] = lambda: resources.group
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def async_cluster(resources):
    resources.cluster.sync_replication = False
    return resources.cluster


@pytest.fixture()
def sync_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(volume_replication_module, 'sync_replication_controller', mock)
    monkeypatch.setattr(consistency_group_module, 'sync_replication_controller', mock)
    return mock


@pytest.fixture()
def lvol_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(volume_module, 'lvol_controller', mock)
    monkeypatch.setattr(volume_replication_module, 'lvol_controller', mock)
    monkeypatch.setattr(consistency_group_module, 'lvol_controller', mock)
    return mock


@pytest.fixture()
def policy_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(volume_replication_module, 'replication_policy_controller', mock)
    monkeypatch.setattr(replication_module, 'replication_policy_controller', mock)
    monkeypatch.setattr(consistency_group_module, 'replication_policy_controller', mock)
    return mock


@pytest.fixture()
def group_controller(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(consistency_group_module, 'consistency_group_controller', mock)
    return mock


@pytest.fixture(autouse=True)
def controllers(sync_controller, lvol_controller, policy_controller, group_controller):
    """Every controller the routes may reach is a mock: no test can fall
    through to a real one (and to the database)."""


def _cluster_status(state='healthy', **overrides):
    fields = dict(
        state=state, degraded=state == 'degraded', resyncing=state == 'resyncing', completed=True,
        peer_ready=state == 'healthy', diverged=state != 'healthy',
        last_replicated_at=NOW if state == 'healthy' else NOW - timedelta(seconds=90),
        lag_seconds=0 if state == 'healthy' else 90, bytes_behind=0 if state == 'healthy' else 3 * 4096,
        computed_at=NOW, lvs=())
    fields.update(overrides)
    return ClusterSyncStatus(**fields)


def _entry(ip='10.0.1.10'):
    return NvmeConnectEntry(
        transport='tcp', ip=ip, port=4420, nqn=factories.VOLUME_NQN, reconnect_delay=2, ctrl_loss_tmo=60,
        fast_io_fail_tmo=10, nr_io_queues=4, keep_alive_tmo=5, connect=f'nvme connect -a {ip}', ns_id=1)


def _detail(response):
    return response.json()['detail']


# ---------------------------------------------------------------------------
# site parameter
# ---------------------------------------------------------------------------

class TestSiteRequired:

    @pytest.mark.parametrize('method, path', [
        ('post', 'failover'), ('post', 'demote'), ('get', 'status'), ('get', 'sync-status')])
    @pytest.mark.parametrize('query', ['', '?site='])
    def test_volume_route_without_site_is_400(self, client, sync_controller, method, path, query):
        response = getattr(client, method)(REPLICATION_URL + path + query)

        assert response.status_code == 400
        assert 'site is required' in _detail(response)['message']
        assert sync_controller.method_calls == []

    def test_connect_without_site_is_400(self, client, lvol_controller):
        response = client.get(VOLUME_URL + 'connect')

        assert response.status_code == 400
        lvol_controller.connect_lvol.assert_not_called()

    @pytest.mark.parametrize('method, path', [
        ('post', 'replication/failover'), ('post', 'replication/demote'), ('get', 'replication/status'),
        ('get', 'replication/sync-status')])
    def test_group_route_without_site_is_400(self, client, sync_controller, method, path):
        response = getattr(client, method)(GROUP_URL + path)

        assert response.status_code == 400
        assert sync_controller.method_calls == []

    def test_unknown_site_is_400(self, client, sync_controller):
        sync_controller.sync_demote_lvol.side_effect = SyncReplicationSiteError("site 'x' is not a site")

        response = client.post(REPLICATION_URL + 'demote?site=x')

        assert response.status_code == 400
        assert _detail(response) == {'message': "site 'x' is not a site"}


# ---------------------------------------------------------------------------
# volume promote (failover)
# ---------------------------------------------------------------------------

class TestVolumePromote:

    def test_done_is_200_with_the_site_connection_strings(self, client, sync_controller):
        sync_controller.sync_promote_lvol.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={VOLUME_ID: [_entry()]})

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 200
        sync_controller.sync_promote_lvol.assert_called_once_with(VOLUME_ID, SITE, force=False)
        body = response.json()
        assert body['lvol_id'] == VOLUME_ID
        [entry] = body['connection_strings']
        assert entry['ip'] == '10.0.1.10'
        assert entry['reconnect-delay'] == 2
        assert entry['ctrl-loss-tmo'] == 60
        assert entry['connect'] == 'nvme connect -a 10.0.1.10'

    @pytest.mark.parametrize('query, force', [('', True), ('&planned=false', True), ('&planned=true', False)])
    def test_force_is_not_planned(self, client, sync_controller, query, force):
        sync_controller.sync_promote_lvol.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={VOLUME_ID: []})

        client.post(REPLICATION_URL + f'failover?site={SITE}{query}')

        sync_controller.sync_promote_lvol.assert_called_once_with(VOLUME_ID, SITE, force=force)

    def test_in_progress_is_409_with_the_task(self, client, sync_controller):
        sync_controller.sync_promote_lvol.return_value = SyncPromoteResult(in_progress=True, task_id=TASK_ID)

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 409
        assert _detail(response) == {'message': 'promote in progress; call again', 'task_id': TASK_ID}

    def test_gate_is_409_with_the_problems(self, client, sync_controller):
        sync_controller.sync_promote_lvol.side_effect = SyncGateError(
            'sync-replication planned', ['LVS_1: distrib_11 on n1: replica_unsynced'])

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 409
        detail = _detail(response)
        assert detail['problems'] == ['LVS_1: distrib_11 on n1: replica_unsynced']
        assert detail['message'].startswith('sync-replication planned gate failed')

    @pytest.mark.parametrize('error', [
        SyncPromoteRefusedError('volume(s) not demoted on site site-a', [VOLUME_ID]),
        SyncPromoteRefusedError('other volumes of the LVS are still active on site site-a',
                                [OTHER_VOLUME_ID]),
    ])
    def test_refused_is_409_with_the_volumes(self, client, sync_controller, error):
        sync_controller.sync_promote_lvol.side_effect = error

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 409
        assert _detail(response) == {'message': str(error), 'volumes': error.volumes}

    def test_a_failed_promote_is_409_with_its_task_and_volumes(self, client, sync_controller):
        sync_controller.sync_promote_lvol.side_effect = SyncPromoteFailedError(
            'the last promote to site site-b failed: task t1: failed: ANA refused', [VOLUME_ID], 't1')

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 409
        assert _detail(response) == {
            'message': 'the last promote to site site-b failed: task t1: failed: ANA refused',
            'volumes': [VOLUME_ID], 'task_id': 't1'}

    def test_site_offline_is_412(self, client, sync_controller):
        sync_controller.sync_promote_lvol.side_effect = SyncSiteOfflineError('site site-a is not online')

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 412
        assert _detail(response) == {'message': 'site site-a is not online'}

    def test_ana_failure_is_500(self, client, sync_controller):
        sync_controller.sync_promote_lvol.side_effect = SyncAnaError('ANA RPC failed on n4')

        response = client.post(REPLICATION_URL + f'failover?site={SITE}&planned=true')

        assert response.status_code == 500
        assert response.json()['detail'] == 'ANA RPC failed on n4'

    def test_a_generation_is_400(self, client, sync_controller):
        response = client.post(REPLICATION_URL + f'failover?site={SITE}&generation=1')

        assert response.status_code == 400
        sync_controller.sync_promote_lvol.assert_not_called()

    def test_the_async_fail_over_is_not_used(self, client, sync_controller, lvol_controller):
        sync_controller.sync_promote_lvol.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={VOLUME_ID: []})

        client.post(REPLICATION_URL + f'failover?site={SITE}')

        lvol_controller.replicate_lvol_on_target_cluster.assert_not_called()


# ---------------------------------------------------------------------------
# volume demote, status, no-ops
# ---------------------------------------------------------------------------

class TestVolumeDemote:

    @pytest.mark.parametrize('demoted', [[VOLUME_ID], []])
    def test_is_204(self, client, sync_controller, lvol_controller, demoted):
        sync_controller.sync_demote_lvol.return_value = demoted

        response = client.post(REPLICATION_URL + f'demote?site={SITE}')

        assert response.status_code == 204
        sync_controller.sync_demote_lvol.assert_called_once_with(VOLUME_ID, SITE)
        lvol_controller.demote_lvol.assert_not_called()

    def test_gate_is_409(self, client, sync_controller):
        sync_controller.sync_demote_lvol.side_effect = SyncGateError('sync-replication planned', ['x'])

        response = client.post(REPLICATION_URL + f'demote?site={SITE}')

        assert response.status_code == 409
        assert _detail(response)['problems'] == ['x']


class TestVolumeStatus:

    @pytest.mark.parametrize('role, dto_role', [('primary', 'source'), ('secondary', 'secondary')])
    def test_healthy_fills_the_async_dto(self, client, sync_controller, resources, role, dto_role):
        sync_controller.volume_sync_status.return_value = VolumeSyncStatus(
            role=role, site=SITE, cluster=_cluster_status())

        response = client.get(REPLICATION_URL + f'status?site={SITE}')

        assert response.status_code == 200
        args, kwargs = sync_controller.volume_sync_status.call_args
        assert args == (resources.volume, SITE)
        assert kwargs == {'max_age': volume_replication_module.constants.SYNC_STATUS_CACHE_SEC}
        body = response.json()
        assert body['role'] == dto_role
        assert body['state'] == 'in_sync'
        assert body['resyncing'] is False
        assert body['lag_seconds'] == 0
        assert body['outstanding_bytes'] == 0
        assert datetime.fromisoformat(body['last_replicated_at']) == NOW

    @pytest.mark.parametrize('state, resyncing', [('degraded', False), ('resyncing', True)])
    def test_not_healthy_is_degraded(self, client, sync_controller, state, resyncing):
        sync_controller.volume_sync_status.return_value = VolumeSyncStatus(
            role='primary', site=SITE, cluster=_cluster_status(state))

        body = client.get(REPLICATION_URL + f'status?site={SITE}').json()

        assert body['state'] == 'degraded'
        assert body['resyncing'] is resyncing
        assert body['lag_seconds'] == 90
        assert body['outstanding_bytes'] == 3 * 4096
        assert datetime.fromisoformat(body['last_replicated_at']) == NOW - timedelta(seconds=90)

    def test_unknown_last_in_sync_time_is_null(self, client, sync_controller):
        sync_controller.volume_sync_status.return_value = VolumeSyncStatus(
            role='primary', site=SITE, cluster=_cluster_status('degraded', last_replicated_at=None,
                                                               lag_seconds=None))

        body = client.get(REPLICATION_URL + f'status?site={SITE}').json()

        assert body['last_replicated_at'] is None
        assert body['lag_seconds'] is None


class TestVolumeSyncStatus:

    def test_fields(self, client, sync_controller):
        sync_controller.volume_sync_status.return_value = VolumeSyncStatus(
            role='secondary', site=SITE, cluster=_cluster_status('resyncing', completed=True))

        response = client.get(REPLICATION_URL + f'sync-status?site={SITE}')

        assert response.status_code == 200
        body = response.json()
        assert set(body) == {'site', 'role', 'state', 'last_replicated_at', 'lag_seconds', 'bytes_behind',
                             'diverged', 'completed', 'degraded', 'resyncing', 'peer_ready'}
        assert body['site'] == SITE
        assert body['role'] == 'secondary'
        assert body['state'] == 'resyncing'
        assert body['lag_seconds'] == 90
        assert body['bytes_behind'] == 3 * 4096
        assert (body['diverged'], body['completed'], body['degraded'], body['resyncing'],
                body['peer_ready']) == (True, True, False, True, False)

    def test_healthy_is_peer_ready(self, client, sync_controller):
        sync_controller.volume_sync_status.return_value = VolumeSyncStatus(
            role='primary', site=SITE, cluster=_cluster_status())

        body = client.get(REPLICATION_URL + f'sync-status?site={SITE}').json()

        assert (body['state'], body['peer_ready'], body['diverged'], body['bytes_behind']) == \
            ('healthy', True, False, 0)

    def test_non_sync_cluster_is_400(self, client, sync_controller, async_cluster):
        response = client.get(REPLICATION_URL + f'sync-status?site={SITE}')

        assert response.status_code == 400
        assert 'not a sync-replication cluster' in _detail(response)['message']
        sync_controller.volume_sync_status.assert_not_called()


class TestVolumeNoOps:

    @pytest.mark.parametrize('body', [{}, {'source_cluster_id': CLUSTER_ID}])
    def test_failback_is_204(self, client, lvol_controller, body):
        response = client.post(REPLICATION_URL + 'failback', json=body)

        assert response.status_code == 204
        lvol_controller.replication_failback.assert_not_called()

    def test_failback_still_needs_a_body(self, client, lvol_controller):
        assert client.post(REPLICATION_URL + 'failback').status_code == 422

    @pytest.mark.parametrize('policy', [REPLICATION_POLICY_ID, None])
    def test_policy_update_is_a_no_op(self, client, policy_controller, policy):
        response = client.put(VOLUME_URL, json={'replication_policy_id': policy})

        assert response.status_code == 204
        assert policy_controller.method_calls == []

    def test_policy_update_keeps_the_other_attributes(self, client, lvol_controller, policy_controller):
        response = client.put(VOLUME_URL, json={
            'name': 'renamed', 'size': '20G', 'replication_policy_id': REPLICATION_POLICY_ID})

        assert response.status_code == 204
        lvol_controller.set_lvol.assert_called_once()
        assert lvol_controller.set_lvol.call_args.kwargs['name'] == 'renamed'
        lvol_controller.resize_lvol.assert_called_once()
        assert policy_controller.method_calls == []

    def test_relationship_is_404(self, client, policy_controller):
        response = client.get(REPLICATION_URL)

        assert response.status_code == 404
        policy_controller.get_relationship.assert_not_called()

    def test_cluster_relationship_is_404(self, client, policy_controller):
        response = client.get(RELATIONSHIP_URL)

        assert response.status_code == 404
        policy_controller.get_relationship.assert_not_called()


class TestAsyncOperationsRefused:
    """The controllers refuse them on a sync cluster (tested against the real
    database); the routes answer that refusal with a 400."""

    @pytest.mark.parametrize('path, controller, method', [
        ('start', 'lvol', 'replication_start'),
        ('stop', 'lvol', 'replication_stop'),
        ('trigger', 'lvol', 'replication_trigger'),
        ('commit', 'lvol', 'replication_commit'),
        ('cutover-proceed', 'policy', 'set_cutover_proceed'),
    ])
    def test_is_400(self, client, lvol_controller, policy_controller, path, controller, method):
        mock = lvol_controller if controller == 'lvol' else policy_controller
        getattr(mock, method).side_effect = SyncReplicationUnsupportedError(
            'Replication x is not supported on a sync-replication cluster')

        response = client.post(REPLICATION_URL + path)

        assert response.status_code == 400
        assert 'sync-replication' in _detail(response)['message']


class TestConnect:

    def test_sync_forwards_the_site(self, client, lvol_controller):
        lvol_controller.connect_lvol.return_value = ([_entry()], None)

        response = client.get(VOLUME_URL + f'connect?site={SITE}&host_nqn=nqn.host')

        assert response.status_code == 200
        lvol_controller.connect_lvol.assert_called_once_with(VOLUME_ID, host_nqn='nqn.host', site=SITE)
        assert response.json()[0]['reconnect-delay'] == 2

    def test_sync_unknown_site_is_400(self, client, lvol_controller):
        lvol_controller.connect_lvol.side_effect = SyncReplicationSiteError("site 'x' is not a site")

        assert client.get(VOLUME_URL + 'connect?site=x').status_code == 400

    @pytest.mark.parametrize('query', ['', f'?site={SITE}'])
    def test_async_ignores_the_site(self, client, lvol_controller, async_cluster, query):
        lvol_controller.connect_lvol.return_value = ([_entry()], None)

        response = client.get(VOLUME_URL + 'connect' + query)

        assert response.status_code == 200
        lvol_controller.connect_lvol.assert_called_once_with(VOLUME_ID, host_nqn=None)


# ---------------------------------------------------------------------------
# consistency groups
# ---------------------------------------------------------------------------

class TestGroup:

    def test_promote_done_is_200_with_every_member(self, client, sync_controller):
        sync_controller.sync_promote_group.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={VOLUME_ID: [_entry()], OTHER_VOLUME_ID: [_entry('10.0.1.11')]})

        response = client.post(GROUP_URL + f'replication/failover?site={SITE}&planned=true')

        assert response.status_code == 200
        sync_controller.sync_promote_group.assert_called_once_with(GROUP_ID, SITE, force=False)
        members = response.json()['members']
        assert [(m['lvol_id'], m['connection_strings'][0]['ip']) for m in members] == [
            (VOLUME_ID, '10.0.1.10'), (OTHER_VOLUME_ID, '10.0.1.11')]
        assert 'reconnect-delay' in members[0]['connection_strings'][0]

    def test_promote_forced_by_default(self, client, sync_controller):
        sync_controller.sync_promote_group.return_value = SyncPromoteResult(in_progress=True, task_id=TASK_ID)

        response = client.post(GROUP_URL + f'replication/failover?site={SITE}')

        assert response.status_code == 409
        assert _detail(response)['task_id'] == TASK_ID
        sync_controller.sync_promote_group.assert_called_once_with(GROUP_ID, SITE, force=True)

    def test_promote_does_not_need_a_policy(self, client, sync_controller, policy_controller, resources):
        resources.group.policy_id = ''
        sync_controller.sync_promote_group.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={})

        response = client.post(GROUP_URL + f'replication/failover?site={SITE}')

        assert response.status_code == 200
        assert response.json() == {'members': []}
        policy_controller.failover_group.assert_not_called()

    def test_missing_member_is_409_with_the_ids(self, client, sync_controller):
        sync_controller.sync_promote_group.side_effect = SyncGroupMemberError(
            'members not found', [OTHER_VOLUME_ID])

        response = client.post(GROUP_URL + f'replication/failover?site={SITE}')

        assert response.status_code == 409
        assert _detail(response) == {'message': 'members not found', 'volumes': [OTHER_VOLUME_ID]}

    def test_promote_site_offline_is_412(self, client, sync_controller):
        sync_controller.sync_promote_group.side_effect = SyncSiteOfflineError('site site-a is not online')

        assert client.post(GROUP_URL + f'replication/failover?site={SITE}').status_code == 412

    def test_demote_is_204(self, client, sync_controller, group_controller):
        sync_controller.sync_demote_group.return_value = [VOLUME_ID]

        response = client.post(GROUP_URL + f'replication/demote?site={SITE}')

        assert response.status_code == 204
        sync_controller.sync_demote_group.assert_called_once_with(GROUP_ID, SITE)
        group_controller.demote_group.assert_not_called()

    def test_an_empty_group_is_204_on_demote_and_200_without_members_on_promote(self, client, sync_controller):
        sync_controller.sync_demote_group.return_value = []
        sync_controller.sync_promote_group.return_value = SyncPromoteResult(
            in_progress=False, connection_strings={})

        assert client.post(GROUP_URL + f'replication/demote?site={SITE}').status_code == 204
        response = client.post(GROUP_URL + f'replication/failover?site={SITE}&planned=true')
        assert response.status_code == 200 and response.json() == {'members': []}

    def test_a_failed_group_promote_is_409_with_its_task(self, client, sync_controller):
        sync_controller.sync_promote_group.side_effect = SyncPromoteFailedError(
            'the last promote to site site-b failed', [VOLUME_ID, OTHER_VOLUME_ID], 't1')

        response = client.post(GROUP_URL + f'replication/failover?site={SITE}')

        assert response.status_code == 409
        assert _detail(response)['task_id'] == 't1'
        assert _detail(response)['volumes'] == [VOLUME_ID, OTHER_VOLUME_ID]

    def test_demote_gate_is_409(self, client, sync_controller):
        sync_controller.sync_demote_group.side_effect = SyncGateError('sync-replication planned', ['x'])

        assert client.post(GROUP_URL + f'replication/demote?site={SITE}').status_code == 409

    def test_status_fills_the_async_group_dto(self, client, sync_controller, lvol_controller):
        sync_controller.group_sync_status.return_value = GroupSyncStatus(
            role='primary', site=SITE, member_count=3, cluster=_cluster_status('resyncing'))

        response = client.get(GROUP_URL + f'replication/status?site={SITE}')

        assert response.status_code == 200
        args, kwargs = sync_controller.group_sync_status.call_args
        assert args == (GROUP_ID, SITE)
        body = response.json()
        assert (body['role'], body['state'], body['resyncing'], body['member_count'],
                body['outstanding_bytes'], body['lag_seconds']) == ('source', 'degraded', True, 3, 3 * 4096, 90)
        lvol_controller.get_replication_info.assert_not_called()

    def test_sync_status(self, client, sync_controller):
        sync_controller.group_sync_status.return_value = GroupSyncStatus(
            role='secondary', site=SITE, member_count=0, cluster=_cluster_status())

        response = client.get(GROUP_URL + f'replication/sync-status?site={SITE}')

        assert response.status_code == 200
        body = response.json()
        assert (body['site'], body['role'], body['state'], body['peer_ready']) == \
            (SITE, 'secondary', 'healthy', True)

    def test_sync_status_on_a_non_sync_cluster_is_400(self, client, sync_controller, async_cluster):
        assert client.get(GROUP_URL + f'replication/sync-status?site={SITE}').status_code == 400
        sync_controller.group_sync_status.assert_not_called()

    @pytest.mark.parametrize('body', [{}, {'source_cluster_id': CLUSTER_ID}])
    def test_failback_is_204(self, client, group_controller, body):
        response = client.post(GROUP_URL + 'replication/failback', json=body)

        assert response.status_code == 204
        group_controller.failback_group.assert_not_called()

    def test_failback_still_needs_a_body(self, client):
        assert client.post(GROUP_URL + 'replication/failback').status_code == 422

    @pytest.mark.parametrize('policy', [REPLICATION_POLICY_ID, None])
    def test_policy_is_a_no_op(self, client, group_controller, policy):
        response = client.put(GROUP_URL + 'replication', json={'replication_policy_id': policy})

        assert response.status_code == 204
        assert group_controller.method_calls == []


# ---------------------------------------------------------------------------
# an async cluster: unchanged
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures('async_cluster')
class TestAsyncClusterUnchanged:

    def test_failover_ignores_the_site(self, client, sync_controller, lvol_controller):
        lvol_controller.replicate_lvol_on_target_cluster.return_value = True

        response = client.post(REPLICATION_URL + f'failover?site={SITE}')

        assert response.status_code == 204
        lvol_controller.replicate_lvol_on_target_cluster.assert_called_once_with(VOLUME_ID, generation=0)
        assert sync_controller.method_calls == []

    def test_failover_warnings_are_still_200(self, client, lvol_controller):
        lvol_controller.replicate_lvol_on_target_cluster.return_value = {'warnings': ['member x left']}

        response = client.post(REPLICATION_URL + 'failover')

        assert response.status_code == 200
        assert response.json() == {'warnings': ['member x left']}

    def test_failover_documents_both_200_bodies(self, client):
        responses = client.app.openapi()['paths'][REPLICATION_URL.replace(VOLUME_ID, '{volume_id}')
                                                  .replace(CLUSTER_ID, '{cluster_id}')
                                                  .replace(POOL_ID, '{pool_id}') + 'failover']['post']['responses']
        assert 'SyncPromoteResultDTO' in responses['200']['description']
        assert 'warnings' in responses['200']['description']

    def test_demote_ignores_the_site(self, client, sync_controller, lvol_controller):
        lvol_controller.demote_lvol.return_value = {'demoted': True}

        response = client.post(REPLICATION_URL + f'demote?site={SITE}')

        assert response.status_code == 204
        lvol_controller.demote_lvol.assert_called_once_with(VOLUME_ID)
        assert sync_controller.method_calls == []

    def test_status_without_site(self, client, sync_controller, lvol_controller):
        lvol_controller.get_replication_info.return_value = {'role': 'source', 'state': 'in_sync'}

        response = client.get(REPLICATION_URL + 'status')

        assert response.status_code == 200
        assert (response.json()['role'], response.json()['state']) == ('source', 'in_sync')
        lvol_controller.get_replication_info.assert_called_once_with(VOLUME_ID)
        assert sync_controller.method_calls == []

    def test_failback_calls_the_controller(self, client, lvol_controller):
        lvol_controller.replication_failback.return_value = True

        assert client.post(REPLICATION_URL + 'failback', json={}).status_code == 204
        lvol_controller.replication_failback.assert_called_once_with(VOLUME_ID, source_cluster_id=None)

    def test_policy_update_attaches(self, client, policy_controller):
        assert client.put(VOLUME_URL, json={'replication_policy_id': REPLICATION_POLICY_ID}).status_code == 204
        policy_controller.attach_policy.assert_called_once_with(VOLUME_ID, REPLICATION_POLICY_ID)

    def test_relationship(self, client, policy_controller):
        policy_controller.get_relationship.return_value = None

        assert client.get(RELATIONSHIP_URL).status_code == 404
        policy_controller.get_relationship.assert_called_once_with(VOLUME_ID)

    def test_start_calls_the_controller(self, client, lvol_controller):
        lvol_controller.replication_start.return_value = True

        assert client.post(REPLICATION_URL + 'start').status_code == 204
        lvol_controller.replication_start.assert_called_once()

    def test_group_failover_needs_a_policy(self, client, sync_controller, resources):
        resources.group.policy_id = ''

        assert client.post(GROUP_URL + f'replication/failover?site={SITE}').status_code == 412
        assert sync_controller.method_calls == []

    def test_group_status(self, client, sync_controller, group_controller):
        group_controller.list_members.return_value = []
        group_controller.aggregate_group_replication_info.return_value = {
            'role': 'none', 'state': 'not_replicating'}

        response = client.get(GROUP_URL + 'replication/status')

        assert response.status_code == 200
        assert response.json()['state'] == 'not_replicating'
        assert sync_controller.method_calls == []

    def test_group_policy_attaches(self, client, group_controller, resources):
        response = client.put(GROUP_URL + 'replication', json={'replication_policy_id': REPLICATION_POLICY_ID})

        assert response.status_code == 204
        group_controller.attach_group_policy.assert_called_once_with(resources.group, REPLICATION_POLICY_ID)
