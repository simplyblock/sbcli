# coding=utf-8
"""Unit tests for /api/v2/clusters/{id}/alerts.

The endpoint itself is thin -- the rules live in
``simplyblock_core.controllers.alerts_controller`` and are tested against
hand-built cluster states in ``tests/unit/test_alerts_controller.py``. What is
pinned here is the HTTP contract: scoping, the shape a consumer parses, and
the two query filters.
"""

from unittest.mock import patch

import pytest

from tests.unit.web.api.v2._factories import CLUSTER_ID, STORAGE_NODE_ID

BASE = f'/api/v2/clusters/{CLUSTER_ID}/alerts'


def _alert(alert_id='node_offline:' + STORAGE_NODE_ID, kind='node_offline',
           severity='critical', status='firing', **extra):
    alert = {
        'id': alert_id,
        'kind': kind,
        'severity': severity,
        'status': status,
        'message': 'node worker-3 offline',
        'cluster_id': CLUSTER_ID,
        'node_id': STORAGE_NODE_ID,
        'device_id': '',
        'since': '2026-09-12 11:50:00+00:00',
        'first_seen': '2026-09-12 11:50:00+00:00',
        'details': {'status': 'offline'},
    }
    alert.update(extra)
    return alert


@pytest.fixture()
def alerts():
    with patch('simplyblock_core.controllers.alerts_controller.get_alerts') as m:
        m.return_value = [_alert()]
        yield m


class TestListAlerts:

    def test_returns_the_clusters_alerts(self, client, db, cluster, alerts):
        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        (body,) = response.json()
        assert body['id'] == 'node_offline:' + STORAGE_NODE_ID
        assert body['kind'] == 'node_offline'
        assert body['severity'] == 'critical'
        assert body['status'] == 'firing'
        assert body['message'] == 'node worker-3 offline'
        assert body['cluster_id'] == CLUSTER_ID
        assert body['node_id'] == STORAGE_NODE_ID
        assert body['device_id'] is None
        assert body['details'] == {'status': 'offline'}
        alerts.assert_called_once_with(
            CLUSTER_ID, include_history=False, history_seconds=None)

    def test_a_healthy_cluster_returns_an_empty_list(self, client, db, cluster, alerts):
        alerts.return_value = []

        response = client.get(f'{BASE}/')

        assert response.status_code == 200
        assert response.json() == []

    def test_a_resolved_alert_is_returned_with_its_end_time(self, client, db,
                                                            cluster, alerts):
        """The resolution has to be observable by the same poll loop that saw
        the alert start, so it is part of the feed rather than an absence."""
        alerts.return_value = [_alert(status='resolved',
                                      resolved_at='2026-09-12 12:00:00+00:00')]

        response = client.get(f'{BASE}/')

        (body,) = response.json()
        assert body['status'] == 'resolved'
        assert body['resolved_at'] == '2026-09-12 12:00:00+00:00'

    def test_a_device_alert_carries_the_device(self, client, db, cluster, alerts):
        from tests.unit.web.api.v2._factories import DEVICE_ID
        alerts.return_value = [_alert(
            alert_id='device_unavailable:' + DEVICE_ID,
            kind='device_unavailable', device_id=DEVICE_ID)]

        response = client.get(f'{BASE}/')

        (body,) = response.json()
        assert body['device_id'] == DEVICE_ID
        assert body['node_id'] == STORAGE_NODE_ID

    def test_a_cluster_wide_alert_has_no_node(self, client, db, cluster, alerts):
        alerts.return_value = [_alert(alert_id='cluster_suspended:' + CLUSTER_ID,
                                      kind='cluster_suspended', node_id='')]

        response = client.get(f'{BASE}/')

        (body,) = response.json()
        assert body['node_id'] is None
        assert body['cluster_id'] == CLUSTER_ID


class TestFilters:

    def test_severity_filter(self, client, db, cluster, alerts):
        alerts.return_value = [
            _alert(),
            _alert(alert_id='cluster_capacity_critical:' + CLUSTER_ID,
                   kind='cluster_capacity_critical', severity='warning',
                   node_id=''),
        ]

        response = client.get(f'{BASE}/?severity=warning')

        assert [b['kind'] for b in response.json()] == ['cluster_capacity_critical']

    def test_status_filter(self, client, db, cluster, alerts):
        alerts.return_value = [
            _alert(),
            _alert(alert_id='node_down:' + STORAGE_NODE_ID, kind='node_down',
                   status='resolved', resolved_at='2026-09-12 12:00:00+00:00'),
        ]

        response = client.get(f'{BASE}/?status=resolved')

        assert [b['kind'] for b in response.json()] == ['node_down']

    def test_no_filter_returns_both_states(self, client, db, cluster, alerts):
        alerts.return_value = [
            _alert(),
            _alert(alert_id='node_down:' + STORAGE_NODE_ID, kind='node_down',
                   status='resolved', resolved_at='2026-09-12 12:00:00+00:00'),
        ]

        response = client.get(f'{BASE}/')

        assert len(response.json()) == 2

    def test_an_unknown_severity_is_rejected(self, client, db, cluster, alerts):
        response = client.get(f'{BASE}/?severity=nonsense')

        assert response.status_code == 422


class TestScoping:

    def test_unknown_cluster_returns_404(self, client, db, alerts):
        db.get_cluster_by_id.side_effect = KeyError('Cluster not found')

        response = client.get(
            '/api/v2/clusters/11111111-1111-1111-1111-111111111112/alerts/')

        assert response.status_code == 404
        alerts.assert_not_called()

    def test_the_path_is_scoped_to_the_cluster(self, client, db, cluster, alerts):
        client.get(f'{BASE}/')

        alerts.assert_called_once_with(
            CLUSTER_ID, include_history=False, history_seconds=None)


class TestHistory:
    """Active-only by default. A feed that also carries what is already over
    makes "what is wrong" something the caller has to compute."""

    def test_history_is_off_by_default(self, client, db, cluster, alerts):
        client.get(f'{BASE}/')

        assert alerts.call_args.kwargs['include_history'] is False
        assert alerts.call_args.kwargs['history_seconds'] is None

    def test_history_can_be_requested(self, client, db, cluster, alerts):
        client.get(f'{BASE}/?history=true')

        assert alerts.call_args.kwargs['include_history'] is True

    def test_history_can_be_bounded_by_a_duration(self, client, db, cluster, alerts):
        client.get(f'{BASE}/?history_seconds=3600')

        assert alerts.call_args.kwargs['history_seconds'] == 3600

    def test_a_duration_alone_implies_history(self, client, db, cluster, alerts):
        """history_seconds is meaningless without history, so asking for one
        is asking for the other."""
        alerts.return_value = [
            _alert(),
            _alert(alert_id='node_down:' + STORAGE_NODE_ID, kind='node_down',
                   status='resolved', resolved_at='2026-09-12 12:00:00+00:00'),
        ]

        response = client.get(f'{BASE}/?history_seconds=3600')

        assert response.status_code == 200
        assert len(response.json()) == 2

    def test_a_zero_duration_is_rejected(self, client, db, cluster, alerts):
        assert client.get(f'{BASE}/?history_seconds=0').status_code == 422

    def test_a_negative_duration_is_rejected(self, client, db, cluster, alerts):
        assert client.get(f'{BASE}/?history_seconds=-5').status_code == 422
