"""Unit tests for /api/v2/clusters/{id}/arbitration (two-node arbitration).

The arbiter's decisions are tested in tests/unit/test_two_node_arbiter.py;
pinned here is the HTTP contract: two-node scoping, member validation, the
records the endpoints write and the audit events.
"""

from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.models.arbitration import ClusterArbitration
from simplyblock_core.models.storage_node import StorageNode
from tests.unit.web.api.v2._factories import CLUSTER_ID, make_storage_node

BASE = f'/api/v2/clusters/{CLUSTER_ID}/arbitration'
NODE_A = 'aaaaaaaa-0000-0000-0000-00000000000a'
NODE_B = 'bbbbbbbb-0000-0000-0000-00000000000b'


def _node(uuid, **attrs):
    return make_storage_node(uuid=uuid, **attrs)


@pytest.fixture()
def two_nodes(db, cluster):
    nodes = [_node(NODE_B), _node(NODE_A)]
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    return nodes


@pytest.fixture()
def record(db, two_nodes):
    rec = ClusterArbitration()
    rec.cluster_id = CLUSTER_ID
    rec.preferred_node = NODE_A
    db.get_cluster_arbitration.return_value = rec
    return rec


@pytest.fixture()
def audit():
    with patch('simplyblock_core.controllers.events_controller.log_event_cluster') as m:
        yield m


def _applied(db, obj):
    """Apply the mutation the endpoint passed to atomic_update to ``obj``."""
    (target, fn), _ = db.atomic_update.call_args
    fn(obj)
    return target


class TestGet:

    def test_creates_the_record_with_the_lowest_node_id_preferred(self, client, db, two_nodes):
        db.get_cluster_arbitration.return_value = None
        with patch.object(ClusterArbitration, 'write_to_db') as write:
            response = client.get(f'{BASE}/')
        assert response.status_code == 200
        body = response.json()
        assert body['preferred_node'] == NODE_A
        assert body['fenced_taint'] == constants.TWO_NODE_FENCED_TAINT
        assert 'lease_age_ms' in body and 'enabled' in body
        write.assert_called_once()

    def test_refuses_a_cluster_that_is_not_two_nodes(self, client, db, cluster):
        db.get_cluster_arbitration.return_value = None
        db.get_storage_nodes_by_cluster_id.return_value = [_node(NODE_A)]
        assert client.get(f'{BASE}/').status_code == 409

    def test_removed_nodes_do_not_count(self, client, db, cluster):
        db.get_cluster_arbitration.return_value = None
        db.get_storage_nodes_by_cluster_id.return_value = [
            _node(NODE_A), _node(NODE_B, status=StorageNode.STATUS_REMOVED)]
        assert client.get(f'{BASE}/').status_code == 409


class TestPreferred:

    def test_sets_the_preferred_node_and_audits(self, client, db, record, audit):
        response = client.put(f'{BASE}/preferred', json={'node_id': NODE_B})
        assert response.status_code == 204
        fresh = ClusterArbitration(record.to_dict())
        _applied(db, fresh)
        assert fresh.preferred_node == NODE_B
        audit.assert_called_once()

    def test_refuses_a_non_member(self, client, db, record):
        assert client.put(f'{BASE}/preferred', json={'node_id': 'node-x'}).status_code == 422
        db.atomic_update.assert_not_called()


class TestOverride:

    def test_records_the_operator_decision(self, client, db, record, audit):
        response = client.post(f'{BASE}/override',
                               json={'winner': NODE_A, 'reason': 'BMC confirmed node B off'})
        assert response.status_code == 202
        fresh = ClusterArbitration(record.to_dict())
        _applied(db, fresh)
        assert fresh.override['winner'] == NODE_A
        assert fresh.override['reason'] == 'BMC confirmed node B off'
        assert fresh.override['at'] > 0
        audit.assert_called_once()

    def test_needs_a_reason_of_ten_characters(self, client, db, record):
        response = client.post(f'{BASE}/override', json={'winner': NODE_A, 'reason': 'short'})
        assert response.status_code == 422
        db.atomic_update.assert_not_called()

    def test_refuses_a_non_member(self, client, db, record):
        response = client.post(f'{BASE}/override',
                               json={'winner': 'node-x', 'reason': 'not a member of this cluster'})
        assert response.status_code == 422


class TestRemediation:

    def test_records_positive_fencing_on_the_node(self, client, db, two_nodes):
        response = client.put(f'{BASE}/remediation', json={'node_id': NODE_B, 'fenced': True})
        assert response.status_code == 204
        target = _applied(db, two_nodes[0])
        assert target.get_id() == NODE_B
        assert two_nodes[0].remediation_fenced is True

    def test_refuses_a_non_member(self, client, db, two_nodes):
        response = client.put(f'{BASE}/remediation', json={'node_id': 'node-x', 'fenced': True})
        assert response.status_code == 422


class TestClusterFlag:

    def test_put_cluster_sets_the_arbitration_flag(self, client, db, cluster):
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/', json={'two_node_arbitration': True})
        assert response.status_code == 204
        _applied(db, cluster)
        assert cluster.two_node_arbitration is True
