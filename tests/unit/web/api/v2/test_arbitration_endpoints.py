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

    def test_creates_the_record_without_a_preferred_node(self, client, db, two_nodes):
        db.get_cluster_arbitration.return_value = None
        with patch.object(ClusterArbitration, 'write_to_db') as write:
            response = client.get(f'{BASE}/')
        assert response.status_code == 200
        body = response.json()
        assert not body.get('preferred_node')
        assert body['fenced_taint'] == constants.TWO_NODE_FENCED_TAINT
        assert 'lease_age_ms' in body and 'enabled' in body
        write.assert_called_once()

    def test_reports_a_pending_rebuild_as_not_redundant(self, client, db, two_nodes):
        # campaign c3, F10: a re-admitted node is not a full replica until rebuilt
        rec = ClusterArbitration()
        rec.cluster_id = CLUSTER_ID
        db.get_cluster_arbitration.return_value = rec
        body = client.get(f'{BASE}/').json()
        assert body['redundant'] is True and body['rebuild_pending'] == []
        rec.rebuild = {NODE_B: {"reason": "re-admitted", "since": 1, "scheduled_at": 0,
                                "queued": False}}
        body = client.get(f'{BASE}/').json()
        assert body['redundant'] is False and body['rebuild_pending'] == [NODE_B]
        assert body['rebuild_stuck'] == []

    def test_reports_a_stuck_rebuild(self, client, db, two_nodes):
        # campaign c4, F12: a rebuild failing on unavailable devices is flagged
        rec = ClusterArbitration()
        rec.cluster_id = CLUSTER_ID
        rec.rebuild = {NODE_B: {"reason": "re-admitted", "since": 1, "scheduled_at": 2,
                                "queued": True, "stuck": True, "stuck_error": 520}}
        rec.device_recovery = {NODE_B: {"reason": "rebuild stuck", "at": 3}}
        db.get_cluster_arbitration.return_value = rec
        body = client.get(f'{BASE}/').json()
        assert body['rebuild_stuck'] == [NODE_B] and body['redundant'] is False
        assert body['device_recovery'][NODE_B]['reason'] == "rebuild stuck"

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

    def test_is_accepted_but_ignored(self, client, db, record, audit):
        # Deprecated: there is no preferred node. Older operators still call
        # it, so it answers 204 but stores and audits nothing.
        response = client.put(f'{BASE}/preferred', json={'node_id': NODE_B})
        assert response.status_code == 204
        db.atomic_update.assert_not_called()
        audit.assert_not_called()

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


# docs/design/two-node-kubernetes-fencing.md, section 3.5
HOST_B = 'nqn.2014-08.org.nvmexpress:uuid:host-b'
HOST_B2 = 'nqn.2014-08.io.simplyblock:uuid:host-b-uid'


class TestKubernetesFencing:

    def test_get_reports_authority_host_fencing_and_client_hosts(self, client, db, record):
        body = client.get(f'{BASE}/').json()
        assert body['fencing_authority'] == 'witness'
        assert body['host_fencing'] is False
        assert body['storage_fenced_condition'] == 'StorageFenced'
        assert set(body['client_hosts']) == {NODE_A, NODE_B}
        assert body['host_fences'] == {}

    def test_client_host_is_stored_on_the_member(self, client, db, two_nodes):
        response = client.put(f'{BASE}/client-host',
                              json={'node_id': NODE_B, 'host_nqns': [HOST_B, ' ' + HOST_B, HOST_B2]})
        assert response.status_code == 204
        node = StorageNode()
        _applied(db, node)
        assert node.client_host_nqns == sorted([HOST_B, HOST_B2])

    def test_client_host_rejects_non_members_and_non_nqns(self, client, db, two_nodes):
        assert client.put(f'{BASE}/client-host',
                          json={'node_id': 'other', 'host_nqns': [HOST_B]}).status_code == 422
        assert client.put(f'{BASE}/client-host',
                          json={'node_id': NODE_B, 'host_nqns': ['host-b']}).status_code == 422
        db.atomic_update.assert_not_called()

    def test_release_is_refused_until_releasable(self, client, db, record, audit):
        assert client.post(f'{BASE}/host-fence/release', json={'node_id': NODE_B}).status_code == 404
        record.host_fences = {NODE_B: {'state': 'revoked', 'on': NODE_A, 'host_nqn': HOST_B}}
        assert client.post(f'{BASE}/host-fence/release', json={'node_id': NODE_B}).status_code == 409
        db.atomic_update.assert_not_called()

    def test_release_marks_a_releasable_fence_restoring(self, client, db, record, audit):
        record.host_fences = {NODE_B: {'state': 'releasable', 'on': NODE_A, 'host_nqn': HOST_B}}
        response = client.post(f'{BASE}/host-fence/release', json={'node_id': NODE_B})
        assert response.status_code == 202
        fresh = ClusterArbitration(record.to_dict())
        _applied(db, fresh)
        assert fresh.host_fences[NODE_B]['state'] == 'restoring'
        assert fresh.host_fences[NODE_B]['release_requested'] is True
        audit.assert_called_once()

    def test_put_cluster_sets_authority_and_host_fencing(self, client, db, cluster):
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/',
                              json={'two_node_fencing_authority': 'pacemaker'})
        assert response.status_code == 204
        _applied(db, cluster)
        assert cluster.two_node_fencing_authority == 'pacemaker'
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/', json={'two_node_host_fencing': True})
        assert response.status_code == 204
        _applied(db, cluster)
        assert cluster.two_node_host_fencing is True

    def test_put_cluster_rejects_an_unknown_authority(self, client, db, cluster):
        response = client.put(f'/api/v2/clusters/{CLUSTER_ID}/',
                              json={'two_node_fencing_authority': 'quorum'})
        assert response.status_code == 422

    def test_client_host_refuses_an_nqn_another_member_reports(self, client, db, two_nodes):
        # review on #1527, point 6: a shared NQN would let a fence revoke the survivor
        two_nodes[1].client_host_nqns = [HOST_B]
        response = client.put(f'{BASE}/client-host', json={'node_id': NODE_B, 'host_nqns': [HOST_B]})
        assert response.status_code == 409
        db.atomic_update.assert_not_called()
