"""Group-level replication routes on the consistency-group resource
(design-csi-addons-replication.md §14.4/§14.5): the whole group promotes as one
unit through its policy, and its replication status is the roll-up of its members.
"""
import simplyblock_core.controllers.consistency_group_controller as cgc
from simplyblock_core.controllers.consistency_group_controller import ConsistencyGroupError

from tests.unit.web.api.v2 import _factories as factories

BASE = (f'/api/v2/clusters/{factories.CLUSTER_ID}'
        f'/consistency-groups/{factories.CONSISTENCY_GROUP_ID}')


class TestGroupReplicationEnableDisable:

    def test_a_policy_id_attaches_the_whole_group(self, client, db, cluster,
                                                  consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        resp = client.put(f'{BASE}/replication',
                          json={"replication_policy_id": factories.REPLICATION_POLICY_ID})
        assert resp.status_code == 204
        consistency_group_controller.attach_group_policy.assert_called_once()
        args = consistency_group_controller.attach_group_policy.call_args.args
        assert args[1] == factories.REPLICATION_POLICY_ID

    def test_null_detaches_the_group(self, client, db, cluster,
                                     consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        resp = client.put(f'{BASE}/replication', json={"replication_policy_id": None})
        assert resp.status_code == 204
        consistency_group_controller.detach_group_policy.assert_called_once()
        consistency_group_controller.attach_group_policy.assert_not_called()

    def test_omitting_the_field_is_rejected(self, client, db, cluster,
                                            consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        resp = client.put(f'{BASE}/replication', json={})
        assert resp.status_code == 422
        consistency_group_controller.attach_group_policy.assert_not_called()
        consistency_group_controller.detach_group_policy.assert_not_called()

    def test_an_attach_error_is_mapped_to_409(self, client, db, cluster,
                                              consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.attach_group_policy.side_effect = \
            ConsistencyGroupError("replication policy no-such-policy not found")
        resp = client.put(f'{BASE}/replication',
                          json={"replication_policy_id": factories.REPLICATION_POLICY_ID})
        assert resp.status_code == 409


class TestGroupReplicationStatus:

    def test_rolls_up_member_status(self, client, db, cluster, lvol_controller, monkeypatch):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        monkeypatch.setattr(cgc, 'list_members',
                            lambda group: [{"lvol_id": "v1"}, {"lvol_id": "v2"}])
        lvol_controller.get_replication_info.side_effect = [
            {"role": "source", "state": "in_sync", "last_replicated_at": 100.0, "lag_seconds": 3},
            {"role": "source", "state": "degraded", "last_replicated_at": 80.0, "lag_seconds": 9},
        ]
        resp = client.get(f'{BASE}/replication/status')
        assert resp.status_code == 200
        body = resp.json()
        assert body["member_count"] == 2
        assert body["role"] == "source"
        assert body["state"] == "degraded"   # worst member
        assert body["lag_seconds"] == 9      # worst member

    def test_never_404s_for_an_unreplicated_group(self, client, db, cluster,
                                                  lvol_controller, monkeypatch):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group(policy_id="")
        monkeypatch.setattr(cgc, 'list_members', lambda group: [{"lvol_id": "v1"}])
        lvol_controller.get_replication_info.return_value = None
        resp = client.get(f'{BASE}/replication/status')
        assert resp.status_code == 200
        assert resp.json()["state"] == "not_replicating"


class TestGroupFailover:

    def test_fails_the_whole_group_over_through_its_policy(self, client, db, cluster,
                                                           replication_policy_controller):
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id=factories.REPLICATION_POLICY_ID)
        replication_policy_controller.failover_policy.return_value = [
            {"lvol_id": "v1", "status": "ok"}, {"lvol_id": "v2", "status": "ok"}]
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 200
        replication_policy_controller.failover_policy.assert_called_once_with(
            factories.REPLICATION_POLICY_ID)

    def test_refuses_when_the_group_is_not_attached_to_a_policy(self, client, db, cluster,
                                                               replication_policy_controller):
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id="")
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 412
        replication_policy_controller.failover_policy.assert_not_called()


class TestGroupDemote:

    def test_all_members_demoted_returns_204(self, client, db, cluster,
                                             consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.demote_group.return_value = {
            "demoted": True, "members": [], "error": None}
        resp = client.post(f'{BASE}/replication/demote')
        assert resp.status_code == 204

    def test_still_converging_returns_202_with_detail(self, client, db, cluster,
                                                      consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.demote_group.return_value = {
            "demoted": False, "members": [{"lvol_id": "v2", "demoted": False}], "error": None}
        resp = client.post(f'{BASE}/replication/demote')
        assert resp.status_code == 202
        assert resp.json()["demoted"] is False

    def test_hard_error_returns_500(self, client, db, cluster,
                                    consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.demote_group.return_value = {
            "demoted": False, "members": [], "error": "v2: peer unreachable"}
        resp = client.post(f'{BASE}/replication/demote')
        assert resp.status_code == 500


class TestGroupFailback:

    def test_configures_failback_for_the_whole_group(self, client, db, cluster,
                                                     consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.failback_group.return_value = {
            "configured": True, "members": []}
        resp = client.post(f'{BASE}/replication/failback',
                          json={"source_cluster_id": factories.TARGET_CLUSTER_ID})
        assert resp.status_code == 204
        consistency_group_controller.failback_group.assert_called_once()
        assert consistency_group_controller.failback_group.call_args.kwargs["source_cluster_id"] \
            == factories.TARGET_CLUSTER_ID

    def test_a_failed_member_returns_500(self, client, db, cluster,
                                         consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.failback_group.return_value = {
            "configured": False, "members": [{"lvol_id": "v1", "configured": False, "error": "x"}]}
        resp = client.post(f'{BASE}/replication/failback', json={})
        assert resp.status_code == 500
