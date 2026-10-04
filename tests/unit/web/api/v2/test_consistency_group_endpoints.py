"""Group-level replication routes on the consistency-group resource
(design-csi-addons-replication.md §14.4/§14.5): the whole group promotes as one
unit through its policy, and its replication status is the roll-up of its members.
"""
from unittest.mock import MagicMock

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


class TestGroupDelete:

    def test_empty_group_is_deleted(self, client, db, cluster,
                                    consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        resp = client.delete(BASE)
        assert resp.status_code == 204
        consistency_group_controller.delete_group.assert_called_once()

    def test_group_with_members_is_refused_as_409(self, client, db, cluster,
                                                  consistency_group_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.delete_group.side_effect = \
            ConsistencyGroupError("consistency group ramen-e2e-cg still has 2 member(s)")
        resp = client.delete(BASE)
        assert resp.status_code == 409


class TestGroupDetail:

    def test_exposes_the_groups_replication_policy(self, client, db, cluster):
        """The group detail must carry its replication policy id. A group attached
        with attach_group_policy stores it on group.policy_id, and the group drill's
        recovery-point read is keyed on the policy, so a client needs to read the
        policy off the group rather than infer it from placement (empty on a
        group-first attach)."""
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id=factories.REPLICATION_POLICY_ID)
        resp = client.get(f'{BASE}/')
        assert resp.status_code == 200
        assert resp.json()["policy_id"] == factories.REPLICATION_POLICY_ID

    def test_policy_id_is_null_for_an_unattached_group(self, client, db, cluster):
        """A standalone group with no policy attached reports a null policy id, not
        a fabricated one."""
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id="")
        resp = client.get(f'{BASE}/')
        assert resp.status_code == 200
        assert resp.json()["policy_id"] is None


class TestGroupReplicationStatus:

    def test_rolls_up_member_status(self, client, db, cluster, lvol_controller, monkeypatch):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        monkeypatch.setattr(cgc, 'list_members',
                            lambda group: [{"lvol_id": "v1"}, {"lvol_id": "v2"}])
        db.get_lvol_by_id.side_effect = lambda i: MagicMock(**{"get_id.return_value": i})
        lvol_controller.get_replication_info_bulk.return_value = {
            "v1": {"state": "in_sync", "last_replicated_at": 100.0, "lag_seconds": 3,
                   "outstanding_count": 0, "outstanding_bytes": 0, "resyncing": False},
            "v2": {"state": "degraded", "last_replicated_at": 80.0, "lag_seconds": 9,
                   "outstanding_count": 0, "outstanding_bytes": 0, "resyncing": False},
        }
        lvol_controller.replication_role.return_value = "source"
        resp = client.get(f'{BASE}/replication/status')
        assert resp.status_code == 200
        body = resp.json()
        assert body["member_count"] == 2
        assert body["role"] == "source"
        assert body["state"] == "degraded"   # worst member
        assert body["lag_seconds"] == 9      # worst member

    def test_group_status_uses_one_bulk_read_not_per_member(self, client, db, cluster,
                                                            lvol_controller, monkeypatch):
        # Regression: 2026-10-01 -- replication_status read per member
        # (get_replication_info once each); for an 8-member group its N
        # unscoped table scans exceeded the csi-addons status RPC deadline, so
        # the VGR's lastSyncTime -- and Ramen's lastGroupSyncTime -- never
        # populated and the DR protect/failover gate hung, even though the data
        # was replicating. The rollup must do ONE bulk read, not N.
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        monkeypatch.setattr(cgc, 'list_members',
                            lambda group: [{"lvol_id": f"v{i}"} for i in range(8)])
        db.get_lvol_by_id.side_effect = lambda i: MagicMock(**{"get_id.return_value": i})
        lvol_controller.get_replication_info_bulk.return_value = {
            f"v{i}": {"state": "in_sync", "last_replicated_at": 100.0, "lag_seconds": 2,
                      "outstanding_count": 0, "outstanding_bytes": 0, "resyncing": False}
            for i in range(8)}
        lvol_controller.replication_role.return_value = "source"
        resp = client.get(f'{BASE}/replication/status')
        assert resp.status_code == 200
        assert resp.json()["member_count"] == 8
        # The N+1 is gone: exactly one bulk read, and no per-member scan.
        lvol_controller.get_replication_info_bulk.assert_called_once()
        assert lvol_controller.get_replication_info.call_count == 0

    def test_never_404s_for_an_unreplicated_group(self, client, db, cluster,
                                                  lvol_controller, monkeypatch):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group(policy_id="")
        monkeypatch.setattr(cgc, 'list_members', lambda group: [{"lvol_id": "v1"}])
        db.get_lvol_by_id.side_effect = lambda i: MagicMock(**{"get_id.return_value": i})
        lvol_controller.get_replication_info_bulk.return_value = {}   # nothing replicating
        resp = client.get(f'{BASE}/replication/status')
        assert resp.status_code == 200
        assert resp.json()["state"] == "not_replicating"


class TestGroupFailover:

    def test_fails_only_the_group_members_over(self, client, db, cluster,
                                               replication_policy_controller):
        # Fails over ONLY the group's members (failover_group), never every volume
        # on the shared policy (failover_policy would sweep in unrelated workloads).
        group = factories.make_consistency_group(policy_id=factories.REPLICATION_POLICY_ID)
        db.get_consistency_group_by_id.return_value = group
        replication_policy_controller.failover_group.return_value = [
            {"lvol_id": "v1", "status": "failed_over"}, {"lvol_id": "v2", "status": "failed_over"}]
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 200
        replication_policy_controller.failover_group.assert_called_once_with(group)

    def test_member_failure_is_surfaced_as_409(self, client, db, cluster,
                                               replication_policy_controller):
        # An all-or-nothing group fail-over that could not complete must NOT return
        # 2xx, or the driver reads it as success and promotes to a group with no
        # clones (silent no-op, 2026-09-27).
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id=factories.REPLICATION_POLICY_ID)
        replication_policy_controller.failover_group.return_value = [
            {"lvol_id": "v1", "status": "failed", "detail": "no common generation"}]
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 409

    def test_a_detached_group_is_failed_over_from_its_replicated_generation(
            self, client, db, cluster, replication_policy_controller):
        # A relocate detaches the group and deletes its demoted source volumes; the
        # promote on the peer must still reach failover_group, which restores the
        # group from its newest replicated generation there (2026-10-04: the old
        # 412 "not attached to a replication policy" stranded the relocate).
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group(policy_id="")
        replication_policy_controller.failover_group.return_value = [
            {"lvol_id": "SRC", "status": "failed_over", "target_lvol_id": "CLONE"}]
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 200
        replication_policy_controller.failover_group.assert_called_once()

    def test_a_detached_group_with_nothing_to_promote_is_409(self, client, db, cluster,
                                                            replication_policy_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group(policy_id="")
        replication_policy_controller.failover_group.return_value = []
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 409

    def test_an_empty_result_is_surfaced_as_409(self, client, db, cluster,
                                                replication_policy_controller):
        # An empty member list means nothing was promoted -- a fail-back that could
        # not resolve its peer group, or an empty group. It must NOT read as 2xx, or
        # the driver promotes to a group with no clones (the fail-back silent no-op,
        # live 2026-09-27).
        db.get_consistency_group_by_id.return_value = \
            factories.make_consistency_group(policy_id=factories.REPLICATION_POLICY_ID)
        replication_policy_controller.failover_group.return_value = []
        resp = client.post(f'{BASE}/replication/failover')
        assert resp.status_code == 409


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


class TestGroupColocationRoutes:
    """Co-location routes (docs/consistency-group-colocation.md)."""

    def test_the_join_plan_names_the_pre_join_migration(self, client, db, cluster, monkeypatch):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        from simplyblock_core.controllers import cg_colocation
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        db.get_lvol_by_id.return_value = factories.make_volume()
        plan = cg_colocation.JoinPlan(steps=["migrate", "join"], migrate_ids=["v", "sib"],
                                      target_node_id="N1")
        monkeypatch.setattr(route.cg_colocation, "late_join_plan", lambda g, v: plan)
        resp = client.post(f'{BASE}/members/plan', json={"lvol_id": factories.VOLUME_ID})
        assert resp.status_code == 200
        assert resp.json() == {"steps": ["migrate", "join"], "target_node_id": "N1",
                               "migrate_lvol_ids": ["v", "sib"], "target_nqn": ""}

    def test_a_join_that_can_never_succeed_is_409(self, client, db, cluster, monkeypatch):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        from simplyblock_core.controllers import cg_colocation
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        db.get_lvol_by_id.return_value = factories.make_volume()

        def refuse(g, v):
            raise cg_colocation.ColocationError("volume is in pool P2")
        monkeypatch.setattr(route.cg_colocation, "late_join_plan", refuse)
        resp = client.post(f'{BASE}/members/plan', json={"lvol_id": factories.VOLUME_ID})
        assert resp.status_code == 409

    def test_colocating_a_lone_member_is_a_no_op(self, client, db, cluster, monkeypatch):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        monkeypatch.setattr(route.cg_colocation, "_db", lambda: db)
        group = factories.make_consistency_group()
        group.members = {factories.VOLUME_ID: {"joined_seq": 1, "removed_seq": 0}}
        db.get_consistency_group_by_id.return_value = group
        volume = factories.make_volume()
        db.get_lvol_by_id.return_value = volume
        db.get_lvols.return_value = [volume]
        db.get_consistency_groups.return_value = [group]
        resp = client.post(f'{BASE}/members/{factories.VOLUME_ID}/colocate', json={})
        # The member is alone in its group, so there is nothing to co-locate
        # with: a no-op, not a refusal.
        assert resp.status_code == 204

    def test_group_migration_create_maps_conflicts_to_409(self, client, db, cluster,
                                                          consistency_group_controller, monkeypatch):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.list_members.return_value = [
            {"lvol_id": factories.VOLUME_ID, "removed_seq": 0}]
        mc = MagicMock()
        mc.MigrationConflictError = type("MigrationConflictError", (Exception,), {})
        mc.PreconditionError = type("PreconditionError", (Exception,), {})
        mc.create_group_migration.side_effect = mc.MigrationConflictError("already migrating")
        monkeypatch.setattr(route, "migration_controller", mc)
        resp = client.post(f'{BASE}/migration', json={"target_node_id": "N2"})
        assert resp.status_code == 409
        mc.create_group_migration.assert_called_once_with(factories.VOLUME_ID, "N2", host_nqn=None)
