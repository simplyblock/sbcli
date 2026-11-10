"""Group-level replication routes on the consistency-group resource
(design-csi-addons-replication.md §14.4/§14.5): the whole group promotes as one
unit through its policy, and its replication status is the roll-up of its members.
"""
from unittest.mock import MagicMock

import simplyblock_core.controllers.consistency_group_controller as cgc
from simplyblock_core.controllers.consistency_group_controller import (
    ConsistencyGroupError,
)
from simplyblock_core.controllers.replication_policy_controller import (
    ReplicationConfigError,
)
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

    def test_a_member_that_cannot_start_replicating_is_mapped_to_409(
            self, client, db, cluster, consistency_group_controller):
        # A member refused by replication_start (a policy whose target is the
        # group's own cluster: #1502) surfaces from attach_group_policy as a
        # ReplicationConfigError, not a ConsistencyGroupError. Unmapped, it was
        # an unhandled 500 with a traceback and no access-log line (run
        # 20261020-1300-ebs, test 2). Regression: 2026-10-20-attach-500.
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.attach_group_policy.side_effect = \
            ReplicationConfigError("Could not start replication of m1 to target t")
        resp = client.put(f'{BASE}/replication',
                          json={"replication_policy_id": factories.REPLICATION_POLICY_ID})
        assert resp.status_code == 409
        assert "Could not start replication" in resp.json()["detail"]


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


class TestGroupResolution:

    def test_reports_the_live_group_and_the_pv_handles(self, client, db, cluster,
                                                       replication_policy_controller):
        """A VGR keeps its original group handle across a relocate; the driver
        resolves it here to the group holding the data and each PV's handle to
        the volume serving it (2026-10-04, WordPress's VRG waiting on destination
        info against the emptied source group)."""
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        replication_policy_controller.resolve_group.return_value = {
            "active_cluster_id": "cl-b", "active_group_id": "g-b",
            "members": [{"origin_handle": "cl-a:pool-a:orig", "active_handle": "cl-b:pool-b:clone"}],
        }
        resp = client.get(f'{BASE}/replication/resolution')
        assert resp.status_code == 200
        assert resp.json() == {
            "cluster_id": factories.CLUSTER_ID, "group_id": factories.CONSISTENCY_GROUP_ID,
            "active_cluster_id": "cl-b", "active_group_id": "g-b",
            "members": [{"origin_handle": "cl-a:pool-a:orig", "active_handle": "cl-b:pool-b:clone"}],
        }

    def test_a_group_nothing_serves_reports_no_active_group(self, client, db, cluster,
                                                            replication_policy_controller):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        replication_policy_controller.resolve_group.return_value = {
            "active_cluster_id": "", "active_group_id": "", "members": []}
        resp = client.get(f'{BASE}/replication/resolution')
        assert resp.status_code == 200
        assert resp.json()["active_group_id"] == "" and resp.json()["members"] == []
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
                               "migrate_lvol_ids": ["v", "sib"], "target_nqn": "",
                               "colocation": None}

    def test_the_join_plan_carries_the_colocation_outcome(self, client, db, cluster, monkeypatch):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        from simplyblock_core.controllers import cg_colocation
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        db.get_lvol_by_id.return_value = factories.make_volume()
        plan = cg_colocation.JoinPlan(steps=["join", "colocate"], target_nqn="A", colocation={
            "target_nqn": "", "target_is_new": True, "moving_lvol_ids": ["a1", "v"], "refusal": ""})
        monkeypatch.setattr(route.cg_colocation, "late_join_plan", lambda g, v: plan)
        resp = client.post(f'{BASE}/members/plan', json={"lvol_id": factories.VOLUME_ID})
        assert resp.status_code == 200
        assert resp.json()["colocation"] == {"target_nqn": "", "target_is_new": True,
                                             "moving_lvol_ids": ["a1", "v"], "refusal": ""}

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


_MOVE_RECORD = {"id": "M1", "request_id": "r1", "target_nqn": "nqn.t", "target_is_new": True,
    "namespaces_at": "source", "deadline": 0, "status": "reserved",
    "node_ids": ["N1"],
    "volumes": [{"lvol_id": "a1", "source_nqn": "nqn.a", "source_nsid": 1,
                 "ns_uuid": "u1", "nguid": "g1", "bdev": "LVS/a1", "target_nsid": 0}]}


class TestSubsystemMoveRoutes:
    """The subsystem-move resource (design-cg-live-subsystem-flip.md §9)."""

    @staticmethod
    def _route(db, monkeypatch, **fns):
        import simplyblock_web.api.v2.cluster.consistency_group as route
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        for name, fn in fns.items():
            monkeypatch.setattr(route.cg_subsystem_move, name, fn)
        return route

    def test_create_returns_the_move(self, client, db, cluster, monkeypatch):
        self._route(db, monkeypatch, create_subsystem_move=lambda g, lid, rid, **kw: dict(_MOVE_RECORD))
        resp = client.post(f'{BASE}/subsystem-moves', json={"lvol_id": "a1", "request_id": "r1"})
        assert resp.status_code == 200
        assert resp.json() == {"id": "M1", "request_id": "r1", "target_nqn": "nqn.t",
                               "target_is_new": True, "namespaces_at": "source", "deadline": 0,
                               "status": "reserved", "purpose": "join",
                               "volumes": [{"lvol_id": "a1", "source_nqn": "nqn.a", "source_nsid": 1,
                                            "ns_uuid": "u1", "nguid": "g1", "target_nsid": 0}]}

    def test_create_at_the_subsystem_limit_is_409_naming_node_and_limit(self, client, db, cluster,
                                                                        monkeypatch):
        from simplyblock_core.controllers import cg_colocation

        def refuse(g, lid, rid, **kw):
            raise cg_colocation.SubsystemLimitReached("N1", 500)
        self._route(db, monkeypatch, create_subsystem_move=refuse)
        resp = client.post(f'{BASE}/subsystem-moves', json={"lvol_id": "a1", "request_id": "r1"})
        assert resp.status_code == 409
        assert resp.json()["detail"]["reason"] == "SubsystemLimitReached"
        assert (resp.json()["detail"]["node_id"], resp.json()["detail"]["limit"]) == ("N1", 500)

    def test_create_while_another_move_is_active_is_409(self, client, db, cluster, monkeypatch):
        from simplyblock_core.controllers import cg_subsystem_move

        def refuse(g, lid, rid, **kw):
            raise cg_subsystem_move.MoveConflictError("move M0 already active")
        self._route(db, monkeypatch, create_subsystem_move=refuse)
        resp = client.post(f'{BASE}/subsystem-moves', json={"lvol_id": "a1", "request_id": "r1"})
        assert resp.status_code == 409
        assert resp.json()["detail"]["reason"] == "MoveConflict"

    def test_create_is_a_join_unless_the_request_says_leave(self, client, db, cluster, monkeypatch):
        """Regression/feature: 2026-11-03-cg-relabel-move"""
        seen = []

        def create(g, lid, rid, purpose="join"):
            seen.append(purpose)
            return dict(_MOVE_RECORD, purpose=purpose)
        self._route(db, monkeypatch, create_subsystem_move=create)
        resp = client.post(f'{BASE}/subsystem-moves', json={"lvol_id": "a1", "request_id": "r1"})
        assert resp.status_code == 200 and resp.json()["purpose"] == "join"
        resp = client.post(f'{BASE}/subsystem-moves',
                           json={"lvol_id": "a1", "request_id": "r2", "purpose": "leave"})
        assert resp.status_code == 200 and resp.json()["purpose"] == "leave"
        assert seen == ["join", "leave"]

    def test_create_with_an_unknown_purpose_is_rejected(self, client, db, cluster, monkeypatch):
        """Regression/feature: 2026-11-03-cg-relabel-move"""
        self._route(db, monkeypatch)
        resp = client.post(f'{BASE}/subsystem-moves',
                           json={"lvol_id": "a1", "request_id": "r1", "purpose": "swap"})
        assert resp.status_code == 422

    def test_a_request_without_a_request_id_is_rejected(self, client, db, cluster, monkeypatch):
        self._route(db, monkeypatch)
        resp = client.post(f'{BASE}/subsystem-moves', json={"lvol_id": "a1"})
        assert resp.status_code == 422

    def test_prepare_returns_the_target_connect_entries(self, client, db, cluster, monkeypatch):
        seen = {}

        def prepare(g, mid, host_nqn=None):
            seen["args"] = (mid, host_nqn)
            return dict(_MOVE_RECORD), [{"nqn": "nqn.t", "ip": "10.0.0.1"}]
        self._route(db, monkeypatch, prepare_subsystem_move=prepare)
        resp = client.post(f'{BASE}/subsystem-moves/M1/prepare', json={"host_nqn": "nqn.host"})
        assert resp.status_code == 200
        assert resp.json()["connect"] == [{"nqn": "nqn.t", "ip": "10.0.0.1"}]
        assert resp.json()["move"]["id"] == "M1"
        assert seen["args"] == ("M1", "nqn.host")

    def test_a_move_past_its_deadline_is_409(self, client, db, cluster, monkeypatch):
        from simplyblock_core.controllers import cg_subsystem_move

        def late(g, mid, deadline):
            raise cg_subsystem_move.DeadlinePassedError("deadline passed")
        self._route(db, monkeypatch, move_subsystem_move=late)
        resp = client.post(f'{BASE}/subsystem-moves/M1/move', json={"deadline": 1})
        assert resp.status_code == 409
        assert resp.json()["detail"]["reason"] == "DeadlinePassed"

    def test_a_failed_move_is_409_naming_where_the_namespaces_are(self, client, db, cluster,
                                                                  monkeypatch):
        from simplyblock_core.controllers import cg_subsystem_move

        def fail(g, mid, deadline):
            raise cg_subsystem_move.MoveFailedError("add failed", "source")
        self._route(db, monkeypatch, move_subsystem_move=fail)
        resp = client.post(f'{BASE}/subsystem-moves/M1/move', json={"deadline": 2000000000})
        assert resp.status_code == 409
        assert resp.json()["detail"] == {"reason": "MoveFailed", "message": "add failed",
                                         "namespaces_at": "source"}

    def test_commit_and_rollback_are_204(self, client, db, cluster, monkeypatch):
        calls = []
        self._route(db, monkeypatch,
                    commit_subsystem_move=lambda g, mid: calls.append(("commit", mid)),
                    rollback_subsystem_move=lambda g, mid: calls.append(("rollback", mid)))
        assert client.post(f'{BASE}/subsystem-moves/M1/commit').status_code == 204
        assert client.post(f'{BASE}/subsystem-moves/M1/rollback').status_code == 204
        assert calls == [("commit", "M1"), ("rollback", "M1")]

    def test_a_step_overlapping_another_step_is_409(self, client, db, cluster, monkeypatch):
        from simplyblock_core.controllers import cg_subsystem_move

        def busy(g, mid, **kw):
            raise cg_subsystem_move.MoveConflictError("another step of subsystem move M1 is running")
        self._route(db, monkeypatch, prepare_subsystem_move=busy, rollback_subsystem_move=busy)
        for step in ("prepare", "rollback"):
            resp = client.post(f'{BASE}/subsystem-moves/M1/{step}')
            assert resp.status_code == 409, step
            assert resp.json()["detail"]["reason"] == "MoveConflict"

    def test_an_unknown_move_is_404(self, client, db, cluster, monkeypatch):
        def missing(g, mid):
            raise KeyError(mid)
        self._route(db, monkeypatch, get_subsystem_move=missing, commit_subsystem_move=missing)
        assert client.get(f'{BASE}/subsystem-moves/nope').status_code == 404
        assert client.post(f'{BASE}/subsystem-moves/nope/commit').status_code == 404


class TestTakeGeneration:
    """POST /snapshots takes one generation. A request id makes a retry return
    the generation the first call took, so the answer is that generation, not
    whichever generation happens to be the latest."""

    @staticmethod
    def _row(seq):
        return {"group_seq": seq, "created_at": 1, "expected": 1, "present": 1,
                "complete": True, "members": []}

    def _setup(self, db, consistency_group_controller, seq_taken):
        db.get_consistency_group_by_id.return_value = factories.make_consistency_group()
        consistency_group_controller.create_group_snapshot_for_group.return_value = (seq_taken, None)
        consistency_group_controller.list_generations.return_value = [self._row(3), self._row(4)]

    def test_the_request_id_is_passed_and_its_generation_answered(
            self, client, db, cluster, consistency_group_controller):
        self._setup(db, consistency_group_controller, seq_taken=3)

        resp = client.post(f'{BASE}/snapshots', json={'request_id': 'sbk-uid-1'})

        assert resp.status_code == 200
        assert resp.json()['group_seq'] == 3
        _, kwargs = consistency_group_controller.create_group_snapshot_for_group.call_args
        assert kwargs['request_id'] == 'sbk-uid-1'

    def test_no_body_still_takes_a_generation(
            self, client, db, cluster, consistency_group_controller):
        self._setup(db, consistency_group_controller, seq_taken=4)

        resp = client.post(f'{BASE}/snapshots')

        assert resp.status_code == 200
        assert resp.json()['group_seq'] == 4
        _, kwargs = consistency_group_controller.create_group_snapshot_for_group.call_args
        assert kwargs['request_id'] == ''
