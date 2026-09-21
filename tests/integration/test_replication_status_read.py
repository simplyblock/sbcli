"""The steady-state fields of the typed replication status read.

``lvol_controller.get_replication_info`` backs the
``GET .../volumes/{id}/replication/status`` endpoint the csi-addons adapter
polls on every reconcile. The fields under test here are the ones the design
adds for that contract: ``role`` (which end of the relationship the volume
is), ``resyncing`` (a divergence catch-up in flight), ``last_replicated_at``
(the truthful ``lastSyncTime`` source), the last-cycle figures, and the
``rpo_target_seconds`` override of the derived lag budget.

``get_replication_info_bulk`` backs the Prometheus replication metrics
(design §11): the same fields, computed for a whole cluster's volumes with
one get_job_tasks/get_replication_policies/get_replication_targets read
instead of get_replication_info's own per-volume reads.

The derivations read LVol, snapshot, task, relationship, and policy records
through ``DBController`` accessors, so these tests belong to the FDB-backed
tier. Nothing above the database runs: no shipping, no RPC, no storage node.
"""

import time

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

CLUSTER_ID = "rsr-cluster-1"
TARGET_CLUSTER_ID = "rsr-cluster-2"
NODE_ID = "rsr-node-1"
POOL_ID = "rsr-pool-1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


@pytest.fixture
def node(db):
    node = StorageNode()
    node.uuid = NODE_ID
    node.cluster_id = CLUSTER_ID
    node.status = StorageNode.STATUS_ONLINE
    node.write_to_db(db.kv_store)
    return node


def _write_lvol(db, uuid, policy_id="", do_replicate=False):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.cluster_id = CLUSTER_ID
    lvol.pool_uuid = POOL_ID
    lvol.node_id = NODE_ID
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.status = LVol.STATUS_ONLINE
    lvol.replication_policy_id = policy_id
    lvol.do_replicate = do_replicate
    lvol.write_to_db(db.kv_store)
    return lvol


def _write_policy(db, rpo_target_seconds=0):
    target = ReplicationTarget()
    target.uuid = "rsr-target-1"
    target.cluster_id = CLUSTER_ID
    target.target_name = "rsr-target"
    target.target_cluster_id = TARGET_CLUSTER_ID
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)

    policy = ReplicationPolicy()
    policy.uuid = "rsr-policy-1"
    policy.cluster_id = CLUSTER_ID
    policy.policy_name = "rsr-policy"
    policy.target_id = target.get_id()
    policy.rpo_target_seconds = rpo_target_seconds
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.write_to_db(db.kv_store)
    return policy


def _write_shipped_snapshot(db, lvol, uuid, created_at, used_size=4096,
                            start_time=None, end_time=None,
                            replicate_to_source=False, done=True):
    """A snapshot with its shipping task, replicated when *done*."""
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER_ID
    snap.lvol = lvol
    snap.created_at = created_at
    snap.used_size = used_size
    snap.snap_type = SnapShot.TYPE_INTERNAL
    if done:
        snap.target_replicated_snap_uuid = f"{uuid}-on-target"
    snap.write_to_db(db.kv_store)

    task = JobSchedule()
    task.uuid = f"task-{uuid}"
    task.cluster_id = CLUSTER_ID
    task.node_id = NODE_ID
    task.date = created_at
    task.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    task.function_params = {"snapshot_id": uuid,
                            "replicate_to_source": replicate_to_source}
    if start_time is not None:
        task.function_params["start_time"] = start_time
    if end_time is not None:
        task.function_params["end_time"] = end_time
    task.status = JobSchedule.STATUS_DONE if done else JobSchedule.STATUS_RUNNING
    task.write_to_db(db.kv_store)
    return snap, task


def _write_relationship(db, source_lvol, target_lvol, state):
    rep = LVolReplication()
    rep.uuid = "rsr-rep-1"
    rep.source_lvol = source_lvol
    rep.target_lvol = target_lvol
    rep.source_cluster_id = CLUSTER_ID
    rep.target_cluster_id = TARGET_CLUSTER_ID
    rep.state = state
    rep.write_to_db(db.kv_store)
    return rep


class TestRole:

    def test_an_unreplicated_volume_reports_none(self, db, node):
        _write_lvol(db, "rsr-lv-plain")

        info = lvol_controller.get_replication_info("rsr-lv-plain")

        assert info["role"] == "none"
        assert info["state"] == "not_replicating"
        assert info["resyncing"] is False
        assert info["last_replicated_at"] is None

    def test_a_policy_attached_volume_is_the_source(self, db, node):
        """The relationship record only materializes at cutover or fail-over,
        so for a volume's whole healthy replicated life the role must come
        from its replication configuration."""
        policy = _write_policy(db)
        _write_lvol(db, "rsr-lv-src", policy_id=policy.get_id(), do_replicate=True)

        info = lvol_controller.get_replication_info("rsr-lv-src")

        assert info["role"] == "source"

    def test_the_target_end_of_a_relationship_is_secondary(self, db, node):
        source = _write_lvol(db, "rsr-lv-a", do_replicate=True)
        target = _write_lvol(db, "rsr-lv-b")
        _write_relationship(db, source, target, LVolReplication.STATE_REPLICATING)

        assert lvol_controller.get_replication_info("rsr-lv-b")["role"] == "secondary"
        assert lvol_controller.get_replication_info("rsr-lv-a")["role"] == "source"

    def test_a_failed_over_relationship_reports_failed_over(self, db, node):
        source = _write_lvol(db, "rsr-lv-a")
        target = _write_lvol(db, "rsr-lv-b")
        _write_relationship(db, source, target, LVolReplication.STATE_FAILED_OVER)

        assert lvol_controller.get_replication_info("rsr-lv-b")["role"] == "failed_over"


class TestLastReplicated:

    def test_last_replicated_at_and_cycle_figures(self, db, node):
        """``lastSyncTime`` is the newest fully replicated snapshot's creation
        time, and the last-cycle figures describe that snapshot's shipping —
        not merely the newest task, which may still be in flight."""
        lvol = _write_lvol(db, "rsr-lv-ship", do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol, "rsr-snap-1", now - 120, used_size=1024,
                                start_time=now - 115, end_time=now - 110)
        _write_shipped_snapshot(db, lvol, "rsr-snap-2", now - 60, used_size=2048,
                                start_time=now - 55, end_time=now - 43)
        _write_shipped_snapshot(db, lvol, "rsr-snap-3", now - 30, done=False)

        info = lvol_controller.get_replication_info("rsr-lv-ship")

        assert info["last_replicated_at"] == now - 60
        assert info["last_cycle_bytes"] == 2048
        assert info["last_cycle_seconds"] == 12
        assert info["outstanding_count"] == 1


class TestResyncing:

    def test_reverse_shipping_in_flight_reports_resyncing(self, db, node):
        """A fail-back ships toward the recovered source with
        ``replicate_to_source`` tasks; while one is outstanding the volume is
        reconciling a divergence."""
        lvol = _write_lvol(db, "rsr-lv-back", do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol, "rsr-snap-r", now - 10,
                                replicate_to_source=True, done=False)

        assert lvol_controller.get_replication_info("rsr-lv-back")["resyncing"] is True

    def test_an_active_final_cutover_task_reports_resyncing(self, db, node):
        lvol = _write_lvol(db, "rsr-lv-final", do_replicate=True)
        task = JobSchedule()
        task.uuid = "rsr-final-1"
        task.cluster_id = CLUSTER_ID
        task.node_id = NODE_ID
        task.date = int(time.time())
        task.function_name = JobSchedule.FN_REPLICATION_FINAL
        task.function_params = {"lvol_id": lvol.get_id()}
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

        assert lvol_controller.get_replication_info("rsr-lv-final")["resyncing"] is True

    def test_a_finished_final_cutover_task_does_not(self, db, node):
        lvol = _write_lvol(db, "rsr-lv-done", do_replicate=True)
        task = JobSchedule()
        task.uuid = "rsr-final-2"
        task.cluster_id = CLUSTER_ID
        task.node_id = NODE_ID
        task.date = int(time.time())
        task.function_name = JobSchedule.FN_REPLICATION_FINAL
        task.function_params = {"lvol_id": lvol.get_id()}
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        assert lvol_controller.get_replication_info("rsr-lv-done")["resyncing"] is False


class TestLagBudget:

    def test_the_declared_rpo_target_replaces_the_derived_budget(self, db, node):
        """With ``rpo_target_seconds`` on the policy, compliance is computed
        against the operator's declared objective, not the three-intervals
        heuristic."""
        policy = _write_policy(db, rpo_target_seconds=600)
        lvol = _write_lvol(db, "rsr-lv-rpo", policy_id=policy.get_id(),
                           do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol, "rsr-snap-rpo", now - 30)

        info = lvol_controller.get_replication_info("rsr-lv-rpo")

        assert info["lag_budget_seconds"] == 600

    def test_without_a_declared_target_the_heuristic_stands(self, db, node):
        policy = _write_policy(db)
        lvol = _write_lvol(db, "rsr-lv-heur", policy_id=policy.get_id(),
                           do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol, "rsr-snap-heur", now - 30)

        info = lvol_controller.get_replication_info("rsr-lv-heur")

        # interval_min defaults to 1: max(3 * 60, 300) = 300.
        assert info["lag_budget_seconds"] == 300


class TestReplicationInfoBulk:
    """``get_replication_info_bulk`` backs the Prometheus replication metrics
    (design §11): one get_job_tasks/get_replication_policies/
    get_replication_targets read per cluster instead of get_replication_info's
    own per-volume reads, so its regression risk is specifically the grouping
    and label-resolution logic -- whether it reaches the SAME numbers
    get_replication_info does, for the right volume, and no others.
    """

    def test_matches_the_single_volume_values_for_a_shipped_volume(self, db, node):
        policy = _write_policy(db, rpo_target_seconds=600)
        lvol = _write_lvol(db, "rsr-bulk-ship", policy_id=policy.get_id(), do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol, "rsr-bulk-snap-1", now - 120, used_size=1024,
                                start_time=now - 115, end_time=now - 110)
        _write_shipped_snapshot(db, lvol, "rsr-bulk-snap-2", now - 60, used_size=2048,
                                start_time=now - 55, end_time=now - 43)
        _write_shipped_snapshot(db, lvol, "rsr-bulk-snap-3", now - 30, done=False)

        single = lvol_controller.get_replication_info("rsr-bulk-ship")
        bulk = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [lvol])[lvol.get_id()]

        for field in ("lag_seconds", "outstanding_bytes", "last_cycle_bytes",
                      "last_cycle_seconds", "state", "failing_count",
                      "max_retry_reached"):
            assert bulk[field] == single[field], field
        assert bulk["lag_budget_seconds"] == 600

    def test_non_replicating_lvol_is_absent(self, db, node):
        _write_lvol(db, "rsr-bulk-plain", do_replicate=False)

        result = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [
            _write_lvol(db, "rsr-bulk-plain2", do_replicate=False),
        ])

        assert result == {}

    def test_never_shipped_volume_reports_defaults(self, db, node):
        lvol = _write_lvol(db, "rsr-bulk-unshipped", do_replicate=True)

        info = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [lvol])[lvol.get_id()]

        assert info["lag_seconds"] is None
        assert info["outstanding_bytes"] == 0
        assert info["state"] == "not_replicating"

    def test_resolves_policy_and_peer_cluster(self, db, node):
        policy = _write_policy(db, rpo_target_seconds=900)
        lvol = _write_lvol(db, "rsr-bulk-policy", policy_id=policy.get_id(), do_replicate=True)

        info = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [lvol])[lvol.get_id()]

        assert info["policy_id"] == policy.get_id()
        assert info["policy_name"] == policy.policy_name
        assert info["peer_cluster"] == TARGET_CLUSTER_ID
        assert info["rpo_target_seconds"] == 900

    def test_a_volume_with_no_policy_reports_no_peer_cluster(self, db, node):
        lvol = _write_lvol(db, "rsr-bulk-nopolicy", do_replicate=True)

        info = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [lvol])[lvol.get_id()]

        assert info["policy_id"] == ""
        assert info["peer_cluster"] == ""

    def test_two_volumes_do_not_cross_contaminate_items(self, db, node):
        """The grouping-by-lvol-id step is this function's own logic, unlike
        the per-volume math it reuses from _replication_cycle_stats -- a bug
        here would put one volume's snapshots on another's ledger."""
        policy = _write_policy(db)
        lvol_a = _write_lvol(db, "rsr-bulk-a", policy_id=policy.get_id(), do_replicate=True)
        lvol_b = _write_lvol(db, "rsr-bulk-b", policy_id=policy.get_id(), do_replicate=True)
        now = int(time.time())
        _write_shipped_snapshot(db, lvol_a, "rsr-bulk-snap-a", now - 30, used_size=1024)
        _write_shipped_snapshot(db, lvol_b, "rsr-bulk-snap-b1", now - 30, used_size=2048)
        _write_shipped_snapshot(db, lvol_b, "rsr-bulk-snap-b2", now - 20, used_size=4096)

        result = lvol_controller.get_replication_info_bulk(CLUSTER_ID, [lvol_a, lvol_b])

        assert result[lvol_a.get_id()]["last_cycle_bytes"] == 1024
        assert result[lvol_b.get_id()]["last_cycle_bytes"] == 4096
