"""The latest-replicated-snapshot read.

A test-failover drill (design §14) has to resolve its test point -- the
newest fully replicated snapshot, per volume or per consistency-group
generation -- WITHOUT touching real replication state to find out what it is.
``lvol_controller.latest_replicated_snapshot`` and
``replication_policy_controller.latest_replicated_generation`` expose exactly
the selection the real fail-over path already computes internally
(``last_replicated_target_snapshot`` and
``_resolve_group_failover_generation``), as reads.

The per-volume selection algorithm itself (task-done, not-in-deletion,
newest-first, generation walk-back) is already covered by the fake-DB unit
suite in ``simplyblock_core/test/test_failover_snapshot_selection.py``; these
tests cover only what is NEW here: resolving a volume id to its selection
(the wrapper), and the group form's refusal rules. Both derivations read
LVol, snapshot, task, and consistency-group records through ``DBController``
accessors, so these tests belong to the FDB-backed tier.
"""

import time

import pytest

from simplyblock_core.controllers import lvol_controller, replication_policy_controller
from simplyblock_core.controllers.replication_policy_controller import ReplicationConfigError
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.replication import ConsistencyGroup, ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

CLUSTER_ID = "rsn-cluster-1"
TARGET_CLUSTER_ID = "rsn-cluster-2"
NODE_ID = "rsn-node-1"
POOL_ID = "rsn-pool-1"


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


def _write_lvol(db, uuid, policy_id="", group_id=""):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.cluster_id = CLUSTER_ID
    lvol.pool_uuid = POOL_ID
    lvol.node_id = NODE_ID
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.status = LVol.STATUS_ONLINE
    lvol.replication_policy_id = policy_id
    lvol.do_replicate = True
    lvol.group_id = group_id
    lvol.write_to_db(db.kv_store)
    return lvol


def _write_policy(db, uuid="rsn-policy-1", consistency_group=False):
    target = ReplicationTarget()
    target.uuid = f"{uuid}-target"
    target.cluster_id = CLUSTER_ID
    target.target_name = f"{uuid}-target"
    target.target_cluster_id = TARGET_CLUSTER_ID
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)

    policy = ReplicationPolicy()
    policy.uuid = uuid
    policy.cluster_id = CLUSTER_ID
    policy.policy_name = uuid
    policy.target_id = target.get_id()
    policy.consistency_group = consistency_group
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.write_to_db(db.kv_store)
    return policy


def _write_group(db, policy, uuid="rsn-group-1"):
    group = ConsistencyGroup()
    group.uuid = uuid
    group.cluster_id = CLUSTER_ID
    group.group_name = uuid
    group.policy_id = policy.get_id()
    group.write_to_db(db.kv_store)
    return group


def _write_replicated_pair(db, lvol, source_uuid, created_at, group_id="", group_seq=0,
                           target_uuid=None, target_deleting=False):
    """A source snapshot with a DONE shipping task and a live target copy.

    The target id is prefixed rather than suffixed onto the source id:
    ``get_snapshot_by_id`` reads by key-prefix scan (design of
    ``BaseModel.read_from_db``), so a source id that is itself a PREFIX of
    its target id (e.g. ``"snap-1"`` / ``"snap-1-on-target"``) makes a lookup
    of the source id match both records and raise "Multiple values present".
    """
    target_uuid = target_uuid or f"tgt-{source_uuid}"

    target_snap = SnapShot()
    target_snap.uuid = target_uuid
    target_snap.cluster_id = TARGET_CLUSTER_ID
    target_snap.pool_uuid = POOL_ID
    target_snap.lvol = lvol
    target_snap.created_at = created_at
    target_snap.used_size = 4096
    target_snap.size = 4096
    target_snap.status = (SnapShot.STATUS_IN_DELETION if target_deleting
                          else SnapShot.STATUS_ONLINE)
    target_snap.write_to_db(db.kv_store)

    source_snap = SnapShot()
    source_snap.uuid = source_uuid
    source_snap.cluster_id = CLUSTER_ID
    source_snap.lvol = lvol
    source_snap.created_at = created_at
    source_snap.used_size = 4096
    source_snap.target_replicated_snap_uuid = target_uuid
    source_snap.group_id = group_id
    source_snap.group_seq = group_seq
    source_snap.write_to_db(db.kv_store)

    task = JobSchedule()
    task.uuid = f"task-{source_uuid}"
    task.cluster_id = CLUSTER_ID
    task.node_id = NODE_ID
    task.date = created_at
    task.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    task.function_params = {"snapshot_id": source_uuid, "replicate_to_source": False}
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)

    return source_snap, target_snap


class TestPerVolumeRead:

    def test_volume_with_no_replication_returns_none(self, db, node):
        _write_lvol(db, "rsn-lv-plain")

        assert lvol_controller.latest_replicated_snapshot("rsn-lv-plain") is None

    def test_unknown_volume_raises_key_error(self, db, node):
        with pytest.raises(KeyError):
            lvol_controller.latest_replicated_snapshot("rsn-lv-does-not-exist")

    def test_returns_the_target_side_copy_of_the_newest_replicated_snapshot(self, db, node):
        lvol = _write_lvol(db, "rsn-lv-ship")
        now = int(time.time())
        _write_replicated_pair(db, lvol, "rsn-snap-old", now - 120)
        _, newest_target = _write_replicated_pair(db, lvol, "rsn-snap-new", now - 10)

        result = lvol_controller.latest_replicated_snapshot("rsn-lv-ship")

        assert result is not None
        assert result.get_id() == newest_target.get_id()
        assert result.cluster_id == TARGET_CLUSTER_ID

    def test_a_target_copy_mid_deletion_is_skipped(self, db, node):
        lvol = _write_lvol(db, "rsn-lv-prune")
        now = int(time.time())
        _, older_target = _write_replicated_pair(db, lvol, "rsn-snap-good", now - 120)
        _write_replicated_pair(db, lvol, "rsn-snap-pruning", now - 10,
                               target_deleting=True)

        result = lvol_controller.latest_replicated_snapshot("rsn-lv-prune")

        assert result is not None
        assert result.get_id() == older_target.get_id()


class TestGroupRead:

    def test_refuses_a_policy_without_a_consistency_group(self, db, node):
        policy = _write_policy(db, "rsn-policy-solo", consistency_group=False)

        with pytest.raises(ReplicationConfigError, match="no consistency group"):
            replication_policy_controller.latest_replicated_generation(policy.get_id())

    def test_the_newest_generation_complete_for_every_member(self, db, node):
        policy = _write_policy(db, "rsn-policy-group", consistency_group=True)
        group = _write_group(db, policy)
        lv_a = _write_lvol(db, "rsn-lv-a", policy_id=policy.get_id(), group_id=group.get_id())
        lv_b = _write_lvol(db, "rsn-lv-b", policy_id=policy.get_id(), group_id=group.get_id())
        now = int(time.time())
        # Generation 1: both members. Generation 2: only lv_a -- incomplete,
        # so the resolved generation must stay 1.
        _write_replicated_pair(db, lv_a, "rsn-g1-a", now - 100, group_id=group.get_id(),
                               group_seq=1)
        _write_replicated_pair(db, lv_b, "rsn-g1-b", now - 100, group_id=group.get_id(),
                               group_seq=1)
        _write_replicated_pair(db, lv_a, "rsn-g2-a", now - 10, group_id=group.get_id(),
                               group_seq=2)

        seq, members = replication_policy_controller.latest_replicated_generation(
            policy.get_id())

        assert seq == 1
        assert set(members) == {"rsn-lv-a", "rsn-lv-b"}
        assert members["rsn-lv-a"].get_id() == "tgt-rsn-g1-a"
        assert members["rsn-lv-b"].get_id() == "tgt-rsn-g1-b"

    def test_refuses_when_no_generation_is_complete_for_every_member(self, db, node):
        policy = _write_policy(db, "rsn-policy-partial", consistency_group=True)
        group = _write_group(db, policy)
        lv_a = _write_lvol(db, "rsn-lv-c", policy_id=policy.get_id(), group_id=group.get_id())
        _write_lvol(db, "rsn-lv-d", policy_id=policy.get_id(), group_id=group.get_id())
        now = int(time.time())
        # Only lv_c has ever replicated; lv_d has nothing at any generation.
        _write_replicated_pair(db, lv_a, "rsn-g1-c", now - 10, group_id=group.get_id(),
                               group_seq=1)

        with pytest.raises(ReplicationConfigError, match="mixed-generation"):
            replication_policy_controller.latest_replicated_generation(policy.get_id())

    def test_refuses_once_every_member_has_already_failed_over(self, db, node):
        policy = _write_policy(db, "rsn-policy-done", consistency_group=True)
        group = _write_group(db, policy)
        lv_a = _write_lvol(db, "rsn-lv-e", policy_id=policy.get_id(), group_id=group.get_id())
        now = int(time.time())
        _write_replicated_pair(db, lv_a, "rsn-g1-e", now - 10, group_id=group.get_id(),
                               group_seq=1)

        clone = _write_lvol(db, "rsn-clone-e")
        rep = LVolReplication()
        rep.uuid = "rsn-rep-e"
        rep.source_lvol = lv_a
        rep.target_lvol = clone
        rep.source_cluster_id = CLUSTER_ID
        rep.target_cluster_id = TARGET_CLUSTER_ID
        rep.state = LVolReplication.STATE_FAILED_OVER
        rep.write_to_db(db.kv_store)

        with pytest.raises(ReplicationConfigError, match="no member left"):
            replication_policy_controller.latest_replicated_generation(policy.get_id())
