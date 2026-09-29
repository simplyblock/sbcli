"""Idempotent replication-policy attach and detach.

The csi-addons adapter re-drives every verb on each reconcile, so attach and
detach must be safe to repeat: attaching a volume to the policy it already
follows returns success without restarting replication, and detaching a
volume that follows no policy returns success without touching the volume.

The guards under test decide by reading model state (the volume's
``replication_policy_id``, the policy, and the target records) through
``DBController`` accessors, so these tests belong to the FDB-backed tier.
Nothing above the database is exercised: ``replication_start`` and
``replication_stop`` are the side effects whose *absence* is asserted, and
they are mocked.
"""

from unittest.mock import MagicMock

import pytest

from simplyblock_core.controllers import replication_policy_controller as rpc_module
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ReplicationPolicy, ReplicationTarget
from simplyblock_core.models.storage_node import StorageNode

CLUSTER_ID = "rpi-cluster-1"
TARGET_CLUSTER_ID = "rpi-cluster-2"
NODE_ID = "rpi-node-1"
POOL_ID = "rpi-pool-1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _write_node(db):
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


def _write_policy(db, uuid="rpi-policy-1"):
    target = ReplicationTarget()
    target.uuid = "rpi-target-1"
    target.cluster_id = CLUSTER_ID
    target.target_name = "rpi-target"
    target.target_cluster_id = TARGET_CLUSTER_ID
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)

    policy = ReplicationPolicy()
    policy.uuid = uuid
    policy.cluster_id = CLUSTER_ID
    policy.policy_name = "rpi-policy"
    policy.target_id = target.get_id()
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.write_to_db(db.kv_store)
    return policy


def test_attach_to_the_policy_already_followed_is_a_no_op(db, monkeypatch):
    """Same-policy attach returns success without restarting replication.

    Pins existing behavior (the short-circuit in ``attach_policy`` predates
    this change): a restart here would drop the delta base and force a full
    re-sync, which is exactly what an idempotent re-drive must never do.
    """
    _write_node(db)
    policy = _write_policy(db)
    _write_lvol(db, "rpi-lv-attached", policy_id=policy.get_id(), do_replicate=True)
    start = MagicMock()
    monkeypatch.setattr(rpc_module.lvol_controller, "replication_start", start)

    assert rpc_module.attach_policy("rpi-lv-attached", policy.uuid) is True

    start.assert_not_called()
    fresh = db.get_lvol_by_id("rpi-lv-attached")
    assert fresh.replication_policy_id == policy.get_id()


def test_detach_of_a_non_attached_volume_is_a_no_op(db, monkeypatch):
    """Detaching a volume that follows no policy returns success untouched.

    The volume replicates through the legacy start/stop path
    (``do_replicate=True``, no policy): a policy detach must not reach through
    and stop THAT replication, nor purge snapshots — there is no policy
    residue to clean.
    """
    _write_node(db)
    _write_lvol(db, "rpi-lv-loose", policy_id="", do_replicate=True)
    stop = MagicMock()
    monkeypatch.setattr(rpc_module.lvol_controller, "replication_stop", stop)

    assert rpc_module.detach_policy("rpi-lv-loose") is True

    stop.assert_not_called()
    fresh = db.get_lvol_by_id("rpi-lv-loose")
    assert fresh.do_replicate is True


def test_repeated_detach_stays_successful(db, monkeypatch):
    """The second and every further detach of the same volume succeeds."""
    _write_node(db)
    _write_lvol(db, "rpi-lv-twice", policy_id="", do_replicate=False)
    monkeypatch.setattr(rpc_module.lvol_controller, "replication_stop", MagicMock())

    assert rpc_module.detach_policy("rpi-lv-twice") is True
    assert rpc_module.detach_policy("rpi-lv-twice") is True
