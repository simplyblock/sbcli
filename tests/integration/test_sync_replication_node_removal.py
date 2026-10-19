"""Node removal admission on a sync-replication cluster, against the real
FoundationDB: nothing can be drained there (volume migration is not
supported), so a node that owns volumes or snapshots is refused before the
removal shuts anything down - the DELETE and the Kubernetes drain alike.
Storage nodes (RPC) are mocked; the database never is.
"""
import uuid
from unittest.mock import patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import node_drain_steps
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

db = stack.db
rpcs = stack.rpcs
_fresh, _update = stack._fresh, stack._update


@pytest.fixture()
def admissible():
    """The checks of check_removal_admission that are not about volumes
    pass: fault tolerance, failure-domain balance, replica relocation."""
    with patch.object(ops, "_check_ftt_allows_node_removal", return_value=(True, "")), \
            patch.object(ops, "_check_replica_relocation_feasible", return_value=(True, "")), \
            patch("simplyblock_core.controllers.cluster_expansion.preconditions."
                  "check_fd_admission_for_remove", return_value=(True, "")):
        yield


def _layout(db, *, sync=True):
    """a0 owns LVS_1 (remote triplet b0 b1 b2), b0 owns LVS_2 (remote a0 a1 a2)."""
    cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
    if not sync:
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
    return cluster, a, b, owner_a


def _volume(db, cluster, owner):
    return publish._volume(db, cluster, owner, publish._pool(db, cluster))


def _snapshot_only(db, cluster, owner):
    """A snapshot of a volume of ``owner`` that was deleted since."""
    gone = LVol()
    gone.uuid = str(uuid.uuid4())
    gone.node_id = owner.get_id()
    gone.lvs_name = owner.lvstore
    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.cluster_id = cluster.get_id()
    snap.lvol = gone
    snap.snap_name = f"snap-{snap.uuid[:8]}"
    snap.status = SnapShot.STATUS_ONLINE
    snap.snap_bdev = f"{owner.lvstore}/SNAP_{snap.uuid[:8]}"
    snap.write_to_db(db.kv_store)
    return snap


class TestAdmission:

    def test_a_sync_owner_with_a_volume_is_refused(self, db, rpcs, admissible):
        cluster, a, b, owner = _layout(db)
        _volume(db, cluster, owner)
        ok, reason = ops.check_removal_admission(_fresh(db, owner), db)
        assert ok is False
        assert "1 volume(s) and 0 snapshot(s)" in reason
        assert "not supported on a sync-replication cluster" in reason

    @pytest.mark.parametrize("check_snapshots", [True, False])
    def test_a_sync_owner_with_a_snapshot_only_is_refused(self, db, rpcs, admissible, check_snapshots):
        cluster, a, b, owner = _layout(db)
        _snapshot_only(db, cluster, owner)
        ok, reason = ops.check_removal_admission(_fresh(db, owner), db, check_snapshots=check_snapshots)
        assert ok is False
        assert "0 volume(s) and 1 snapshot(s)" in reason

    def test_outside_sync_replication_the_drain_takes_the_volumes(self, db, rpcs, admissible):
        cluster, a, b, owner = _layout(db, sync=False)
        _volume(db, cluster, owner)
        assert ops.check_removal_admission(_fresh(db, owner), db) == (True, "")

    def test_a_sync_owner_without_volumes_is_admitted(self, db, rpcs, admissible):
        cluster, a, b, owner = _layout(db)
        assert ops.check_removal_admission(_fresh(db, owner), db) == (True, "")

    def test_a_remote_role_holder_without_volumes_is_admitted(self, db, rpcs, admissible):
        # b1 holds LVS_1's remote secondary and LVS_2's secondary, owns nothing
        cluster, a, b, owner = _layout(db)
        _volume(db, cluster, owner)
        assert ops.check_removal_admission(_fresh(db, b[1]), db) == (True, "")


class TestRemovalIsRefusedBeforeAnyChange:

    def test_the_delete_shuts_nothing_down(self, db, rpcs, admissible):
        cluster, a, b, owner = _layout(db)
        _volume(db, cluster, owner)
        with patch.object(ops, "shutdown_storage_node") as shutdown, \
                patch.object(ops, "set_node_status") as set_status, \
                patch.object(ops.tasks_controller, "add_node_removal_task") as add_task:
            assert ops.remove_storage_node(owner.get_id()) is False
        shutdown.assert_not_called()
        set_status.assert_not_called()
        add_task.assert_not_called()
        assert _fresh(db, owner).status == StorageNode.STATUS_ONLINE
        assert not rpcs[owner.get_id()].method_calls

    @pytest.mark.parametrize("data", ["volume", "snapshot"])
    def test_the_drain_stamps_nothing(self, db, rpcs, admissible, data):
        cluster, a, b, owner = _layout(db)
        if data == "volume":
            _volume(db, cluster, owner)
        else:
            _snapshot_only(db, cluster, owner)
        with pytest.raises(PreconditionError, match="sync-replication"):
            node_drain_steps.prepare_node_for_removal(owner.get_id())
        assert _fresh(db, owner).status == StorageNode.STATUS_ONLINE
        assert not rpcs[owner.get_id()].method_calls
