"""Snapshot replication into a single-node cluster, whose storage node has no
secondary: a supercluster's members are single-node clusters, and moving a
volume between them replicates into one.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication


def _task():
    return SimpleNamespace(uuid="t-1", cluster_id="c-1", node_id="n-1",
                           function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                           status=JobSchedule.STATUS_SUSPENDED, canceled=False,
                           retry=0, max_retry=8,
                           function_params={"snapshot_id": "s-1", "replicate_to_source": False,
                                            "remote_lvol_id": "r-1"},
                           function_result="", write_to_db=MagicMock())


class _RecordStep(Exception):
    """Raised by the first read of the snapshot-record step, which marks that
    the finish got past the conversion; the record step is not under test."""


def _blank_id_raises(node_id):
    if not node_id:
        raise KeyError("StorageNode lookup with a blank id")
    return MagicMock()


class TestFinishOnASingleNodeTarget(unittest.TestCase):

    def test_the_landing_volume_is_converted_on_the_only_node(self):
        primary = MagicMock()
        primary.get_id.return_value = "n-p"
        primary.secondary_node_id = ""
        primary.transfer_hublvol = None
        primary.rpc_client.return_value.bdev_lvol_convert.return_value = True
        remote_lv = MagicMock(top_bdev="LVS_1/LVOL_92", lvs_name="LVS_1", node_id="n-p")
        db = MagicMock()
        db.get_lvol_by_id.return_value = remote_lv
        db.get_storage_node_by_id.side_effect = _blank_id_raises
        db.get_snapshots.side_effect = _RecordStep
        task = _task()
        with patch.object(snapshot_replication, "db", db), \
                self.assertRaises(_RecordStep), \
                patch.object(snapshot_replication, "_stable_target_leader", return_value=(primary, "")), \
                patch.object(snapshot_replication, "_transfer_source_node", return_value=MagicMock()), \
                patch.object(snapshot_replication, "_resolve_chain_target", return_value=(None, None, True)), \
                patch.object(snapshot_replication, "_require_lvs_leader", return_value=True), \
                patch.object(snapshot_replication, "_prechained_nodes_for", return_value=set()), \
                patch.object(snapshot_replication, "SnapShot"):
            snapshot_replication.process_snap_replicate_finish(task, MagicMock())

        primary.rpc_client.return_value.bdev_lvol_convert.assert_called_once_with("LVS_1/LVOL_92")
        self.assertEqual(task.function_params["converted_nodes"], ["n-p"])


class TestDeltaIntoASingleNodeTarget(unittest.TestCase):

    def test_a_node_without_a_secondary_takes_the_delta(self):
        """No secondary means no member to leave holes on: chaining the only
        node is the whole precondition, and the transfer ships the delta."""
        node = MagicMock()
        node.get_id.return_value = "n-p"
        node.secondary_node_id = ""
        node.rpc_client.return_value.bdev_lvol_add_clone.return_value = True
        remote_lv = SimpleNamespace(top_bdev="LVS_1/LVOL_92")
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = _blank_id_raises
        task = _task()
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_resolve_chain_target",
                             return_value=({"snap_bdev": "LVS_1/SNAP_1"}, None, True)):
            allowed = snapshot_replication._prechain_landing_volume(
                task, MagicMock(), False, remote_lv, node)

        self.assertTrue(allowed)
        self.assertEqual(task.function_params["prechained_node_ids"], ["n-p"])
