"""Snapshot replication into a single-node cluster, whose storage node has no
secondary: a supercluster's members are single-node clusters, and moving a
volume between them replicates into one.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import ChainRequest, FinishRequest, SourceSnapshot


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
        """No secondary: the only node's convert makes the copy
        (peer.service.finish_landing)."""
        primary = MagicMock()
        primary.get_id.return_value = "n-p"
        primary.secondary_node_id = ""
        primary.rpc_client.return_value.bdev_lvol_convert.return_value = True
        landing = SimpleNamespace(top_bdev="LVS_1/LVOL_92", lvs_name="LVS_1", node_id="n-p",
                                  nodes=["n-p"], status="online", lvol_priority_class=0,
                                  get_id=lambda: "landing")
        db = MagicMock()
        db.get_snapshot_by_id.side_effect = KeyError("no copy recorded yet")
        db.get_lvol_by_id.return_value = landing
        db.get_storage_node_by_id.side_effect = _blank_id_raises
        record = MagicMock()
        record.to_dict.return_value = {"uuid": "copy"}
        with patch.object(peer_service, "stable_leader", return_value=(primary, "")), \
                patch.object(peer_service, "require_leader", return_value=True), \
                patch.object(peer_service, "_record_copy", return_value=record), \
                patch.object(peer_service, "_retire_landing") as retire:
            answer = peer_service.finish_landing(db, FinishRequest(
                source_cluster_id="c-a", landing_lvol_id="landing", link="source",
                snapshot=SourceSnapshot(snapshot_id="s-1", snap_name="s-1", size=1)))

        primary.rpc_client.return_value.bdev_lvol_convert.assert_called_once_with("LVS_1/LVOL_92")
        self.assertEqual((answer.state, answer.converted_on), ("done", ["n-p"]))
        retire.assert_called_once()


class TestDeltaIntoASingleNodeTarget(unittest.TestCase):

    def test_a_node_without_a_secondary_takes_the_delta(self):
        """No secondary means no member to leave holes on: chaining the only
        node is the whole precondition, and the transfer ships the delta. The
        destination decides that (peer.service.chain_landing)."""
        node = MagicMock()
        node.get_id.return_value = "n-p"
        node.secondary_node_id = ""
        rpc = node.rpc_client.return_value
        rpc.bdev_lvol_add_clone.return_value = True
        rpc.bdev_get.return_value = None            # not a clone yet
        db = MagicMock()
        db.get_lvol_by_id.return_value = SimpleNamespace(top_bdev="LVS_1/LVOL_92")
        db.get_snapshot_by_id.return_value = SimpleNamespace(
            snap_bdev="LVS_1/SNAP_1", lvol=SimpleNamespace(nodes=["n-p"], node_id="n-p"))
        db.get_storage_node_by_id.side_effect = (
            lambda node_id: node if node_id == "n-p" else _blank_id_raises(node_id))

        answer = peer_service.chain_landing(db, ChainRequest(
            landing_lvol_id="landing", leader_node_id="n-p", predecessor_id="copy"))

        self.assertEqual((answer.state, answer.chained_on), ("chained", ["n-p"]))
        rpc.bdev_lvol_add_clone.assert_called_once_with("LVS_1/LVOL_92", "LVS_1/SNAP_1")
