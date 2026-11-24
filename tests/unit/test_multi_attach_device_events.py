"""Distrib device events on multi-attach clusters: the home node of a LUN
served by another node reaches it remotely, like any peer. Its I/O error is
about that path and must not fail the LUN for the whole cluster (2026-11-20
FTT2 bed: a home node's error_read on the dead serving node's export,
processed after the takeover, marked the taken-over LUN unavailable and
blocked every LVS of the 1+0 cluster)."""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import main_distr_event_collector as collector


def _run(serving_node_id, multi=True, event_node="node-1", served_since=0, event_ms=0):
    dev = NVMeDevice()
    dev.uuid = "dev-1"
    dev.node_id = "node-1"
    dev.cluster_device_order = 3
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.multi_attach = multi
    dev.serving_node_id = serving_node_id
    dev.served_since = served_since

    home = MagicMock()
    home.get_id.return_value = "node-1"
    home.status = StorageNode.STATUS_ONLINE
    home.cluster_id = "c1"
    home.nvme_devices = [dev]

    peer = MagicMock()
    peer.get_id.return_value = "node-3"
    peer.status = StorageNode.STATUS_ONLINE
    peer.cluster_id = "c1"
    peer.nvme_devices = []

    event = MagicMock()
    event.message = "error_read"
    event.node_id = event_node
    event.storage_id = 3
    event.object_dict = {}
    event.date = event_ms

    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: peer if nid == "node-3" else home
    db.get_storage_nodes_by_cluster_id.return_value = [home, peer]
    with patch.object(collector, "db", db), \
            patch.object(collector.device_controller, "device_set_unavailable") as unavailable, \
            patch.object(collector.device_controller, "device_set_io_error"), \
            patch.object(collector.distr_controller, "send_dev_status_event") as peer_event, \
            patch.object(collector, "remove_remote_device_from_node") as removed, \
            patch.object(NVMeDevice, "lock_device_connection"), \
            patch.object(NVMeDevice, "release_device_connection"), \
            patch.object(NVMeDevice, "is_connection_in_progress_to_node", return_value=False):
        collector.process_device_event(event, collector.logger)
    _run.removed = removed
    _run.status = event.status
    return unavailable, peer_event


class TestHomeNodeOfLunServedElsewhere(unittest.TestCase):

    def test_home_node_error_is_left_to_the_takeover(self):
        unavailable, peer_event = _run(serving_node_id="node-2")
        unavailable.assert_not_called()
        peer_event.assert_not_called()
        _run.removed.assert_not_called()
        self.assertEqual(_run.status, "skipped:multi_attach_remote_path")

    def test_peer_error_is_left_to_the_takeover(self):
        # 2026-11-21 FTT2 bed: per-node "unavailable" sent during a takeover
        # stayed in the node's map; on 1+0 its reads failed and SPDK aborted.
        unavailable, peer_event = _run(serving_node_id="node-2", event_node="node-3")
        unavailable.assert_not_called()
        peer_event.assert_not_called()
        _run.removed.assert_not_called()

    def test_regular_cluster_peer_error_is_processed_as_before(self):
        _run(serving_node_id="", multi=False, event_node="node-3")
        self.assertNotEqual(_run.status, "skipped:multi_attach_remote_path")

    def test_home_node_serving_its_lun_fails_it(self):
        unavailable, peer_event = _run(serving_node_id="")
        unavailable.assert_called_once()
        peer_event.assert_not_called()

    def test_home_node_error_right_after_its_lun_came_back_is_left_to_the_takeover(self):
        # 2026-11-24 FTT1 bed: the takeover moved the LUN back to its home
        # node; the home node's read error on the old path, 0.1 s later, set
        # the device unavailable cluster-wide (EIO on 3 of 4 volumes).
        unavailable, peer_event = _run(serving_node_id="", served_since=1_000_000,
                                       event_ms=1_000_100)
        unavailable.assert_not_called()
        peer_event.assert_not_called()
        self.assertEqual(_run.status, "skipped:multi_attach_takeover_path")

    def test_home_node_error_well_after_the_takeover_fails_it(self):
        unavailable, _ = _run(serving_node_id="", served_since=1_000_000,
                              event_ms=1_000_000 + 60_000)
        unavailable.assert_called_once()

    def test_regular_cluster_unchanged(self):
        unavailable, _ = _run(serving_node_id="node-2", multi=False)
        unavailable.assert_called_once()


if __name__ == "__main__":
    unittest.main()
