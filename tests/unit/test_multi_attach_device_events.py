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


def _run(serving_node_id, multi=True):
    dev = NVMeDevice()
    dev.uuid = "dev-1"
    dev.node_id = "node-1"
    dev.cluster_device_order = 3
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.multi_attach = multi
    dev.serving_node_id = serving_node_id

    home = MagicMock()
    home.get_id.return_value = "node-1"
    home.status = StorageNode.STATUS_ONLINE
    home.cluster_id = "c1"
    home.nvme_devices = [dev]

    event = MagicMock()
    event.message = "error_read"
    event.node_id = "node-1"
    event.storage_id = 3
    event.object_dict = {}

    db = MagicMock()
    db.get_storage_node_by_id.return_value = home
    db.get_storage_nodes_by_cluster_id.return_value = [home]
    with patch.object(collector, "db", db), \
            patch.object(collector.device_controller, "device_set_unavailable") as unavailable, \
            patch.object(collector.device_controller, "device_set_io_error"), \
            patch.object(collector.distr_controller, "send_dev_status_event") as peer_event, \
            patch.object(collector, "remove_remote_device_from_node"), \
            patch.object(NVMeDevice, "lock_device_connection"), \
            patch.object(NVMeDevice, "release_device_connection"), \
            patch.object(NVMeDevice, "is_connection_in_progress_to_node", return_value=False):
        collector.process_device_event(event, collector.logger)
    return unavailable, peer_event


class TestHomeNodeOfLunServedElsewhere(unittest.TestCase):

    def test_error_is_handled_as_a_peer_path_error(self):
        unavailable, peer_event = _run(serving_node_id="node-2")
        unavailable.assert_not_called()
        peer_event.assert_called_once()

    def test_home_node_serving_its_lun_fails_it(self):
        unavailable, peer_event = _run(serving_node_id="")
        unavailable.assert_called_once()
        peer_event.assert_not_called()

    def test_regular_cluster_unchanged(self):
        unavailable, _ = _run(serving_node_id="node-2", multi=False)
        unavailable.assert_called_once()


if __name__ == "__main__":
    unittest.main()
