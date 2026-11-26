"""A device that was failed and migrated out stays out.

``failed_and_migrated`` says the device's data was moved elsewhere and its
place in the cluster given up. A replacement enters as a new device record
(``new_device_from_failed``); the old record is never brought back.

Lab, 2026-11-23: a node was marked unreachable for a moment while a peer
restarted and then cleared back to ONLINE. The node monitor re-admits a
recovered node's devices, skipped the online, removed and failed ones, and set
the three devices of that node that had been migrated out ONLINE again. SPDK
could open none of them, and with the peer still restarting the cluster was
suspended and client IO failed.

Run against the real FoundationDB: the monitor and the device controller read
the node back from the database, so the state is what they stored. Replaced
are the SPDK RPC client and the fan-out of device events to the distribs.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import distr_controller
from simplyblock_core.controllers import device_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor

CLUSTER = "3b1f0c5a-0000-4000-8000-000000000001"
NODE = "3b1f0c5a-0000-4000-8000-000000000101"
STATUSES = (
    NVMeDevice.STATUS_ONLINE,
    NVMeDevice.STATUS_UNAVAILABLE,
    NVMeDevice.STATUS_FAILED_AND_MIGRATED,
    NVMeDevice.STATUS_FAILED,
    NVMeDevice.STATUS_REMOVED,
    NVMeDevice.STATUS_NEW,
)


def _device_id(status):
    return f"3b1f0c5a-0000-4000-8000-0000000002{STATUSES.index(status):02d}"


@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    cluster = Cluster()
    cluster.uuid = CLUSTER
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.write_to_db(controller.kv_store)
    node = StorageNode()
    node.uuid = NODE
    node.cluster_id = CLUSTER
    node.status = StorageNode.STATUS_ONLINE
    node.hostname = "worker-1"
    node.nvme_devices = []
    for index, status in enumerate(STATUSES):
        dev = NVMeDevice()
        dev.uuid = _device_id(status)
        dev.cluster_id = CLUSTER
        dev.node_id = NODE
        dev.status = status
        dev.cluster_device_order = -1 if status == NVMeDevice.STATUS_NEW else index
        node.nvme_devices.append(dev)
    node.write_to_db(controller.kv_store)
    with patch.object(StorageNode, "rpc_client", return_value=MagicMock()), \
            patch.object(distr_controller, "send_dev_status_event"), \
            patch.object(distr_controller, "disconnect_device"), \
            patch.object(device_controller, "connect_peers_to_node_devices"), \
            patch.object(device_controller.tasks_controller, "add_device_mig_task_for_node"):
        yield controller


def _statuses(db):
    return {dev.get_id(): dev.status for dev in db.get_storage_node_by_id(NODE).nvme_devices}


def test_a_node_coming_back_online_readmits_only_what_the_outage_took(db):
    storage_node_monitor.readmit_devices_after_node_online(NODE)

    after = _statuses(db)
    assert after[_device_id(NVMeDevice.STATUS_UNAVAILABLE)] == NVMeDevice.STATUS_ONLINE
    for status in STATUSES:
        if status != NVMeDevice.STATUS_UNAVAILABLE:
            assert after[_device_id(status)] == status, status


@pytest.mark.parametrize("state", [
    NVMeDevice.STATUS_ONLINE,
    NVMeDevice.STATUS_UNAVAILABLE,
    NVMeDevice.STATUS_REMOVED,
    NVMeDevice.STATUS_READONLY,
    NVMeDevice.STATUS_FAILED,
])
@pytest.mark.parametrize("cause", [
    device_controller.CAUSE_OTHER,
    device_controller.CAUSE_DEVICE_RESTART,
    device_controller.CAUSE_NODE_RECOVERY,
    device_controller.CAUSE_LOCAL_FAILURE,
    device_controller.CAUSE_FAILURE_MIGRATION,
])
def test_no_writer_takes_a_device_out_of_failed_and_migrated(db, state, cause):
    device_id = _device_id(NVMeDevice.STATUS_FAILED_AND_MIGRATED)

    assert device_controller.device_set_state(device_id, state, cause=cause) is False

    assert _statuses(db)[device_id] == NVMeDevice.STATUS_FAILED_AND_MIGRATED


def test_setting_it_to_the_state_it_is_in_is_not_refused(db):
    device_id = _device_id(NVMeDevice.STATUS_FAILED_AND_MIGRATED)

    assert device_controller.device_set_state(
        device_id, NVMeDevice.STATUS_FAILED_AND_MIGRATED,
        cause=device_controller.CAUSE_FAILURE_MIGRATION)

    assert _statuses(db)[device_id] == NVMeDevice.STATUS_FAILED_AND_MIGRATED


def test_a_failed_device_still_reaches_failed_and_migrated(db):
    device_id = _device_id(NVMeDevice.STATUS_FAILED)

    assert device_controller.device_set_failed_and_migrated(device_id)

    assert _statuses(db)[device_id] == NVMeDevice.STATUS_FAILED_AND_MIGRATED


def test_a_removal_event_for_it_leaves_it_as_it_is(db):
    """SPDK reports the removal of a drive that is long migrated out (it is
    pulled, or its stale stack is torn down): the forced remove that follows
    used to walk the record through unavailable to removed."""
    device_id = _device_id(NVMeDevice.STATUS_FAILED_AND_MIGRATED)

    device_controller.device_remove(device_id, force=True,
                                    cause=device_controller.CAUSE_LOCAL_FAILURE)

    assert _statuses(db)[device_id] == NVMeDevice.STATUS_FAILED_AND_MIGRATED
