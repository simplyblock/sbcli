# coding=utf-8

import uuid as _uuid_mod
from unittest.mock import MagicMock, call, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from tests.ftt2.conftest import patch_externals, prepare_node_for_restart


def _namespace_device(node, namespace_name, pci, serial, is_partition=False):
    dev = NVMeDevice()
    dev.uuid = str(_uuid_mod.uuid4())
    dev.cluster_id = node.cluster_id
    dev.node_id = node.uuid
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.pcie_address = pci
    dev.device_name = f"{namespace_name}p1" if is_partition else namespace_name
    dev.namespace_name = "" if is_partition else namespace_name
    dev.namespace_id = 0 if is_partition else 1
    dev.nvme_bdev = f"bdev_{dev.device_name}"
    dev.serial_number = serial
    dev.is_partition = is_partition
    dev.size = 100_000_000_000
    return dev


@pytest.mark.usefixtures("ftt2_env")
def test_remove_storage_node_deletes_gpt_by_namespace_name(ftt2_env):
    env = ftt2_env
    db = env["db"]
    node = db.get_storage_node_by_id(env["nodes"][0].uuid)
    node.status = StorageNode.STATUS_OFFLINE
    node.nvme_devices = [
        _namespace_device(node, "nvme10n1", "0000:00:03.0", "ser-1"),
        _namespace_device(node, "nvme10n1", "0000:00:03.0", "ser-1-part", is_partition=True),
        _namespace_device(node, "nvme10n2", "0000:00:03.0", "ser-2"),
    ]
    node.jm_device = None
    node.write_to_db(db.kv_store)

    snode_api = MagicMock()
    with patch("simplyblock_core.storage_node_ops.tasks_controller.get_active_node_tasks", return_value=[]), \
         patch("simplyblock_core.storage_node_ops.distr_controller.disconnect_device"), \
         patch("simplyblock_core.storage_node_ops.health_controller._check_node_api", return_value=True), \
         patch("simplyblock_core.storage_node_ops.SNodeClient", return_value=snode_api), \
         patch("simplyblock_core.storage_node_ops.device_controller.device_set_failed") as set_failed:
        storage_node_ops.remove_storage_node(node.uuid)

    assert snode_api.delete_dev_gpt_partitions.call_args_list == [
        call(nvme_name="nvme10n1"),
        call(nvme_name="nvme10n2"),
    ]
    assert set_failed.call_count == 3


@pytest.mark.usefixtures("ftt2_env")
def test_remove_storage_node_continues_after_namespace_delete_rpc_error(ftt2_env):
    env = ftt2_env
    db = env["db"]
    node = db.get_storage_node_by_id(env["nodes"][0].uuid)
    node.status = StorageNode.STATUS_OFFLINE
    node.nvme_devices = [
        _namespace_device(node, "nvme11n1", "0000:00:03.0", "ser-1"),
    ]
    node.jm_device = None
    node.write_to_db(db.kv_store)

    snode_api = MagicMock()
    snode_api.delete_dev_gpt_partitions.side_effect = RuntimeError("simulated rpc failure")

    with patch("simplyblock_core.storage_node_ops.tasks_controller.get_active_node_tasks", return_value=[]), \
         patch("simplyblock_core.storage_node_ops.distr_controller.disconnect_device"), \
         patch("simplyblock_core.storage_node_ops.health_controller._check_node_api", return_value=True), \
         patch("simplyblock_core.storage_node_ops.SNodeClient", return_value=snode_api), \
         patch("simplyblock_core.storage_node_ops.set_node_status") as set_node_status, \
         patch("simplyblock_core.storage_node_ops.device_controller.device_set_failed") as set_failed:
        storage_node_ops.remove_storage_node(node.uuid)

    snode_api.spdk_process_kill.assert_called_once()
    set_node_status.assert_called_once_with(node.uuid, StorageNode.STATUS_REMOVED)
    set_failed.assert_called_once()


@pytest.mark.usefixtures("ftt2_env")
def test_restart_storage_node_updates_legacy_controller_only_device_metadata(ftt2_env):
    env = ftt2_env
    prepare_node_for_restart(env, 0)
    db = DBController()
    node = db.get_storage_node_by_id(env["nodes"][0].uuid)

    legacy_dev = node.nvme_devices[0]
    legacy_dev.pcie_address = "0000:00:03.0"
    legacy_dev.serial_number = "legacy-controller-serial"
    legacy_dev.namespace_name = ""
    legacy_dev.namespace_id = 0
    legacy_dev.device_name = "legacy-controller"
    legacy_dev.nvme_bdev = "legacy_bdev"
    legacy_dev.nvme_controller = "legacy_ctrl"
    node.nvme_devices = [legacy_dev]
    node.ssd_pcie = ["0000:00:03.0"]
    node.write_to_db(db.kv_store)

    discovered = NVMeDevice({
        "uuid": str(_uuid_mod.uuid4()),
        "cluster_id": node.cluster_id,
        "node_id": node.uuid,
        "status": NVMeDevice.STATUS_ONLINE,
        "pcie_address": "0000:00:03.0",
        "namespace_id": 1,
        "namespace_name": "nvme0n1",
        "device_name": "nvme0n1",
        "nvme_bdev": "nvme_new_ns1",
        "nvme_controller": "nvme_ctrl_3",
        "serial_number": "new-namespace-serial:1",
        "size": legacy_dev.size,
    })

    patches = patch_externals()
    for p in patches:
        p.start()
    try:
        with patch("simplyblock_core.storage_node_ops.addNvmeDevices", return_value=[discovered]):
            result = storage_node_ops.restart_storage_node(node.uuid)
    finally:
        for p in patches:
            p.stop()

    assert result is True
    updated = db.get_storage_node_by_id(node.uuid)
    updated_dev = updated.nvme_devices[0]
    assert updated_dev.namespace_id == 1
    assert updated_dev.namespace_name == "nvme0n1"
    assert updated_dev.device_name == "nvme0n1"
    assert updated_dev.nvme_bdev == "nvme_new_ns1"
    assert updated_dev.nvme_controller == "nvme_ctrl_3"
    assert updated_dev.serial_number == "new-namespace-serial:1"
