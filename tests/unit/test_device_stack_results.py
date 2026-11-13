"""SFAM-2834: device stacks are built in worker threads during restart and
node add. Their results used to be dropped, so a device whose
bdev_alceml_create failed was later marked ONLINE although it did not exist
in SPDK. A failed stack must now fail the bring-up."""

from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.rpc_client import RPCConnectionError


def test_collects_falsy_and_raising_jobs():
    a, b, c = object(), object(), object()

    def boom():
        raise RPCConnectionError("Could not reach remote")

    failed = storage_node_ops._run_threads_collecting([
        (a, lambda: "ok", ()),
        (b, lambda: None, ()),
        (c, boom, ()),
    ])
    assert failed == [b, c]


def _dev(dev_id, status=NVMeDevice.STATUS_UNAVAILABLE):
    dev = MagicMock()
    dev.status = status
    dev.get_id.return_value = dev_id
    return dev


def test_restart_fails_when_one_stack_fails():
    good, bad = _dev("good"), _dev("bad")
    snode = MagicMock()
    snode.nvme_devices = [good, bad]

    def stack(rpc, nvme, node, after_restart):
        if nvme is bad:
            raise RPCConnectionError("Could not reach remote")
        return nvme

    with patch.object(storage_node_ops, "DBController"), \
            patch.object(storage_node_ops, "_create_storage_device_stack", side_effect=stack):
        assert storage_node_ops._prepare_cluster_devices_on_restart(snode) is False

    # The node record (and so the device statuses) must not be written.
    snode.write_to_db.assert_not_called()


def test_restart_proceeds_when_all_stacks_succeed():
    devs = [_dev("d1"), _dev("d2")]
    snode = MagicMock()
    snode.nvme_devices = list(devs)
    snode.jm_device = None

    with patch.object(storage_node_ops, "DBController"), \
            patch.object(storage_node_ops, "_create_storage_device_stack",
                         side_effect=lambda rpc, nvme, node, after_restart: nvme):
        assert storage_node_ops._prepare_cluster_devices_on_restart(snode) is True
    snode.write_to_db.assert_called()
