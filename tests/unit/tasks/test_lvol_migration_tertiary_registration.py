"""_setup_snap_transfer registers the landing volume on the target's tertiary
whether or not the secondary is registered.

The tertiary used to be registered only inside the secondary's branch, so a
tertiary behind an offline / departing secondary never held the landing
volume; the add_clone / convert that followed then skipped it for want of the
bdev, and its in-memory tree never learnt the chain (replica_chain, follow-up
of bf23 2026-11-12 LVS_16).

Pure logic test: RPC clients, hub manager and size logging are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_lvol_migration as runner
from simplyblock_core.rpc_client import RPCClient


def _snap():
    snap = MagicMock()
    snap.uuid = "65de8f8c-91d1-4084-920d-d31706b9f1c4"
    snap.snap_name = "SNAP_29"
    snap.snap_bdev = "LVS_7/SNAP_29"
    snap.size = 10 * 1024 ** 3
    snap.lvol = None
    return snap


def _node(node_id, lvstore):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.lvstore = lvstore
    return node


def _run(with_secondary, ter_register_ok=True):
    src_node = _node("src-node", "LVS_7")
    tgt_node = _node("tgt-node", "LVS_1")
    src_rpc = MagicMock(spec=RPCClient)
    tgt_rpc = MagicMock(spec=RPCClient)
    sec_rpc = MagicMock(spec=RPCClient)
    ter_rpc = MagicMock(spec=RPCClient)
    short = runner._snap_tgt_short_name(_snap())
    tgt_rpc.bdev_get.return_value = {"uuid": "u-1", "driver_specific": {"lvol": {"blobid": 9}}}
    tgt_rpc.bdev_lvol_get_lvols.return_value = [{"name": short, "map_id": 7}]
    src_rpc.bdev_lvol_transfer.return_value = {"ok": True}
    for rpc in (sec_rpc, ter_rpc):
        rpc.bdev_get.return_value = None
    sec_rpc.bdev_lvol_register.return_value = True
    ter_rpc.bdev_lvol_register.return_value = ter_register_ok
    kwargs = {"tgt_ter": _node("ter-node", ""), "ter_rpc": ter_rpc}
    if with_secondary:
        kwargs.update(tgt_sec=_node("sec-node", ""), sec_rpc=sec_rpc)
    with patch.object(runner, "_log_spdk_bdev_size"), \
         patch.object(runner, "_delete_bdev_blocking") as delete, \
         patch.object(runner.hub_manager, "acquire", return_value=("ctrl0", "hub0", None)):
        transfer, err = runner._setup_snap_transfer(
            _snap(), 0, src_node, tgt_node, src_rpc, tgt_rpc, "tcp",
            lvol_size_mib=10240, existing_bdev_info=tgt_rpc.bdev_get.return_value,
            **kwargs)
    return transfer, err, sec_rpc, ter_rpc, short, delete


class TestTertiaryRegistration(unittest.TestCase):

    def test_tertiary_is_registered_without_a_secondary(self):
        transfer, err, sec_rpc, ter_rpc, short, _ = _run(with_secondary=False)
        self.assertIsNone(err)
        self.assertIsNotNone(transfer)
        ter_rpc.bdev_lvol_register.assert_called_once_with(short, "LVS_1", "u-1", 9, 0)
        sec_rpc.bdev_lvol_register.assert_not_called()

    def test_both_are_registered_with_a_secondary(self):
        _, err, sec_rpc, ter_rpc, short, _ = _run(with_secondary=True)
        self.assertIsNone(err)
        sec_rpc.bdev_lvol_register.assert_called_once_with(short, "LVS_1", "u-1", 9, 0)
        ter_rpc.bdev_lvol_register.assert_called_once_with(short, "LVS_1", "u-1", 9, 0)

    def test_a_failed_tertiary_registration_aborts_and_cleans_up(self):
        transfer, err, _, _, _, delete = _run(with_secondary=False, ter_register_ok=False)
        self.assertIsNone(transfer)
        self.assertIn("tertiary", err)
        delete.assert_called_once()


if __name__ == "__main__":
    unittest.main()
