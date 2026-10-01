"""Regression test: _setup_snap_transfer must NOT re-assert
bdev_lvol_set_migration_flag on a target bdev it is reusing from an earlier
attempt.

Live on 2026-09-28 (run 4, first CRD removal): the first transfer attempt
failed at the convert on the overlap tertiary, the retry found its target
bdev still present ("reusing owned writable bdev") and re-sent the migration
flag. SPDK answered with spdk_lvs_queued_failed_IO -> leadership dropped ->
lvol ports fenced; the monitor marked the target node down, the migration
aborted and the target's tertiary followed a hop later. The flag is set once,
by the attempt that creates the bdev. Mirrors the batch runner's stopgap.

Pure logic test: RPC clients, hub manager and size logging are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_lvol_migration as runner


def _snap(size=10 * 1024 ** 3):
    snap = MagicMock()
    snap.uuid = "65de8f8c-91d1-4084-920d-d31706b9f1c4"
    snap.snap_name = "SNAP_29"
    snap.snap_bdev = "LVS_7/SNAP_29"
    snap.size = size
    snap.lvol = None
    return snap


def _node(node_id, lvstore):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.lvstore = lvstore
    return node


def _setup(existing_bdev_info):
    src_node = _node("src-node", "LVS_7")
    tgt_node = _node("tgt-node", "LVS_1")
    src_rpc = MagicMock()
    tgt_rpc = MagicMock()
    short = runner._snap_tgt_short_name(_snap())
    bdev = [{"uuid": "u", "driver_specific": {"lvol": {"blobid": 1}}}]
    tgt_rpc.get_bdevs.return_value = bdev
    tgt_rpc.create_lvol.return_value = True
    tgt_rpc.bdev_lvol_set_migration_flag.return_value = True
    tgt_rpc.bdev_lvol_get_lvols.return_value = [{"name": short, "map_id": 7}]
    src_rpc.bdev_lvol_transfer.return_value = {"ok": True}
    with patch.object(runner, "_log_spdk_bdev_size"), \
         patch.object(runner.hub_manager, "acquire", return_value=("ctrl0", "hub0", None)), \
         patch.object(runner.migration_controller, "_ensure_lvstore_primary_leader",
                      return_value=(True, None)):
        transfer, err = runner._setup_snap_transfer(
            _snap(), 0, src_node, tgt_node, src_rpc, tgt_rpc, "tcp",
            lvol_size_mib=10240, existing_bdev_info=existing_bdev_info)
    return transfer, err, tgt_rpc


class TestNoFlagReassertOnReusedTargetBdev(unittest.TestCase):

    def test_reused_target_bdev_is_not_flagged_again(self):
        transfer, err, tgt_rpc = _setup(existing_bdev_info=[
            {"uuid": "u", "driver_specific": {"lvol": {"blobid": 1}}}])
        self.assertIsNone(err)
        self.assertIsNotNone(transfer)
        tgt_rpc.create_lvol.assert_not_called()
        tgt_rpc.bdev_lvol_set_migration_flag.assert_not_called()

    def test_freshly_created_target_bdev_is_flagged_once(self):
        transfer, err, tgt_rpc = _setup(existing_bdev_info=[])
        self.assertIsNone(err)
        self.assertIsNotNone(transfer)
        tgt_rpc.create_lvol.assert_called_once()
        tgt_rpc.bdev_lvol_set_migration_flag.assert_called_once_with(
            "LVS_1/" + runner._snap_tgt_short_name(_snap()))


if __name__ == "__main__":
    unittest.main()
