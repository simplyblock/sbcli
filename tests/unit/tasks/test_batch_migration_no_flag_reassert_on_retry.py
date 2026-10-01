"""Regression test: _handle_intermediate_barrier must NOT re-assert
bdev_lvol_set_migration_flag on a failed batch_final_step retry.

Root cause this guards against: re-firing bdev_lvol_set_migration_flag once
per failed-cutover retry gave a live leadership race (check-then-act across a
separate RPC round-trip) one more roll every time. Live node-removal runs
traced this session showed the reassert landing on nodes whose leadership had
just moved, triggering spdk_lvs_queued_failed_IO -> self-demotion -> port
block -> the node reported "down", failing the round and forcing yet another
retry (and another reassert) -- a self-perpetuating cycle solo migrations
(which only ever set the flag once, at creation) never hit.

Pure logic test with hub_manager/_build_paths/_build_batch_final_args/
_commit_intermediate_snapshot_chain mocked, so this belongs in the unit tier
rather than integration/migration (which provisions a real FDB).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup

import simplyblock_core.services.tasks_runner_batch_migration as runner


def _group():
    g = LVolMigrationGroup()
    g.uuid = "group-1"
    g.target_nqn = "nqn.test:target"
    g.members = [{"ns_id": 1, "migration_id": "worker-1"}]
    g.intermediates_done = ["worker-1"]
    g.intermediate_more_needed = []
    g.intermediate_round = 0
    g.write_to_db = MagicMock()
    return g


def _member_migrations():
    m = MagicMock()
    m.uuid = "worker-1"
    m.lvol_id = "lvol-1"
    return [m]


def _path_entry(node_id):
    return {
        "rpc": MagicMock(),
        "ips": ["192.168.1.1"],
        "port": 4420,
        "trtype": "tcp",
        "node_id": node_id,
    }


class TestNoFlagReassertOnRetry(unittest.TestCase):

    def test_failed_batch_final_step_does_not_reassert_migration_flag(self):
        group = _group()
        member_migrations = _member_migrations()
        src_node = MagicMock()
        src_node.get_id.return_value = "src-node"
        tgt_node = MagicMock()
        tgt_node.get_id.return_value = "tgt-node"
        tgt_node.lvstore = "LVS_1"
        src_rpc = MagicMock()
        tgt_rpc = MagicMock()

        final_step_rpc = MagicMock()
        final_step_rpc.bdev_lvol_batch_transfer_final_step.side_effect = RuntimeError("connection error")
        src_node.rpc_client.return_value = final_step_rpc

        src_paths = [_path_entry("src-node")]
        tgt_paths = [_path_entry("tgt-node")]

        mock_db = MagicMock()
        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "migration_controller") as mock_mc, \
             patch.object(runner.hub_manager, "acquire", return_value=("ctrl0", "hub_bdev0", None)), \
             patch.object(runner, "_build_batch_final_args",
                          return_value=(["lvol_name1"], ["lvol_id1"], ["snap1"])), \
             patch.object(runner, "_commit_intermediate_snapshot_chain", return_value=None), \
             patch.object(runner, "_build_paths", return_value=(src_paths, tgt_paths, set())):
            batch_ok, batch_err = runner._handle_intermediate_barrier(
                group, member_migrations, src_node, tgt_node, src_rpc, tgt_rpc)

        # Retry path taken: batch_final_step failed, round forced forward,
        # caller told to keep waiting.
        assert (batch_ok, batch_err) == (None, None)
        assert group.intermediate_round == 1

        # The regression: no migration-flag reassert anywhere on this path.
        mock_mc.set_migration_flag_on_primary.assert_not_called()
        tgt_rpc.bdev_lvol_set_migration_flag.assert_not_called()
        for p in (*src_paths, *tgt_paths):
            p["rpc"].bdev_lvol_set_migration_flag.assert_not_called()


if __name__ == "__main__":
    unittest.main()
