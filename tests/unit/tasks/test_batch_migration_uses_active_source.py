"""The batch orchestrator reads from the group's ACTIVE source, not the primary.

For the whole of a node removal the drained primary is stopped and
create_batch_migration pins an online replica as group.active_source_node_id.
The orchestrator ignored that field: its liveness guard looked at the stopped
primary (status migrating_lvols), suspended five times and failed the group on
every target the operator tried (2026-09-28, run 6: four targets burnt, the
drain stuck at "1 of 6 volumes migrated"). RPCs, the guard and the hub go to
the active source; lvstore names and the replica set stay the primary's.

Pure logic tests: the DB and RPC clients are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup

import simplyblock_core.services.tasks_runner_batch_migration as runner


def _node(node_id, lvstore, secondary="", tertiary=""):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvstore = lvstore
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    return n


def _group(source="primary", active=""):
    g = LVolMigrationGroup()
    g.uuid = "group-1"
    g.source_node_id = source
    g.active_source_node_id = active
    g.target_node_id = "tgt"
    g.target_nqn = "nqn.test:shared"
    return g


class TestGroupSourceNodes(unittest.TestCase):

    def test_no_fallback_means_the_primary_is_the_source(self):
        primary = _node("primary", "LVS_1")
        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.side_effect = lambda i: {"primary": primary}[i]
        with patch.object(runner, "db", mock_db):
            p, s = runner._group_source_nodes(_group())
        self.assertIs(p, primary)
        self.assertIs(s, primary)

    def test_fallback_source_is_read_from_the_group(self):
        primary = _node("primary", "LVS_1")
        replica = _node("replica", "LVS_7")
        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.side_effect = lambda i: {"primary": primary, "replica": replica}[i]
        with patch.object(runner, "db", mock_db):
            p, s = runner._group_source_nodes(_group(active="replica"))
        self.assertIs(p, primary)
        self.assertIs(s, replica)

    def test_missing_active_source_raises_like_a_missing_primary(self):
        primary = _node("primary", "LVS_1")
        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.side_effect = lambda i: {"primary": primary}[i]
        with patch.object(runner, "db", mock_db), self.assertRaises(KeyError):
            runner._group_source_nodes(_group(active="gone"))


class TestBdevNamesFollowThePrimary(unittest.TestCase):

    def test_final_args_name_the_source_bdev_after_the_primary_lvstore(self):
        primary = _node("primary", "LVS_1")
        replica = _node("replica", "LVS_7")
        tgt = _node("tgt", "LVS_10")
        m = MagicMock()
        m.uuid = "mig-1"
        m.lvol_id = "lvol-1"
        m.snaps_migrated = []
        m.snaps_preexisting_on_target = []
        group = _group(active="replica")
        group.ordered_migration_ids = MagicMock(return_value=["mig-1"])
        lvol = MagicMock()
        lvol.lvol_bdev = "LVOL_1"
        tgt_short = runner._lvol_tgt_bdev_name("LVOL_1")
        tgt_rpc = MagicMock()
        tgt_rpc.bdev_lvol_get_lvols.return_value = [{"name": "LVS_10/" + tgt_short, "map_id": 42}]
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        with patch.object(runner, "db", mock_db):
            names, ids, snaps = runner._build_batch_final_args(
                group, [m], replica, tgt, tgt_rpc, primary_src_node=primary)
        self.assertEqual(names, ["LVS_1/LVOL_1"])  # the primary's lvstore, read via the replica
        self.assertEqual(ids, [42])
        self.assertEqual(snaps, [""])


class TestSourceSubsystemCleanupWithAFallbackSource(unittest.TestCase):

    def test_the_active_replica_is_deleted_once_and_the_other_replica_too(self):
        primary = _node("primary", "LVS_1", secondary="replica", tertiary="third")
        replica = _node("replica", "LVS_7")
        third = _node("third", "LVS_9")
        tgt = _node("tgt", "LVS_10")
        src_rpc, tgt_rpc, third_rpc = MagicMock(), MagicMock(), MagicMock()
        mock_db = MagicMock()
        mock_db.get_storage_node_by_id.side_effect = lambda i: {"replica": replica, "third": third}[i]
        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_build_paths", return_value=([], [], set())), \
             patch.object(runner, "_get_source_tertiary_node", return_value=third), \
             patch.object(runner, "_make_rpc", return_value=third_rpc):
            runner._delete_source_subsystem(_group(active="replica"), replica, src_rpc, tgt, tgt_rpc,
                                            primary_src_node=primary)
        src_rpc.subsystem_delete.assert_called_once_with("nqn.test:shared")
        third_rpc.subsystem_delete.assert_called_once_with("nqn.test:shared")
        # the secondary IS the active source: not looked up and not deleted a second time
        mock_db.get_storage_node_by_id.assert_not_called()


if __name__ == "__main__":
    unittest.main()
