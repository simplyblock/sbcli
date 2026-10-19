"""_build_paths must be told which node is the PRIMARY whenever the migration
reads from a replica standing in for it.

SRC paths are built from the primary's lvstore ports and the primary's own
secondary/tertiary; with a fallback source, src_node is that replica and
without ``primary_src_node`` the paths carried the replica's lvstore and the
replica's replicas. The batch orchestrator and the solo runner's data phases
passed it; the target-side cleanup did not, so its overlap set (which target
nodes also serve a SRC path and must not be torn down) was computed from the
wrong node. Asserted on the source: every _build_paths call in the two runners
names the primary.
"""
import inspect
import re
import unittest

import simplyblock_core.services.tasks_runner_batch_migration as batch
import simplyblock_core.services.tasks_runner_lvol_migration as solo


def _calls(module):
    src = inspect.getsource(module)
    return [m.group(0) for m in re.finditer(r"_build_paths\((?:[^()]|\([^()]*\))*\)", src)
            if not m.group(0).startswith("_build_paths(src_node, tgt_node, src_rpc, tgt_rpc, primary_src_node=None)")]


class TestEveryBuildPathsCallNamesThePrimary(unittest.TestCase):

    def test_solo_runner(self):
        calls = [c for c in _calls(solo) if "def _build_paths" not in c]
        self.assertTrue(calls)
        for c in calls:
            self.assertIn("primary_src_node=", c, c)

    def test_batch_orchestrator(self):
        calls = _calls(batch)
        self.assertTrue(calls)
        for c in calls:
            self.assertIn("primary_src_node=", c, c)

    def test_cleanup_target_receives_the_primary_from_both_dispatchers(self):
        src = inspect.getsource(solo)
        calls = [m.group(0) for m in re.finditer(r"_handle_cleanup_target\((?:[^()]|\([^()]*\))*\)", src)
                 if not src[max(0, m.start() - 4):m.start()].endswith("def ")]
        self.assertEqual(len(calls), 2, calls)
        for c in calls:
            self.assertIn("primary_src_node=primary_src_node", c, c)


if __name__ == "__main__":
    unittest.main()
