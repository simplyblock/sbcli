"""Cluster-level LVol geometry switch (``Cluster.lvol_geometry_enabled``).

Enabled (default): a new lvol takes the explicit ndcs/npcs, else the
cluster's distr_ndcs/distr_npcs. Disabled: every new lvol is 0x0.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.controllers.lvol_controller import _lvol_geometry
from simplyblock_core.models.cluster import Cluster


def _cluster(enabled=True):
    c = Cluster()
    c.uuid = "cluster-1"
    c.distr_ndcs = 2
    c.distr_npcs = 1
    c.lvol_geometry_enabled = enabled
    return c


class TestLvolGeometry(unittest.TestCase):

    def test_enabled_inherits_cluster_geometry(self):
        self.assertEqual(_lvol_geometry(_cluster(), 0, 0), (2, 1))

    def test_enabled_explicit_values_win(self):
        self.assertEqual(_lvol_geometry(_cluster(), 4, 2), (4, 2))
        self.assertEqual(_lvol_geometry(_cluster(), 1, None), (1, 0))

    def test_disabled_is_always_zero(self):
        self.assertEqual(_lvol_geometry(_cluster(False), 0, 0), (0, 0))
        self.assertEqual(_lvol_geometry(_cluster(False), 4, 2), (0, 0))


class TestSetLvolGeometry(unittest.TestCase):

    def _run(self, cluster, enable):
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.atomic_update.side_effect = lambda obj, fn: fn(cluster)
        with patch.object(cluster_ops, "db_controller", db):
            cluster_ops.set_lvol_geometry("cluster-1", enable=enable)

    def test_disable_then_enable(self):
        c = _cluster()
        self._run(c, False)
        self.assertFalse(c.lvol_geometry_enabled)
        self._run(c, True)
        self.assertTrue(c.lvol_geometry_enabled)


class TestCli(unittest.TestCase):

    def _parse(self, argv):
        from simplyblock_cli.cli import CLIWrapper
        with patch("sys.argv", ["sbctl", *argv]):
            return CLIWrapper().parser.parse_args(argv)

    def test_create_and_add_flag(self):
        for cmd in (["cluster", "add"], ["cluster", "create"]):
            self.assertFalse(self._parse(cmd).disable_lvol_geometry)
            self.assertTrue(self._parse([*cmd, "--disable-lvol-geometry"]).disable_lvol_geometry)

    def test_set_lvol_geometry_dispatch(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        args = self._parse(["cluster", "set-lvol-geometry", "cluster-1", "--disable"])
        with patch("simplyblock_cli.clibase.cluster_ops.set_lvol_geometry") as fn:
            self.assertTrue(CLIWrapperBase.cluster__set_lvol_geometry(MagicMock(), None, args))
        fn.assert_called_once_with("cluster-1", enable=False)
