"""test_activation_growth_guard.py — a cluster does not grow by re-activation.

Once activated, a cluster's node set is fixed. Nodes added afterwards are
integrated by the expansion flow (``sn add-node --expansion`` on an ACTIVE
cluster), which rotates roles and rebalances data. Re-activation would pull
them in with none of that.

The path that used to allow it: activate, suspend, add nodes, activate again.
The existing check only refused an activation of an already-ACTIVE cluster, and
a suspended cluster is not ACTIVE, so it walked straight past.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.models.cluster import Cluster


def _cluster(status=Cluster.STATUS_SUSPENDED, activated=()):
    cluster = MagicMock()
    cluster.status = status
    cluster.activated_node_ids = list(activated)
    cluster.get_id.return_value = "cl-1"
    return cluster


def _nodes(*ids):
    out = []
    for node_id in ids:
        node = MagicMock()
        node.get_id.return_value = node_id
        out.append(node)
    return out


class TestActivationRefusesAddedNodes(unittest.TestCase):

    def _activate(self, cluster, nodes):
        """Run only far enough to hit (or pass) the growth guard."""
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "set_cluster_status"), \
                patch.object(cluster_ops, "logger"):
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_nodes_by_cluster_id.return_value = nodes
            # Anything past the guard is out of scope here; a sentinel proves
            # the guard let the call through.
            with patch.object(cluster_ops, "utils") as utils:
                utils.set_storage_mcp_max_unavailable.side_effect = RuntimeError("past-guard")
                try:
                    cluster_ops._cluster_activate("cl-1")
                except ValueError as refused:
                    return str(refused)
                except Exception:
                    return None          # got past the guard
        return None

    def test_added_node_is_refused(self):
        cluster = _cluster(activated=("n1", "n2"))
        message = self._activate(cluster, _nodes("n1", "n2", "n3"))
        self.assertIsNotNone(message, "activation accepted an added node")
        self.assertIn("already been activated", message)
        self.assertIn("n3", message)
        self.assertIn("--expansion", message)

    def test_same_node_set_is_allowed(self):
        cluster = _cluster(activated=("n1", "n2"))
        self.assertIsNone(self._activate(cluster, _nodes("n1", "n2")))

    def test_fewer_nodes_is_allowed(self):
        """A removed node is not growth; activation may proceed."""
        cluster = _cluster(activated=("n1", "n2", "n3"))
        self.assertIsNone(self._activate(cluster, _nodes("n1", "n2")))

    def test_first_activation_is_allowed(self):
        cluster = _cluster(status=Cluster.STATUS_UNREADY, activated=())
        self.assertIsNone(self._activate(cluster, _nodes("n1", "n2", "n3")))

    def test_force_does_not_bypass_it(self):
        """Forcing would incorporate nodes with roles never rotated for them."""
        cluster = _cluster(activated=("n1",))
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "set_cluster_status"), \
                patch.object(cluster_ops, "logger"):
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_nodes_by_cluster_id.return_value = _nodes("n1", "n2")
            with self.assertRaises(ValueError):
                cluster_ops._cluster_activate("cl-1", force=True)


class TestRecordingTheActivatedSet(unittest.TestCase):

    def test_the_node_set_is_frozen_sorted(self):
        cluster = _cluster()
        with patch.object(cluster_ops, "db_controller") as db:
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_nodes_by_cluster_id.return_value = _nodes("n2", "n1")
            cluster_ops._record_activated_nodes("cl-1")
            db.atomic_update.assert_called_once()
            mutator = db.atomic_update.call_args[0][1]
        target = MagicMock()
        mutator(target)
        self.assertEqual(target.activated_node_ids, ["n1", "n2"])

    def test_bookkeeping_failure_is_swallowed(self):
        """An activation that succeeded must not fail over its own bookkeeping."""
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "logger"):
            db.get_cluster_by_id.side_effect = RuntimeError("db gone")
            cluster_ops._record_activated_nodes("cl-1")      # must not raise


if __name__ == "__main__":
    unittest.main()
