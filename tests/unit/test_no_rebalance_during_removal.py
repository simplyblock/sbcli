"""A cluster rebalance must not start while a node is leaving.

A rebalance spreads data across the CURRENT placement. A removal is in the
middle of changing it: the departing node's slots are already marked dead
(storage_ID=-1) in every peer's cluster map, so the rebalance migrations run
against them and fail -- "mig error: 576" -- with max_retry=-1, i.e. for ever.

That deadlocks the removal that made the rebalance pointless. An unfinished
balancing master keeps the cluster in REBALANCING; create_migration refuses to
run there ("Cluster ... is rebalancing; wait for it to finish before
migrating"); so the removal's volume drain can never start; so the node can
never finish leaving; so the placement never settles.

Cluster a6e7569d, 2026-09-15: recovering an unrelated node started
balancing_on_restart with two subtasks that reached 182 retries in ~20 minutes,
progress restarting from zero each lap, while the removal sat suspended behind
them.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.cluster_id = "c1"
    node.lvstore_stack = [{"type": "bdev_distr", "name": f"distr_{node_id}"}]
    return node


def _rebalance(nodes):
    """Returns the subtask names the rebalance would create."""
    created = []
    db = MagicMock()
    db.get_storage_node_by_id.return_value = nodes[0]
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    db.get_job_tasks.return_value = []
    with patch.object(tasks_controller, "db", db), \
         patch.object(tasks_controller, "_add_task",
                      side_effect=lambda *a, **k: created.append(a[3]) or "t"), \
         patch("simplyblock_core.models.base_model.BaseModel.write_to_db"), \
         patch.object(tasks_controller.tasks_events, "task_create"):
        result = tasks_controller.add_device_mig_task_for_node(nodes[0].get_id())
    return result, created


class TestRebalanceStandsDownForARemoval(unittest.TestCase):

    def test_no_rebalance_while_any_node_is_departing(self):
        for status in StorageNode.DEPARTING_STATUSES:
            result, created = _rebalance([_node("healthy", StorageNode.STATUS_ONLINE),
                                          _node("leaving", status)])
            self.assertFalse(result, status)
            self.assertEqual(created, [], f"{status}: placement is still moving")

    def test_migrating_lvols_specifically(self):
        """The status the live deadlock was in."""
        result, created = _rebalance([_node("healthy", StorageNode.STATUS_ONLINE),
                                      _node("a1b050f1", StorageNode.STATUS_MIGRATING_LVOLS)])
        self.assertFalse(result)
        self.assertEqual(created, [])

    def test_the_departing_node_need_not_be_the_trigger(self):
        """The node whose recovery triggers the rebalance is a different,
        healthy node -- that is exactly how this happened."""
        result, created = _rebalance([_node("recovered", StorageNode.STATUS_ONLINE),
                                      _node("other", StorageNode.STATUS_ONLINE),
                                      _node("leaving", StorageNode.STATUS_IN_REMOVAL)])
        self.assertFalse(result)
        self.assertEqual(created, [])


class TestNormalRebalancesStillRun(unittest.TestCase):

    def test_a_settled_cluster_rebalances(self):
        result, created = _rebalance([_node("n1", StorageNode.STATUS_ONLINE),
                                      _node("n2", StorageNode.STATUS_ONLINE)])
        self.assertTrue(result)
        self.assertEqual(created, ["distr_n1", "distr_n2"])

    def test_a_merely_down_node_does_not_block_it(self):
        """DOWN/OFFLINE nodes can come back; they are not a topology change."""
        for status in (StorageNode.STATUS_DOWN, StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE):
            result, created = _rebalance([_node("n1", StorageNode.STATUS_ONLINE),
                                          _node("n2", status)])
            self.assertTrue(result, status)
            self.assertIn("distr_n1", created, status)


if __name__ == "__main__":
    unittest.main()
