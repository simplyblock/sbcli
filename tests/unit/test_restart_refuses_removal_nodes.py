"""A node the removal has shut down is never restarted, even by force.

A restart would bring it back into service in the middle of its removal --
devices failed or migrated, volumes moving, journal and replicas being
dismantled. The paths that could do it: a full-cluster graceful start, the
operator's node recycle (which calls the restart endpoint with force), and
`sbctl sn restart --force`.
"""

from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, status):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    return n


@pytest.mark.parametrize("status", StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
def test_a_forced_restart_is_refused_for_a_node_in_removal(status):
    db = MagicMock()
    db.get_storage_node_by_id.return_value = _node("n1", status)
    impl = MagicMock(return_value=True)
    with patch.object(storage_node_ops, "DBController", return_value=db), \
            patch.object(storage_node_ops, "_restart_storage_node_impl", impl):
        assert storage_node_ops.restart_storage_node("n1", force=True) is False
    impl.assert_not_called()


def test_graceful_start_leaves_removal_nodes_alone():
    nodes = [_node("up", StorageNode.STATUS_OFFLINE),
             _node("leaving", StorageNode.STATUS_MIGRATING_LVOLS),
             _node("gone", StorageNode.STATUS_REMOVED)]
    db = MagicMock()
    db.get_cluster_by_id.return_value = MagicMock(status="active")
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    db.get_storage_node_by_id.side_effect = lambda nid: _node(nid, StorageNode.STATUS_ONLINE)
    with patch.object(cluster_ops, "db_controller", db), \
            patch.object(cluster_ops.storage_node_ops, "shutdown_storage_node") as shutdown, \
            patch.object(cluster_ops.storage_node_ops, "restart_storage_node") as restart:
        cluster_ops.cluster_grace_startup("c1")
    assert [c.args[0] for c in shutdown.call_args_list] == ["up"]
    assert [c.args[0] for c in restart.call_args_list] == ["up"]
