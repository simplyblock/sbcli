"""Deleting a cluster with force brings it down completely: every storage
node is shut down and deleted without migrating anything off it, every pool
is deleted with the volumes and snapshots in it, and then the cluster
record. Without force, a cluster that still has nodes or pools is refused as
before."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import cluster_ops
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, status):
    node = SimpleNamespace(uuid=uuid, status=status, cluster_id="c1", remove=MagicMock())
    node.get_id = lambda: uuid
    return node


def _record(uuid):
    record = SimpleNamespace(uuid=uuid, remove=MagicMock())
    record.get_id = lambda: uuid
    return record


@pytest.fixture
def world():
    nodes = [_node("n1", StorageNode.STATUS_ONLINE), _node("n2", StorageNode.STATUS_OFFLINE)]
    pool = _record("p1")
    lvol, snap = _record("v1"), _record("s1")
    cluster = _record("c1")
    db = MagicMock()
    db.get_cluster_by_id.return_value = cluster
    state = {"nodes": list(nodes), "pools": [pool]}
    db.get_storage_nodes_by_cluster_id.side_effect = lambda _: list(state["nodes"])
    db.get_pools.side_effect = lambda _: list(state["pools"])
    db.get_lvols_by_pool_id.return_value = [lvol]
    db.get_snapshots_by_pool_id.return_value = [snap]
    return SimpleNamespace(db=db, nodes=nodes, pool=pool, lvol=lvol, snap=snap, cluster=cluster, state=state)


def test_without_force_a_cluster_with_nodes_is_refused(world):
    with patch.object(cluster_ops, "db_controller", world.db):
        with pytest.raises(ValueError, match="Storage nodes found"):
            cluster_ops.delete_cluster("c1")
    world.cluster.remove.assert_not_called()


def test_force_shuts_down_and_deletes_every_node_then_the_pools_and_the_cluster(world):
    removed = []

    def set_status(node_id, status, caused_by=""):
        assert status == StorageNode.STATUS_REMOVED
        removed.append(node_id)

    def delete_node(node_id, force=False):
        assert force and node_id in removed
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]

    def delete_pool_record(pool):
        world.state["pools"] = []

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.storage_node_ops, "shutdown_storage_node") as shutdown, \
            patch.object(cluster_ops.storage_node_ops, "set_node_status", side_effect=set_status), \
            patch.object(cluster_ops.storage_node_ops, "delete_storage_node", side_effect=delete_node), \
            patch.object(cluster_ops, "_remove_pool_record", side_effect=delete_pool_record):
        cluster_ops.delete_cluster("c1", force=True)

    # Only a node whose SPDK is up is shut down; an offline one has nothing to stop.
    shutdown.assert_called_once_with("n1", force=True)
    assert sorted(removed) == ["n1", "n2"]
    world.lvol.remove.assert_called_once()
    world.snap.remove.assert_called_once()
    world.cluster.remove.assert_called_once()


def test_a_shutdown_that_fails_does_not_stop_the_teardown(world):
    deleted = []

    def delete_node(node_id, force=False):
        deleted.append(node_id)
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.storage_node_ops, "shutdown_storage_node",
                         side_effect=RuntimeError("snode api unreachable")), \
            patch.object(cluster_ops.storage_node_ops, "set_node_status"), \
            patch.object(cluster_ops.storage_node_ops, "delete_storage_node", side_effect=delete_node), \
            patch.object(cluster_ops, "_remove_pool_record",
                         side_effect=lambda pool: world.state.update(pools=[])):
        cluster_ops.delete_cluster("c1", force=True)

    assert sorted(deleted) == ["n1", "n2"]
    world.cluster.remove.assert_called_once()
