"""Deleting a cluster with force brings it down completely: every storage
node is shut down and deleted without migrating anything off it, every pool
is deleted with the volumes and snapshots in it, and then the cluster
record. Without force, a cluster that still has nodes or pools is refused as
before. Only one forced delete of a cluster runs at a time: the operator
repeats the request while the first one is still working, and a second teardown
racing the first kills SPDK on the same nodes through the same agents."""

import threading
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

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
    db.acquire_lvstore_lock.return_value = (True, None)
    return SimpleNamespace(db=db, nodes=nodes, pool=pool, lvol=lvol, snap=snap, cluster=cluster, state=state)


def test_without_force_a_cluster_with_nodes_is_refused(world):
    with patch.object(cluster_ops, "db_controller", world.db):
        with pytest.raises(ValueError, match="Storage nodes found"):
            cluster_ops.delete_cluster("c1")
    world.cluster.remove.assert_not_called()


def test_force_deletes_the_pools_before_the_nodes_then_the_cluster(world):
    """The pools go first, with the nodes still up and the cluster's KMS
    configuration still there for their keys; then every node, whatever its
    status; then the cluster."""
    order = []

    def delete_pool(pool_id):
        order.append(("pool", pool_id))
        world.state["pools"] = []

    def delete_node(node_id):
        order.append(("node", node_id))
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]
        return True

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.pool_controller, "force_delete_pool", side_effect=delete_pool), \
            patch.object(cluster_ops.storage_node_ops, "force_delete_storage_node", side_effect=delete_node):
        cluster_ops.delete_cluster("c1", force=True)

    assert order == [("pool", "p1"), ("node", "n1"), ("node", "n2")]
    world.cluster.remove.assert_called_once()


def test_a_node_whose_spdk_stop_is_not_confirmed_is_reported_and_the_teardown_goes_on(world, caplog):
    def delete_node(node_id):
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]
        return node_id != "n1"

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.pool_controller, "force_delete_pool",
                         side_effect=lambda _id: world.state.update(pools=[])), \
            patch.object(cluster_ops.storage_node_ops, "force_delete_storage_node", side_effect=delete_node):
        cluster_ops.delete_cluster("c1", force=True)

    assert any("n1" in r.getMessage() and r.levelname == "ERROR" for r in caplog.records)
    world.cluster.remove.assert_called_once()


def _forced_delete(world, *, delete_node=None, delete_pool=None):
    """Run a forced delete with the pool and node removal stubbed."""
    def default_pool(_id):
        world.state["pools"] = []

    def default_node(node_id):
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]
        return True

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.pool_controller, "force_delete_pool", side_effect=delete_pool or default_pool), \
            patch.object(cluster_ops.storage_node_ops, "force_delete_storage_node",
                         side_effect=delete_node or default_node) as node_delete:
        cluster_ops.delete_cluster("c1", force=True)
    return node_delete


def test_a_forced_delete_already_running_refuses_the_second_request_and_starts_no_teardown(world):
    """The operator repeats its DELETE every 30 seconds while the first is still
    working. A second teardown killing SPDK on the same nodes through the same
    agents is what overloads them, so the repeat is refused with a conflict the
    caller retries."""
    world.db.acquire_lvstore_lock.return_value = (False, "cluster-delete:other")

    with patch.object(cluster_ops, "db_controller", world.db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops.pool_controller, "force_delete_pool") as delete_pool, \
            patch.object(cluster_ops.storage_node_ops, "force_delete_storage_node") as delete_node:
        with pytest.raises(ValueError, match="already in progress"):
            cluster_ops.delete_cluster("c1", force=True)

    delete_pool.assert_not_called()
    delete_node.assert_not_called()
    world.cluster.remove.assert_not_called()


def test_the_lock_is_held_for_the_whole_teardown_and_released_after_it(world):
    held_during = []

    def delete_node(node_id):
        held_during.append(world.db.release_lvstore_lock.call_count == 0)
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]
        return True

    _forced_delete(world, delete_node=delete_node)

    assert held_during == [True, True]
    (cluster_id, name, owner), _ = world.db.acquire_lvstore_lock.call_args
    assert cluster_id == "c1" and name == cluster_ops.CLUSTER_DELETE_LOCK
    world.db.release_lvstore_lock.assert_called_once_with("c1", cluster_ops.CLUSTER_DELETE_LOCK, owner)


def test_the_lock_is_released_when_the_teardown_fails(world):
    def delete_node(_node_id):
        raise RuntimeError("the node API is gone")

    with pytest.raises(RuntimeError):
        _forced_delete(world, delete_node=delete_node)

    world.db.release_lvstore_lock.assert_called_once()
    world.cluster.remove.assert_not_called()


def test_the_lock_is_refreshed_while_a_slow_teardown_runs(world):
    """A teardown outlives the lock's TTL when an agent answers slowly, and a
    lock that lapsed mid-teardown would let the next request start a second one."""
    refreshed = threading.Event()
    world.db.refresh_lvstore_lock.side_effect = lambda *_: refreshed.set() or True

    def delete_node(node_id):
        assert refreshed.wait(5), "the lock was not refreshed while the teardown ran"
        world.state["nodes"] = [n for n in world.state["nodes"] if n.get_id() != node_id]
        return True

    with patch.object(cluster_ops.constants, "LVSTORE_MUTATION_LOCK_HEARTBEAT_SEC", 0.05):
        _forced_delete(world, delete_node=delete_node)

    world.db.refresh_lvstore_lock.assert_called_with("c1", cluster_ops.CLUSTER_DELETE_LOCK, ANY)


def test_deleting_an_empty_cluster_without_force_takes_no_lock(world):
    world.state.update(nodes=[], pools=[])

    with patch.object(cluster_ops, "db_controller", world.db), patch.object(cluster_ops, "cluster_events"):
        cluster_ops.delete_cluster("c1")

    world.db.acquire_lvstore_lock.assert_not_called()
    world.cluster.remove.assert_called_once()
