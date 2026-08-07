# coding=utf-8
"""Edge model persistence: bounded reads over the shared FDB keyspace.

Only point reads and cluster-prefixed range reads — the "no new table scans"
rule (docs/edge_clusters_analysis.md §1.3). Writes go through the models'
write_to_db / DBController.atomic_update like everywhere else.
"""
from typing import List, Optional

from simplyblock_core.db_controller import DBController
from simplyblock_edge.models import EdgeNode, EdgeVolume

_db = DBController()


def kv_store():
    return _db.kv_store


def atomic_update(obj, mutate_fn):
    return _db.atomic_update(obj, mutate_fn)


def get_edge_nodes(cluster_id: str) -> List[EdgeNode]:
    return EdgeNode().read_from_db(_db.kv_store, id=f"{cluster_id}/")


def get_edge_node_by_id(cluster_id: str, node_id: str) -> EdgeNode:
    nodes = EdgeNode().read_from_db(_db.kv_store, id=f"{cluster_id}/{node_id}")
    if not nodes:
        raise KeyError(f"EdgeNode not found: {node_id}")
    return nodes[0]


def get_edge_volumes(cluster_id: str) -> List[EdgeVolume]:
    return EdgeVolume().read_from_db(_db.kv_store, id=f"{cluster_id}/")


def get_edge_volume_by_id(cluster_id: str, volume_id: str) -> EdgeVolume:
    volumes = EdgeVolume().read_from_db(_db.kv_store, id=f"{cluster_id}/{volume_id}")
    if not volumes:
        raise KeyError(f"EdgeVolume not found: {volume_id}")
    return volumes[0]


def get_edge_volume_by_name(cluster_id: str, name: str) -> Optional[EdgeVolume]:
    for volume in get_edge_volumes(cluster_id):
        if volume.volume_name == name:
            return volume
    return None


def get_edge_clusters():
    """All clusters of type edge (the cluster table is small; this mirrors how
    every monitor sweeps clusters)."""
    from simplyblock_core.models.cluster import Cluster
    return [cluster for cluster in _db.get_clusters()
            if cluster.cluster_type == Cluster.TYPE_EDGE]


def get_cluster(cluster_id: str):
    return _db.get_cluster_by_id(cluster_id)
