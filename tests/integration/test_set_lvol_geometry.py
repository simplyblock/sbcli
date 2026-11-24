"""`cluster set-lvol-geometry` persists Cluster.lvol_geometry_enabled."""

from simplyblock_core import cluster_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster


def test_disable_then_enable():
    db = DBController()
    cluster = Cluster()
    cluster.uuid = "cluster-lvol-geometry"
    cluster.write_to_db(db.kv_store)

    cluster_ops.set_lvol_geometry(cluster.uuid, enable=False)
    assert db.get_cluster_by_id(cluster.uuid).lvol_geometry_enabled is False

    cluster_ops.set_lvol_geometry(cluster.uuid, enable=True)
    assert db.get_cluster_by_id(cluster.uuid).lvol_geometry_enabled is True
