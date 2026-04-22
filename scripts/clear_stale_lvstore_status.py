from simplyblock_core import db_controller
from simplyblock_core.models.storage_node import StorageNode


db = db_controller.DBController()
clusters = db.get_clusters()
changed = []

for cluster in clusters:
    for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
        if (
            node.status == StorageNode.STATUS_ONLINE
            and node.lvstore_status == "in_creation"
            and not node.restart_phases
        ):
            node.lvstore_status = "ready"
            node.write_to_db()
            changed.append(node.get_id())

print(changed)
