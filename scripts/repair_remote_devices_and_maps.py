from simplyblock_core import distr_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import _connect_to_remote_devs


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"


db = DBController()
for node in db.get_storage_nodes_by_cluster_id(CLUSTER_ID):
    if node.status != StorageNode.STATUS_ONLINE:
        print(f"skip remote-devices {node.get_id()} status={node.status}")
        continue
    node = db.get_storage_node_by_id(node.get_id())
    before = len(node.remote_devices)
    node.remote_devices = _connect_to_remote_devs(node, force_connect_restarting_nodes=True)
    after = len(node.remote_devices)
    node.write_to_db()
    print(f"remote-devices {node.get_id()} {before}->{after}")

for node in db.get_storage_nodes_by_cluster_id(CLUSTER_ID):
    if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
        print(f"skip map {node.get_id()} status={node.status}")
        continue
    print(f"refresh-map {node.get_id()}")
    distr_controller.send_cluster_map_to_node(node)
