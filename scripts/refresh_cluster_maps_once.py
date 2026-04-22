from simplyblock_core import distr_controller
from simplyblock_core.db_controller import DBController


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"


db = DBController()
for node in db.get_storage_nodes_by_cluster_id(CLUSTER_ID):
    if node.status not in ["online", "down"]:
        print(f"skip {node.get_id()} status={node.status}")
        continue
    print(f"refresh {node.get_id()} status={node.status}")
    distr_controller.send_cluster_map_to_node(node)
