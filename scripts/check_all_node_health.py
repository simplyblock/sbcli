from simplyblock_core.controllers import health_controller
from simplyblock_core.db_controller import DBController


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"


db = DBController()
failed = []
for node in db.get_storage_nodes_by_cluster_id(CLUSTER_ID):
    ok = health_controller.check_node(node.get_id())
    print(f"{node.get_id()} {node.mgmt_ip} status={node.status} db_health={node.health_check} direct_health={ok}")
    if not ok:
        failed.append(node.get_id())

if failed:
    print("FAILED:", ",".join(failed))
    raise SystemExit(1)

print("ALL_OK")
