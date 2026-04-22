from simplyblock_core import distr_controller
from simplyblock_core.db_controller import DBController


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"


def distribs_from_stack(stack):
    for bdev in stack:
        if bdev["type"] == "bdev_raid":
            return bdev["distribs_list"]
    return []


db = DBController()
nodes = {n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(CLUSTER_ID)}
devices = {}
for node in nodes.values():
    for dev in node.nvme_devices:
        devices[dev.get_id()] = dev

for primary in nodes.values():
    if not primary.lvstore_stack:
        continue

    check_nodes = [primary]
    for peer_id in [primary.secondary_node_id, primary.tertiary_node_id]:
        if peer_id and peer_id in nodes:
            check_nodes.append(nodes[peer_id])

    for target in check_nodes:
        for distr in distribs_from_stack(primary.lvstore_stack):
            try:
                ret = target.rpc_client().distr_get_cluster_map(distr)
            except Exception as exc:
                print(f"RPC_FAIL primary={primary.get_id()} target={target.get_id()} distr={distr} err={exc}")
                continue
            if not ret:
                print(f"NO_MAP primary={primary.get_id()} target={target.get_id()} distr={distr}")
                continue
            results, passed = distr_controller.parse_distr_cluster_map(ret, nodes, devices)
            if passed:
                continue
            print(f"MAP_FAIL primary={primary.get_id()} target={target.get_id()} distr={distr}")
            for row in results:
                if row["Results"] != "ok":
                    print(
                        "  "
                        f"{row['Kind']} {row['UUID']} "
                        f"found={row['Found Status']} desired={row['Desired Status']} "
                        f"result={row['Results']}"
                    )
