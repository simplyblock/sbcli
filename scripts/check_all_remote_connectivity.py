from simplyblock_core.db_controller import DBController


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"


def bdev_names(node):
    try:
        return {b["name"] for b in node.rpc_client().get_bdevs() or []}
    except Exception as exc:
        print(f"RPC_FAIL node={node.get_id()} ip={node.mgmt_ip} err={exc}")
        return None


db = DBController()
nodes = [n for n in db.get_storage_nodes_by_cluster_id(CLUSTER_ID) if n.status == "online"]
nodes_by_id = {n.get_id(): n for n in nodes}
names_by_node = {n.get_id(): bdev_names(n) for n in nodes}

failures = 0
for target in nodes:
    names = names_by_node[target.get_id()]
    if names is None:
        failures += 1
        continue

    for peer in nodes:
        if peer.get_id() == target.get_id():
            continue

        for dev in peer.nvme_devices:
            expected = f"remote_{dev.alceml_bdev}n1"
            in_db = any(
                rd.get_id() == dev.get_id() and rd.remote_bdev == expected
                for rd in target.remote_devices
            )
            in_spdk = expected in names
            if not in_db or not in_spdk:
                failures += 1
                print(
                    f"DATA_FAIL target={target.get_id()} peer={peer.get_id()} "
                    f"dev={dev.get_id()} expected={expected} in_db={in_db} in_spdk={in_spdk}"
                )

        expected_jm = f"remote_jm_{peer.get_id()}n1"
        in_db_jm = any(
            rjm.get_id() == peer.get_id() and rjm.remote_bdev == expected_jm
            for rjm in target.remote_jm_devices
        )
        in_spdk_jm = expected_jm in names
        if not in_db_jm or not in_spdk_jm:
            failures += 1
            print(
                f"JM_FAIL target={target.get_id()} peer={peer.get_id()} "
                f"expected={expected_jm} in_db={in_db_jm} in_spdk={in_spdk_jm}"
            )

print(f"checked_nodes={len(nodes)} failures={failures}")
raise SystemExit(1 if failures else 0)
