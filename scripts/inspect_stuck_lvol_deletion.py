from simplyblock_core.db_controller import DBController


LVIDS = [
    "a5fd8d4e-764c-462e-9729-925b6b12fcf5",
    "05114aac-fbe8-49bd-856d-28a3db579c9e",
]


db = DBController()
for lvid in LVIDS:
    lvol = db.get_lvol_by_id(lvid)
    print(
        f"LVOL id={lvid} name={lvol.lvol_name} status={lvol.status} "
        f"deletion_status={lvol.deletion_status} top_bdev={lvol.top_bdev} "
        f"base_bdev={lvol.base_bdev} nqn={lvol.nqn}"
    )
    for node_id in lvol.nodes:
        node = db.get_storage_node_by_id(node_id)
        try:
            bdevs = node.rpc_client().get_bdevs() or []
            bdev_names = {b["name"] for b in bdevs}
            subs = node.rpc_client().subsystem_list() or []
            matching_subs = [
                s for s in subs
                if s.get("nqn") == lvol.nqn or s.get("nqn", "").endswith(f":lvol:{lvid}")
            ]
            lvstores = node.rpc_client().bdev_lvol_get_lvstores(lvol.lvs_name) or []
            print(
                f"  node={node_id} ip={node.mgmt_ip} status={node.status} "
                f"top={lvol.top_bdev in bdev_names} base={lvol.base_bdev in bdev_names} "
                f"subsys={len(matching_subs)} lvstores={lvstores}"
            )
        except Exception as exc:
            print(f"  node={node_id} ip={node.mgmt_ip} RPC_FAIL {exc}")
