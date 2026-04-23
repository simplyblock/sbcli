from simplyblock_core.db_controller import DBController


CLUSTER_ID = "10293de0-b91c-4618-b17a-5c3e688686f4"
MISMATCHES = [
    ("dbdda8a9-040a-4415-9f83-6236d3d7e552", "376d710d-de8a-4817-ba8d-cb87be45c933"),
    ("b2ec7653-1fc3-4cdb-a0b6-75fe1ed9b0bf", "c1fe8ce4-455d-45bc-b26d-0d3f8a266827"),
    ("1bec25a8-d815-45d2-ae76-b1bd6c21584b", "5655272f-fbc1-4b93-86cf-b80801d21251"),
]


db = DBController()
for target_id, dev_id in MISMATCHES:
    target = db.get_storage_node_by_id(target_id)
    dev = db.get_storage_device_by_id(dev_id)
    expected_prefix = f"remote_{dev.alceml_bdev}"
    in_db = any(rd.get_id() == dev_id for rd in target.remote_devices)
    rpc = target.rpc_client(timeout=5, retry=1)
    bdevs = rpc.get_bdevs()
    found = [b["name"] for b in bdevs or [] if b["name"].startswith(expected_prefix)]
    print(
        f"target={target_id} target_ip={target.mgmt_ip} dev={dev_id} "
        f"dev_node={dev.node_id} expected={expected_prefix} in_db={in_db} found_bdevs={found}"
    )
