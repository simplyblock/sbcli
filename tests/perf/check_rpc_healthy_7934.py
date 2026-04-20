from simplyblock_core.db_controller import DBController


TARGET_NODE = "7934a434-382e-4f09-be26-42057e7d885c"
CTRLS = [
    "remote_alceml_b398e52f-6bc9-467a-818d-10ab09ec75c4",
    "remote_alceml_eee80c47-e16e-42c9-91c9-3787483dcb98",
]
BDEVS = [
    "remote_alceml_b398e52f-6bc9-467a-818d-10ab09ec75c4n1",
    "remote_alceml_eee80c47-e16e-42c9-91c9-3787483dcb98n1",
]


def main():
    db = DBController()
    node = db.get_storage_node_by_id(TARGET_NODE)
    rpc = node.rpc_client()
    print(f"node={node.get_id()} {node.mgmt_ip}:{node.rpc_port}")
    for ctrl in CTRLS:
        ret, err = rpc.bdev_nvme_controller_list_2(ctrl)
        print(f"ctrl={ctrl} ret={ret} err={err}")
    for bdev in BDEVS:
        ret = rpc.get_bdevs(bdev)
        print(f"bdev={bdev} present={bool(ret)}")


if __name__ == "__main__":
    main()
