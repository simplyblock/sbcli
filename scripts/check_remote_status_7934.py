from simplyblock_core.db_controller import DBController


TARGET_NODE = "7934a434-382e-4f09-be26-42057e7d885c"
DEVICE_IDS = {
    "b398e52f-6bc9-467a-818d-10ab09ec75c4",
    "eee80c47-e16e-42c9-91c9-3787483dcb98",
}


def main():
    db = DBController()
    node = db.get_storage_node_by_id(TARGET_NODE)
    print(f"target_node={node.get_id()} remote_devices={len(node.remote_devices)}")
    found = 0
    for rd in node.remote_devices:
        if rd.get_id() in DEVICE_IDS:
            found += 1
            print(
                f"device={rd.get_id()} status={rd.status} remote_bdev={rd.remote_bdev}"
            )
    if found == 0:
        print("none_found")


if __name__ == "__main__":
    main()
