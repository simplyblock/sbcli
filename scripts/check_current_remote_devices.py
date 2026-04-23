from simplyblock_core import db_controller


def main():
    db = db_controller.DBController()
    nodes = db.get_storage_nodes()
    devices = []
    for node in nodes:
        for dev in node.nvme_devices:
            devices.append((node.get_id(), dev.get_id(), dev.alceml_bdev, dev.status))

    for target in nodes:
        remote_by_id = {dev.get_id(): dev for dev in target.remote_devices}
        print(f"NODE {target.get_id()} status={target.status} remote_count={len(target.remote_devices)}")
        for owner_id, dev_id, alceml_bdev, status in devices:
            if owner_id == target.get_id():
                continue
            if status != "online":
                continue
            rem = remote_by_id.get(dev_id)
            if not rem:
                print(f"  MISSING {dev_id} owner={owner_id} expected=remote_{alceml_bdev}n1")
            elif rem.status != "online":
                print(f"  BAD_STATUS {dev_id} owner={owner_id} remote={rem.remote_bdev} status={rem.status}")


if __name__ == "__main__":
    main()
