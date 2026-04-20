from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice


MISMATCHES = [
    ("dbdda8a9-040a-4415-9f83-6236d3d7e552", "376d710d-de8a-4817-ba8d-cb87be45c933"),
    ("b2ec7653-1fc3-4cdb-a0b6-75fe1ed9b0bf", "c1fe8ce4-455d-45bc-b26d-0d3f8a266827"),
    ("1bec25a8-d815-45d2-ae76-b1bd6c21584b", "5655272f-fbc1-4b93-86cf-b80801d21251"),
]


db = DBController()
for target_id, dev_id in MISMATCHES:
    target = db.get_storage_node_by_id(target_id)
    dev = db.get_storage_device_by_id(dev_id)
    expected_bdev = f"remote_{dev.alceml_bdev}n1"
    if not target.rpc_client().get_bdevs(expected_bdev):
        print(f"skip target={target_id} dev={dev_id}: {expected_bdev} not found in SPDK")
        continue

    new_remote_devices = [rd for rd in target.remote_devices if rd.get_id() != dev_id]
    remote_device = RemoteDevice()
    remote_device.uuid = dev.uuid
    remote_device.alceml_name = dev.alceml_name
    remote_device.node_id = dev.node_id
    remote_device.size = dev.size
    remote_device.status = NVMeDevice.STATUS_ONLINE
    remote_device.nvmf_multipath = dev.nvmf_multipath
    remote_device.remote_bdev = expected_bdev
    new_remote_devices.append(remote_device)
    target.remote_devices = new_remote_devices
    target.write_to_db()
    print(f"upserted target={target_id} dev={dev_id} bdev={expected_bdev} count={len(target.remote_devices)}")
