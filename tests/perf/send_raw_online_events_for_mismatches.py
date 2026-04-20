import datetime

from simplyblock_core.db_controller import DBController


MISMATCHES = [
    ("dbdda8a9-040a-4415-9f83-6236d3d7e552", "376d710d-de8a-4817-ba8d-cb87be45c933"),
    ("b2ec7653-1fc3-4cdb-a0b6-75fe1ed9b0bf", "c1fe8ce4-455d-45bc-b26d-0d3f8a266827"),
    ("1bec25a8-d815-45d2-ae76-b1bd6c21584b", "5655272f-fbc1-4b93-86cf-b80801d21251"),
]


db = DBController()
for target_id, dev_id in MISMATCHES:
    target = db.get_storage_node_by_id(target_id)
    dev = db.get_storage_device_by_id(dev_id)
    events = {
        "events": [{
            "timestamp": datetime.datetime.now().isoformat("T", "seconds") + "Z",
            "event_type": "device_status",
            "storage_ID": dev.cluster_device_order,
            "status": "online",
        }]
    }
    print(f"raw online event target={target_id} dev={dev_id} storage_ID={dev.cluster_device_order}")
    target.rpc_client(timeout=5, retry=1).distr_status_events_update(events)
