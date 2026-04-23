from simplyblock_core.db_controller import DBController


TARGET_ID = "dbdda8a9-040a-4415-9f83-6236d3d7e552"
DEV_ID = "376d710d-de8a-4817-ba8d-cb87be45c933"


db = DBController()
target = db.get_storage_node_by_id(TARGET_ID)
for rd in target.remote_devices:
    if rd.get_id() == DEV_ID:
        print(rd.to_dict())
        break
else:
    print("not found")
