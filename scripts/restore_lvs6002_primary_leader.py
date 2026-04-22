from simplyblock_core.db_controller import DBController


NODE_ID = "1bec25a8-d815-45d2-ae76-b1bd6c21584b"
LVS_NAME = "LVS_6002"


db = DBController()
node = db.get_storage_node_by_id(NODE_ID)
rpc = node.rpc_client()
print(f"before={rpc.bdev_lvol_get_lvstores(LVS_NAME)}")
ret = rpc.bdev_lvol_set_leader(LVS_NAME, leader=True)
print(f"set_leader_ret={ret}")
print(f"after={rpc.bdev_lvol_get_lvstores(LVS_NAME)}")
