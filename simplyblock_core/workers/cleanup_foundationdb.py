import time
import datetime

from simplyblock_core import cluster_ops, constants
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.kv_store import KVStore, DBController

cl_list = cluster_ops.list()

#lvols = lvol_controller.list_lvols()
db_controller = DBController()
db_controller.get_pools()

def PoolStatObject(lvols, st_date, end_date):
    for lv_ob in lvols:
        index = "object/PoolStatObject/%s/%s/" % (lv_ob.pool_uuid, lv_ob.pool_uuid)

        start = index + st_date
        end = index + end_date

        fdb = KVStore()
        fdb.db.clear_range(start, end)

def LVolStatObject(lvols, st_date, end_date):

    for lv_ob in lvols:
        index = "object/LVolStatObject/%s/%s/" % (lv_ob.pool_uuid, lv_ob.uuid)

        start = index + st_date
        end = index + end_date

        fdb = KVStore()
        fdb.db.clear_range(start, end)

def DeviceStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            for device in node.nvme_devices:
                device_id = device.get_id()

                index = "object/DeviceStatObject/%s/%s/" % (cluster_id, device_id)

                start = index + st_date
                end = index + end_date

                fdb = KVStore()
                fdb.db.clear_range(start, end)

def NodeStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            node_id = node.get_id()
            index = "object/NodeStatObject/%s/%s/" % (cluster_id, node_id)

            start = index + st_date
            end = index + end_date

            fdb = KVStore()
            fdb.db.clear_range(start, end)
def ClusterStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        index = "object/ClusterStatObject/%s/%s/" % (cluster_id, cluster_id)

        start = index + st_date
        end = index + end_date

        fdb = KVStore()
        fdb.db.clear_range(start, end)
while True:
    clusters = db_controller.get_clusters()
    lvols = lvol_controller.list_lvols(True)
    st_date = time.time() # seconds
    end_date = st_date - datetime.timedelta(days=1)

    LVolStatObject(lvols,st_date,end_date)
    PoolStatObject(lvols,st_date,end_date)  
    DeviceStatObject(clusters,st_date,end_date)
    NodeStatObject(clusters, st_date, end_date)
    ClusterStatObject(clusters, st_date, end_date)
    time.sleep(constants.FDB_CHECK_INTERVAL_SEC)