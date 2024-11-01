# coding=utf-8
import json
import logging

import time
import sys

from simplyblock_core import constants, kv_store, utils
from simplyblock_core.models.stats import DeviceStatObject, NodeStatObject, ClusterStatObject

# Import the GELF logger
from graypy import GELFTCPHandler

# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFTCPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_controller = kv_store.DBController()

logger.info("Starting Log aggregation service ...")
while True:

    clusters = db_controller.get_clusters()
    for cl in clusters:
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        if not snodes:
            logger.error(f"Cluster has no storage nodes: {cl.get_id()}")

        node_records = []
        for node in snodes:
            logger.info("Node: %s", node.get_id())
            for device in node.nvme_devices:
                logger.info("Getting device stats: %s", device.uuid)

                f_record = DeviceStatObject().read_from_db(
                    db_controller.kv_store, id="%s/%s" % (device.cluster_id, device.get_id()), limit=1)

                date = f_record[0].date
                date = int(date/5)*5

                for i in range(12):
                    st_date = date + (i*5)
                    end_date = date + ((i+1)*5)
                    objects = f_record.get_range(db_controller.kv_store, st_date, end_date)
                    print(len(objects))
                    if objects:
                        print(",".join([r.date for r in objects]))
                        new_rec = utils.sum_records(objects)
                        new_rec.date = st_date
                        new_rec.record_duration = 5
                        new_rec.record_start_time = st_date
                        new_rec.record_end_time = end_date
                        # new_rec.write_to_db(db_controller.kv_store)
                        # for rec in objects:
                        #     rec.remove(db_controller.kv_store)
                    else:
                        break

    time.sleep(constants.DEV_STAT_COLLECTOR_INTERVAL_SEC)
    exit(0)
