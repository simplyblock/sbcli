# coding=utf-8
import time
from importlib import reload

import fdb

from simplyblock_core import constants, utils
from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)

logger.info("Starting node monitor")
MAX_RETRY = 6
retry = MAX_RETRY
error_open = False
snode_api = SNodeClient("0.0.0.0:5000")


while True:

    try:
        is_up, _ = snode_api.spdk_process_is_up()
    except:
        is_up = False

    if is_up:
        if error_open is True and retry <= 0:
            # snode_api.spdk_process_kill()
            error_open = False
            retry = MAX_RETRY
        else:
            try:
                reload(fdb)
                fdb.api_version(constants.KVD_DB_VERSION)
                db = fdb.open(constants.KVD_DB_FILE_PATH)
                db.options.set_transaction_timeout(5)
                d =  db.get_range_startswith(b"/",  limit=1)
                logger.info("Open db ok")
                error_open = False
                retry = MAX_RETRY
            except Exception as e:
                logger.error(e)
                logger.info("Open db error")
                error_open = True
                retry -= 1

    time.sleep(10)
