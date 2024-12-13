# coding=utf-8
import json
import os
import time

from simplyblock_core import utils
from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)

logger.info("Starting node monitor")

snode_api = SNodeClient("0.0.0.0:5000")
MAX_RETRY = 6
retry = MAX_RETRY
db_available = True

while True:

    try:
        is_up, _ = snode_api.spdk_process_is_up()
    except:
        is_up = False

    if is_up:
        if db_available is False and retry <= 0:
            snode_api.spdk_process_kill()
            retry = MAX_RETRY

        else:
            try:
                data = os.popen("fdbcli --exec \"status json\"").read().strip()
                data = json.loads(data)
                db_available = data["client"]['database_status']['available']
            except Exception as e:
                logger.error(e)
                db_available = False

            if db_available:
                logger.info("Open db ok")
                retry = MAX_RETRY
            else:
                logger.info("Open db error")
                retry -= 1

    time.sleep(10)
