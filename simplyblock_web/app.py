#!/usr/bin/env python
# encoding: utf-8

import logging

from fastapi import FastAPI
import uvicorn

from simplyblock_web.api import public
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)
logger.setLevel(constants.LOG_WEB_LEVEL)
logging.getLogger().setLevel(constants.LOG_WEB_LEVEL)


core_utils.init_sentry_sdk()


app = FastAPI()
app.include_router(public, prefix='/api')


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=5000, log_level='debug', forwarded_allow_ips='192.168.1.0/24')
