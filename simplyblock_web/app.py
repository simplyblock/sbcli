#!/usr/bin/env python
# encoding: utf-8

import logging

import uvicorn

from simplyblock_web.api import public
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)


if __name__ == '__main__':
    logging.getLogger('werkzeug').setLevel(constants.LOG_WEB_LEVEL)
    uvicorn.run(public, root_path='/api')
