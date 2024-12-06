#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask, request
import math
import os
import sys
import utils

from simplyblock_core import utils as sbc_utils, constants
from blueprints import snode_ops_k8s

logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(constants.LOG_LEVEL)


app = Flask(__name__)


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


@app.route('/prepare-cores', methods=['POST'])
def prepare_cores_api():
    """Handle prepare_cores via a web API."""
    data = request.json
    prepare_cores(data)
    return utils.get_response("Done")

def prepare_cores(data):
    cpu_count = os.cpu_count()
    spdk_cpu_mask = data.get('spdk_cpu_mask')
    if not spdk_cpu_mask:
        spdk_cpu_mask = hex(int(math.pow(2, cpu_count))-2)
    logger.info("Preparing spdk cores")
    sbc_utils.prepare_cores(spdk_cpu_mask)


app.register_blueprint(snode_ops_k8s.bp)


app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

if __name__ == '__main__':
    spdk_cpu_mask = None
    if len(os.environ.get("CPUMASK")) > 0:
        spdk_cpu_mask = os.environ.get("CPUMASK")
    data = {"spdk_cpu_mask": spdk_cpu_mask}
    prepare_cores(data)
    app.run(host='0.0.0.0')
