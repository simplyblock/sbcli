#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask

import utils
from simplyblock_core import constants
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


app.register_blueprint(snode_ops_k8s.bp)


app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

if __name__ == '__main__':
    app.run(host='0.0.0.0')
