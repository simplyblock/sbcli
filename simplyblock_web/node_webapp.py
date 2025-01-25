#!/usr/bin/env python
# encoding: utf-8
import argparse
import logging

from flask import Flask

import utils
from simplyblock_core import constants

logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)


app = Flask(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


MODES = [
    "storage_node",
    "caching_docker_node",
    "caching_kubernetes_node",
]

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=MODES)


if __name__ == '__main__':
    args = parser.parse_args()

    mode = args.mode
    if mode == "caching_docker_node":
        from blueprints import node_api_basic, node_api_caching_docker
        app.register_blueprint(node_api_basic.bp)
        app.register_blueprint(node_api_caching_docker.bp)

    if mode == "caching_kubernetes_node":
        from blueprints import node_api_basic, node_api_caching_ks
        app.register_blueprint(node_api_basic.bp)
        app.register_blueprint(node_api_caching_ks.bp)

    if mode == "storage_node":
        from blueprints import snode_ops
        app.register_blueprint(snode_ops.bp)

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
