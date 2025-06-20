#!/usr/bin/env python
# encoding: utf-8
from simplyblock_core import utils as core_utils
import argparse

from flask_openapi3 import OpenAPI

from simplyblock_web import utils
from simplyblock_core import constants

logger = core_utils.get_logger(__name__)


app = OpenAPI(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.register_error_handler(Exception, utils.error_handler)


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


MODES = [
    "caching_docker_node",
    "caching_kubernetes_node",
    "storage_node",
    "storage_node_k8s",
]

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=MODES)


if __name__ == '__main__':
    args = parser.parse_args()

    mode = args.mode
    if mode == "caching_docker_node":
        from simplyblock_web.blueprints import node_api_basic, caching_node_ops
        app.register_api(node_api_basic.api)
        app.register_api(caching_node_ops.api)

    if mode == "caching_kubernetes_node":
        from simplyblock_web.blueprints import node_api_basic, caching_node_ops_k8s
        app.register_api(node_api_basic.api)
        app.register_api(caching_node_ops_k8s.api)

    if mode == "storage_node":
        from simplyblock_web.blueprints import snode_ops
        app.register_api(snode_ops.api)

    if mode == "storage_node_k8s":
        from simplyblock_web.blueprints import snode_ops_k8s
        app.register_api(snode_ops_k8s.api)

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
