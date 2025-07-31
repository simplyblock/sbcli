#!/usr/bin/env python
# encoding: utf-8
from simplyblock_core import utils as core_utils
import argparse

from flask_openapi3 import OpenAPI

from simplyblock_core import constants
from simplyblock_web import utils
from simplyblock_web.api import internal as internal_api

logger = core_utils.get_logger(__name__)


app = OpenAPI(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.register_error_handler(Exception, utils.error_handler)


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


MODES = [
    "storage_node",
    "storage_node_k8s",
]

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=MODES)


if __name__ == '__main__':
    args = parser.parse_args()

    mode = args.mode
    if mode == "storage_node":
        app.register_api(internal_api.storage_node.docker.api)

    if mode == "storage_node_k8s":
        app.register_api(internal_api.storage_node.kubernetes.api)

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
