#!/usr/bin/env python
# encoding: utf-8

import argparse
import json
import traceback

from flask_openapi3 import OpenAPI
from flask import Response
from werkzeug.exceptions import HTTPException

from simplyblock_web import utils
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)


app = OpenAPI(__name__)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True


@app.errorhandler(Exception)
def handle_exception(exception: Exception):
    """Return JSON instead of HTML for any exception."""
    # start with the correct headers and status code from the error

    return {
        **{
            attr: getattr(exception, attr)
            for attr in ['code', 'name', 'description']
            if hasattr(exception, attr)
        },
        'stacktrace': [
            (frame.filename, frame.lineno, frame.name, frame.line)
            for frame
            in traceback.extract_tb(exception.__traceback__)
        ]
    }, exception.code if hasattr(exception, 'code') else 500


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
        from simplyblock_web.blueprints import node_api_basic, node_api_caching_docker
        app.register_api(node_api_basic.api)
        app.register_api(node_api_caching_docker.api)

    if mode == "caching_kubernetes_node":
        from simplyblock_web.blueprints import node_api_basic, node_api_caching_ks
        app.register_api(node_api_basic.api)
        app.register_api(node_api_caching_ks.api)

    if mode == "storage_node":
        from simplyblock_web.blueprints import snode_ops
        app.register_api(snode_ops.api)

    if mode == "storage_node_k8s":
        from simplyblock_web.blueprints import snode_ops_k8s
        app.register_api(snode_ops_k8s.api)

    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
