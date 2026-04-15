#!/usr/bin/env python
# encoding: utf-8
import argparse
import os

from flask_openapi3 import OpenAPI
from flask_httpauth import HTTPBasicAuth

from simplyblock_core import constants
from simplyblock_core import utils as core_utils
from simplyblock_web import utils
from simplyblock_web.api import internal as internal_api

logger = core_utils.get_logger(__name__)

security_schemes = {
    "basicAuth": {
        "type": "http",
        "scheme": "basic"
    }
}
app = OpenAPI(__name__, security_schemes=security_schemes)
app.url_map.strict_slashes = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.register_error_handler(Exception, utils.error_handler)

auth = HTTPBasicAuth()
app.basic_auth = auth
@auth.verify_password
def verify_password(username, password):
    access_token = os.getenv("SNODE_ACCESS_TOKEN", "access_token")
    if username == "token" and password == access_token:
        return True

@app.before_request
@auth.login_required
def before_request():
    pass

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
