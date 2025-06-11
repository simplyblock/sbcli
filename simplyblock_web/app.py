#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask, redirect, request

from simplyblock_web import utils
from simplyblock_web.api import public_api
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)


core_utils.init_sentry_sdk()


app = Flask(__name__)
app.logger.setLevel(constants.LOG_WEB_LEVEL)
app.url_map.strict_slashes = False
app.register_error_handler(Exception, utils.error_handler)


# Add routes
app.register_blueprint(public_api, url_prefix='/api')


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


# Redirect unqualified URLs to the API
@app.before_request
def redirect_v1():
    if request.path.startswith('/api') or request.path.startswith('/static'):
        return None
    return redirect(f'/api/v1{request.path}' + ('?' + request.query_string.decode() if request.query_string else ''), code=308)


if __name__ == '__main__':
    logging.getLogger('werkzeug').setLevel(constants.LOG_WEB_LEVEL)
    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
