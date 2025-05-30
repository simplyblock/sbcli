#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask, redirect

from simplyblock_web import utils
from simplyblock_web.api import v1
from simplyblock_web.auth_middleware import token_required
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)


core_utils.init_sentry_sdk()


app = Flask(__name__)
app.logger.setLevel(constants.LOG_WEB_LEVEL)
app.url_map.strict_slashes = False
app.register_error_handler(Exception, utils.error_handler)


# Add routes
app.register_blueprint(v1.api, path_prefix='/api/v1')


@app.before_request
@token_required
def before_request():
    pass


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


# Redirect unqualified URLs to the API
@app.route('/<path:path>')
def redirect_v1(path):
    return redirect('/api/v1/{path}', code=308)


app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
if __name__ == '__main__':
    logging.getLogger('werkzeug').setLevel(constants.LOG_WEB_LEVEL)
    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
