#!/usr/bin/env python
# encoding: utf-8

import logging
import sys
from flask import Flask

import utils
from blueprints import web_api_cluster, web_api_mgmt_node, web_api_device, \
    web_api_lvol, web_api_storage_node, web_api_pool, web_api_caching_node, web_api_snapshot, web_api_deployer
from auth_middleware import token_required
from simplyblock_core import constants
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app

logger_handler = logging.StreamHandler(sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)


app = Flask(__name__)
app.url_map.strict_slashes = False


# Add routes
app.register_blueprint(web_api_cluster.bp)
app.register_blueprint(web_api_mgmt_node.bp)
app.register_blueprint(web_api_device.bp)
app.register_blueprint(web_api_lvol.bp)
app.register_blueprint(web_api_snapshot.bp)
app.register_blueprint(web_api_storage_node.bp)
app.register_blueprint(web_api_pool.bp)
app.register_blueprint(web_api_caching_node.bp)
app.register_blueprint(web_api_deployer.bp)


@app.before_request
@token_required
def before_request():
    pass


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/cluster/metrics': make_wsgi_app()
})

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
