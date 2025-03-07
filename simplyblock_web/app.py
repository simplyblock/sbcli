#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask

import utils
from blueprints import web_api_cluster, web_api_mgmt_node, web_api_device, \
    web_api_lvol, web_api_storage_node, web_api_pool, web_api_caching_node, \
    web_api_snapshot, web_api_deployer, swagger_ui_blueprint
from auth_middleware import token_required
from simplyblock_core import constants, utils as core_utils
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app

logger = core_utils.get_logger(__name__)



app = Flask(__name__)
app.logger.setLevel(constants.LOG_WEB_LEVEL)
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
app.register_blueprint(swagger_ui_blueprint.bp, url_prefix=swagger_ui_blueprint.SWAGGER_URL)


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
    logging.getLogger('werkzeug').setLevel(constants.LOG_WEB_LEVEL)
    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
