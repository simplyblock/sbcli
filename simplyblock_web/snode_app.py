#!/usr/bin/env python
# encoding: utf-8

import logging
from flask import Flask

import utils
from simplyblock_core import constants

logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(constants.LOG_LEVEL)


app = Flask(__name__)


@app.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")


# Add snode_ops routes
from blueprints import snode_ops
app.register_blueprint(snode_ops.bp)


app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=constants.LOG_WEB_DEBUG)
