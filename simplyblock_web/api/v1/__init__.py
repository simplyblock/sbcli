import logging
import os

from flask import jsonify
from flask import Flask

from simplyblock_web.auth_middleware import token_required
from simplyblock_web import utils
from simplyblock_core import constants

from . import cluster
from . import mgmt_node
from . import device
from . import lvol
from . import snapshot
from . import storage_node
from . import pool
from . import swagger_ui
from . import metrics


api = Flask('API v1')
api.url_map.strict_slashes = False
api.logger.setLevel(logging.DEBUG)
api.register_blueprint(cluster.bp)
api.register_blueprint(mgmt_node.bp)
api.register_blueprint(device.bp)
api.register_blueprint(lvol.bp)
api.register_blueprint(snapshot.bp)
api.register_blueprint(storage_node.bp)
api.register_blueprint(pool.bp)
api.register_blueprint(swagger_ui.bp, url_prefix=swagger_ui.SWAGGER_URL)
api.register_blueprint(metrics.bp)


@api.before_request
@token_required
def before_request():
    pass


@api.route('/', methods=['GET'])
def status():
    return utils.get_response("Live")

@api.route('/health/fdb', methods=['GET'])
def health_fdb():
    fdb_cluster_file = constants.KVD_DB_FILE_PATH

    if not os.path.exists(fdb_cluster_file):
        return jsonify({
            "fdb_connected": False,
            "message": "FDB cluster file not found"
        }), 503

    try:
        with open(fdb_cluster_file, 'r') as f:
            cluster_data = f.read().strip()
            if not cluster_data:
                return jsonify({
                    "fdb_connected": False,
                    "message": "FDB cluster file is empty"
                }), 503
    except Exception as e:
        return jsonify({
            "fdb_connected": False,
            "message": f"Failed to read FDB cluster file: {str(e)}"
        }), 503

    return jsonify({
        "fdb_connected": True,
    }), 200
