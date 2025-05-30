from flask import Blueprint

from simplyblock_web.auth_middleware import token_required

from . import cluster
from . import mgmt_node
from . import device
from . import lvol
from . import snapshot
from . import storage_node
from . import pool
from . import swagger_ui
from . import metrics


api = Blueprint('API v1', __name__, url_prefix='v1')
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
