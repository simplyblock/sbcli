from flask_openapi3 import APIBlueprint

from . import caching_node
from . import cluster
from . import device
from . import lvol
from . import management_node
from . import pool
from . import snapshot
from . import storage_node
from .auth import api_token_required


storage_node.instance_api.register_api(device.api)
pool.instance_api.register_api(lvol.api)
pool.instance_api.register_api(snapshot.api)

cluster.instance_api.register_api(caching_node.api)
cluster.instance_api.register_api(pool.api)
cluster.instance_api.register_api(storage_node.api)


api = APIBlueprint(
        'API v2',
        __name__,
        url_prefix='v2',
        abp_security=[{'token_v2': []}],
)
api.register_api(cluster.api)
api.register_api(management_node.api)


@api.before_request
@api_token_required
def auth():
    pass
