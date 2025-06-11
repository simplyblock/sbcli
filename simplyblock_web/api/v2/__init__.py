from flask_openapi3 import APIBlueprint

from . import caching_node
from . import cluster
from . import device
from . import lvol
from . import management_node
from . import pool
from . import snapshot
from . import storage_node


cluster.instance_api.register_api(caching_node.api, path_prefix='/caching_node')
cluster.instance_api.register_api(device.api, path_prefix='/device')
cluster.instance_api.register_api(lvol.api, path_prefix='/volume')
cluster.instance_api.register_api(management_node.api, path_prefix='/management_node')
cluster.instance_api.register_api(pool.api, path_prefix='/pool')
cluster.instance_api.register_api(snapshot.api, path_prefix='/snapshot')
cluster.instance_api.register_api(storage_node.api, path_prefix='/storage_node')


api = APIBlueprint('API v2', __name__)
api.register_api(cluster.api, path_prefix='/cluster')
