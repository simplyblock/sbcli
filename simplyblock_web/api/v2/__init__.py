from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import cluster
from . import device
from . import volume
from . import management_node
from . import pool
from . import snapshot
from . import storage_node

from simplyblock_core.db_controller import DBController

_db = DBController()
security = HTTPBearer()


def _verify_api_token(
        credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
        cluster_id: Optional[str] = None,
):
    authorized_cluster_id = next((
        cluster.id
        for cluster
        in _db.get_clusters()
        if cluster.secret == credentials.credentials
    ), None)
    if (authorized_cluster_id is None) or (cluster_id is not None and cluster_id != authorized_cluster_id):
        raise HTTPException(401, 'Invalid token')

# Assemble routes here to avoid circular imports
device.api.include_router(device.instance_api)

storage_node.instance_api.include_router(device.api)
storage_node.api.include_router(storage_node.instance_api)

cluster.instance_api.include_router(storage_node.api)


volume.api.include_router(volume.instance_api)
pool.instance_api.include_router(volume.api)

snapshot.api.include_router(snapshot.instance_api)
pool.instance_api.include_router(snapshot.api)

pool.api.include_router(pool.instance_api)

cluster.instance_api.include_router(pool.api)


cluster.api.include_router(cluster.instance_api)
management_node.api.include_router(management_node.instance_api)

api = APIRouter(
    dependencies=[Depends(_verify_api_token)],
)
api.include_router(cluster.api)
api.include_router(management_node.api)
