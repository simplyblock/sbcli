from fastapi import APIRouter, Depends

from . import cluster
from . import backup
from . import device
from . import volume
from . import management_node
from . import pool
from . import snapshot
from . import storage_node
from . import task
from . import migration
from . import meta
from ._auth import verify_api_token

# Assemble routes here to avoid circular imports
device.api.include_router(device.instance_api)

storage_node.instance_api.include_router(device.api)
storage_node.api.include_router(storage_node.instance_api)

cluster.instance_api.include_router(storage_node.api)

task.api.include_router(task.instance_api)

cluster.instance_api.include_router(task.api)

migration.api.include_router(migration.instance_api)
volume.instance_api.include_router(migration.api)

volume.api.include_router(volume.instance_api)
pool.instance_api.include_router(volume.api)

snapshot.api.include_router(snapshot.instance_api)
pool.instance_api.include_router(snapshot.api)

pool.api.include_router(pool.instance_api)

cluster.instance_api.include_router(pool.api)


backup.api.include_router(backup.policy_api)
cluster.instance_api.include_router(backup.api)

cluster.api.include_router(cluster.instance_api)
management_node.api.include_router(management_node.instance_api)

api = APIRouter()
api.include_router(cluster.api, dependencies=[Depends(verify_api_token)])
api.include_router(management_node.api, dependencies=[Depends(verify_api_token)])
api.include_router(meta.api, prefix="/_meta")
