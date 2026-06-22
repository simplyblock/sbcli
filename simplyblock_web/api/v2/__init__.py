from fastapi import APIRouter, Depends

from . import cluster
from . import management_node
from . import meta
from ._auth import verify_api_token

api = APIRouter()
api.include_router(cluster.api, prefix='/clusters', dependencies=[Depends(verify_api_token)])
api.include_router(management_node.api, prefix='/management-nodes', dependencies=[Depends(verify_api_token)])
api.include_router(meta.api, prefix='/_meta')
