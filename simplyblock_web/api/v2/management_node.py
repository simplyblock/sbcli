from typing import List

from fastapi import APIRouter

from simplyblock_core.db_controller import DBController

from ._dependencies import Cluster, ManagementNode
from ._dtos import ManagementNodeDTO


api = APIRouter()
db = DBController()


@api.get('/', name='management_nodes:list')
def list(cluster: Cluster) -> List[ManagementNodeDTO]:
    return [
        ManagementNodeDTO.from_model(management_node)
        for management_node
        in db.get_mgmt_nodes()
        if management_node.cluster_id == cluster.get_id()
    ]


instance_api = APIRouter(prefix='/{management_node_id}')


@instance_api.get('/', name='management_node:detail')
def get(cluster: Cluster, management_node: ManagementNode) -> ManagementNodeDTO:
    return ManagementNodeDTO.from_model(management_node)


api.include_router(instance_api)
