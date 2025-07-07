from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

from .cluster import Cluster
from .dtos import ManagementNodeDTO

api = APIRouter(prefix='/management-nodes')
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


def _lookup_management_node(management_node_id: UUID) -> MgmtNode:
    management_node = db.get_mgmt_node_by_id(str(management_node_id))
    if management_node is None:
        raise HTTPException(404, f'ManagementNode {management_node_id} not found')

    return management_node


ManagementNode = Annotated[MgmtNode, Depends(_lookup_management_node)]


@instance_api.get('/', name='management_node:detail')
def get(cluster: Cluster, management_node: ManagementNode) -> ManagementNodeDTO:
    return ManagementNodeDTO.from_model(management_node)
