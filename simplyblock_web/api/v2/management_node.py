from ipaddress import IPv4Address
from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils as core_utils
from simplyblock_core.models.mgmt_node import MgmtNode

from .cluster import Cluster

api = APIRouter(prefix='/management_nodes')
db = DBController()


class ManagementNodeDTO(BaseModel):
    id: UUID
    status: str
    hostname: str
    ip: IPv4Address

    @staticmethod
    def from_model(model: MgmtNode):
        return ManagementNodeDTO(
            id=UUID(model.get_id()),
            status=model.status,
            hostname=model.hostname,
            ip=IPv4Address(model.mgmt_ip),
        )


@api.get('/')
def list(cluster: Cluster) -> List[ManagementNodeDTO]:
    return [
        ManagementNodeDTO.from_model(management_node)
        for management_node
        in db.get_mgmt_nodes()
        if management_node.cluster_id == cluster.get_id()
    ]


instance_api = APIRouter(prefix='/<management_node_id>')


def _lookup_management_node(management_node_id: Annotated[str, StringConstraints(core_utils.UUID_PATTERN)]) -> MgmtNode:
    management_node = db.get_mgmt_node_by_id(management_node_id)
    if management_node is None:
        raise HTTPException(404, f'ManagementNode {management_node_id} not found')

    return management_node


ManagementNode = Annotated[MgmtNode, Depends(_lookup_management_node)]


@instance_api.get('/')
def get(cluster: Cluster, management_node: ManagementNode) -> ManagementNodeDTO:
    return ManagementNodeDTO.from_model(management_node)


api.include_router(instance_api)
