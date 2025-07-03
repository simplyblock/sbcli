from threading import Thread
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import caching_node_controller
from simplyblock_core import utils as core_utils
from simplyblock_core.models.caching_node import CachingNode as CachingNodeModel
from simplyblock_web import utils as web_utils

from .cluster import Cluster
from .dtos import CachingNodeDTO
from . import util


api = APIRouter(prefix='/caching_nodes')
db = DBController()


@api.get('/', name='clusters:caching_nodes:list')
def list(cluster: Cluster) -> List[CachingNodeDTO]:
    return [
        CachingNodeDTO.from_model(caching_node)
        for caching_node
        in db.get_caching_nodes_by_cluster_id(cluster.get_id())
    ]


class CachingNodeParams(BaseModel):
    node_ip: str = Field(pattern=web_utils.IP_PATTERN)
    interface_name: str
    data_nics: List[str] = Field([])
    spdk_mem: util.Size = Field(ge=core_utils.parse_size('1GiB'))
    spdk_cpu_mask: Optional[str]
    spdk_image: Optional[str]
    namespace: Optional[str]
    multipathing: bool = Field(True)


@api.post('/', name='clusters:caching_nodes:create', status_code=201, responses={201: {"content": None}})
def add(cluster: Cluster, parameters: CachingNodeParams) -> Response:
    Thread(
        target=caching_node_controller.add_node,
        kwargs={
            'cluster_id': cluster.get_id(),
            'node_ip': parameters.node_ip,
            'iface_name': parameters.interface_name,
            'data_nics_list': parameters.data_nics,
            'spdk_cpu_mask': parameters.spdk_cpu_mask,
            'spdk_mem': parameters.spdk_mem,
            'spdk_image': parameters.spdk_image,
            'namespace': parameters.namespace,
            'multipathing': parameters.multipathing,
        },
    ).start()

    return Response(status_code=201)  # FIXME: Provide URL for checking task status


instance_api = APIRouter(prefix='/<caching_node_id>')


def _caching_node_lookup(caching_node_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]) -> CachingNodeModel:
    caching_node = db.get_caching_node_by_id(caching_node_id)
    if caching_node is None:
        raise HTTPException(404, f'Caching Node {caching_node_id} not found')

    return caching_node


CachingNode = Annotated[CachingNodeModel, Depends(_caching_node_lookup)]


@instance_api.get('/', name='clusters:caching_nodes:detail')
def get(cluster: Cluster, caching_node: CachingNode) -> CachingNodeDTO:
    return CachingNodeDTO.from_model(caching_node)


class _LVolBody(BaseModel):
    lvol_id: str = Field(pattern=core_utils.UUID_PATTERN)


@instance_api.post('/connect', name='clusters:caching_nodes:connect')
def connect(cluster: Cluster, caching_node: CachingNode, parameters: _LVolBody):
    lvol = db.get_lvol_by_id(parameters.lvol_id)
    if lvol is None:
        raise HTTPException(404, f'LVol {parameters.lvol_id} not found')

    dev_path_or_false = caching_node_controller.connect(caching_node.get_id(), lvol.get_id())
    if not dev_path_or_false:
        raise ValueError('Failed to connect LVol')

    return dev_path_or_false


@instance_api.post('/disconnect', name='clusters:caching_nodes:disconnect', status_code=204, responses={204: {"content": None}})
def disconnect(cluster: Cluster, caching_node: CachingNode, parameters: _LVolBody) -> Response:
    lvol = db.get_lvol_by_id(parameters.lvol_id)
    if lvol is None:
        raise HTTPException(404, f'LVol {parameters.lvol_id} not found')

    if not caching_node_controller.disconnect(caching_node.get_id(), lvol.get_id()):
        raise ValueError('Failed to disconnect LVol')

    return Response(status_code=204)


@instance_api.get('/volumes', name='clusters:caching_nodes:volumes')
def list_lvols(cluster: Cluster, caching_node: CachingNode):
    return [
        {
            "UUID": clvol.lvol.get_id(),
            "Hostname": clvol.lvol.hostname,
            "Size": clvol.lvol.size,
            "Path": clvol.device_path,
            "Status": clvol.lvol.status,
        }
        for clvol
        in caching_node.lvols
    ]


@instance_api.post('/recreate', name='clusters:caching_nodes:recreate', status_code=204, responses={204: {"content": None}})
def recreate(cluster: Cluster, caching_node: CachingNode) -> Response:
    if not caching_node_controller.recreate(caching_node.get_id()):
        raise ValueError('Failed to recreate caching node')

    return Response(status_code=204)
