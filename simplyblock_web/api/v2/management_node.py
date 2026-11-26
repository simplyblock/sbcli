import builtins
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from simplyblock_core import mgmt_node_ops
from simplyblock_core.db_controller import DBController

from . import util
from ._dependencies import Cluster, ManagementNode
from ._dtos import ManagementNodeDTO

api = APIRouter()
db = DBController()


class ManagementNodeParams(BaseModel):
    """A control plane registering itself (docs/stretched-control-plane.md):
    on Kubernetes the operator calls this from the joining site, where no
    host exists to run ``control-plane add`` on."""
    #: The site this control plane runs in; required on a stretched control
    #: plane, refused otherwise.
    site: str = ""
    #: Where the other site's standby control plane redirects mutating calls.
    api_endpoint: str = ""
    #: The management address recorded for the node; the Kubernetes placeholder by default.
    mgmt_ip: str = "0.0.0.0"
    mode: str = Field("kubernetes", pattern="^(kubernetes|docker)$")


@api.get('/', name='management_nodes:list')
def list(cluster: Cluster) -> builtins.list[ManagementNodeDTO]:
    return [
        ManagementNodeDTO.from_model(management_node)
        for management_node
        in db.get_mgmt_nodes()
        if management_node.cluster_id == cluster.get_id()
    ]


@api.post('/', name='management_nodes:create', status_code=201,
          responses={400: {"description": "The site is missing, not allowed or malformed (detail.message)"},
                     409: {"description": "A management node with that address already exists"}})
def add(request: Request, cluster: Cluster, parameters: ManagementNodeParams,
        response_format: util.CreationResponseFormatParameter = "full"):
    """Register a management node of ``cluster``: the second site's control
    plane of a stretched cluster announces itself with its ``site`` and the
    API endpoint the other site redirects to."""
    try:
        node_id = mgmt_node_ops.add_mgmt_node(
            parameters.mgmt_ip, parameters.mode, cluster.get_id(),
            site=parameters.site, site_api_endpoint=parameters.api_endpoint)
    except ValueError as e:
        raise util.sync_error(400, str(e)) from e
    if not node_id:
        raise HTTPException(409, f"a management node with address {parameters.mgmt_ip} already exists")
    node = db.get_mgmt_node_by_id(node_id)
    return util.creation_response(
        request, response_format,
        entity_id=UUID(node.get_id()),
        route_name='management_node:detail',
        route_kwargs={'management_node_id': UUID(node.get_id())},
        get_full=lambda _: ManagementNodeDTO.from_model(node),
    )


instance_api = APIRouter(prefix='/{management_node_id}')


@instance_api.get('/', name='management_node:detail')
def get(cluster: Cluster, management_node: ManagementNode) -> ManagementNodeDTO:
    return ManagementNodeDTO.from_model(management_node)


api.include_router(instance_api)
