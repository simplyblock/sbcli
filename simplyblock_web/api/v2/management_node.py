from flask import abort
from flask_openapi3 import APIBlueprint
from pydantic import Field

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils as core_utils
from simplyblock_core.models.mgmt_node import MgmtNode

from .cluster import ClusterPath

api = APIBlueprint('management node', __name__, url_prefix='/management_nodes')
db = DBController()


@api.get('/')
def list(path: ClusterPath):
    return [
        management_node.get_clean_dict()
        for management_node
        in db.get_mgmt_nodes()
        if management_node.cluster_id == path.cluster_id
    ]


instance_api = APIBlueprint('management node instance', __name__, url_prefix='/<management_node_id>')


class ManagementNodePath(ClusterPath):
    management_node_id: str = Field(core_utils.UUID_PATTERN)

    def management_node(self) -> MgmtNode:
        management_node = db.get_mgmt_node_by_id(self.storage_node_id)
        if management_node is None:
            abort(404)

        return management_node


@instance_api.get('/')
def get(path: ClusterPath):
    return db.get_mgmt_node_by_id(path.management_node().get_id()).get_clean_dict()


api.register_api(instance_api)
