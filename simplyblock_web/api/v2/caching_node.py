from threading import Thread
from typing import List, Optional

from flask import abort, jsonify
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import caching_node_controller
from simplyblock_core import utils as core_utils
from simplyblock_web import utils as web_utils

from .cluster import ClusterPath


api = APIBlueprint('caching node', __name__, url_prefix='/cluster/<cluster_id>/caching_node')
db = DBController()


@api.get('/')
def list(path: ClusterPath):
    return [
        caching_node.get_clean_dict()
        for caching_node
        in db.get_caching_nodes_by_cluster_id(path.cluster_id)
    ]


class CachingNodeParams(BaseModel):
    node_ip: str = Field(pattern=web_utils.IP_PATTERN)
    interface_name: str
    data_nics: List[str] = Field([])
    spdk_mem: int = Field(ge=core_utils.parse_size('1GiB'))
    spdk_cpu_mask: Optional[str]
    spdk_image: Optional[str]
    namespace: Optional[str]
    multipathing: bool = Field(True)


@api.put('/')
def add(path: ClusterPath, body: CachingNodeParams):
    Thread(
        target=caching_node_controller.add_node,
        kwargs={
            'cluster_id': path.cluster_id,
            'node_ip': body.node_ip,
            'iface_name': body.interface_name,
            'data_nics_list': body.data_nics,
            'spdk_cpu_mask': body.spdk_cpu_mask,
            'spdk_mem': body.spdk_mem,
            'spdk_image': body.spdk_image,
            'namespace': body.namespace,
            'multipathing': body.multipathing,
        },
    ).start()

    return None, 201  # FIXME: Provide URL for checking task status


instance_api = APIBlueprint('caching node instance', __name__, url_prefix='/<caching_node_id>')


class CachingNodePath(ClusterPath):
    caching_node_id: str = Field(pattern=core_utils.UUID_PATTERN)


@instance_api.get('/')
def get(path: CachingNodePath):
    caching_node = db.get_caching_node_by_id(path.caching_node_id)
    if caching_node is None:
        abort(404)

    return caching_node.get_clean_dict()


class _LVolBody(BaseModel):
    lvol_id: str = Field(pattern=core_utils.UUID_PATTERN)


@instance_api.put('/connect')
def connect(path: CachingNodePath, body: _LVolBody):
    caching_node = db.get_caching_node_by_id(path.caching_node_id)
    if caching_node is None:
        abort(404)

    lvol = db.get_lvol_by_id(body.lvol_id)
    if lvol is None:
        abort(404)

    dev_path_or_false = caching_node_controller.connect(caching_node.get_id(), lvol.get_id())
    if not dev_path_or_false:
        raise ValueError('Failed to connect LVol')

    return jsonify(dev_path_or_false)


@instance_api.put('/disconnect')
def disconnect(path: CachingNodePath, body: _LVolBody):
    caching_node = db.get_caching_node_by_id(path.caching_node_id)
    if caching_node is None:
        abort(404)

    lvol = db.get_lvol_by_id(body.lvol_id)
    if lvol is None:
        abort(404)

    if not caching_node_controller.disconnect(caching_node.get_id(), lvol.get_id()):
        raise ValueError('Failed to disconnect LVol')


@instance_api.get('/lvol')
def list_lvols(path: CachingNodePath):
    caching_node = db.get_caching_node_by_id(path.caching_node_id)
    if caching_node is None:
        abort(404)

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


@instance_api.put('/recreate')
def recreate(path: CachingNodePath):
    caching_node = db.get_caching_node_by_id(path.caching_node_id)
    if caching_node is None:
        abort(404)

    if not caching_node_controller.recreate(caching_node.get_id()):
        raise ValueError('Failed to recreate caching node')


api.register_api(instance_api)
