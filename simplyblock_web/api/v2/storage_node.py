from threading import Thread
from typing import Annotated, List, Optional

from flask import abort, url_for
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import tasks_controller
from simplyblock_core import storage_node_ops, utils as core_utils
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_web import utils as web_utils

from . import util as util
from .cluster import ClusterPath


api = APIBlueprint('storage_node', __name__, url_prefix='/storage_nodes')
db = DBController()


class _ForceDefaultTrueQuery(BaseModel):
    force: bool = Field(True)


@api.get('/')
def list(path: ClusterPath):
    return [
        storage_node.get_clean_dict()
        for storage_node
        in db.get_storage_nodes_by_cluster_id(path.cluster_id)
    ]


class StorageNodeParams(BaseModel):
    node_address: Annotated[str, Field(web_utils.IP_PATTERN)]
    interface_name: str
    max_snapshots: int = Field(500)
    ha_jm: bool = Field(True)
    test_device: bool = Field(False)
    spdk_image: Optional[str]
    spdk_debug: bool = Field(False)
    full_page_unmap: bool = Field(False)
    data_nics: List[str] = Field([])
    namespace: str = Field('default')
    jm_percent: int = Field(3, ge=0, le=100)
    partitions: int = Field(1)
    iobuf_small_pool_count: int = Field(0)
    iobuf_large_pool_count: int = Field(0)


@api.put('/')
def add(path: ClusterPath, body: StorageNodeParams):
    cluster = path.cluster()
    task_id_or_false = tasks_controller.add_node_add_task(
        path.cluster_id,
        {
            'cluster_id': cluster.get_id(),
            'node_addr': body.node_address,
            'iface_name': body.interface_name,
            'data_nics_list': body.data_nics,
            'max_snap': body.max_snapshots,
            'spdk_image': body.spdk_image,
            'spdk_debug': body.spdk_debug,
            'small_bufsize': body.iobuf_small_pool_count,
            'large_bufsize': body.iobuf_large_pool_count,
            'num_partitions_per_dev': body.partitions,
            'jm_percent': body.jm_percent,
            'enable_test_device': body.test_device,
            'namespace': body.namespace,
            'enable_ha_jm': body.ha_jm,
            'full_page_unmap': body.full_page_unmap,
        }
    )
    if not task_id_or_false:
        raise ValueError('Failed to create add-node task')

    task_url = url_for('api.v2.cluster.task.get', cluster_id=cluster.get_id(), task_id=task_id_or_false)
    return '', 201, {'Location': task_url}


instance_api = APIBlueprint('instance', __name__, url_prefix='/<storage_node_id>')


class StorageNodePath(ClusterPath):
    storage_node_id: Annotated[str, Field(core_utils.UUID_PATTERN)]

    def storage_node(self) -> StorageNode:
        storage_node = db.get_storage_node_by_id(self.storage_node_id)
        if storage_node is None:
            abort(404)

        return storage_node


@instance_api.get('/')
def get(path: StorageNodePath):
    return path.storage_node().get_clean_dict()


@instance_api.delete('/')
def delete(path: StorageNodePath):
    none_or_false = storage_node_ops.remove_storage_node(path.storage_node().get_id())
    if none_or_false == False:  # noqa
        raise ValueError('Failed to remove storage node')


@instance_api.get('/capacity')
def capacity(path: StorageNodePath, query: util.HistoryQuery):
    storage_node = path.storage_node()
    records_or_false = storage_node_ops.get_node_iostats_history(
        storage_node.get_id(),
        query.history,
        parse_sizes=False,
        with_sizes=True
    )
    if not records_or_false:
        raise ValueError('Failed to compute capacity')
    return records_or_false


@instance_api.get('/iostats')
def iostats(path: StorageNodePath, query: util.HistoryQuery):
    storage_node = path.storage_node()
    records_or_false = storage_node_ops.get_node_iostats_history(
            storage_node.get_id(),
            query.history,
            parse_sizes=False,
            with_sizes=True
    )
    if not records_or_false:
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.get('/nic')
def nics(path: StorageNodePath):
    storage_node = path.storage_node()
    return [
        {
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.get_transport_type(),
            "Status": nic.status,
        }
        for nic in storage_node.data_nics
    ]


class _NICPath(StorageNodePath):
    nic_id: str


@instance_api.get('/nic/<nic_id>/iostats')
def nic_iostats(path: _NICPath):
    storage_node = path.storage_node()
    nic = next((
        nic
        for nic
        in storage_node.data_nics
        if nic.get_id() == path.nic_id
    ), None)
    if nic is None:
        abort(404)

    return [
        record.get_clean_dict()
        for record in db.get_port_stats(storage_node.get_id(), nic.get_id())
    ]


@instance_api.post('/suspend')
def suspend(path: StorageNodePath, query: _ForceDefaultTrueQuery):
    storage_node = path.storage_node()
    if not storage_node_ops.suspend_storage_node(storage_node.get_id(), query.force):
        raise ValueError('Failed to suspend storage node')


@instance_api.post('/resume')
def resume(path: StorageNodePath):
    storage_node = path.storage_node()
    if not storage_node_ops.resume_storage_node(storage_node.get_id()):
        raise ValueError('Failed to resume storage node')


@instance_api.post('/shutdown')
def shutdown(path: StorageNodePath, query: _ForceDefaultTrueQuery):
    storage_node = path.storage_node()
    Thread(
        target=storage_node_ops.shutdown_storage_node,
        args=(storage_node.get_id(), query.force)
    ).start()

    return None, 201  # FIXME: Provide URL for checking task status


class _RestartQuery(BaseModel):
    force: bool = Field(False)
    reattach_volume: bool = Field(False)


@instance_api.post('/start')  # Same as restart for now
@instance_api.post('/restart')
def restart(path: StorageNodePath, query: _RestartQuery):
    storage_node = path.storage_node()
    Thread(
        target=storage_node_ops.restart_storage_node,
        kwargs={
            "node_id": storage_node.get_id(),
            "node_ip": storage_node.mgmt_ip,
            "force": query.force,
            "reattach_volume": query.reattach_volume,
        }
    ).start()

    return None, 201  # FIXME: Provide URL for checking task status


api.register_api(instance_api)
