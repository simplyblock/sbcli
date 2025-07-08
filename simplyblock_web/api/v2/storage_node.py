from threading import Thread
from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import tasks_controller
from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode as StorageNodeModel
from simplyblock_web import utils as web_utils

from . import util as util
from .cluster import Cluster
from .dtos import StorageNodeDTO


api = APIRouter(prefix='/storage-nodes')
db = DBController()


@api.get('/', name='clusters:storage-nodes:list')
def list(cluster: Cluster) -> List[StorageNodeDTO]:
    return [
        StorageNodeDTO.from_model(storage_node)
        for storage_node
        in db.get_storage_nodes_by_cluster_id(cluster.get_id())
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
    jm_percent: util.Percent = Field(3)
    partitions: int = Field(1)
    iobuf_small_pool_count: int = Field(0)
    iobuf_large_pool_count: int = Field(0)


@api.post('/', name='clusters:storage-nodes:create', status_code=201, responses={201: {"content": None}})
def add(request: Request, cluster: Cluster, parameters: StorageNodeParams) -> Response:
    task_id_or_false = tasks_controller.add_node_add_task(
        cluster.get_id(),
        {
            'cluster_id': cluster.get_id(),
            'node_addr': parameters.node_address,
            'iface_name': parameters.interface_name,
            'data_nics_list': parameters.data_nics,
            'max_snap': parameters.max_snapshots,
            'spdk_image': parameters.spdk_image,
            'spdk_debug': parameters.spdk_debug,
            'small_bufsize': parameters.iobuf_small_pool_count,
            'large_bufsize': parameters.iobuf_large_pool_count,
            'num_partitions_per_dev': parameters.partitions,
            'jm_percent': parameters.jm_percent,
            'enable_test_device': parameters.test_device,
            'namespace': parameters.namespace,
            'enable_ha_jm': parameters.ha_jm,
            'full_page_unmap': parameters.full_page_unmap,
        }
    )
    if not task_id_or_false:
        raise ValueError('Failed to create add-node task')

    task_url = request.app.url_path_for('clusters:storage-nodes:detail', cluster_id=cluster.get_id(), task_id=task_id_or_false)
    return Response(status_code=201, headers={'Location': task_url})


instance_api = APIRouter(prefix='/{storage_node_id}')


def _lookup_storage_node(storage_node_id: UUID) -> StorageNodeModel:
    try:
        return db.get_storage_node_by_id(str(storage_node_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


StorageNode = Annotated[StorageNodeModel, Depends(_lookup_storage_node)]


@instance_api.get('/', name='clusters:storage-nodes:detail')
def get(cluster: Cluster, storage_node: StorageNode):
    return StorageNodeDTO.from_model(storage_node)


@instance_api.delete('/', name='clusters:storage-nodes:delete')
def delete(
        cluster: Cluster, storage_node: StorageNode, force_remove: bool = False, force_migrate: bool = False) -> Response:
    none_or_false = storage_node_ops.remove_storage_node(
            storage_node.get_id(), force_remove=force_remove, force_migrate=force_migrate
    )
    if none_or_false == False:  # noqa
        raise ValueError('Failed to remove storage node')

    return Response(status_code=204)


@instance_api.get('/capacity', name='clusters:storage-nodes:capacity')
def capacity(cluster: Cluster, storage_node: StorageNode, history: Optional[str] = None):
    storage_node = storage_node
    records_or_false = storage_node_ops.get_node_iostats_history(
        storage_node.get_id(),
        history,
        parse_sizes=False,
        with_sizes=True
    )
    if not records_or_false:
        raise ValueError('Failed to compute capacity')
    return records_or_false


@instance_api.get('/iostats', name='clusters:storage-nodes:iostats')
def iostats(cluster: Cluster, storage_node: StorageNode, history: Optional[str] = None):
    storage_node = storage_node
    records_or_false = storage_node_ops.get_node_iostats_history(
            storage_node.get_id(),
            history,
            parse_sizes=False,
            with_sizes=True
    )
    if not records_or_false:
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.get('/nics', name='clusters:storage-nodes:nics:list')
def nics(cluster: Cluster, storage_node: StorageNode):
    storage_node = storage_node
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


@instance_api.get('/nics/{nic_id}/iostats', name='clusters:storage-nodes:nics:iostats')
def nic_iostats(cluster: Cluster, storage_node: StorageNode, nic_id: str):
    storage_node = storage_node
    nic = next((
        nic
        for nic
        in storage_node.data_nics
        if nic.get_id() == nic_id
    ), None)
    if nic is None:
        raise HTTPException(404, f'NIC {nic_id} not found')

    return [
        record.get_clean_dict()
        for record in db.get_port_stats(storage_node.get_id(), nic.get_id())
    ]


@instance_api.post('/suspend', name='clusters:storage-nodes:suspend', status_code=204, responses={204: {"content": None}})
def suspend(cluster: Cluster, storage_node: StorageNode, force: bool = False) -> Response:
    storage_node = storage_node
    if not storage_node_ops.suspend_storage_node(storage_node.get_id(), force):
        raise ValueError('Failed to suspend storage node')

    return Response(status_code=204)


@instance_api.post('/resume', name='clusters:storage-nodes:resume', status_code=204, responses={204: {"content": None}})
def resume(cluster: Cluster, storage_node: StorageNode) -> Response:
    storage_node = storage_node
    if not storage_node_ops.resume_storage_node(storage_node.get_id()):
        raise ValueError('Failed to resume storage node')

    return Response(status_code=204)


@instance_api.post('/shutdown', name='clusters:storage-nodes:shutdown', status_code=202, responses={202: {"content": None}})
def shutdown(cluster: Cluster, storage_node: StorageNode, force: bool = False) -> Response:
    storage_node = storage_node
    Thread(
        target=storage_node_ops.shutdown_storage_node,
        args=(storage_node.get_id(), force)
    ).start()

    return Response(status_code=202)  # FIXME: Provide URL for checking task status


class _RestartParams(BaseModel):
    force: bool = False
    reattach_volume: bool = False


@instance_api.post('/start', name='clusters:storage-nodes:start', status_code=202, responses={202: {"content": None}})  # Same as restart for now
@instance_api.post('/restart', name='clusters:storage-nodes:restart', status_code=202, responses={202: {"content": None}})
def restart(cluster: Cluster, storage_node: StorageNode, parameters: _RestartParams = _RestartParams()) -> Response:
    storage_node = storage_node
    Thread(
        target=storage_node_ops.restart_storage_node,
        kwargs={
            "node_id": storage_node.get_id(),
            "force": parameters.force,
            "reattach_volume": parameters.reattach_volume,
        }
    ).start()

    return Response(status_code=202)  # FIXME: Provide URL for checking task status
