from threading import Thread
from typing import List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.storage_node import StorageNode as StorageNodeModel
from simplyblock_core import storage_node_ops

from ... import util as util
from ..._dependencies import Cluster, StorageNode
from ..._sse import WATCH_RESPONSES, WatchParam, sse_response
from .device import api as device_api
from ..._dtos import StorageNodeDTO, TaskDTO


api = APIRouter()
db = DBController()


def _storage_node_dto(storage_node: StorageNodeModel) -> StorageNodeDTO:
    ret = db.get_node_capacity(storage_node, 1)
    return StorageNodeDTO.from_model(storage_node, ret[0] if ret else None)


@api.get('/', name='clusters:storage-nodes:list', response_model=List[StorageNodeDTO], responses=WATCH_RESPONSES)
def list(cluster: Cluster, watch: WatchParam = False) -> Union[List[StorageNodeDTO], EventSourceResponse]:
    if watch:
        return sse_response(
            storage_node_ops.watch_storage_nodes(cluster.get_id()),
            _storage_node_dto,
        )
    data = []
    for storage_node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
        node_stat_obj = None
        ret = db.get_node_capacity(storage_node, 1)
        if ret:
            node_stat_obj = ret[0]
        data.append(StorageNodeDTO.from_model(storage_node, node_stat_obj))
    return data


class StorageNodeParams(BaseModel):
    node_address: str
    interface_name: str
    max_snapshots: Optional[int] = 500
    ha_jm: Optional[bool] = True
    test_device: Optional[bool] = False
    spdk_image: Optional[str] = ""
    spdk_debug: bool = False
    data_nics: List[str] = []
    namespace: str = 'default'
    id_device_by_nqn: Optional[bool] = False
    jm_percent: util.Percent = 3
    partitions: int = 1
    iobuf_small_pool_count: int = 0
    iobuf_large_pool_count: int = 0
    cr_name: str = ""
    cr_namespace: str = ""
    cr_plural: str = ""
    ha_jm_count: Optional[int] = None
    format_4k: bool = False
    spdk_proxy_image: Optional[str] = None
    spdk_sys_mem: Optional[str] = None
    failure_domain: Optional[int] = None
    expand: bool = False


@api.post('/', name='clusters:storage-nodes:create', status_code=201, responses={201: {"content": None}})
def add(request: Request, cluster: Cluster, parameters: StorageNodeParams, response_format: util.CreationResponseFormatParameter = "identifier"):
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
            'id_device_by_nqn': parameters.id_device_by_nqn,
            'cr_name': parameters.cr_name,
            'cr_namespace': parameters.cr_namespace,
            'cr_plural': parameters.cr_plural,
            "ha_jm_count": parameters.ha_jm_count,
            "format_4k": parameters.format_4k,
            "spdk_proxy_image": parameters.spdk_proxy_image,
            "spdk_sys_mem": parameters.spdk_sys_mem,
            "failure_domain": parameters.failure_domain,
            "expansion": parameters.expand,
        }
    )
    if not task_id_or_false:
        raise ValueError('Failed to create add-node task')

    return util.creation_response(
        request, response_format,
        entity_id=UUID(task_id_or_false),
        route_name='clusters:tasks:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id()), 'task_id': UUID(task_id_or_false)},
        get_full=lambda id: TaskDTO.from_model(db.get_task_by_id(str(id))),
    )


instance_api = APIRouter(prefix='/{storage_node_id}')


@instance_api.get('/', name='clusters:storage-nodes:detail', response_model=StorageNodeDTO, responses=WATCH_RESPONSES)
def get(cluster: Cluster, storage_node: StorageNode, watch: WatchParam = False) -> Union[StorageNodeDTO, EventSourceResponse]:
    if watch:
        return sse_response(
            storage_node_ops.watch_storage_node(cluster.get_id(), storage_node.get_id()),
            _storage_node_dto,
            single=True,
        )
    node_stat_obj = None
    ret = db.get_node_capacity(storage_node, 1)
    if ret:
        node_stat_obj = ret[0]
    return StorageNodeDTO.from_model(storage_node, node_stat_obj)


@instance_api.delete('/', name='clusters:storage-nodes:delete')
def delete(
        cluster: Cluster, storage_node: StorageNode, force_remove: bool = False, force_migrate: bool = False, force_delete: bool = False) -> Response:
    none_or_false = storage_node_ops.remove_storage_node(
            storage_node.get_id(), force_remove=force_remove, force_migrate=force_migrate
    )
    if none_or_false == False:  # noqa
        raise ValueError('Failed to remove storage node')

    if force_delete:
        none_or_false = storage_node_ops.delete_storage_node(
            storage_node.get_id(), force=force_delete
        )
        if none_or_false == False:  # noqa
            raise ValueError('Failed to delete storage node')

    return Response(status_code=204)


@instance_api.get('/capacity', name='clusters:storage-nodes:capacity')
def capacity(cluster: Cluster, storage_node: StorageNode, history: Optional[str] = None):
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
    return [
        {
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.trtype,
            "Status": nic.status,
        }
        for nic in storage_node.data_nics
    ]


@instance_api.get('/nics/{nic_id}/iostats', name='clusters:storage-nodes:nics:iostats')
def nic_iostats(cluster: Cluster, storage_node: StorageNode, nic_id: str):
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
    ret = storage_node_ops.suspend_storage_node(storage_node.get_id(), force)
    if isinstance(ret, tuple):
        ok, reason = ret
        if not ok:
            raise ValueError(reason)
    elif not ret:
        raise ValueError('Failed to suspend storage node')

    return Response(status_code=204)


@instance_api.post('/resume', name='clusters:storage-nodes:resume', status_code=204, responses={204: {"content": None}})
def resume(cluster: Cluster, storage_node: StorageNode) -> Response:
    if not storage_node_ops.resume_storage_node(storage_node.get_id()):
        raise ValueError('Failed to resume storage node')

    return Response(status_code=204)


@instance_api.post('/shutdown', name='clusters:storage-nodes:shutdown', status_code=202, responses={202: {"content": None}})
def shutdown(cluster: Cluster, storage_node: StorageNode, force: bool = False) -> Response:
    if not force:
        from simplyblock_core.storage_node_ops import _check_ftt_allows_node_removal
        from simplyblock_core.db_controller import DBController
        allowed, reason = _check_ftt_allows_node_removal(storage_node.get_id(), DBController())
        if not allowed:
            raise ValueError(reason)

    Thread(
        target=storage_node_ops.shutdown_storage_node,
        args=(storage_node.get_id(), force)
    ).start()

    return Response(status_code=202)  # FIXME: Provide URL for checking task status


class _RestartParams(BaseModel):
    force: bool = False
    reattach_volume: bool = False
    node_address: Optional[str] = None
    new_ssd_pcie: List[str] = []


@instance_api.post('/start', name='clusters:storage-nodes:start', status_code=202, responses={202: {"content": None}})  # Same as restart for now
@instance_api.post('/restart', name='clusters:storage-nodes:restart', status_code=202, responses={202: {"content": None}})
def restart(cluster: Cluster, storage_node: StorageNode, parameters: _RestartParams) -> Response:
    Thread(
        target=storage_node_ops.restart_storage_node,
        kwargs={
            "node_id": storage_node.get_id(),
            "force": parameters.force,
            "node_address": parameters.node_address,
            "reattach_volume": parameters.reattach_volume,
            "new_ssd_pcie": parameters.new_ssd_pcie,
        }
    ).start()

    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/promote', name='clusters:storage-nodes:start', status_code=204, responses={204: {"content": None}})
def promote(cluster: Cluster, storage_node: StorageNode) -> Response:
    storage_node_ops.make_sec_new_primary(storage_node.uuid)
    return Response(status_code=204)


instance_api.include_router(device_api, prefix='/devices')
api.include_router(instance_api)
