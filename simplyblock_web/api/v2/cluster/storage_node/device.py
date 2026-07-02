from typing import Callable, List, Optional, Union

from fastapi import APIRouter, Response
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode as StorageNodeModel

from ..._dependencies import Cluster, StorageNode, Device
from ..._dtos import DeviceDTO
from ..._watch import RawState, WATCH_RESPONSES, WatchParam, watch_response


api = APIRouter()
db = DBController()


def _make_device_dto(storage_node_id: str) -> Callable[[dict], DeviceDTO]:
    def build(data: dict) -> DeviceDTO:
        device = NVMeDevice(data)
        ret = db.get_device_stats(device, 1)
        return DeviceDTO.from_model(device, storage_node_id, ret[0] if ret else None)
    return build


def _make_device_projection(node_id: str, device_id: Optional[str] = None) -> Callable[[RawState], RawState]:
    # Devices are embedded in their StorageNode record; explode them into
    # per-device entries so the diff yields device-level events.
    def project(state: RawState) -> RawState:
        for node in state.values():
            if node.get('uuid') == node_id:
                return {
                    device['uuid']: device
                    for device in node.get('nvme_devices', [])
                    if device_id is None or device.get('uuid') == device_id
                }
        return {}
    return project


@api.get('/', name='clusters:storage_nodes:devices:list', response_model=List[DeviceDTO], responses=WATCH_RESPONSES)
def list(cluster: Cluster, storage_node: StorageNode, watch: WatchParam = False) -> Union[List[DeviceDTO], EventSourceResponse]:
    if watch:
        node_id = storage_node.get_id()
        return watch_response(
            StorageNodeModel,
            _make_device_projection(node_id),
            _make_device_dto(node_id),
            ancestors=[(ClusterModel, cluster.get_id()), (StorageNodeModel, node_id)],
        )
    data = []
    for device in storage_node.nvme_devices:
        stat_obj = None
        ret = db.get_device_stats(device, 1)
        if ret:
            stat_obj = ret[0]
        data.append(DeviceDTO.from_model(device, storage_node.get_id(), stat_obj))
    return data


instance_api = APIRouter(prefix='/{device_id}')


@instance_api.get('/', name='clusters:storage_nodes:devices:detail', response_model=DeviceDTO, responses=WATCH_RESPONSES)
def get(cluster: Cluster, storage_node: StorageNode, device: Device, watch: WatchParam = False) -> Union[DeviceDTO, EventSourceResponse]:
    if watch:
        node_id = storage_node.get_id()
        return watch_response(
            StorageNodeModel,
            _make_device_projection(node_id, device.get_id()),
            _make_device_dto(node_id),
            single_id=device.get_id(),
            ancestors=[(ClusterModel, cluster.get_id()), (StorageNodeModel, node_id)],
        )
    stat_obj = None
    ret = db.get_device_stats(device, 1)
    if ret:
        stat_obj = ret[0]
    return DeviceDTO.from_model(device, storage_node.get_id(), stat_obj)


@instance_api.post('/remove', name='clusters:storage_nodes:devices:remove', status_code=204, responses={204: {"content": None}})
def remove(cluster: Cluster, storage_node: StorageNode, device: Device, force: bool = False) -> Response:
    if not device_controller.device_remove(device.get_id(), force):
        raise ValueError('Failed to remove device')

    return Response(status_code=204)

@instance_api.post('/restart', name='clusters:storage_nodes:devices:restart', status_code=204, responses={204: {"content": None}})
def restart(cluster: Cluster, storage_node: StorageNode, device: Device, force: bool = False) -> Response:
    if not device_controller.restart_device(device.get_id(), force):
        raise ValueError('Failed to restart device')

    return Response(status_code=204)


@instance_api.get('/capacity', name='clusters:storage_nodes:devices:capacity')
def capacity(
        cluster: Cluster, storage_node: StorageNode, device: Device,
        history: Optional[str] = None
):
    records_or_false = device_controller.get_device_capacity(device.get_id(), history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute device capacity')
    return records_or_false


@instance_api.get('/iostats', name='clusters:storage_nodes:devices:iostats')
def iostats(
        cluster: Cluster, storage_node: StorageNode, device: Device,
        history: Optional[str] = None
):
    records_or_false = device_controller.get_device_iostats(device.get_id(), history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.post('/reset', name='clusters:storage_nodes:devices:reset', status_code=204, responses={204: {"content": None}})
def reset(cluster: Cluster, storage_node: StorageNode, device: Device) -> Response:
    if not device_controller.reset_storage_device(device.get_id()):
        raise ValueError('Failed to reset device')

    return Response(status_code=204)


api.include_router(instance_api)
