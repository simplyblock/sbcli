from typing import List, Optional

from fastapi import APIRouter, Response

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import device_controller

from ..._dependencies import Cluster, StorageNode, Device
from ..._dtos import DeviceDTO, DeviceHealthInfoDTO

api = APIRouter()
db = DBController()


@api.get('/', name='clusters:storage_nodes:devices:list')
def list(cluster: Cluster, storage_node: StorageNode) -> List[DeviceDTO]:
    data = []
    for device in storage_node.nvme_devices:
        stat_obj = None
        ret = db.get_device_stats(device, 1)
        if ret:
            stat_obj = ret[0]
        data.append(DeviceDTO.from_model(device, storage_node.get_id(), stat_obj))
    return data


instance_api = APIRouter(prefix='/{device_id}')


@instance_api.get('/', name='clusters:storage_nodes:devices:detail')
def get(cluster: Cluster, storage_node: StorageNode, device: Device) -> DeviceDTO:
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

@instance_api.get('/get-device-health-info', name='clusters:storage_nodes:devices:get-device-health-info')
def reset(cluster: Cluster, storage_node: StorageNode, device: Device) -> Response:
    ret = device_controller.get_device_health_info(device.get_id())
    if not ret:
        raise ValueError('Failed to get device health info')
    return DeviceHealthInfoDTO.from_model(device, ret)


api.include_router(instance_api)
