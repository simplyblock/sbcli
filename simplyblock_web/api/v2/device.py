from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice

from .cluster import Cluster
from .storage_node import StorageNode
from .dtos import DeviceDTO


api = APIRouter(prefix='/devices')
db = DBController()


@api.get('/', name='clusters:storage_nodes:devices:list')
def list(cluster: Cluster, storage_node: StorageNode) -> List[DeviceDTO]:
    return [
        DeviceDTO.from_model(device)
        for device in storage_node.nvme_devices
    ]

instance_api = APIRouter(prefix='/{device_id}')


def _lookup_device(storage_node: StorageNode, device_id: UUID) -> NVMeDevice:
    try:
        return db.get_storage_device_by_id(str(device_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


Device = Annotated[NVMeDevice, Depends(_lookup_device)]


@instance_api.get('/', name='clusters:storage_nodes:devices:detail')
def get(cluster: Cluster, storage_node: StorageNode, device: Device) -> DeviceDTO:
    return DeviceDTO.from_model(device)


@instance_api.delete('/', name='clusters:storage_nodes:devices:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, storage_node: StorageNode, device: Device) -> Response:
    if not device_controller.device_remove(device.get_id()):
        raise ValueError('Failed to remove device')

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
