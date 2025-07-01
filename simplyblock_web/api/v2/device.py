from ipaddress import IPv4Address
from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core import utils as core_utils

from .cluster import Cluster
from .storage_node import StorageNode


api = APIRouter(prefix='/devices')
db = DBController()


class DeviceDTO(BaseModel):
    id: UUID
    status: str
    health_check: bool
    size: int
    io_error: bool
    is_partition: bool
    nvmf_ips: List[IPv4Address]
    nvmf_nqn: str = ""
    nvmf_port: int = 0

    @staticmethod
    def from_model(model: NVMeDevice):
        return DeviceDTO(
            id=UUID(model.get_id()),
            status=model.status,
            health_check=model.health_check,
            size=model.size,
            io_error=model.io_error,
            is_partition=model.is_partition,
            nvmf_ips=[IPv4Address(ip) for ip in model.nvmf_ip.split(',')],
            nvmf_nqn=model.nvmf_nqn,
            nvmf_port=model.nvmf_port,
        )


@api.get('/')
def list(cluster: Cluster, storage_node: StorageNode) -> List[DeviceDTO]:
    return [
        DeviceDTO.from_model(device)
        for device in storage_node.nvme_devices
    ]

instance_api = APIRouter(prefix='/<device_id>')


def _device_lookup(
        storage_node: StorageNode,
        device_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]
) -> NVMeDevice:
    for device in storage_node.nvme_devices:
        if device.get_id() == device_id:
            return device
    raise HTTPException(404, f'Device {device_id} not found')


Device = Annotated[NVMeDevice, Depends(_device_lookup)]


@instance_api.get('/')
def get(cluster: Cluster, storage_node: StorageNode, device: Device) -> DeviceDTO:
    return DeviceDTO.from_model(device)


@instance_api.delete('/', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, storage_node: StorageNode, device: Device) -> Response:
    if not device_controller.device_remove(device.get_id()):
        raise ValueError('Failed to remove device')

    return Response(status_code=204)


@instance_api.get('/capacity')
def capacity(
        cluster: Cluster, storage_node: StorageNode, device: Device,
        history: Optional[str] = None
):
    records_or_false = device_controller.get_device_capacity(device.get_id(), history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute device capacity')
    return records_or_false


@instance_api.get('/iostats')
def iostats(
        cluster: Cluster, storage_node: StorageNode, device: Device,
        history: Optional[str] = None
):
    records_or_false = device_controller.get_device_iostats(device.get_id(), history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.post('/reset', status_code=204, responses={204: {"content": None}})
def reset(cluster: Cluster, storage_node: StorageNode, device: Device) -> Response:
    if not device_controller.reset_storage_device(device.get_id()):
        raise ValueError('Failed to reset device')

    return Response(status_code=204)


api.include_router(instance_api)
