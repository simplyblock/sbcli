from flask import abort
from flask_openapi3 import APIBlueprint
from pydantic import Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core import utils as core_utils

from . import util
from .storage_node import StorageNodePath


api = APIBlueprint('device', __name__, url_prefix='/devices')
db = DBController()


@api.get('/')
def list(path: StorageNodePath):
    storage_node = path.storage_node()
    return [
        device.get_clean_dict()
        for device in storage_node.nvme_devices
    ]

instance_api = APIBlueprint('instance', __name__, url_prefix='/<device_id>')


class DevicePath(StorageNodePath):
    device_id: str = Field(pattern=core_utils.UUID_PATTERN)

    def device(self) -> NVMeDevice:
        for device in self.storage_node().nvme_devices:
            if device.get_id() == self.device_id:
                return device
        abort(404)


@instance_api.get('/')
def get(path: DevicePath):
    return path.device().get_clean_dict()


@instance_api.delete('/')
def delete(path: DevicePath):
    device = path.device()
    if not device_controller.device_remove(device.get_id()):
        raise ValueError('Failed to remove device')


@instance_api.get('/capacity')
def capacity(path: DevicePath, query: util.HistoryQuery):
    device = path.device()
    records_or_false = device_controller.get_device_capacity(device.get_id(), query.history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute device capacity')
    return records_or_false


@instance_api.get('/iostats')
def iostats(path: DevicePath, query: util.HistoryQuery):
    device = path.device()
    records_or_false = device_controller.get_device_iostats(device.get_id(), query.history, parse_sizes=False)
    if not records_or_false:
        raise ValueError('Failed to compute iostats')
    return records_or_false


@instance_api.post('/reset')
def reset(path: DevicePath):
    device = path.device()
    if not device_controller.reset_storage_device(device.get_id()):
        raise ValueError('Failed to reset device')


api.register_api(instance_api)
