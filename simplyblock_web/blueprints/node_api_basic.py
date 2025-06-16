#!/usr/bin/env python
# encoding: utf-8
import logging

import cpuinfo

from pydantic import BaseModel, Field
from flask_openapi3 import APIBlueprint

from simplyblock_core import shell_utils, utils as core_utils
from simplyblock_web import utils, node_utils

logger = logging.getLogger(__name__)

api = APIBlueprint("node_api_basic", __name__, url_prefix="/")

cpu_info = cpuinfo.get_cpu_info()
hostname, _, _ = shell_utils.run_command("hostname -s")
system_id = ""
try:
    system_id, _, _ = shell_utils.run_command("dmidecode -s system-uuid")
except Exception:
    pass


@api.get('/scan_devices',
    summary='Enumerate connected devices',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'object',
            'required': ['nvme_devices', 'nvme_pcie_list', 'spdk_devices', 'spdk_pcie_list'],
            'properties': {
                'nvme_devices': {'type': 'array', 'items': {'type': 'string'}},
                'nvme_pcie_list': {'type': 'array', 'items': {'type': 'string'}},
                'spdk_devices': {'type': 'array', 'items': {'type': 'string'}},
                'spdk_pcie_list': {'type': 'array', 'items': {'type': 'string'}},
            },
        })}},
    },
})
def scan_devices():
    """Query lists of connected NVMe and SPDK devices
    """
    return utils.get_response({
        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),
        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),
    })


@api.get('/info',
    summary='Get node info',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'object',
            'additionalProperties': True,
        })}},
    },
})
def get_info():
    """Retrieve information about the node's configuration and hardware
    """
    out = {
        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpu_info['count'],
        "cpu_hz": cpu_info['hz_advertised'][0],

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),

        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),

        "network_interface": core_utils.get_nics_data()
    }
    return utils.get_response(out)


class _NVMeParams(BaseModel):
    ip: str = Field(pattern=utils.IP_PATTERN)
    port: int = Field(ge=0, le=65536)
    nqn: str


@api.post('/nvme_connect',
    summary='Connect NVMe-oF target',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'boolean',
        })}},
    },
})
def connect_to_nvme(body: _NVMeParams):
    """Connect to the indicated NVMe-oF target.
    """
    st = f"nvme connect --transport=tcp --traddr={body.ip} --trsvcid={body.port} --nqn={body.nqn}"
    logger.debug(st)
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    if ret_code == 0:
        return utils.get_response(True)
    else:
        return utils.get_response(ret_code, error=err)


class _DisconnectParams(BaseModel):
    dev_path: str


@api.post('/disconnect_device',
    summary='Disconnect NVMe-oF device by name',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'integer',
        })}},
    },
})
def disconnect_device(body: _DisconnectParams):
    """Disconnect from indicated NVMe-oF target
    """
    st = f"nvme disconnect --device={body.dev_path}"
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


class _DisconnectNQNParams(BaseModel):
    nqn: str


@api.post('/disconnect_nqn',
    summary='Disconnect NVMe-oF device by NQN',
    responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'integer',
    })}}},
})
def disconnect_nqn(body: _DisconnectNQNParams):
    """Disconnect from indicated NVMe-oF target
    """
    st = f"nvme disconnect --nqn={body.nqn}"
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


@api.post('/disconnect_all',
    summary='Disconnect all NVMe-oF devices',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'integer',
        })}},
    },
})
def disconnect_all():
    """Disconnect from all NVMe-oF devices
    """
    st = "nvme disconnect-all"
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)
