# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseNodeObject
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice


class StorageNode(BaseNodeObject):

    alceml_cpu_cores: List[int] = []
    alceml_cpu_index: int = 0
    alceml_worker_cpu_cores: List[int] = []
    alceml_worker_cpu_index: int = 0
    api_endpoint: str = ""
    app_thread_mask: str = ""
    baseboard_sn: str = ""
    cloud_instance_id: str = ""
    cloud_instance_public_ip: str = ""
    cloud_instance_type: str = ""
    cluster_id: str = ""
    cpu: int = 0
    cpu_hz: int = 0
    ctrl_secret: str = ""
    data_nics: List[IFace] = []
    distrib_cpu_cores: List[int] = []
    distrib_cpu_index: int = 0
    distrib_cpu_mask: str = ""
    enable_ha_jm: bool = False
    enable_test_device: bool = False
    health_check: bool = True
    host_nqn: str = ""
    host_secret: str = ""
    hostname: str = ""
    hugepages: int = 0
    ib_devices: List[IFace] = []
    iobuf_large_bufsize: int = 0
    iobuf_large_pool_count: int = 0
    iobuf_small_bufsize: int = 0
    iobuf_small_pool_count: int = 0
    is_secondary_node: bool = False
    jc_singleton_mask: str = ""
    jm_cpu_mask: str = ""
    jm_device: JMDevice = None
    jm_percent: int = 3
    jm_vuid: int = 0
    lvols: int = 0
    lvstore: str = ""
    lvstore_stack: List[dict] = []
    lvstore_stack_secondary_1: List[dict] = []
    lvstore_stack_secondary_2: List[dict] = []
    max_lvol: int = 0
    max_prov: int = 0
    max_snap: int = 0
    memory: int = 0
    mgmt_ip: str = ""
    namespace: str = ""
    node_lvs: str = "lvs"
    num_partitions_per_dev: int = 1
    number_of_devices: int = 0
    number_of_distribs: int = 4
    nvme_devices: List[NVMeDevice] = []
    online_since: str = ""
    partitions_count: int = 0
    poller_cpu_cores: List[int] = []
    pollers_mask: str = ""
    primary_ip: str = ""
    raid: str = ""
    remote_devices: List[NVMeDevice] = []
    remote_jm_devices: List[JMDevice] = []
    rpc_password: str = ""
    rpc_port: int = -1
    rpc_username: str = ""
    secondary_node_id: str = ""
    sequential_number: int = 0
    services: List[str] = []
    spdk_cpu_mask: str = ""
    spdk_debug: bool = False
    spdk_image: str = ""
    spdk_mem: int = 0
    subsystem: str = ""
    system_uuid: str = ""
