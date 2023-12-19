# coding=utf-8
import logging
import time

from management import constants
from management import utils
from management.kv_store import DBController
from management.models.compute_node import ComputeNode
from management.models.nvme_device import NVMeDevice
from management.models.storage_node import StorageNode
from management import services
from management import spdk_installer
from management.pci_utils import  bind_spdk_driver
from management.rpc_client import RPCClient

logger = logging.getLogger()



def suspend_cluster():
    global_settings = DBController().get_global_settings()
    global_settings.cluster_status = "suspended"
    global_settings.write_to_db(DBController().kv_store)
    return "Done"


def unsuspend_cluster():
    global_settings = DBController().get_global_settings()
    global_settings.cluster_status = "active"
    global_settings.write_to_db(DBController().kv_store)
    return "Done"
