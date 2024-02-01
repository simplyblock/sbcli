# coding=utf-8

from simplyblock_core.models.base_model import BaseModel
from typing import List


# SOME GLOBAL SETTINGS FOR THE FORMATTING OF NVME NAMESPACES
class GlobalSettings(BaseModel):

    attributes = {
        "NS_LB_SIZE": {"type": int, "default": -1},  # the logical block size in bytes ,e.g. 512
        "NS_SIZE_IN_LBS": {"type": int, "default": -1},  # the size of a partition in blocks, e.g. 40
        "LB_PER_PAGE": {"type": int, "default": -1},  # logical blocks per page
        "MODEL_IDS": {"type": List[str], "default": []},  # a list of supported model-ids with lbaf and thin provisioning factor
        "NVME_PROGRAM_FAIL_COUNT": {"type": int, "default": -1},
        "NVME_ERASE_FAIL_COUNT": {"type": int, "default": -1},
        "NVME_CRC_ERROR_COUNT": {"type": int, "default": -1},
        "DEVICE_OVERLOAD_STDEV_VALUE": {"type": int, "default": 1},
        "DEVICE_OVERLOAD_CAPACITY_THRESHOLD": {"type": int, "default": 100},
        "cluster_status": {"type": str, "default": "new"},
    }

    def __init__(self, data=None):
        super(GlobalSettings, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "global"

    def get_id(self):
        return "0"
