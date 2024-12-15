# coding=utf-8

from typing import List

from simplyblock_core import utils
from simplyblock_core.models.base_model import BaseModel


class Pool(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_INACTIVE = "inactive"

    STATUS_CODE_MAP = {
        STATUS_ACTIVE: 1,
        STATUS_INACTIVE: 2,
    }

    cluster_id: str = ""
    groups: List[str] = []
    lvol_max_size: int = 0
    lvols: List[str] = []
    max_r_mbytes_per_sec: int = 0
    max_rw_ios_per_sec: int = 0
    max_rw_mbytes_per_sec: int = 0
    max_w_mbytes_per_sec: int = 0
    pool_max_size: int = 0
    pool_name: str = ""
    secret: str = ""
    users: List[str] = []


    def get_clean_dict(self):
        data = super(Pool, self).get_clean_dict()
        data['pool_max_size'] = utils.humanbytes(data['pool_max_size'])
        data['lvol_max_size'] = utils.humanbytes(data['lvol_max_size'])
        data['status_code'] = self.get_status_code()
        return data

    def has_qos(self):
        return 0 < (self.max_rw_ios_per_sec + self.max_rw_mbytes_per_sec + self.max_r_mbytes_per_sec + self.max_w_mbytes_per_sec)

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1
