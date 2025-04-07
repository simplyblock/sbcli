# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel


class Pool(BaseModel):

    STATUS_ACTIVE = "active"
    STATUS_INACTIVE = "inactive"

    _STATUS_CODE_MAP = {
        STATUS_ACTIVE: 1,
        STATUS_INACTIVE: 2,
    }

    cluster_id: str = ""
    groups: List[str] = []
    lvol_max_size: int = 0
    lvols: int = 0
    max_r_mbytes_per_sec: int = 0
    max_rw_ios_per_sec: int = 0
    max_rw_mbytes_per_sec: int = 0
    max_w_mbytes_per_sec: int = 0
    pool_max_size: int = 0
    pool_name: str = ""
    numeric_id: int = 0
    secret: str = ""
    users: List[str] = []


    def has_qos(self):
        return 0 < (self.max_rw_ios_per_sec + self.max_rw_mbytes_per_sec + self.max_r_mbytes_per_sec + self.max_w_mbytes_per_sec)
