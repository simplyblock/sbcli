# coding=utf-8

from typing import List

from simplyblock_core.models.base_model import BaseModel


class ManagedDatabase(BaseModel):
    """Identifying information of a Managed Database
    """
    uuid: str = ""
    name: str = ""
    deployment_id: str = ""
    pvc_id: str = ""
    type: str = ""
    version: str = ""
    vcpu_count: int = 0
    memory_size: int = 0
    disk_size: int = 0
    storage_class: str = ""
    status: str = ""


    def get_clean_dict(self):
        data = super(ManagedDatabase, self).get_clean_dict()
        data['status_code'] = self.get_status_code()
        return data
