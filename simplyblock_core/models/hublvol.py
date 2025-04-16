# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class HubLVol(BaseModel):
    """Identifying information of a HubLVol
    """
    uuid: str = ""
    nqn: str = ""
    name: str = ""
    nvmf_port: int = 9099

    def get_remote_bdev_name(self):
        return f"{self.name}n1"