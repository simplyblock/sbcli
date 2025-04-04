# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class IFace(BaseModel):

    if_name: str = ""
    ip4_address: str = ""
    net_type: str = ""
    port_number: int = -1

    def get_transport_type(self):
        if self.net_type == 'ether':
            return "TCP"
        if self.net_type == 'ib':
            return "RDMA"
        return "TCP"
