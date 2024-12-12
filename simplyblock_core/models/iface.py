# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class IFace(BaseModel):

    attributes = {
        "uuid": {"type": str, 'default': ""},
        "if_name": {"type": str, 'default': ""},
        "ip4_address": {"type": str, 'default': ""},
        "port_number": {"type": int, 'default': -1},
        "net_type": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},
    }

    def __init__(self, data=None):
        super(IFace, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.uuid

    def get_transport_type(self):
        if self.net_type == 'ether':
            return "TCP"
        if self.net_type == 'ib':
            return "RDMA"
        return "TCP"
