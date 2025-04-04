# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class PortStat(BaseModel):

    bytes_received: int = 0
    bytes_sent: int = 0
    date: int = 0
    dropin: int = 0
    dropout: int = 0
    errin: int = 0
    errout: int = 0
    in_speed: int = 0
    node_id: str = ""
    out_speed: int = 0
    packets_received: int = 0
    packets_sent: int = 0

    def get_id(self):
        return "%s/%s/%s" % (self.node_id, self.uuid, self.date)

