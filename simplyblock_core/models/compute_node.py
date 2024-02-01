# coding=utf-8

from simplyblock_core.models.base_model import BaseModel


class ComputeNode(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_ERROR = 'error'
    STATUS_REPLACED = 'replaced'
    STATUS_SUSPENDED = 'suspended'
    STATUS_IN_CREATION = 'in_creation'
    STATUS_IN_SHUTDOWN = 'in_shutdown'
    STATUS_RESTARTING = 'restarting'
    STATUS_REMOVED = 'removed'

    attributes = {
        "baseboard_sn": {"type": str, 'default': ""},
        "system_uuid": {"type": str, 'default': ""},
        "hostname": {"type": str, 'default': ""},
        "host_nqn": {"type": str, 'default': ""},
        "status": {"type": str, 'default': "in_creation"},
    }

    def __init__(self, data=None):
        super(ComputeNode, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.baseboard_sn
