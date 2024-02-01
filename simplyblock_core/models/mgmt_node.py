# coding=utf-8

from datetime import datetime

from simplyblock_core.models.base_model import BaseModel


class MgmtNode(BaseModel):

    STATUS_ONLINE = 'online'
    STATUS_OFFLINE = 'offline'
    STATUS_SUSPENDED = 'suspended'
    STATUS_IN_CREATION = 'in_creation'
    STATUS_IN_SHUTDOWN = 'in_shutdown'
    STATUS_RESTARTING = 'restarting'
    STATUS_UNREACHABLE = 'unreachable'

    STATUS_CODE_MAP = {
        STATUS_ONLINE: 0,
        STATUS_OFFLINE: 1,
        STATUS_SUSPENDED: 2,

        STATUS_IN_CREATION: 10,
        STATUS_IN_SHUTDOWN: 11,
        STATUS_RESTARTING: 12,

        STATUS_UNREACHABLE: 20,

    }
    attributes = {
        "baseboard_sn": {"type": str, 'default': ""},
        "system_uuid": {"type": str, 'default': ""},
        "hostname": {"type": str, 'default': ""},
        "status": {"type": str, 'default': ""},
        "docker_ip_port": {"type": str, 'default': ""},
        "cluster_id": {"type": str, 'default': ""},
        "mgmt_ip": {"type": str, 'default': ""},
        "updated_at": {"type": str, 'default': str(datetime.now())},

    }

    def __init__(self, data=None):
        super(MgmtNode, self).__init__()
        self.set_attrs(self.attributes, data)
        self.object_type = "object"

    def get_id(self):
        return self.system_uuid

    def get_status_code(self):
        if self.status in self.STATUS_CODE_MAP:
            return self.STATUS_CODE_MAP[self.status]
        else:
            return -1
