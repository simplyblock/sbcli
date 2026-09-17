from typing import ClassVar

from simplyblock_core.indices import Index
from simplyblock_core.models.base_model import BaseNodeObject


class MgmtNode(BaseNodeObject):

    _INDEXES: ClassVar[tuple] = (
        Index('hostname'),
    )

    baseboard_sn: str = ""
    cluster_id: str = ""
    docker_ip_port: str = ""
    hostname: str = ""
    mgmt_ip: str = ""
    mode: str = ""
