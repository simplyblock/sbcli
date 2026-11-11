from typing import ClassVar

from simplyblock_core.models.base_model import BaseNodeObject
from simplyblock_core.models.indices import Index


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
    # The site this management node runs in on a stretched control plane
    # (docs/stretched-control-plane.md); "" on a cluster that is not stretched.
    site: str = ""
