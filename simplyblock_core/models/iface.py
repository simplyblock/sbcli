
from simplyblock_core.models.base_model import BaseModel


class IFace(BaseModel):

    if_name: str = ""
    #: The address peers and clients connect to. On the pod network it is the node's
    #: Service address, which outlives the pod; see ``listen_address``.
    ip4_address: str = ""
    #: The address the SPDK target binds its listeners to, when it is not ``ip4_address``:
    #: the pod's own address, which a Service fronts and a restart changes.
    listen_ip4: str = ""
    net_type: str = ""
    port_number: int = -1
    trtype: str = "TCP"

    @property
    def listen_address(self) -> str:
        """Where a listener on this interface binds. Every listener site uses this and
        every connect site uses ``ip4_address``; the two differ only on the pod network."""
        return self.listen_ip4 or self.ip4_address
