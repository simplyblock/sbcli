# coding=utf-8
from typing import Optional

from pydantic import BaseModel


class NvmeConnectEntry(BaseModel):
    """Typed representation of a single NVMe-oF connect target.

    Returned by connect_lvol and create_migration.  Replaces the ad-hoc dicts
    that were built in both places with hyphenated keys.
    """
    transport: str
    ip: str
    port: int
    nqn: str
    reconnect_delay: int
    ctrl_loss_tmo: int
    fast_io_fail_tmo: int
    nr_io_queues: int
    keep_alive_tmo: int
    host_iface: str = ""
    tls: bool = False
    connect: str
    # Present only for lvol connect (not migration pre-connect)
    ns_id: Optional[int] = None
    allowed_hosts: list[str] = []
