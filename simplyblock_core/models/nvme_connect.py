# coding=utf-8
from typing import Optional

from pydantic import BaseModel, ConfigDict


def _hyphenate(field_name: str) -> str:
    """Map a snake_case field name to its hyphenated JSON key (``nr_io_queues`` -> ``nr-io-queues``)."""
    return field_name.replace("_", "-")


class NvmeConnectEntry(BaseModel):
    """Typed representation of a single NVMe-oF connect target.

    Returned by connect_lvol and create_migration.  Replaces the ad-hoc dicts
    that were built in both places with hyphenated keys.

    Python attributes and constructor keywords use snake_case
    (``nr_io_queues``); the JSON representation uses hyphenated keys
    (``nr-io-queues``) to match the ``nvme connect`` option names.  Serialize
    with ``by_alias=True`` (FastAPI response models do this by default) to emit
    the hyphenated keys.
    """
    model_config = ConfigDict(alias_generator=_hyphenate, populate_by_name=True)

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
