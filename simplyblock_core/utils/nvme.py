# coding=utf-8
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, ConfigDict, SecretStr

if TYPE_CHECKING:
    from simplyblock_core.models.pool import Pool


class HostConnectAuth(BaseModel):
    """Resolved NVMe-oF authentication for a single connecting host.

    Built from a volume's persisted allowed-hosts entry together with its pool.
    The effective keys mirror what ``add_lvol_on_node`` registers on the
    subsystem: a DHCHAP pool contributes the shared pool key pair (and no PSK),
    while any other pool uses the per-host keys stored on the allowed-hosts
    entry itself. Keeping this resolution in one place is what lets the lvol and
    migration connect paths agree on the credentials a client must present.
    """

    model_config = ConfigDict(frozen=True)

    nqn: str
    psk: SecretStr = SecretStr("")
    dhchap_key: SecretStr = SecretStr("")
    dhchap_ctrlr_key: SecretStr = SecretStr("")

    @classmethod
    def from_entry(cls, entry: dict, pool: "Optional[Pool]") -> "HostConnectAuth":
        """Resolve the effective auth for one matched allowed-hosts ``entry``."""
        if pool is not None and pool.dhchap:
            return cls(
                nqn=entry["nqn"],
                dhchap_key=pool.dhchap_key,
                dhchap_ctrlr_key=pool.dhchap_ctrlr_key,
            )
        return cls(
            nqn=entry["nqn"],
            psk=SecretStr(entry.get("psk") or ""),
            dhchap_key=SecretStr(entry.get("dhchap_key") or ""),
            dhchap_ctrlr_key=SecretStr(entry.get("dhchap_ctrlr_key") or ""),
        )
