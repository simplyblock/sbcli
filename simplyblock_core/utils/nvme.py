from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, SecretStr

from simplyblock_core import constants

if TYPE_CHECKING:
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.cluster import Cluster
    from simplyblock_core.models.lvol_model import LVol
    from simplyblock_core.models.pool import Pool


def _hyphenate(field_name: str) -> str:
    return field_name.replace("_", "-")


class NvmeConnectEntry(BaseModel):
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
    ns_id: int | None = None
    allowed_hosts: list[str] = []
    # Set when the volume has been failed over; the CSI driver uses this UUID
    # for device lookup instead of the original source lvol UUID.
    target_lvol_id: str | None = None


class HostConnectAuth(BaseModel):
    model_config = ConfigDict(frozen=True)

    nqn: str
    psk: SecretStr = SecretStr("")
    dhchap_key: SecretStr = SecretStr("")
    dhchap_ctrlr_key: SecretStr = SecretStr("")

    @classmethod
    def from_entry(cls, entry: dict, pool: "Pool | None") -> "HostConnectAuth":
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

    @classmethod
    def resolve(cls, lvol: "LVol", host_nqn: str | None,
                db: "DBController") -> "HostConnectAuth | None":
        """Resolve the connecting host's auth for *lvol*.

        Returns None when the volume allows any host. Raises ValueError when the
        volume restricts hosts but the request names none, or names one that is
        not allowed.
        """
        if not lvol.allowed_hosts:
            return None
        if not host_nqn:
            raise ValueError(f"Volume {lvol.get_id()} has allowed hosts configured; --host-nqn is required")
        matched_entry = next((h for h in lvol.allowed_hosts if h["nqn"] == host_nqn), None)
        if matched_entry is None:
            raise ValueError(f"Host NQN {host_nqn} not found in allowed hosts for volume {lvol.get_id()}")
        return cls.from_entry(matched_entry, db.get_pool_by_id(lvol.pool_uuid))


def build_nvme_connect_entry(
    *,
    transport: str,
    ip: str,
    port: int,
    nqn: str,
    ctrl_loss_tmo: int,
    cluster: "Cluster",
    host_entry: HostConnectAuth | None,
    host_nqn: str | None,
    ns_id: int | None = None,
    allowed_hosts: list | None = None,
) -> NvmeConnectEntry:
    """Build one ``NvmeConnectEntry`` for a resolved (transport, ip, port) target.

    Shared by the lvol and migration connect paths, which differ only in how
    they select NICs (and thus the transport) and whether they carry ``ns_id`` /
    ``allowed_hosts``; the command assembly and field population are identical.
    """
    keep_alive_tmo = (constants.LVOL_NVME_KEEP_ALIVE_TO_TCP
                      if transport == "tcp" else constants.LVOL_NVME_KEEP_ALIVE_TO)
    client_data_nic_str = f"--host-iface={cluster.client_data_nic}" if cluster.client_data_nic else ""
    tls_str = host_auth_str = ""
    if host_entry:
        host_auth_str = f" --hostnqn={host_nqn}"
        if host_entry.psk.get_secret_value():
            tls_str = " --tls"
        if host_entry.dhchap_key.get_secret_value():
            host_auth_str += f" --dhchap-secret={host_entry.dhchap_key.get_secret_value()}"
        if host_entry.dhchap_ctrlr_key.get_secret_value():
            host_auth_str += f" --dhchap-ctrl-secret={host_entry.dhchap_ctrlr_key.get_secret_value()}"
    elif host_nqn:
        host_auth_str = f" --hostnqn={host_nqn}"
    connect_cmd = (
        f"sudo nvme connect"
        f" --reconnect-delay={constants.LVOL_NVME_CONNECT_RECONNECT_DELAY}"
        f" --ctrl-loss-tmo={ctrl_loss_tmo}"
        f" --fast_io_fail_tmo={constants.LVOL_NVME_CONNECT_FAST_IO_FAIL_TO}"
        f" --nr-io-queues={cluster.client_qpair_count}"
        f" --keep-alive-tmo={keep_alive_tmo}"
        f" --transport={transport} --traddr={ip} --trsvcid={port} --nqn={nqn}"
        f" {client_data_nic_str}{tls_str}{host_auth_str}"
    )
    return NvmeConnectEntry(
        ns_id=ns_id,
        transport=transport,
        ip=ip,
        port=port,
        nqn=nqn,
        reconnect_delay=constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
        ctrl_loss_tmo=ctrl_loss_tmo,
        fast_io_fail_tmo=constants.LVOL_NVME_CONNECT_FAST_IO_FAIL_TO,
        nr_io_queues=cluster.client_qpair_count,
        keep_alive_tmo=keep_alive_tmo,
        host_iface=cluster.client_data_nic or "",
        connect=connect_cmd,
        tls=bool(host_entry and host_entry.psk.get_secret_value()),
        allowed_hosts=allowed_hosts if allowed_hosts is not None else [],
    )
