"""SAN array plugins: the vendor adapters of :mod:`simplyblock_core.storage_arrays`
as :class:`~simplyblock_core.array.ArrayPlugin` s.

One plugin per supported array family (``pure-flasharray``, ``netapp-ontap``,
... see :data:`SAN_PLUGINS`). Each wraps an
:class:`~simplyblock_core.storage_arrays.client.ArrayClient`, which already
gives the plugin contract its guarantees: idempotent create / map / unmap /
delete, grow-only resize, one re-authentication on a rejected session.

Ownership. SAN arrays mostly have no free-form tags, so a LUN's owner is
encoded in its name: ``sb-<cluster hash 8><lun hash 16>`` (27 characters,
inside every supported array's name limit). The cluster part makes every
operation refuse a LUN of another cluster (:class:`ForeignLunError`); the LUN
part is derived from the name the control plane asked for, so a retried
create finds the LUN it made before.

Hosts. A storage node is registered with the initiators of the protocol named
by the ``protocol`` option (``iscsi``, ``fc``, ``nvme-tcp``, ``nvme-rdma``,
``nvme-fc``, ``sdc``), taken from what its agent reports
(``host_initiators``: ``iscsi_iqn``, ``fc_wwpns``, ``nvme_hostnqn``,
``sdc_guid``). The host object is named ``sb-<hostname>`` (shortened with a
hash suffix where the array's limit is shorter).

Identity. ``wwn`` is returned in the form the hosts report it (lsblk ``WWN``):
the NAA hex digits for SCSI, ``eui.<NGUID>`` for NVMe namespaces.

Options (``Cluster.array_options``): ``protocol`` (required), ``ca_bundle``
(PEM), ``verify_tls`` (default true), ``timeout_s``, and the adapter's own
options (``option_docs`` of the adapter: pool, SVM, CPG, port group, ...).
``host_portals`` (iSCSI / NVMe discovery portals the nodes log in to) is read
by the node-side scan, not here.
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid as uuid_mod
from typing import Any

from pydantic import SecretStr, ValidationError

from simplyblock_core.array import ArrayError, ArrayPlugin, ForeignLunError
from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays import base as sa
from simplyblock_core.storage_arrays.client import MANAGED_PREFIX, ArrayClient

logger = logging.getLogger()

#: Options consumed by the plugin itself; everything else goes to the adapter.
_PLUGIN_OPTIONS = frozenset({"protocol", "ca_bundle", "verify_tls", "timeout_s",
                             "host_portals"})

#: ``host_initiators`` keys per protocol.
_INITIATOR_KEYS = {
    sa.Protocol.ISCSI: "iscsi_iqn",
    sa.Protocol.FC: "fc_wwpns",
    sa.Protocol.NVME_TCP: "nvme_hostnqn",
    sa.Protocol.NVME_RDMA: "nvme_hostnqn",
    sa.Protocol.NVME_FC: "nvme_hostnqn",
    sa.Protocol.SDC: "sdc_guid",
}

_CLUSTER_HEX = 8
_LUN_HEX = 16


def _hex(text: str, n: int) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:n]


def lun_uuid(cluster_id: str, name: str) -> str:
    """The volume UUID whose managed name encodes cluster and LUN name."""
    hex32 = _hex(f"cluster:{cluster_id}", _CLUSTER_HEX) + _hex(f"lun:{cluster_id}/{name}", 24)
    return str(uuid_mod.UUID(hex32))


def cluster_prefix(cluster_id: str) -> str:
    return MANAGED_PREFIX + _hex(f"cluster:{cluster_id}", _CLUSTER_HEX)


def host_wwn(vol: sa.Volume, protocol: sa.Protocol) -> str:
    """The identity a host reports for ``vol`` (lsblk ``WWN``)."""
    if protocol.is_nvme and vol.nguid:
        return vol.nguid.lower()
    if vol.wwn:
        w = vol.wwn.lower()
        return w.removeprefix("naa.")
    return vol.identifier.lower()


def host_object_name(hostname: str, max_len: int) -> str:
    clean = re.sub(r"[^A-Za-z0-9-]", "-", hostname or "").strip("-") or "host"
    name = f"{MANAGED_PREFIX}{clean}"
    if len(name) <= max_len:
        return name
    suffix = "-" + _hex(hostname, 8)
    return name[: max_len - len(suffix)].rstrip("-") + suffix


class _StaticCredentials:
    """The plugin gets its credentials already resolved (from the KMS)."""

    def __init__(self, fields: dict[str, SecretStr]):
        self._credential = Credential(fields)

    def resolve(self, ref, context):
        return self._credential

    def invalidate(self, ref=None):
        pass


def _as_list(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [v for v in re.split(r"[\s,]+", value) if v]
    return [str(v) for v in value if v]


class SanArrayPlugin(ArrayPlugin):
    """Base of the per-vendor plugins; ``vendor`` is the adapter key."""

    vendor: str = ""

    def __init__(self, endpoint: str = "", options: dict | None = None, credentials=None,
                 cluster_id: str = ""):
        super().__init__(endpoint, options, credentials, cluster_id)
        self.client: ArrayClient | None = None
        #: Tests: a ``requests`` transport (vendor fake) and session kwargs.
        self.transport: Any = None
        self.session_kwargs: dict[str, Any] = {}

    # ------------------------------------------------------------ setup

    @property
    def protocol(self) -> sa.Protocol:
        raw = str(self.options.get("protocol") or "").strip().lower()
        if not raw:
            raise ArrayError(f"array plugin {self.vendor}: option 'protocol' is required "
                             f"({', '.join(p.value for p in sa.Protocol)})")
        try:
            return sa.Protocol(raw)
        except ValueError:
            raise ArrayError(f"array plugin {self.vendor}: unknown protocol {raw!r}") from None

    def connect(self, endpoint: str, credentials: dict, options: dict) -> None:
        super().connect(endpoint, credentials, options)
        if not self.cluster_id:
            raise ArrayError("the plugin has no cluster id")
        proto = self.protocol
        adapter_opts = {k: v for k, v in self.options.items() if k not in _PLUGIN_OPTIONS}
        try:
            config = sa.ArrayConfig(
                id=f"{self.cluster_id}",
                vendor=self.vendor,
                endpoint=endpoint,
                # The plugin resolves nothing itself: the provider below hands
                # back the credentials it was given.
                credential_ref={"kind": "vault-kv", "mount": "-", "path": "-"},
                ca_bundle=self.options.get("ca_bundle") or None,
                verify_tls=_truthy(self.options.get("verify_tls", True)),
                timeout_s=float(self.options.get("timeout_s") or 60.0),
                options=adapter_opts,
            )
        except ValidationError as e:
            raise ArrayError(f"array plugin {self.vendor}: {_first_error(e)}") from None
        fields = {k: v if isinstance(v, SecretStr) else SecretStr(str(v))
                  for k, v in (credentials or {}).items()}
        self.client = ArrayClient(config, _StaticCredentials(fields),  # type: ignore[arg-type]
                                  transport=self.transport, session_kwargs=self.session_kwargs)
        caps = self.client.capabilities
        if proto not in caps.protocols:
            raise ArrayError(f"{self.client.adapter_cls.display_name} does not support "
                             f"{proto.value} (supported: "
                             f"{', '.join(sorted(p.value for p in caps.protocols))})")
        # Validates credential shape and options, logs in.
        self._run(lambda c: c.get_info())

    def _client(self) -> ArrayClient:
        if self.client is None:
            raise ArrayError("array plugin is not connected")
        return self.client

    def _run(self, fn):
        try:
            return fn(self._client())
        except ArrayError:
            raise
        except sa.ArrayError as e:
            raise ArrayError(str(e)) from e

    def capabilities(self) -> dict:
        caps = self._client().capabilities if self.client else None
        protocols = sorted(p.value for p in caps.protocols) if caps else []
        return {"protocols": protocols, "supports_reservations": False, "max_attachments": 0}

    # ---------------------------------------------------------- ownership

    def _prefix(self) -> str:
        return cluster_prefix(self.cluster_id)

    def _owned_volume(self, lun_id: str) -> sa.Volume | None:
        vol = self._run(lambda c: c.get_volume(lun_id))
        if vol is not None and not vol.name.startswith(self._prefix()):
            raise ForeignLunError(
                f"LUN {lun_id} ({vol.name}) was not created by simplyblock cluster "
                f"{self.cluster_id}; it is not touched")
        return vol

    def _view(self, vol: sa.Volume) -> dict:
        hosts = [m.host_id for m in self._run(lambda c: c.list_mappings(vol.id))]
        return {"id": vol.id, "wwn": host_wwn(vol, self.protocol), "size": vol.size_bytes,
                "name": vol.name, "hosts": hosts}

    # --------------------------------------------------------- operations

    def ensure_host(self, name: str, initiators: dict) -> str:
        proto = self.protocol
        key = _INITIATOR_KEYS[proto]
        addresses = _as_list((initiators or {}).get(key))
        if not addresses:
            raise ArrayError(
                f"host {name!r} reports no {proto.value} initiator ({key}); check the "
                "host's initiator setup (iSCSI initiatorname, FC HBA, NVMe hostnqn, SDC)")
        try:
            spec = sa.HostSpec(
                name=host_object_name(name, self._client().capabilities.max_name_len),
                initiators=tuple(sa.Initiator(protocol=proto, address=a) for a in addresses))
        except ValidationError as e:
            raise ArrayError(f"host {name!r}: {_first_error(e)}") from None
        return self._run(lambda c: c.ensure_host(spec))

    def create_lun(self, name: str, size_bytes: int, tags: dict) -> dict:
        self.cluster_tags(tags, name)   # refuses without a cluster id
        if int(size_bytes) <= 0:
            raise ArrayError("a LUN needs a size")
        vol = self._run(lambda c: c.create_volume(lun_uuid(self.cluster_id, name), int(size_bytes)))
        return {"id": vol.id, "wwn": host_wwn(vol, self.protocol), "size": vol.size_bytes}

    def attach(self, lun_id: str, host_id: str) -> None:
        if self._owned_volume(lun_id) is None:
            raise ArrayError(f"LUN {lun_id} not found")
        self._run(lambda c: c.map_volume(lun_id, host_id))

    def detach(self, lun_id: str, host_id: str, force: bool = False) -> None:
        if self._owned_volume(lun_id) is None:
            return
        # An unmap at the array takes effect for the host at once, queued
        # I/O included: the same with and without ``force``.
        self._run(lambda c: c.unmap_volume(lun_id, host_id))

    def resize(self, lun_id: str, size_bytes: int) -> None:
        if self._owned_volume(lun_id) is None:
            raise ArrayError(f"LUN {lun_id} not found")
        self._run(lambda c: c.resize_volume(lun_id, int(size_bytes)))

    def get(self, lun_id: str) -> dict:
        vol = self._owned_volume(lun_id)
        if vol is None:
            raise ArrayError(f"LUN {lun_id} not found")
        return self._view(vol)

    def delete_lun(self, lun_id: str) -> None:
        if self._owned_volume(lun_id) is None:
            return
        self._run(lambda c: c.delete_volume(lun_id))

    def list_luns(self) -> list[dict]:
        """Not offered by the SAN adapters (no portable prefix listing);
        ``array_controller.delete_cluster_luns`` deletes the recorded LUNs."""
        raise NotImplementedError(f"{self.vendor}: listing LUNs is not supported")


def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() not in ("0", "false", "no", "off", "")


def _first_error(e: ValidationError) -> str:
    err = e.errors()[0]
    loc = ".".join(str(p) for p in err.get("loc", ()))
    return f"{loc}: {err.get('msg')}" if loc else str(err.get("msg"))


class PureFlashArrayPlugin(SanArrayPlugin):
    vendor = "pure-flasharray"


class NetAppOntapPlugin(SanArrayPlugin):
    vendor = "netapp-ontap"


class DellPowerStorePlugin(SanArrayPlugin):
    vendor = "dell-powerstore"


class DellPowerMaxPlugin(SanArrayPlugin):
    vendor = "dell-powermax"


class DellPowerFlexPlugin(SanArrayPlugin):
    vendor = "dell-powerflex"


class HpeWsapiPlugin(SanArrayPlugin):
    vendor = "hpe-wsapi"


class HpeNimblePlugin(SanArrayPlugin):
    vendor = "hpe-nimble"


class IbmFlashSystemPlugin(SanArrayPlugin):
    vendor = "ibm-flashsystem"


class HitachiVspPlugin(SanArrayPlugin):
    vendor = "hitachi-vsp"


class HuaweiDoradoPlugin(SanArrayPlugin):
    vendor = "huawei-dorado"


class InfinidatInfiniBoxPlugin(SanArrayPlugin):
    vendor = "infinidat-infinibox"


#: Plugin name (= adapter key) -> import path, merged into BUILTIN_PLUGINS.
SAN_PLUGINS = {
    cls.vendor: f"{__name__}:{cls.__name__}"
    for cls in (PureFlashArrayPlugin, NetAppOntapPlugin, DellPowerStorePlugin,
                DellPowerMaxPlugin, DellPowerFlexPlugin, HpeWsapiPlugin, HpeNimblePlugin,
                IbmFlashSystemPlugin, HitachiVspPlugin, HuaweiDoradoPlugin,
                InfinidatInfiniBoxPlugin)
}
