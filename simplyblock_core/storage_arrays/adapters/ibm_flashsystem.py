"""IBM FlashSystem / SAN Volume Controller (IBM Storage Virtualize) REST API.

The Storage Virtualize REST API (port 7443) is a thin wrapper around the CLI:
every command is ``POST /rest/<command>[/<object>]`` with the command's
flags as a JSON object (``-unit b`` -> ``{"unit": "b"}``, ``-thin`` ->
``{"thin": true}``). Positional arguments go into the path.

Auth: ``POST /rest/auth`` with headers ``X-Auth-Username`` /
``X-Auth-Password`` returns ``{"token": ...}``; every later call carries
``X-Auth-Token``. Tokens expire after an idle period (one hour by default)
and the array then answers 403, which the client turns into a re-login.

Errors: a failed command answers HTTP 500 with the CLI's message, e.g.
``CMMVC5753E The specified object does not exist or is not a suitable
candidate.`` The ``CMMVCnnnnE`` code is mapped as follows (anything else is
an :class:`ArrayRequestError`):

* not found: CMMVC5753E, CMMVC5754E, CMMVC5804E, CMMVC5842E
* conflict: CMMVC6035E (name exists), CMMVC6578E (iSCSI name already
  assigned or invalid), CMMVC5867E (WWPN already assigned or invalid),
  CMMVC9328E (NQN already assigned), CMMVC5878E (volume already mapped to
  that host), CMMVC6071E (mapped to another host, needs ``force``),
  CMMVC5840E (volume still mapped), CMMVC5871E (host still has mappings)

Objects: volumes are addressed by their numeric id (``Volume.id``), hosts by
name (``ensure_host`` returns the host name). Note that the array reuses the
lowest free id after a deletion, so a stale volume id can address a
different volume later -- callers pass ``expected_identifier`` (the WWN) to
the client, which then refuses to act on the wrong volume.

Volumes are created with ``mkvolume`` (``-thin`` unless ``thin`` is False);
the virtual capacity has 512-byte granularity (extents only govern the real
allocation). Resizing grows with ``expandvdisksize`` (a delta). Deletion uses
``rmvolume``. Volume identity: ``vdisk_UID`` (32 hex) -> ``naa.<lowercase>``.

Hosts: ``mkhost`` takes exactly one of ``-fcwwpn`` / ``-iscsiname`` /
``-nqn``, so a host with FC and iSCSI initiators is created with one kind and
the other added with ``addhostport``. ``mkhost`` and ``addhostport`` always
carry ``-force`` (needed for FC WWPNs that are not logged in).

Initiator ownership: ``mkhost``/``addhostport`` fail with CMMVC6578E /
CMMVC5867E / CMMVC9328E when a port already belongs to another host; the
adapter first reads its own host (``lshost <name>``, ports in the ``nodes``
list) so ports it already owns are never re-added, and treats those codes as
:class:`ArrayConflictError`.

Checked against vendor source
-----------------------------
IBM Ansible collection ``ibm.storage_virtualize`` (``module_utils/
ibm_svc_utils.py``, ``modules/ibm_svc_host.py``, ``ibm_svc_manage_volume.py``,
``ibm_svc_vol_map.py``), IBM block CSI driver (``controllers/array_action/
array_mediator_svc.py``) and the OpenStack Cinder Storwize driver
(``storwize_svc/storwize_const.py``, ``storwize_svc_common.py``):

* REST root ``https://<host>:7443/rest``, ``POST /rest/auth`` with
  ``X-Auth-Username``/``X-Auth-Password`` -> ``{"token"}``, then
  ``X-Auth-Token``; positional args in the path, flags as a JSON object.
* A failed command is HTTP 500 whose body is the CLI text with the
  ``CMMVCnnnnE`` code (Ansible ``svc_obj_info`` reads the code from it).
* Codes: CMMVC5753E / CMMVC5754E / CMMVC5804E / CMMVC5842E (not found /
  not mapped), CMMVC6035E (exists), CMMVC6071E (already mapped to a host),
  CMMVC5867E / CMMVC6578E / CMMVC9328E (FC / iSCSI / NVMe port already
  assigned or invalid).
* ``mkhost -protocol`` values ``scsi``, ``iscsi``, ``fcscsi``, ``tcpnvme``,
  ``rdmanvme``, ``fcnvme``; ``mkhost``/``addhostport`` with ``-force``;
  ``-fcwwpn`` ``:``-separated, ``-iscsiname``/``-nqn`` ``,``-separated, one
  of them per ``mkhost``.
* ``lshost <name>`` ports under ``nodes`` (``WWPN``, ``iscsi_name``,
  ``nqn``); ``lsvdisk <id>`` is a list (volume, then copies);
  ``lsvdiskhostmap`` with one row may be a bare dict (``host_name``,
  ``SCSI_id``); ``mkvdiskhostmap -force``.
* ``mkvolume`` with ``size``/``unit=b``/``thin``/``iogrp``,
  ``expandvdisksize`` with a byte delta, ``rmvolume``; ``lssystem``
  ``code_level`` is ``"<version> (build ...)"``.

Verify against a real system
----------------------------
* ``/rest/v1`` on code levels that reject ``/rest`` (no vendor source uses
  it); set the ``rest_root`` option.
* HTTP status on an expired token (assumed 403, 401 handled the same) and on
  bad credentials at ``/rest/auth`` (assumed 403); the Ansible collection
  only special-cases 429.
* Codes CMMVC5840E (rmvolume of a mapped volume), CMMVC5871E (rmhost with
  mappings) and CMMVC5878E (volume already mapped to that host) are from
  memory of the CLI reference; no vendor driver handles them (Cinder and CSI
  check ``lsvdiskhostmap`` first, as this adapter does).
* NVMe ``-protocol`` on levels before 8.6 (``nvme`` only?) -- set
  ``nvme_protocol_value`` if the array rejects ``tcpnvme`` / ``rdmanvme``.
* Whether a SCSI host may hold both FC and iSCSI ports (``addhostport`` of the
  other kind) on code levels that split ``-protocol`` into ``fcscsi`` and
  ``iscsi``. SCSI hosts are created without ``-protocol`` (default ``scsi``).
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from ..base import (
    ArrayAdapter,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    Capabilities,
    HostSpec,
    Mapping,
    OptionDoc,
    Protocol,
    Volume,
)
from ..http import body_text, json_of

_CODE_RE = re.compile(r"CMMVC\d{4}[EW]")

NOT_FOUND_CODES = frozenset({"CMMVC5753E", "CMMVC5754E", "CMMVC5804E", "CMMVC5842E"})
CONFLICT_CODES = frozenset({
    "CMMVC6035E",  # object already exists
    "CMMVC6578E",  # iSCSI name already assigned or not valid
    "CMMVC5867E",  # WWPN already assigned or not valid
    "CMMVC9328E",  # NQN already assigned
    "CMMVC5878E",  # volume already mapped to this host
    "CMMVC6071E",  # volume mapped to another host; needs -force
    "CMMVC5840E",  # volume not deleted: still mapped
    "CMMVC5871E",  # host has mappings
})
#: Codes meaning "this port is owned by another host" on mkhost/addhostport.
PORT_TAKEN_CODES = frozenset({"CMMVC6578E", "CMMVC5867E", "CMMVC9328E"})
_PORT_SEP = {"nqn": ",", "fcwwpn": ":", "iscsiname": ","}

_NVME_PROTOCOL = {Protocol.NVME_TCP: "tcpnvme", Protocol.NVME_RDMA: "rdmanvme"}


def ibm_wwn(vdisk_uid: str) -> str:
    return "naa." + vdisk_uid.lower()


def _rows(data: Any) -> list[dict[str, Any]]:
    """List views can come back as a bare dict when there is one row."""
    if data is None:
        return []
    if isinstance(data, dict):
        return [data]
    return [d for d in data if isinstance(d, dict)]


def _detail(data: Any) -> dict[str, Any]:
    """Detail views: a dict, or a list (object first, then e.g. copies)."""
    rows = _rows(data)
    return rows[0] if rows else {}


class IbmFlashSystemAdapter(ArrayAdapter):
    vendor = "ibm-flashsystem"
    display_name = "IBM FlashSystem / Storage Virtualize"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP,
                             Protocol.NVME_RDMA}),
        size_granularity_bytes=512,
        min_size_bytes=1 << 20,
        max_name_len=63,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("pool", "Storage pool (mdisk group) name or id for new volumes.",
                  required=True),
        OptionDoc("thin", "Create thin-provisioned volumes (mkvolume -thin). In a "
                  "data reduction pool this is a DRP thin volume.", default=True),
        OptionDoc("io_group", "I/O group (name or id) that owns new volumes; "
                  "default: chosen by the array."),
        OptionDoc("rest_root", "REST path root; '/rest/v1' on code levels that "
                  "reject '/rest'.", default="/rest"),
        OptionDoc("nvme_protocol_value", "Override the mkhost -protocol value used "
                  "for NVMe hosts (default: tcpnvme / rdmanvme by transport)."),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._root = "/" + str(self.option("rest_root")).strip("/")

    # -- session --------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop("X-Auth-Token", None)
        resp = self.session.post(f"{self._root}/auth", headers={
            "X-Auth-Username": self.secret("username"),
            "X-Auth-Password": self.secret("password"),
        })
        token = (json_of(resp) or {}).get("token")
        if not token:
            raise ArrayRequestError("Storage Virtualize /auth returned no token")
        self.session.headers["X-Auth-Token"] = token

    def close(self) -> None:
        # There is no logout command; tokens lapse on their own.
        self.session.headers.pop("X-Auth-Token", None)

    # -- request helper ---------------------------------------------------------

    def _cmd(self, command: str, obj: str | None = None, *, missing_ok: bool = False,
             **opts: Any) -> Any:
        """Run one CLI command. Returns the decoded JSON (``None`` on an empty
        body, or when ``missing_ok`` and the object does not exist)."""
        path = f"{self._root}/{command}"
        if obj is not None:
            path += "/" + quote(str(obj), safe="")
        resp = self.session.post(path, json=opts, ok=(500,))
        if resp.status_code == 500:
            text = body_text(resp)
            m = _CODE_RE.search(text)
            code = m.group(0) if m else ""
            msg = f"{command}{' ' + str(obj) if obj is not None else ''}: {text.strip()}"
            if code in NOT_FOUND_CODES:
                if missing_ok:
                    return None
                raise ArrayNotFoundError(msg)
            if code in CONFLICT_CODES:
                raise ArrayConflictError(msg)
            raise ArrayRequestError(msg, status=500)
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as e:
            raise ArrayRequestError(f"{command}: expected JSON, got {body_text(resp)}") from e

    # -- volumes ----------------------------------------------------------------

    @staticmethod
    def _volume(row: dict[str, Any]) -> Volume:
        uid = str(row.get("vdisk_UID") or "")
        return Volume(id=str(row["id"]), name=row["name"], size_bytes=int(row["capacity"]),
                      wwn=ibm_wwn(uid) if uid else None)

    def get_info(self) -> ArrayInfo:
        s = _detail(self._cmd("lssystem"))
        return ArrayInfo(vendor=self.vendor, model=str(s.get("product_name", "")),
                         version=str(s.get("code_level", "")).split(" ")[0],
                         serial=str(s.get("id", "")))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        opts: dict[str, Any] = {"name": name, "pool": str(self.option("pool")),
                                "size": size_bytes, "unit": "b"}
        if self.option("thin"):
            opts["thin"] = True
        if self.option("io_group") not in (None, ""):
            opts["iogrp"] = str(self.option("io_group"))
        out = self._cmd("mkvolume", **opts) or {}
        vid = out.get("id")
        vol = self.get_volume(str(vid)) if vid is not None else self.find_volume(name)
        if vol is None:
            raise ArrayRequestError(f"mkvolume {name} reported success but the volume "
                                    "cannot be read back")
        return vol

    def get_volume(self, volume_id: str) -> Volume | None:
        row = _detail(self._cmd("lsvdisk", volume_id, missing_ok=True, bytes=True))
        return self._volume(row) if row else None

    def find_volume(self, name: str) -> Volume | None:
        rows = _rows(self._cmd("lsvdisk", filtervalue=f"name={name}", bytes=True))
        rows = [r for r in rows if r.get("name") == name]
        return self._volume(rows[0]) if rows else None

    def _require(self, volume_id: str) -> Volume:
        vol = self.get_volume(volume_id)
        if vol is None:
            raise ArrayNotFoundError(f"Volume {volume_id} does not exist")
        return vol

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        vol = self._require(volume_id)
        delta = size_bytes - vol.size_bytes
        if delta < 0:
            raise ArrayConflictError(
                f"Refusing to shrink volume {vol.name} ({vol.size_bytes} -> {size_bytes})")
        if delta:
            self._cmd("expandvdisksize", vol.id, size=delta, unit="b")
        return self._require(vol.id)

    def delete_volume(self, volume_id: str) -> None:
        self._cmd("rmvolume", volume_id)

    # -- hosts -------------------------------------------------------------------

    def _host(self, name: str) -> dict[str, Any] | None:
        data = self._cmd("lshost", name, missing_ok=True)
        if data is None:
            return None
        merged: dict[str, Any] = {}
        nodes: list[dict[str, Any]] = []
        for row in _rows(data):
            nodes.extend(n for n in row.get("nodes", []) if isinstance(n, dict))
            merged.update({k: v for k, v in row.items() if k != "nodes"})
        merged["nodes"] = nodes
        return merged

    @staticmethod
    def _ports(host: dict[str, Any]) -> dict[str, set[str]]:
        ports: dict[str, set[str]] = {"iscsiname": set(), "fcwwpn": set(), "nqn": set()}
        for n in host.get("nodes", []):
            if n.get("WWPN"):
                ports["fcwwpn"].add(str(n["WWPN"]).lower())
            if n.get("iscsi_name"):
                ports["iscsiname"].add(str(n["iscsi_name"]).lower())
            if n.get("nqn"):
                ports["nqn"].add(str(n["nqn"]))
        return ports

    def _nvme_protocol(self, spec: HostSpec) -> str | None:
        nvme = {p for p in spec.protocols if p.is_nvme}
        if not nvme:
            return None
        if len(nvme) > 1:
            raise ArrayUnsupportedError(
                "A Storage Virtualize NVMe host has one transport; register "
                f"{', '.join(sorted(p.value for p in nvme))} initiators as separate hosts")
        override = self.option("nvme_protocol_value")
        return str(override) if override else _NVME_PROTOCOL[next(iter(nvme))]

    def ensure_host(self, spec: HostSpec) -> str:
        wanted = {
            "nqn": spec.addresses(Protocol.NVME_TCP, Protocol.NVME_RDMA),
            "fcwwpn": [a.upper() for a in spec.addresses(Protocol.FC)],
            "iscsiname": spec.addresses(Protocol.ISCSI),
        }
        protocol = self._nvme_protocol(spec)
        host = self._host(spec.name)
        try:
            if host is None:
                # mkhost takes exactly one of -fcwwpn / -iscsiname / -nqn; the
                # other port kinds are added with addhostport below.
                kind = next(k for k, v in wanted.items() if v)
                opts: dict[str, Any] = {"name": spec.name,
                                        kind: _PORT_SEP[kind].join(wanted[kind]),
                                        "force": True}
                if protocol:
                    opts["protocol"] = protocol
                self._cmd("mkhost", **opts)
                have = {k: {v.lower() for v in wanted[kind]} if k == kind else set()
                        for k in wanted}
            else:
                have = self._ports(host)
            for key, values in wanted.items():
                missing = [v for v in values if v.lower() not in have[key]
                           and v not in have[key]]
                if missing:
                    port_opts: dict[str, Any] = {key: _PORT_SEP[key].join(missing),
                                                 "force": True}
                    self._cmd("addhostport", spec.name, **port_opts)
        except ArrayConflictError as e:
            code = _CODE_RE.search(str(e))
            if code and code.group(0) in PORT_TAKEN_CODES:
                raise ArrayConflictError(
                    f"An initiator of host {spec.name} is registered to another host "
                    f"(or is invalid): {e}") from e
            raise
        return spec.name

    def delete_host(self, host_id: str) -> None:
        try:
            self._cmd("rmhost", host_id)
        except ArrayNotFoundError:
            pass

    # -- mappings -------------------------------------------------------------------

    def _maps(self, volume_id: str) -> list[dict[str, Any]]:
        data = self._cmd("lsvdiskhostmap", volume_id, missing_ok=True)
        if data is None:
            raise ArrayNotFoundError(f"Volume {volume_id} does not exist")
        return _rows(data)

    @staticmethod
    def _mapping(volume_id: str, row: dict[str, Any]) -> Mapping:
        scsi = str(row.get("SCSI_id") or "")
        return Mapping(volume_id=volume_id, host_id=str(row["host_name"]),
                       lun=int(scsi) if scsi.isdigit() else None)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return [self._mapping(volume_id, r) for r in self._maps(volume_id)
                if r.get("host_name")]

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        for m in self.list_mappings(volume_id):
            if m.host_id == host_id:
                return m
        try:
            # force: allow the volume to be mapped to more than one host.
            self._cmd("mkvdiskhostmap", volume_id, host=host_id, force=True)
        except ArrayConflictError as e:
            if "CMMVC5878E" not in str(e):  # raced with another mapper: fine
                raise
        for m in self.list_mappings(volume_id):
            if m.host_id == host_id:
                return m
        raise ArrayError(f"Mapping of volume {volume_id} to host {host_id} not found "
                         "after mkvdiskhostmap")

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        if not any(m.host_id == host_id for m in self.list_mappings(volume_id)):
            return
        self._cmd("rmvdiskhostmap", volume_id, missing_ok=True, host=host_id)
