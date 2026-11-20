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
* conflict: CMMVC6035E (name exists), CMMVC6581E (IQN/NQN already assigned
  or invalid), CMMVC5867E (WWPN already assigned or invalid), CMMVC5878E
  (volume already mapped to that host), CMMVC6071E (mapped to another host,
  needs ``force``), CMMVC5840E (volume still mapped), CMMVC5871E (host
  still has mappings)

Objects: volumes are addressed by their numeric id (``Volume.id``), hosts by
name (``ensure_host`` returns the host name). Note that the array reuses the
lowest free id after a deletion, so a stale volume id can address a
different volume later -- callers pass ``expected_identifier`` (the WWN) to
the client, which then refuses to act on the wrong volume.

Volumes are created with ``mkvolume`` (``-thin`` unless ``thin`` is False);
the virtual capacity has 512-byte granularity (extents only govern the real
allocation). Resizing grows with ``expandvdisksize`` (a delta). Deletion uses
``rmvolume``. Volume identity: ``vdisk_UID`` (32 hex) -> ``naa.<lowercase>``.

Initiator ownership: ``mkhost``/``addhostport`` fail with CMMVC6581E /
CMMVC5867E when a port already belongs to another host; the adapter first
reads its own host (``lshost <name>``, ports in the ``nodes`` list) so ports
it already owns are never re-added, and treats those codes as
:class:`ArrayConflictError`.

Verify against a real system
----------------------------
* REST root: ``/rest`` (as used by IBM's Ansible collection). Code level
  8.5+ documents ``/rest/v1``; set the ``rest_root`` option if ``/rest`` is
  rejected.
* HTTP status on an expired token (assumed 403, 401 handled the same) and on
  bad credentials at ``/rest/auth`` (assumed 403).
* Error body format: plain text with the CMMVC message (JSON bodies are also
  parsed). Codes CMMVC5804E, CMMVC5840E (rmvolume of a mapped volume; newer
  levels may answer with a different code), CMMVC5871E (rmhost with
  mappings) and the NQN-already-assigned code (assumed CMMVC6581E as for
  IQNs) are from memory of the CLI reference.
* ``mkhost -protocol`` values for NVMe hosts (``tcpnvme``, ``rdmanvme`` on
  8.6+; older levels only knew ``nvme``) -- set ``nvme_protocol_value`` if
  the array rejects them. SCSI hosts are created without ``-protocol``.
* Detail views (``lsvdisk <id>``, ``lshost <name>``) may come back as a list
  of dicts (object, then copies) or one dict; ``lsvdiskhostmap`` with a
  single row may come back as a dict instead of a one-element list. Both
  shapes are accepted.
* ``mkvolume -unit b`` with a size that is not a 512-byte multiple (we never
  send one).
* ``mkhost -force`` for FC WWPNs that are not logged in (we always pass it).
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
    "CMMVC6581E",  # IQN (and, we assume, NQN) already assigned or not valid
    "CMMVC5867E",  # WWPN already assigned or not valid
    "CMMVC5878E",  # volume already mapped to this host
    "CMMVC6071E",  # volume mapped to another host; needs -force
    "CMMVC5840E",  # volume not deleted: still mapped
    "CMMVC5871E",  # host has mappings
})
#: Codes meaning "this port is owned by another host" on mkhost/addhostport.
PORT_TAKEN_CODES = frozenset({"CMMVC6581E", "CMMVC5867E"})

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
            "iscsiname": spec.addresses(Protocol.ISCSI),
            "fcwwpn": [a.upper() for a in spec.addresses(Protocol.FC)],
            "nqn": spec.addresses(Protocol.NVME_TCP, Protocol.NVME_RDMA),
        }
        protocol = self._nvme_protocol(spec)
        host = self._host(spec.name)
        try:
            if host is None:
                opts: dict[str, Any] = {"name": spec.name}
                if wanted["iscsiname"]:
                    opts["iscsiname"] = ",".join(wanted["iscsiname"])
                if wanted["fcwwpn"]:
                    opts["fcwwpn"] = ":".join(wanted["fcwwpn"])
                    opts["force"] = True
                if wanted["nqn"]:
                    opts["nqn"] = ",".join(wanted["nqn"])
                if protocol:
                    opts["protocol"] = protocol
                self._cmd("mkhost", **opts)
                return spec.name
            have = self._ports(host)
            for key, values in wanted.items():
                missing = [v for v in values if v.lower() not in have[key]
                           and v not in have[key]]
                if not missing:
                    continue
                sep = ":" if key == "fcwwpn" else ","
                opts = {key: sep.join(missing)}
                if key == "fcwwpn":
                    opts["force"] = True
                self._cmd("addhostport", spec.name, **opts)
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
