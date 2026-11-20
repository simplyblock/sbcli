"""HPE WSAPI: Alletra Storage MP B10000, Primera and 3PAR StoreServ.

Auth: ``POST /api/v1/credentials {"user", "password"}`` returns ``{"key"}``,
sent as the ``X-HP3PAR-WSAPI-SessionKey`` header on every later request and
released with ``DELETE /api/v1/credentials/<key>``. An expired or unknown key
is answered with HTTP 401/403 (mapped to :class:`ArrayAuthError`, so the client
logs in again).

Objects are addressed by name: ``Volume.id`` is the volume name and the host
id is the host name. Errors carry ``{"code": <int>, "desc": "..."}``.

Volumes: ``POST /api/v1/volumes {name, cpg, sizeMiB, tpvv|reduce}``; grow is
``PUT /api/v1/volumes/<name> {"action": 3, "sizeMiB": <increment>}`` -- GROW
takes the *delta*, not the new size. SCSI identity: ``naa.<wwn>`` from the
volume's ``wwn`` field (``60002AC...``).

Hosts: ``POST /api/v1/hosts {name, persona, iSCSINames|FCWWNs}``; paths are
added with ``PUT /api/v1/hosts/<name> {"pathOperation": 1, ...}``. Note the
asymmetry: requests use ``iSCSINames``/``FCWWNs``, responses report
``iSCSIPaths[].name``/``FCPaths[].wwn``.

VLUNs (mappings): ``POST /api/v1/vluns {volumeName, hostname, lun, autoLun,
maxAutoLun}`` answers 201 with an empty body and a ``Location`` header
``/api/v1/vluns/<vol>,<lun>,<host>``. ``GET /api/v1/vluns?query="volumeName EQ
<vol>"`` lists both the VLUN *template* and one *active* VLUN per host path,
so entries are de-duplicated by (volume, host).

Only iSCSI and FC hosts are supported: NVMe/TCP and NVMe/FC hosts exist on
Alletra MP and Primera (python-3parclient's ``createHost`` sends an ``NQN``
field) but NVMe host paths and exports are not implemented here, so NVMe is
refused up front.

Checked against vendor source
-----------------------------
(python-3parclient ``hpe3parclient/client.py``/``http.py`` and its WSAPI mock
``test/HPE3ParMockServer_flask.py``; HPE ``hpe_storage_flowkit_py``
``v1/core/rest_client.py``/``session.py``/``utils/volume_utils.py``, the
library behind Cinder's Alletra MP driver; Cinder ``hpe_3par_common.py``,
``hpe_3par_iscsi.py``; HPE ``common-host-libs`` ``linux/device.go``.)

* Login/logout, the session header, ``{"code", "desc"}`` errors.
* Error codes: 5 INV_USER_PASS (403 on login), 16 EXISTENT_HOST (409), 17
  NON_EXISTENT_HOST (404), 19 NON_EXISTENT_VLUN (404), 22 EXISTENT_VOL
  (409), 23 NON_EXISTENT_VOL (404), 26 EXPORTED_VLUN (409, host still
  exported), 34 "resource in use" (409, e.g. volume in a VV set or
  exported), 73 EXISTENT_PATH (409). Code 29 is *not* a VLUN conflict:
  Cinder sees it as HTTP 400 on a volume delete, so it is no longer treated
  as a conflict.
* 403 is also used for non-auth refusals (PERM_DENIED, 150 cannot grow this
  volume type, 153 new size exceeds the CPG limit, 120 volume in remote copy,
  VV_SIZE_CANNOT_REDUCE, ...). python-3parclient still re-authenticates once
  on any 401/403, so a 403 stays an auth error except for the listed
  non-auth codes (``_FORBIDDEN_NOT_AUTH``), which fail directly.
* GROW (``action`` 3) takes the increment in MiB; minimum create size is
  256 MiB, name limit 31 characters (vendor mock).
* ``reduce: true`` is the dedup+compression create key on Primera *and*
  Alletra MP (python-3parclient maps ``tdvv``+``compression`` to it when the
  WSAPI is Primera-class; flowkit sends ``tpvv``/``reduce``); ``tpvv: true``
  is thin. 3PAR (pre-Primera) dedup/compression is not offered.
* ``POST /hosts`` and ``PUT /hosts`` refuse ``iSCSINames`` together with
  ``FCWWNs`` (INV_INPUT_PARAM_CONFLICT), hence one path type per request; a
  host may carry both types (Cinder's iSCSI driver adds iSCSI names to an
  existing host that has none).
* VLUN query ``"volumeName EQ <vol>"`` (quotes included), ``members`` list,
  template VLUNs ``active: false`` plus active per-path VLUNs; the
  ``Location`` header is ``/api/v1/vluns/<vol>,<lun>,<host>``.
* Personas: 1 Generic, 2 Generic-ALUA, 8 VMware, 11 WindowsServer.
* Volume ``wwn`` field; HPE host tooling shows 3PAR-family WWIDs as
  ``naa.60002ac...``.

Verify against a real system
----------------------------
* The status/code for an expired or unknown session key (the fake uses 401
  code 6); python-3parclient re-authenticates on both 401 and 403.
* Size granularity on Alletra MP (the 256 MiB step is from the vendor mock
  and growVolume's "rounded up to the next chunklet size (e.g. 256 or 1000
  MiB)"); we round to 256 MiB so requested and reported sizes agree.
* A filtered VLUN query with no hits answering 200 with an empty
  ``members`` list (python-3parclient implies it but cannot tell it apart
  from a 404) and the status/code for a duplicate VLUN.
* Max session count per user (stale keys from crashed processes may exhaust
  it; flowkit reuses one session per user and drops it after 14 minutes);
  we always ``DELETE`` the key on close.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote, unquote

import requests

from ..base import (
    ArrayAdapter,
    ArrayAuthError,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    ArrayNotFoundError,
    ArrayRequestError,
    Capabilities,
    HostSpec,
    Mapping,
    OptionDoc,
    Protocol,
    Volume,
)
from ..http import body_text, json_of

MIB = 1 << 20
_GRANULARITY = 256 * MIB

SESSION_HEADER = "X-HP3PAR-WSAPI-SessionKey"
_API = "/api/v1"

GROW_VOLUME = 3
PATH_ADD = 1

_NOT_FOUND_CODES = frozenset({17, 19, 23})
_CONFLICT_CODES = frozenset({16, 22, 26, 34, 73})
#: 403 codes that are refusals, not session/credential problems: 120 volume
#: in remote copy, 150 cannot grow this volume type, 153 new size exceeds the
#: CPG limit, 215 remote copy group already started.
_FORBIDDEN_NOT_AUTH = frozenset({120, 150, 153, 215})

_LOCATION_RE = re.compile(r"/api/v1/vluns/(?P<vol>[^,]+),(?P<lun>\d+),(?P<host>[^,/]*)")


def wsapi_wwn(wwn: str) -> str:
    return "naa." + wwn.lower()


class HpeWsapiAdapter(ArrayAdapter):
    vendor = "hpe-wsapi"
    display_name = "HPE Alletra MP / Primera / 3PAR"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC}),
        size_granularity_bytes=_GRANULARITY,
        min_size_bytes=_GRANULARITY,
        max_name_len=31,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("cpg", "Common Provisioning Group new volumes are created in.",
                  required=True),
        OptionDoc("provisioning", "'tpvv' (thin) or 'reduce' (thin + dedup + "
                  "compression, Primera / Alletra MP).", default="tpvv"),
        OptionDoc("persona", "Host persona id for new hosts (2 = Generic-ALUA).",
                  default=2),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._key: str | None = None
        provisioning = str(self.option("provisioning")).lower()
        if provisioning not in ("tpvv", "reduce"):
            raise ArrayError(f"{self.display_name}: provisioning must be 'tpvv' or "
                             f"'reduce', not {provisioning!r}")
        self._provisioning = provisioning

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop(SESSION_HEADER, None)
        resp = self._call("POST", "/credentials",
                          json={"user": self.secret("username"),
                                "password": self.secret("password")})
        key = (json_of(resp) or {}).get("key")
        if not key:
            raise ArrayRequestError("WSAPI login returned no session key")
        self._key = key
        self.session.headers[SESSION_HEADER] = key

    def close(self) -> None:
        key, self._key = self._key, None
        self.session.headers.pop(SESSION_HEADER, None)
        if key:
            try:
                self.session.delete(f"{_API}/credentials/{quote(key, safe='')}",
                                    headers={SESSION_HEADER: key}, ok=(401, 403, 404))
            except Exception:
                pass

    # -- request helpers -----------------------------------------------------

    def _call(self, method: str, path: str, *, params: Any = None, json: Any = None,
              missing_ok: bool = False) -> requests.Response:
        """One WSAPI request; maps ``{"code", "desc"}`` errors to ArrayErrors.
        With ``missing_ok`` a not-found answer is returned instead of raised."""
        resp = self.session.request(method, f"{_API}{path}", params=params, json=json,
                                    ok=(400, 401, 403, 404, 409))
        status = resp.status_code
        if status < 400:
            return resp
        code: int | None = None
        desc = body_text(resp)
        try:
            body = resp.json()
            if isinstance(body, dict):
                code = int(body["code"]) if body.get("code") is not None else None
                desc = str(body.get("desc", desc))
        except (ValueError, TypeError, KeyError):
            pass
        msg = f"{method} {path} -> HTTP {status}" + (f" code {code}" if code is not None else "") \
            + f": {desc}"
        if status == 403 and code in _FORBIDDEN_NOT_AUTH:
            raise ArrayRequestError(msg, status=status)
        if status in (401, 403):
            raise ArrayAuthError(msg)
        if status == 404 or code in _NOT_FOUND_CODES:
            if missing_ok:
                return resp
            raise ArrayNotFoundError(msg)
        if status == 409 or code in _CONFLICT_CODES:
            raise ArrayConflictError(msg)
        raise ArrayRequestError(msg, status=status)

    @staticmethod
    def _found(resp: requests.Response) -> bool:
        return resp.status_code < 400

    @staticmethod
    def _volume(item: dict[str, Any]) -> Volume:
        wwn = item.get("wwn") or ""
        return Volume(id=item["name"], name=item["name"],
                      size_bytes=int(item["sizeMiB"]) * MIB,
                      wwn=wsapi_wwn(wwn) if wwn else None)

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        system = json_of(self._call("GET", "/system")) or {}
        return ArrayInfo(vendor=self.vendor, model=str(system.get("model", "")),
                         version=str(system.get("systemVersion", "")),
                         serial=str(system.get("serialNumber", "")))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        body: dict[str, Any] = {"name": name, "cpg": self.option("cpg"),
                                "sizeMiB": -(-size_bytes // MIB)}
        body["reduce" if self._provisioning == "reduce" else "tpvv"] = True
        self._call("POST", "/volumes", json=body)
        vol = self.get_volume(name)
        if vol is None:
            raise ArrayRequestError(f"Volume {name} not found right after creation")
        return vol

    def get_volume(self, volume_id: str) -> Volume | None:
        resp = self._call("GET", f"/volumes/{quote(volume_id, safe='')}", missing_ok=True)
        return self._volume(json_of(resp)) if self._found(resp) else None

    def find_volume(self, name: str) -> Volume | None:
        return self.get_volume(name)

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        vol = self.get_volume(volume_id)
        if vol is None:
            raise ArrayNotFoundError(f"Volume {volume_id} does not exist")
        delta = -(-size_bytes // MIB) - vol.size_bytes // MIB
        if delta > 0:
            self._call("PUT", f"/volumes/{quote(volume_id, safe='')}",
                       json={"action": GROW_VOLUME, "sizeMiB": delta})
        elif delta < 0:
            raise ArrayConflictError(f"WSAPI cannot shrink volume {volume_id}")
        new = self.get_volume(volume_id)
        if new is None:
            raise ArrayNotFoundError(f"Volume {volume_id} vanished during resize")
        return new

    def delete_volume(self, volume_id: str) -> None:
        self._call("DELETE", f"/volumes/{quote(volume_id, safe='')}")

    def _host(self, name: str) -> dict[str, Any] | None:
        resp = self._call("GET", f"/hosts/{quote(name, safe='')}", missing_ok=True)
        return json_of(resp) if self._found(resp) else None

    def ensure_host(self, spec: HostSpec) -> str:
        unsupported = [p for p in spec.protocols if not p.is_scsi]
        if unsupported:
            raise ArrayError(f"{self.display_name} adapter does not support "
                             + ", ".join(sorted(p.value for p in unsupported)))
        iqns = spec.addresses(Protocol.ISCSI)
        wwns = [w.upper() for w in spec.addresses(Protocol.FC)]
        host = self._host(spec.name)
        if host is None:
            # Create with one path type (WSAPI refuses iSCSINames together
            # with FCWWNs: INV_INPUT_PARAM_CONFLICT); a second type is added below.
            body: dict[str, Any] = {"name": spec.name, "persona": int(self.option("persona"))}
            if iqns:
                body["iSCSINames"] = iqns
            else:
                body["FCWWNs"] = wwns
            self._call("POST", "/hosts", json=body)
            if not (iqns and wwns):
                return spec.name
            host = {"iSCSIPaths": [{"name": i} for i in iqns], "FCPaths": []}
        have_iqns = {str(p.get("name", "")).lower() for p in host.get("iSCSIPaths") or []}
        have_wwns = {str(p.get("wwn", "")).upper() for p in host.get("FCPaths") or []}
        add_iqns = [i for i in iqns if i.lower() not in have_iqns]
        add_wwns = [w for w in wwns if w not in have_wwns]
        # One path type per request: a modify with both is refused too.
        if add_iqns:
            self._call("PUT", f"/hosts/{quote(spec.name, safe='')}",
                       json={"pathOperation": PATH_ADD, "iSCSINames": add_iqns})
        if add_wwns:
            self._call("PUT", f"/hosts/{quote(spec.name, safe='')}",
                       json={"pathOperation": PATH_ADD, "FCWWNs": add_wwns})
        return spec.name

    def delete_host(self, host_id: str) -> None:
        self._call("DELETE", f"/hosts/{quote(host_id, safe='')}", missing_ok=True)

    def _vluns(self, volume_id: str, host_id: str | None = None) -> list[Mapping]:
        resp = self._call("GET", "/vluns", params={"query": f'"volumeName EQ {volume_id}"'},
                          missing_ok=True)
        if not self._found(resp):
            return []
        seen: dict[tuple[str, str], Mapping] = {}
        for m in (json_of(resp) or {}).get("members", []):
            host = m.get("hostname")
            if not host or m.get("volumeName") != volume_id:
                continue
            if host_id is not None and host != host_id:
                continue
            # The template and every active per-path VLUN share (vol, lun, host).
            seen.setdefault((volume_id, host),
                            Mapping(volume_id=volume_id, host_id=host, lun=int(m["lun"])))
        return list(seen.values())

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        existing = self._vluns(volume_id, host_id)
        if existing:
            return existing[0]
        resp = self._call("POST", "/vluns",
                          json={"volumeName": volume_id, "hostname": host_id, "lun": 0,
                                "autoLun": True, "maxAutoLun": 0})
        m = _LOCATION_RE.search(unquote(resp.headers.get("Location", "")))
        if m:
            return Mapping(volume_id=volume_id, host_id=host_id, lun=int(m["lun"]))
        existing = self._vluns(volume_id, host_id)
        if not existing:
            raise ArrayRequestError(f"VLUN {volume_id} -> {host_id} not found after creation")
        return existing[0]

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        for m in self._vluns(volume_id, host_id):
            self._call("DELETE", "/vluns/" + quote(f"{volume_id},{m.lun},{host_id}", safe=","),
                       missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return self._vluns(volume_id)
