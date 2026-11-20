"""Dell PowerFlex (formerly ScaleIO / VxFlex OS), REST gateway API, PowerFlex
3.6 and 4.x.

Auth (option ``auth``):

* ``token`` (3.x gateway): ``GET /api/login`` with HTTP basic
  ``username:password`` returns a session token as a JSON *string*; every
  later request uses basic auth ``username:<token>``. The token expires after
  8 h, or 10 min idle (-> 401; the client logs in again).
  ``GET /api/logout`` invalidates it.
* ``oauth`` (4.x, PowerFlex Manager gateway): ``POST /rest/auth/login``
  ``{"username", "password"}`` returns ``{"access_token", "refresh_token",
  ...}``; requests carry ``Authorization: Bearer <access_token>``. The
  ``/api/...`` paths below are unchanged behind the 4.x gateway.
  ``POST /rest/auth/logout {"refresh_token"}`` ends the session. The access
  token is short-lived (minutes); rather than refreshing it, the adapter
  logs in again on the 401 (``ArrayClient`` does this transparently).
* ``auto`` (default): ``GET /api/login`` as for ``token`` (served by 3.x
  and 4.x gateways alike), then ``GET /api/version`` (a JSON string such as
  ``"3.6"`` / ``"4.5"``): below 4.0 the token goes in basic auth as for
  ``token``, from 4.0 on as ``Authorization: Bearer <token>``. This is how
  Dell's own clients pick the scheme (goscaleio ``Authenticate`` /
  ``BearerAuthenticationMinVersion``; PyPowerFlex ``get_api_version``).

Volumes are addressed by their 16-hex-digit id (``Volume::<id>``). Sizes
are in KiB on create and GiB on resize and are rounded up by the array to a
multiple of 8 GiB, so the adapter declares 8 GiB granularity and minimum.
Names are at most 31 characters. Volumes are created thin
(``ThinProvisioned``) in the ``storage_pool_id`` option's pool.

Hosts: PowerFlex's native protocol is the SDC (storage data client) kernel
driver, which registers itself with the MDM when installed on the host. The
adapter never creates SDCs: ``ensure_host`` looks the SDC up by its GUID
(the initiator address, ``drv_cfg --query_guid`` on the host) and returns
its id as the host id. An SDC is one OS instance, so a host has exactly one
SDC GUID; specs with more are refused (:class:`ArrayUnsupportedError`).

SDC names stand in for host ownership (option ``sdc_naming``):

* ``claim`` (default): an unnamed SDC is named after the host
  (``setSdcName``); an SDC already carrying the *same* name is that host;
  an SDC carrying a *different* name belongs to another host and raises
  :class:`ArrayConflictError` (an admin-named SDC must therefore be
  registered under its PowerFlex name).
* ``ignore``: names are neither read nor written; any host name maps to the
  SDC with the given GUID (no ownership conflict detection).

``delete_host`` is a no-op for SDCs: the SDC belongs to the host's PowerFlex
installation (removing it is ``removeSdc``, an admin decision) and keeps the
name ``claim`` gave it.

Mappings are SDC mappings (``addMappedSdc`` / ``removeMappedSdc``); the
volume's ``mappedSdcInfo`` lists them (``null``, not ``[]``, when there are
none). Mapping a volume to a second SDC needs ``allowMultipleMappings``
(sent as ``"true"``, as csi-powerflex does).
SDC mappings have no LUN number (``Mapping.lun`` is ``None``) and SDC
volumes no NAA WWN or NGUID, so ``Volume.identifier`` is ``id.<volume id>``.

NVMe/TCP (4.x NVMe hosts, ``/api/types/Host/instances``) is not
implemented: a 4.x NVMe host has exactly one NQN and is mapped with
``addMappedHost`` rather than ``addMappedSdc`` (see below).

Errors: the gateway answers ``{"message", "httpStatusCode", "errorCode"}``,
and reports "not found" as HTTP **500** (sometimes 400) rather than 404, so
missing objects are recognised by message text; any other 500 stays an
:class:`ArrayRequestError`. Booleans and numbers in action bodies are sent as
strings (``"true"``, ``"8388608"``), as Dell's clients send them.

Checked against vendor source
-----------------------------
(PyPowerFlex = github.com/dell/python-powerflex, goscaleio =
github.com/dell/goscaleio, csi-powerflex = github.com/dell/csi-powerflex,
ansible-powerflex = github.com/dell/ansible-powerflex)

* ``GET /api/login`` (basic user:password -> JSON string token), basic
  ``user:<token>`` on 3.x, ``GET /api/logout`` (PyPowerFlex
  ``base_client.py`` ``_login`` / ``_logout``); a 4.x gateway still serves
  ``/api/login`` and takes that token as a Bearer token (goscaleio
  ``api.go`` ``basicAuthenticate``, ``api/api.go``); ``GET /api/version``
  picks the scheme (PyPowerFlex ``is_version_3``).
* 4.x OAuth: ``POST /rest/auth/login {"username", "password"}`` ->
  ``{"access_token", "refresh_token"}``, ``POST /rest/auth/logout
  {"refresh_token"}`` answering 204 (PyPowerFlex ``_appliance_login`` /
  ``_appliance_logout``).
* Error texts: "Could not find the volume" (csi-powerflex
  ``sioGatewayVolumeNotFound``); ``queryIdByKey`` for a missing name answers
  "Not found" (goscaleio ``instance.go`` ``GetVolume``); "Volume name
  already in use. Please use a different name."
  (``sioGatewayVolumeNameInUse``).
* Gen1 arrays create volumes in multiples of 8 GiB, rounding up
  (csi-powerflex ``validateVolSize``); Gen2/EC arrays use 1 GiB, which the
  8 GiB granularity also satisfies.
* ``addMappedSdc {"sdcId", "allowMultipleMappings", "accessMode"}`` with
  ``accessMode`` ``ReadWrite`` (goscaleio ``MapVolumeSdcParam``; PyPowerFlex
  ``add_mapped_sdc``; ansible-powerflex ``get_access_mode``);
  ``allowMultipleMappings`` as a boolean string (csi-powerflex
  ``strconv.FormatBool``, PyPowerFlex ``str(bool)``).
* ``removeMappedSdc``, ``setVolumeSize {"sizeInGB"}``, ``removeVolume
  {"removeMode": "ONLY_ME"}``, ``queryIdByKey {"name"}``, create body
  ``volumeSizeInKb`` / ``storagePoolId`` / ``volumeType`` as strings
  (goscaleio ``sdc.go``, ``volume.go``, ``instance.go``,
  ``types/v1/types.go`` ``VolumeParam``).
* Fields ``Sdc.sdcGuid`` / ``Sdc.name``, ``Volume.mappedSdcInfo[].sdcId``,
  ``System.systemVersionName`` (goscaleio ``types/v1/types.go``).
* NVMe hosts map with ``addMappedHost {"hostId", ...}`` (goscaleio
  ``nvme_host.go`` ``MapVolumeNVMe``) -- not implemented here.

Verify against a real system
----------------------------
* ``auth=oauth`` against a 3.x gateway (assumed: 404 for
  ``/rest/auth/login``); whether ``GET /api/logout`` ends a Bearer session
  on 4.x (the adapter calls it best-effort).
* 4.x token lifetime and whether a refresh-token grant would be preferable
  to re-login.
* Status codes (500 vs 400 vs 404) and the wording for "already mapped",
  "not mapped" and an SDC name in use.
* That ``setVolumeSize`` rounds a non-multiple of 8 up rather than
  rejecting it (the adapter always sends multiples of 8, so this only
  matters for volumes created outside simplyblock).
* Whether SDC names are unique system-wide (the fake assumes so).
"""
from __future__ import annotations

import re
from typing import Any

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

GIB = 1 << 30
KIB = 1 << 10

_AUTH_MODES = ("auto", "token", "oauth")
_NAMING_MODES = ("claim", "ignore")
_NOT_FOUND = ("could not find", "not found", "does not exist", "no such")
_CONFLICT = ("already in use", "already exists", "already mapped", "is mapped",
             "name in use")
_NOT_MAPPED = ("not mapped",)


def _json_string(text: str | None) -> str:
    """A body that is a bare JSON string (``"abc"``), unquoted."""
    return (text or "").strip().strip('"')


def _message(body: Any, fallback: str) -> str:
    if isinstance(body, dict) and body.get("message"):
        msg = str(body["message"])
        code = body.get("errorCode")
        return f"{msg} (errorCode {code})" if code is not None else msg
    return fallback


class DellPowerFlexAdapter(ArrayAdapter):
    vendor = "dell-powerflex"
    display_name = "Dell PowerFlex"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.SDC}),
        size_granularity_bytes=8 * GIB,
        min_size_bytes=8 * GIB,
        max_name_len=31,
        max_initiators_per_host=1,
    )
    option_docs = (
        OptionDoc("storage_pool_id", "Id of the storage pool new volumes are created in.",
                  required=True),
        OptionDoc("protection_domain_id", "Protection domain of the storage pool "
                  "(sent on create; only needed where the gateway asks for it)."),
        OptionDoc("auth", "'token' (3.x /api/login, basic auth), 'oauth' (4.x "
                  "/rest/auth/login) or 'auto' (/api/login, then basic or Bearer by "
                  "/api/version).", default="auto"),
        OptionDoc("sdc_naming", "'claim': name unnamed SDCs after the host and treat an "
                  "SDC named differently as another host's; 'ignore': never read or "
                  "write SDC names.", default="claim"),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        if self.option("auth") not in _AUTH_MODES:
            raise ArrayError(f"PowerFlex option auth must be one of {', '.join(_AUTH_MODES)}")
        if self.option("sdc_naming") not in _NAMING_MODES:
            raise ArrayError(
                f"PowerFlex option sdc_naming must be one of {', '.join(_NAMING_MODES)}")
        self._mode = ""
        self._refresh_token = ""

    # -- session ------------------------------------------------------------

    def _reset_auth(self) -> None:
        self.session.clear_auth()
        self.session.headers.pop("Authorization", None)
        self._mode = ""
        self._refresh_token = ""

    def login(self) -> None:
        self._reset_auth()
        mode = self.option("auth")
        user, password = self.secret("username"), self.secret("password")
        if mode == "oauth":
            resp = self.session.post("/rest/auth/login",
                                     json={"username": user, "password": password})
            body = json_of(resp) or {}
            token = body.get("access_token") if isinstance(body, dict) else None
            if not token:
                raise ArrayRequestError("PowerFlex /rest/auth/login returned no access_token")
            self.session.headers["Authorization"] = f"Bearer {token}"
            self._refresh_token = str(body.get("refresh_token") or "")
            self._mode = "oauth"
            return
        resp = self.session.get("/api/login", auth=(user, password))
        token = _json_string(resp.text)
        if not token:
            raise ArrayRequestError("PowerFlex /api/login returned no token")
        if mode == "auto":
            version = _json_string(self.session.get("/api/version", auth=(user, token)).text)
            m = re.match(r"^(\d+)\.", version)
            if not m:
                raise ArrayRequestError(f"PowerFlex /api/version returned {version!r}")
            if int(m.group(1)) >= 4:
                # goscaleio: 4.x takes the /api/login token as a Bearer token.
                self.session.headers["Authorization"] = f"Bearer {token}"
                self._mode = "bearer"
                return
        self.session.set_basic_auth(user, token)
        self._mode = "token"

    def close(self) -> None:
        try:
            if self._mode in ("token", "bearer"):
                self.session.get("/api/logout")
            elif self._mode == "oauth":
                self.session.post("/rest/auth/logout",
                                  json={"refresh_token": self._refresh_token})
        except Exception:
            pass
        self._mode = ""

    # -- request helpers -----------------------------------------------------

    def _req(self, method: str, path: str, *, json: Any = None,
             missing_ok: bool = False) -> Any:
        """Issue a request; returns the decoded body, or ``None`` for a
        missing object when ``missing_ok``."""
        resp = self.session.request(method, path, json=json, ok=(400, 404, 500))
        if resp.status_code < 300:
            if not resp.content:
                return None
            try:
                return resp.json()
            except ValueError:
                return resp.text
        try:
            body = resp.json()
        except ValueError:
            body = None
        msg = _message(body, body_text(resp))
        lowered = msg.lower()
        if resp.status_code == 404 or any(s in lowered for s in _NOT_FOUND):
            if missing_ok:
                return None
            raise ArrayNotFoundError(f"{method} {path}: {msg}")
        if any(s in lowered for s in _CONFLICT):
            raise ArrayConflictError(f"{method} {path}: {msg}")
        raise ArrayRequestError(f"{method} {path} -> HTTP {resp.status_code}: {msg}",
                                status=resp.status_code)

    def _raw_volume(self, volume_id: str, missing_ok: bool = False) -> dict[str, Any] | None:
        body = self._req("GET", f"/api/instances/Volume::{volume_id}", missing_ok=missing_ok)
        if body is None:
            return None
        if not isinstance(body, dict):
            raise ArrayRequestError(f"PowerFlex returned no volume for {volume_id}")
        return body

    @staticmethod
    def _volume(v: dict[str, Any]) -> Volume:
        return Volume(id=str(v["id"]), name=v.get("name") or "",
                      size_bytes=int(v["sizeInKb"]) * KIB)

    def _action(self, volume_id: str, action: str, body: dict[str, Any]) -> Any:
        return self._req("POST", f"/api/instances/Volume::{volume_id}/action/{action}",
                         json=body)

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        systems = self._req("GET", "/api/types/System/instances") or []
        system = systems[0] if isinstance(systems, list) and systems else {}
        return ArrayInfo(vendor=self.vendor, model="PowerFlex",
                         version=str(system.get("systemVersionName") or ""),
                         serial=str(system.get("id") or ""))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        body: dict[str, Any] = {
            "name": name,
            "volumeSizeInKb": str(size_bytes // KIB),
            "storagePoolId": self.option("storage_pool_id"),
            "volumeType": "ThinProvisioned",
        }
        if self.option("protection_domain_id"):
            body["protectionDomainId"] = self.option("protection_domain_id")
        created = self._req("POST", "/api/types/Volume/instances", json=body)
        vid = created.get("id") if isinstance(created, dict) else None
        if not vid:
            raise ArrayRequestError("PowerFlex volume create returned no id")
        raw = self._raw_volume(vid)
        assert raw is not None
        return self._volume(raw)

    def get_volume(self, volume_id: str) -> Volume | None:
        raw = self._raw_volume(volume_id, missing_ok=True)
        return self._volume(raw) if raw else None

    def find_volume(self, name: str) -> Volume | None:
        vid = self._req("POST", "/api/types/Volume/instances/action/queryIdByKey",
                        json={"name": name}, missing_ok=True)
        if not vid or not isinstance(vid, str):
            return None
        return self.get_volume(vid)

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        size_gb = -(-size_bytes // GIB)
        self._action(volume_id, "setVolumeSize", {"sizeInGB": str(size_gb)})
        raw = self._raw_volume(volume_id)
        assert raw is not None
        return self._volume(raw)

    def delete_volume(self, volume_id: str) -> None:
        self._action(volume_id, "removeVolume", {"removeMode": "ONLY_ME"})

    # -- hosts (SDCs) -----------------------------------------------------------

    def _sdcs(self) -> list[dict[str, Any]]:
        body = self._req("GET", "/api/types/Sdc/instances")
        return list(body) if isinstance(body, list) else []

    def ensure_host(self, spec: HostSpec) -> str:
        guids = spec.addresses(Protocol.SDC)
        if len(guids) != 1 or len(spec.initiators) != 1:
            raise ArrayUnsupportedError(
                "A PowerFlex host is one SDC: give exactly one SDC GUID per host")
        guid = guids[0]
        sdc = next((s for s in self._sdcs()
                    if str(s.get("sdcGuid") or "").lower() == guid.lower()), None)
        if sdc is None:
            raise ArrayNotFoundError(
                f"No SDC with GUID {guid} is registered with PowerFlex; install the SDC "
                "on the host and connect it to the MDM first")
        sdc_id = str(sdc["id"])
        if self.option("sdc_naming") == "claim":
            current = sdc.get("name") or ""
            if current and current != spec.name:
                raise ArrayConflictError(
                    f"SDC {guid} is registered as host {current!r}, not {spec.name!r}")
            if not current:
                self._req("POST", f"/api/instances/Sdc::{sdc_id}/action/setSdcName",
                          json={"sdcName": spec.name})
        return sdc_id

    def delete_host(self, host_id: str) -> None:
        """No-op: SDCs belong to the host's PowerFlex installation."""

    # -- mappings ---------------------------------------------------------------

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        raw = self._raw_volume(volume_id)
        assert raw is not None
        info = raw.get("mappedSdcInfo") or []  # null when unmapped
        return [Mapping(volume_id=volume_id, host_id=str(m["sdcId"]), lun=None)
                for m in info if m.get("sdcId")]

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        for m in self.list_mappings(volume_id):
            if m.host_id == host_id:
                return m
        self._action(volume_id, "addMappedSdc", {
            "sdcId": host_id, "allowMultipleMappings": "true", "accessMode": "ReadWrite"})
        return Mapping(volume_id=volume_id, host_id=host_id, lun=None)

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        raw = self._raw_volume(volume_id, missing_ok=True)
        if raw is None:
            return
        if not any(str(m.get("sdcId")) == host_id for m in raw.get("mappedSdcInfo") or []):
            return
        try:
            self._action(volume_id, "removeMappedSdc", {"sdcId": host_id})
        except ArrayRequestError as e:
            if not any(s in str(e).lower() for s in _NOT_MAPPED):
                raise
