"""Dell PowerStore, REST API (PowerStoreOS 3.x / 4.x).

Auth: HTTP basic on ``GET /api/rest/login_session`` opens a session. The
response carries the session cookie (``auth_cookie``) and a CSRF token in the
``DELL-EMC-TOKEN`` header; every later request sends the cookie, and every
non-GET request must also send the token header (the array answers 403
without it). Basic auth is *not* kept on the session afterwards, so an expired
session surfaces as 401 and :class:`ArrayClient` logs in again.

Queries use the PostgREST-style syntax: ``select=a,b`` picks fields,
``name=eq.x`` filters, ``limit``/``offset`` page; a partial page is answered
with ``206 Partial Content`` and a ``Content-Range`` header.

Errors: ``{"messages": [{"code": "0x...", "severity": "Error",
"message_l10n": "...", "arguments": [...]}]}``. Semantic failures (name in
use, initiator owned by another host, volume still attached, shrink) come
back as ``422 Unprocessable Entity``; malformed requests as 400.

Objects are addressed by id (UUID) for volumes and hosts. Volume identity:
``wwn`` is returned as ``naa.<hex>``, ``nguid`` as ``nguid.<32 hex>``; this
adapter reports them as ``naa.<hex>`` / ``eui.<hex>`` (lowercase).

Sizes are bytes; PowerStore requires a multiple of 8 KiB (8192) and at least
1 MiB.

Verify against a real system
----------------------------
* The session cookie name (``auth_cookie``) and that the CSRF header is
  exactly ``DELL-EMC-TOKEN`` on both the login response and requests.
* That ``POST /volume`` and ``POST /host`` answer ``201 {"id": ...}`` and that
  PATCH/DELETE/attach/detach answer ``204`` with an empty body.
* The status code (422 assumed) and ``message_l10n`` wording for: duplicate
  name, initiator already registered to another host, delete of an attached
  volume/host, shrink, detach of a non-attached volume. Classification below
  is by wording ("already", "in use", "attached", "mapped", "not found").
* The host field holding initiators on GET is ``host_initiators``
  (create uses ``initiators``, modify ``add_initiators``), each
  ``{"port_name", "port_type"}``; ``port_type`` values ``iSCSI``/``FC``/
  ``NVMe`` (NVMe/TCP and NVMe/FC both ``NVMe``), and that FC ``port_name`` is
  accepted/returned colon-separated lowercase (``21:00:...``).
* That ``logical_unit_number`` in ``host_volume_mapping`` is the NVMe NSID for
  NVMe hosts, and which LUN the array picks first (fake: 1).
* That a ``name=eq.<value>`` filter needs no quoting for host names containing
  dots or commas.
* That the size multiple is 8192 bytes on 4.x as on 3.x.
"""
from __future__ import annotations

from http.cookies import CookieError, SimpleCookie
from typing import Any

import requests

from ..base import (
    ArrayAdapter,
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
    colon_wwpn,
    normalize_wwpn,
)
from ..http import json_of

API = "/api/rest"
TOKEN_HEADER = "DELL-EMC-TOKEN"
VOLUME_FIELDS = "id,name,size,wwn,nguid"
HOST_FIELDS = "id,name,host_initiators"
MAPPING_FIELDS = "id,host_id,host_group_id,volume_id,logical_unit_number"
_PAGE = 1000

_NOT_FOUND = ("not found", "does not exist", "not exist", "no such")
_CONFLICT = ("already", "in use", "exists", "attached", "mapped", "is used", "duplicate")


def _messages(resp: requests.Response) -> str:
    try:
        body = json_of(resp) or {}
    except ArrayRequestError:
        return ""
    if not isinstance(body, dict):
        return ""
    return " ".join(str(m.get("message_l10n") or m.get("code") or "")
                    for m in body.get("messages", []) if isinstance(m, dict))


def _classify(method: str, path: str, resp: requests.Response) -> ArrayError:
    text = _messages(resp) or f"HTTP {resp.status_code}"
    lowered = text.lower()
    msg = f"{method} {path} -> HTTP {resp.status_code}: {text}"
    if resp.status_code == 404 or any(k in lowered for k in _NOT_FOUND):
        return ArrayNotFoundError(msg)
    if any(k in lowered for k in _CONFLICT):
        return ArrayConflictError(msg)
    return ArrayRequestError(msg, status=resp.status_code)


def powerstore_wwn(value: str | None) -> str | None:
    v = (value or "").strip().lower().removeprefix("naa.")
    return f"naa.{v}" if v else None


def powerstore_nguid(value: str | None) -> str | None:
    v = (value or "").strip().lower().removeprefix("nguid.").removeprefix("eui.")
    return f"eui.{v}" if v else None


_PORT_TYPE = {
    Protocol.ISCSI: "iSCSI",
    Protocol.FC: "FC",
    Protocol.NVME_TCP: "NVMe",
    Protocol.NVME_FC: "NVMe",
}


def _port_key(port_type: str, port_name: str) -> str:
    """Comparable form of an initiator as the array reports it."""
    if port_type == "FC":
        try:
            return normalize_wwpn(port_name)
        except ValueError:
            return port_name.lower()
    if port_type == "iSCSI":
        return port_name.lower()
    return port_name


class DellPowerStoreAdapter(ArrayAdapter):
    vendor = "dell-powerstore"
    display_name = "Dell PowerStore"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NVME_FC}),
        size_granularity_bytes=8192,
        min_size_bytes=1 << 20,
        max_name_len=128,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("os_type", "os_type of hosts simplyblock creates.", default="Linux"),
        OptionDoc("appliance_id", "Create volumes on this appliance of a multi-appliance "
                  "cluster (e.g. 'A1'); default: the array places them."),
    )

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop(TOKEN_HEADER, None)
        self.session.http.cookies.clear()
        resp = self.session.get(f"{API}/login_session",
                                auth=(self.secret("username"), self.secret("password")))
        token = resp.headers.get(TOKEN_HEADER)
        if not token:
            raise ArrayRequestError(f"PowerStore login returned no {TOKEN_HEADER} header")
        self.session.headers[TOKEN_HEADER] = token
        self._adopt_cookies(resp)

    def _adopt_cookies(self, resp: requests.Response) -> None:
        """Keep the session cookie. ``requests`` normally stores it already;
        this also covers transports that do not populate the cookie jar."""
        raw = resp.headers.get("Set-Cookie")
        if not raw:
            return
        jar = self.session.http.cookies
        try:
            parsed = SimpleCookie()
            parsed.load(raw)
        except CookieError:
            return
        for name, morsel in parsed.items():
            if name not in jar:
                jar.set(name, morsel.value, path=morsel["path"] or "/")

    def close(self) -> None:
        if TOKEN_HEADER in self.session.headers:
            try:
                self.session.post(f"{API}/logout", ok=(401, 403))
            except Exception:
                pass
            self.session.headers.pop(TOKEN_HEADER, None)

    # -- request helpers -----------------------------------------------------

    def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              json: Any = None, missing_ok: bool = False) -> requests.Response | None:
        resp = self.session.request(method, f"{API}{path}", params=params, json=json,
                                    ok=(400, 404, 422))
        if resp.status_code in (400, 404, 422):
            err = _classify(method, path, resp)
            if missing_ok and isinstance(err, ArrayNotFoundError):
                return None
            raise err
        return resp

    def _one(self, path: str, select: str) -> dict[str, Any] | None:
        resp = self._call("GET", path, params={"select": select}, missing_ok=True)
        return json_of(resp) if resp is not None else None

    def _list(self, path: str, select: str, **filters: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            params: dict[str, Any] = {"select": select, **{k: f"eq.{v}" for k, v in filters.items()},
                                      "limit": _PAGE, "offset": offset}
            resp = self.session.request("GET", f"{API}{path}", params=params,
                                        ok=(206, 400, 422))
            if resp.status_code in (400, 422):
                raise _classify("GET", path, resp)
            items = json_of(resp) or []
            out.extend(items)
            if resp.status_code != 206 or not items:
                return out
            offset += len(items)

    @staticmethod
    def _volume(item: dict[str, Any]) -> Volume:
        return Volume(id=item["id"], name=item["name"], size_bytes=int(item["size"]),
                      wwn=powerstore_wwn(item.get("wwn")),
                      nguid=powerstore_nguid(item.get("nguid")))

    def _require_volume(self, volume_id: str) -> Volume:
        vol = self.get_volume(volume_id)
        if vol is None:
            raise ArrayNotFoundError(f"PowerStore volume {volume_id} not found")
        return vol

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        clusters = self._list("/cluster", "id,name,global_id")
        cluster = clusters[0] if clusters else {}
        model = version = ""
        try:
            appliances = self._list("/appliance", "id,name,model")
            model = next((a.get("model", "") for a in appliances if a.get("model")), "")
            software = self._list("/software_installed", "id,release_version,is_cluster",
                                  is_cluster="true")
            version = next((s.get("release_version", "") for s in software), "")
        except (ArrayRequestError, ArrayNotFoundError):
            pass
        return ArrayInfo(vendor=self.vendor, model=model,
                         version=f"PowerStoreOS {version}".strip(),
                         serial=cluster.get("global_id") or cluster.get("id", ""))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        body: dict[str, Any] = {"name": name, "size": size_bytes}
        if self.option("appliance_id"):
            body["appliance_id"] = self.option("appliance_id")
        resp = self._call("POST", "/volume", json=body)
        created = json_of(resp) if resp is not None else None
        if not created or "id" not in created:
            raise ArrayRequestError(f"PowerStore volume create for {name} returned no id")
        return self._require_volume(created["id"])

    def get_volume(self, volume_id: str) -> Volume | None:
        item = self._one(f"/volume/{volume_id}", VOLUME_FIELDS)
        return self._volume(item) if item else None

    def find_volume(self, name: str) -> Volume | None:
        items = self._list("/volume", VOLUME_FIELDS, name=name)
        return self._volume(items[0]) if items else None

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        self._call("PATCH", f"/volume/{volume_id}", json={"size": size_bytes})
        return self._require_volume(volume_id)

    def delete_volume(self, volume_id: str) -> None:
        self._call("DELETE", f"/volume/{volume_id}")

    def _host_by_name(self, name: str) -> dict[str, Any] | None:
        items = self._list("/host", HOST_FIELDS, name=name)
        return items[0] if items else None

    def ensure_host(self, spec: HostSpec) -> str:
        wanted: list[dict[str, str]] = []
        for i in spec.initiators:
            port_name = colon_wwpn(i.address) if i.protocol == Protocol.FC else i.normalized()
            wanted.append({"port_name": port_name, "port_type": _PORT_TYPE[i.protocol]})
        host = self._host_by_name(spec.name)
        if host is None:
            resp = self._call("POST", "/host", json={
                "name": spec.name, "os_type": self.option("os_type"), "initiators": wanted})
            created = json_of(resp) if resp is not None else None
            if not created or "id" not in created:
                raise ArrayRequestError(f"PowerStore host create for {spec.name} returned no id")
            return str(created["id"])
        have = {_port_key(p.get("port_type", ""), p.get("port_name", ""))
                for p in host.get("host_initiators") or []}
        missing = [w for w in wanted if _port_key(w["port_type"], w["port_name"]) not in have]
        if missing:
            self._call("PATCH", f"/host/{host['id']}", json={"add_initiators": missing})
        return str(host["id"])

    def delete_host(self, host_id: str) -> None:
        self._call("DELETE", f"/host/{host_id}", missing_ok=True)

    def _mappings(self, volume_id: str) -> list[Mapping]:
        items = self._list("/host_volume_mapping", MAPPING_FIELDS, volume_id=volume_id)
        out = []
        for i in items:
            host = i.get("host_id") or i.get("host_group_id")
            if host:
                lun = i.get("logical_unit_number")
                out.append(Mapping(volume_id=i["volume_id"], host_id=host,
                                   lun=int(lun) if lun is not None else None))
        return out

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        for m in self._mappings(volume_id):
            if m.host_id == host_id:
                return m
        self._call("POST", f"/volume/{volume_id}/attach", json={"host_id": host_id})
        for m in self._mappings(volume_id):
            if m.host_id == host_id:
                return m
        raise ArrayRequestError(
            f"PowerStore attached volume {volume_id} to host {host_id} but lists no mapping")

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        if not any(m.host_id == host_id for m in self._mappings(volume_id)):
            return
        self._call("POST", f"/volume/{volume_id}/detach", json={"host_id": host_id},
                   missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return self._mappings(volume_id)
