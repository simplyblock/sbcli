"""Fake Dell PowerStore REST API (the subset the adapter uses).

Quirks reproduced: basic auth only on ``login_session``; session cookie
``auth_cookie`` plus ``DELL-EMC-TOKEN`` CSRF header required on non-GET
requests (403 without it); PostgREST-style ``select``/``eq.`` filters with
400 on unknown fields and embedded selects (``host(host_initiators)``);
``limit``/``offset`` paging answered with 206 and ``Content-Range``; 201
``{"id"}`` on create, 204 on modify/delete/attach; 422 with
``{"messages": [...]}`` for semantic errors (a duplicate volume name answers
``name_conflict_status``, 422 or 500 on real arrays); 400 "Host is not
attached to volume" for a detach of an unattached volume; sizes in bytes, a
multiple of 8192; ``wwn``/``nguid`` prefixed ``naa.``/``nguid.``; a volume
gets its NVMe ``nsid`` on its first attach to an NVMe host.
"""
from __future__ import annotations

import re
import secrets
import uuid
from http.cookies import CookieError, SimpleCookie
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import Initiator, Protocol

from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

API = "/api/rest"
_ID = r"(?P<oid>[^/]+)"
_FC_RE = re.compile(r"^([0-9a-f]{2}:){7}[0-9a-f]{2}$", re.IGNORECASE)
MIN_SIZE = 1 << 20
MAX_SIZE = 256 << 40
GRANULARITY = 8192

_FIELDS = {
    "volume": {"id", "name", "description", "type", "wwn", "nguid", "nsid", "size", "state",
               "appliance_id", "creation_timestamp", "is_replication_destination",
               "performance_policy_id", "protection_policy_id"},
    "host": {"id", "name", "description", "os_type", "host_group_id", "host_initiators",
             "host_connectivity"},
    "host_volume_mapping": {"id", "host_id", "host_group_id", "volume_id",
                            "logical_unit_number"},
    "cluster": {"id", "global_id", "name", "management_address", "state",
                "appliance_count"},
    "appliance": {"id", "name", "model", "service_tag", "express_service_code"},
    "software_installed": {"id", "release_version", "build_version", "is_cluster",
                           "release_timestamp"},
}


# (kind, embedded relation) -> foreign key on the row
_EMBEDS = {("host_volume_mapping", "host"): "host_id",
           ("host_volume_mapping", "volume"): "volume_id"}


def _err(status: int, message: str, code: str = "0xE04040010005") -> FakeError:
    return FakeError(status, {"messages": [{"code": code, "severity": "Error",
                                            "message_l10n": message, "arguments": []}]})


def _not_found(kind: str, oid: str) -> FakeError:
    return _err(404, f"The {kind} {oid} was not found.", "0xE04040010005")


def _unprocessable(message: str) -> FakeError:
    return _err(422, message, "0xE0A07001000C")


class FakeDellPowerStore(FakeArray):
    vendor = "dell-powerstore"

    def __init__(self, username: str = "admin", password: str = "ps-pass"):
        super().__init__()
        self.username = username
        self.password = password
        self.sessions: dict[str, str] = {}             # auth_cookie -> CSRF token
        self.volumes: dict[str, dict[str, Any]] = {}   # id -> volume
        self.hosts: dict[str, dict[str, Any]] = {}     # id -> host
        self.mappings: list[dict[str, Any]] = []
        self.page_limit = 2000                         # array-side default/max page size
        self.first_lun = 1
        self.next_nsid = 1
        self.name_conflict_status = 422

    # -- fake controls ------------------------------------------------------

    def expire_sessions(self) -> None:
        self.sessions.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields.get("password", self.password)

    def add_volume(self, name: str, size: int) -> str:
        vid = str(uuid.uuid4())
        self.volumes[vid] = {
            "id": vid, "name": name, "description": None, "type": "Primary",
            "wwn": "naa.68ccf098" + secrets.token_hex(12),
            "nguid": "nguid." + secrets.token_hex(16), "nsid": None,
            "size": size, "state": "Ready", "appliance_id": "A1",
            "creation_timestamp": "2026-11-20T00:00:00+00:00",
            "is_replication_destination": False, "performance_policy_id": "default_medium",
            "protection_policy_id": None,
        }
        return vid

    # -- auth ---------------------------------------------------------------

    def _session(self, req: Request) -> None:
        cookie: str | None = None
        try:
            jar = SimpleCookie()
            jar.load(req.headers.get("Cookie", ""))
            if "auth_cookie" in jar:
                cookie = jar["auth_cookie"].value
        except CookieError:
            pass
        if cookie is None or cookie not in self.sessions:
            raise _err(401, "Unauthorized: the session is invalid or expired.",
                       "0xE09040010001")
        if req.method != "GET" and req.headers.get("DELL-EMC-TOKEN") != self.sessions[cookie]:
            raise _err(403, "Forbidden: missing or invalid DELL-EMC-TOKEN.", "0xE09040010003")


    # -- LIO data path (fakes.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return list(self.volumes)

    def lio_all_iqns(self) -> list[str]:
        return sorted({p["port_name"].lower() for h in self.hosts.values() for p in h["host_initiators"] if p["port_type"] == "iSCSI"})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        for h in self.hosts.values():
            if host_id in (h["id"], h["name"]):
                return [p["port_name"].lower() for p in h["host_initiators"] if p["port_type"] == "iSCSI"]
        return []

    @route("GET", API + "/login_session")
    def login_session(self, req):
        if req.basic_auth() != (self.username, self.password):
            raise _err(401, "Unauthorized: invalid credentials.", "0xE09040010001")
        cookie, token = secrets.token_hex(16), secrets.token_hex(16)
        self.sessions[cookie] = token
        return Reply(200, [{"id": secrets.token_hex(8), "user": self.username,
                            "role_ids": ["1"], "idle_timeout": 3600,
                            "is_password_change_required": False,
                            "is_built_in_user": True}],
                     {"DELL-EMC-TOKEN": token,
                      "Set-Cookie": f"auth_cookie={cookie}; Path=/; Secure; HttpOnly"})

    @route("POST", API + "/logout")
    def logout(self, req):
        self._session(req)
        jar = SimpleCookie()
        jar.load(req.headers.get("Cookie", ""))
        self.sessions.pop(jar["auth_cookie"].value, None)
        return Reply(204)

    # -- query engine ------------------------------------------------------------

    def _query(self, req: Request, kind: str, rows: list[dict[str, Any]]) -> Reply:
        fields = _FIELDS[kind]
        for key, values in req.query.items():
            if key in ("select", "limit", "offset", "order"):
                continue
            if key not in fields:
                raise _err(400, f"Invalid filter field: {key}.", "0xE04040020009")
            m = re.match(r"^eq\.(.*)$", values[0], re.DOTALL)
            if not m:
                raise _err(400, f"Unsupported filter operator in {key}={values[0]}.",
                           "0xE04040020009")
            want = m.group(1)
            rows = [r for r in rows if _as_filter(r.get(key)) == want]
        select = req.q("select")
        names = self._select(kind, select) if select else ["id"]
        offset = int(req.q("offset", "0") or 0)
        limit = min(int(req.q("limit", str(self.page_limit)) or self.page_limit),
                    self.page_limit)
        total = len(rows)
        page = rows[offset:offset + limit]
        body = [self._row(kind, r, names) for r in page]
        if offset + len(page) < total:
            end = offset + len(page) - 1
            return Reply(206, body, {"Content-Range": f"{offset}-{end}/{total}"})
        return Reply(200, body)

    def _project(self, req: Request, kind: str, row: dict[str, Any]) -> Reply:
        select = req.q("select")
        names = self._select(kind, select) if select else sorted(_FIELDS[kind])
        return Reply(200, self._row(kind, row, names))

    @staticmethod
    def _select(kind: str, select: str) -> list[str]:
        """Top-level select items; ``rel(a,b)`` embeds a related object."""
        names, depth, cur = [], 0, ""
        for ch in select:
            if ch == "," and depth == 0:
                names.append(cur)
                cur = ""
                continue
            depth += (ch == "(") - (ch == ")")
            cur += ch
        names.append(cur)
        bad = []
        for n in names:
            m = re.match(r"^(\w+)\(([\w,]+)\)$", n)
            if m:
                rel, sub = m.group(1), m.group(2).split(",")
                if (kind, rel) not in _EMBEDS or any(f not in _FIELDS[rel] for f in sub):
                    bad.append(n)
            elif n not in _FIELDS[kind]:
                bad.append(n)
        if bad:
            raise _err(400, f"Invalid select field: {','.join(bad)}.", "0xE04040020009")
        return names

    def _row(self, kind: str, row: dict[str, Any], names: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for n in names:
            m = re.match(r"^(\w+)\(([\w,]+)\)$", n)
            if not m:
                out[n] = row.get(n)
                continue
            rel, sub = m.group(1), m.group(2).split(",")
            store = self.hosts if rel == "host" else self.volumes
            target = store.get(row.get(_EMBEDS[(kind, rel)]) or "")
            out[rel] = {f: target.get(f) for f in sub} if target else None
        return out

    # -- system ------------------------------------------------------------------

    @route("GET", API + "/cluster")
    def get_cluster(self, req):
        self._session(req)
        return self._query(req, "cluster", [{
            "id": "0", "global_id": "PS4a7b5c3e9f01", "name": "fake-powerstore",
            "management_address": "10.0.0.10", "state": "Configured", "appliance_count": 1}])

    @route("GET", API + "/appliance")
    def get_appliance(self, req):
        self._session(req)
        return self._query(req, "appliance", [{
            "id": "A1", "name": "fake-powerstore-appliance-1", "model": "PowerStore 1200T",
            "service_tag": "FAKE123", "express_service_code": "0"}])

    @route("GET", API + "/software_installed")
    def get_software(self, req):
        self._session(req)
        return self._query(req, "software_installed", [
            {"id": "c1", "release_version": "4.0.0.0", "build_version": "4.0.0.0.5.123",
             "is_cluster": True, "release_timestamp": "2026-01-01T00:00:00Z"},
            {"id": "a1", "release_version": "4.0.0.0", "build_version": "4.0.0.0.5.123",
             "is_cluster": False, "release_timestamp": "2026-01-01T00:00:00Z"},
        ])

    # -- volumes -----------------------------------------------------------------

    def _vol(self, oid: str) -> dict[str, Any]:
        if oid not in self.volumes:
            raise _not_found("volume", oid)
        return self.volumes[oid]

    @staticmethod
    def _check_size(size: Any) -> int:
        if not isinstance(size, int) or isinstance(size, bool):
            raise _err(400, "Invalid value for size.", "0xE04040020009")
        if size < MIN_SIZE or size > MAX_SIZE:
            raise _err(400, f"Size {size} is out of range ({MIN_SIZE}..{MAX_SIZE}).",
                       "0xE04040020009")
        if size % GRANULARITY:
            raise _err(400, f"Size {size} must be a multiple of {GRANULARITY}.",
                       "0xE04040020009")
        return size

    @route("GET", API + "/volume")
    def list_volumes(self, req):
        self._session(req)
        return self._query(req, "volume", list(self.volumes.values()))

    @route("GET", API + "/volume/" + _ID)
    def get_volume(self, req, oid):
        self._session(req)
        return self._project(req, "volume", self._vol(oid))

    @route("POST", API + "/volume")
    def post_volume(self, req):
        self._session(req)
        body = req.json() or {}
        name = body.get("name")
        if not name or len(name) > 128:
            raise _err(400, "Invalid value for name.", "0xE04040020009")
        size = self._check_size(body.get("size"))
        if any(v["name"] == name for v in self.volumes.values()):
            raise _err(self.name_conflict_status,
                       f"The name {name} is already in use by another volume.",
                       "0xE0A07001000C")
        vid = self.add_volume(name, size)
        if body.get("appliance_id"):
            self.volumes[vid]["appliance_id"] = body["appliance_id"]
        return Reply(201, {"id": vid})

    @route("PATCH", API + "/volume/" + _ID)
    def patch_volume(self, req, oid):
        self._session(req)
        vol = self._vol(oid)
        body = req.json() or {}
        if "size" in body:
            size = self._check_size(body["size"])
            if size < vol["size"]:
                raise _unprocessable("The new size must be greater than the current size "
                                     "of the volume.")
            vol["size"] = size
        return Reply(204)

    @route("DELETE", API + "/volume/" + _ID)
    def delete_volume(self, req, oid):
        self._session(req)
        self._vol(oid)
        if any(m["volume_id"] == oid for m in self.mappings):
            raise _unprocessable(f"Volume {oid} cannot be deleted because it is attached "
                                 "to one or more hosts.")
        del self.volumes[oid]
        return Reply(204)

    @route("POST", API + "/volume/" + _ID + "/attach")
    def attach(self, req, oid):
        self._session(req)
        self._vol(oid)
        body = req.json() or {}
        host_id = body.get("host_id")
        if not host_id or host_id not in self.hosts:
            raise _not_found("host", str(host_id))
        if any(m["volume_id"] == oid and m["host_id"] == host_id for m in self.mappings):
            raise _unprocessable(f"Volume {oid} is already attached to host {host_id}.")
        used = {m["logical_unit_number"] for m in self.mappings if m["host_id"] == host_id}
        lun = body.get("logical_unit_number")
        if lun is None:
            lun = next(n for n in range(self.first_lun, 16384) if n not in used)
        elif lun in used:
            raise _unprocessable(f"Logical unit number {lun} is already in use on the host.")
        vol = self.volumes[oid]
        nvme = all(p["port_type"] == "NVMe" for p in self.hosts[host_id]["host_initiators"])
        if nvme and self.hosts[host_id]["host_initiators"] and vol["nsid"] is None:
            vol["nsid"] = self.next_nsid
            self.next_nsid += 1
        self.mappings.append({"id": str(uuid.uuid4()), "host_id": host_id,
                              "host_group_id": None, "volume_id": oid,
                              "logical_unit_number": lun})
        return Reply(204)

    @route("POST", API + "/volume/" + _ID + "/detach")
    def detach(self, req, oid):
        self._session(req)
        self._vol(oid)
        host_id = (req.json() or {}).get("host_id")
        before = len(self.mappings)
        self.mappings = [m for m in self.mappings
                         if not (m["volume_id"] == oid and m["host_id"] == host_id)]
        if len(self.mappings) == before:
            raise _err(400, "Host is not attached to volume.", "0xE0A08001000F")
        return Reply(204)

    @route("GET", API + "/host_volume_mapping")
    def list_mappings(self, req):
        self._session(req)
        return self._query(req, "host_volume_mapping", list(self.mappings))

    # -- hosts ----------------------------------------------------------------------

    def _host(self, oid: str) -> dict[str, Any]:
        if oid not in self.hosts:
            raise _not_found("host", oid)
        return self.hosts[oid]

    def _claim(self, host_id: str, initiators: Any) -> list[dict[str, Any]]:
        if not isinstance(initiators, list):
            raise _err(400, "Invalid value for initiators.", "0xE04040020009")
        out = []
        for i in initiators:
            port_name, port_type = i.get("port_name"), i.get("port_type")
            if port_type not in ("iSCSI", "FC", "NVMe"):
                raise _err(400, f"Invalid port_type {port_type}.", "0xE04040020009")
            if not port_name:
                raise _err(400, "Invalid value for port_name.", "0xE04040020009")
            if port_type == "FC" and not _FC_RE.match(port_name):
                raise _err(400, f"Invalid FC port_name {port_name}.", "0xE04040020009")
            for other in self.hosts.values():
                for p in other["host_initiators"]:
                    if p["port_name"].lower() == port_name.lower():
                        if other["id"] == host_id:
                            raise _unprocessable(
                                f"The initiator {port_name} is already added to this host.")
                        raise _unprocessable(
                            f"The initiator {port_name} is already registered to host "
                            f"{other['name']}.")
            out.append({"port_name": port_name, "port_type": port_type,
                        "active_sessions": [], "chap_single_username": None,
                        "chap_mutual_username": None})
        return out

    @route("GET", API + "/host")
    def list_hosts(self, req):
        self._session(req)
        return self._query(req, "host", list(self.hosts.values()))

    @route("GET", API + "/host/" + _ID)
    def get_host(self, req, oid):
        self._session(req)
        return self._project(req, "host", self._host(oid))

    @route("POST", API + "/host")
    def post_host(self, req):
        self._session(req)
        body = req.json() or {}
        name = body.get("name")
        if not name or len(name) > 128:
            raise _err(400, "Invalid value for name.", "0xE04040020009")
        if body.get("os_type") not in ("Windows", "Linux", "ESXi", "AIX", "HP-UX", "Solaris"):
            raise _err(400, f"Invalid os_type {body.get('os_type')}.", "0xE04040020009")
        if any(h["name"] == name for h in self.hosts.values()):
            raise _unprocessable(f"The name {name} is already in use by another host.")
        hid = str(uuid.uuid4())
        initiators = self._claim(hid, body.get("initiators") or [])
        self.hosts[hid] = {"id": hid, "name": name, "description": None,
                           "os_type": body["os_type"], "host_group_id": None,
                           "host_initiators": initiators,
                           "host_connectivity": "Local_Only"}
        return Reply(201, {"id": hid})

    @route("PATCH", API + "/host/" + _ID)
    def patch_host(self, req, oid):
        self._session(req)
        host = self._host(oid)
        body = req.json() or {}
        if "initiators" in body:
            raise _err(400, "Use add_initiators/remove_initiators to modify initiators.",
                       "0xE04040020009")
        added = self._claim(oid, body.get("add_initiators") or [])
        host["host_initiators"].extend(added)
        return Reply(204)

    @route("DELETE", API + "/host/" + _ID)
    def delete_host(self, req, oid):
        self._session(req)
        self._host(oid)
        if any(m["host_id"] == oid for m in self.mappings):
            raise _unprocessable(f"Host {oid} cannot be deleted because it has volumes "
                                 "attached.")
        del self.hosts[oid]
        return Reply(204)


def _as_filter(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return "" if value is None else str(value)


def contract_setup() -> ContractSetup:
    fake = FakeDellPowerStore(username="admin", password="ps-pass")
    return ContractSetup(
        fake=fake,
        config=make_config("dell-powerstore", "https://powerstore.fake.example"),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("ps-pass")}),
        bad_credential=Credential({"username": SecretStr("admin"),
                                   "password": SecretStr("wrong")}),
        initiators={
            Protocol.ISCSI: (
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-a"),),
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-b"),),
            ),
            Protocol.FC: (
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b"),),
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:FF:4C:1A:2C"),),
            ),
            Protocol.NVME_TCP: (
                (Initiator(protocol=Protocol.NVME_TCP,
                           address="nqn.2014-08.org.nvmexpress:uuid:host-a"),),
                (Initiator(protocol=Protocol.NVME_TCP,
                           address="nqn.2014-08.org.nvmexpress:uuid:host-b"),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_volume(name, size),
    )
