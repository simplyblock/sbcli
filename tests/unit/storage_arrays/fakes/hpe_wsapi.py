"""Fake HPE WSAPI (Alletra MP / Primera / 3PAR), the subset the adapter uses.

Quirks reproduced: session keys created with ``POST /api/v1/credentials`` and
checked on ``X-HP3PAR-WSAPI-SessionKey``; ``{"code", "desc"}`` error bodies;
volumes addressed by name with sizes in MiB rounded up to 256 MiB; GROW
taking an increment; host requests using ``iSCSINames``/``FCWWNs`` while
responses report ``iSCSIPaths``/``FCPaths``; VLUN creation answering 201
with an empty body and a ``Location`` header; and VLUN listings that contain
the template plus one active VLUN per host path.

Error codes and statuses follow python-3parclient's own WSAPI mock
(``test/HPE3ParMockServer_flask.py``): 5 INV_USER_PASS, 12 INV_INPUT, 15
NON_EXISTENT_CPG, 16 EXISTENT_HOST, 17 NON_EXISTENT_HOST, 19
NON_EXISTENT_VLUN, 22 EXISTENT_VOL, 23 NON_EXISTENT_VOL, 26 EXPORTED_VLUN,
39 INV_INPUT_WRONG_TYPE, 40 INV_INPUT_MISSING_REQUIRED, 43
INV_INPUT_EXCEEDS_RANGE (create below 256 MiB), 44 INV_INPUT_PARAM_CONFLICT,
57 INV_INPUT_EXCEEDS_LENGTH, 73 EXISTENT_PATH, 152 INV_INPUT_VV_GROW_SIZE,
153 VV_NEW_SIZE_EXCEED_CPG_LIMIT (a *403* that is not an auth failure), plus
34 "resource in use" (409) from HPE flowkit. The session-expiry answer (401
code 6) is a guess.
"""
from __future__ import annotations

import re
import secrets
from typing import Any
from urllib.parse import unquote

from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import Initiator, Protocol

from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_A = "/api/v1"
_UNIT_MIB = 256
_MIN_MIB = 256
_MAX_NAME = 31
_QUERY_RE = re.compile(r'^"?\s*volumeName\s+EQ\s+(?P<vol>\S+?)\s*"?$')


def _err(status: int, code: int, desc: str) -> FakeError:
    return FakeError(status, {"code": code, "desc": desc})


class FakeHpeWsapi(FakeArray):
    vendor = "hpe-wsapi"

    def __init__(self, username: str = "3paradm", password: str = "3pardata",
                 cpgs: tuple[str, ...] = ("SSD_r6",), paths_per_host: int = 2,
                 cpg_limit_mib: int | None = None):
        super().__init__()
        self.username = username
        self.password = password
        self.cpgs = set(cpgs)
        #: active VLUN entries listed per mapping (one per host port path)
        self.paths_per_host = paths_per_host
        #: growth limit per volume; exceeding it answers 403 code 153
        self.cpg_limit_mib = cpg_limit_mib
        self.sessions: set[str] = set()
        self.volumes: dict[str, dict[str, Any]] = {}   # name -> volume
        self.hosts: dict[str, dict[str, Any]] = {}     # name -> host
        self.vluns: list[dict[str, Any]] = []          # templates {volumeName, hostname, lun}
        self.created_bodies: list[dict[str, Any]] = []

    # -- fake controls ------------------------------------------------------

    def expire_sessions(self) -> None:
        self.sessions.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields["password"]

    def add_volume(self, name: str, size: int) -> str:
        mib = -(-size // (1 << 20))
        mib = -(-mib // _UNIT_MIB) * _UNIT_MIB
        self.volumes[name] = {
            "id": self.next_id(), "name": name, "sizeMiB": mib,
            "wwn": "60002AC" + secrets.token_hex(13).upper()[:25],
            "provisioningType": 2, "userCPG": next(iter(self.cpgs)), "state": 1,
            "uuid": secrets.token_hex(16),
        }
        return name

    # -- auth ---------------------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.headers.get("X-HP3PAR-WSAPI-SessionKey") not in self.sessions:
            raise _err(401, 6, "invalid session key")


    # -- LIO data path (fakes.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return list(self.volumes)

    def lio_all_iqns(self) -> list[str]:
        return sorted({p["name"].lower() for h in self.hosts.values() for p in h.get("iSCSIPaths", [])})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        host = self.hosts.get(host_id) or {}
        return [p["name"].lower() for p in host.get("iSCSIPaths", [])]

    @route("POST", _A + "/credentials")
    def login(self, req):
        body = req.json() or {}
        if body.get("user") != self.username or body.get("password") != self.password:
            raise _err(403, 5, "invalid username or password")
        key = "0-" + secrets.token_hex(16) + "-" + secrets.token_hex(4)
        self.sessions.add(key)
        return Reply(201, {"key": key})

    @route("DELETE", _A + "/credentials/(?P<key>[^/]+)")
    def logout(self, req, key):
        key = unquote(key)
        if key not in self.sessions:
            raise _err(404, 6, "invalid session key")
        self.sessions.discard(key)
        return Reply(200)

    @route("GET", _A + "/system")
    def system(self, req):
        self._auth(req)
        return Reply(200, {"id": 412345, "name": "fake-b10000", "systemVersion": "10.4.0.123",
                           "model": "HPE Alletra Storage MP B10000",
                           "serialNumber": "CZ12345678", "IPv4Addr": "192.0.2.10"})

    # -- volumes -------------------------------------------------------------

    def _vol(self, name: str) -> dict[str, Any]:
        name = unquote(name)
        if name not in self.volumes:
            raise _err(404, 23, "volume does not exist")
        return self.volumes[name]

    @route("POST", _A + "/volumes")
    def post_volumes(self, req):
        self._auth(req)
        body = req.json() or {}
        self.created_bodies.append(body)
        name, cpg, size = body.get("name"), body.get("cpg"), body.get("sizeMiB")
        if not name or not cpg or not isinstance(size, int):
            raise _err(400, 40, "missing required field")
        if len(name) > _MAX_NAME:
            raise _err(400, 57, "invalid input: string length exceeds limit")
        if cpg not in self.cpgs:
            raise _err(404, 15, "CPG does not exist")
        if name in self.volumes:
            raise _err(409, 22, "volume exists")
        if size < _MIN_MIB:
            raise _err(400, 43, "Minimum volume size is 256 MiB")
        self.add_volume(name, size << 20)
        return Reply(201, None, {"Location": f"{_A}/volumes/{name}"})

    @route("GET", _A + "/volumes/(?P<name>[^/]+)")
    def get_volume(self, req, name):
        self._auth(req)
        return Reply(200, self._vol(name))

    @route("PUT", _A + "/volumes/(?P<name>[^/]+)")
    def put_volume(self, req, name):
        self._auth(req)
        vol = self._vol(name)
        body = req.json() or {}
        if body.get("action") == 3:
            delta = body.get("sizeMiB")
            if not isinstance(delta, int) or delta <= 0:
                raise _err(400, 152, "invalid grow size")
            new = -(-(vol["sizeMiB"] + delta) // _UNIT_MIB) * _UNIT_MIB
            if self.cpg_limit_mib is not None and new > self.cpg_limit_mib:
                raise _err(403, 153, "new volume size exceeds CPG limit")
            vol["sizeMiB"] = new
            return Reply(200)
        raise _err(400, 12, "invalid input: unsupported action")

    @route("DELETE", _A + "/volumes/(?P<name>[^/]+)")
    def delete_volume(self, req, name):
        self._auth(req)
        vol = self._vol(name)
        if any(v["volumeName"] == vol["name"] for v in self.vluns):
            raise _err(409, 34, "volume is exported (has VLUNs)")
        del self.volumes[vol["name"]]
        return Reply(200)

    # -- hosts ----------------------------------------------------------------

    def _host(self, name: str) -> dict[str, Any]:
        name = unquote(name)
        if name not in self.hosts:
            raise _err(404, 17, "host does not exist")
        return self.hosts[name]

    def _claim(self, host: str, body: dict[str, Any]) -> None:
        iqns = [str(i) for i in body.get("iSCSINames") or []]
        wwns = [str(w).upper().replace(":", "") for w in body.get("FCWWNs") or []]
        for w in wwns:
            if not re.fullmatch(r"[0-9A-F]{16}", w):
                raise _err(400, 39, "invalid WWN")
        for other in self.hosts.values():
            if other["name"] == host:
                continue
            taken = ({p["name"].lower() for p in other["iSCSIPaths"]} & {i.lower() for i in iqns}
                     or {p["wwn"] for p in other["FCPaths"]} & set(wwns))
            if taken:
                raise _err(409, 73, f"path {min(taken)} is already used by "
                           f"host {other['name']}")
        h = self.hosts[host]
        for i in iqns:
            if i.lower() not in {p["name"].lower() for p in h["iSCSIPaths"]}:
                h["iSCSIPaths"].append({"name": i, "IPAddr": "0.0.0.0", "hostSpeed": 0})
        for w in wwns:
            if w not in {p["wwn"] for p in h["FCPaths"]}:
                h["FCPaths"].append({"wwn": w, "hostSpeed": 0})

    @route("POST", _A + "/hosts")
    def post_hosts(self, req):
        self._auth(req)
        body = req.json() or {}
        name = body.get("name")
        if not name:
            raise _err(400, 40, "missing required field")
        if len(name) > _MAX_NAME:
            raise _err(400, 57, "invalid input: string length exceeds limit")
        if name in self.hosts:
            raise _err(409, 16, "host exists")
        if body.get("iSCSINames") and body.get("FCWWNs"):
            raise _err(400, 44, "invalid input: iSCSINames and FCWWNs are exclusive")
        self.hosts[name] = {"id": self.next_id(), "name": name,
                            "persona": body.get("persona", 2),
                            "iSCSIPaths": [], "FCPaths": [], "descriptors": {}}
        try:
            self._claim(name, body)
        except FakeError:
            del self.hosts[name]
            raise
        return Reply(201, None, {"Location": f"{_A}/hosts/{name}"})

    @route("GET", _A + "/hosts/(?P<name>[^/]+)")
    def get_host(self, req, name):
        self._auth(req)
        return Reply(200, self._host(name))

    @route("PUT", _A + "/hosts/(?P<name>[^/]+)")
    def put_host(self, req, name):
        self._auth(req)
        host = self._host(name)
        body = req.json() or {}
        if body.get("pathOperation") != 1:
            raise _err(400, 46, "invalid input: unsupported pathOperation")
        if body.get("iSCSINames") and body.get("FCWWNs"):
            raise _err(400, 44, "invalid input: iSCSINames and FCWWNs are exclusive")
        self._claim(host["name"], body)
        return Reply(200)

    @route("DELETE", _A + "/hosts/(?P<name>[^/]+)")
    def delete_host(self, req, name):
        self._auth(req)
        host = self._host(name)
        if any(v["hostname"] == host["name"] for v in self.vluns):
            raise _err(409, 26, "host has exported VLUNs")
        del self.hosts[host["name"]]
        return Reply(200)

    # -- VLUNs ------------------------------------------------------------------

    def _vlun_members(self, vol: str | None) -> list[dict[str, Any]]:
        out = []
        for t in self.vluns:
            if vol is not None and t["volumeName"] != vol:
                continue
            v = self.volumes[t["volumeName"]]
            base = {"lun": t["lun"], "volumeName": t["volumeName"], "hostname": t["hostname"],
                    "volumeWWN": v["wwn"], "type": 4}
            out.append({**base, "active": False})
            for port in range(self.paths_per_host):
                out.append({**base, "active": True, "remoteName": f"path{port}",
                            "portPos": {"node": port, "slot": 2, "cardPort": 1}})
        return out

    @route("GET", _A + "/vluns")
    def get_vluns(self, req):
        self._auth(req)
        query = req.q("query")
        vol = None
        if query is not None:
            m = _QUERY_RE.match(query)
            if not m:
                raise _err(400, 92, "invalid query")
            vol = m["vol"]
        members = self._vlun_members(vol)
        return Reply(200, {"total": len(members), "members": members})

    @route("POST", _A + "/vluns")
    def post_vluns(self, req):
        self._auth(req)
        body = req.json() or {}
        vol, host = body.get("volumeName"), body.get("hostname")
        if vol not in self.volumes:
            raise _err(404, 23, "volume does not exist")
        if host not in self.hosts:
            raise _err(404, 17, "host does not exist")
        if any(t["volumeName"] == vol and t["hostname"] == host for t in self.vluns):
            # The real code for a duplicate VLUN is not in vendor source.
            raise _err(409, 0, "VLUN exists")
        used = {t["lun"] for t in self.vluns if t["hostname"] == host}
        start = int(body.get("lun", 0))
        if body.get("autoLun"):
            lun = next(n for n in range(start, 16384) if n not in used)
        elif start in used:
            raise _err(409, 0, "LUN already in use")
        else:
            lun = start
        self.vluns.append({"volumeName": vol, "hostname": host, "lun": lun})
        return Reply(201, None, {"Location": f"{_A}/vluns/{vol},{lun},{host}"})

    @route("DELETE", _A + "/vluns/(?P<spec>[^/]+)")
    def delete_vlun(self, req, spec):
        self._auth(req)
        parts = unquote(spec).split(",")
        if len(parts) != 3 or not parts[1].isdigit():
            raise _err(400, 12, "invalid VLUN id")
        vol, lun, host = parts[0], int(parts[1]), parts[2]
        for t in self.vluns:
            if t["volumeName"] == vol and t["lun"] == lun and t["hostname"] == host:
                self.vluns.remove(t)
                return Reply(200)
        raise _err(404, 19, "VLUN does not exist")


def contract_setup() -> ContractSetup:
    fake = FakeHpeWsapi(username="3paradm", password="wsapi-pw")
    return ContractSetup(
        fake=fake,
        config=make_config("hpe-wsapi", "https://wsapi.fake.example:443", cpg="SSD_r6"),
        credential=Credential({"username": SecretStr("3paradm"),
                               "password": SecretStr("wsapi-pw")}),
        bad_credential=Credential({"username": SecretStr("3paradm"),
                                   "password": SecretStr("wrong")}),
        initiators={
            Protocol.ISCSI: (
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-a"),),
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-b"),),
            ),
            Protocol.FC: (
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b"),),
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c"),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_volume(name, size),
    )
