"""Fake Dell PowerFlex REST gateway (``/api/login`` token auth on 3.6 and
4.x, 4.x ``/rest/auth/login`` OAuth), the subset the adapter uses.

Quirks reproduced: the ``/api/login`` token is a JSON *string*, sent as
basic auth ``user:<token>`` on 3.x and as ``Bearer <token>`` on 4.x
(goscaleio); ``/api/version`` answers a JSON string (``"3.6"`` / ``"4.5"``)
and accepts the basic-auth token on both; 3.x answers 404 for
``/rest/auth/login``; "not found" is
HTTP 500 with ``{"message", "httpStatusCode", "errorCode"}``; sizes round up
to 8 GiB; ``mappedSdcInfo`` is ``null`` when unmapped; a second SDC mapping
needs ``allowMultipleMappings``; SDCs exist before (and independently of)
any host registration. Error codes are placeholders, not PowerFlex's.
"""
from __future__ import annotations

import json
import secrets
import uuid
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

GIB = 1 << 30
_KB_PER_8GIB = 8 * 1024 * 1024
_VOL = r"/api/instances/Volume::(?P<vid>[0-9a-zA-Z]+)"


def _err(msg: str, status: int = 500, code: int = 0) -> FakeError:
    return FakeError(status, {"message": msg, "httpStatusCode": status, "errorCode": code})


def _bool(v: Any) -> bool:
    return str(v).upper() == "TRUE"


class FakePowerFlex(FakeArray):
    vendor = "dell-powerflex"

    def __init__(self, username: str = "admin", password: str = "pf-pass",
                 generation: int = 4):
        super().__init__()
        if generation not in (3, 4):
            raise ValueError("generation must be 3 or 4")
        self.generation = generation
        self.username = username
        self.password = password
        self.tokens: set[str] = set()          # /api/login session tokens
        self.access_tokens: set[str] = set()   # 4.x /rest/auth/login tokens
        self.system_id = "4a54e5c8a1b2c3d4"
        self.pools = {"7d2e1c5b00000001": {"id": "7d2e1c5b00000001", "name": "sb-pool",
                                           "protectionDomainId": "e1f2a3b400000000"}}
        self.volumes: dict[str, dict[str, Any]] = {}
        self.sdcs: dict[str, dict[str, Any]] = {}

    # -- fake controls ------------------------------------------------------

    @property
    def pool_id(self) -> str:
        return next(iter(self.pools))

    def _new_id(self) -> str:
        return f"{secrets.token_hex(4)}{self.next_id():08x}"

    def expire_sessions(self) -> None:
        self.tokens.clear()
        self.access_tokens.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields["password"]

    def add_sdc(self, guid: str, name: str | None = None) -> str:
        sid = self._new_id()
        self.sdcs[sid] = {"id": sid, "sdcGuid": guid, "name": name,
                          "sdcIp": f"10.0.0.{len(self.sdcs) + 10}", "sdcApproved": True,
                          "mdmConnectionState": "Connected",
                          "links": [{"rel": "self", "href": f"/api/instances/Sdc::{sid}"}]}
        return sid

    def add_volume(self, name: str | None, size_bytes: int, pool_id: str | None = None) -> str:
        vid = self._new_id()
        kb = -(-max(size_bytes // 1024, 1) // _KB_PER_8GIB) * _KB_PER_8GIB
        self.volumes[vid] = {
            "id": vid, "name": name, "sizeInKb": kb,
            "storagePoolId": pool_id or self.pool_id, "volumeType": "ThinProvisioned",
            "vtreeId": self._new_id(), "ancestorVolumeId": None, "mappedSdcInfo": None,
            "creationTime": 1763600000, "isObfuscated": False,
            "links": [{"rel": "self", "href": f"/api/instances/Volume::{vid}"}],
        }
        return vid

    # -- auth ---------------------------------------------------------------

    def _auth(self, req: Request, basic_ok: bool = False) -> None:
        if self.generation == 3 or basic_ok:
            creds = req.basic_auth()
            if creds and creds[1] in self.tokens:
                return
        if self.generation >= 4:
            header = req.headers.get("Authorization", "")
            if header.startswith("Bearer ") and (header[7:] in self.access_tokens
                                                 or header[7:] in self.tokens):
                return
        raise _err("Unauthorized", 401)


    # -- LIO data path (testing.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return []   # PowerFlex presents through its SDC, not iSCSI

    def lio_all_iqns(self) -> list[str]:
        return sorted(set())

    def lio_host_iqns(self, host_id: str) -> list[str]:
        return []

    @route("GET", "/api/version")
    def api_version(self, req):
        self._auth(req, basic_ok=True)
        version = "3.6" if self.generation == 3 else "4.5"
        return Reply(200, json.dumps(version).encode(), {"Content-Type": "application/json"})

    @route("GET", "/api/login")
    def api_login(self, req):
        if req.basic_auth() != (self.username, self.password):
            raise _err("Unauthorized", 401)
        token = secrets.token_urlsafe(24)
        self.tokens.add(token)
        # The gateway returns the token as a JSON string literal.
        return Reply(200, json.dumps(token).encode(), {"Content-Type": "application/json"})

    @route("GET", "/api/logout")
    def api_logout(self, req):
        creds = req.basic_auth()
        if creds:
            self.tokens.discard(creds[1])
        header = req.headers.get("Authorization", "")
        if header.startswith("Bearer "):
            self.tokens.discard(header[7:])
        return Reply(200, None)

    @route("POST", "/rest/auth/login")
    def rest_login(self, req):
        if self.generation != 4:
            return Reply(404, b"<html><body>HTTP Status 404 - Not Found</body></html>",
                         {"Content-Type": "text/html"})
        body = req.json() or {}
        if (body.get("username"), body.get("password")) != (self.username, self.password):
            raise FakeError(401, {"error": "invalid_grant",
                                  "error_description": "Invalid user credentials"})
        token = secrets.token_urlsafe(32)
        self.access_tokens.add(token)
        return Reply(200, {"access_token": token, "expires_in": 300,
                           "refresh_token": secrets.token_urlsafe(32),
                           "refresh_expires_in": 1800, "token_type": "Bearer",
                           "scope": "openid profile email"})

    @route("POST", "/rest/auth/logout")
    def rest_logout(self, req):
        header = req.headers.get("Authorization", "")
        self.access_tokens.discard(header[7:])
        return Reply(204, None)

    # -- system -----------------------------------------------------------------

    @route("GET", "/api/types/System/instances")
    def systems(self, req):
        self._auth(req)
        version = "R3_6.0.0" if self.generation == 3 else "R4_5.0.0"
        return Reply(200, [{"id": self.system_id, "name": "fake-pflex",
                            "systemVersionName": f"DellEMC PowerFlex Version: {version}",
                            "mdmClusterState": "ClusteredNormal"}])

    # -- volumes -------------------------------------------------------------

    def _vol(self, vid: str) -> dict[str, Any]:
        if vid not in self.volumes:
            raise _err("Could not find the volume", 500, 79)
        return self.volumes[vid]

    @route("POST", "/api/types/Volume/instances")
    def create_volume(self, req):
        self._auth(req)
        body = req.json() or {}
        name = body.get("name")
        if name and len(name) > 31:
            raise _err("Name is too long", 500, 7)
        if name and any(v["name"] == name for v in self.volumes.values()):
            raise _err("Volume name already in use", 500, 6)
        pool = body.get("storagePoolId")
        if pool not in self.pools:
            raise _err("Could not find the Storage Pool", 500, 61)
        if body.get("volumeType") not in ("ThinProvisioned", "ThickProvisioned"):
            raise _err("Invalid volume type", 400, 1)
        try:
            kb = int(body["volumeSizeInKb"])
        except (KeyError, TypeError, ValueError):
            raise _err("Invalid volume size", 400, 1) from None
        vid = self.add_volume(name, kb * 1024, pool)
        return Reply(200, {"id": vid})

    @route("GET", _VOL)
    def get_volume(self, req, vid):
        self._auth(req)
        return Reply(200, self._vol(vid))

    @route("POST", "/api/types/Volume/instances/action/queryIdByKey")
    def query_id(self, req):
        self._auth(req)
        name = (req.json() or {}).get("name")
        for v in self.volumes.values():
            if v["name"] == name:
                return Reply(200, v["id"])
        raise _err("Could not find the volume", 500, 79)

    @route("POST", _VOL + "/action/(?P<action>[A-Za-z]+)")
    def volume_action(self, req, vid, action):
        self._auth(req)
        v = self._vol(vid)
        body = req.json() or {}
        mapped = v["mappedSdcInfo"] or []
        if action == "setVolumeSize":
            gb = int(body["sizeInGB"])
            kb = -(-gb // 8) * 8 * 1024 * 1024
            if kb < v["sizeInKb"]:
                raise _err("Volume size cannot be decreased", 500, 76)
            v["sizeInKb"] = kb
            return Reply(200, {})
        if action == "removeVolume":
            if body.get("removeMode") not in ("ONLY_ME", "INCLUDING_DESCENDANTS",
                                              "DESCENDANTS_ONLY", "WHOLE_VTREE"):
                raise _err("Invalid remove mode", 400, 1)
            if mapped:
                raise _err("The volume is mapped to an SDC; unmap it first", 500, 80)
            del self.volumes[vid]
            return Reply(200, {})
        if action == "addMappedSdc":
            sid = body.get("sdcId")
            if sid not in self.sdcs:
                raise _err("Could not find the SDC", 500, 100)
            if any(m["sdcId"] == sid for m in mapped):
                raise _err("The volume is already mapped to this SDC", 500, 81)
            if mapped and not _bool(body.get("allowMultipleMappings")):
                raise _err("The volume is already mapped to another SDC; "
                           "set allowMultipleMappings", 500, 82)
            sdc = self.sdcs[sid]
            v["mappedSdcInfo"] = mapped + [{
                "sdcId": sid, "sdcIp": sdc["sdcIp"], "sdcName": sdc["name"],
                "limitIops": 0, "limitBwInMbps": 0,
                "accessMode": body.get("accessMode", "ReadWrite")}]
            return Reply(200, {})
        if action == "removeMappedSdc":
            sid = body.get("sdcId")
            rest = [m for m in mapped if m["sdcId"] != sid]
            if len(rest) == len(mapped):
                raise _err("The volume is not mapped to SDC", 500, 83)
            v["mappedSdcInfo"] = rest or None
            return Reply(200, {})
        raise _err(f"Unknown action {action}", 400, 1)

    # -- SDCs -----------------------------------------------------------------

    @route("GET", "/api/types/Sdc/instances")
    def list_sdcs(self, req):
        self._auth(req)
        return Reply(200, list(self.sdcs.values()))

    @route("POST", r"/api/instances/Sdc::(?P<sid>[0-9a-zA-Z]+)/action/setSdcName")
    def set_sdc_name(self, req, sid):
        self._auth(req)
        if sid not in self.sdcs:
            raise _err("Could not find the SDC", 500, 100)
        name = (req.json() or {}).get("sdcName")
        if any(s["name"] == name and s["id"] != sid for s in self.sdcs.values()):
            raise _err("SDC name already in use", 500, 6)
        self.sdcs[sid]["name"] = name
        return Reply(200, {})


def _guid() -> str:
    return str(uuid.uuid4()).upper()


def contract_setup(generation: int = 4) -> ContractSetup:
    fake = FakePowerFlex(username="admin", password="pf-pass", generation=generation)
    guid_a, guid_b = _guid(), _guid()
    # SDCs register themselves with the MDM when installed on a host; the
    # adapter only finds them, so the fake starts with both hosts' SDCs.
    fake.add_sdc(guid_a)
    fake.add_sdc(guid_b)
    return ContractSetup(
        fake=fake,
        config=make_config("dell-powerflex", "https://powerflex.fake.example",
                           storage_pool_id=fake.pool_id),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("pf-pass")}),
        bad_credential=Credential({"username": SecretStr("admin"),
                                   "password": SecretStr("wrong")}),
        initiators={
            Protocol.SDC: (
                (Initiator(protocol=Protocol.SDC, address=guid_a),),
                (Initiator(protocol=Protocol.SDC, address=guid_b),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_volume(name, size),
        has_lun_numbers=False,
    )
