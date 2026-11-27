"""Fake Pure FlashArray REST 2.x (the subset the adapter uses).

Modelled on py-pure-client (``_common/apis/*_v_2_36.py``, ``models``):
connections of NVMe hosts carry ``nsid`` and no ``lun`` (REST 2.27+), SCSI
ones ``lun``; errors are HTTP 400 ``{"errors": [{"message", "context"}]}``.
"""
from __future__ import annotations

import secrets
from typing import Any, ClassVar

from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import Initiator, Protocol

from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_V = r"/api/(?P<ver>2\.\d+)"


def _err(msg: str, status: int = 400) -> FakeError:
    return FakeError(status, {"errors": [{"message": msg, "context": ""}]})


class FakePureFlashArray(FakeArray):
    vendor = "pure-flasharray"
    versions: ClassVar[list[str]] = ["1.19", "2.0", "2.4", "2.17", "2.21", "2.36", "2.40"]

    def __init__(self, api_token: str = "pure-token"):
        super().__init__()
        self.api_token = api_token
        self.sessions: set[str] = set()
        self.volumes: dict[str, dict[str, Any]] = {}   # id -> volume
        self.hosts: dict[str, dict[str, Any]] = {}     # name -> host
        self.connections: list[dict[str, Any]] = []    # {host, volume_id, lun|nsid}

    # -- fake controls ------------------------------------------------------

    def expire_sessions(self) -> None:
        self.sessions.clear()

    def set_credential(self, **fields: str) -> None:
        self.api_token = fields["api_token"]

    def add_volume(self, name: str, size: int) -> str:
        vid = f"{secrets.token_hex(4)}-{self.next_id()}"
        self.volumes[vid] = {"id": vid, "name": name, "provisioned": size,
                             "serial": secrets.token_hex(12).upper(), "destroyed": False}
        return vid

    # -- auth ---------------------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.headers.get("x-auth-token") not in self.sessions:
            raise _err("Authentication failed.", 401)


    # -- LIO data path (fakes.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return [v["id"] for v in self.volumes.values() if not v.get("destroyed")]

    def lio_all_iqns(self) -> list[str]:
        return sorted({str(i).lower() for h in self.hosts.values() for i in h.get("iqns", [])})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        host = self.hosts.get(host_id)
        return [str(i).lower() for i in (host or {}).get("iqns", [])]

    @route("GET", "/api/api_version")
    def api_version(self, req):
        return Reply(200, {"version": self.versions})

    @route("POST", _V + "/login")
    def login(self, req, ver):
        if req.headers.get("api-token") != self.api_token:
            raise _err("Invalid API token.", 401)
        token = secrets.token_hex(8)
        self.sessions.add(token)
        return Reply(200, {"items": [{"username": "sb"}]}, {"x-auth-token": token})

    @route("POST", _V + "/logout")
    def logout(self, req, ver):
        self.sessions.discard(req.headers.get("x-auth-token"))
        return Reply(200, {})

    @route("GET", _V + "/arrays")
    def arrays(self, req, ver):
        self._auth(req)
        return Reply(200, {"items": [{"id": "fake-array-id", "name": "fake-fa",
                                      "os": "Purity//FA", "version": "6.5.0"}]})

    @route("GET", _V + "/controllers")
    def controllers(self, req, ver):
        self._auth(req)
        return Reply(200, {"items": [{"name": "CT0", "model": "FA-X70R4"}]})

    # -- volumes -------------------------------------------------------------

    def _select(self, req) -> list[dict[str, Any]]:
        if req.q("ids"):
            ids = req.q("ids").split(",")
            missing = [i for i in ids if i not in self.volumes]
            if missing:
                raise _err("Volume does not exist.")
            return [self.volumes[i] for i in ids]
        names = (req.q("names") or "").split(",")
        found = [v for v in self.volumes.values() if v["name"] in names]
        if len(found) < len(names):
            raise _err("Volume does not exist.")
        return found

    @route("GET", _V + "/volumes")
    def get_volumes(self, req, ver):
        self._auth(req)
        return Reply(200, {"items": self._select(req)})

    @route("POST", _V + "/volumes")
    def post_volumes(self, req, ver):
        self._auth(req)
        name = req.q("names")
        body = req.json()
        if any(v["name"] == name for v in self.volumes.values()):
            raise _err("Volume already exists.")
        size = int(body["provisioned"])
        if size % 512:
            raise _err("Volume size must be a multiple of 512.")
        vid = self.add_volume(name, size)
        return Reply(200, {"items": [self.volumes[vid]]})

    @route("PATCH", _V + "/volumes")
    def patch_volumes(self, req, ver):
        self._auth(req)
        body = req.json()
        vols = self._select(req)
        for v in vols:
            if "provisioned" in body:
                size = int(body["provisioned"])
                if size < v["provisioned"] and not body.get("truncate"):
                    raise _err("Implicit truncation is not permitted.")
                v["provisioned"] = size
            if body.get("destroyed"):
                if any(c["volume_id"] == v["id"] for c in self.connections):
                    raise _err("Volume has host connections.")
                v["destroyed"] = True
        return Reply(200, {"items": vols})

    @route("DELETE", _V + "/volumes")
    def delete_volumes(self, req, ver):
        self._auth(req)
        for v in self._select(req):
            if not v["destroyed"]:
                raise _err("Volume must be destroyed before eradication.")
            del self.volumes[v["id"]]
        return Reply(200, {})

    # -- hosts ----------------------------------------------------------------

    def _owner(self, key: str, value: str) -> str | None:
        for h in self.hosts.values():
            if value.lower() in (x.lower() for x in h[key]):
                return h["name"]
        return None

    @route("GET", _V + "/hosts")
    def get_hosts(self, req, ver):
        self._auth(req)
        name = req.q("names")
        if name not in self.hosts:
            raise _err("Host does not exist.")
        return Reply(200, {"items": [self.hosts[name]]})

    def _claim(self, host: str, body: dict[str, Any], prefix: str = "") -> None:
        for key in ("iqns", "wwns", "nqns"):
            for value in body.get(prefix + key, []):
                owner = self._owner(key, value)
                if owner and owner != host:
                    raise _err(f"{value} is already in use by host {owner}.")
                if key == "wwns" and len(value.replace(":", "")) != 16:
                    raise _err("Invalid WWN.")
                if value.lower() not in (x.lower() for x in self.hosts[host][key]):
                    self.hosts[host][key].append(value)

    @route("POST", _V + "/hosts")
    def post_hosts(self, req, ver):
        self._auth(req)
        name = req.q("names")
        if name in self.hosts:
            raise _err("Host already exists.")
        self.hosts[name] = {"name": name, "iqns": [], "wwns": [], "nqns": []}
        try:
            self._claim(name, req.json() or {})
        except FakeError:
            del self.hosts[name]
            raise
        return Reply(200, {"items": [self.hosts[name]]})

    @route("PATCH", _V + "/hosts")
    def patch_hosts(self, req, ver):
        self._auth(req)
        name = req.q("names")
        if name not in self.hosts:
            raise _err("Host does not exist.")
        self._claim(name, req.json() or {}, prefix="add_")
        return Reply(200, {"items": [self.hosts[name]]})

    @route("DELETE", _V + "/hosts")
    def delete_hosts(self, req, ver):
        self._auth(req)
        name = req.q("names")
        if name not in self.hosts:
            raise _err("Host does not exist.")
        if any(c["host"] == name for c in self.connections):
            raise _err("Host has volume connections.")
        del self.hosts[name]
        return Reply(200, {})

    # -- connections ------------------------------------------------------------

    def _conn_item(self, c):
        v = self.volumes[c["volume_id"]]
        item = {"host": {"name": c["host"]}, "host_group": None,
                "volume": {"id": v["id"], "name": v["name"]}, "lun": None, "nsid": None}
        item["nsid" if "nsid" in c else "lun"] = c.get("nsid", c.get("lun"))
        return item

    @route("GET", _V + "/connections")
    def get_connections(self, req, ver):
        self._auth(req)
        vid, host = req.q("volume_ids"), req.q("host_names")
        if vid and vid not in self.volumes:
            raise _err("Volume does not exist.")
        items = [self._conn_item(c) for c in self.connections
                 if (not vid or c["volume_id"] == vid) and (not host or c["host"] == host)]
        return Reply(200, {"items": items})

    @route("POST", _V + "/connections")
    def post_connections(self, req, ver):
        self._auth(req)
        vid, host = req.q("volume_ids"), req.q("host_names")
        if vid not in self.volumes or self.volumes[vid]["destroyed"]:
            raise _err("Volume does not exist.")
        if host not in self.hosts:
            raise _err("Host does not exist.")
        if any(c["volume_id"] == vid and c["host"] == host for c in self.connections):
            raise _err("Connection already exists.")
        key = "nsid" if self.hosts[host]["nqns"] else "lun"
        used = {c.get(key) for c in self.connections if c["host"] == host}
        c = {"host": host, "volume_id": vid,
             key: next(n for n in range(1, 4096) if n not in used)}
        self.connections.append(c)
        return Reply(200, {"items": [self._conn_item(c)]})

    @route("DELETE", _V + "/connections")
    def delete_connections(self, req, ver):
        self._auth(req)
        vid, host = req.q("volume_ids"), req.q("host_names")
        before = len(self.connections)
        self.connections = [c for c in self.connections
                            if not (c["volume_id"] == vid and c["host"] == host)]
        if len(self.connections) == before:
            raise _err("Connection does not exist.")
        return Reply(200, {})


def contract_setup() -> ContractSetup:
    fake = FakePureFlashArray(api_token="pure-token")
    return ContractSetup(
        fake=fake,
        config=make_config("pure-flasharray", "https://pure.fake.example"),
        credential=Credential({"api_token": SecretStr("pure-token")}),
        bad_credential=Credential({"api_token": SecretStr("wrong")}),
        initiators={
            Protocol.ISCSI: (
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-a"),),
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-b"),),
            ),
            Protocol.FC: (
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2b"),),
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c"),),
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
