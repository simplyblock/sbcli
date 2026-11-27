"""Fake Infinidat InfiniBox REST API (``/api/rest``), the subset the adapter
uses.

Quirks reproduced: every reply is a ``{"result", "error", "metadata"}``
envelope (metadata ``page``/``page_size``/``pages_total``/
``number_of_objects``); lists are paged and filtered with
``field=eq:value``; destructive requests without ``?approved=true`` get
``APPROVAL_REQUIRED``; object ids are integers; volume ``serial`` is the
NAA-6 identifier without its leading ``6``; port types are
case-insensitive and FC addresses are accepted with or without colons;
``GET /system`` reports ``model``/``full_model``/integer ``serial_number``.

Codes taken from Infinidat's infinisdk / CSI driver: ``VOLUME_NOT_FOUND``,
``HOST_NOT_FOUND``, ``POOL_NOT_FOUND``, ``LUN_NOT_FOUND``,
``MAPPING_ALREADY_EXISTS``, ``APPROVAL_REQUIRED``. The name-conflict, port
ownership, mapped-volume and host-with-LUNs codes, the 403 status of
``APPROVAL_REQUIRED`` and LUN numbering from 1 are the fake's own reading.
Infinidat's simulator (``infinisim``) is not publicly available.
"""
from __future__ import annotations

from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import Initiator, Protocol, normalize_wwpn

from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_R = "/api/rest"
_ID = r"(?P<oid>\d+)"
_SYSTEM_ID = "000004e2"


def _err(status: int, code: str, message: str) -> FakeError:
    return FakeError(status, {
        "result": None,
        "error": {"code": code, "message": message, "is_remote": False, "data": None,
                  "reasons": [], "severity": "ERROR"},
        "metadata": {"ready": True}})


def _ok(result: Any, status: int = 200, metadata: dict[str, Any] | None = None) -> Reply:
    return Reply(status, {"result": result, "error": None,
                          "metadata": metadata or {"ready": True}})


def _port_address(ptype: str, address: str) -> str:
    if ptype == "FC":
        try:
            return normalize_wwpn(address)
        except ValueError:
            raise _err(400, "INVALID_PORT_ADDRESS", f"Invalid FC address {address}") from None
    return address.lower()


class FakeInfiniBox(FakeArray):
    vendor = "infinidat-infinibox"

    def __init__(self, username: str = "admin", password: str = "ibox-pass"):
        super().__init__()
        self.username = username
        self.password = password
        self.pools: dict[int, dict[str, Any]] = {}
        self.volumes: dict[int, dict[str, Any]] = {}
        self.hosts: dict[int, dict[str, Any]] = {}
        self.luns: list[dict[str, Any]] = []   # {host_id, volume_id, lun, id}
        #: require ``approved=true`` on every non-GET request, not only on
        #: destructive ones (which requests need approval varies by operation
        #: and version; infinisdk approves all of them)
        self.approval_for_all = False
        self.add_pool("sb-pool")

    # -- fake controls ------------------------------------------------------

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields["password"]

    def add_pool(self, name: str) -> int:
        pid = self.next_id()
        self.pools[pid] = {"id": pid, "name": name, "physical_capacity": 1 << 50,
                           "virtual_capacity": 1 << 52, "state": "NORMAL"}
        return pid

    def add_volume(self, name: str, size: int, pool_id: int | None = None) -> int:
        vid = self.next_id()
        self.volumes[vid] = {
            "id": vid, "name": name, "size": size, "used": 0, "allocated": 0,
            "pool_id": pool_id or next(iter(self.pools)), "provtype": "THIN",
            "type": "MASTER", "mapped": False, "write_protected": False,
            "serial": f"742b0f{_SYSTEM_ID}{vid:017x}", "created_at": 1763600000000,
            "parent_id": 0, "family_id": vid, "rmr_source": False, "rmr_target": False,
        }
        return vid

    # -- dispatch helpers ---------------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.basic_auth() != (self.username, self.password):
            raise _err(401, "UNAUTHORIZED", "Invalid credentials")
        if self.approval_for_all and req.method != "GET":
            self._approved(req, f"{req.method} {req.path}")

    @staticmethod
    def _approved(req: Request, what: str) -> None:
        if (req.q("approved") or "").lower() != "true":
            raise _err(403, "APPROVAL_REQUIRED", f"{what} requires approval")

    def _page(self, req: Request, items: list[dict[str, Any]]) -> Reply:
        for key, values in req.query.items():
            if key in ("page", "page_size", "approved", "fields", "sort"):
                continue
            raw = values[0]
            op, _, value = raw.partition(":") if ":" in raw else ("eq", "", raw)
            if op != "eq":
                raise _err(400, "UNSUPPORTED_FILTER", f"fake: filter op {op}")
            items = [i for i in items if str(i.get(key)) == value]
        page = int(req.q("page") or 1)
        size = int(req.q("page_size") or 50)
        if not 1 <= size <= 1000:
            raise _err(400, "BAD_PAGE_SIZE", "page_size must be 1..1000")
        total = len(items)
        pages = max(1, -(-total // size))
        chunk = items[(page - 1) * size: page * size]
        return _ok(chunk, metadata={"ready": True, "number_of_objects": total,
                                    "page_size": size, "pages_total": pages, "page": page})

    def _vol(self, vid: int) -> dict[str, Any]:
        if vid not in self.volumes:
            raise _err(404, "VOLUME_NOT_FOUND", f"Volume {vid} not found")
        return self.volumes[vid]

    def _host(self, hid: int) -> dict[str, Any]:
        if hid not in self.hosts:
            raise _err(404, "HOST_NOT_FOUND", f"Host {hid} not found")
        return self.hosts[hid]

    def _host_view(self, h: dict[str, Any]) -> dict[str, Any]:
        luns = [self._lun_view(m) for m in self.luns if m["host_id"] == h["id"]]
        return {**h, "ports": [dict(p) for p in h["ports"]], "luns": luns}

    def _lun_view(self, m: dict[str, Any]) -> dict[str, Any]:
        return {"id": m["id"], "lun": m["lun"], "volume_id": m["volume_id"],
                "host_id": m["host_id"], "host_cluster_id": 0, "clustered": False}

    # -- system / pools --------------------------------------------------------------


    # -- LIO data path (fakes.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return [str(i) for i in self.volumes]

    def lio_all_iqns(self) -> list[str]:
        return sorted({p["address"].lower() for h in self.hosts.values() for p in h["ports"] if p["type"] == "ISCSI"})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        for h in self.hosts.values():
            if host_id in (str(h["id"]), h["name"]):
                return [p["address"].lower() for p in h["ports"] if p["type"] == "ISCSI"]
        return []

    @route("GET", _R + "/system")
    def system(self, req):
        self._auth(req)
        return _ok({"name": "ibox-fake", "serial_number": 1250, "model": "F4304",
                    "full_model": "InfiniBox F4304", "version": "7.3.10.0",
                    "product_id": "INFINIBOX", "wwnn": "57:42:b0:f0:00:04:e2:00"})

    @route("GET", _R + "/pools")
    def list_pools(self, req):
        self._auth(req)
        return self._page(req, list(self.pools.values()))

    # -- volumes --------------------------------------------------------------------

    @route("GET", _R + "/volumes")
    def list_volumes(self, req):
        self._auth(req)
        return self._page(req, list(self.volumes.values()))

    @route("POST", _R + "/volumes")
    def create_volume(self, req):
        self._auth(req)
        body = req.json() or {}
        name, size, pool_id = str(body.get("name") or ""), body.get("size"), body.get("pool_id")
        if not isinstance(size, int) or size <= 0 or size % 512:
            raise _err(400, "BAD_REQUEST", "size must be a positive multiple of 512 bytes")
        if pool_id not in self.pools:
            raise _err(404, "POOL_NOT_FOUND", f"Pool {pool_id} not found")
        if body.get("provtype", "THIN") not in ("THIN", "THICK"):
            raise _err(400, "BAD_REQUEST", "provtype must be THIN or THICK")
        if any(v["name"] == name for v in self.volumes.values()):
            raise _err(409, "VOLUME_NAME_CONFLICT", f"Volume name {name} already exists")
        vid = self.add_volume(name, size, pool_id)
        self.volumes[vid]["provtype"] = body.get("provtype", "THIN")
        return _ok(self.volumes[vid], 201)

    @route("GET", _R + "/volumes/" + _ID)
    def get_volume(self, req, oid):
        self._auth(req)
        return _ok(self._vol(int(oid)))

    @route("PUT", _R + "/volumes/" + _ID)
    def update_volume(self, req, oid):
        self._auth(req)
        v = self._vol(int(oid))
        body = req.json() or {}
        if "size" in body:
            size = body["size"]
            if not isinstance(size, int) or size <= 0 or size % 512:
                raise _err(400, "BAD_REQUEST", "size must be a positive multiple of 512 bytes")
            if size < v["size"]:
                self._approved(req, "Shrinking a volume")
            v["size"] = size
        return _ok(v)

    @route("DELETE", _R + "/volumes/" + _ID)
    def delete_volume(self, req, oid):
        self._auth(req)
        v = self._vol(int(oid))
        self._approved(req, "Deleting a volume")
        if any(m["volume_id"] == v["id"] for m in self.luns):
            raise _err(409, "VOLUME_IS_MAPPED", "Volume is mapped; unmap it first")
        del self.volumes[v["id"]]
        return _ok(v)

    @route("GET", _R + "/volumes/" + _ID + "/luns")
    def volume_luns(self, req, oid):
        self._auth(req)
        vid = self._vol(int(oid))["id"]
        return _ok([self._lun_view(m) for m in self.luns if m["volume_id"] == vid])

    # -- hosts ------------------------------------------------------------------------

    @route("GET", _R + "/hosts")
    def list_hosts(self, req):
        self._auth(req)
        return self._page(req, [self._host_view(h) for h in self.hosts.values()])

    @route("POST", _R + "/hosts")
    def create_host(self, req):
        self._auth(req)
        name = (req.json() or {}).get("name")
        if not name:
            raise _err(400, "BAD_REQUEST", "name is required")
        if any(h["name"] == name for h in self.hosts.values()):
            raise _err(409, "HOST_NAME_CONFLICT", f"Host name {name} already exists")
        hid = self.next_id()
        self.hosts[hid] = {"id": hid, "name": name, "ports": [], "security_method": "NONE",
                           "host_type": "LINUX", "host_cluster_id": 0}
        return _ok(self._host_view(self.hosts[hid]), 201)

    @route("DELETE", _R + "/hosts/" + _ID)
    def delete_host(self, req, oid):
        self._auth(req)
        h = self._host(int(oid))
        self._approved(req, "Deleting a host")
        if any(m["host_id"] == h["id"] for m in self.luns):
            raise _err(409, "HOST_HAS_LUNS", "Host has mapped volumes")
        del self.hosts[h["id"]]
        return _ok(self._host_view(h))

    @route("POST", _R + "/hosts/" + _ID + "/ports")
    def add_port(self, req, oid):
        self._auth(req)
        h = self._host(int(oid))
        body = req.json() or {}
        ptype = str(body.get("type", "")).upper()
        if ptype not in ("ISCSI", "FC", "NVME"):
            raise _err(400, "BAD_REQUEST", f"Unsupported port type {ptype}")
        address = _port_address(ptype, str(body.get("address", "")))
        for other in self.hosts.values():
            for p in other["ports"]:
                if p["type"] == ptype and p["address"] == address:
                    if other["id"] == h["id"]:
                        raise _err(409, "PORT_ALREADY_EXISTS",
                                   f"Port {address} already belongs to this host")
                    raise _err(409, "PORT_ALREADY_BELONGS_TO_HOST",
                               f"Port {address} belongs to host {other['name']}")
        port = {"type": ptype, "address": address, "host_id": h["id"]}
        h["ports"].append(port)
        return _ok(port, 201)

    @route("POST", _R + "/hosts/" + _ID + "/luns")
    def map_lun(self, req, oid):
        self._auth(req)
        h = self._host(int(oid))
        vid = (req.json() or {}).get("volume_id")
        if not isinstance(vid, int):
            raise _err(400, "BAD_REQUEST", "volume_id must be an integer")
        self._vol(vid)
        if any(m["host_id"] == h["id"] and m["volume_id"] == vid for m in self.luns):
            raise _err(409, "MAPPING_ALREADY_EXISTS", "Volume already mapped to host")
        used = {m["lun"] for m in self.luns if m["host_id"] == h["id"]}
        lun = next(n for n in range(1, 4096) if n not in used)
        m = {"id": self.next_id(), "host_id": h["id"], "volume_id": vid, "lun": lun}
        self.luns.append(m)
        self.volumes[vid]["mapped"] = True
        return _ok(self._lun_view(m), 201)

    @route("DELETE", _R + "/hosts/" + _ID + r"/luns/volume_id/(?P<vid>\d+)")
    def unmap_lun(self, req, oid, vid):
        self._auth(req)
        h = self._host(int(oid))
        self._approved(req, "Unmapping a volume")
        match = [m for m in self.luns if m["host_id"] == h["id"] and m["volume_id"] == int(vid)]
        if not match:
            raise _err(404, "LUN_NOT_FOUND", f"Volume {vid} is not mapped to host {h['id']}")
        self.luns.remove(match[0])
        if int(vid) in self.volumes:
            self.volumes[int(vid)]["mapped"] = any(
                m["volume_id"] == int(vid) for m in self.luns)
        return _ok(self._lun_view(match[0]))

    def not_routed(self, req: Request) -> Reply:
        return Reply(404, {"result": None, "error": {
            "code": "NOT_FOUND", "message": f"fake: no route for {req.method} {req.path}"},
            "metadata": {"ready": True}})


def contract_setup() -> ContractSetup:
    fake = FakeInfiniBox(username="admin", password="ibox-pass")
    return ContractSetup(
        fake=fake,
        config=make_config("infinidat-infinibox", "https://ibox.fake.example", pool="sb-pool"),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("ibox-pass")}),
        bad_credential=Credential({"username": SecretStr("admin"),
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
        create_foreign_volume=lambda name, size: str(fake.add_volume(name, size)),
    )
