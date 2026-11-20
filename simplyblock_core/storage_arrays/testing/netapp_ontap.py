"""Fake NetApp ONTAP 9.x REST API (the subset the adapter uses).

Modelled behaviour:

* HTTP basic auth on every request (``401`` + ONTAP error body otherwise).
* Collections answer ``{"records": [...], "num_records": n}``; query
  parameters filter on (dotted, array-flattening) fields with ``|`` for OR
  and ``*`` wildcards; ``fields=`` projects the records (key fields ``uuid``
  and ``name`` are always returned), so an adapter that forgets to ask for a
  field does not get it.
* Errors: ``{"error": {"message": ..., "code": ...}}``; ``404`` for unknown
  objects, ``409`` for duplicates, ``400`` for everything else. Numeric
  codes are illustrative.
* FlexVol create/resize/delete are asynchronous: ``202`` with a job link;
  the effect happens when the job runs, after ``job_polls`` polls of
  ``/api/cluster/jobs/{uuid}``.
* Size rules: FlexVol >= 20 MiB (rounded up to 4 KiB); LUN/namespace must fit
  in its FlexVol (usable = size minus snapshot reserve), cannot shrink, and a
  namespace is a multiple of its block size.
* Name rules: FlexVol ``[A-Za-z_][A-Za-z0-9_]*``; LUN/namespace path
  ``/vol/<flexvol>/<name>``.
* An initiator / host NQN may be in several igroups / subsystems (as on
  ONTAP); a namespace can be mapped to one subsystem only; mapped
  LUNs/namespaces/igroups/subsystems cannot be deleted.
"""
from __future__ import annotations

import copy
import fnmatch
import re
import secrets
import string
import uuid as uuid_mod
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_U = r"(?P<uuid>[0-9a-fA-F-]{36})"
_RESERVED = frozenset({"fields", "return_records", "return_timeout", "max_records", "order_by",
                       "allow_delete_while_mapped", "allow_delete_with_hosts"})
_FLEXVOL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,202}$")
_PATH_RE = re.compile(r"^/vol/(?P<vol>[^/]+)/(?P<name>[A-Za-z0-9_.\-{}]{1,255})$")
_IGROUP_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
_SUBSYS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]{0,95}$")
_WWPN_RE = re.compile(r"^([0-9a-f]{2}:){7}[0-9a-f]{2}$", re.IGNORECASE)
_SERIAL_CHARS = string.ascii_letters + string.digits + "-]?$"
MIN_VOLUME = 20 << 20
MAX_LUN = 128 << 40


def _err(status: int, message: str, code: str = "1", target: str | None = None) -> FakeError:
    body: dict[str, Any] = {"error": {"message": message, "code": code}}
    if target:
        body["error"]["target"] = target
    return FakeError(status, body)


def _not_found(what: str = "entry") -> FakeError:
    return _err(404, f"{what} doesn't exist", "4")


def _resolve(obj: Any, parts: list[str]) -> list[Any]:
    if not parts:
        return obj if isinstance(obj, list) else [obj]
    if isinstance(obj, list):
        return [v for e in obj for v in _resolve(e, parts)]
    if isinstance(obj, dict) and parts[0] in obj:
        return _resolve(obj[parts[0]], parts[1:])
    return []


def _matches(rec: dict[str, Any], key: str, pattern: str) -> bool:
    values = _resolve(rec, key.split("."))
    texts = [str(v).lower() for v in values]  # True -> "true", as in ONTAP queries
    for p in pattern.split("|"):
        if any(fnmatch.fnmatchcase(t, p.lower()) for t in texts):
            return True
    return False


def _project(obj: Any, paths: list[list[str]]) -> Any:
    if any(not p for p in paths):
        return copy.deepcopy(obj)
    if isinstance(obj, list):
        return [_project(e, paths) for e in obj]
    if not isinstance(obj, dict):
        return copy.deepcopy(obj)
    out: dict[str, Any] = {}
    groups: dict[str, list[list[str]]] = {}
    for p in paths:
        groups.setdefault(p[0], []).append(p[1:])
    for key, sub in groups.items():
        if key in obj:
            out[key] = _project(obj[key], sub)
    return out


class FakeNetAppOntap(FakeArray):
    vendor = "netapp-ontap"

    def __init__(self, username: str = "admin", password: str = "netapp1!",
                 svm: str = "svm1"):
        super().__init__()
        self.username = username
        self.password = password
        self.svms: dict[str, str] = {svm: str(uuid_mod.uuid4())}
        self.aggregates: list[dict[str, Any]] = [
            {"uuid": str(uuid_mod.uuid4()), "name": "aggr1",
             "space": {"block_storage": {"available": 10 << 40}}},
            {"uuid": str(uuid_mod.uuid4()), "name": "aggr2",
             "space": {"block_storage": {"available": 30 << 40}}},
        ]
        self.volumes: dict[str, dict[str, Any]] = {}
        self.luns: dict[str, dict[str, Any]] = {}
        self.namespaces: dict[str, dict[str, Any]] = {}
        self.igroups: dict[str, dict[str, Any]] = {}
        self.lun_maps: list[dict[str, Any]] = []
        self.subsystems: dict[str, dict[str, Any]] = {}
        self.subsystem_maps: list[dict[str, Any]] = []
        self.jobs: dict[str, dict[str, Any]] = {}
        #: Number of job polls answering ``running`` before a job runs.
        self.job_polls = 1
        #: When True, jobs never finish.
        self.jobs_stuck = False
        self._job_failures: list[tuple[str, str]] = []

    # -- fake controls ------------------------------------------------------

    def set_credential(self, **fields: str) -> None:
        self.username = fields["username"]
        self.password = fields["password"]

    def fail_next_job(self, message: str, code: str = "917504") -> None:
        """Make the next job created end in ``failure`` with this message."""
        self._job_failures.append((message, code))

    def svm_ref(self, name: str) -> dict[str, str]:
        if name not in self.svms:
            raise _err(404, f'SVM "{name}" not found', "2621462")
        return {"name": name, "uuid": self.svms[name]}

    def add_flexvol(self, name: str, size: int, svm: str = "svm1",
                    snapshot_reserve: int = 0) -> str:
        vid = str(uuid_mod.uuid4())
        self.volumes[vid] = {
            "uuid": vid, "name": name, "svm": self.svm_ref(svm), "size": size,
            "type": "rw", "style": "flexvol", "state": "online",
            "guarantee": {"type": "none"},
            "space": {"snapshot": {"reserve_percent": snapshot_reserve}},
            "snapshot_policy": {"name": "default"},
            "aggregates": [{"name": "aggr1", "uuid": self.aggregates[0]["uuid"]}],
            "comment": "",
        }
        return vid

    def add_lun(self, path: str, size: int, svm: str = "svm1") -> str:
        vol = self._vol_of_path(path, svm)
        lid = str(uuid_mod.uuid4())
        self.luns[lid] = {
            "uuid": lid, "name": path, "svm": self.svm_ref(svm), "os_type": "linux",
            "serial_number": "".join(secrets.choice(_SERIAL_CHARS) for _ in range(12)),
            "space": {"size": size, "guarantee": {"requested": False, "reserved": False}},
            "location": {"logical_unit": path.rsplit("/", 1)[1],
                         "volume": {"name": vol["name"], "uuid": vol["uuid"]}},
            "status": {"mapped": False, "state": "online"},
        }
        return lid

    def add_foreign_lun(self, name: str, size: int) -> str:
        flex = name.replace("-", "_")
        self.add_flexvol(flex, size * 2)
        return self.add_lun(f"/vol/{flex}/{name}", size)

    # -- auth / plumbing ------------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.basic_auth() != (self.username, self.password):
            raise _err(401, "User is not authorized.", "6")

    def _collection(self, req: Request, records: list[dict[str, Any]],
                    keys: tuple[str, ...] = ("uuid", "name")) -> Reply:
        filters = {k: v[0] for k, v in req.query.items() if k not in _RESERVED and v}
        found = [r for r in records if all(_matches(r, k, v) for k, v in filters.items())]
        return Reply(200, {"records": [self._fields(r, req, keys) for r in found],
                           "num_records": len(found),
                           "_links": {"self": {"href": req.path}}})

    @staticmethod
    def _fields(rec: dict[str, Any], req: Request, keys: tuple[str, ...]) -> dict[str, Any]:
        fields = req.q("fields")
        if fields == "*":
            return copy.deepcopy(rec)
        paths = [[k] for k in keys if k in rec]
        if fields:
            paths += [f.strip().split(".") for f in fields.split(",") if f.strip()]
        return _project(rec, paths)

    @staticmethod
    def _created(req: Request, rec: dict[str, Any], location: str) -> Reply:
        if req.q("return_records") == "true":
            return Reply(201, {"num_records": 1, "records": [copy.deepcopy(rec)]},
                         {"Location": location})
        return Reply(201, None, {"Location": location})

    def _svm_of(self, body: dict[str, Any]) -> dict[str, str]:
        svm = (body.get("svm") or {}).get("name")
        if not svm:
            raise _err(400, 'Field "svm.name" is required.', "2")
        return self.svm_ref(svm)

    def _vol_of_path(self, path: str, svm: str) -> dict[str, Any]:
        m = _PATH_RE.match(path or "")
        if not m:
            raise _err(400, f'Invalid path "{path}". Paths must be of the form '
                            '"/vol/<volume>/<name>".', "5374082")
        for v in self.volumes.values():
            if v["name"] == m["vol"] and v["svm"]["name"] == svm:
                return v
        raise _err(404, f'Volume "{m["vol"]}" in SVM "{svm}" does not exist.', "917927")

    @staticmethod
    def _usable(vol: dict[str, Any]) -> int:
        reserve = vol["space"]["snapshot"]["reserve_percent"]
        return vol["size"] * (100 - reserve) // 100

    # -- jobs -----------------------------------------------------------------

    def _job(self, description: str, action: Callable[[], None]) -> Reply:
        jid = str(uuid_mod.uuid4())
        failure = self._job_failures.pop(0) if self._job_failures else None
        self.jobs[jid] = {"uuid": jid, "description": description, "state": "queued",
                          "message": "", "code": 0, "_polls": self.job_polls,
                          "_action": action, "_failure": failure}
        href = f"/api/cluster/jobs/{jid}"
        return Reply(202, {"job": {"uuid": jid, "_links": {"self": {"href": href}}}})

    @route("GET", r"/api/cluster/jobs/" + _U)
    def get_job(self, req, uuid):
        self._auth(req)
        job = self.jobs.get(uuid)
        if job is None:
            raise _not_found()
        if job["state"] in ("queued", "running"):
            if self.jobs_stuck or job["_polls"] > 0:
                job["_polls"] -= 1
                job["state"] = "running"
                job["message"] = "In progress"
            elif job["_failure"]:
                job["state"], job["message"] = "failure", job["_failure"][0]
                job["code"] = int(job["_failure"][1])
            else:
                try:
                    job["_action"]()
                    job["state"], job["message"] = "success", "success"
                except FakeError as e:
                    err = e.reply.body["error"]
                    job["state"], job["message"] = "failure", err["message"]
                    job["code"] = int(err["code"])
        return Reply(200, {k: v for k, v in job.items() if not k.startswith("_")})

    # -- cluster ----------------------------------------------------------------

    @route("GET", "/api/cluster")
    def cluster(self, req):
        self._auth(req)
        return Reply(200, {"name": "fake-ontap", "uuid": "1cd8a442-86d1-11e0-ae1c-123478563412",
                           "version": {"full": "NetApp Release 9.14.1: Mon Jan 01 00:00:00 UTC "
                                               "2024", "generation": 9, "major": 14,
                                       "minor": 1}})

    @route("GET", "/api/cluster/nodes")
    def nodes(self, req):
        self._auth(req)
        return self._collection(req, [
            {"uuid": "6ab1b4b3-0000-0000-0000-000000000001", "name": "fake-ontap-01",
             "model": "AFF-A400", "serial_number": "721802000123"},
            {"uuid": "6ab1b4b3-0000-0000-0000-000000000002", "name": "fake-ontap-02",
             "model": "AFF-A400", "serial_number": "721802000124"},
        ])

    @route("GET", "/api/svm/svms")
    def get_svms(self, req):
        self._auth(req)
        return self._collection(req, [{"uuid": u, "name": n} for n, u in self.svms.items()])

    @route("GET", "/api/storage/aggregates")
    def get_aggregates(self, req):
        self._auth(req)
        return self._collection(req, self.aggregates)

    # -- FlexVols ---------------------------------------------------------------

    @route("GET", "/api/storage/volumes")
    def get_volumes(self, req):
        self._auth(req)
        return self._collection(req, list(self.volumes.values()))

    @route("GET", "/api/storage/volumes/" + _U)
    def get_volume(self, req, uuid):
        self._auth(req)
        if uuid not in self.volumes:
            raise _not_found()
        return Reply(200, self._fields(self.volumes[uuid], req, ("uuid", "name")) if req.q(
            "fields") else copy.deepcopy(self.volumes[uuid]))

    @route("POST", "/api/storage/volumes")
    def post_volume(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        name = body.get("name", "")
        if not _FLEXVOL_RE.match(name):
            raise _err(400, f'Invalid volume name "{name}". Volume names must begin with a '
                            'letter or underscore and contain only letters, digits and '
                            'underscores.', "917888")
        size = int(body.get("size", 0))
        if size < MIN_VOLUME:
            raise _err(400, f"Volume size {size} is too small; minimum is {MIN_VOLUME}.",
                       "917534")
        aggrs = body.get("aggregates") or []
        if len(aggrs) > 1:
            # Several aggregates would create a FlexGroup: the adapter must not.
            raise _err(400, "Specifying multiple aggregates creates a FlexGroup volume, "
                            "which is not supported for LUNs.", "917833")
        known = {a["name"]: a for a in self.aggregates}
        for a in aggrs:
            if a.get("name") not in known:
                raise _err(404, f'Aggregate "{a.get("name")}" does not exist.', "917927")
        size = -(-size // 4096) * 4096

        def create() -> None:
            if any(v["name"] == name and v["svm"]["name"] == svm["name"]
                   for v in self.volumes.values()):
                raise _err(409, f'Duplicate volume name "{name}".', "917535")
            vid = self.add_flexvol(name, size, svm["name"],
                                   int(((body.get("space") or {}).get("snapshot") or {})
                                       .get("reserve_percent", 5)))
            vol = self.volumes[vid]
            vol["guarantee"] = {"type": (body.get("guarantee") or {}).get("type", "volume")}
            vol["snapshot_policy"] = {"name": (body.get("snapshot_policy") or {})
                                      .get("name", "default")}
            vol["comment"] = body.get("comment", "")
            if aggrs:
                a = known[aggrs[0]["name"]]
                vol["aggregates"] = [{"name": a["name"], "uuid": a["uuid"]}]

        return self._job(f"POST /api/storage/volumes/{name}", create)

    @route("PATCH", "/api/storage/volumes/" + _U)
    def patch_volume(self, req, uuid):
        self._auth(req)
        if uuid not in self.volumes:
            raise _not_found()
        body = req.json() or {}
        vol = self.volumes[uuid]

        def modify() -> None:
            if "size" in body:
                size = -(-int(body["size"]) // 4096) * 4096
                reserve = vol["space"]["snapshot"]["reserve_percent"]
                biggest = max([lun["space"]["size"] for lun in self._luns_in(uuid)] or [0])
                if size < MIN_VOLUME or size * (100 - reserve) // 100 < biggest:
                    raise _err(400, "Cannot shrink the volume below the space used by its "
                                    "LUNs or namespaces.", "917627")
                vol["size"] = size

        return self._job(f"PATCH /api/storage/volumes/{uuid}", modify)

    def _luns_in(self, vol_uuid: str) -> list[dict[str, Any]]:
        objs = list(self.luns.values()) + list(self.namespaces.values())
        return [o for o in objs if o["location"]["volume"]["uuid"] == vol_uuid]

    @route("DELETE", "/api/storage/volumes/" + _U)
    def delete_volume(self, req, uuid):
        self._auth(req)
        if uuid not in self.volumes:
            raise _not_found()

        def delete() -> None:
            contained = self._luns_in(uuid)
            if any(o["status"]["mapped"] for o in contained):
                raise _err(400, "Volume contains mapped LUNs or namespaces.", "917536")
            for o in contained:
                self.luns.pop(o["uuid"], None)
                self.namespaces.pop(o["uuid"], None)
            del self.volumes[uuid]

        return self._job(f"DELETE /api/storage/volumes/{uuid}", delete)

    # -- LUNs and namespaces ------------------------------------------------------

    def _check_size(self, vol: dict[str, Any], size: int, block: int = 512) -> None:
        if size < 4096 or size > MAX_LUN:
            raise _err(400, f"Size {size} is out of range (4096..{MAX_LUN}).", "5374855")
        if size % block:
            raise _err(400, f"Size {size} is not a multiple of the block size {block}.",
                       "72089652")
        if size > self._usable(vol):
            raise _err(400, f'Size {size} exceeds the space available in volume '
                            f'"{vol["name"]}".', "5374857")

    @route("GET", "/api/storage/luns")
    def get_luns(self, req):
        self._auth(req)
        return self._collection(req, list(self.luns.values()))

    @route("GET", "/api/storage/luns/" + _U)
    def get_lun(self, req, uuid):
        self._auth(req)
        if uuid not in self.luns:
            raise _not_found()
        return Reply(200, copy.deepcopy(self.luns[uuid]))

    @route("POST", "/api/storage/luns")
    def post_lun(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        path = body.get("name", "")
        vol = self._vol_of_path(path, svm["name"])
        if not body.get("os_type"):
            raise _err(400, 'Field "os_type" is required.', "2")
        if any(o["name"] == path for o in list(self.luns.values()) +
               list(self.namespaces.values())):
            raise _err(409, f'A LUN or namespace already exists at path "{path}".', "5374863")
        size = int((body.get("space") or {}).get("size", 0))
        self._check_size(vol, size)
        lid = self.add_lun(path, size, svm["name"])
        lun = self.luns[lid]
        lun["os_type"] = body["os_type"]
        requested = bool(((body.get("space") or {}).get("guarantee") or {}).get("requested"))
        lun["space"]["guarantee"] = {"requested": requested, "reserved": requested}
        return self._created(req, lun, f"/api/storage/luns/{lid}")

    @route("PATCH", "/api/storage/luns/" + _U)
    def patch_lun(self, req, uuid):
        self._auth(req)
        lun = self.luns.get(uuid)
        if lun is None:
            raise _not_found()
        size = ((req.json() or {}).get("space") or {}).get("size")
        if size is not None:
            size = int(size)
            if size < lun["space"]["size"]:
                raise _err(400, "Reducing the size of a LUN might cause permanent data loss "
                                "or corruption and is not supported by the LUN REST API.",
                           "5374922")
            self._check_size(self.volumes[lun["location"]["volume"]["uuid"]], size)
            lun["space"]["size"] = size
        return Reply(200, {})

    @route("DELETE", "/api/storage/luns/" + _U)
    def delete_lun(self, req, uuid):
        self._auth(req)
        lun = self.luns.get(uuid)
        if lun is None:
            raise _not_found()
        if lun["status"]["mapped"] and req.q("allow_delete_while_mapped") != "true":
            raise _err(400, "The LUN is mapped; unmap it or set allow_delete_while_mapped.",
                       "5374875")
        self.lun_maps = [m for m in self.lun_maps if m["lun"]["uuid"] != uuid]
        del self.luns[uuid]
        return Reply(200, {})

    @route("GET", "/api/storage/namespaces")
    def get_namespaces(self, req):
        self._auth(req)
        return self._collection(req, list(self.namespaces.values()))

    @route("POST", "/api/storage/namespaces")
    def post_namespace(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        path = body.get("name", "")
        vol = self._vol_of_path(path, svm["name"])
        if not body.get("os_type"):
            raise _err(400, 'Field "os_type" is required.', "2")
        if any(o["name"] == path for o in list(self.luns.values()) +
               list(self.namespaces.values())):
            raise _err(409, f'A LUN or namespace already exists at path "{path}".', "72090019")
        space = body.get("space") or {}
        block = int(space.get("block_size", 4096))
        if block not in (512, 4096):
            raise _err(400, "block_size must be 512 or 4096.", "72089653")
        size = int(space.get("size", 0))
        self._check_size(vol, size, block)
        nid = str(uuid_mod.uuid4())
        self.namespaces[nid] = {
            "uuid": nid, "name": path, "svm": svm, "os_type": body["os_type"],
            "space": {"size": size, "block_size": block},
            "location": {"namespace": path.rsplit("/", 1)[1],
                         "volume": {"name": vol["name"], "uuid": vol["uuid"]}},
            "status": {"mapped": False, "state": "online"},
        }
        return self._created(req, self.namespaces[nid], f"/api/storage/namespaces/{nid}")

    @route("PATCH", "/api/storage/namespaces/" + _U)
    def patch_namespace(self, req, uuid):
        self._auth(req)
        ns = self.namespaces.get(uuid)
        if ns is None:
            raise _not_found()
        size = ((req.json() or {}).get("space") or {}).get("size")
        if size is not None:
            size = int(size)
            if size < ns["space"]["size"]:
                raise _err(400, "Shrinking a namespace is not supported.", "72090031")
            self._check_size(self.volumes[ns["location"]["volume"]["uuid"]], size,
                             ns["space"]["block_size"])
            ns["space"]["size"] = size
        return Reply(200, {})

    @route("DELETE", "/api/storage/namespaces/" + _U)
    def delete_namespace(self, req, uuid):
        self._auth(req)
        ns = self.namespaces.get(uuid)
        if ns is None:
            raise _not_found()
        if ns["status"]["mapped"] and req.q("allow_delete_while_mapped") != "true":
            raise _err(400, "The namespace is mapped to a subsystem.", "72090024")
        self.subsystem_maps = [m for m in self.subsystem_maps if m["namespace"]["uuid"] != uuid]
        del self.namespaces[uuid]
        return Reply(200, {})

    # -- igroups -------------------------------------------------------------------

    def _check_initiator(self, group: dict[str, Any], name: str) -> None:
        proto = group["protocol"]
        is_iscsi = name.lower().startswith(("iqn.", "eui.", "naa."))
        is_fc = bool(_WWPN_RE.match(name))
        if not (is_iscsi or is_fc) or (proto == "iscsi" and not is_iscsi) or (
                proto == "fcp" and not is_fc):
            raise _err(400, f'Initiator "{name}" is not valid for an igroup of protocol '
                            f'"{proto}".', "1254324")
        for g in self.igroups.values():
            if g is group or g["svm"]["name"] != group["svm"]["name"]:
                continue
            if name.lower() in (i["name"].lower() for i in g["initiators"]) and \
                    g["os_type"] != group["os_type"]:
                raise _err(400, f'Initiator "{name}" is in igroup "{g["name"]}" with a '
                                "different os_type.", "1254323")
        if name.lower() in (i["name"].lower() for i in group["initiators"]):
            raise _err(409, f'Initiator "{name}" is already in igroup "{group["name"]}".',
                       "1254327")

    @route("GET", "/api/protocols/san/igroups")
    def get_igroups(self, req):
        self._auth(req)
        return self._collection(req, list(self.igroups.values()))

    @route("POST", "/api/protocols/san/igroups")
    def post_igroup(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        name = body.get("name", "")
        if not _IGROUP_RE.match(name):
            raise _err(400, f'Invalid igroup name "{name}".', "5374737")
        if any(g["name"] == name and g["svm"]["name"] == svm["name"]
               for g in self.igroups.values()):
            raise _err(409, f'An igroup named "{name}" already exists.', "5374734")
        proto = body.get("protocol", "mixed")
        if proto not in ("iscsi", "fcp", "mixed"):
            raise _err(400, f'Invalid protocol "{proto}".', "2")
        gid = str(uuid_mod.uuid4())
        group = {"uuid": gid, "name": name, "svm": svm, "protocol": proto,
                 "os_type": body.get("os_type", "linux"), "initiators": []}
        for i in body.get("initiators") or []:
            self._check_initiator(group, i["name"])
            group["initiators"].append({"name": i["name"]})
        self.igroups[gid] = group
        return self._created(req, group, f"/api/protocols/san/igroups/{gid}")

    @route("PATCH", "/api/protocols/san/igroups/" + _U)
    def patch_igroup(self, req, uuid):
        self._auth(req)
        group = self.igroups.get(uuid)
        if group is None:
            raise _not_found()
        proto = (req.json() or {}).get("protocol")
        if proto is not None:
            if proto not in ("iscsi", "fcp", "mixed"):
                raise _err(400, f'Invalid protocol "{proto}".', "2")
            group["protocol"] = proto
        return Reply(200, {})

    @route("POST", "/api/protocols/san/igroups/" + _U + "/initiators")
    def post_igroup_initiators(self, req, uuid):
        self._auth(req)
        group = self.igroups.get(uuid)
        if group is None:
            raise _not_found()
        body = req.json() or {}
        records = body.get("records") or [body]
        for r in records:
            self._check_initiator(group, r["name"])
        for r in records:
            group["initiators"].append({"name": r["name"]})
        return Reply(201, {})

    @route("DELETE", "/api/protocols/san/igroups/" + _U)
    def delete_igroup(self, req, uuid):
        self._auth(req)
        if uuid not in self.igroups:
            raise _not_found()
        if any(m["igroup"]["uuid"] == uuid for m in self.lun_maps) and \
                req.q("allow_delete_while_mapped") != "true":
            raise _err(400, "The igroup is mapped to one or more LUNs.", "5374852")
        del self.igroups[uuid]
        return Reply(200, {})

    # -- lun-maps ----------------------------------------------------------------------

    @route("GET", "/api/protocols/san/lun-maps")
    def get_lun_maps(self, req):
        self._auth(req)
        return self._collection(req, self.lun_maps, keys=("svm", "lun", "igroup"))

    def _find(self, table: dict[str, dict[str, Any]], ref: dict[str, Any], svm: str,
              what: str) -> dict[str, Any]:
        for obj in table.values():
            if obj["svm"]["name"] != svm:
                continue
            if ref.get("uuid") == obj["uuid"] or (ref.get("name") and
                                                  ref.get("name") == obj["name"]):
                return obj
        raise _err(404, f'{what} "{ref.get("name") or ref.get("uuid")}" does not exist.', "4")

    @route("POST", "/api/protocols/san/lun-maps")
    def post_lun_map(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        lun = self._find(self.luns, body.get("lun") or {}, svm["name"], "LUN")
        group = self._find(self.igroups, body.get("igroup") or {}, svm["name"], "igroup")
        if any(m["lun"]["uuid"] == lun["uuid"] and m["igroup"]["uuid"] == group["uuid"]
               for m in self.lun_maps):
            raise _err(409, "The LUN is already mapped to this igroup.", "5374922")
        used = {m["logical_unit_number"] for m in self.lun_maps
                if m["igroup"]["uuid"] == group["uuid"]}
        number = body.get("logical_unit_number")
        if number is None:
            number = next(n for n in range(4096) if n not in used)
        elif number in used:
            raise _err(409, f"LUN number {number} is in use in this igroup.", "5374921")
        m = {"svm": svm, "lun": {"uuid": lun["uuid"], "name": lun["name"]},
             "igroup": {"uuid": group["uuid"], "name": group["name"]},
             "logical_unit_number": number}
        self.lun_maps.append(m)
        lun["status"]["mapped"] = True
        return self._created(req, m, f"/api/protocols/san/lun-maps/{lun['uuid']}/"
                                     f"{group['uuid']}")

    @route("DELETE", "/api/protocols/san/lun-maps/" + r"(?P<lun>[0-9a-fA-F-]{36})/"
           + r"(?P<igroup>[0-9a-fA-F-]{36})")
    def delete_lun_map(self, req, lun, igroup):
        self._auth(req)
        before = len(self.lun_maps)
        self.lun_maps = [m for m in self.lun_maps
                         if not (m["lun"]["uuid"] == lun and m["igroup"]["uuid"] == igroup)]
        if len(self.lun_maps) == before:
            raise _not_found()
        if lun in self.luns:
            self.luns[lun]["status"]["mapped"] = any(
                m["lun"]["uuid"] == lun for m in self.lun_maps)
        return Reply(200, {})

    # -- NVMe subsystems -----------------------------------------------------------------

    @route("GET", "/api/protocols/nvme/subsystems")
    def get_subsystems(self, req):
        self._auth(req)
        return self._collection(req, list(self.subsystems.values()))

    @staticmethod
    def _check_nqn(nqn: str) -> None:
        if not nqn.startswith("nqn."):
            raise _err(400, f'Invalid host NQN "{nqn}".', "72089705")

    @route("POST", "/api/protocols/nvme/subsystems")
    def post_subsystem(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        name = body.get("name", "")
        if not _SUBSYS_RE.match(name):
            raise _err(400, f'Invalid subsystem name "{name}".', "72089771")
        if not body.get("os_type"):
            raise _err(400, 'Field "os_type" is required.', "2")
        if any(s["name"] == name and s["svm"]["name"] == svm["name"]
               for s in self.subsystems.values()):
            raise _err(409, f'Subsystem "{name}" already exists.', "72089709")
        hosts = []
        for h in body.get("hosts") or []:
            self._check_nqn(h["nqn"])
            hosts.append({"nqn": h["nqn"]})
        sid = str(uuid_mod.uuid4())
        self.subsystems[sid] = {"uuid": sid, "name": name, "svm": svm,
                                "os_type": body["os_type"], "comment": body.get("comment", ""),
                                "hosts": hosts,
                                "target_nqn": f"nqn.1992-08.com.netapp:sn.{sid.replace('-', '')}"
                                              f":subsystem.{name}"}
        return self._created(req, self.subsystems[sid], f"/api/protocols/nvme/subsystems/{sid}")

    @route("POST", "/api/protocols/nvme/subsystems/" + _U + "/hosts")
    def post_subsystem_hosts(self, req, uuid):
        self._auth(req)
        sub = self.subsystems.get(uuid)
        if sub is None:
            raise _not_found()
        body = req.json() or {}
        records = body.get("records") or [body]
        have = {h["nqn"] for h in sub["hosts"]}
        for r in records:
            self._check_nqn(r["nqn"])
            if r["nqn"] in have:
                raise _err(409, f'Host "{r["nqn"]}" is already in subsystem "{sub["name"]}".',
                           "72089708")
        sub["hosts"].extend({"nqn": r["nqn"]} for r in records)
        return Reply(201, {})

    @route("DELETE", "/api/protocols/nvme/subsystems/" + _U + "/hosts/(?P<nqn>[^/]+)")
    def delete_subsystem_host(self, req, uuid, nqn):
        self._auth(req)
        sub = self.subsystems.get(uuid)
        if sub is None:
            raise _not_found()
        nqn = unquote(nqn)
        if nqn not in {h["nqn"] for h in sub["hosts"]}:
            raise _not_found()
        sub["hosts"] = [h for h in sub["hosts"] if h["nqn"] != nqn]
        return Reply(200, {})

    @route("DELETE", "/api/protocols/nvme/subsystems/" + _U)
    def delete_subsystem(self, req, uuid):
        self._auth(req)
        sub = self.subsystems.get(uuid)
        if sub is None:
            raise _not_found()
        if any(m["subsystem"]["uuid"] == uuid for m in self.subsystem_maps) and \
                req.q("allow_delete_while_mapped") != "true":
            raise _err(400, "The subsystem has mapped namespaces.", "72090025")
        if sub["hosts"] and req.q("allow_delete_with_hosts") != "true":
            raise _err(400, "The subsystem has hosts; set allow_delete_with_hosts.",
                       "72090026")
        for m in [m for m in self.subsystem_maps if m["subsystem"]["uuid"] == uuid]:
            self.namespaces[m["namespace"]["uuid"]]["status"]["mapped"] = False
        self.subsystem_maps = [m for m in self.subsystem_maps if m["subsystem"]["uuid"] != uuid]
        del self.subsystems[uuid]
        return Reply(200, {})

    # -- subsystem-maps --------------------------------------------------------------------

    @route("GET", "/api/protocols/nvme/subsystem-maps")
    def get_subsystem_maps(self, req):
        self._auth(req)
        return self._collection(req, self.subsystem_maps, keys=("svm", "namespace", "subsystem"))

    @route("POST", "/api/protocols/nvme/subsystem-maps")
    def post_subsystem_map(self, req):
        self._auth(req)
        body = req.json() or {}
        svm = self._svm_of(body)
        ns = self._find(self.namespaces, body.get("namespace") or {}, svm["name"], "Namespace")
        sub = self._find(self.subsystems, body.get("subsystem") or {}, svm["name"],
                         "Subsystem")
        if any(m["namespace"]["uuid"] == ns["uuid"] for m in self.subsystem_maps):
            raise _err(400, "The namespace is already mapped to a subsystem. A namespace "
                            "can be mapped to only one subsystem.", "72089771")
        used = {int(m["nsid"].rstrip("h"), 16) for m in self.subsystem_maps
                if m["subsystem"]["uuid"] == sub["uuid"]}
        nsid = next(n for n in range(1, 4096) if n not in used)
        m = {"svm": svm, "namespace": {"uuid": ns["uuid"], "name": ns["name"]},
             "subsystem": {"uuid": sub["uuid"], "name": sub["name"]},
             "nsid": f"{nsid:08x}h"}
        self.subsystem_maps.append(m)
        ns["status"]["mapped"] = True
        return self._created(req, m, f"/api/protocols/nvme/subsystem-maps/{sub['uuid']}/"
                                     f"{ns['uuid']}")

    @route("DELETE", "/api/protocols/nvme/subsystem-maps/" + r"(?P<sub>[0-9a-fA-F-]{36})/"
           + r"(?P<ns>[0-9a-fA-F-]{36})")
    def delete_subsystem_map(self, req, sub, ns):
        self._auth(req)
        before = len(self.subsystem_maps)
        self.subsystem_maps = [m for m in self.subsystem_maps
                               if not (m["subsystem"]["uuid"] == sub
                                       and m["namespace"]["uuid"] == ns)]
        if len(self.subsystem_maps) == before:
            raise _not_found()
        if ns in self.namespaces:
            self.namespaces[ns]["status"]["mapped"] = False
        return Reply(200, {})

    def not_routed(self, req: Request) -> Reply:
        return Reply(404, {"error": {"message": f"API not found: {req.method} {req.path}",
                                     "code": "3"}})


def contract_setup(**options: Any) -> ContractSetup:
    fake = FakeNetAppOntap(username="admin", password="netapp1!", svm="svm1")
    return ContractSetup(
        fake=fake,
        config=make_config("netapp-ontap", "https://ontap.fake.example",
                           **{"svm": "svm1", **options}),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("netapp1!")}),
        bad_credential=Credential({"username": SecretStr("admin"),
                                   "password": SecretStr("wrong")}),
        # Default protocol_family is scsi: the contract maps every volume to
        # hosts of each listed protocol, so only SCSI initiators are listed.
        # NVMe is covered by tests/unit/storage_arrays/test_netapp_ontap.py.
        initiators={
            Protocol.ISCSI: (
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-a"),),
                (Initiator(protocol=Protocol.ISCSI, address="iqn.2026-01.io.sb:host-b"),),
            ),
            Protocol.FC: (
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:FF:4C:1A:2B"),),
                (Initiator(protocol=Protocol.FC, address="21:00:00:24:ff:4c:1a:2c"),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_foreign_lun(name, size),
    )
