"""Fake Hitachi VSP Configuration Manager REST API (the subset the adapter uses).

Reproduces: both path layouts (embedded ``/ConfigurationManager/v1/objects``
and server ``.../objects/storages/{storageDeviceId}``), basic-auth sessions
with ``Authorization: Session <token>``, ``202`` + job objects for every
change with the change applied only when the job runs (after ``job_polls``
"Running" polls), job failures carrying the Hitachi error body, LDEVs that
read back as ``NOT DEFINED`` after deletion, per-port host groups with
default group 0, FC WWN uniqueness per port (iSCSI names may repeat across
targets, as on the array), LU paths listed on the LDEV, the resource-group
lock, and ``{"errorSource", "message", "solution", "messageId"}`` errors.
"""
from __future__ import annotations

import re
import secrets
from collections.abc import Callable
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_ROOT = "/ConfigurationManager/v1/objects"
_P = _ROOT + r"(?:/storages/(?P<sdid>\d{12}))?"
_MAX_LDEV = 65279
NOT_DEFINED = "NOT DEFINED"
_UNITS = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}


def _body(msg: str, msg_id: str, source: str = "", solution: str = "") -> dict[str, Any]:
    return {"errorSource": source, "message": msg, "solution": solution or
            "Check the request and the storage system configuration.",
            "messageId": msg_id}


def _err(status: int, msg: str, msg_id: str = "KART40040-E", source: str = "") -> FakeError:
    return FakeError(status, _body(msg, msg_id, source))


def _storage_err(msg: str, ssb1: str = "2E11", ssb2: str = "0001") -> FakeError:
    """A failure inside the storage system: in a job's ``error``."""
    body = _body(msg, "KART30000-E")
    body["errorCode"] = {"SSB1": ssb1, "SSB2": ssb2}
    return FakeError(400, body)


class FakeHitachiVsp(FakeArray):
    vendor = "hitachi-vsp"

    def __init__(self, username: str = "sbadmin", password: str = "vsp-secret",
                 serial: int = 451234, model: str = "VSP E990",
                 ports: dict[str, str] | None = None, pools: tuple[int, ...] = (0,)):
        super().__init__()
        self.username, self.password = username, password
        self.serial = serial
        self.model = model
        self.storage_device_id = f"{938000000000 + serial}"
        self.sessions: dict[str, int] = {}           # token -> sessionId
        self.pools = set(pools)
        self.ldevs: dict[int, dict[str, Any]] = {}   # defined LDEVs only
        self.ports = ports or {"CL1-A": "FIBRE", "CL2-A": "FIBRE",
                               "CL1-B": "ISCSI", "CL2-B": "ISCSI"}
        #: (port, number) -> {"name", "mode", "wwns": [], "iscsis": [], "luns": {lun: ldev}}
        self.groups: dict[tuple[str, int], dict[str, Any]] = {}
        for port in self.ports:
            self.groups[(port, 0)] = self._group(f"{port[2:].replace('-', '')}-G00",
                                                 "LINUX/IRIX")
        self.jobs: dict[int, dict[str, Any]] = {}
        self._pending: dict[int, Callable[[], list[str]]] = {}
        self._polls_left: dict[int, int] = {}
        #: "Running" answers before a job executes.
        self.job_polls = 1
        #: Model requires the resource-group lock for changes.
        self.require_lock = False
        self.lock_owner: str | None = None
        self.max_count = 16384

    @staticmethod
    def _group(name: str, mode: str) -> dict[str, Any]:
        return {"name": name, "mode": mode, "modeOptions": [], "wwns": [], "iscsis": [],
                "luns": {}}

    # -- fake controls ------------------------------------------------------

    def expire_sessions(self) -> None:
        self.sessions.clear()
        self.lock_owner = None

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields["password"]

    def add_ldev(self, label: str, size: int, ldev_id: int | None = None) -> str:
        lid = ldev_id if ldev_id is not None else self._free_ldev(0, _MAX_LDEV)
        self.ldevs[lid] = {"pool": min(self.pools), "blocks": size // 512, "label": label}
        return str(lid)

    def _free_ldev(self, start: int, end: int) -> int:
        lid = next((i for i in range(start, end + 1) if i not in self.ldevs), None)
        if lid is None:
            raise _storage_err("No free LDEV number in the specified range.", "2E11", "0012")
        return lid

    def naa(self, lid: int) -> str:
        s = f"{self.serial:06X}"
        return f"60060E80{s}0030{s}0000{lid:04X}"

    # -- auth / prefix -------------------------------------------------------

    def _prefix(self, sdid: str | None) -> str:
        if sdid is None:
            return _ROOT
        if sdid != self.storage_device_id:
            raise _err(404, f"The storage system {sdid} is not registered.", "KART40053-E")
        return f"{_ROOT}/storages/{sdid}"

    def _token(self, req: Request) -> str:
        header = req.headers.get("Authorization", "")
        token = header[8:] if header.startswith("Session ") else ""
        if token not in self.sessions:
            raise _err(401, "The session is invalid or has timed out.", "KART40047-E",
                       req.path)
        return token

    def _auth(self, req: Request, sdid: str | None) -> str:
        self._prefix(sdid)
        return self._token(req)

    @route("POST", _P + "/sessions")
    def post_session(self, req, sdid):
        self._prefix(sdid)
        if req.basic_auth() != (self.username, self.password):
            raise _err(401, "Authentication failed.", "KART40046-E", req.path)
        token = secrets.token_hex(16)
        self.sessions[token] = self.next_id()
        return Reply(200, {"token": token, "sessionId": self.sessions[token]})

    @route("DELETE", _P + r"/sessions/(?P<sid>\d+)")
    def delete_session(self, req, sdid, sid):
        token = self._auth(req, sdid)
        if self.sessions.get(token) != int(sid):
            raise _err(404, f"Session {sid} does not exist.", "KART40050-E")
        del self.sessions[token]
        if self.lock_owner == token:
            self.lock_owner = None
        return Reply(200, {"sessionId": int(sid)})

    @route("GET", _ROOT + r"/storages/(?P<which>instance|\d{12})")
    def get_storage(self, req, which):
        self._auth(req, None if which == "instance" else which)
        return Reply(200, {"storageDeviceId": self.storage_device_id, "model": self.model,
                           "serialNumber": self.serial, "svpIp": "192.0.2.10",
                           "dkcMicroVersion": "93-07-23-80/00", "communicationModes": [
                               {"communicationMode": "lanConnectionMode"}]})

    # -- jobs -----------------------------------------------------------------

    def _job(self, req: Request, sdid: str | None, token: str,
             execute: Callable[[], list[str]], *, lock_request: bool = False) -> Reply:
        if not lock_request:
            if self.lock_owner and self.lock_owner != token:
                raise _err(409, "The resource group is locked by another user.",
                           "KART40049-E")
            if self.require_lock and self.lock_owner != token:
                raise _storage_err("The resource group is not locked.", "2E10", "A001")
        jid = self.next_id()
        prefix = self._prefix(sdid)
        self.jobs[jid] = {
            "jobId": jid, "self": f"{prefix}/jobs/{jid}", "userId": self.username,
            "status": "Initializing", "state": "Queued",
            "createdTime": "2026-11-20T10:00:00Z", "updatedTime": "2026-11-20T10:00:00Z",
            "request": {"requestUrl": f"https://vsp.fake{req.path}",
                        "requestMethod": req.method,
                        "requestBody": req.body.decode() if req.body else ""},
            "affectedResources": [],
        }
        self._pending[jid] = execute
        self._polls_left[jid] = self.job_polls
        return Reply(202, dict(self.jobs[jid]))

    @route("GET", _P + r"/jobs/(?P<jid>\d+)")
    def get_job(self, req, sdid, jid):
        self._auth(req, sdid)
        job = self.jobs.get(int(jid))
        if job is None:
            raise _err(404, f"Job {jid} does not exist.", "KART40050-E")
        jid_i = int(jid)
        if jid_i in self._pending:
            if self._polls_left[jid_i] > 0:
                self._polls_left[jid_i] -= 1
                job.update(status="Running", state="Started")
            else:
                execute = self._pending.pop(jid_i)
                try:
                    job.update(status="Completed", state="Succeeded",
                               affectedResources=execute(),
                               completedTime="2026-11-20T10:00:05Z")
                except FakeError as e:
                    job.update(status="Completed", state="Failed",
                               error=e.reply.body, completedTime="2026-11-20T10:00:05Z")
        return Reply(200, dict(job))

    # -- resource-group lock -------------------------------------------------------

    @route("POST", _P + r"/services/resource-group-service/actions/(?P<act>lock|unlock)/invoke")
    def rg_lock(self, req, sdid, act):
        token = self._auth(req, sdid)
        prefix = self._prefix(sdid)

        def run() -> list[str]:
            if act == "lock":
                if self.lock_owner not in (None, token):
                    raise _storage_err("The resource group is locked by another user.",
                                       "2E10", "A002")
                self.lock_owner = token
            elif self.lock_owner == token:
                self.lock_owner = None
            return [f"{prefix}/services/resource-group-service"]

        return self._job(req, sdid, token, run, lock_request=True)

    # -- LDEVs ------------------------------------------------------------------

    def _ldev_view(self, lid: int) -> dict[str, Any]:
        ldev = self.ldevs.get(lid)
        if ldev is None:
            return {"ldevId": lid, "emulationType": NOT_DEFINED, "ssid": "0004",
                    "resourceGroupId": 0}
        paths = [{"portId": port, "hostGroupNumber": num, "hostGroupName": g["name"],
                  "lun": lun}
                 for (port, num), g in sorted(self.groups.items())
                 for lun, target in sorted(g["luns"].items()) if target == lid]
        view = {
            "ldevId": lid, "clprId": 0, "emulationType": "OPEN-V-CVS",
            "byteFormatCapacity": f"{ldev['blocks'] * 512 / (1 << 30):.2f} G",
            "blockCapacity": ldev["blocks"], "numOfPorts": len(paths),
            "attributes": ["CVS", "HDP"], "label": ldev["label"], "status": "NML",
            "mpBladeId": 0, "ssid": "0004", "poolId": ldev["pool"],
            "numOfUsedBlock": 0, "isFullAllocationEnabled": False,
            "resourceGroupId": 0, "dataReductionStatus": "DISABLED",
            "dataReductionMode": "disabled", "isAluaEnabled": False,
            "naaId": self.naa(lid),
        }
        if paths:
            view["ports"] = paths
        return view

    def _ldev_id(self, raw: str) -> int:
        lid = int(raw)
        if lid > _MAX_LDEV:
            raise _err(400, f"The value of ldevId ({lid}) is out of range.", "KART40042-E")
        return lid

    @route("GET", _P + r"/ldevs/(?P<lid>\d+)")
    def get_ldev(self, req, sdid, lid):
        self._auth(req, sdid)
        return Reply(200, self._ldev_view(self._ldev_id(lid)))

    @route("GET", _P + r"/ldevs")
    def list_ldevs(self, req, sdid):
        self._auth(req, sdid)
        option = req.q("ldevOption")
        pool = req.q("poolId")
        if pool is not None and option != "dpVolume":
            raise _err(400, "poolId can be specified only with ldevOption=dpVolume.",
                       "KART40044-E")
        if option not in (None, "defined", "dpVolume"):
            raise _err(400, f"fake: ldevOption={option} not implemented", "KART40044-E")
        count = int(req.q("count", "100") or 100)
        if not 1 <= count <= self.max_count:
            raise _err(400, f"The value of count ({count}) is out of range.", "KART40042-E")
        head = int(req.q("headLdevId", "0") or 0)
        data = [self._ldev_view(lid) for lid in sorted(self.ldevs)
                if lid >= head and (pool is None or self.ldevs[lid]["pool"] == int(pool))]
        return Reply(200, {"data": data[:count]})

    @staticmethod
    def _blocks(body: dict[str, Any]) -> int:
        if "blockCapacity" in body:
            return int(body["blockCapacity"])
        m = re.match(r"^(\d+)([KMGT])$", str(body.get("byteFormatCapacity", "")))
        if not m:
            raise _err(400, "Specify blockCapacity or byteFormatCapacity.", "KART40041-E")
        return int(m.group(1)) * _UNITS[m.group(2)] // 512

    @route("POST", _P + r"/ldevs")
    def post_ldev(self, req, sdid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        prefix = self._prefix(sdid)
        if int(body.get("poolId", -1)) not in self.pools:
            raise _err(400, f"The specified pool ({body.get('poolId')}) does not exist.",
                       "KART40045-E")
        blocks = self._blocks(body)
        if blocks <= 0:
            raise _err(400, "The capacity is invalid.", "KART40042-E")
        if ("startLdevId" in body) != ("endLdevId" in body):
            raise _err(400, "Specify both startLdevId and endLdevId.", "KART40041-E")

        def run() -> list[str]:
            if "ldevId" in body:
                lid = int(body["ldevId"])
                if lid in self.ldevs:
                    raise _storage_err(f"LDEV {lid} is already defined.", "2E11", "0011")
            else:
                lid = self._free_ldev(int(body.get("startLdevId", 0)),
                                      int(body.get("endLdevId", _MAX_LDEV)))
            self.ldevs[lid] = {"pool": int(body["poolId"]), "blocks": blocks, "label": ""}
            return [f"{prefix}/ldevs/{lid}"]

        return self._job(req, sdid, token, run)

    @route("PUT", _P + r"/ldevs/(?P<lid>\d+)")
    def put_ldev(self, req, sdid, lid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        lid_i = self._ldev_id(lid)
        prefix = self._prefix(sdid)
        label = body.get("label")
        if label is None:
            raise _err(400, "fake: only label changes are implemented", "KART40041-E")
        if len(label) > 32:
            raise _err(400, "The label must be 32 characters or fewer.", "KART40042-E")

        def run() -> list[str]:
            if lid_i not in self.ldevs:
                raise _storage_err(f"LDEV {lid_i} is not defined.", "2E10", "0000")
            self.ldevs[lid_i]["label"] = label
            return [f"{prefix}/ldevs/{lid_i}"]

        return self._job(req, sdid, token, run)

    @route("POST", _P + r"/ldevs/(?P<lid>\d+)/actions/expand/invoke")
    def expand_ldev(self, req, sdid, lid):
        token = self._auth(req, sdid)
        lid_i = self._ldev_id(lid)
        params = (req.json() or {}).get("parameters") or {}
        prefix = self._prefix(sdid)
        add = int(params.get("additionalBlockCapacity", 0))
        if add <= 0:
            raise _err(400, "additionalBlockCapacity must be positive.", "KART40042-E")

        def run() -> list[str]:
            if lid_i not in self.ldevs:
                raise _storage_err(f"LDEV {lid_i} is not defined.", "2E10", "0000")
            self.ldevs[lid_i]["blocks"] += add
            return [f"{prefix}/ldevs/{lid_i}"]

        return self._job(req, sdid, token, run)

    def _paths_of(self, lid: int) -> int:
        return sum(1 for g in self.groups.values() for t in g["luns"].values() if t == lid)

    @route("DELETE", _P + r"/ldevs/(?P<lid>\d+)")
    def delete_ldev(self, req, sdid, lid):
        token = self._auth(req, sdid)
        lid_i = self._ldev_id(lid)
        prefix = self._prefix(sdid)

        def run() -> list[str]:
            if lid_i not in self.ldevs:
                raise _storage_err(f"LDEV {lid_i} is not defined.", "2E10", "0000")
            if self._paths_of(lid_i):
                raise _storage_err(f"LU paths are defined for LDEV {lid_i}.", "2E11", "0007")
            del self.ldevs[lid_i]
            return [f"{prefix}/ldevs/{lid_i}"]

        return self._job(req, sdid, token, run)

    # -- ports and host groups -------------------------------------------------------

    def _port(self, port: str | None) -> str:
        if port not in self.ports:
            raise _err(404, f"The specified port ({port}) does not exist.", "KART40050-E")
        return port  # type: ignore[return-value]

    @route("GET", _P + r"/ports/(?P<port>[^/]+)")
    def get_port(self, req, sdid, port):
        self._auth(req, sdid)
        self._port(port)
        info: dict[str, Any] = {"portId": port, "portType": self.ports[port],
                                "portAttributes": ["TAR"], "portSpeed": "AUT",
                                "lunSecuritySetting": True}
        if self.ports[port] == "FIBRE":
            info.update(loopId="EF", fabricMode=True, portConnection="PtoP",
                        wwn=f"50060e80{self.serial:06x}{list(self.ports).index(port):02x}")
        else:
            info.update(ipv4Address="192.0.2.20", tcpPort="3260")
        return Reply(200, info)

    def _group_view(self, port: str, num: int) -> dict[str, Any]:
        g = self.groups[(port, num)]
        view: dict[str, Any] = {"hostGroupId": f"{port},{num}", "portId": port,
                                "hostGroupNumber": num, "hostGroupName": g["name"],
                                "hostMode": g["mode"], "resourceGroupId": 0}
        if g["modeOptions"]:
            view["hostModeOptions"] = list(g["modeOptions"])
        if self.ports[port] == "ISCSI":
            view["iscsiName"] = f"iqn.1994-04.jp.co.hitachi:rsd.h8m.t.{self.serial:05d}." \
                                f"{port[2:].replace('-', '')}{num:03x}"
        return view

    def _existing_group(self, port: str | None, num: Any) -> tuple[str, int]:
        port = self._port(port)
        try:
            key = (port, int(num))
        except (TypeError, ValueError) as e:
            raise _err(400, "hostGroupNumber is required.", "KART40041-E") from e
        if key not in self.groups:
            raise _err(404, f"Host group {port},{num} does not exist.", "KART40050-E")
        return key

    @route("GET", _P + r"/host-groups")
    def list_groups(self, req, sdid):
        self._auth(req, sdid)
        port = self._port(req.q("portId"))
        return Reply(200, {"data": [self._group_view(p, n) for (p, n) in sorted(self.groups)
                                    if p == port]})

    @route("POST", _P + r"/host-groups")
    def post_group(self, req, sdid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        port = self._port(body.get("portId"))
        name = str(body.get("hostGroupName") or "")
        prefix = self._prefix(sdid)
        limit = 32 if self.ports[port] == "ISCSI" else 64
        if not name or len(name) > limit:
            raise _err(400, f"hostGroupName must be 1-{limit} characters.", "KART40042-E")
        mode = body.get("hostMode", "LINUX/IRIX")
        if mode not in ("LINUX/IRIX", "VMWARE_EX", "WIN_EX", "AIX", "HP-UX", "SOLARIS"):
            raise _err(400, f"The hostMode ({mode}) is invalid.", "KART40042-E")

        def run() -> list[str]:
            if any(p == port and g["name"] == name for (p, _), g in self.groups.items()):
                raise _storage_err(f"The host group name {name} is already used on {port}.",
                                   "2E11", "4E00")
            num = body.get("hostGroupNumber")
            if num is None:
                num = next(n for n in range(1, 255) if (port, n) not in self.groups)
            self.groups[(port, int(num))] = self._group(name, mode)
            self.groups[(port, int(num))]["modeOptions"] = list(body.get("hostModeOptions", []))
            return [f"{prefix}/host-groups/{port},{num}"]

        return self._job(req, sdid, token, run)

    @route("DELETE", _P + r"/host-groups/(?P<port>[^/,]+),(?P<num>\d+)")
    def delete_group(self, req, sdid, port, num):
        token = self._auth(req, sdid)
        key = self._existing_group(port, num)
        prefix = self._prefix(sdid)
        if key[1] == 0:
            raise _err(400, "Host group 0 cannot be deleted.", "KART40042-E")

        def run() -> list[str]:
            if self.groups[key]["luns"]:
                raise _storage_err(f"LU paths are defined in host group {port},{num}.",
                                   "2E11", "0007")
            del self.groups[key]
            return [f"{prefix}/host-groups/{port},{num}"]

        return self._job(req, sdid, token, run)

    # -- initiators --------------------------------------------------------------------

    @route("GET", _P + r"/host-wwns")
    def list_wwns(self, req, sdid):
        self._auth(req, sdid)
        port, num = self._existing_group(req.q("portId"), req.q("hostGroupNumber"))
        g = self.groups[(port, num)]
        return Reply(200, {"data": [
            {"hostWwnId": f"{port},{num},{w}", "portId": port, "hostGroupNumber": num,
             "hostGroupName": g["name"], "hostWwn": w, "wwnNickname": "-"}
            for w in g["wwns"]]})

    @route("POST", _P + r"/host-wwns")
    def post_wwn(self, req, sdid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        port, num = self._existing_group(body.get("portId"), body.get("hostGroupNumber"))
        wwn = str(body.get("hostWwn", "")).lower()
        prefix = self._prefix(sdid)
        if self.ports[port] != "FIBRE":
            raise _err(400, f"Port {port} is not a Fibre Channel port.", "KART40042-E")
        if not re.match(r"^[0-9a-f]{16}$", wwn):
            raise _err(400, f"The hostWwn ({wwn}) is invalid.", "KART40042-E")

        def run() -> list[str]:
            for (p, n), g in self.groups.items():
                if p == port and wwn in g["wwns"]:
                    raise _storage_err(
                        f"The WWN {wwn} is already registered in host group {p},{n}.",
                        "B957", "4184")
            self.groups[(port, num)]["wwns"].append(wwn)
            return [f"{prefix}/host-wwns/{port},{num},{wwn}"]

        return self._job(req, sdid, token, run)

    @route("GET", _P + r"/host-iscsis")
    def list_iscsis(self, req, sdid):
        self._auth(req, sdid)
        port, num = self._existing_group(req.q("portId"), req.q("hostGroupNumber"))
        g = self.groups[(port, num)]
        return Reply(200, {"data": [
            {"hostIscsiId": f"{port},{num},{n}", "portId": port, "hostGroupNumber": num,
             "hostGroupName": g["name"], "iscsiName": n, "iscsiNickname": "-"}
            for n in g["iscsis"]]})

    @route("POST", _P + r"/host-iscsis")
    def post_iscsi(self, req, sdid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        port, num = self._existing_group(body.get("portId"), body.get("hostGroupNumber"))
        name = str(body.get("iscsiName", ""))
        prefix = self._prefix(sdid)
        if self.ports[port] != "ISCSI":
            raise _err(400, f"Port {port} is not an iSCSI port.", "KART40042-E")
        if not name.startswith(("iqn.", "eui.")):
            raise _err(400, f"The iscsiName ({name}) is invalid.", "KART40042-E")

        def run() -> list[str]:
            g = self.groups[(port, num)]
            # The same initiator may log in to several targets of a port; only
            # a duplicate within one target is refused.
            if name.lower() in (n.lower() for n in g["iscsis"]):
                raise _storage_err(f"The iSCSI name {name} is already registered.",
                                   "B957", "4185")
            g["iscsis"].append(name)
            return [f"{prefix}/host-iscsis/{port},{num},{name}"]

        return self._job(req, sdid, token, run)

    # -- LU paths ---------------------------------------------------------------------

    @route("GET", _P + r"/luns")
    def list_luns(self, req, sdid):
        self._auth(req, sdid)
        port, num = self._existing_group(req.q("portId"), req.q("hostGroupNumber"))
        g = self.groups[(port, num)]
        return Reply(200, {"data": [
            {"lunId": f"{port},{num},{lun}", "portId": port, "hostGroupNumber": num,
             "hostMode": g["mode"], "lun": lun, "ldevId": lid, "isCommandDevice": False,
             "luHostReserve": {"openSystem": False, "persistent": False,
                               "pgrKey": False, "mainframe": False, "acaReserve": False}}
            for lun, lid in sorted(g["luns"].items())]})

    @route("POST", _P + r"/luns")
    def post_lun(self, req, sdid):
        token = self._auth(req, sdid)
        body = req.json() or {}
        port, num = self._existing_group(body.get("portId"), body.get("hostGroupNumber"))
        lid = int(body.get("ldevId", -1))
        prefix = self._prefix(sdid)
        lun = body.get("lun")
        if lun is not None and not 0 <= int(lun) <= 2047:
            raise _err(400, f"The value of lun ({lun}) is out of range.", "KART40042-E")

        def run() -> list[str]:
            g = self.groups[(port, num)]
            if lid not in self.ldevs:
                raise _storage_err(f"LDEV {lid} is not defined.", "2E10", "0000")
            if lid in g["luns"].values():
                raise _storage_err(f"LDEV {lid} is already mapped in host group "
                                   f"{port},{num}.", "B958", "0233")
            n = int(lun) if lun is not None else next(
                i for i in range(2048) if i not in g["luns"])
            if n in g["luns"]:
                raise _storage_err(f"LUN {n} is already in use in host group "
                                   f"{port},{num}.", "B958", "0234")
            g["luns"][n] = lid
            return [f"{prefix}/luns/{port},{num},{n}"]

        return self._job(req, sdid, token, run)

    @route("DELETE", _P + r"/luns/(?P<port>[^/,]+),(?P<num>\d+),(?P<lun>\d+)")
    def delete_lun(self, req, sdid, port, num, lun):
        token = self._auth(req, sdid)
        key = self._existing_group(port, num)
        prefix = self._prefix(sdid)
        if int(lun) not in self.groups[key]["luns"]:
            raise _err(404, f"LU path {port},{num},{lun} does not exist.", "KART40050-E")

        def run() -> list[str]:
            self.groups[key]["luns"].pop(int(lun), None)
            return [f"{prefix}/luns/{port},{num},{lun}"]

        return self._job(req, sdid, token, run)


def contract_setup(**options: Any) -> ContractSetup:
    fake = FakeHitachiVsp(username="sbadmin", password="vsp-secret")
    opts: dict[str, Any] = {"pool_id": 0, "ports": ["CL1-A", "CL2-A", "CL1-B", "CL2-B"]}
    opts.update(options)
    return ContractSetup(
        fake=fake,
        config=make_config("hitachi-vsp", "https://vsp.fake.example", **opts),
        credential=Credential({"username": SecretStr("sbadmin"),
                               "password": SecretStr("vsp-secret")}),
        bad_credential=Credential({"username": SecretStr("sbadmin"),
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
        create_foreign_volume=lambda name, size: fake.add_ldev(name, size),
    )
