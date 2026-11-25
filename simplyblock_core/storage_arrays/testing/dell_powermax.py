"""Fake Dell PowerMax Unisphere REST API 10.x (the subset the adapter uses).

Quirks reproduced: basic auth on every request (401 plain-text body);
``{"message": "..."}`` error bodies, 400 for most failures, 404 for missing
objects ("Could not find volume", "... cannot be found"), 409 for "already
exists", 500 for a masking view naming a missing port group; NVMe host
initiators as ``<host NQN>:<32 upper-case hex host ID>``; versioned paths (``/univmax/restapi/{ver}/...``) with
unknown versions refused; sizes in cylinders (1 CYL = 1920 KiB), also
accepting MB/GB/TB; ``executionOption: ASYNCHRONOUS`` answered with
``202`` + job that runs on the second poll of ``/{ver}/system/job/{id}``
(``SCHEDULED`` -> ``RUNNING`` -> ``SUCCEEDED`` or ``job_failure_status``
with the error in ``result``); volume listing as an iterator (``count``/``resultList``, no
``resultList`` when empty); non-unique volume identifiers; masked storage
groups cannot lose their last device; host LUN addresses in hex; port groups
bound to one protocol.
"""
from __future__ import annotations

import math
import re
import secrets
from collections.abc import Callable
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

SYMMETRIX_ID = "000197900123"
CYL_BYTES = 1920 * 1024
_ROOT = "/univmax/restapi"
_V = _ROOT + r"/(?P<ver>\d+)"
_SLO = _V + r"/sloprovisioning/symmetrix/(?P<sym>\d{12})"
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_UNIT_MB = {"MB": 1, "GB": 1024, "TB": 1024 * 1024}


def _err(status: int, message: str) -> FakeError:
    return FakeError(status, {"message": message})


def _bad(message: str) -> FakeError:
    return _err(400, message)


def _proto_of(initiator: str) -> str:
    v = initiator.lower()
    if v.startswith(("iqn.", "eui.")):
        return "iscsi"
    if v.startswith("nqn."):
        return "nvme-tcp"
    return "fc"


class FakeDellPowerMax(FakeArray):
    vendor = "dell-powermax"
    api_versions = ("100", "101")

    def __init__(self, username: str = "smc", password: str = "pmax-pass",
                 symmetrix_id: str = SYMMETRIX_ID):
        super().__init__()
        self.username = username
        self.password = password
        self.symmetrix_id = symmetrix_id
        self.volumes: dict[str, dict[str, Any]] = {}          # device id -> volume
        self.storage_groups: dict[str, dict[str, Any]] = {}   # name -> sg
        self.hosts: dict[str, dict[str, Any]] = {}            # name -> host
        self.port_groups: dict[str, dict[str, Any]] = {
            "sb_pg_iscsi": {"protocol": "iscsi", "ports": ["SE-1E:0", "SE-2E:0"]},
            "sb_pg_fc": {"protocol": "fc", "ports": ["FA-1D:4", "FA-2D:4"]},
            "sb_pg_nvme": {"protocol": "nvme-tcp", "ports": ["OR-1C:2", "OR-2C:2"]},
        }
        self.views: dict[str, dict[str, Any]] = {}            # name -> masking view
        self.jobs: dict[str, dict[str, Any]] = {}
        self._job_fns: dict[str, Callable[[], Reply]] = {}
        self.first_lun = 1
        self.job_failure_status = "FAILED"
        self._dev = 0x00100

    # -- fake controls ------------------------------------------------------

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields.get("password", self.password)

    def _new_device(self, identifier: str, cyl: int) -> str:
        self._dev += 1
        dev = f"{self._dev:05X}"
        wwn = ("6000097000" + self.symmetrix_id[-12:] + dev.encode().hex()).upper()
        self.volumes[dev] = {
            "volumeId": dev, "type": "TDEV", "emulation": "FBA", "ssid": "FFFFFFFF",
            "status": "Ready", "reserved": False, "pinned": False,
            "volume_identifier": identifier, "cap_cyl": cyl, "wwn": wwn,
            "effective_wwn": wwn, "has_effective_wwn": False, "encapsulated": False,
            "storageGroupId": [], "snapvx_source": False, "snapvx_target": False,
            "mobility_id_enabled": False,
        }
        return dev

    def add_volume(self, name: str, size: int, storage_group: str | None = None) -> str:
        """Create a device directly (optionally inside a storage group)."""
        dev = self._new_device(name, math.ceil(size / CYL_BYTES))
        if storage_group:
            sg = self.storage_groups.setdefault(storage_group, self._sg_record(
                storage_group, "SRP_1", None))
            sg["volumes"].append(dev)
            self.volumes[dev]["storageGroupId"].append(storage_group)
        return dev

    # -- dispatch helpers ----------------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.basic_auth() != (self.username, self.password):
            raise FakeError(401, "Unauthorized", {"WWW-Authenticate": 'Basic realm="Unisphere"'})

    def _check(self, req: Request, ver: str, sym: str | None = None) -> None:
        self._auth(req)
        if ver not in self.api_versions:
            raise _err(404, f"Unsupported REST API version {ver}")
        if sym is not None and sym != self.symmetrix_id:
            raise _err(404, f"Cannot find Symmetrix {sym}")

    def _run(self, req: Request, fn: Callable[[], Reply]) -> Reply:
        """Honour ``executionOption``: run now, or queue as a job."""
        body = req.json() or {}
        option = body.get("executionOption", "SYNCHRONOUS")
        if option not in ("SYNCHRONOUS", "ASYNCHRONOUS"):
            raise _bad(f"Invalid executionOption {option}")
        if option == "SYNCHRONOUS":
            return fn()
        job_id = str(1_000_000 + self.next_id())
        self.jobs[job_id] = {
            "jobId": job_id, "name": f"{req.method} {req.path}", "status": "SCHEDULED",
            "username": self.username, "result": None,
            "last_modified_date": "Nov-20-2026 00:00:00.000",
            "resourceLink": f"https://unisphere.fake.example{req.path}",
            "task": [{"execution_order": 1, "description": f"{req.method} {req.path}"}],
            "_polls": 0,
        }
        self._job_fns[job_id] = fn
        return Reply(202, self._public_job(job_id))

    def _public_job(self, job_id: str) -> dict[str, Any]:
        return {k: v for k, v in self.jobs[job_id].items() if not k.startswith("_")}


    # -- LIO data path (testing.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return list(self.volumes)

    def lio_all_iqns(self) -> list[str]:
        return sorted({str(i).lower() for h in self.hosts.values() for i in h.get("initiator", []) if str(i).lower().startswith("iqn.")})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        host = self.hosts.get(host_id) or {}
        return [str(i).lower() for i in host.get("initiator", []) if str(i).lower().startswith("iqn.")]

    @route("GET", _V + r"/system/job/(?P<job_id>\d+)")
    def get_job(self, req, ver, job_id):
        self._check(req, ver)
        job = self.jobs.get(job_id)
        if job is None:
            raise _err(404, f"Cannot find job {job_id}")
        job["_polls"] += 1
        if job["status"] == "SCHEDULED":
            job["status"] = "RUNNING"
        elif job["status"] == "RUNNING":
            fn = self._job_fns.pop(job_id)
            try:
                fn()
                job["status"], job["result"] = "SUCCEEDED", "Succeeded"
            except FakeError as e:
                body = e.reply.body if isinstance(e.reply.body, dict) else {}
                job["status"] = self.job_failure_status
                job["result"] = f"Failed : {body.get('message', e.reply.status)}"
        return Reply(200, self._public_job(job_id))

    # -- system -------------------------------------------------------------------------

    @route("GET", _ROOT + "/version")
    def version(self, req):
        self._auth(req)
        return Reply(200, {"version": "T10.1.0.0", "api_version": "101",
                           "supported_api_versions": list(self.api_versions)})

    @route("GET", _V + r"/system/symmetrix/(?P<sym>\d{12})")
    def get_symmetrix(self, req, ver, sym):
        self._check(req, ver, sym)
        return Reply(200, {"symmetrixId": sym, "device_count": len(self.volumes),
                           "ucode": "6079.225.0", "model": "PowerMax_8500",
                           "local": True, "default_fba_srp": "SRP_1"})

    # -- volumes ------------------------------------------------------------------------

    def _vol(self, dev: str) -> dict[str, Any]:
        if dev not in self.volumes:
            raise _err(404, f"Could not find volume {dev}")
        return self.volumes[dev]

    def _vol_view(self, v: dict[str, Any]) -> dict[str, Any]:
        cyl = v["cap_cyl"]
        mapped = any(v["volumeId"] in self.storage_groups[sg]["volumes"]
                     and self.storage_groups[sg]["maskingview"] for sg in v["storageGroupId"])
        return {**v,
                "cap_gb": round(cyl * 1920 / 1024 / 1024, 2),
                "cap_mb": round(cyl * 1.875, 2),
                "num_of_storage_groups": len(v["storageGroupId"]),
                "num_of_front_end_paths": 0, "mapped": mapped,
                "allocated_percent": 0}

    @staticmethod
    def _cylinders(attrs: dict[str, Any]) -> int:
        unit = attrs.get("capacityUnit", "GB")
        try:
            size = float(attrs["volume_size"])
        except (KeyError, TypeError, ValueError) as e:
            raise _bad("Invalid volume_size") from e
        if size <= 0:
            raise _bad("volume_size must be positive")
        if unit == "CYL":
            if size != int(size):
                raise _bad("volume_size in CYL must be a whole number")
            return int(size)
        if unit not in _UNIT_MB:
            raise _bad(f"Invalid capacityUnit {unit}")
        return math.ceil(size * _UNIT_MB[unit] / 1.875)

    def _identifier(self, attrs: dict[str, Any]) -> str:
        ident = attrs.get("volumeIdentifier") or {}
        if not ident:
            return ""
        if ident.get("volumeIdentifierChoice") != "identifier_name":
            raise _bad("Unsupported volumeIdentifierChoice")
        name = ident.get("identifier_name") or ""
        if len(name) > 64:
            raise _bad("The volume identifier must not exceed 64 characters")
        return name

    @route("GET", _SLO + "/volume")
    def list_volumes(self, req, ver, sym):
        self._check(req, ver, sym)
        rows = list(self.volumes.values())
        if req.q("volume_identifier") is not None:
            want = req.q("volume_identifier") or ""
            if want.startswith("<like>"):
                rows = [v for v in rows if want[6:] in v["volume_identifier"]]
            else:
                rows = [v for v in rows if v["volume_identifier"] == want]
        if req.q("storageGroupId") is not None:
            sg = req.q("storageGroupId")
            rows = [v for v in rows if sg in v["storageGroupId"]]
        body: dict[str, Any] = {"count": len(rows), "maxPageSize": 1000,
                                "expirationTime": 1_900_000_000_000}
        if rows:
            body["id"] = f"{secrets.token_hex(4)}-iterator"
            body["resultList"] = {"result": [{"volumeId": v["volumeId"]} for v in rows],
                                  "from": 1, "to": len(rows)}
        return Reply(200, body)

    @route("GET", _SLO + r"/volume/(?P<dev>[0-9A-Fa-f]+)")
    def get_volume(self, req, ver, sym, dev):
        self._check(req, ver, sym)
        return Reply(200, self._vol_view(self._vol(dev)))

    @route("PUT", _SLO + r"/volume/(?P<dev>[0-9A-Fa-f]+)")
    def put_volume(self, req, ver, sym, dev):
        self._check(req, ver, sym)
        vol = self._vol(dev)
        body = req.json() or {}
        expand = ((body.get("editVolumeActionParam") or {}).get("expandVolumeParam") or {})
        if not expand:
            raise _bad("Unsupported editVolumeActionParam")
        cyl = self._cylinders(expand.get("volumeAttribute") or {})

        def do() -> Reply:
            if cyl < vol["cap_cyl"]:
                raise _bad(f"The new size of volume {dev} must be larger than the "
                           "current size")
            vol["cap_cyl"] = cyl
            return Reply(200, self._vol_view(vol))
        return self._run(req, do)

    @route("DELETE", _SLO + r"/volume/(?P<dev>[0-9A-Fa-f]+)")
    def delete_volume(self, req, ver, sym, dev):
        self._check(req, ver, sym)
        vol = self._vol(dev)
        if vol["storageGroupId"]:
            raise _bad(f"A problem occurred deleting the volume {dev}: the device is a "
                       f"member of storage group {vol['storageGroupId'][0]}")
        del self.volumes[dev]
        return Reply(204)

    # -- storage groups ------------------------------------------------------------------

    @staticmethod
    def _sg_record(name: str, srp: str, slo: str | None) -> dict[str, Any]:
        return {"storageGroupId": name, "srp": srp, "slo": slo or "NONE",
                "volumes": [], "maskingview": []}

    def _sg(self, name: str) -> dict[str, Any]:
        if name not in self.storage_groups:
            raise _err(404, f"The requested storage group {name} cannot be found")
        return self.storage_groups[name]

    def _sg_view(self, sg: dict[str, Any]) -> dict[str, Any]:
        cyl = sum(self.volumes[d]["cap_cyl"] for d in sg["volumes"])
        out = {"storageGroupId": sg["storageGroupId"], "srp": sg["srp"], "slo": sg["slo"],
               "num_of_vols": len(sg["volumes"]), "num_of_child_sgs": 0,
               "num_of_parent_sgs": 0, "num_of_masking_views": len(sg["maskingview"]),
               "num_of_snapshots": 0, "cap_gb": round(cyl * 1920 / 1024 / 1024, 2),
               "device_emulation": "FBA", "type": "Standalone", "unprotected": True,
               "compression": True}
        if sg["maskingview"]:
            out["maskingview"] = list(sg["maskingview"])
        return out

    def _add_devices(self, sg: dict[str, Any], attrs_list: list[dict[str, Any]]) -> None:
        for attrs in attrs_list:
            cyl = self._cylinders(attrs)
            ident = self._identifier(attrs)
            for _ in range(int(attrs.get("num_of_vols", 1))):
                dev = self._new_device(ident, cyl)
                sg["volumes"].append(dev)
                self.volumes[dev]["storageGroupId"].append(sg["storageGroupId"])

    @route("POST", _SLO + "/storagegroup")
    def post_sg(self, req, ver, sym):
        self._check(req, ver, sym)
        body = req.json() or {}
        name = body.get("storageGroupId") or ""
        if not _NAME_RE.match(name):
            raise _bad(f"Invalid storage group name {name!r}")
        srp = body.get("srpId", "None")
        if srp not in ("SRP_1", "None"):
            raise _err(404, f"Cannot find SRP {srp}")
        params = body.get("sloBasedStorageGroupParam") or []
        for p in params:
            if p.get("sloId", "None") not in ("None", "Diamond", "Platinum", "Gold",
                                              "Silver", "Bronze", "Optimized"):
                raise _bad(f"Invalid sloId {p.get('sloId')}")
            if "workloadSelection" in p and p["workloadSelection"] not in ("None", None):
                raise _bad("workloadSelection is not supported on this array")

        def do() -> Reply:
            if name in self.storage_groups:
                raise _err(409, "The requested storage group resource already exists")
            slo = next((p.get("sloId") for p in params if p.get("sloId") != "None"), None)
            sg = self._sg_record(name, srp, slo)
            self.storage_groups[name] = sg
            try:
                for p in params:
                    self._add_devices(sg, p.get("volumeAttributes") or [])
            except FakeError:
                del self.storage_groups[name]
                raise
            return Reply(200, self._sg_view(sg))
        return self._run(req, do)

    @route("GET", _SLO + r"/storagegroup/(?P<name>[^/]+)")
    def get_sg(self, req, ver, sym, name):
        self._check(req, ver, sym)
        return Reply(200, self._sg_view(self._sg(name)))

    @route("PUT", _SLO + r"/storagegroup/(?P<name>[^/]+)")
    def put_sg(self, req, ver, sym, name):
        self._check(req, ver, sym)
        sg = self._sg(name)
        edit = (req.json() or {}).get("editStorageGroupActionParam") or {}
        if "removeVolumeParam" in edit:
            devs = edit["removeVolumeParam"].get("volumeId") or []

            def remove() -> Reply:
                for d in devs:
                    if d not in sg["volumes"]:
                        raise _bad(f"Storage Group {name} does not contain volume {d}")
                if sg["maskingview"] and len(set(sg["volumes"]) - set(devs)) == 0:
                    raise _bad(f"Cannot remove the last volume from Storage Group {name} "
                               "because it is part of a masking view")
                for d in devs:
                    sg["volumes"].remove(d)
                    self.volumes[d]["storageGroupId"].remove(name)
                return Reply(200, self._sg_view(sg))
            return self._run(req, remove)
        expand = edit.get("expandStorageGroupParam") or {}
        add = expand.get("addVolumeParam")
        if add is not None:
            if not add.get("create_new_volumes", False):
                raise _bad("Only create_new_volumes is supported by the fake")

            def grow() -> Reply:
                self._add_devices(sg, add.get("volumeAttributes") or [])
                return Reply(200, self._sg_view(sg))
            return self._run(req, grow)
        raise _bad("Unsupported editStorageGroupActionParam")

    @route("DELETE", _SLO + r"/storagegroup/(?P<name>[^/]+)")
    def delete_sg(self, req, ver, sym, name):
        self._check(req, ver, sym)
        sg = self._sg(name)
        if sg["maskingview"]:
            raise _bad(f"Storage Group {name} is part of masking view {sg['maskingview'][0]}")
        if sg["volumes"]:
            raise _bad(f"Storage Group {name} is not empty")
        del self.storage_groups[name]
        return Reply(204)

    # -- hosts -------------------------------------------------------------------------------

    def _host(self, name: str) -> dict[str, Any]:
        if name not in self.hosts:
            raise _err(404, f"Host {name} cannot be found")
        return self.hosts[name]

    def _host_view(self, h: dict[str, Any]) -> dict[str, Any]:
        return {"hostId": h["hostId"], "num_of_masking_views": len(h["maskingview"]),
                "num_of_initiators": len(h["initiator"]), "num_of_host_groups": 0,
                "port_flags_override": False, "consistent_lun": False,
                "initiator": list(h["initiator"]), "maskingview": list(h["maskingview"]),
                "type": {"fc": "Fibre", "iscsi": "iSCSI", "nvme-tcp": "NVMe/TCP"}.get(
                    _proto_of(h["initiator"][0]) if h["initiator"] else "", "")}

    def _claim(self, host: str, initiators: list[str]) -> list[str]:
        out = []
        for i in initiators:
            proto = _proto_of(i)
            if proto == "fc" and not re.match(r"^[0-9a-f]{16}$", i):
                raise _bad(f"Invalid initiator {i}: expected a 16-digit WWN")
            if proto == "nvme-tcp" and not re.match(r"^nqn\..+:[0-9A-F]{32}$", i):
                raise _bad(f"Invalid initiator {i}: expected <host NQN>:<host ID>")
            for other in self.hosts.values():
                if i.lower() in (x.lower() for x in other["initiator"]):
                    if other["hostId"] == host:
                        raise _bad(f"Initiator {i} is already in Host {host}")
                    raise _bad(f"Initiator {i} is already in use by Host {other['hostId']}")
            out.append(i)
        protos = {_proto_of(x) for x in out + (self.hosts[host]["initiator"]
                                               if host in self.hosts else [])}
        if len(protos) > 1:
            raise _bad("A host cannot mix initiator types")
        return out

    @route("POST", _SLO + "/host")
    def post_host(self, req, ver, sym):
        self._check(req, ver, sym)
        body = req.json() or {}
        name = body.get("hostId") or ""
        if not _NAME_RE.match(name):
            raise _bad(f"Invalid host name {name!r}")
        initiators = body.get("initiatorId") or []

        def do() -> Reply:
            if name in self.hosts:
                raise _err(409, f"Host {name} already exists")
            claimed = self._claim(name, initiators)
            self.hosts[name] = {"hostId": name, "initiator": claimed, "maskingview": []}
            return Reply(200, self._host_view(self.hosts[name]))
        return self._run(req, do)

    @route("GET", _SLO + r"/host/(?P<name>[^/]+)")
    def get_host(self, req, ver, sym, name):
        self._check(req, ver, sym)
        return Reply(200, self._host_view(self._host(name)))

    @route("PUT", _SLO + r"/host/(?P<name>[^/]+)")
    def put_host(self, req, ver, sym, name):
        self._check(req, ver, sym)
        host = self._host(name)
        edit = (req.json() or {}).get("editHostActionParam") or {}
        add = (edit.get("addInitiatorParam") or {}).get("initiator")
        if add is None:
            raise _bad("Unsupported editHostActionParam")

        def do() -> Reply:
            host["initiator"].extend(self._claim(name, add))
            return Reply(200, self._host_view(host))
        return self._run(req, do)

    @route("DELETE", _SLO + r"/host/(?P<name>[^/]+)")
    def delete_host(self, req, ver, sym, name):
        self._check(req, ver, sym)
        host = self._host(name)
        if host["maskingview"]:
            raise _bad(f"Host {name} is part of masking view {host['maskingview'][0]}")
        del self.hosts[name]
        return Reply(204)

    # -- masking views ---------------------------------------------------------------

    def _view(self, name: str) -> dict[str, Any]:
        if name not in self.views:
            raise _err(404, f"Masking View {name} cannot be found")
        return self.views[name]

    @route("POST", _SLO + "/maskingview")
    def post_view(self, req, ver, sym):
        self._check(req, ver, sym)
        body = req.json() or {}
        name = body.get("maskingViewId") or ""
        if not _NAME_RE.match(name):
            raise _bad(f"Invalid masking view name {name!r}")
        host = ((body.get("hostOrHostGroupSelection") or {})
                .get("useExistingHostParam") or {}).get("hostId")
        pg = ((body.get("portGroupSelection") or {})
              .get("useExistingPortGroupParam") or {}).get("portGroupId")
        sg = ((body.get("storageGroupSelection") or {})
              .get("useExistingStorageGroupParam") or {}).get("storageGroupId")
        if not (host and pg and sg):
            raise _bad("A masking view needs an existing host, port group and storage group")

        def do() -> Reply:
            if name in self.views:
                raise _err(409, "The requested masking view resource already exists")
            h, s = self._host(host), self._sg(sg)
            if pg not in self.port_groups:
                raise _err(500, "Port Group on Symmetrix cannot be found")
            if not s["volumes"]:
                raise _bad(f"Storage Group {sg} is empty")
            protos = {_proto_of(i) for i in h["initiator"]}
            if protos and protos != {self.port_groups[pg]["protocol"]}:
                raise _bad(f"Port Group {pg} does not match the protocol of Host {host}")
            used = {lun for v in self.views.values() if v["host"] == host
                    for lun in v["luns"].values()}
            luns = {}
            for dev in s["volumes"]:
                lun = next(n for n in range(self.first_lun, 4096) if n not in used)
                used.add(lun)
                luns[dev] = lun
            self.views[name] = {"maskingViewId": name, "host": host, "pg": pg, "sg": sg,
                                "luns": luns}
            h["maskingview"].append(name)
            s["maskingview"].append(name)
            return Reply(200, self._view_view(self.views[name]))
        return self._run(req, do)

    @staticmethod
    def _view_view(v: dict[str, Any]) -> dict[str, Any]:
        return {"maskingViewId": v["maskingViewId"], "hostId": v["host"],
                "portGroupId": v["pg"], "storageGroupId": v["sg"]}

    @route("GET", _SLO + r"/maskingview/(?P<name>[^/]+)")
    def get_view(self, req, ver, sym, name):
        self._check(req, ver, sym)
        return Reply(200, self._view_view(self._view(name)))

    @route("GET", _SLO + r"/maskingview/(?P<name>[^/]+)/connections")
    def get_connections(self, req, ver, sym, name):
        self._check(req, ver, sym)
        v = self._view(name)
        want = req.q("volume_id")
        rows = []
        for dev in self.storage_groups[v["sg"]]["volumes"]:
            if want and dev != want:
                continue
            lun = v["luns"].get(dev)
            if lun is None:  # device added after the view was created
                continue
            for initiator in self.hosts[v["host"]]["initiator"]:
                for port in self.port_groups[v["pg"]]["ports"]:
                    rows.append({"volumeId": dev, "host_lun_address": f"{lun:04X}",
                                 "cap_gb": round(self.volumes[dev]["cap_cyl"] * 1920
                                                 / 1024 / 1024, 2),
                                 "initiatorId": initiator, "alias": initiator,
                                 "dir_port": port, "logged_in": False, "on_fabric": False})
        return Reply(200, {"maskingViewConnection": rows})

    @route("DELETE", _SLO + r"/maskingview/(?P<name>[^/]+)")
    def delete_view(self, req, ver, sym, name):
        self._check(req, ver, sym)
        v = self._view(name)
        self.hosts[v["host"]]["maskingview"].remove(name)
        self.storage_groups[v["sg"]]["maskingview"].remove(name)
        del self.views[name]
        return Reply(204)


def contract_setup(**options: Any) -> ContractSetup:
    fake = FakeDellPowerMax(username="smc", password="pmax-pass")
    opts: dict[str, Any] = {
        "symmetrix_id": SYMMETRIX_ID,
        "port_group": {"iscsi": "sb_pg_iscsi", "fc": "sb_pg_fc", "nvme-tcp": "sb_pg_nvme"},
    }
    opts.update(options)
    return ContractSetup(
        fake=fake,
        config=make_config("dell-powermax", "https://unisphere.fake.example:8443", **opts),
        credential=Credential({"username": SecretStr("smc"),
                               "password": SecretStr("pmax-pass")}),
        bad_credential=Credential({"username": SecretStr("smc"),
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
            Protocol.NVME_TCP: (
                (Initiator(protocol=Protocol.NVME_TCP,
                           address="nqn.2014-08.org.nvmexpress:uuid:"
                                   "8c8e1a3c-0b7e-4b8e-9d2a-0000000000a1"),),
                (Initiator(protocol=Protocol.NVME_TCP,
                           address="nqn.2014-08.org.nvmexpress:uuid:"
                                   "8c8e1a3c-0b7e-4b8e-9d2a-0000000000b2"),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_volume(name, size, "customer_sg"),
    )
