"""Fake Huawei OceanStor Dorado DeviceManager REST API (the subset the adapter uses).

Quirks reproduced:

* every answer is HTTP 200 with ``{"data": ..., "error": {"code", "description"}}``;
  failures only show in ``error.code``; a query matching nothing omits ``data``;
* login at ``/deviceManager/rest/xx/sessions`` returns ``deviceid`` +
  ``iBaseToken``; later calls must use ``/deviceManager/rest/<deviceid>/`` and
  the ``iBaseToken`` header, else ``-401``; wrong password is 1077987870;
* every value is a string; LUN ``CAPACITY`` is in 512-byte sectors;
* initiators are free-standing objects bound to a host via ``PARENTID``;
* mapping is host group + LUN group + mapping view; a host can be in one host
  group only; host LUN ids are assigned when a LUN becomes visible to a host
  and reported inside the ``ASSOCIATEMETADATA`` JSON string.

Codes marked "unverified" below are stand-ins: the real array refuses those
operations, but we do not know with which code. The adapter must not depend
on them.
"""
from __future__ import annotations

import json
import secrets
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_D = r"/deviceManager/rest/(?P<dev>[^/]+)"

UNAUTHORIZED = -401
WRONG_PASSWORD = 1077987870
OBJECT_NOT_EXIST = 1077948996
OBJECT_NAME_EXISTS = 1077948993
OBJECT_ID_NOT_UNIQUE = 1077948997
LUN_NOT_EXIST = 1077936859
LUN_ALREADY_IN_GROUP = 1077936862
HOST_NOT_EXIST = 1077937498
HOSTGROUP_NOT_EXIST = 1077937500
HOST_ALREADY_IN_GROUP = 1077937501
HOST_NOT_IN_GROUP = 1073745412
HOSTGROUP_NOT_IN_VIEW = 1073804552
LUNGROUP_NOT_IN_VIEW = 1073804554
HOSTGROUP_ALREADY_IN_VIEW = 1073804556
LUNGROUP_ALREADY_IN_VIEW = 1073804560
MAPPINGVIEW_NOT_EXIST = 1077951819
PARAMETER_INCORRECT = 50331651
#: unverified: object still in use (LUN in a group, host with initiators, ...)
IN_USE = 1077949000
#: unverified: host already in another host group / initiator bound elsewhere
ALREADY_ASSOCIATED = 1077949002


def _fail(code: int, description: str) -> FakeError:
    return FakeError(200, {"data": {}, "error": {"code": code, "description": description,
                                                 "suggestion": ""}})


def _ok(data: Any = None) -> Reply:
    body: dict[str, Any] = {"error": {"code": 0, "description": "0"}}
    if data is not None and data != []:
        body["data"] = data
    return Reply(200, body)


def _filter_name(req: Request) -> str | None:
    f = req.q("filter")
    if not f:
        return None
    key, _, value = f.partition("::")
    if key != "NAME":
        raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
    return value


class FakeHuaweiDorado(FakeArray):
    vendor = "huawei-dorado"

    def __init__(self, username: str = "admin", password: str = "Admin@storage1",
                 pools: tuple[str, ...] = ("StoragePool001",)):
        super().__init__()
        self.username = username
        self.password = password
        self.device_id = "2102353GTH10L8000008"
        self.tokens: set[str] = set()
        self.pools = {str(i): {"ID": str(i), "NAME": n, "USAGETYPE": "1"}
                      for i, n in enumerate(pools)}
        self.luns: dict[str, dict[str, Any]] = {}
        self.hosts: dict[str, dict[str, Any]] = {}
        self.hostgroups: dict[str, dict[str, Any]] = {}   # + "hosts": set
        self.lungroups: dict[str, dict[str, Any]] = {}    # + "luns": set
        self.views: dict[str, dict[str, Any]] = {}        # + "hostgroups", "lungroups"
        self.initiators: dict[str, dict[str, dict[str, Any]]] = {
            "iscsi_initiator": {}, "fc_initiator": {}}
        self.host_lun_ids: dict[tuple[str, str], int] = {}

    # -- fake controls -------------------------------------------------------------

    def expire_sessions(self) -> None:
        self.tokens.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields["username"]
        self.password = fields["password"]

    def add_volume(self, name: str, size: int) -> str:
        lid = str(self.next_id())
        pool = next(iter(self.pools.values()))
        self.luns[lid] = {
            "ID": lid, "NAME": name, "TYPE": "11", "PARENTID": pool["ID"],
            "PARENTNAME": pool["NAME"], "CAPACITY": str(size // 512), "ALLOCTYPE": "1",
            "SECTORSIZE": "512", "HEALTHSTATUS": "1", "RUNNINGSTATUS": "27",
            "WWN": "6" + secrets.token_hex(4) + "1001f4e7d" + secrets.token_hex(5)
                   + f"{int(lid):08x}"[:4],
            "DESCRIPTION": "", "ISADD2LUNGROUP": "false"}
        return lid

    # -- auth ----------------------------------------------------------------------

    def _auth(self, req: Request, dev: str) -> None:
        if dev != self.device_id or req.headers.get("iBaseToken") not in self.tokens:
            raise _fail(UNAUTHORIZED, "The user is not logged in or the session has expired.")

    @route("POST", r"/deviceManager/rest/xx/sessions")
    def login(self, req):
        body = req.json() or {}
        if body.get("username") != self.username or body.get("password") != self.password:
            raise _fail(WRONG_PASSWORD, "The user name or password is incorrect.")
        if str(body.get("scope")) not in ("0", "1"):
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        token = secrets.token_hex(16)
        self.tokens.add(token)
        reply = _ok({"deviceid": self.device_id, "iBaseToken": token, "accountstate": 1,
                     "lastloginip": "10.0.0.1", "level": 2, "pwdchangetime": -1,
                     "usergroup": "", "userid": body["username"], "username": body["username"],
                     "userscope": "0"})
        reply.headers["Set-Cookie"] = f"session={secrets.token_hex(8)}; Secure; HttpOnly"
        return reply

    @route("DELETE", _D + "/sessions")
    def logout(self, req, dev):
        self.tokens.discard(req.headers.get("iBaseToken"))
        return _ok()

    @route("GET", _D + "/system/")
    def system(self, req, dev):
        self._auth(req, dev)
        return _ok({"ID": self.device_id, "NAME": "fake-dorado", "TYPE": "201",
                    "PRODUCTMODE": "811", "productModeString": "OceanStor Dorado 5000 V6",
                    "PRODUCTVERSION": "V600R005C20", "pointRelease": "6.1.5",
                    "HEALTHSTATUS": "1", "RUNNINGSTATUS": "1"})

    @route("GET", _D + "/storagepool")
    def storagepool(self, req, dev):
        self._auth(req, dev)
        name = _filter_name(req)
        return _ok([p for p in self.pools.values() if name is None or p["NAME"] == name])

    # -- LUNs --------------------------------------------------------------------------

    def _lun(self, lid: str) -> dict[str, Any]:
        if lid not in self.luns:
            raise _fail(LUN_NOT_EXIST, "The LUN does not exist.")
        return self.luns[lid]

    @route("GET", _D + "/lun")
    def get_luns(self, req, dev):
        self._auth(req, dev)
        name = _filter_name(req)
        return _ok([lun for lun in self.luns.values() if name is None or lun["NAME"] == name])

    @route("POST", _D + "/lun")
    def post_lun(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        name, cap = b.get("NAME"), b.get("CAPACITY")
        if not name or len(name) > 31 or not isinstance(cap, int) or cap <= 0:
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if str(b.get("PARENTID")) not in self.pools:
            raise _fail(OBJECT_NOT_EXIST, "The specified storage pool does not exist.")
        if any(lun["NAME"] == name for lun in self.luns.values()):
            raise _fail(OBJECT_NAME_EXISTS, "The name already exists.")
        lid = self.add_volume(name, cap * 512)
        self.luns[lid]["PARENTID"] = str(b["PARENTID"])
        self.luns[lid]["PARENTNAME"] = self.pools[str(b["PARENTID"])]["NAME"]
        self.luns[lid]["ALLOCTYPE"] = str(b.get("ALLOCTYPE", 1))
        return _ok(self.luns[lid])

    @route("GET", _D + r"/lun/(?P<lid>\d+)")
    def get_lun(self, req, dev, lid):
        self._auth(req, dev)
        return _ok(self._lun(lid))

    @route("PUT", _D + "/lun/expand")
    def expand_lun(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        lun = self._lun(str(b.get("ID")))
        cap = b.get("CAPACITY")
        if not isinstance(cap, int) or cap < int(lun["CAPACITY"]):
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        lun["CAPACITY"] = str(cap)
        return _ok()

    @route("DELETE", _D + r"/lun/(?P<lid>\d+)")
    def delete_lun(self, req, dev, lid):
        self._auth(req, dev)
        self._lun(lid)
        if any(lid in g["luns"] for g in self.lungroups.values()):
            raise _fail(IN_USE, "The LUN is in a LUN group.")
        del self.luns[lid]
        return _ok()

    @route("GET", _D + "/lun/associate")
    def lun_associate(self, req, dev):
        self._auth(req, dev)
        if req.q("ASSOCIATEOBJTYPE") != "21":
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        hid = req.q("ASSOCIATEOBJID") or ""
        if hid not in self.hosts:
            raise _fail(HOST_NOT_EXIST, "The host does not exist.")
        rows = []
        for (h, lid), n in sorted(self.host_lun_ids.items()):
            if h == hid:
                row = dict(self.luns[lid])
                row["ASSOCIATEMETADATA"] = json.dumps({"HostLUNID": n,
                                                       "mappingviewId": self._view_of(hid)})
                rows.append(row)
        return _ok(rows)

    # -- hosts ----------------------------------------------------------------------------

    def _host(self, hid: str) -> dict[str, Any]:
        if hid not in self.hosts:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        return self.hosts[hid]

    @route("GET", _D + "/host")
    def get_hosts(self, req, dev):
        self._auth(req, dev)
        name = _filter_name(req)
        return _ok([h for h in self.hosts.values() if name is None or h["NAME"] == name])

    @route("POST", _D + "/host")
    def post_host(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        name = b.get("NAME")
        if not name or len(name) > 31:
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if any(h["NAME"] == name for h in self.hosts.values()):
            raise _fail(OBJECT_NAME_EXISTS, "The name already exists.")
        hid = str(self.next_id())
        self.hosts[hid] = {"ID": hid, "NAME": name, "TYPE": "21",
                           "OPERATIONSYSTEM": str(b.get("OPERATIONSYSTEM", 0)),
                           "ISADD2HOSTGROUP": "false", "HEALTHSTATUS": "1",
                           "RUNNINGSTATUS": "1", "DESCRIPTION": b.get("DESCRIPTION", "")}
        return _ok(self.hosts[hid])

    @route("GET", _D + r"/host/(?P<hid>\d+)")
    def get_host(self, req, dev, hid):
        self._auth(req, dev)
        return _ok(self._host(hid))

    @route("DELETE", _D + r"/host/(?P<hid>\d+)")
    def delete_host(self, req, dev, hid):
        self._auth(req, dev)
        if hid not in self.hosts:
            raise _fail(HOST_NOT_EXIST, "The host does not exist.")
        if any(hid in g["hosts"] for g in self.hostgroups.values()):
            raise _fail(IN_USE, "The host is in a host group.")
        if any(i["PARENTID"] == hid for c in self.initiators.values() for i in c.values()):
            raise _fail(IN_USE, "The host has initiators.")
        del self.hosts[hid]
        return _ok()

    @route("DELETE", _D + "/host/associate")
    def host_leave_group(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        g = self.hostgroups.get(str(b.get("ID")))
        if g is None:
            raise _fail(HOSTGROUP_NOT_EXIST, "The host group does not exist.")
        hid = str(b.get("ASSOCIATEOBJID"))
        if hid not in g["hosts"]:
            raise _fail(HOST_NOT_IN_GROUP, "The host is not in the host group.")
        g["hosts"].discard(hid)
        self.hosts[hid]["ISADD2HOSTGROUP"] = "false"
        self._refresh()
        return _ok()

    # -- initiators -------------------------------------------------------------------------

    def _ini_lookup(self, coll: str, iid: str) -> dict[str, Any] | None:
        for k, v in self.initiators[coll].items():
            if k.lower() == iid.lower():
                return v
        return None

    @route("GET", _D + r"/(?P<coll>iscsi_initiator|fc_initiator)")
    def list_initiators(self, req, dev, coll):
        self._auth(req, dev)
        parent = req.q("PARENTID")
        return _ok([i for i in self.initiators[coll].values()
                    if parent is None or i["PARENTID"] == parent])

    @route("POST", _D + r"/(?P<coll>iscsi_initiator|fc_initiator)")
    def add_initiator(self, req, dev, coll):
        self._auth(req, dev)
        b = req.json() or {}
        iid = str(b.get("ID") or "")
        if coll == "fc_initiator" and (len(iid) != 16 or any(
                c not in "0123456789abcdef" for c in iid)):
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if not iid:
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if self._ini_lookup(coll, iid) is not None:
            raise _fail(OBJECT_ID_NOT_UNIQUE, "The ID already exists.")
        self.initiators[coll][iid] = {
            "ID": iid, "TYPE": "222" if coll == "iscsi_initiator" else "223",
            "ISFREE": "true", "PARENTID": "", "PARENTNAME": "", "PARENTTYPE": "",
            "RUNNINGSTATUS": "28", "HEALTHSTATUS": "1", "MULTIPATHTYPE": "0"}
        return _ok(self.initiators[coll][iid])

    @route("GET", _D + r"/(?P<coll>iscsi_initiator|fc_initiator)/(?P<iid>[^/]+)")
    def get_initiator(self, req, dev, coll, iid):
        self._auth(req, dev)
        ini = self._ini_lookup(coll, iid)
        if ini is None:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        return _ok(ini)

    @route("PUT", _D + r"/(?P<coll>iscsi_initiator|fc_initiator)/(?P<iid>[^/]+)")
    def put_initiator(self, req, dev, coll, iid):
        self._auth(req, dev)
        b = req.json() or {}
        if iid in ("remove_iscsi_from_host", "remove_fc_from_host"):
            ini = self._ini_lookup(coll, str(b.get("ID") or ""))
            if ini is None:
                raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
            ini.update(ISFREE="true", PARENTID="", PARENTNAME="", PARENTTYPE="")
            return _ok()
        ini = self._ini_lookup(coll, iid)
        if ini is None:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        if "PARENTID" in b:
            hid = str(b["PARENTID"])
            host = self._host(hid)
            if str(b.get("PARENTTYPE")) != "21":
                raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
            if ini["ISFREE"] == "false" and ini["PARENTID"] != hid:
                raise _fail(ALREADY_ASSOCIATED, "The initiator has been added to another host.")
            ini.update(ISFREE="false", PARENTID=hid, PARENTNAME=host["NAME"], PARENTTYPE="21")
        return _ok()

    # -- groups and views ---------------------------------------------------------------------

    def _create_named(self, store: dict[str, dict[str, Any]], req: Request,
                      obj_type: str, **sets: set) -> Reply:
        b = req.json() or {}
        name = b.get("NAME")
        if not name or len(name) > 31:
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if any(o["NAME"] == name for o in store.values()):
            raise _fail(OBJECT_NAME_EXISTS, "The name already exists.")
        oid = str(self.next_id())
        store[oid] = {"ID": oid, "NAME": name, "TYPE": obj_type, "DESCRIPTION": "", **sets}
        return _ok(self._public(store[oid]))

    @staticmethod
    def _public(obj: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in obj.items() if not isinstance(v, set)}

    def _list_named(self, store, req) -> Reply:
        name = _filter_name(req)
        return _ok([self._public(o) for o in store.values() if name is None or o["NAME"] == name])

    @route("GET", _D + "/hostgroup")
    def get_hostgroups(self, req, dev):
        self._auth(req, dev)
        return self._list_named(self.hostgroups, req)

    @route("POST", _D + "/hostgroup")
    def post_hostgroup(self, req, dev):
        self._auth(req, dev)
        return self._create_named(self.hostgroups, req, "14", hosts=set())

    @route("DELETE", _D + r"/hostgroup/(?P<gid>\d+)")
    def delete_hostgroup(self, req, dev, gid):
        self._auth(req, dev)
        g = self.hostgroups.get(gid)
        if g is None:
            raise _fail(HOSTGROUP_NOT_EXIST, "The host group does not exist.")
        if g["hosts"] or any(gid in v["hostgroups"] for v in self.views.values()):
            raise _fail(IN_USE, "The host group is in use.")
        del self.hostgroups[gid]
        return _ok()

    @route("POST", _D + "/hostgroup/associate")
    def hostgroup_add_host(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        g = self.hostgroups.get(str(b.get("ID")))
        if g is None:
            raise _fail(HOSTGROUP_NOT_EXIST, "The host group does not exist.")
        hid = str(b.get("ASSOCIATEOBJID"))
        if hid not in self.hosts:
            raise _fail(HOST_NOT_EXIST, "The host does not exist.")
        if hid in g["hosts"]:
            raise _fail(HOST_ALREADY_IN_GROUP, "The host already exists in the host group.")
        if any(hid in o["hosts"] for o in self.hostgroups.values()):
            raise _fail(ALREADY_ASSOCIATED, "The host already belongs to another host group.")
        g["hosts"].add(hid)
        self.hosts[hid]["ISADD2HOSTGROUP"] = "true"
        self._refresh()
        return _ok()

    @route("GET", _D + "/hostgroup/associate")
    def hostgroups_of(self, req, dev):
        self._auth(req, dev)
        t, oid = req.q("ASSOCIATEOBJTYPE"), req.q("ASSOCIATEOBJID") or ""
        if t == "21":
            return _ok([self._public(g) for g in self.hostgroups.values() if oid in g["hosts"]])
        if t == "245":
            v = self.views.get(oid)
            return _ok([self._public(self.hostgroups[g]) for g in v["hostgroups"]] if v else [])
        raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")

    @route("GET", _D + "/lungroup")
    def get_lungroups(self, req, dev):
        self._auth(req, dev)
        return self._list_named(self.lungroups, req)

    @route("POST", _D + "/lungroup")
    def post_lungroup(self, req, dev):
        self._auth(req, dev)
        return self._create_named(self.lungroups, req, "256", luns=set())

    @route("DELETE", _D + r"/lungroup/(?P<gid>\d+)")
    def delete_lungroup(self, req, dev, gid):
        self._auth(req, dev)
        g = self.lungroups.get(gid)
        if g is None:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        if g["luns"] or any(gid in v["lungroups"] for v in self.views.values()):
            raise _fail(IN_USE, "The LUN group is in use.")
        del self.lungroups[gid]
        return _ok()

    @route("POST", _D + "/lungroup/associate")
    def lungroup_add_lun(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        g = self.lungroups.get(str(b.get("ID")))
        if g is None:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        if str(b.get("ASSOCIATEOBJTYPE")) != "11":
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        lid = str(b.get("ASSOCIATEOBJID"))
        self._lun(lid)
        if lid in g["luns"]:
            raise _fail(LUN_ALREADY_IN_GROUP, "The LUN is already in the LUN group.")
        g["luns"].add(lid)
        self.luns[lid]["ISADD2LUNGROUP"] = "true"
        self._refresh()
        return _ok()

    @route("DELETE", _D + "/lungroup/associate")
    def lungroup_remove_lun(self, req, dev):
        self._auth(req, dev)
        b = req.json() or {}
        g = self.lungroups.get(str(b.get("ID")))
        lid = str(b.get("ASSOCIATEOBJID"))
        if g is None or lid not in g["luns"]:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        g["luns"].discard(lid)
        if lid in self.luns and not any(lid in o["luns"] for o in self.lungroups.values()):
            self.luns[lid]["ISADD2LUNGROUP"] = "false"
        self._refresh()
        return _ok()

    @route("GET", _D + "/lungroup/associate")
    def lungroups_of(self, req, dev):
        self._auth(req, dev)
        if req.q("ASSOCIATEOBJTYPE") != "11":
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        lid = req.q("ASSOCIATEOBJID") or ""
        self._lun(lid)
        return _ok([self._public(g) for g in self.lungroups.values() if lid in g["luns"]])

    @route("GET", _D + "/mappingview")
    def get_views(self, req, dev):
        self._auth(req, dev)
        return self._list_named(self.views, req)

    @route("POST", _D + "/mappingview")
    def post_view(self, req, dev):
        self._auth(req, dev)
        return self._create_named(self.views, req, "245", hostgroups=set(), lungroups=set())

    @route("DELETE", _D + r"/mappingview/(?P<vid>\d+)")
    def delete_view(self, req, dev, vid):
        self._auth(req, dev)
        v = self.views.get(vid)
        if v is None:
            raise _fail(MAPPINGVIEW_NOT_EXIST, "The mapping view does not exist.")
        if v["hostgroups"] or v["lungroups"]:
            raise _fail(IN_USE, "The mapping view is in use.")
        del self.views[vid]
        return _ok()

    @route("PUT", _D + r"/mappingview/(?P<action>create_associate|remove_associate)")
    def view_associate(self, req, dev, action):
        self._auth(req, dev)
        b = req.json() or {}
        v = self.views.get(str(b.get("ID")))
        if v is None:
            raise _fail(MAPPINGVIEW_NOT_EXIST, "The mapping view does not exist.")
        t, oid = str(b.get("ASSOCIATEOBJTYPE")), str(b.get("ASSOCIATEOBJID"))
        if t == "14":
            key, store, already, missing = ("hostgroups", self.hostgroups,
                                            HOSTGROUP_ALREADY_IN_VIEW, HOSTGROUP_NOT_IN_VIEW)
        elif t == "256":
            key, store, already, missing = ("lungroups", self.lungroups,
                                            LUNGROUP_ALREADY_IN_VIEW, LUNGROUP_NOT_IN_VIEW)
        else:
            raise _fail(PARAMETER_INCORRECT, "The entered parameter is incorrect.")
        if oid not in store:
            raise _fail(OBJECT_NOT_EXIST, "The object does not exist.")
        if action == "create_associate":
            if oid in v[key]:
                raise _fail(already, "The group is already in the mapping view.")
            if v[key]:
                raise _fail(PARAMETER_INCORRECT, "A mapping view holds one group of each kind.")
            v[key].add(oid)
        else:
            if oid not in v[key]:
                raise _fail(missing, "The group is not in the mapping view.")
            v[key].discard(oid)
        self._refresh()
        return _ok()

    # -- visibility -------------------------------------------------------------------------------

    def _view_of(self, hid: str) -> str:
        for v in self.views.values():
            if any(hid in self.hostgroups[g]["hosts"] for g in v["hostgroups"]):
                return v["ID"]
        return ""

    def _refresh(self) -> None:
        """Recompute which LUNs each host sees; keep existing host LUN ids."""
        visible: set[tuple[str, str]] = set()
        for v in self.views.values():
            hosts = {h for g in v["hostgroups"] for h in self.hostgroups[g]["hosts"]}
            luns = {lun for g in v["lungroups"] for lun in self.lungroups[g]["luns"]}
            visible |= {(h, lun) for h in hosts for lun in luns}
        self.host_lun_ids = {k: n for k, n in self.host_lun_ids.items() if k in visible}
        for h, lun in sorted(visible - set(self.host_lun_ids)):
            used = {n for (hh, _), n in self.host_lun_ids.items() if hh == h}
            self.host_lun_ids[(h, lun)] = next(n for n in range(1, 4096) if n not in used)


def contract_setup() -> ContractSetup:
    fake = FakeHuaweiDorado(username="admin", password="Admin@storage1")
    return ContractSetup(
        fake=fake,
        config=make_config("huawei-dorado", "https://dorado.fake.example:8088",
                           storage_pool="StoragePool001"),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("Admin@storage1")}),
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
        create_foreign_volume=lambda name, size: fake.add_volume(name, size),
    )
