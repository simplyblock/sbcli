"""Fake HPE Alletra 5000/6000 / Nimble, NimbleOS REST v1 (subset the adapter uses).

Quirks reproduced: tokens from ``POST /v1/tokens`` checked on ``X-Auth-Token``
and released by token *id*; ``{"data": ...}`` envelopes on requests and
responses; ``{"messages": [{"code": "SM_...", ...}]}`` errors; 42-hex object
ids; sizes in MiB; volumes that must be offline before deletion and cannot be
offlined or deleted while ACRs exist; FC WWPNs in colon form; and LUN 0 for
every iSCSI ACR with volume-scoped targets.

Modelled on the NimbleOS SDK and Cinder: error bodies carry a generic
``SM_http_<status>`` message followed by the specific one (``SM_eexist``,
``SM_http_not_found``, ``SM_vol_size_decreased``, ...); offlining an offline
volume is refused with "Object is already in requested state"; and with
``async_ops`` a mutating request answers 202 + ``SM_async_job_id`` and the
job is polled on ``GET /v1/jobs/<id>`` until ``state`` is ``done``. The
specific codes for ACR/online refusals (``SM_vol_online``,
``SM_vol_has_acl``, ``SM_vol_has_connections``, ``SM_igrp_has_acl``) are
the fake's own.
"""
from __future__ import annotations

import re
import secrets
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential
from simplyblock_core.storage_arrays.base import Initiator, Protocol

from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_ID = r"(?P<obj_id>(?!detail$)[^/]+)"
_ID_RE = re.compile(r"^[0-9a-f]{42}$")
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.:\-]{0,214}$")
_WWPN_RE = re.compile(r"^([0-9a-f]{2}:){7}[0-9a-f]{2}$")
_NG_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.:\-]{0,63}$")


_HTTP_CODE = {400: "SM_http_bad_request", 401: "SM_http_unauthorized",
              404: "SM_http_not_found", 409: "SM_http_conflict"}


def _err(status: int, code: str, text: str) -> FakeError:
    messages = [{"code": code, "severity": "error", "text": text}]
    generic = _HTTP_CODE.get(status)
    if generic and generic != code:
        messages.insert(0, {"code": generic, "severity": "error", "text": text})
    return FakeError(status, {"messages": messages})


class FakeHpeNimble(FakeArray):
    vendor = "hpe-nimble"

    def __init__(self, username: str = "admin", password: str = "admin",
                 volume_scoped_targets: bool = True):
        super().__init__()
        self.username = username
        self.password = password
        self.volume_scoped_targets = volume_scoped_targets
        #: "METHOD kind" pairs (kind = volume, initiator_group, initiator, acr)
        #: that answer 202 + an async job instead of the object.
        self.async_ops: set[str] = set()
        #: polls a job reports ``running`` before ``done``
        self.job_polls = 2
        self.jobs: dict[str, int] = {}                 # job id -> polls left
        self.tokens: dict[str, str] = {}               # token id -> session_token
        self.pools = {self._new_id(0x0a): "default", self._new_id(0x0a): "fast"}
        self.perfpolicies = {self._new_id(0x03): "default", self._new_id(0x03): "Oracle OLTP"}
        self.volumes: dict[str, dict[str, Any]] = {}   # id -> volume
        self.groups: dict[str, dict[str, Any]] = {}    # id -> initiator group
        self.initiators: dict[str, dict[str, Any]] = {}
        self.acrs: dict[str, dict[str, Any]] = {}

    def _new_id(self, kind: int) -> str:
        # NimbleOS ids: 42 hex chars, group-unique prefix + object type + counter.
        return f"06{secrets.token_hex(9)}{kind:02x}{self.next_id():020x}"[:42]

    # -- fake controls ------------------------------------------------------

    def expire_sessions(self) -> None:
        self.tokens.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields.get("username", self.username)
        self.password = fields["password"]

    def add_volume(self, name: str, size: int, pool_id: str | None = None,
                   perfpolicy_id: str | None = None) -> str:
        vid = self._new_id(0x2a)
        pool_id = pool_id or next(iter(self.pools))
        perfpolicy_id = perfpolicy_id or next(iter(self.perfpolicies))
        self.volumes[vid] = {
            "id": vid, "name": name, "size": -(-size // (1 << 20)),
            "serial_number": secrets.token_hex(8) + "6c9ce900" + secrets.token_hex(4),
            "target_name": f"iqn.2007-11.com.nimblestorage:{name}-v{secrets.token_hex(8)}",
            "online": True, "vol_state": "online", "pool_id": pool_id,
            "pool_name": self.pools[pool_id], "perfpolicy_id": perfpolicy_id,
            "perfpolicy_name": self.perfpolicies[perfpolicy_id], "access_control_records": [],
        }
        return vid

    # -- envelope helpers -----------------------------------------------------

    def _auth(self, req: Request) -> None:
        if req.headers.get("X-Auth-Token") not in self.tokens.values():
            raise _err(401, "SM_http_unauthorized", "Unauthorized: session token invalid")

    @staticmethod
    def _body(req: Request) -> dict[str, Any]:
        body = req.json()
        if not isinstance(body, dict) or not isinstance(body.get("data"), dict):
            raise _err(400, "SM_missing_arg", "Missing data envelope")
        return body["data"]

    @staticmethod
    def _page(items: list[dict[str, Any]]) -> Reply:
        return Reply(200, {"startRow": 0, "endRow": len(items), "totalRows": len(items),
                           "data": items})

    @staticmethod
    def _filter(req: Request, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = items
        for key, values in req.query.items():
            if key in ("startRow", "endRow", "pageSize", "fields", "sortBy"):
                continue
            out = [i for i in out if str(i.get(key, "")).lower() == values[0].lower()]
        return out

    def _get(self, table: dict[str, dict[str, Any]], obj_id: str, what: str) -> dict[str, Any]:
        if not _ID_RE.match(obj_id):
            raise _err(400, "SM_invalid_arg", f"Invalid {what} id: {obj_id}")
        if obj_id not in table:
            raise _err(404, "SM_http_not_found", f"Object not found: {what} {obj_id}")
        return table[obj_id]

    def _done(self, op: str, status: int, data: Any) -> Reply:
        """The reply for a completed mutation, or 202 + a job for ``async_ops``."""
        if op not in self.async_ops:
            return Reply(status, {"data": data} if data is not None else {})
        jid = self._new_id(0x29)
        self.jobs[jid] = self.job_polls
        return Reply(202, {"messages": [{"code": "SM_async_job_id", "severity": "info",
                                         "text": f"Job {jid} is running",
                                         "arguments": {"job_id": jid}}]})


    # -- LIO data path (fakes.lio) -----------------------------------------
    def lio_volume_ids(self) -> list[str]:
        return list(self.volumes)

    def lio_all_iqns(self) -> list[str]:
        return sorted({i["iqn"].lower() for i in self.initiators.values() if i.get("iqn")})

    def lio_host_iqns(self, host_id: str) -> list[str]:
        gids = {g["id"] for g in self.groups.values() if host_id in (g["id"], g["name"])}
        return [i["iqn"].lower() for i in self.initiators.values()
                if i["initiator_group_id"] in gids and i.get("iqn")]

    @route("GET", "/v1/jobs/" + _ID)
    def get_job(self, req, obj_id):
        self._auth(req)
        if obj_id not in self.jobs:
            raise _err(404, "SM_http_not_found", f"Object not found: job {obj_id}")
        left = self.jobs[obj_id]
        self.jobs[obj_id] = max(0, left - 1)
        return Reply(200, {"data": {"id": obj_id, "state": "running" if left else "done",
                                    "result": "" if left else "success"}})

    # -- auth / info ----------------------------------------------------------

    @route("POST", "/v1/tokens")
    def login(self, req):
        data = self._body(req)
        if data.get("username") != self.username or data.get("password") != self.password:
            raise _err(401, "SM_http_unauthorized", "Unauthorized: invalid credentials")
        tid, token = self._new_id(0x1f), secrets.token_hex(16)
        self.tokens[tid] = token
        return Reply(201, {"data": {"id": tid, "session_token": token,
                                    "username": self.username, "app_name": "",
                                    "source_ip": "192.0.2.1"}})

    @route("DELETE", "/v1/tokens/" + _ID)
    def logout(self, req, obj_id):
        self._auth(req)
        self._get(self.tokens, obj_id, "token")  # type: ignore[arg-type]
        del self.tokens[obj_id]
        return Reply(200, {})

    @route("GET", "/v1/arrays/detail")
    def arrays(self, req):
        self._auth(req)
        return self._page([{"id": "0906b8bc6e4a3c2a7f000000000000000000000001",
                            "name": "fake-af40", "model": "AF40", "serial": "AF-123456",
                            "version": "6.1.2.100-1010", "status": "reachable"}])

    @route("GET", "/v1/pools/detail")
    def pools_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, [{"id": k, "name": v}
                                             for k, v in self.pools.items()]))

    @route("GET", "/v1/performance_policies/detail")
    def perfpolicies_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, [{"id": k, "name": v}
                                             for k, v in self.perfpolicies.items()]))

    # -- volumes -------------------------------------------------------------

    @route("POST", "/v1/volumes")
    def post_volumes(self, req):
        self._auth(req)
        data = self._body(req)
        name, size = data.get("name"), data.get("size")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise _err(400, "SM_invalid_arg", "Invalid argument: name")
        if not isinstance(size, int) or size < 1:
            raise _err(400, "SM_invalid_arg", "Invalid argument: size (MiB)")
        if any(v["name"] == name for v in self.volumes.values()):
            raise _err(409, "SM_eexist", f"Object exists: volume {name}")
        pool_id, pp_id = data.get("pool_id"), data.get("perfpolicy_id")
        if pool_id is not None and pool_id not in self.pools:
            raise _err(404, "SM_http_not_found", f"Object not found: pool {pool_id}")
        if pp_id is not None and pp_id not in self.perfpolicies:
            raise _err(404, "SM_http_not_found", f"Object not found: perfpolicy {pp_id}")
        vid = self.add_volume(name, size << 20, pool_id, pp_id)
        return self._done("POST volume", 201, self.volumes[vid])

    @route("GET", "/v1/volumes/detail")
    def volumes_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, list(self.volumes.values())))

    @route("GET", "/v1/volumes/" + _ID)
    def get_volume(self, req, obj_id):
        self._auth(req)
        return Reply(200, {"data": self._get(self.volumes, obj_id, "volume")})

    @route("PUT", "/v1/volumes/" + _ID)
    def put_volume(self, req, obj_id):
        self._auth(req)
        vol = self._get(self.volumes, obj_id, "volume")
        data = self._body(req)
        if "size" in data:
            size = data["size"]
            if not isinstance(size, int) or size < 1:
                raise _err(400, "SM_invalid_arg", "Invalid argument: size (MiB)")
            if size < vol["size"] and not data.get("force"):
                raise _err(400, "SM_vol_size_decreased",
                           "the operation will decrease the size of the volume. "
                           "use force option to proceed.")
            vol["size"] = size
        if "online" in data:
            if bool(data["online"]) == vol["online"]:
                raise _err(400, "SM_ealready", "Object is already in requested state")
            if not data["online"] and self._vol_acrs(obj_id) and not data.get("force"):
                raise _err(409, "SM_vol_has_connections",
                           "Volume has access control records; cannot offline")
            vol["online"] = bool(data["online"])
            vol["vol_state"] = "online" if vol["online"] else "offline"
        return self._done("PUT volume", 200, vol)

    @route("DELETE", "/v1/volumes/" + _ID)
    def delete_volume(self, req, obj_id):
        self._auth(req)
        vol = self._get(self.volumes, obj_id, "volume")
        if vol["online"]:
            raise _err(409, "SM_vol_online", "Volume must be offline before it can be deleted")
        if self._vol_acrs(obj_id):
            raise _err(409, "SM_vol_has_acl", "Volume has access control records")
        del self.volumes[obj_id]
        return self._done("DELETE volume", 200, None)

    # -- initiator groups / initiators ----------------------------------------------

    def _group_view(self, g: dict[str, Any]) -> dict[str, Any]:
        members = [i for i in self.initiators.values() if i["initiator_group_id"] == g["id"]]
        view = dict(g)
        if g["access_protocol"] == "iscsi":
            view["iscsi_initiators"] = [{"id": i["id"], "label": i["label"], "iqn": i["iqn"],
                                         "ip_address": "*"} for i in members]
            view["fc_initiators"] = None
        else:
            view["fc_initiators"] = [{"wwpn": i["wwpn"], "alias": ""} for i in members]
            view["iscsi_initiators"] = None
        return view

    @route("GET", "/v1/initiator_groups/detail")
    def groups_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, [self._group_view(g)
                                             for g in self.groups.values()]))

    @route("GET", "/v1/initiator_groups/" + _ID)
    def get_group(self, req, obj_id):
        self._auth(req)
        return Reply(200, {"data": self._group_view(self._get(self.groups, obj_id, "group"))})

    @route("POST", "/v1/initiator_groups")
    def post_group(self, req):
        self._auth(req)
        data = self._body(req)
        name, proto = data.get("name"), data.get("access_protocol")
        if not isinstance(name, str) or not _NG_NAME_RE.match(name):
            raise _err(400, "SM_invalid_arg", "Invalid argument: name")
        if proto not in ("iscsi", "fc"):
            raise _err(400, "SM_invalid_arg", "Invalid argument: access_protocol")
        if any(g["name"] == name for g in self.groups.values()):
            raise _err(409, "SM_eexist", f"Object exists: initiator group {name}")
        gid = self._new_id(0x02)
        self.groups[gid] = {"id": gid, "name": name, "access_protocol": proto,
                            "description": data.get("description", ""),
                            "target_subnets": [], "host_type": "auto"}
        return self._done("POST initiator_group", 201, self._group_view(self.groups[gid]))

    @route("DELETE", "/v1/initiator_groups/" + _ID)
    def delete_group(self, req, obj_id):
        self._auth(req)
        self._get(self.groups, obj_id, "group")
        if any(a["initiator_group_id"] == obj_id for a in self.acrs.values()):
            raise _err(409, "SM_igrp_has_acl",
                       "Initiator group is in use by access control records")
        for iid in [i for i, v in self.initiators.items() if v["initiator_group_id"] == obj_id]:
            del self.initiators[iid]
        del self.groups[obj_id]
        return self._done("DELETE initiator_group", 200, None)

    @route("GET", "/v1/initiators/detail")
    def initiators_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, list(self.initiators.values())))

    @route("POST", "/v1/initiators")
    def post_initiator(self, req):
        self._auth(req)
        data = self._body(req)
        gid, proto = data.get("initiator_group_id"), data.get("access_protocol")
        group = self._get(self.groups, str(gid), "group")
        if proto != group["access_protocol"]:
            raise _err(400, "SM_invalid_arg", "access_protocol does not match initiator group")
        item = {"id": self._new_id(0x04), "access_protocol": proto,
                "initiator_group_id": gid, "initiator_group_name": group["name"],
                "label": "", "iqn": "", "ip_address": "", "wwpn": "", "alias": ""}
        if proto == "iscsi":
            iqn, label = data.get("iqn"), data.get("label")
            if not iqn or not label or len(label) > 64:
                raise _err(400, "SM_missing_arg", "iqn and label are required")
            if any(i["iqn"].lower() == iqn.lower() for i in self.initiators.values()):
                raise _err(409, "SM_eexist", f"Object exists: initiator {iqn}")
            item.update(iqn=iqn, label=label, ip_address=data.get("ip_address", "*"))
        else:
            wwpn = str(data.get("wwpn", ""))
            if not _WWPN_RE.match(wwpn):
                raise _err(400, "SM_invalid_arg", "Invalid argument: wwpn (colon form)")
            if any(i["wwpn"] == wwpn for i in self.initiators.values()):
                raise _err(409, "SM_eexist", f"Object exists: initiator {wwpn}")
            item.update(wwpn=wwpn, alias=data.get("alias", ""))
        self.initiators[item["id"]] = item
        return self._done("POST initiator", 201, item)

    # -- access control records ------------------------------------------------

    def _vol_acrs(self, vol_id: str) -> list[dict[str, Any]]:
        return [a for a in self.acrs.values() if a["vol_id"] == vol_id]

    @route("GET", "/v1/access_control_records/detail")
    def acrs_detail(self, req):
        self._auth(req)
        return self._page(self._filter(req, list(self.acrs.values())))

    @route("POST", "/v1/access_control_records")
    def post_acr(self, req):
        self._auth(req)
        data = self._body(req)
        vol = self._get(self.volumes, str(data.get("vol_id")), "volume")
        group = self._get(self.groups, str(data.get("initiator_group_id")), "group")
        if data.get("apply_to", "both") not in ("volume", "snapshot", "both", "pe", "vvol_volume",
                                                 "vvol_snapshot"):
            raise _err(400, "SM_invalid_arg", "Invalid argument: apply_to")
        if any(a["initiator_group_id"] == group["id"] for a in self._vol_acrs(vol["id"])):
            raise _err(409, "SM_eexist", "Object exists: access control record")
        if group["access_protocol"] == "iscsi" and self.volume_scoped_targets:
            lun = 0
        else:
            used = {a["lun"] for a in self.acrs.values()
                    if a["initiator_group_id"] == group["id"]}
            lun = next(n for n in range(2048) if n not in used)
        aid = self._new_id(0x0d)
        self.acrs[aid] = {"id": aid, "vol_id": vol["id"], "vol_name": vol["name"],
                          "initiator_group_id": group["id"],
                          "initiator_group_name": group["name"],
                          "apply_to": data.get("apply_to", "both"), "lun": lun,
                          "chap_user_id": "", "pe_id": "", "snap_id": ""}
        return self._done("POST acr", 201, self.acrs[aid])

    @route("DELETE", "/v1/access_control_records/" + _ID)
    def delete_acr(self, req, obj_id):
        self._auth(req)
        self._get(self.acrs, obj_id, "access control record")
        del self.acrs[obj_id]
        return self._done("DELETE acr", 200, None)


def contract_setup() -> ContractSetup:
    fake = FakeHpeNimble(username="admin", password="nimble-pw")
    return ContractSetup(
        fake=fake,
        config=make_config("hpe-nimble", "https://nimble.fake.example:5392"),
        credential=Credential({"username": SecretStr("admin"),
                               "password": SecretStr("nimble-pw")}),
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
