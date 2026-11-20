"""Fake IBM Storage Virtualize REST API (the subset the adapter uses).

Mirrors the real API's shape: every command is ``POST /rest[/v1]/<cmd>[/<obj>]``
with CLI flags as JSON; failures are HTTP 500 with the CLI's ``CMMVCnnnnE``
message as plain text; an unknown/expired token is HTTP 403. Quirks kept:

* values are strings (ids, capacities, SCSI ids);
* ``capacity`` is human-readable (``10.00GB``) unless ``bytes`` is passed;
* detail views (``lsvdisk <id>``) are a list: the volume, then its copies;
* a one-row ``lsvdiskhostmap`` comes back as a bare dict;
* object ids are the lowest free integer, so a deleted volume's id is reused;
* ``mkvdiskhostmap`` to a second host needs ``force`` (CMMVC6071E);
* FC WWPNs that are not logged in need ``force`` on mkhost/addhostport.
"""
from __future__ import annotations

import secrets
from typing import Any

from pydantic import SecretStr

from simplyblock_core.credentials import Credential

from ..base import Initiator, Protocol
from . import ContractSetup, FakeArray, FakeError, Reply, Request, make_config, route

_ROOT = r"/rest(?:/v1)?"

E_NOT_EXIST = "CMMVC5753E The specified object does not exist or is not a suitable candidate."
E_NAME_RULES = ("CMMVC5754E The specified object does not exist, or the name supplied "
                "does not meet the naming rules.")
E_IN_REQUEST = "CMMVC5842E The action failed because an object that was in the request does not exist."
E_EXISTS = "CMMVC6035E The action failed as the object already exists."
E_IQN = ("CMMVC6581E The command has failed because the maximum number of allowed iSCSI "
         "qualified names (IQNs) has been reached, or the IQN is already assigned or is not valid.")
E_WWPN = ("CMMVC5867E The action failed because the worldwide port name is already "
          "assigned or is not valid.")
E_ALREADY_MAPPED = ("CMMVC5878E The virtual disk (VDisk)-to-host mapping was not created "
                    "because this VDisk is already mapped to this host.")
E_MULTI_HOST = ("CMMVC6071E The VDisk-to-host mapping was not created because the VDisk is "
                "already mapped to a host.")
E_VDISK_MAPPED = ("CMMVC5840E The virtual disk (VDisk) was not deleted because it is mapped "
                  "to a host or because it is part of a FlashCopy or Remote Copy mapping, "
                  "or is involved in an image mode migrate.")
E_HOST_MAPPED = ("CMMVC5871E The action failed because one or more of the configured port "
                 "names is in a mapping.")
E_BAD_NAME = ("CMMVC6527E The name that you have entered is not valid. The name can contain "
              "letters, numbers, spaces, periods, dashes, and underscores. The name must "
              "begin with a letter or an underscore.")
E_PARAM = "CMMVC5707E Required parameters are missing."
E_SHRINK = "CMMVC5709E [-size] is not a supported parameter."


def _cli_error(text: str) -> FakeError:
    return FakeError(500, text.encode(), {"Content-Type": "text/plain"})


def _human(size: int) -> str:
    for unit, div in (("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20)):
        if size >= div:
            return f"{size / div:.2f}{unit}"
    return f"{size}B"


def _units(unit: str | None) -> int:
    return 1 << (10 * ["b", "kb", "mb", "gb", "tb", "pb"].index((unit or "mb").lower()))


def _valid_name(name: str) -> bool:
    return bool(name) and len(name) <= 63 and (name[0].isalpha() or name[0] == "_") and all(
        c.isalnum() or c in "._- " for c in name)


class FakeIbmFlashSystem(FakeArray):
    vendor = "ibm-flashsystem"

    def __init__(self, username: str = "superuser", password: str = "passw0rd",
                 pools: tuple[str, ...] = ("Pool0",)):
        super().__init__()
        self.username = username
        self.password = password
        self.pools = {name: str(i) for i, name in enumerate(pools)}
        self.tokens: set[str] = set()
        self.vdisks: dict[str, dict[str, Any]] = {}  # id -> vdisk
        self.hosts: dict[str, dict[str, Any]] = {}   # name -> host
        self.maps: list[dict[str, Any]] = []         # {vdisk_id, host, scsi}
        self.commands: list[tuple[str, str | None, dict[str, Any]]] = []

    # -- fake controls ------------------------------------------------------------

    def expire_sessions(self) -> None:
        self.tokens.clear()

    def set_credential(self, **fields: str) -> None:
        self.username = fields["username"]
        self.password = fields["password"]

    @staticmethod
    def _free_id(used) -> str:
        n = 0
        while str(n) in used:
            n += 1
        return str(n)

    def add_volume(self, name: str, size: int, thin: bool = False) -> str:
        vid = self._free_id(self.vdisks)
        self.vdisks[vid] = {"id": vid, "name": name, "capacity": size,
                            "vdisk_UID": "600507680C8" + secrets.token_hex(11).upper()[:21],
                            "mdisk_grp_name": next(iter(self.pools)), "IO_group_name": "io_grp0",
                            "thin": thin}
        return vid

    # -- dispatch -------------------------------------------------------------------

    @route("POST", _ROOT + "/auth")
    def auth(self, req: Request):
        if (req.headers.get("X-Auth-Username") != self.username
                or req.headers.get("X-Auth-Password") != self.password):
            raise FakeError(403, {"code": 403, "message": "Forbidden"})
        token = secrets.token_hex(16)
        self.tokens.add(token)
        return Reply(200, {"token": token})

    @route("POST", _ROOT + r"/(?P<cmd>[a-z]+)(?:/(?P<obj>[^/]+))?")
    def command(self, req: Request, cmd: str, obj: str | None = None):
        if cmd == "auth":  # pragma: no cover - matched above
            return self.auth(req)
        if req.headers.get("X-Auth-Token") not in self.tokens:
            raise FakeError(403, {"code": 403, "message": "Forbidden"})
        opts = req.json() or {}
        self.commands.append((cmd, obj, opts))
        handler = getattr(self, f"_c_{cmd}", None)
        if handler is None:
            raise FakeError(404, "Not Found")
        result = handler(obj, opts)
        return Reply(200, result)

    # -- lookups ----------------------------------------------------------------------

    def _vdisk(self, ref: str | None) -> dict[str, Any]:
        if ref in self.vdisks:
            return self.vdisks[ref]
        for v in self.vdisks.values():
            if v["name"] == ref:
                return v
        raise _cli_error(E_NOT_EXIST)

    def _host(self, ref: str | None) -> dict[str, Any]:
        if ref in self.hosts:
            return self.hosts[ref]
        for h in self.hosts.values():
            if h["id"] == ref:
                return h
        raise _cli_error(E_NOT_EXIST)

    def _vdisk_row(self, v: dict[str, Any], opts: dict[str, Any]) -> dict[str, Any]:
        cap = str(v["capacity"]) if opts.get("bytes") else _human(v["capacity"])
        return {"id": v["id"], "name": v["name"], "IO_group_id": "0",
                "IO_group_name": v["IO_group_name"], "status": "online",
                "mdisk_grp_id": self.pools[v["mdisk_grp_name"]],
                "mdisk_grp_name": v["mdisk_grp_name"], "capacity": cap, "type": "striped",
                "FC_id": "", "FC_name": "", "RC_id": "", "RC_name": "",
                "vdisk_UID": v["vdisk_UID"], "fc_map_count": "0", "copy_count": "1",
                "fast_write_state": "empty", "se_copy_count": "1" if v["thin"] else "0",
                "RC_change": "no", "compressed_copy_count": "0", "parent_mdisk_grp_id": "0",
                "parent_mdisk_grp_name": v["mdisk_grp_name"], "formatting": "no",
                "encrypt": "no", "volume_id": v["id"], "volume_name": v["name"],
                "function": ""}

    # -- system -------------------------------------------------------------------------

    def _c_lssystem(self, obj, opts):
        return {"id": "000002042140A0B2", "name": "fake-fs7300", "location": "local",
                "code_level": "8.6.0.4 (build 169.12.2403211122000)",
                "product_name": "IBM FlashSystem 7300"}

    # -- volumes --------------------------------------------------------------------------

    def _c_lsvdisk(self, obj, opts):
        if obj is not None:
            v = self._vdisk(obj)
            row = self._vdisk_row(v, opts)
            cap = row["capacity"]
            copy = {"copy_id": "0", "status": "online", "sync": "yes", "primary": "yes",
                    "mdisk_grp_name": v["mdisk_grp_name"], "type": "striped",
                    "capacity": cap, "real_capacity": cap if not v["thin"] else "0.00MB",
                    "autoexpand": "on" if v["thin"] else "", "se_copy": "yes" if v["thin"] else "no"}
            return [row, copy]
        rows = [self._vdisk_row(v, opts) for v in self.vdisks.values()]
        fv = opts.get("filtervalue")
        if fv:
            key, _, value = str(fv).partition("=")
            rows = [r for r in rows if str(r.get(key)) == value]
        return rows

    def _c_mkvolume(self, obj, opts):
        name, pool = opts.get("name"), opts.get("pool")
        if "size" not in opts or not pool:
            raise _cli_error(E_PARAM)
        if pool not in self.pools and pool not in self.pools.values():
            raise _cli_error(E_NAME_RULES)
        if name is not None and not _valid_name(name):
            raise _cli_error(E_BAD_NAME)
        if any(v["name"] == name for v in self.vdisks.values()):
            raise _cli_error(E_EXISTS)
        size = int(opts["size"]) * _units(opts.get("unit"))
        size = -(-size // 512) * 512
        vid = self.add_volume(name or "", size, thin=bool(opts.get("thin")))
        v = self.vdisks[vid]
        if not name:
            v["name"] = f"volume{vid}"
        if pool in self.pools:
            v["mdisk_grp_name"] = pool
        if opts.get("iogrp") is not None:
            v["IO_group_name"] = str(opts["iogrp"])
        return {"id": vid, "message": f"Volume, id [{vid}], successfully created"}

    def _c_expandvdisksize(self, obj, opts):
        v = self._vdisk(obj)
        if "size" not in opts:
            raise _cli_error(E_PARAM)
        delta = int(opts["size"]) * _units(opts.get("unit"))
        if delta <= 0:
            raise _cli_error(E_SHRINK)
        v["capacity"] += -(-delta // 512) * 512

    def _c_rmvolume(self, obj, opts):
        v = self._vdisk(obj)
        if any(m["vdisk_id"] == v["id"] for m in self.maps) and not opts.get("removehostmappings"):
            raise _cli_error(E_VDISK_MAPPED)
        self.maps = [m for m in self.maps if m["vdisk_id"] != v["id"]]
        del self.vdisks[v["id"]]

    # -- hosts ------------------------------------------------------------------------------

    def _owner(self, kind: str, value: str) -> str | None:
        for h in self.hosts.values():
            if value.lower() in (p.lower() for p in h[kind]):
                return h["name"]
        return None

    def _parse_ports(self, opts: dict[str, Any]) -> dict[str, list[str]]:
        ports: dict[str, list[str]] = {"iscsiname": [], "fcwwpn": [], "nqn": []}
        if opts.get("iscsiname"):
            ports["iscsiname"] = [p for p in str(opts["iscsiname"]).split(",") if p]
        if opts.get("fcwwpn"):
            wwpns = [p for p in str(opts["fcwwpn"]).split(":") if p]
            for w in wwpns:
                if len(w) != 16 or any(c not in "0123456789abcdefABCDEF" for c in w):
                    raise _cli_error(E_WWPN)
            if not opts.get("force"):
                # The fake has no fabric: no WWPN is ever logged in.
                raise _cli_error(E_WWPN)
            ports["fcwwpn"] = [w.upper() for w in wwpns]
        if opts.get("nqn"):
            ports["nqn"] = [p for p in str(opts["nqn"]).split(",") if p]
        return ports

    def _claim(self, host: str | None, ports: dict[str, list[str]]) -> None:
        errors = {"iscsiname": E_IQN, "fcwwpn": E_WWPN, "nqn": E_IQN}
        for kind, values in ports.items():
            for value in values:
                if self._owner(kind, value) is not None:  # even when it is this host
                    raise _cli_error(errors[kind])

    def _c_mkhost(self, obj, opts):
        name = opts.get("name")
        if not name:
            raise _cli_error(E_PARAM)
        if not _valid_name(name):
            raise _cli_error(E_BAD_NAME)
        if name in self.hosts:
            raise _cli_error(E_EXISTS)
        protocol = opts.get("protocol", "scsi")
        if protocol not in ("scsi", "iscsi", "fcscsi", "tcpnvme", "rdmanvme", "fcnvme", "nvme"):
            raise _cli_error(E_PARAM)
        ports = self._parse_ports(opts)
        if not any(ports.values()):
            raise _cli_error(E_PARAM)
        if ports["nqn"] and protocol not in ("tcpnvme", "rdmanvme", "fcnvme", "nvme"):
            raise _cli_error(E_PARAM)
        self._claim(None, ports)
        hid = self._free_id({h["id"] for h in self.hosts.values()})
        self.hosts[name] = {"id": hid, "name": name, "protocol": protocol,
                            "iscsiname": ports["iscsiname"], "fcwwpn": ports["fcwwpn"],
                            "nqn": ports["nqn"]}
        return {"id": hid, "message": f"Host, id [{hid}], successfully created"}

    def _c_addhostport(self, obj, opts):
        h = self._host(obj)
        ports = self._parse_ports(opts)
        if not any(ports.values()):
            raise _cli_error(E_PARAM)
        self._claim(h["name"], ports)
        for kind, values in ports.items():
            h[kind].extend(values)

    def _c_lshost(self, obj, opts):
        if obj is None:
            return [{"id": h["id"], "name": h["name"], "port_count": str(
                len(h["iscsiname"]) + len(h["fcwwpn"]) + len(h["nqn"])),
                "iogrp_count": "4", "status": "online", "protocol": h["protocol"]}
                for h in self.hosts.values()]
        h = self._host(obj)
        nodes = ([{"WWPN": w, "node_logged_in_count": "0", "state": "offline"}
                  for w in h["fcwwpn"]]
                 + [{"iscsi_name": i, "node_logged_in_count": "0", "state": "offline"}
                    for i in h["iscsiname"]]
                 + [{"nqn": n, "node_logged_in_count": "0", "state": "offline"}
                    for n in h["nqn"]])
        return {"id": h["id"], "name": h["name"], "port_count": str(len(nodes)),
                "type": "generic", "iogrp_count": "4", "status": "offline",
                "protocol": h["protocol"], "host_cluster_id": "", "host_cluster_name": "",
                "mapping_count": str(sum(1 for m in self.maps if m["host"] == h["name"])),
                "nodes": nodes}

    def _c_rmhost(self, obj, opts):
        h = self._host(obj)
        if any(m["host"] == h["name"] for m in self.maps) and not opts.get("force"):
            raise _cli_error(E_HOST_MAPPED)
        self.maps = [m for m in self.maps if m["host"] != h["name"]]
        del self.hosts[h["name"]]

    # -- mappings -------------------------------------------------------------------------------

    def _map_row(self, m: dict[str, Any]) -> dict[str, Any]:
        v, h = self.vdisks[m["vdisk_id"]], self.hosts[m["host"]]
        return {"id": v["id"], "name": v["name"], "SCSI_id": str(m["scsi"]),
                "host_id": h["id"], "host_name": h["name"], "vdisk_UID": v["vdisk_UID"],
                "IO_group_id": "0", "IO_group_name": v["IO_group_name"],
                "mapping_type": "private", "host_cluster_id": "", "host_cluster_name": "",
                "protocol": h["protocol"]}

    def _c_lsvdiskhostmap(self, obj, opts):
        v = self._vdisk(obj)
        rows = [self._map_row(m) for m in self.maps if m["vdisk_id"] == v["id"]]
        return rows[0] if len(rows) == 1 else rows  # quirk: one row -> bare dict

    def _c_mkvdiskhostmap(self, obj, opts):
        v = self._vdisk(obj)
        if not opts.get("host"):
            raise _cli_error(E_PARAM)
        h = self._host(opts["host"])
        mine = [m for m in self.maps if m["vdisk_id"] == v["id"]]
        if any(m["host"] == h["name"] for m in mine):
            raise _cli_error(E_ALREADY_MAPPED)
        if mine and not opts.get("force"):
            raise _cli_error(E_MULTI_HOST)
        used = {m["scsi"] for m in self.maps if m["host"] == h["name"]}
        scsi = int(opts["scsi"]) if opts.get("scsi") is not None else next(
            n for n in range(4096) if n not in used)
        self.maps.append({"vdisk_id": v["id"], "host": h["name"], "scsi": scsi})
        return {"id": str(scsi),
                "message": f"Virtual Disk to Host map, id [{scsi}], successfully created"}

    def _c_rmvdiskhostmap(self, obj, opts):
        v = self._vdisk(obj)
        if not opts.get("host"):
            raise _cli_error(E_PARAM)
        h = self._host(opts["host"])
        before = len(self.maps)
        self.maps = [m for m in self.maps
                     if not (m["vdisk_id"] == v["id"] and m["host"] == h["name"])]
        if len(self.maps) == before:
            raise _cli_error(E_IN_REQUEST)


def contract_setup() -> ContractSetup:
    fake = FakeIbmFlashSystem(username="superuser", password="passw0rd")
    return ContractSetup(
        fake=fake,
        config=make_config("ibm-flashsystem", "https://flashsystem.fake.example:7443",
                           pool="Pool0"),
        credential=Credential({"username": SecretStr("superuser"),
                               "password": SecretStr("passw0rd")}),
        bad_credential=Credential({"username": SecretStr("superuser"),
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
                           address="nqn.2014-08.org.nvmexpress:uuid:host-a"),),
                (Initiator(protocol=Protocol.NVME_TCP,
                           address="nqn.2014-08.org.nvmexpress:uuid:host-b"),),
            ),
        },
        create_foreign_volume=lambda name, size: fake.add_volume(name, size),
    )
