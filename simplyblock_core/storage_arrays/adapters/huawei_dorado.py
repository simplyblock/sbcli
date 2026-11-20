"""Huawei OceanStor Dorado (V6 / OceanStor 6.x) DeviceManager REST API.

Auth: ``POST /deviceManager/rest/xx/sessions`` with ``{username, password,
scope: "0"}`` (scope 0 = local user) returns ``data.deviceid`` and
``data.iBaseToken`` (plus a session cookie, kept by the ``requests``
session). Every later call goes to ``/deviceManager/rest/<deviceid>/...``
with the ``iBaseToken`` header. Logout is ``DELETE .../sessions``.

Envelope: answers are HTTP 200 even on failure, with ``{"data": ...,
"error": {"code": <int>, "description": "..."}}``; code 0 is success. A
query that matches nothing omits ``data``. All values are strings. Codes
mapped here (from Huawei's own CSI driver, eSDK_K8S_Plugin):

* auth: -401 (not logged in / session expired), 1077949069 (user offline),
  1077987870 / 1077949081 / 1077949061 (bad user or password),
  1077949070 / 1077987871 (account locked), 1077949071 (IP locked)
* not found: 1077948996 (object), 1077936859 (LUN), 1077937498 (host),
  1077937500 (host group), 1077951819 (mapping view)
* conflict: 1077948993 (name exists), 1077948997 (id not unique)
* tolerated as "already done": 1077936862 (LUN already in LUN group),
  1077937501 (host already in host group), 1073745412 (host not in host
  group), 1073804556 / 1073804560 (host/LUN group already in mapping view),
  1073804552 / 1073804554 (host/LUN group not in mapping view)

LUNs: ``POST /lun {NAME, PARENTID: <pool id>, CAPACITY: <512-byte sectors>,
ALLOCTYPE: 1}`` (thin; Dorado LUNs are always thin), ``PUT /lun/expand {ID,
CAPACITY}``, ``DELETE /lun/<id>``. Identity: ``WWN`` -> ``naa.<lowercase>``.
The ``storage_pool`` option is a pool *name*, resolved once per session via
``GET /storagepool?filter=NAME::<name>``.

Hosts and initiators: ``POST /host {NAME, OPERATIONSYSTEM: 0}`` (Linux).
iSCSI/FC initiators are objects of their own (``/iscsi_initiator/<iqn>``,
``/fc_initiator/<wwpn>``) with ``ISFREE`` / ``PARENTID``; one is created if
the array does not know it yet, then bound with ``PUT ... {PARENTTYPE: 21,
PARENTID: <host id>}``. An initiator whose ``ISFREE`` is ``"false"`` and
``PARENTID`` is another host raises :class:`ArrayConflictError`. Hosts are
addressed by id (``ensure_host`` returns it).

Mapping: the classic host group + LUN group + mapping view triple, one per
host, as Huawei's CSI driver does: ``sb_hg_<hostid>`` holds the host,
``sb_lg_<hostid>`` holds the LUNs mapped to it, ``sb_mv_<hostid>`` joins them.
Map/unmap add/remove the LUN to/from the host's LUN group; ``delete_host``
tears the triple down, unbinds the initiators and deletes the host. The
host LUN id comes from ``GET /lun/associate?TYPE=11&ASSOCIATEOBJTYPE=21&
ASSOCIATEOBJID=<host>`` (``HostLUNID`` inside the ``ASSOCIATEMETADATA`` JSON
string). ``list_mappings`` reports only mappings made through ``sb_lg_``
groups; a LUN in a customer's LUN group is never touched (deleting it then
fails on the array).

NVMe hosts are not supported by this adapter (iSCSI and FC only).

Checked against vendor source
-----------------------------
Huawei CSI ``eSDK_K8S_Plugin`` (``storage/constants.go``,
``storage/oceanstorage/base/client*.go``, ``attacher/attacher.go``,
``oceanstor/client/client_lun.go``, ``csi/backend/plugin/oceanstor.go``) and
the OpenStack Cinder Huawei driver (``huawei/rest_client.py``,
``constants.py``, ``huawei_utils.py``):

* Login/logout, ``deviceid``/``iBaseToken``, the envelope and every error
  code listed above (``WrongPasswordErrorCodes``, ``AccountBeenLocked``,
  ``NeedReLogin`` on -401 / 1077949069, the per-object constants).
* ``CAPACITY`` in 512-byte sectors (CSI sends ``ceil(bytes / 512)`` with no
  further rounding; the 1 MiB rounding here is merely conservative);
  ``ALLOCTYPE`` 1 = thin; ``PUT /lun/expand {ID, CAPACITY}``.
* Names: 31 characters (CSI ``maxLunNameLength``, Cinder
  ``MAX_NAME_LENGTH``; V6 may allow more).
* ``filter=NAME::<name>&range=[0-100]`` with a literal ``::`` (CSI uses the
  first row as the exact match; only a value is ever escaped). The adapter
  used to URL-encode the whole ``filter`` parameter (``NAME%3A%3A...``); it
  now builds the query like the vendor drivers.
* Initiators: ``GET /iscsi_initiator/<iqn>`` / ``/fc_initiator/<wwpn>``
  (1077948996 = unknown), ``ISFREE``/``PARENTID`` ownership as in CSI's
  attacher, ``POST /iscsi_initiator {ID[, TYPE]}`` (1077948997 = exists),
  bind with ``PUT .../<id> {PARENTTYPE: 21, PARENTID}``; unbind with ``PUT
  /iscsi_initiator/remove_iscsi_from_host`` / ``/fc_initiator/
  remove_fc_from_host {TYPE, ID}``; list a host's with ``?PARENTTYPE=21&
  PARENTID=`` -- Cinder re-checks ``PARENTID`` on each row and so does this
  adapter now (an ignored filter would otherwise unbind every initiator on
  the array).
* ``POST /host {NAME, OPERATIONSYSTEM: 0}``; host group / LUN group /
  mapping view calls, ``DELETE`` with a JSON body (``/lungroup/associate``,
  ``/host/associate``), ``PUT /mappingview/create_associate`` /
  ``remove_associate``, ``GET /lun/associate?TYPE=11&ASSOCIATEOBJTYPE=21&
  ASSOCIATEOBJID=`` with ``HostLUNID`` in ``ASSOCIATEMETADATA``.
* ``GET /system/``: ``ID``, ``PRODUCTMODE``, ``PRODUCTVERSION``,
  ``productModeString``, ``pointRelease``.

Verify against a real system
----------------------------
* Whether ``CAPACITY`` stays in 512-byte units for 4K-sector LUNs
  (``SECTORSIZE``).
* Creating an FC initiator object for a WWPN that has never logged in
  (``POST /fc_initiator {ID, TYPE: "223"}``): neither vendor driver does it
  (CSI skips unknown or offline WWPNs).
* Error codes for: deleting a LUN that is still in a LUN group, deleting a
  host that still has initiators or group membership, binding an initiator
  that is bound elsewhere, and a host already in another host group (a host
  can belong to one host group only; the adapter checks this itself and
  raises :class:`ArrayConflictError`).
* Some firmware may answer an expired session with HTTP 401 instead of the
  -401 envelope (the vendor drivers only read the envelope); both are
  treated as an auth error.
"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

from ..base import (
    ArrayAdapter,
    ArrayAuthError,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    ArrayNotFoundError,
    ArrayRequestError,
    Capabilities,
    HostSpec,
    Mapping,
    OptionDoc,
    Protocol,
    Volume,
)
from ..http import json_of

SECTOR = 512
REST = "/deviceManager/rest"

AUTH_CODES = frozenset({-401, 1077949069, 1077987870, 1077949081, 1077949061,
                        1077949070, 1077987871, 1077949071})
NOT_FOUND_CODES = frozenset({1077948996, 1077936859, 1077937498, 1077937500, 1077951819})
CONFLICT_CODES = frozenset({1077948993, 1077948997})

LUN_ALREADY_IN_GROUP = 1077936862
HOST_ALREADY_IN_GROUP = 1077937501
HOST_NOT_IN_GROUP = 1073745412
GROUP_ALREADY_IN_VIEW = frozenset({1073804556, 1073804560})
GROUP_NOT_IN_VIEW = frozenset({1073804552, 1073804554})

OBJ_HOST = 21
OBJ_HOSTGROUP = 14
OBJ_LUN = 11
OBJ_LUNGROUP = 256
OBJ_MAPPINGVIEW = 245
TYPE_ISCSI_INITIATOR = "222"
TYPE_FC_INITIATOR = "223"

_LG, _HG, _MV = "sb_lg_", "sb_hg_", "sb_mv_"


def huawei_wwn(wwn: str) -> str:
    return "naa." + wwn.lower()


class HuaweiDoradoAdapter(ArrayAdapter):
    vendor = "huawei-dorado"
    display_name = "Huawei OceanStor Dorado"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC}),
        size_granularity_bytes=1 << 20,
        min_size_bytes=1 << 20,
        max_name_len=31,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("storage_pool", "Name of the storage pool new LUNs are created in.",
                  required=True),
        OptionDoc("scope", "Login scope: '0' local user, '1' LDAP user.", default="0"),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._base = ""
        self._pool_id: str | None = None

    # -- session -----------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop("iBaseToken", None)
        self._base = ""
        resp = self.session.post(f"{REST}/xx/sessions", json={
            "username": self.secret("username"), "password": self.secret("password"),
            "scope": str(self.option("scope"))})
        data = self._unwrap("POST", "/xx/sessions", resp)
        if not isinstance(data, dict) or not data.get("deviceid") or not data.get("iBaseToken"):
            raise ArrayRequestError("OceanStor login returned no deviceid/iBaseToken")
        self.session.headers["iBaseToken"] = data["iBaseToken"]
        self._base = f"{REST}/{data['deviceid']}"

    def close(self) -> None:
        if self._base and "iBaseToken" in self.session.headers:
            try:
                self.session.delete(f"{self._base}/sessions")
            except Exception:
                pass

    # -- request helpers ------------------------------------------------------------

    @staticmethod
    def _unwrap(method: str, path: str, resp, tolerate: frozenset[int] = frozenset(),
                missing_ok: bool = False) -> Any:
        body = json_of(resp) or {}
        err = body.get("error") or {}
        try:
            code = int(err.get("code", 0))
        except (TypeError, ValueError):
            code = -1
        if code == 0 or code in tolerate:
            return body.get("data")
        msg = f"{method} {path}: error {code}: {err.get('description', '')}".rstrip()
        if err.get("suggestion"):
            msg += f" ({err['suggestion']})"
        if code in AUTH_CODES:
            raise ArrayAuthError(msg)
        if code in NOT_FOUND_CODES:
            if missing_ok:
                return None
            raise ArrayNotFoundError(msg)
        if code in CONFLICT_CODES:
            raise ArrayConflictError(msg)
        raise ArrayRequestError(msg)

    def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              body: Any = None, tolerate: frozenset[int] = frozenset(),
              missing_ok: bool = False) -> Any:
        if not self._base:
            raise ArrayAuthError("OceanStor session not established")
        resp = self.session.request(method, f"{self._base}{path}", params=params, json=body)
        return self._unwrap(method, path, resp, tolerate, missing_ok)

    def _list(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        data = self._call("GET", path, params=params)
        return [d for d in data if isinstance(d, dict)] if isinstance(data, list) else []

    @staticmethod
    def _name_query(path: str, name: str) -> str:
        """``<path>?filter=NAME::<name>&range=[0-100]`` with the ``::``
        separator and the range brackets left literal, as Huawei's CSI driver
        and Cinder driver send them; only the name itself is percent-encoded."""
        return f"{path}?filter=NAME::{quote(name, safe='')}&range=[0-100]"

    def _by_name(self, path: str, name: str) -> dict[str, Any] | None:
        rows = self._list(self._name_query(path, name))
        # The filter is treated as an exact match by the vendor drivers; check.
        return next((r for r in rows if r.get("NAME") == name), None)

    # -- info / pool ------------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        s = self._call("GET", "/system/") or {}
        return ArrayInfo(
            vendor=self.vendor,
            model=str(s.get("productModeString") or s.get("PRODUCTMODE") or ""),
            version=str(s.get("pointRelease") or s.get("PRODUCTVERSION") or ""),
            serial=str(s.get("ID", "")))

    def _pool(self) -> str:
        if self._pool_id is None:
            name = str(self.option("storage_pool"))
            pool = self._by_name("/storagepool", name)
            if pool is None:
                raise ArrayError(f"OceanStor storage pool {name!r} not found")
            self._pool_id = str(pool["ID"])
        return self._pool_id

    # -- LUNs -----------------------------------------------------------------------------

    @staticmethod
    def _volume(lun: dict[str, Any]) -> Volume:
        wwn = str(lun.get("WWN") or "")
        return Volume(id=str(lun["ID"]), name=lun["NAME"],
                      size_bytes=int(lun["CAPACITY"]) * SECTOR,
                      wwn=huawei_wwn(wwn) if wwn else None)

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        lun = self._call("POST", "/lun", body={
            "NAME": name, "PARENTID": self._pool(), "CAPACITY": size_bytes // SECTOR,
            "ALLOCTYPE": 1, "DESCRIPTION": "simplyblock"})
        return self._volume(lun)

    def get_volume(self, volume_id: str) -> Volume | None:
        lun = self._call("GET", f"/lun/{volume_id}", missing_ok=True)
        return self._volume(lun) if lun else None

    def find_volume(self, name: str) -> Volume | None:
        lun = self._by_name("/lun", name)
        return self._volume(lun) if lun else None

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        self._call("PUT", "/lun/expand", body={"ID": volume_id,
                                               "CAPACITY": size_bytes // SECTOR})
        vol = self.get_volume(volume_id)
        if vol is None:
            raise ArrayNotFoundError(f"LUN {volume_id} does not exist")
        return vol

    def delete_volume(self, volume_id: str) -> None:
        self._call("DELETE", f"/lun/{volume_id}")

    # -- hosts and initiators ---------------------------------------------------------------

    @staticmethod
    def _initiator_kinds(spec: HostSpec) -> list[tuple[str, str, str]]:
        """(collection, TYPE, address) for each initiator."""
        out = [("iscsi_initiator", TYPE_ISCSI_INITIATOR, a)
               for a in spec.addresses(Protocol.ISCSI)]
        out += [("fc_initiator", TYPE_FC_INITIATOR, a) for a in spec.addresses(Protocol.FC)]
        return out

    def _initiator(self, collection: str, address: str) -> dict[str, Any] | None:
        data = self._call("GET", f"/{collection}/{address}", missing_ok=True)
        return data if isinstance(data, dict) and data else None

    def ensure_host(self, spec: HostSpec) -> str:
        host = self._by_name("/host", spec.name)
        host_id = str(host["ID"]) if host else None
        # Check ownership before creating anything.
        pending: list[tuple[str, str, str, bool]] = []  # collection, type, address, exists
        for collection, itype, address in self._initiator_kinds(spec):
            ini = self._initiator(collection, address)
            if ini is not None and str(ini.get("ISFREE", "true")).lower() == "false":
                parent = str(ini.get("PARENTID") or "")
                if parent and parent != host_id:
                    raise ArrayConflictError(
                        f"Initiator {address} is registered to another host "
                        f"({ini.get('PARENTNAME') or parent})")
                continue  # already ours
            pending.append((collection, itype, address, ini is not None))
        if host_id is None:
            created = self._call("POST", "/host", body={
                "NAME": spec.name, "TYPE": OBJ_HOST, "OPERATIONSYSTEM": 0,
                "DESCRIPTION": "simplyblock"})
            host_id = str(created["ID"])
        for collection, itype, address, exists in pending:
            if not exists:
                self._call("POST", f"/{collection}", body={"ID": address, "TYPE": itype},
                           tolerate=frozenset({1077948997}))
            self._call("PUT", f"/{collection}/{address}",
                       body={"TYPE": itype, "ID": address,
                             "PARENTTYPE": OBJ_HOST, "PARENTID": host_id})
        return host_id

    def delete_host(self, host_id: str) -> None:
        host = self._call("GET", f"/host/{host_id}", missing_ok=True)
        if not host:
            return
        if self._host_luns(host_id):
            raise ArrayConflictError(f"Host {host_id} still has LUNs mapped")
        view = self._by_name("/mappingview", _MV + host_id)
        hg = self._by_name("/hostgroup", _HG + host_id)
        lg = self._by_name("/lungroup", _LG + host_id)
        if view is not None:
            for obj_type, group in ((OBJ_HOSTGROUP, hg), (OBJ_LUNGROUP, lg)):
                if group is not None:
                    self._call("PUT", "/mappingview/remove_associate", body={
                        "ID": view["ID"], "ASSOCIATEOBJTYPE": obj_type,
                        "ASSOCIATEOBJID": group["ID"]}, tolerate=GROUP_NOT_IN_VIEW)
            self._call("DELETE", f"/mappingview/{view['ID']}", missing_ok=True)
        if hg is not None:
            self._call("DELETE", "/host/associate", body={
                "ID": hg["ID"], "ASSOCIATEOBJTYPE": OBJ_HOST, "ASSOCIATEOBJID": host_id},
                tolerate=frozenset({HOST_NOT_IN_GROUP}))
            self._call("DELETE", f"/hostgroup/{hg['ID']}", missing_ok=True)
        if lg is not None:
            self._call("DELETE", f"/lungroup/{lg['ID']}", missing_ok=True)
        for collection, itype, action in (
                ("iscsi_initiator", TYPE_ISCSI_INITIATOR, "remove_iscsi_from_host"),
                ("fc_initiator", TYPE_FC_INITIATOR, "remove_fc_from_host")):
            # Cinder re-checks PARENTID on every row of this query; so do we,
            # so that an ignored filter can never unbind another host's ports.
            rows = self._list(f"/{collection}", {"PARENTTYPE": OBJ_HOST,
                                                 "PARENTID": host_id})
            for ini in rows:
                if str(ini.get("PARENTID") or "") != host_id:
                    continue
                self._call("PUT", f"/{collection}/{action}",
                           body={"ID": ini["ID"], "TYPE": itype})
        self._call("DELETE", f"/host/{host_id}", missing_ok=True)

    # -- mapping ------------------------------------------------------------------------------

    def _host_luns(self, host_id: str) -> dict[str, int | None]:
        """LUN id -> host LUN id for every LUN the host sees."""
        rows = self._list("/lun/associate", {"TYPE": OBJ_LUN, "ASSOCIATEOBJTYPE": OBJ_HOST,
                                             "ASSOCIATEOBJID": host_id})
        out: dict[str, int | None] = {}
        for r in rows:
            lun_no: int | None = None
            try:
                meta = json.loads(r.get("ASSOCIATEMETADATA") or "{}")
                if meta.get("HostLUNID") is not None:
                    lun_no = int(meta["HostLUNID"])
            except (TypeError, ValueError):
                pass
            out[str(r["ID"])] = lun_no
        return out

    def _ensure_group(self, path: str, name: str, extra: dict[str, Any]) -> str:
        found = self._by_name(path, name)
        if found is None:
            try:
                found = self._call("POST", path, body={"NAME": name, **extra})
            except ArrayConflictError:  # created concurrently
                found = self._by_name(path, name)
            if not found:
                raise ArrayRequestError(f"Could not create {path[1:]} {name}")
        return str(found["ID"])

    def _ensure_view(self, host_id: str) -> str:
        """Create (or complete) the host's group triple; returns the LUN group id."""
        if self._call("GET", f"/host/{host_id}", missing_ok=True) is None:
            raise ArrayNotFoundError(f"Host {host_id} does not exist")
        lg = self._ensure_group("/lungroup", _LG + host_id, {"APPTYPE": 0})
        hg = self._ensure_group("/hostgroup", _HG + host_id, {})
        groups = self._list("/hostgroup/associate", {"ASSOCIATEOBJTYPE": OBJ_HOST,
                                                     "ASSOCIATEOBJID": host_id})
        others = [g for g in groups if str(g.get("ID")) != hg]
        if others:
            raise ArrayConflictError(
                f"Host {host_id} already belongs to host group "
                f"{others[0].get('NAME') or others[0].get('ID')}; an OceanStor host can be "
                "in one host group only")
        if not groups:
            self._call("POST", "/hostgroup/associate", body={
                "ID": hg, "ASSOCIATEOBJTYPE": OBJ_HOST, "ASSOCIATEOBJID": host_id},
                tolerate=frozenset({HOST_ALREADY_IN_GROUP}))
        mv = self._ensure_group("/mappingview", _MV + host_id, {})
        for obj_type, group in ((OBJ_HOSTGROUP, hg), (OBJ_LUNGROUP, lg)):
            self._call("PUT", "/mappingview/create_associate", body={
                "ID": mv, "ASSOCIATEOBJTYPE": obj_type, "ASSOCIATEOBJID": group},
                tolerate=GROUP_ALREADY_IN_VIEW)
        return lg

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        seen = self._host_luns(host_id)
        if volume_id in seen:
            return Mapping(volume_id=volume_id, host_id=host_id, lun=seen[volume_id])
        lg = self._ensure_view(host_id)
        self._call("POST", "/lungroup/associate", body={
            "ID": lg, "ASSOCIATEOBJTYPE": OBJ_LUN, "ASSOCIATEOBJID": volume_id},
            tolerate=frozenset({LUN_ALREADY_IN_GROUP, 1077948997}))
        seen = self._host_luns(host_id)
        if volume_id not in seen:
            raise ArrayError(f"LUN {volume_id} not visible to host {host_id} after mapping")
        return Mapping(volume_id=volume_id, host_id=host_id, lun=seen[volume_id])

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        lg = self._by_name("/lungroup", _LG + host_id)
        if lg is None:
            return
        self._call("DELETE", "/lungroup/associate", body={
            "ID": lg["ID"], "ASSOCIATEOBJTYPE": OBJ_LUN, "ASSOCIATEOBJID": volume_id},
            missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        groups = self._list("/lungroup/associate", {"ASSOCIATEOBJTYPE": OBJ_LUN,
                                                    "ASSOCIATEOBJID": volume_id})
        out = []
        for g in groups:
            name = str(g.get("NAME", ""))
            if not name.startswith(_LG):
                continue
            host_id = name[len(_LG):]
            out.append(Mapping(volume_id=volume_id, host_id=host_id,
                               lun=self._host_luns(host_id).get(volume_id)))
        return out
