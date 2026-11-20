"""HPE Alletra 5000/6000 and Nimble Storage, NimbleOS REST API v1.

The endpoint normally includes the API port: ``https://array:5392``.

Auth: ``POST /v1/tokens {"data": {"username", "password"}}`` returns
``data.session_token`` (sent as ``X-Auth-Token``) and ``data.id`` (used to
release the token with ``DELETE /v1/tokens/<id>``). Every request and
response body is wrapped in ``{"data": ...}``; errors are
``{"messages": [{"code": "SM_...", "severity": "error", "text": "..."}]}``.

Volumes: sizes are in MiB. ``Volume.id`` is the NimbleOS object id. A volume
must be taken offline (``PUT {"data": {"online": false}}``) before it can be
deleted.

Hosts are *initiator groups* (``access_protocol`` ``iscsi`` or ``fc``); the
host id is the group id. Initiators are added one by one with ``POST
/v1/initiators``; FC WWPNs use the colon form. Before adding, the adapter
looks each initiator up and raises :class:`ArrayConflictError` if another
group owns it (whether NimbleOS itself forbids an initiator in two groups is
not relied on). Mappings are *access control records* (ACRs) with
``apply_to: "both"`` (volume and its snapshots); the array assigns the LUN.

Volume identity: NimbleOS volumes report a 32-hex ``serial_number`` and
present it to hosts as a 16-byte **EUI-64** designator (multipath WWID
``2<serial>``, sysfs ``wwid`` ``eui.<serial>``), not an NAA. The adapter
reports it as ``wwn="eui.<serial>"`` (``Volume.wwn`` holds the SCSI
designator, NAA or EUI-64), so the identifier equals the host's sysfs wwid.

Verify against a real system
----------------------------
* The EUI-64 derivation above (``eui.`` + ``serial_number`` lowercase), both
  for iSCSI and FC, on NimbleOS 5.x/6.x.
* Error codes: ``SM_http_unauthorized`` (401) for bad login and expired
  tokens; ``SM_eexist`` (409) for duplicate names and ACRs;
  ``SM_http_not_found``/``SM_enoent`` (404) for unknown ids; ``SM_vol_online``
  (409, deleting an online volume); ``SM_vol_has_acl`` / ACR-related refusal
  when offlining a connected volume; ``SM_invalid_arg`` (400). The adapter
  keys on HTTP status, and treats 400 + ``SM_enoent`` / "not found" as
  not-found.
* Whether ``GET /v1/volumes/<id>`` for a malformed id answers 400 or 404
  (both are treated as "missing" by ``get_volume``).
* Query filtering on any attribute (``/v1/initiators/detail?iqn=...``,
  ``/v1/access_control_records/detail?vol_id=...``) and that a filtered list
  with no hits returns ``data: []`` rather than 404.
* LUN assignment: with volume-scoped iSCSI targets (the default on recent
  NimbleOS) every iSCSI ACR shows ``lun`` 0; group-scoped targets and FC get
  distinct LUNs per initiator group.
* That ``label`` is required for iSCSI initiators (max 64 chars) and is
  unique only within its group.
* Shrinking (``PUT size`` below current) is refused; we never request it.
"""
from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import quote

import requests

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
    colon_wwpn,
    normalize_wwpn,
)
from ..http import body_text, json_of

MIB = 1 << 20
_NOT_FOUND_CODES = frozenset({"SM_enoent", "SM_http_not_found", "SM_no_such_object"})


def nimble_eui(serial: str) -> str:
    return "eui." + serial.lower()


class HpeNimbleAdapter(ArrayAdapter):
    vendor = "hpe-nimble"
    display_name = "HPE Alletra 5000/6000 / Nimble"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC}),
        size_granularity_bytes=MIB,
        min_size_bytes=MIB,
        max_name_len=63,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("pool", "Pool name new volumes are created in (default: the "
                  "array's default pool)."),
        OptionDoc("performance_policy", "Performance policy name for new volumes "
                  "(default: the array default)."),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._token_id: str | None = None
        self._create_extra: dict[str, str] | None = None

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop("X-Auth-Token", None)
        data = self._data("POST", "/v1/tokens",
                          json={"data": {"username": self.secret("username"),
                                         "password": self.secret("password")}})
        token = (data or {}).get("session_token")
        if not token:
            raise ArrayRequestError("NimbleOS login returned no session_token")
        self._token_id = data.get("id")
        self.session.headers["X-Auth-Token"] = token

    def close(self) -> None:
        token_id, self._token_id = self._token_id, None
        if token_id and "X-Auth-Token" in self.session.headers:
            try:
                self.session.delete(f"/v1/tokens/{quote(token_id, safe='')}",
                                    ok=(401, 404))
            except Exception:
                pass
        self.session.headers.pop("X-Auth-Token", None)

    # -- request helpers -----------------------------------------------------

    def _request(self, method: str, path: str, *, params: Any = None, json: Any = None,
                 missing_ok: bool = False) -> requests.Response | None:
        resp = self.session.request(method, path, params=params, json=json,
                                    ok=(400, 401, 403, 404, 409))
        status = resp.status_code
        if status < 400:
            return resp
        codes: list[str] = []
        text = body_text(resp)
        try:
            messages = (resp.json() or {}).get("messages") or []
            codes = [str(m.get("code", "")) for m in messages]
            text = "; ".join(f"{m.get('code', '')}: {m.get('text', '')}" for m in messages) or text
        except (ValueError, AttributeError):
            pass
        msg = f"{method} {path} -> HTTP {status}: {text}"
        if status in (401, 403):
            raise ArrayAuthError(msg)
        if status == 404 or any(c in _NOT_FOUND_CODES for c in codes):
            if missing_ok:
                return None
            raise ArrayNotFoundError(msg)
        if status == 409 or "SM_eexist" in codes:
            raise ArrayConflictError(msg)
        raise ArrayRequestError(msg, status=status)

    def _data(self, method: str, path: str, **kw: Any) -> Any:
        resp = self._request(method, path, **kw)
        if resp is None:
            return None
        return (json_of(resp) or {}).get("data")

    def _list(self, path: str, **filters: str) -> list[dict[str, Any]]:
        return list(self._data("GET", path, params=filters) or [])

    @staticmethod
    def _volume(item: dict[str, Any]) -> Volume:
        serial = item.get("serial_number") or ""
        return Volume(id=item["id"], name=item["name"], size_bytes=int(item["size"]) * MIB,
                      wwn=nimble_eui(serial) if serial else None)

    def _lookup_id(self, path: str, name: str, what: str) -> str:
        items = self._list(path, name=name)
        if not items:
            raise ArrayError(f"{self.display_name} array {self.config.id}: "
                             f"{what} {name!r} not found")
        return str(items[0]["id"])

    def _volume_extra(self) -> dict[str, str]:
        if self._create_extra is None:
            extra: dict[str, str] = {}
            if self.option("pool"):
                extra["pool_id"] = self._lookup_id("/v1/pools/detail", self.option("pool"),
                                                   "pool")
            if self.option("performance_policy"):
                extra["perfpolicy_id"] = self._lookup_id(
                    "/v1/performance_policies/detail", self.option("performance_policy"),
                    "performance policy")
            self._create_extra = extra
        return self._create_extra

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        arrays = self._list("/v1/arrays/detail")
        arr = arrays[0] if arrays else {}
        return ArrayInfo(vendor=self.vendor, model=str(arr.get("model", "")),
                         version=str(arr.get("version", "")),
                         serial=str(arr.get("serial", "")))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        body = {"name": name, "size": -(-size_bytes // MIB), **self._volume_extra()}
        return self._volume(self._data("POST", "/v1/volumes", json={"data": body}))

    def get_volume(self, volume_id: str) -> Volume | None:
        try:
            data = self._data("GET", f"/v1/volumes/{quote(volume_id, safe='')}",
                              missing_ok=True)
        except ArrayRequestError as e:
            if e.status == 400:  # malformed id: cannot exist
                return None
            raise
        return self._volume(data) if data else None

    def find_volume(self, name: str) -> Volume | None:
        items = [i for i in self._list("/v1/volumes/detail", name=name) if i.get("name") == name]
        return self._volume(items[0]) if items else None

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        data = self._data("PUT", f"/v1/volumes/{quote(volume_id, safe='')}",
                          json={"data": {"size": -(-size_bytes // MIB)}})
        return self._volume(data)

    def delete_volume(self, volume_id: str) -> None:
        path = f"/v1/volumes/{quote(volume_id, safe='')}"
        self._data("PUT", path, json={"data": {"online": False}})
        self._request("DELETE", path)

    # -- hosts (initiator groups) -------------------------------------------

    def _group(self, name: str) -> dict[str, Any] | None:
        items = [g for g in self._list("/v1/initiator_groups/detail", name=name)
                 if g.get("name") == name]
        return items[0] if items else None

    def _owner(self, protocol: str, key: str, value: str) -> str | None:
        """Name of the initiator group that owns ``value`` (iqn or wwpn)."""
        for i in self._list("/v1/initiators/detail", access_protocol=protocol, **{key: value}):
            have = str(i.get(key, ""))
            same = (normalize_wwpn(have) == normalize_wwpn(value) if key == "wwpn"
                    else have.lower() == value.lower())
            if same:
                return str(i.get("initiator_group_name") or i.get("initiator_group_id"))
        return None

    def ensure_host(self, spec: HostSpec) -> str:
        unsupported = [p for p in spec.protocols if not p.is_scsi]
        if unsupported:
            raise ArrayError(f"{self.display_name} adapter does not support "
                             + ", ".join(sorted(p.value for p in unsupported)))
        iqns = spec.addresses(Protocol.ISCSI)
        wwpns = [colon_wwpn(w) for w in spec.addresses(Protocol.FC)]
        if iqns and wwpns:
            raise ArrayError(f"{self.display_name}: an initiator group is either iSCSI or "
                             "FC; register the host once per protocol")
        protocol, key, wanted = ("iscsi", "iqn", iqns) if iqns else ("fc", "wwpn", wwpns)

        group = self._group(spec.name)
        if group is not None and group.get("access_protocol") != protocol:
            raise ArrayConflictError(
                f"Initiator group {spec.name} exists with access_protocol "
                f"{group.get('access_protocol')}, not {protocol}")
        if group is not None:
            list_key = "iscsi_initiators" if protocol == "iscsi" else "fc_initiators"
            have = {str(i.get(key, "")).lower() for i in group.get(list_key) or []}
            wanted = [w for w in wanted if w.lower() not in have]
        for value in wanted:
            owner = self._owner(protocol, key, value)
            if owner is not None and owner != spec.name:
                raise ArrayConflictError(
                    f"Initiator {value} already belongs to initiator group {owner}")
        if group is None:
            group = self._data("POST", "/v1/initiator_groups",
                               json={"data": {"name": spec.name, "access_protocol": protocol,
                                              "description": "simplyblock"}})
        group_id = str(group["id"])
        for value in wanted:
            body: dict[str, Any] = {"initiator_group_id": group_id,
                                    "access_protocol": protocol, key: value}
            if protocol == "iscsi":
                body["label"] = f"sb-{hashlib.sha1(value.encode()).hexdigest()[:12]}"
            self._data("POST", "/v1/initiators", json={"data": body})
        return group_id

    def delete_host(self, host_id: str) -> None:
        self._request("DELETE", f"/v1/initiator_groups/{quote(host_id, safe='')}",
                      missing_ok=True)

    # -- mappings (access control records) -----------------------------------

    def _acrs(self, volume_id: str, host_id: str | None = None) -> list[dict[str, Any]]:
        items = self._list("/v1/access_control_records/detail", vol_id=volume_id)
        return [a for a in items if a.get("vol_id") == volume_id and a.get("initiator_group_id")
                and (host_id is None or a.get("initiator_group_id") == host_id)]

    @staticmethod
    def _mapping(acr: dict[str, Any]) -> Mapping:
        lun = acr.get("lun")
        return Mapping(volume_id=acr["vol_id"], host_id=acr["initiator_group_id"],
                       lun=int(lun) if lun is not None else None)

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        existing = self._acrs(volume_id, host_id)
        if existing:
            return self._mapping(existing[0])
        acr = self._data("POST", "/v1/access_control_records",
                         json={"data": {"vol_id": volume_id, "initiator_group_id": host_id,
                                        "apply_to": "both"}})
        return self._mapping(acr)

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        for acr in self._acrs(volume_id, host_id):
            self._request("DELETE",
                          f"/v1/access_control_records/{quote(str(acr['id']), safe='')}",
                          missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return [self._mapping(a) for a in self._acrs(volume_id)]
