"""Dell PowerMax, Unisphere for PowerMax REST API (10.x).

Auth: HTTP basic on every request (no session). Provisioning lives under
``/univmax/restapi/{api_version}/sloprovisioning/symmetrix/{symmetrix_id}``.
Errors are ``{"message": "..."}``, mostly with status 400; 404 for missing
objects; 401 for a rejected credential.

Layout (one object set per simplyblock volume):

* storage group ``<volume name>`` (``sb-<hex>``) in the configured SRP /
  service level, holding exactly one device;
* the device's ``volume_identifier`` is the same name;
* one masking view per (storage group, host): ``<sg>_<host>`` (or
  ``<sg>_<hash>`` when that exceeds 64 characters), joining the storage
  group, the host (initiator group) and the port group configured for the
  host's protocol.

A storage group per volume keeps map/unmap/delete independent per volume
(PowerMax maps storage groups, not devices), at the cost of one SG and one
masking view per mapping; arrays allow tens of thousands of each.

``Volume.id`` is the 5-hex-digit device id (``0012A``). ``Volume.wwn`` is
``naa.`` + the device's ``effective_wwn`` (falls back to ``wwn``). Volume
identifiers are *not* unique on PowerMax; lookups by name prefer the device
in the storage group of that name and refuse to guess between several.

Sizes: devices are allocated in cylinders; on PowerMaxOS (FBA) one cylinder
is 15 tracks x 128 KiB = 1920 KiB, so the size granularity is 1 966 080
bytes and all sizes are sent as ``capacityUnit: "CYL"`` (exact, no GB/MB
rounding). ``cap_cyl`` is read back.

Async jobs: with ``async_jobs`` the adapter sends
``executionOption: ASYNCHRONOUS`` and polls ``/{ver}/system/job/{jobId}`` on
``202 Accepted``; otherwise it sends ``SYNCHRONOUS`` but still follows a 202
should the server answer with a job.

Verify against a real system
----------------------------
* Whether ``sloId: "None"`` (no service level) is accepted on 10.x, and that
  ``workloadSelection`` may be omitted.
* The empty answer of ``GET .../volume?volume_identifier=x`` (assumed:
  ``count: 0`` with no ``resultList``) and whether the filter is exact or
  case-insensitive; iterator paging (``/common/Iterator``) is not followed --
  fine for name lookups that match one device.
* The ``storageGroupId`` filter on ``GET .../volume``.
* Status codes and wording for: SG / host / masking view name already in use,
  initiator already in another initiator group (classified by "already" /
  "in use" / "exist" / "member of"), removing the last device of a masked SG,
  deleting a device still in an SG, shrink.
* Whether a device must be removed from its SG before ``DELETE volume`` (done
  here) and whether 10.x still needs a deallocation step first (not done).
* Whether iSCSI IQNs / NVMe host NQNs must already be logged in to the array
  before ``POST host`` accepts them (FC WWNs need not be).
* NVMe/TCP: initiator ids for NVMe hosts (assumed: the host NQN) and whether
  the volume object carries an ``nguid`` field (read if present).
* ``GET maskingview/{mv}/connections?volume_id=`` returns rows only for
  initiator/port pairs (assumed: one row per initiator x port, logged in or
  not); ``host_lun_address`` is hex. With no rows the mapping's LUN is None.
* Job states (``SUCCEEDED`` / ``FAILED`` / ``VALIDATE_FAILED``; others are
  in progress) and that the failure text is in ``result``.
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Callable
from typing import Any

import requests

from ..base import (
    ArrayAdapter,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    Capabilities,
    HostSpec,
    Mapping,
    OptionDoc,
    Protocol,
    Volume,
)
from ..http import json_of

CYLINDER_BYTES = 1920 * 1024
MAX_NAME = 64
_JOB_DONE = "SUCCEEDED"
_JOB_FAILED = frozenset({"FAILED", "VALIDATE_FAILED"})
_JOB_POLL_S = 2.0

_NOT_FOUND = ("cannot find", "not found", "does not exist", "no such")
_CONFLICT = ("already", "in use", "exists", "member of", "is part of", "masking view")


def _message(resp: requests.Response) -> str:
    try:
        body = json_of(resp)
    except ArrayRequestError:
        body = None
    if isinstance(body, dict) and body.get("message"):
        return str(body["message"])
    return (resp.text or "")[:600] or f"HTTP {resp.status_code}"


def _classify(what: str, text: str, status: int | None = None) -> ArrayError:
    lowered = text.lower()
    msg = f"{what}: {text}"
    if status == 404 or any(k in lowered for k in _NOT_FOUND):
        return ArrayNotFoundError(msg)
    if any(k in lowered for k in _CONFLICT):
        return ArrayConflictError(msg)
    return ArrayRequestError(msg, status=status)


def powermax_wwn(item: dict[str, Any]) -> str | None:
    v = str(item.get("effective_wwn") or item.get("wwn") or "").strip().lower()
    v = v.removeprefix("naa.")
    return f"naa.{v}" if v else None


def initiator_protocol(initiator_id: str) -> Protocol:
    """Protocol of an initiator-group member as Unisphere lists it."""
    v = initiator_id.lower()
    if v.startswith(("iqn.", "eui.")):
        return Protocol.ISCSI
    if v.startswith("nqn."):
        return Protocol.NVME_TCP
    return Protocol.FC


class DellPowerMaxAdapter(ArrayAdapter):
    vendor = "dell-powermax"
    display_name = "Dell PowerMax"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP}),
        size_granularity_bytes=CYLINDER_BYTES,
        min_size_bytes=CYLINDER_BYTES,
        max_name_len=MAX_NAME,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("symmetrix_id", "12-digit array serial (symmetrix id), e.g. '000197900123'.",
                  required=True),
        OptionDoc("srp", "Storage resource pool for new storage groups.", default="SRP_1"),
        OptionDoc("service_level", "Service level (e.g. 'Diamond'); default: none."),
        OptionDoc("port_group", "Port group for masking views: a name, or a map from "
                  "protocol ('iscsi', 'fc', 'nvme-tcp') to name. Needed to map volumes."),
        OptionDoc("api_version", "Unisphere REST version in the URL ('100' = 10.0, "
                  "'101' = 10.1, ...).", default="100"),
        OptionDoc("async_jobs", "Run modifying calls as Unisphere jobs and poll them "
                  "(recommended for large arrays where synchronous calls time out).",
                  default=False),
        OptionDoc("job_timeout_s", "How long to wait for an asynchronous job.", default=600),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._base = f"/univmax/restapi/{self.option('api_version')}"
        self._sym = str(self.option("symmetrix_id"))
        self._slo = f"{self._base}/sloprovisioning/symmetrix/{self._sym}"
        self._sleep: Callable[[float], None] = self.session.sleep

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.set_basic_auth(self.secret("username"), self.secret("password"))

    def close(self) -> None:
        self.session.clear_auth()

    # -- request helpers -----------------------------------------------------

    def _exec(self) -> str:
        return "ASYNCHRONOUS" if self.option("async_jobs") else "SYNCHRONOUS"

    def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              json: Any = None, missing_ok: bool = False) -> Any:
        resp = self.session.request(method, path, params=params, json=json, ok=(400, 404))
        if resp.status_code in (400, 404):
            err = _classify(f"{method} {path} -> HTTP {resp.status_code}", _message(resp),
                            resp.status_code)
            if missing_ok and isinstance(err, ArrayNotFoundError):
                return None
            raise err
        body = json_of(resp)
        if resp.status_code == 202 and isinstance(body, dict) and body.get("jobId"):
            return self._wait_job(str(body["jobId"]), f"{method} {path}")
        return body

    def _wait_job(self, job_id: str, what: str) -> dict[str, Any]:
        polls = max(1, math.ceil(float(self.option("job_timeout_s")) / _JOB_POLL_S))
        for _ in range(polls):
            job = json_of(self.session.get(f"{self._base}/system/job/{job_id}")) or {}
            status = str(job.get("status", "")).upper()
            if status == _JOB_DONE:
                return job
            if status in _JOB_FAILED:
                raise _classify(f"{what} (job {job_id} {status})",
                                str(job.get("result") or "no result"))
            self._sleep(_JOB_POLL_S)
        raise ArrayRequestError(f"{what}: job {job_id} did not finish within "
                                f"{self.option('job_timeout_s')} s")

    @staticmethod
    def _volume(item: dict[str, Any]) -> Volume:
        nguid = str(item.get("nguid") or "").strip().lower().removeprefix("eui.")
        return Volume(id=item["volumeId"], name=item.get("volume_identifier") or "",
                      size_bytes=int(item["cap_cyl"]) * CYLINDER_BYTES,
                      wwn=powermax_wwn(item), nguid=f"eui.{nguid}" if nguid else None)

    def _volume_item(self, device_id: str) -> dict[str, Any] | None:
        return self._call("GET", f"{self._slo}/volume/{device_id}", missing_ok=True)

    def _require_item(self, device_id: str) -> dict[str, Any]:
        item = self._volume_item(device_id)
        if item is None:
            raise ArrayNotFoundError(f"PowerMax device {device_id} not found")
        return item

    def _device_ids(self, **filters: str) -> list[str]:
        body = self._call("GET", f"{self._slo}/volume", params=filters) or {}
        rows = (body.get("resultList") or {}).get("result") or []
        return [r["volumeId"] for r in rows if r.get("volumeId")]

    @staticmethod
    def _cyl(size_bytes: int) -> int:
        return -(-int(size_bytes) // CYLINDER_BYTES)

    def _attrs(self, name: str, size_bytes: int) -> dict[str, Any]:
        return {"num_of_vols": 1, "volume_size": str(self._cyl(size_bytes)),
                "capacityUnit": "CYL",
                "volumeIdentifier": {"identifier_name": name,
                                     "volumeIdentifierChoice": "identifier_name"}}

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        about = self._call("GET", "/univmax/restapi/version") or {}
        supported = about.get("supported_api_versions")
        if supported and str(self.option("api_version")) not in [str(v) for v in supported]:
            raise ArrayUnsupportedError(
                f"Unisphere {about.get('version', '')} does not serve REST version "
                f"{self.option('api_version')} (supported: {', '.join(map(str, supported))})")
        sym = self._call("GET", f"{self._base}/system/symmetrix/{self._sym}") or {}
        version = sym.get("ucode") or ""
        unisphere = about.get("version") or ""
        return ArrayInfo(vendor=self.vendor, model=sym.get("model", ""),
                         version=f"PowerMaxOS {version} (Unisphere {unisphere})".strip(),
                         serial=sym.get("symmetrixId", self._sym))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        attrs = self._attrs(name, size_bytes)
        payload: dict[str, Any] = {
            "srpId": self.option("srp"),
            "storageGroupId": name,
            "emulation": "FBA",
            "sloBasedStorageGroupParam": [{
                "sloId": self.option("service_level") or "None",
                "volumeAttributes": [attrs],
            }],
            "executionOption": self._exec(),
        }
        try:
            self._call("POST", f"{self._slo}/storagegroup", json=payload)
        except ArrayConflictError:
            # Left over from an interrupted create or delete: reuse it if empty.
            sg = self._call("GET", f"{self._slo}/storagegroup/{name}", missing_ok=True)
            if sg is None or int(sg.get("num_of_vols") or 0):
                raise
            self._call("PUT", f"{self._slo}/storagegroup/{name}", json={
                "editStorageGroupActionParam": {"expandStorageGroupParam": {"addVolumeParam": {
                    "create_new_volumes": True, "emulation": "FBA",
                    "volumeAttributes": [attrs]}}},
                "executionOption": self._exec()})
        devices = self._device_ids(storageGroupId=name)
        if len(devices) != 1:
            raise ArrayRequestError(
                f"PowerMax storage group {name} holds {len(devices)} devices after create")
        return self._volume(self._require_item(devices[0]))

    def get_volume(self, volume_id: str) -> Volume | None:
        item = self._volume_item(volume_id)
        return self._volume(item) if item else None

    def find_volume(self, name: str) -> Volume | None:
        items = [i for i in (self._volume_item(d) for d in self._device_ids(volume_identifier=name))
                 if i and i.get("volume_identifier") == name]
        if len(items) > 1:
            own = [i for i in items if name in (i.get("storageGroupId") or [])]
            if len(own) != 1:
                raise ArrayConflictError(
                    f"{len(items)} PowerMax devices are named {name}: "
                    + ", ".join(i["volumeId"] for i in items))
            items = own
        return self._volume(items[0]) if items else None

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        self._call("PUT", f"{self._slo}/volume/{volume_id}", json={
            "editVolumeActionParam": {"expandVolumeParam": {"volumeAttribute": {
                "volume_size": str(self._cyl(size_bytes)), "capacityUnit": "CYL"}}},
            "executionOption": self._exec()})
        return self._volume(self._require_item(volume_id))

    def delete_volume(self, volume_id: str) -> None:
        item = self._require_item(volume_id)
        name = item.get("volume_identifier") or ""
        for sg in item.get("storageGroupId") or []:
            self._call("PUT", f"{self._slo}/storagegroup/{sg}", json={
                "editStorageGroupActionParam": {"removeVolumeParam": {"volumeId": [volume_id]}},
                "executionOption": self._exec()})
            if sg == name:
                self._call("DELETE", f"{self._slo}/storagegroup/{sg}", missing_ok=True)
        self._call("DELETE", f"{self._slo}/volume/{volume_id}")

    # -- hosts ------------------------------------------------------------------

    def ensure_host(self, spec: HostSpec) -> str:
        wanted = [i.normalized() for i in spec.initiators]
        host = self._call("GET", f"{self._slo}/host/{spec.name}", missing_ok=True)
        if host is None:
            self._call("POST", f"{self._slo}/host", json={
                "hostId": spec.name, "initiatorId": wanted, "executionOption": self._exec()})
            return spec.name
        have = {str(i).lower() for i in host.get("initiator") or []}
        missing = [w for w in wanted if w.lower() not in have]
        if missing:
            self._call("PUT", f"{self._slo}/host/{spec.name}", json={
                "editHostActionParam": {"addInitiatorParam": {"initiator": missing}},
                "executionOption": self._exec()})
        return spec.name

    def delete_host(self, host_id: str) -> None:
        self._call("DELETE", f"{self._slo}/host/{host_id}", missing_ok=True)

    # -- mappings ------------------------------------------------------------------

    @staticmethod
    def view_name(sg: str, host: str) -> str:
        name = f"{sg}_{host}"
        if len(name) <= MAX_NAME:
            return name
        return f"{sg}_{hashlib.sha256(host.encode()).hexdigest()[:16]}"

    def _port_group(self, host_id: str) -> str:
        option = self.option("port_group")
        if not option:
            raise ArrayError(f"{self.display_name} array {self.config.id} needs the "
                             "'port_group' option to map volumes")
        if isinstance(option, str):
            return option
        host = self._call("GET", f"{self._slo}/host/{host_id}")
        protocols = {initiator_protocol(str(i)) for i in host.get("initiator") or []}
        if len(protocols) != 1:
            raise ArrayError(f"Cannot tell the protocol of PowerMax host {host_id}")
        protocol = protocols.pop()
        pg = option.get(protocol.value)
        if not pg:
            raise ArrayError(f"No PowerMax port group configured for {protocol.value} "
                             f"(option 'port_group')")
        return str(pg)

    def _views(self, item: dict[str, Any]) -> list[tuple[str, str, str]]:
        """(masking view, host or host group, storage group) for a device."""
        out = []
        for sg in item.get("storageGroupId") or []:
            sg_item = self._call("GET", f"{self._slo}/storagegroup/{sg}", missing_ok=True)
            for mv in (sg_item or {}).get("maskingview") or []:
                mv_item = self._call("GET", f"{self._slo}/maskingview/{mv}", missing_ok=True)
                if mv_item:
                    host = mv_item.get("hostId") or mv_item.get("hostGroupId")
                    if host:
                        out.append((mv, str(host), sg))
        return out

    def _lun(self, view: str, device_id: str) -> int | None:
        body = self._call("GET", f"{self._slo}/maskingview/{view}/connections",
                          params={"volume_id": device_id}, missing_ok=True) or {}
        for row in body.get("maskingViewConnection") or []:
            if row.get("volumeId") == device_id and row.get("host_lun_address"):
                return int(str(row["host_lun_address"]), 16)
        return None

    def _mappings(self, item: dict[str, Any]) -> list[Mapping]:
        seen: dict[str, Mapping] = {}
        for view, host, _sg in self._views(item):
            if host not in seen:
                seen[host] = Mapping(volume_id=item["volumeId"], host_id=host,
                                     lun=self._lun(view, item["volumeId"]))
        return list(seen.values())

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        item = self._require_item(volume_id)
        for m in self._mappings(item):
            if m.host_id == host_id:
                return m
        sg = item.get("volume_identifier") or ""
        if sg not in (item.get("storageGroupId") or []):
            raise ArrayConflictError(
                f"PowerMax device {volume_id} is not in its own storage group {sg!r}")
        if self._call("GET", f"{self._slo}/host/{host_id}", missing_ok=True) is None:
            raise ArrayNotFoundError(f"PowerMax host {host_id} not found")
        self._call("POST", f"{self._slo}/maskingview", json={
            "maskingViewId": self.view_name(sg, host_id),
            "hostOrHostGroupSelection": {"useExistingHostParam": {"hostId": host_id}},
            "portGroupSelection": {"useExistingPortGroupParam": {
                "portGroupId": self._port_group(host_id)}},
            "storageGroupSelection": {"useExistingStorageGroupParam": {"storageGroupId": sg}},
            "executionOption": self._exec()})
        for m in self._mappings(self._require_item(volume_id)):
            if m.host_id == host_id:
                return m
        raise ArrayRequestError(
            f"PowerMax masked device {volume_id} to host {host_id} but lists no mapping")

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        item = self._volume_item(volume_id)
        if item is None:
            return
        own = item.get("volume_identifier") or ""
        for view, host, sg in self._views(item):
            if host != host_id:
                continue
            if sg != own:
                raise ArrayConflictError(
                    f"PowerMax device {volume_id} is mapped to {host_id} through shared "
                    f"storage group {sg} (masking view {view}); unmap it on the array")
            self._call("DELETE", f"{self._slo}/maskingview/{view}", missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        item = self._volume_item(volume_id)
        return self._mappings(item) if item else []
