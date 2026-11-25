"""Hitachi VSP (VSP One Block, VSP E series, VSP 5000, VSP G/F) through the
Configuration Manager REST API.

Deployment modes (option ``storage_device_id``):

* embedded (option absent): the API runs on the SVP / controller and objects
  live under ``/ConfigurationManager/v1/objects/...``;
* REST API server (Ops Center API Configuration Manager): the storage system
  must already be registered on the server; objects live under
  ``/ConfigurationManager/v1/objects/storages/{storageDeviceId}/...``.

Auth: ``POST .../sessions`` with HTTP basic auth (a storage-system user)
returns ``{"token", "sessionId"}``; later requests send ``Authorization:
Session <token>``; ``DELETE .../sessions/{sessionId}`` discards it. An expired
session answers 401, which the client turns into one re-login.

Asynchronous jobs: create/modify/delete requests answer ``202`` with a job
object (``jobId``, ``status``, ``state``, ``affectedResources``, ``error``).
The adapter polls ``GET .../jobs/{jobId}`` until ``status == "Completed"``;
``state`` is then ``Succeeded`` or ``Failed`` (``error`` carries the usual
error body). Polling gives up after ``job_timeout_s``; the job may still
finish on the array, and every operation is safe to re-run.

Object model:

* volume = DP-VOL LDEV in pool ``pool_id``; ``Volume.id`` is the decimal LDEV
  number, ``Volume.name`` the LDEV label (max 32 characters, not unique on the
  array). A deleted LDEV still answers ``GET /ldevs/{id}`` with
  ``emulationType == "NOT DEFINED"``, which reads as "missing".
* host = one host group (FC) / iSCSI target (iSCSI) of the same name on every
  configured port of the matching type; the host id is that name. Initiators
  are ``host-wwns`` / ``host-iscsis`` of each of those groups.
* mapping = one LU path per port (``POST /luns``); the adapter uses the same
  LUN number on every port and reports the LUN of the first configured port.

Volume identity: SCSI ``naa.<naaId>`` (lowercase).

Checked against vendor source (Hitachi's OpenStack Cinder driver
``cinder/volume/drivers/hitachi/hbsd_rest.py`` / ``hbsd_rest_api.py`` and the
Ansible collection ``hitachivantara.vspone_block``, ``plugins/module_utils/
gateway/gateway_manager.py``, ``vsp_volume.py``, ``vsp_host_group_gateway.py``,
``vsp_rg_lock_gateway.py``, ``common/vsp_constants.py``, ``vsp_utils.py``):

* Sessions: ``POST .../sessions`` with basic auth -> ``token``/``sessionId``,
  ``Authorization: Session <token>``, ``DELETE .../sessions/{id}``; 401 means
  re-login. Jobs: 202 + ``jobId``, done at ``status == "Completed"``,
  ``state == "Succeeded"``, new object = last segment of
  ``affectedResources[0]``; a failed job's ``error`` is the usual body.
* ``POST /ldevs {poolId, blockCapacity}`` without ``ldevId`` auto-assigns
  the number (``startLdevId``/``endLdevId`` restrict it); the label is set
  afterwards with ``PUT /ldevs/{id} {"label"}``, exactly as Cinder does.
  Expand: ``POST .../actions/expand/invoke {"parameters":
  {"additionalBlockCapacity"}}``. A deleted LDEV reads ``emulationType ==
  "NOT DEFINED"``.
* Lookup by label: there is no label filter; Cinder scans ``GET /ldevs?
  ldevOption=dpVolume&headLdevId=&count=16384`` stepping the head by
  ``count`` up to LDEV 65535 and ignoring KART40044-E (window past the
  end). This adapter does the same (it used to stop after the first short
  page, which missed LDEVs above the first 16k numbers).
* NAA: derived from the storage serial and the LDEV number (Ansible
  ``NAIDCalculator``), so a reused LDEV number has the same WWN.
* ``GET /ldevs/{id}`` lists LU paths under ``ports`` (``portId``,
  ``hostGroupNumber``, ``hostGroupName``, ``lun``); ``GET /host-wwns`` /
  ``/host-iscsis`` take ``portId`` + ``hostGroupNumber``; ``POST
  /host-groups {portId, hostGroupName, hostMode[, hostModeOptions]}``;
  ``POST /luns {portId, hostGroupNumber, ldevId, lun}``; ``DELETE
  /luns/{port},{gid},{lun}``.
* Resource-group lock: ``.../resource-group-service/actions/lock/invoke``
  with ``{"parameters": {"waitTime"}}`` lives under
  ``/ConfigurationManager/v1/services`` (embedded) or
  ``/ConfigurationManager/v1/{storageDeviceId}/services`` (REST server),
  not under ``/objects`` (fixed here). Ansible POSTs it, Cinder PUTs it.
* Error codes (classified before the text fallback): KART30013-E and
  ``EX_ENOOBJ`` = not found; KART40050-E..52-E, SSB 2E11/2205, 2E11/2207,
  ``EX_EACCES`` = resource group locked; SSB 2E22/0001 (LDEV already
  defined), B958/015A (LU path already defined), B958/0947 (another LDEV on
  that LUN) = conflict. Storage failures carry SSB1/SSB2 in ``errorCode``.

Verify against a real system:

* Older microcode that may require ``ldevId`` on ``POST /ldevs`` (set
  ``ldev_range`` there).
* A crash between LDEV create and label leaves an unlabeled LDEV the client
  cannot find (a label failure is cleaned up here, a crash is not).
* DP-VOL capacity: the adapter rounds to 1 MiB and creates at least 64 MiB
  (Cinder only creates whole GiB); the real floor per model.
* Whether every model returns ``naaId`` (Ansible computes it when absent;
  this adapter then reports no WWN).
* FC refusing a WWN in two host groups of one port and iSCSI allowing an
  initiator in several targets (the ownership scan does not rely on either).
* Which models need the resource-group lock (``use_lock``); Cinder retries
  lock errors for up to two hours, this adapter reports a conflict.
* NVMe-oF (``/nvm-subsystems``) is not implemented: only FC and iSCSI.
"""
from __future__ import annotations

import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from ..base import (
    ArrayAdapter,
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

API = "/ConfigurationManager/"
ROOT = "/ConfigurationManager/v1/objects"
BLOCK = 512
NOT_DEFINED = "NOT DEFINED"
MAX_LUN = 2047
#: ``count`` per ``GET /ldevs`` page (the API maximum). ``headLdevId`` steps
#: by this much, as in Hitachi's Cinder driver (``hbsd_rest.py``).
PAGE_SIZE = 16384
#: Highest LDEV number scanned when no ``ldev_range`` is set (Cinder's
#: ``_MAX_LDEV_ID``).
MAX_LDEV_ID = 65535

_PORT_RE = re.compile(r"^CL[0-9A-Z]+-[A-Z0-9]+$")
_RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
_PORT_TYPES = {"FIBRE": Protocol.FC, "ISCSI": Protocol.ISCSI}
#: HTTP statuses whose Hitachi error body the adapter classifies itself
#: (401/403 stay with RestSession -> ArrayAuthError).
_OWN_STATUSES = (400, 404, 405, 409, 412, 500, 502, 503, 504)

# Structured codes, from Hitachi's Cinder driver (hbsd_rest_api.py, hbsd_rest.py).
#: messageId of "the specified object does not exist".
_MSGID_NOT_FOUND = frozenset({"KART30013-E"})
#: messageIds of a resource-group lock held by someone else.
_MSGID_LOCKED = frozenset({"KART40050-E", "KART40051-E", "KART40052-E"})
#: (SSB1, SSB2) pairs: LDEV already defined, LU path already defined, another
#: LDEV already mapped to that LUN, resource group locked.
_SSB_CONFLICT = frozenset({("2E22", "0001"), ("B958", "015A"), ("B958", "0947"),
                           ("2E11", "2205"), ("2E11", "2207")})
#: errorCode.errorCode values.
_RC_NOT_FOUND = frozenset({"EX_ENOOBJ"})
_RC_CONFLICT = frozenset({"EX_EACCES"})
#: A ``GET /ldevs`` window beyond the array's last LDEV number.
_MSGID_BAD_RANGE = "KART40044-E"
_RC_BAD_RANGE = ("EX_ENLDEV", "EX_INVARG")

_CONFLICT_TEXT = ("already", "in use", "is used", "registered", "duplicat",
                  "lu path is defined", "lu paths are defined", "locked")
_MISSING_TEXT = ("does not exist", "not found", "not defined", "not exist")


def _monotonic() -> float:
    return time.monotonic()


def _classify(where: str, status: int | None, body: Any) -> ArrayError:
    """Map a Hitachi error body (``errorSource``, ``message``, ``solution``,
    ``messageId``, ``errorCode``) to an ArrayError."""
    if isinstance(body, dict):
        message = str(body.get("message") or body.get("error") or "")
        msg_id = str(body.get("messageId") or "")
        codes = body.get("errorCode")
        ssb = ""
        if isinstance(codes, dict) and codes:
            ssb = " (" + ", ".join(f"{k}={v}" for k, v in sorted(codes.items())) + ")"
        text = f"{where}: {msg_id + ' ' if msg_id else ''}{message}{ssb}".strip()
    else:
        message = str(body or "")
        text = f"{where}: {message}"
    if status:
        text += f" [HTTP {status}]"
    lowered = message.lower()
    if isinstance(body, dict):
        raw = body.get("errorCode")
        err: dict[str, Any] = raw if isinstance(raw, dict) else {}
        ssb_pair = (str(err.get("SSB1", "")).upper(), str(err.get("SSB2", "")).upper())
        rc = str(err.get("errorCode", ""))
        mid = str(body.get("messageId") or "")
        if mid in _MSGID_NOT_FOUND or rc in _RC_NOT_FOUND:
            return ArrayNotFoundError(text)
        if mid in _MSGID_LOCKED or ssb_pair in _SSB_CONFLICT or rc in _RC_CONFLICT:
            return ArrayConflictError(text)
    if status == 404:
        return ArrayNotFoundError(text)
    if status in (409, 412):
        return ArrayConflictError(text)
    if status is None or status in (400, 500):
        if any(t in lowered for t in _MISSING_TEXT):
            return ArrayNotFoundError(text)
        if any(t in lowered for t in _CONFLICT_TEXT):
            return ArrayConflictError(text)
    return ArrayRequestError(text, status=status)


def _tail(resource: str) -> str:
    """``/ConfigurationManager/v1/objects/ldevs/123`` -> ``123``."""
    return resource.rstrip("/").rsplit("/", 1)[-1]


class HitachiVspAdapter(ArrayAdapter):
    vendor = "hitachi-vsp"
    display_name = "Hitachi VSP"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.FC, Protocol.ISCSI}),
        size_granularity_bytes=1 << 20,
        min_size_bytes=64 << 20,
        # LDEV labels and iSCSI target names: 32 characters.
        max_name_len=32,
    )
    option_docs = (
        OptionDoc("pool_id", "DP (HDP/HDT) pool number volumes are created in.",
                  required=True),
        OptionDoc("ports", "Target ports hosts are presented on, e.g. "
                  "['CL1-A', 'CL2-A']. FC and iSCSI ports may be mixed; a host "
                  "uses the ports of its initiators' protocol.", required=True),
        OptionDoc("storage_device_id", "storageDeviceId (e.g. '886000123456') when "
                  "the endpoint is an Ops Center API Configuration Manager server; "
                  "leave unset for the API embedded on the SVP/controller."),
        OptionDoc("ldev_range", "Restrict new LDEV numbers (and volume lookups) "
                  "to 'start-end' (decimal LDEV ids)."),
        OptionDoc("host_mode", "hostMode of new host groups.", default="LINUX/IRIX"),
        OptionDoc("host_mode_options", "hostModeOptions numbers for new host groups.",
                  default=()),
        OptionDoc("job_timeout_s", "Seconds to wait for an asynchronous job.",
                  default=300),
        OptionDoc("use_lock", "Take the resource-group lock around configuration "
                  "changes (needed on some models/configurations).", default=False),
        OptionDoc("lock_wait_s", "waitTime for the resource-group lock.", default=30),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        ports = self.option("ports")
        if isinstance(ports, str):
            ports = [p for p in ports.replace(",", " ").split() if p]
        if not ports or not all(isinstance(p, str) and _PORT_RE.match(p.upper())
                                for p in ports):
            raise ArrayError(f"Hitachi VSP option 'ports' must list port ids like "
                             f"'CL1-A'; got {self.option('ports')!r}")
        self._ports: list[str] = list(dict.fromkeys(p.upper() for p in ports))
        try:
            self._pool = int(self.option("pool_id"))
        except (TypeError, ValueError) as e:
            raise ArrayError("Hitachi VSP option 'pool_id' must be a number") from e
        self._range: tuple[int, int] | None = None
        rng = self.option("ldev_range")
        if rng:
            m = _RANGE_RE.match(str(rng))
            if not m or int(m.group(1)) > int(m.group(2)):
                raise ArrayError(f"Hitachi VSP option 'ldev_range' must be 'start-end'; "
                                 f"got {rng!r}")
            self._range = (int(m.group(1)), int(m.group(2)))
        sdid = self.option("storage_device_id")
        self._base = f"{ROOT}/storages/{sdid}" if sdid else ROOT
        # Services are not under /objects: /ConfigurationManager/v1/services
        # (embedded, Hitachi Ansible vsp_rg_lock_gateway.py) or
        # /ConfigurationManager/v1/{storageDeviceId}/services (REST API
        # server, Hitachi Cinder hbsd_rest_api.py ``service_url``).
        self._services = (f"{API}v1/{sdid}/services" if sdid
                          else f"{API}v1/services")
        self._session_id: Any = None
        self._port_types: dict[str, Protocol | None] = {}

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.headers.pop("Authorization", None)
        self._session_id = None
        resp = self.session.post(f"{self._base}/sessions",
                                 auth=(self.secret("username"), self.secret("password")))
        body = json_of(resp) or {}
        token = body.get("token")
        if not token:
            raise ArrayRequestError("Hitachi VSP session request returned no token")
        self._session_id = body.get("sessionId")
        self.session.headers["Authorization"] = f"Session {token}"

    def close(self) -> None:
        if self._session_id is not None and "Authorization" in self.session.headers:
            try:
                self.session.delete(f"{self._base}/sessions/{self._session_id}")
            except Exception:
                pass
        self.session.headers.pop("Authorization", None)
        self._session_id = None

    # -- request helpers -----------------------------------------------------

    def _req(self, method: str, path: str, *, params: Any = None,
             json: Any = None) -> tuple[int, Any]:
        full = path if path.startswith(API) else f"{self._base}{path}"
        resp = self.session.request(method, full, params=params, json=json,
                                    ok=_OWN_STATUSES)
        try:
            body = json_of(resp)
        except ArrayRequestError:
            body = resp.text
        if resp.status_code >= 400:
            raise _classify(f"{method} {path}", resp.status_code, body)
        return resp.status_code, body

    def _get(self, path: str, **params: Any) -> Any:
        return self._req("GET", path, params=params or None)[1]

    def _data(self, path: str, **params: Any) -> list[dict[str, Any]]:
        return list((self._get(path, **params) or {}).get("data", []))

    def _run(self, method: str, path: str, json: Any = None) -> list[str]:
        """Issue a change request, wait for its job and return the job's
        ``affectedResources``."""
        status, body = self._req(method, path, json=json)
        if not isinstance(body, dict) or "jobId" not in body:
            return []
        job = self._wait(body)
        if job.get("state") != "Succeeded":
            raise _classify(f"{method} {path} (job {job.get('jobId')} {job.get('state')})",
                            None, job.get("error") or {"message": "job did not succeed"})
        return [str(r) for r in job.get("affectedResources") or []]

    def _wait(self, job: dict[str, Any]) -> dict[str, Any]:
        timeout = float(self.option("job_timeout_s"))
        sleep = self.session.sleep
        deadline = _monotonic() + timeout
        interval, waited = 0.5, 0.0
        job_id = job["jobId"]
        while job.get("status") != "Completed":
            if waited >= timeout or _monotonic() > deadline:
                raise ArrayRequestError(
                    f"Hitachi VSP job {job_id} not completed after {timeout:g}s "
                    f"(status {job.get('status')}, state {job.get('state')}); "
                    "it may still finish on the array")
            sleep(interval)
            waited += interval
            interval = min(interval * 2, 5.0)
            job = self._get(f"/jobs/{job_id}") or {}
        return job

    @contextmanager
    def _change(self) -> Iterator[None]:
        """Resource-group lock around a configuration change (``use_lock``)."""
        if not self.option("use_lock"):
            yield
            return
        svc = f"{self._services}/resource-group-service/actions"
        self._run("POST", f"{svc}/lock/invoke",
                  {"parameters": {"waitTime": int(self.option("lock_wait_s"))}})
        try:
            yield
        finally:
            try:
                self._run("POST", f"{svc}/unlock/invoke", {})
            except ArrayError:
                pass

    # -- volumes --------------------------------------------------------------

    @staticmethod
    def _defined(ldev: dict[str, Any] | None) -> bool:
        return bool(ldev) and (ldev or {}).get("emulationType", NOT_DEFINED) != NOT_DEFINED

    @staticmethod
    def _volume(ldev: dict[str, Any]) -> Volume:
        naa = str(ldev.get("naaId") or "").lower()
        return Volume(id=str(ldev["ldevId"]), name=str(ldev.get("label") or ""),
                      size_bytes=int(ldev["blockCapacity"]) * BLOCK,
                      wwn=f"naa.{naa}" if naa else None)

    def _ldev(self, volume_id: str) -> dict[str, Any] | None:
        if not str(volume_id).isdigit():
            return None
        try:
            ldev = self._get(f"/ldevs/{int(volume_id)}")
        except ArrayNotFoundError:
            return None
        return ldev if self._defined(ldev) else None

    def _require(self, volume_id: str) -> dict[str, Any]:
        ldev = self._ldev(volume_id)
        if ldev is None:
            raise ArrayNotFoundError(f"LDEV {volume_id} is not defined")
        return ldev

    def get_info(self) -> ArrayInfo:
        sdid = self.option("storage_device_id")
        info = self._get(f"{ROOT}/storages/{sdid}" if sdid else f"{ROOT}/storages/instance")
        info = info or {}
        return ArrayInfo(vendor=self.vendor, model=str(info.get("model", "")),
                         version=str(info.get("dkcMicroVersion", "")),
                         serial=str(info.get("serialNumber", "")))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        body: dict[str, Any] = {"poolId": self._pool, "blockCapacity": size_bytes // BLOCK}
        if self._range:
            body["startLdevId"], body["endLdevId"] = self._range
        with self._change():
            affected = self._run("POST", "/ldevs", body)
            ldev_ids = [_tail(r) for r in affected if "/ldevs/" in r]
            if not ldev_ids:
                raise ArrayRequestError(f"Hitachi VSP LDEV create for {name} reported no LDEV")
            ldev_id = ldev_ids[0]
            try:
                self._run("PUT", f"/ldevs/{ldev_id}", {"label": name})
            except ArrayError:
                # An unlabeled LDEV cannot be found again; do not leak it.
                try:
                    self._run("DELETE", f"/ldevs/{ldev_id}")
                except ArrayError:
                    pass
                raise
        return self._volume(self._require(ldev_id))

    def get_volume(self, volume_id: str) -> Volume | None:
        ldev = self._ldev(volume_id)
        return self._volume(ldev) if ldev else None

    def find_volume(self, name: str) -> Volume | None:
        # ``count`` is a window of LDEV numbers starting at ``headLdevId``, so
        # step the head by ``count`` (not by the last id returned) as Hitachi's
        # Cinder driver does; a window past the array's last LDEV number
        # answers KART40044-E / EX_ENLDEV and ends the scan. ``poolId`` is not
        # combined with ``headLdevId`` (no vendor client does); the pool is
        # checked on each entry instead.
        start, end = self._range if self._range else (0, MAX_LDEV_ID)
        found: dict[int, dict[str, Any]] = {}
        head = start
        while head <= end:
            count = min(PAGE_SIZE, end - head + 1)
            try:
                page = self._data("/ldevs", ldevOption="dpVolume", headLdevId=head,
                                  count=count)
            except ArrayError as e:
                past_end = _MSGID_BAD_RANGE in str(e) or any(
                    rc in str(e) for rc in _RC_BAD_RANGE)
                if past_end and head > start:  # never on the first window
                    break
                raise
            for ldev in page:
                lid = int(ldev["ldevId"])
                if (start <= lid <= end and ldev.get("label") == name
                        and self._defined(ldev)
                        and int(ldev.get("poolId", self._pool)) == self._pool):
                    found[lid] = ldev
            head += count
        if len(found) > 1:
            raise ArrayConflictError(
                f"Several LDEVs labeled {name!r}: " + ", ".join(str(i) for i in sorted(found)))
        if not found:
            return None
        # List entries may omit fields; read the LDEV itself.
        return self.get_volume(str(next(iter(found))))

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        ldev = self._require(volume_id)
        add = size_bytes // BLOCK - int(ldev["blockCapacity"])
        if add > 0:
            with self._change():
                self._run("POST", f"/ldevs/{ldev['ldevId']}/actions/expand/invoke",
                          {"parameters": {"additionalBlockCapacity": add}})
        return self._volume(self._require(volume_id))

    def delete_volume(self, volume_id: str) -> None:
        ldev = self._require(volume_id)
        with self._change():
            self._run("DELETE", f"/ldevs/{ldev['ldevId']}")

    # -- ports and host groups ------------------------------------------------------

    def _port_protocol(self, port: str) -> Protocol | None:
        if port not in self._port_types:
            info = self._get(f"/ports/{port}") or {}
            self._port_types[port] = _PORT_TYPES.get(str(info.get("portType", "")).upper())
        return self._port_types[port]

    def _ports_for(self, protocol: Protocol) -> list[str]:
        return [p for p in self._ports if self._port_protocol(p) == protocol]

    def _groups(self, port: str) -> list[dict[str, Any]]:
        return self._data("/host-groups", portId=port)

    @staticmethod
    def _by_name(groups: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
        return next((g for g in groups if g.get("hostGroupName") == name), None)

    def _host_groups(self, name: str) -> list[tuple[str, int]]:
        """(port, hostGroupNumber) of host ``name`` on every configured port."""
        out = []
        for port in self._ports:
            g = self._by_name(self._groups(port), name)
            if g is not None:
                out.append((port, int(g["hostGroupNumber"])))
        return out

    def _members(self, protocol: Protocol, port: str, number: int) -> set[str]:
        if protocol == Protocol.FC:
            items = self._data("/host-wwns", portId=port, hostGroupNumber=number)
            return {str(i.get("hostWwn", "")).lower() for i in items}
        items = self._data("/host-iscsis", portId=port, hostGroupNumber=number)
        return {str(i.get("iscsiName", "")).lower() for i in items}

    def _add_member(self, protocol: Protocol, port: str, number: int, address: str) -> None:
        if protocol == Protocol.FC:
            self._run("POST", "/host-wwns",
                      {"hostWwn": address, "portId": port, "hostGroupNumber": number})
        else:
            self._run("POST", "/host-iscsis",
                      {"iscsiName": address, "portId": port, "hostGroupNumber": number})

    def ensure_host(self, spec: HostSpec) -> str:
        wanted = {p: spec.addresses(p) for p in (Protocol.FC, Protocol.ISCSI)}
        wanted = {p: a for p, a in wanted.items() if a}
        plan: list[tuple[Protocol, str, list[dict[str, Any]]]] = []
        for protocol, addresses in wanted.items():
            ports = self._ports_for(protocol)
            if not ports:
                raise ArrayError(f"Hitachi VSP array {self.config.id} has no configured "
                                 f"{protocol.value} port (option 'ports': {self._ports})")
            for port in ports:
                groups = self._groups(port)
                # Ownership check before any change: FC refuses a WWN in two
                # host groups of a port, iSCSI does not, so scan both.
                for g in groups:
                    if g.get("hostGroupName") == spec.name:
                        continue
                    taken = self._members(protocol, port, int(g["hostGroupNumber"]))
                    clash = sorted(set(addresses) & taken)
                    if clash:
                        raise ArrayConflictError(
                            f"Initiator {clash[0]} is registered to host group "
                            f"{g.get('hostGroupName')!r} ({g.get('hostGroupId')}) "
                            f"on port {port}")
                plan.append((protocol, port, groups))
        with self._change():
            for protocol, port, groups in plan:
                current = self._by_name(groups, spec.name)
                if current is None:
                    body: dict[str, Any] = {"portId": port, "hostGroupName": spec.name,
                                            "hostMode": self.option("host_mode")}
                    if self.option("host_mode_options"):
                        body["hostModeOptions"] = [int(o) for o in
                                                   self.option("host_mode_options")]
                    affected = self._run("POST", "/host-groups", body)
                    ids = [_tail(r) for r in affected if "/host-groups/" in r]
                    if ids:
                        number = int(ids[0].split(",")[1])
                    else:
                        found = self._by_name(self._groups(port), spec.name)
                        if found is None:
                            raise ArrayRequestError(
                                f"Host group {spec.name} not found on {port} after create")
                        number = int(found["hostGroupNumber"])
                    have: set[str] = set()
                else:
                    number = int(current["hostGroupNumber"])
                    have = self._members(protocol, port, number)
                for address in wanted[protocol]:
                    if address.lower() not in have:
                        self._add_member(protocol, port, number, address)
        return spec.name

    def delete_host(self, host_id: str) -> None:
        targets = [(p, n) for p, n in self._host_groups(host_id) if n != 0]
        if not targets:
            return
        with self._change():
            for port, number in targets:
                try:
                    self._run("DELETE", f"/host-groups/{port},{number}")
                except ArrayNotFoundError:
                    pass

    # -- mappings --------------------------------------------------------------------

    def _paths(self, ldev: dict[str, Any], host_id: str | None = None
               ) -> list[dict[str, Any]]:
        order = {p: i for i, p in enumerate(self._ports)}
        paths = [p for p in ldev.get("ports") or []
                 if p.get("portId") in order
                 and (host_id is None or p.get("hostGroupName") == host_id)]
        return sorted(paths, key=lambda p: order[p["portId"]])

    def _mappings(self, ldev: dict[str, Any], host_id: str | None = None) -> list[Mapping]:
        out: dict[str, Mapping] = {}
        for p in self._paths(ldev, host_id):
            name = str(p.get("hostGroupName"))
            out.setdefault(name, Mapping(volume_id=str(ldev["ldevId"]), host_id=name,
                                         lun=int(p["lun"])))
        return list(out.values())

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        ldev = self._require(volume_id)
        groups = self._host_groups(host_id)
        if not groups:
            raise ArrayNotFoundError(f"Host {host_id} has no host group on "
                                     f"ports {', '.join(self._ports)}")
        present = {(p["portId"], int(p["hostGroupNumber"])): int(p["lun"])
                   for p in self._paths(ldev, host_id)}
        missing = [g for g in groups if g not in present]
        if missing:
            used: set[int] = set()
            for port, number in missing:
                used |= {int(lu["lun"]) for lu in
                         self._data("/luns", portId=port, hostGroupNumber=number)}
            preferred = next(iter(present.values()), None)
            lun = preferred if preferred is not None and preferred not in used else next(
                (n for n in range(MAX_LUN + 1) if n not in used), None)
            if lun is None:
                raise ArrayConflictError(f"Host {host_id} has no free LUN")
            with self._change():
                for port, number in missing:
                    self._run("POST", "/luns", {"portId": port, "hostGroupNumber": number,
                                                "ldevId": int(ldev["ldevId"]), "lun": lun})
            ldev = self._require(volume_id)
        mappings = self._mappings(ldev, host_id)
        if not mappings:
            raise ArrayRequestError(f"LDEV {volume_id} shows no path to {host_id} after mapping")
        return mappings[0]

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        ldev = self._ldev(volume_id)
        if ldev is None:
            return
        paths = self._paths(ldev, host_id)
        if not paths:
            return
        with self._change():
            for p in paths:
                try:
                    self._run("DELETE",
                              f"/luns/{p['portId']},{p['hostGroupNumber']},{p['lun']}")
                except ArrayNotFoundError:
                    pass

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return self._mappings(self._require(volume_id))
