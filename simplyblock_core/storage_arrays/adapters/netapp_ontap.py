"""NetApp ONTAP 9.x REST API (AFF/FAS/ASA; also Lenovo ThinkSystem DM).

Auth: HTTP basic (``username``/``password``) on every request; there is no
session to establish. ``login`` checks that the configured SVM exists.

Layout (``volume_per_lun``, the default): every simplyblock volume is its
own FlexVol ``sb_<hex>`` (FlexVol names cannot contain ``-``) holding one
LUN or NVMe namespace ``sb-<hex>``. The FlexVol is thin (``guarantee none``),
has no snapshot reserve and no snapshot policy, and is sized
``flexvol_headroom_percent`` above the LUN so a fully written thin LUN does
not run its volume out of space. With ``volume_per_lun: false`` LUNs are
placed in one existing FlexVol (``flexvol``) that the operator sizes.

Protocol family. ``create_volume`` has no protocol argument, so the option
``protocol_family`` (``scsi`` default, or ``nvme``) decides whether new
volumes are LUNs (``/api/storage/luns``) or namespaces
(``/api/storage/namespaces``). Existing volumes of either kind are always
found, mapped and deleted correctly. ``capabilities.protocols`` is a class
attribute, so it lists every protocol ONTAP serves; mapping a LUN to an NVMe
host (or a namespace to a SCSI host) raises :class:`ArrayUnsupportedError`.

SCSI hosts are igroups (``protocol mixed``, ``os_type linux``); the host id
is the igroup name; volumes are mapped with ``lun-maps`` and the mapping's
``lun`` is the ``logical_unit_number``. Volume identity: ``naa.600a0980`` +
hex of the 12-character ASCII LUN serial number.

NVMe hosts. ONTAP maps a namespace to exactly one subsystem, so a subsystem
cannot stand for a host if a volume is to be attached to several hosts.
Instead:

* ``ensure_host`` creates a *host subsystem* named after the host holding
  the host NQNs (and nothing else). The host id is its name.
* ``map_volume`` maps the namespace to its own *volume subsystem*
  (``sb_<hex>``, comment :data:`VOLUME_SUBSYSTEM_COMMENT`, created on first
  map) and adds the host's NQNs to it; ``unmap_volume`` removes them.
* The mapping's ``lun`` is the namespace's NSID in the volume subsystem.

Volume subsystems are recognised by their comment and never count as hosts.
Namespaces carry no serial-derived identity in the REST API, so their
:attr:`Volume.identifier` is ``id.<namespace uuid>`` (the same UUID the host
sees as ``/dev/disk/by-id/nvme-uuid.<uuid>``); ``nguid`` is left empty.

ONTAP lets an initiator / host NQN sit in several igroups / subsystems. The
contract does not: before adding one, the adapter looks it up and raises
:class:`ArrayConflictError` if another host (igroup or host subsystem) has it.

Asynchronous calls (FlexVol create/resize/delete) answer ``202`` with a job
link; the adapter polls ``/api/cluster/jobs/{uuid}`` until ``success`` or
``failure``, at most ``job_timeout_s`` seconds.

Verify against a real system
----------------------------
* FlexVol ``DELETE`` of an online volume without a junction path succeeds
  without first taking it offline (the adapter does not offline it).
* A thin LUN/namespace may not be larger than its FlexVol (the fake
  enforces this; it is why resize grows the FlexVol first).
* ``aggregates`` omitted on FlexVol ``POST`` lets ONTAP place the volume
  (9.8+); with several ``aggregates`` configured the adapter picks the one
  with the most ``space.block_storage.available`` (needs read access to
  ``/api/storage/aggregates``, which SVM-scoped accounts may lack).
* Name/duplicate errors: HTTP 409 vs 400, and the numeric ONTAP error codes
  (the adapter classifies by status and message text, not by code).
* FlexVol duplicate name surfaces as a failed job rather than a synchronous
  error.
* ``PATCH`` of an igroup's ``protocol`` to ``mixed`` (only needed for igroups
  not created by this adapter).
* ``nsid`` in subsystem-maps is a hex string such as ``"00000001h"``; the
  subsystem-map ``DELETE`` path is
  ``/subsystem-maps/{subsystem.uuid}/{namespace.uuid}``.
* ``allow_delete_with_hosts`` on subsystem ``DELETE``.
* Namespace resize via ``PATCH space.size`` (grow only).
* Whether query values match case-insensitively (WWPNs are always written in
  lowercase colon form, IQNs in lowercase).
* ASA r2 systems (ONTAP 9.16+ "ASA r2") hide FlexVols; this adapter targets
  the classic unified/ASA personality.
* NVMe/FC hosts are identified by NQN, not WWPN (see :meth:`_nqn`).
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import quote

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
    Initiator,
    Mapping,
    OptionDoc,
    Protocol,
    Volume,
    colon_wwpn,
)
from ..http import json_of

NETAPP_NAA_PREFIX = "naa.600a0980"
VOLUME_SUBSYSTEM_COMMENT = "simplyblock volume subsystem"
FLEXVOL_COMMENT = "simplyblock"
MIN_FLEXVOL_BYTES = 20 << 20
_FAMILIES = ("scsi", "nvme")
_GUARANTEES = ("none", "volume")
_LUN = "lun"
_NS = "namespace"

_LUN_FIELDS = "uuid,name,serial_number,space.size,location.logical_unit,location.volume,svm"
_NS_FIELDS = "uuid,name,space.size,location.namespace,location.volume,svm"


def ontap_wwn(serial: str) -> str:
    """``naa.600a0980`` + hex of the 12-character ASCII LUN serial."""
    return NETAPP_NAA_PREFIX + serial.encode("ascii").hex()


def flexvol_name(name: str) -> str:
    """FlexVol names allow letters, digits and ``_`` only."""
    return name.replace("-", "_")


def parse_nsid(value: Any) -> int | None:
    """``"00000001h"`` (ONTAP's NSID format) or an int -> ``1``."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().lower().removesuffix("h")
    try:
        return int(text, 16)
    except ValueError:
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


class NetAppOntapAdapter(ArrayAdapter):
    vendor = "netapp-ontap"
    display_name = "NetApp ONTAP"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NVME_FC}),
        size_granularity_bytes=4096,
        min_size_bytes=1 << 20,
        max_name_len=96,
    )
    credential_fields = (frozenset({"username", "password"}),)
    option_docs = (
        OptionDoc("svm", "Storage VM (vserver) that owns the volumes, igroups and "
                  "subsystems.", required=True),
        OptionDoc("protocol_family", "'scsi' (LUNs, for iSCSI/FC hosts) or 'nvme' "
                  "(namespaces, for NVMe/TCP and NVMe/FC hosts): what new volumes "
                  "are created as.", default="scsi"),
        OptionDoc("aggregates", "Aggregate name(s) for new FlexVols (list or comma "
                  "separated); with several, the one with most free space is used. "
                  "Default: ONTAP chooses."),
        OptionDoc("volume_per_lun", "Create one FlexVol per LUN/namespace.", default=True),
        OptionDoc("flexvol", "Existing FlexVol for all LUNs/namespaces; required when "
                  "volume_per_lun is false."),
        OptionDoc("space_guarantee", "FlexVol space guarantee: 'none' (thin) or "
                  "'volume'.", default="none"),
        OptionDoc("snapshot_reserve_percent", "FlexVol snapshot reserve (0-90).", default=0),
        OptionDoc("snapshot_policy", "FlexVol snapshot policy.", default="none"),
        OptionDoc("flexvol_headroom_percent", "FlexVol size above the LUN/namespace size, "
                  "for metadata.", default=5),
        OptionDoc("os_type", "os_type of LUNs, namespaces, igroups and subsystems.",
                  default="linux"),
        OptionDoc("namespace_block_size", "NVMe namespace block size (512 or 4096).",
                  default=4096),
        OptionDoc("job_timeout_s", "How long to wait for an asynchronous ONTAP job.",
                  default=300),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._sleep: Callable[[float], None] = self.session.sleep
        self.svm = str(self.option("svm")).strip()
        if not self.svm:
            raise ArrayError("NetApp ONTAP option svm must not be empty")
        self.family = str(self.option("protocol_family")).strip().lower()
        if self.family not in _FAMILIES:
            raise ArrayError(f"NetApp ONTAP protocol_family must be one of {_FAMILIES}, "
                             f"got {self.family!r}")
        self.volume_per_lun = _as_bool(self.option("volume_per_lun"))
        self.shared_flexvol = self.option("flexvol")
        if not self.volume_per_lun and not self.shared_flexvol:
            raise ArrayError("NetApp ONTAP option flexvol is required when volume_per_lun "
                             "is false")
        aggr = self.option("aggregates") or []
        if isinstance(aggr, str):
            aggr = [a for a in (x.strip() for x in aggr.split(",")) if a]
        self.aggregates: list[str] = [str(a) for a in aggr]
        self.guarantee = str(self.option("space_guarantee"))
        if self.guarantee not in _GUARANTEES:
            raise ArrayError(f"NetApp ONTAP space_guarantee must be one of {_GUARANTEES}")
        self.snapshot_reserve = self._int_option("snapshot_reserve_percent", 0, 90)
        self.headroom = self._int_option("flexvol_headroom_percent", 0, 100)
        self.block_size = self._int_option("namespace_block_size", 512, 4096)
        if self.block_size not in (512, 4096):
            raise ArrayError("NetApp ONTAP namespace_block_size must be 512 or 4096")
        self.job_timeout_s = float(self._int_option("job_timeout_s", 1, 86400))
        self.os_type = str(self.option("os_type"))

    def _int_option(self, name: str, lo: int, hi: int) -> int:
        raw = self.option(name)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ArrayError(f"NetApp ONTAP option {name} must be an integer") from None
        if not lo <= value <= hi:
            raise ArrayError(f"NetApp ONTAP option {name} must be in {lo}..{hi}")
        return value

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.set_basic_auth(self.secret("username"), self.secret("password"))
        if not self._records("/api/svm/svms", {"name": self.svm, "fields": "uuid,name"}):
            raise ArrayNotFoundError(f"SVM {self.svm!r} not found on array {self.config.id}")

    def close(self) -> None:
        self.session.clear_auth()

    # -- request helpers -----------------------------------------------------

    def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              json: Any = None) -> requests.Response:
        """One request; ONTAP 400 errors are classified by message."""
        resp = self.session.request(method, path, params=params, json=json, ok=(400,))
        if resp.status_code == 400:
            raise self._error(method, path, json_of(resp) or {})
        return resp

    @staticmethod
    def _error(method: str, path: str, body: dict[str, Any]) -> ArrayError:
        err = body.get("error") or {}
        message = str(err.get("message", "")) or "request failed"
        code = err.get("code", "")
        text = f"{method} {path}: {message} (ONTAP error {code})"
        lowered = message.lower()
        if "doesn't exist" in lowered or "does not exist" in lowered or "not found" in lowered:
            return ArrayNotFoundError(text)
        if any(w in lowered for w in ("already", "duplicate", "in use", "is mapped")):
            return ArrayConflictError(text)
        return ArrayRequestError(text, status=400)

    def _records(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """GET a collection, following ``_links.next``."""
        out: list[dict[str, Any]] = []
        next_path: str | None = path
        query: dict[str, Any] | None = params
        while next_path:
            body = json_of(self._call("GET", next_path, params=query)) or {}
            out.extend(body.get("records", []))
            next_path = ((body.get("_links") or {}).get("next") or {}).get("href")
            query = None
        return out

    def _one(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        records = self._records(path, params)
        return records[0] if records else None

    def _async(self, method: str, path: str, *, params: dict[str, Any] | None = None,
               json: Any = None) -> dict[str, Any]:
        """Issue a request that may answer 202 + job, and wait for the job."""
        resp = self._call(method, path, params=params, json=json)
        body = json_of(resp) or {}
        job = body.get("job")
        if job:
            self._wait_job(job, f"{method} {path}")
        return body

    def _wait_job(self, job: dict[str, Any], what: str) -> None:
        href = ((job.get("_links") or {}).get("self") or {}).get("href") \
            or f"/api/cluster/jobs/{job['uuid']}"
        waited = 0.0
        interval = 0.5
        while True:
            state = json_of(self._call("GET", href,
                                       params={"fields": "uuid,state,message,code"})) or {}
            status = state.get("state")
            if status == "success":
                return
            if status == "failure":
                raise self._error_from_job(what, state)
            if waited >= self.job_timeout_s:
                raise ArrayRequestError(
                    f"{what}: ONTAP job {job.get('uuid')} still {status} after "
                    f"{self.job_timeout_s:.0f}s")
            self._sleep(interval)
            waited += interval
            interval = min(interval * 2, 5.0)

    def _error_from_job(self, what: str, state: dict[str, Any]) -> ArrayError:
        method, _, path = what.partition(" ")
        message = f"{state.get('message') or 'job failed'} [job {state.get('uuid', '')}]"
        return self._error(method, path, {"error": {"message": message,
                                                    "code": state.get("code", "")}})

    @staticmethod
    def _created(resp_body: dict[str, Any]) -> dict[str, Any] | None:
        records = resp_body.get("records") or []
        return records[0] if records else None

    def _svm_ref(self) -> dict[str, str]:
        return {"name": self.svm}

    # -- volumes: lookup -----------------------------------------------------

    def _volume_from_lun(self, rec: dict[str, Any]) -> Volume:
        serial = rec.get("serial_number") or ""
        loc = rec.get("location") or {}
        name = loc.get("logical_unit") or str(rec.get("name", "")).rsplit("/", 1)[-1]
        return Volume(id=rec["uuid"], name=name, size_bytes=int(rec["space"]["size"]),
                      wwn=ontap_wwn(serial) if serial else None)

    @staticmethod
    def _volume_from_ns(rec: dict[str, Any]) -> Volume:
        loc = rec.get("location") or {}
        name = loc.get("namespace") or str(rec.get("name", "")).rsplit("/", 1)[-1]
        return Volume(id=rec["uuid"], name=name, size_bytes=int(rec["space"]["size"]))

    def _kinds(self) -> tuple[str, str]:
        return (_LUN, _NS) if self.family == "scsi" else (_NS, _LUN)

    def _lookup(self, kind: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if kind == _LUN:
            return self._one("/api/storage/luns", {**params, "svm.name": self.svm,
                                                   "fields": _LUN_FIELDS})
        return self._one("/api/storage/namespaces", {**params, "svm.name": self.svm,
                                                     "fields": _NS_FIELDS})

    def _locate(self, volume_id: str) -> tuple[str, dict[str, Any]] | None:
        for kind in self._kinds():
            rec = self._lookup(kind, {"uuid": volume_id})
            if rec is not None:
                return kind, rec
        return None

    def _require(self, volume_id: str) -> tuple[str, dict[str, Any]]:
        found = self._locate(volume_id)
        if found is None:
            raise ArrayNotFoundError(
                f"Volume {volume_id} not found in SVM {self.svm} on array {self.config.id}")
        return found

    def _to_volume(self, kind: str, rec: dict[str, Any]) -> Volume:
        return self._volume_from_lun(rec) if kind == _LUN else self._volume_from_ns(rec)

    def _flexvol_for(self, name: str) -> str:
        return flexvol_name(name) if self.volume_per_lun else str(self.shared_flexvol)

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        cluster = json_of(self._call("GET", "/api/cluster",
                                     params={"fields": "name,uuid,version"})) or {}
        model = ""
        try:
            nodes = self._records("/api/cluster/nodes", {"fields": "name,model,serial_number"})
            model = next((n["model"] for n in nodes if n.get("model")), "")
        except (ArrayRequestError, ArrayNotFoundError):
            pass
        version = (cluster.get("version") or {}).get("full", "")
        return ArrayInfo(vendor=self.vendor, model=model, version=version,
                         serial=cluster.get("uuid", ""))

    def get_volume(self, volume_id: str) -> Volume | None:
        found = self._locate(volume_id)
        return self._to_volume(*found) if found else None

    def find_volume(self, name: str) -> Volume | None:
        path = f"/vol/{self._flexvol_for(name)}/{name}"
        for kind in self._kinds():
            rec = self._lookup(kind, {"name": path})
            if rec is not None:
                return self._to_volume(kind, rec)
        return None

    def _flexvol_bytes(self, size_bytes: int) -> int:
        usable = -(-size_bytes * (100 + self.headroom) // 100)
        total = -(-usable * 100 // (100 - self.snapshot_reserve))
        total = -(-total // 4096) * 4096
        return max(total, MIN_FLEXVOL_BYTES)

    def _get_flexvol(self, name: str) -> dict[str, Any] | None:
        return self._one("/api/storage/volumes",
                         {"name": name, "svm.name": self.svm, "fields": "uuid,name,size"})

    def _pick_aggregate(self) -> list[dict[str, str]]:
        if not self.aggregates:
            return []
        if len(self.aggregates) == 1:
            return [{"name": self.aggregates[0]}]
        # More than one aggregate in the POST would make a FlexGroup: choose one.
        recs = self._records("/api/storage/aggregates",
                             {"name": "|".join(self.aggregates),
                              "fields": "name,space.block_storage.available"})
        if not recs:
            raise ArrayNotFoundError(f"None of the aggregates {self.aggregates} exist")
        best = max(recs, key=lambda r: int(
            ((r.get("space") or {}).get("block_storage") or {}).get("available", 0)))
        return [{"name": best["name"]}]

    def _create_flexvol(self, name: str, size_bytes: int) -> None:
        body: dict[str, Any] = {
            "name": name,
            "svm": self._svm_ref(),
            "size": size_bytes,
            "type": "rw",
            "guarantee": {"type": self.guarantee},
            "space": {"snapshot": {"reserve_percent": self.snapshot_reserve}},
            "snapshot_policy": {"name": str(self.option("snapshot_policy"))},
            "comment": FLEXVOL_COMMENT,
        }
        aggrs = self._pick_aggregate()
        if aggrs:
            body["aggregates"] = aggrs
        self._async("POST", "/api/storage/volumes", json=body)

    def _grow_flexvol(self, vol: dict[str, Any], size_bytes: int) -> None:
        if int(vol.get("size", 0)) < size_bytes:
            self._async("PATCH", f"/api/storage/volumes/{vol['uuid']}",
                        json={"size": size_bytes})

    def _delete_flexvol(self, uuid: str) -> None:
        try:
            self._async("DELETE", f"/api/storage/volumes/{uuid}")
        except ArrayNotFoundError:
            pass

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        flex = self._flexvol_for(name)
        created_flexvol: str | None = None
        if self.volume_per_lun:
            want = self._flexvol_bytes(size_bytes)
            vol = self._get_flexvol(flex)
            if vol is None:  # a re-run may find the FlexVol of an interrupted create
                self._create_flexvol(flex, want)
                vol = self._get_flexvol(flex)
                if vol is None:
                    raise ArrayRequestError(f"FlexVol {flex} missing after create")
                created_flexvol = vol["uuid"]
            else:
                self._grow_flexvol(vol, want)
        path = f"/vol/{flex}/{name}"
        kind = _LUN if self.family == "scsi" else _NS
        try:
            if kind == _LUN:
                self._call("POST", "/api/storage/luns", params={"return_records": "true"}, json={
                    "svm": self._svm_ref(), "name": path, "os_type": self.os_type,
                    "space": {"size": size_bytes, "guarantee": {"requested": False}},
                })
            else:
                self._call("POST", "/api/storage/namespaces", params={"return_records": "true"},
                           json={"svm": self._svm_ref(), "name": path, "os_type": self.os_type,
                                 "space": {"size": size_bytes,
                                           "block_size": self.block_size}})
        except ArrayError:
            if created_flexvol:
                try:
                    self._delete_flexvol(created_flexvol)
                except ArrayError:
                    pass
            raise
        rec = self._lookup(kind, {"name": path})
        if rec is None:
            raise ArrayRequestError(f"{kind} {path} missing after create")
        return self._to_volume(kind, rec)

    def _owns_flexvol(self, kind: str, rec: dict[str, Any]) -> bool:
        """True when the LUN/namespace sits in its own per-volume FlexVol."""
        flex = ((rec.get("location") or {}).get("volume") or {}).get("name", "")
        name = self._to_volume(kind, rec).name
        return bool(flex) and flex != self.shared_flexvol and flex == flexvol_name(name)

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        kind, rec = self._require(volume_id)
        loc_vol = (rec.get("location") or {}).get("volume") or {}
        if self._owns_flexvol(kind, rec):
            vol = self._get_flexvol(loc_vol.get("name", ""))
            if vol is None:
                raise ArrayNotFoundError(f"FlexVol of volume {volume_id} not found")
            self._grow_flexvol(vol, self._flexvol_bytes(size_bytes))
        base = "/api/storage/luns" if kind == _LUN else "/api/storage/namespaces"
        self._call("PATCH", f"{base}/{volume_id}", json={"space": {"size": size_bytes}})
        kind, rec = self._require(volume_id)
        return self._to_volume(kind, rec)

    def delete_volume(self, volume_id: str) -> None:
        kind, rec = self._require(volume_id)
        loc_vol = (rec.get("location") or {}).get("volume") or {}
        if kind == _LUN:
            self._call("DELETE", f"/api/storage/luns/{volume_id}")
        else:
            smap = self._subsystem_map(volume_id)
            if smap is not None:
                sub = smap["subsystem"]
                self._call("DELETE",
                           f"/api/protocols/nvme/subsystem-maps/{sub['uuid']}/{volume_id}")
                self._delete_subsystem(sub["uuid"])
            self._call("DELETE", f"/api/storage/namespaces/{volume_id}")
        # Only a FlexVol of our own (one per LUN) is removed; never the shared one.
        flex = loc_vol.get("name", "")
        if self._owns_flexvol(kind, rec):
            uuid = loc_vol.get("uuid") or (self._get_flexvol(flex) or {}).get("uuid")
            if uuid:
                self._delete_flexvol(uuid)

    # -- hosts ----------------------------------------------------------------

    @staticmethod
    def _nqn(i: Initiator) -> str:
        if i.protocol == Protocol.NVME_FC and ":" in i.address and not i.address.startswith(
                "nqn."):
            raise ArrayUnsupportedError(
                "ONTAP identifies NVMe/FC hosts by host NQN, not by WWPN: "
                f"{i.address!r}")
        return i.address

    def ensure_host(self, spec: HostSpec) -> str:
        if all(i.protocol.is_nvme for i in spec.initiators):
            return self._ensure_nvme_host(spec)
        if all(i.protocol.is_scsi for i in spec.initiators):
            return self._ensure_igroup(spec)
        raise ArrayUnsupportedError(
            f"NetApp ONTAP adapter does not support {sorted(p.value for p in spec.protocols)}")

    # SCSI: igroups

    @staticmethod
    def _igroup_initiator(i: Initiator) -> str:
        return colon_wwpn(i.address) if i.protocol == Protocol.FC else i.normalized()

    @staticmethod
    def _norm_initiator(value: str) -> str:
        v = value.strip().lower()
        if v.startswith(("iqn.", "eui.", "naa.")):
            return v
        return v.replace(":", "").replace("-", "").removeprefix("0x")

    def _igroup(self, name: str) -> dict[str, Any] | None:
        return self._one("/api/protocols/san/igroups",
                         {"name": name, "svm.name": self.svm,
                          "fields": "uuid,name,protocol,os_type,initiators.name"})

    def _ensure_igroup(self, spec: HostSpec) -> str:
        wanted = [self._igroup_initiator(i) for i in spec.initiators]
        owners = self._records("/api/protocols/san/igroups",
                               {"svm.name": self.svm, "initiators.name": "|".join(wanted),
                                "fields": "uuid,name,initiators.name"})
        want_norm = {self._norm_initiator(w) for w in wanted}
        for g in owners:
            if g["name"] == spec.name:
                continue
            taken = want_norm & {self._norm_initiator(i["name"])
                                 for i in g.get("initiators") or []}
            if taken:
                raise ArrayConflictError(
                    f"Initiator(s) {', '.join(sorted(taken))} already belong to igroup "
                    f"{g['name']!r} in SVM {self.svm}")
        group = self._igroup(spec.name)
        if group is None:
            self._call("POST", "/api/protocols/san/igroups",
                       params={"return_records": "true"},
                       json={"svm": self._svm_ref(), "name": spec.name, "os_type": self.os_type,
                             "protocol": "mixed",
                             "initiators": [{"name": w} for w in wanted]})
            return spec.name
        have = {self._norm_initiator(i["name"]) for i in group.get("initiators") or []}
        missing = [w for w in wanted if self._norm_initiator(w) not in have]
        if missing:
            needs = {"iscsi" if i.protocol == Protocol.ISCSI else "fcp"
                     for i in spec.initiators
                     if self._norm_initiator(self._igroup_initiator(i)) not in have}
            if group.get("protocol") not in ("mixed", *needs) or len(needs) > 1:
                self._call("PATCH", f"/api/protocols/san/igroups/{group['uuid']}",
                           json={"protocol": "mixed"})
            self._call("POST", f"/api/protocols/san/igroups/{group['uuid']}/initiators",
                       json={"records": [{"name": m} for m in missing]})
        return spec.name

    # NVMe: host subsystems

    def _subsystems(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        return self._records("/api/protocols/nvme/subsystems",
                             {**params, "svm.name": self.svm,
                              "fields": "uuid,name,comment,os_type,hosts.nqn"})

    @staticmethod
    def _is_volume_subsystem(sub: dict[str, Any]) -> bool:
        return sub.get("comment") == VOLUME_SUBSYSTEM_COMMENT

    def _host_subsystem(self, name: str) -> dict[str, Any] | None:
        subs = [s for s in self._subsystems({"name": name}) if not self._is_volume_subsystem(s)]
        return subs[0] if subs else None

    def _ensure_nvme_host(self, spec: HostSpec) -> str:
        wanted = [self._nqn(i) for i in spec.initiators]
        for other in self._subsystems({"hosts.nqn": "|".join(wanted)}):
            if other["name"] == spec.name or self._is_volume_subsystem(other):
                continue
            taken = set(wanted) & {h["nqn"] for h in other.get("hosts") or []}
            if taken:
                raise ArrayConflictError(
                    f"Host NQN(s) {', '.join(sorted(taken))} already belong to subsystem "
                    f"{other['name']!r} in SVM {self.svm}")
        sub = self._host_subsystem(spec.name)
        if sub is None:
            self._call("POST", "/api/protocols/nvme/subsystems",
                       params={"return_records": "true"},
                       json={"svm": self._svm_ref(), "name": spec.name, "os_type": self.os_type,
                             "hosts": [{"nqn": n} for n in wanted]})
            return spec.name
        have = {h["nqn"] for h in sub.get("hosts") or []}
        missing = [n for n in wanted if n not in have]
        if missing:
            self._add_subsystem_hosts(sub["uuid"], missing)
        return spec.name

    def _add_subsystem_hosts(self, sub_uuid: str, nqns: list[str]) -> None:
        self._call("POST", f"/api/protocols/nvme/subsystems/{sub_uuid}/hosts",
                   json={"records": [{"nqn": n} for n in nqns]})

    def _delete_subsystem(self, sub_uuid: str) -> None:
        try:
            self._call("DELETE", f"/api/protocols/nvme/subsystems/{sub_uuid}",
                       params={"allow_delete_with_hosts": "true"})
        except ArrayNotFoundError:
            pass

    def delete_host(self, host_id: str) -> None:
        group = self._igroup(host_id)
        if group is not None:
            try:
                self._call("DELETE", f"/api/protocols/san/igroups/{group['uuid']}")
            except ArrayNotFoundError:
                pass
        sub = self._host_subsystem(host_id)
        if sub is not None:
            self._delete_subsystem(sub["uuid"])

    # -- mappings -------------------------------------------------------------

    def _lun_maps(self, lun_uuid: str, igroup: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"lun.uuid": lun_uuid, "svm.name": self.svm,
                                  "fields": "lun.uuid,igroup.uuid,igroup.name,"
                                            "logical_unit_number"}
        if igroup is not None:
            params["igroup.name"] = igroup
        return self._records("/api/protocols/san/lun-maps", params)

    @staticmethod
    def _lun_mapping(rec: dict[str, Any]) -> Mapping:
        return Mapping(volume_id=rec["lun"]["uuid"], host_id=rec["igroup"]["name"],
                       lun=rec.get("logical_unit_number"))

    def _subsystem_map(self, ns_uuid: str) -> dict[str, Any] | None:
        return self._one("/api/protocols/nvme/subsystem-maps",
                         {"namespace.uuid": ns_uuid, "svm.name": self.svm,
                          "fields": "namespace.uuid,subsystem.uuid,subsystem.name,nsid"})

    def _volume_subsystem_hosts(self, smap: dict[str, Any]) -> dict[str, Any]:
        sub = smap["subsystem"]
        recs = self._subsystems({"uuid": sub["uuid"]})
        if not recs:
            raise ArrayNotFoundError(f"Subsystem {sub.get('name')} not found")
        return recs[0]

    def _nvme_mappings(self, ns_uuid: str) -> list[Mapping]:
        smap = self._subsystem_map(ns_uuid)
        if smap is None:
            return []
        vsub = self._volume_subsystem_hosts(smap)
        nqns = {h["nqn"] for h in vsub.get("hosts") or []}
        if not nqns:
            return []
        nsid = parse_nsid(smap.get("nsid"))
        out: list[Mapping] = []
        for sub in self._subsystems({"hosts.nqn": "|".join(sorted(nqns))}):
            if self._is_volume_subsystem(sub):
                continue
            if nqns & {h["nqn"] for h in sub.get("hosts") or []}:
                out.append(Mapping(volume_id=ns_uuid, host_id=sub["name"], lun=nsid))
        return out

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        kind, rec = self._require(volume_id)
        if kind == _LUN:
            return self._map_lun(volume_id, host_id)
        return self._map_namespace(volume_id, rec, host_id)

    def _map_lun(self, lun_uuid: str, host_id: str) -> Mapping:
        if self._igroup(host_id) is None:
            if self._host_subsystem(host_id) is not None:
                raise ArrayUnsupportedError(
                    f"Host {host_id} is an NVMe host; volume {lun_uuid} is a SCSI LUN "
                    "(protocol_family scsi)")
            raise ArrayNotFoundError(f"igroup {host_id} not found in SVM {self.svm}")
        existing = self._lun_maps(lun_uuid, host_id)
        if existing:
            return self._lun_mapping(existing[0])
        self._call("POST", "/api/protocols/san/lun-maps", params={"return_records": "true"},
                   json={"svm": self._svm_ref(), "lun": {"uuid": lun_uuid},
                         "igroup": {"name": host_id}})
        created = self._lun_maps(lun_uuid, host_id)
        if not created:
            raise ArrayRequestError(f"lun-map {lun_uuid} -> {host_id} missing after create")
        return self._lun_mapping(created[0])

    def _map_namespace(self, ns_uuid: str, rec: dict[str, Any], host_id: str) -> Mapping:
        host = self._host_subsystem(host_id)
        if host is None:
            if self._igroup(host_id) is not None:
                raise ArrayUnsupportedError(
                    f"Host {host_id} is a SCSI host; volume {ns_uuid} is an NVMe namespace "
                    "(protocol_family nvme)")
            raise ArrayNotFoundError(f"NVMe host subsystem {host_id} not found in SVM {self.svm}")
        host_nqns = [h["nqn"] for h in host.get("hosts") or []]
        smap = self._subsystem_map(ns_uuid)
        if smap is None:
            sub_name = flexvol_name(self._volume_from_ns(rec).name)
            vsubs = [s for s in self._subsystems({"name": sub_name})
                     if self._is_volume_subsystem(s)]
            if vsubs:
                sub_uuid = vsubs[0]["uuid"]
            else:
                body = json_of(self._call(
                    "POST", "/api/protocols/nvme/subsystems", params={"return_records": "true"},
                    json={"svm": self._svm_ref(), "name": sub_name, "os_type": self.os_type,
                          "comment": VOLUME_SUBSYSTEM_COMMENT})) or {}
                created = self._created(body)
                if created is None:
                    created = next(iter(s for s in self._subsystems({"name": sub_name})
                                        if self._is_volume_subsystem(s)), None)
                if created is None:
                    raise ArrayRequestError(f"Subsystem {sub_name} missing after create")
                sub_uuid = created["uuid"]
            self._call("POST", "/api/protocols/nvme/subsystem-maps",
                       params={"return_records": "true"},
                       json={"svm": self._svm_ref(), "namespace": {"uuid": ns_uuid},
                             "subsystem": {"uuid": sub_uuid}})
            smap = self._subsystem_map(ns_uuid)
            if smap is None:
                raise ArrayRequestError(f"subsystem-map of {ns_uuid} missing after create")
        vsub = self._volume_subsystem_hosts(smap)
        if not self._is_volume_subsystem(vsub):
            raise ArrayConflictError(
                f"Namespace {ns_uuid} is mapped to subsystem {vsub['name']!r}, which "
                "simplyblock does not manage")
        have = {h["nqn"] for h in vsub.get("hosts") or []}
        missing = [n for n in host_nqns if n not in have]
        if missing:
            self._add_subsystem_hosts(vsub["uuid"], missing)
        return Mapping(volume_id=ns_uuid, host_id=host_id, lun=parse_nsid(smap.get("nsid")))

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        found = self._locate(volume_id)
        if found is None:
            return
        kind, _ = found
        if kind == _LUN:
            for m in self._lun_maps(volume_id, host_id):
                try:
                    self._call("DELETE", f"/api/protocols/san/lun-maps/{volume_id}/"
                                         f"{m['igroup']['uuid']}")
                except ArrayNotFoundError:
                    pass
            return
        smap = self._subsystem_map(volume_id)
        host = self._host_subsystem(host_id)
        if smap is None or host is None:
            return
        vsub = self._volume_subsystem_hosts(smap)
        if not self._is_volume_subsystem(vsub):
            return
        have = {h["nqn"] for h in vsub.get("hosts") or []}
        for h in host.get("hosts") or []:
            if h["nqn"] in have:
                try:
                    self._call("DELETE", f"/api/protocols/nvme/subsystems/{vsub['uuid']}/hosts/"
                                         f"{quote(h['nqn'], safe='')}")
                except ArrayNotFoundError:
                    pass

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        kind, _ = self._require(volume_id)
        if kind == _LUN:
            return [self._lun_mapping(r) for r in self._lun_maps(volume_id)]
        return self._nvme_mappings(volume_id)
