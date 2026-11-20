"""Infinidat InfiniBox, REST API ``/api/rest`` (InfiniBox 7.x).

Auth: HTTP basic on every request (``username`` / ``password`` credential).
The array also sets a session cookie, which ``requests`` keeps; basic auth
is still sent, so an expired cookie never matters and a rotated password
surfaces as a 401 (``ArrayClient`` then re-resolves the credential).

Every response is an envelope ``{"result": ..., "error": ..., "metadata":
{...}}``; errors carry ``error.code`` (e.g. ``VOLUME_NOT_FOUND``) and
``error.message``. Lists are paged (``page``, ``page_size`` up to 1000;
``metadata.pages_total``) and filtered with ``field=eq:value``.

Destructive requests (delete volume/host, unmap, shrink) need
``?approved=true``; without it the array answers **403**
``APPROVAL_REQUIRED``, which must not be mistaken for an auth failure, so
the adapter always sends the flag and maps that code to
:class:`ArrayRequestError`.

Objects have integer ids, returned here as strings. Volumes are created in
the pool named by the ``pool`` option (resolved to its id once per session),
thin by default. Sizes are bytes; see granularity below.

Volume identity: ``serial`` is the SCSI NAA-6 identifier without the
leading ``6`` (``742b0f...``, 31 hex digits, IEEE company id 0x742B0F);
``wwn = "naa.6" + serial``. A 32-digit serial already starting with ``6`` is
used as-is.

Hosts: ``POST /hosts {name}`` then one ``POST /hosts/{id}/ports
{type, address}`` per initiator (``ISCSI`` IQN; ``FC`` WWPN as 16 lowercase
hex digits). A port owned by another host is refused by the array; when that
happens on a host ``ensure_host`` just created, the empty host is deleted
again. Mappings: ``POST /hosts/{id}/luns {volume_id}`` -> ``{lun, ...}``;
``GET /volumes/{id}/luns`` lists them; ``DELETE
/hosts/{id}/luns/volume_id/{vid}?approved=true`` unmaps. Mappings made
through a host *cluster* (``clustered: true``) are not listed by this adapter
(it never creates them), so ``delete_volume`` on such a volume fails.

Higher-fidelity testing: Infinidat publishes ``infinisim`` (a Python
InfiniBox simulator serving this REST API) and ``infinisdk`` (the official
SDK). Running the contract suite's live tier against infinisim would check
this adapter's API reading; neither is a dependency here.

Verify against a real system:

* Size granularity/minimum: the adapter rounds to 1 MiB (minimum 1 MiB)
  because InfiniBox may round byte sizes up to its own allocation unit; if
  the array rounds to a coarser unit, raise ``size_granularity_bytes`` (a
  silent round-up would make later resizes look like shrinks).
* ``serial`` format (with or without the leading ``6``).
* Error codes and statuses: ``VOLUME_NOT_FOUND``/``HOST_NOT_FOUND``/
  ``POOL_NOT_FOUND`` (404?), name conflict (409 ``*_NAME_CONFLICT``?),
  port owned by another host (409 ``PORT_ALREADY_BELONGS_TO_HOST``?),
  volume still mapped on delete, ``APPROVAL_REQUIRED`` on unmap/delete.
* FC port address format accepted/returned (colons or not) -- both are
  normalised on read.
* That LUN numbers start at 1 and ``GET /volumes/{id}/luns`` includes
  ``host_id``.
* ``GET /system`` field names ``serial_number``, ``model_name``, ``version``.
* NVMe/TCP (``NVMEOF`` port type?) is not implemented.
* Name limit (adapter declares 63 characters).
"""
from __future__ import annotations

from typing import Any

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
    normalize_wwpn,
)
from ..http import body_text

_API = "/api/rest"
_PAGE_SIZE = 1000
_APPROVED = {"approved": "true"}


def infinibox_wwn(serial: str) -> str:
    s = serial.strip().lower()
    if len(s) == 32 and s.startswith("6"):
        return "naa." + s
    return "naa.6" + s


def _port_key(ptype: str, address: str) -> tuple[str, str]:
    ptype = ptype.upper()
    if ptype == "FC":
        try:
            return ptype, normalize_wwpn(address)
        except ValueError:
            pass
    return ptype, address.strip().lower()


class InfinidatAdapter(ArrayAdapter):
    vendor = "infinidat-infinibox"
    display_name = "Infinidat InfiniBox"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC}),
        size_granularity_bytes=1 << 20,
        min_size_bytes=1 << 20,
        max_name_len=63,
    )
    option_docs = (
        OptionDoc("pool", "Name of the pool new volumes are created in.", required=True),
        OptionDoc("provtype", "Provisioning type of new volumes: THIN or THICK.",
                  default="THIN"),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        if str(self.option("provtype")).upper() not in ("THIN", "THICK"):
            raise ArrayError("InfiniBox option provtype must be THIN or THICK")
        self._pool_id: int | None = None

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        self.session.http.cookies.clear()
        self.session.set_basic_auth(self.secret("username"), self.secret("password"))
        self._pool_id = None

    # -- request helpers -----------------------------------------------------

    def _send(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              json: Any = None, missing_ok: bool = False) -> dict[str, Any] | None:
        """Return the response envelope, or ``None`` for a missing object when
        ``missing_ok``; map envelope errors to :class:`ArrayError` types."""
        resp = self.session.request(method, _API + path, params=params, json=json,
                                    ok=(400, 403, 404, 409, 500))
        try:
            body = resp.json() if resp.content else None
        except ValueError:
            body = None
        env: dict[str, Any] = body if isinstance(body, dict) else {}
        err = env.get("error")
        status = resp.status_code
        if status < 300 and not err:
            return env
        err = err if isinstance(err, dict) else {}
        code = str(err.get("code") or "")
        msg = str(err.get("message") or "") or body_text(resp)
        text = f"{method} {path} -> HTTP {status}: {code} {msg}".strip()
        if status == 403 and code == "APPROVAL_REQUIRED":
            raise ArrayRequestError(text, status=status)
        if status == 403:
            raise ArrayAuthError(text)
        if status == 404 or code.endswith("NOT_FOUND"):
            if missing_ok:
                return None
            raise ArrayNotFoundError(text)
        if status == 409 or "CONFLICT" in code or "ALREADY" in code:
            raise ArrayConflictError(text)
        raise ArrayRequestError(text, status=status)

    def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
              json: Any = None, missing_ok: bool = False) -> Any:
        env = self._send(method, path, params=params, json=json, missing_ok=missing_ok)
        return None if env is None else env.get("result")

    def _list(self, path: str, **filters: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            params: dict[str, Any] = {k: f"eq:{v}" for k, v in filters.items()}
            params.update(page=page, page_size=_PAGE_SIZE)
            env = self._send("GET", path, params=params) or {}
            out.extend(env.get("result") or [])
            pages = int((env.get("metadata") or {}).get("pages_total") or 1)
            if page >= pages:
                return out
            page += 1

    @staticmethod
    def _volume(v: dict[str, Any]) -> Volume:
        serial = str(v.get("serial") or "")
        return Volume(id=str(v["id"]), name=v["name"], size_bytes=int(v["size"]),
                      wwn=infinibox_wwn(serial) if serial else None)

    @staticmethod
    def _int_id(object_id: str) -> int | None:
        try:
            return int(object_id)
        except (TypeError, ValueError):
            return None

    def _pool(self) -> int:
        if self._pool_id is None:
            name = self.option("pool")
            pools = self._list("/pools", name=name)
            if not pools:
                raise ArrayNotFoundError(f"InfiniBox pool {name!r} not found")
            self._pool_id = int(pools[0]["id"])
        return self._pool_id

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        system = self._call("GET", "/system") or {}
        return ArrayInfo(vendor=self.vendor, model=str(system.get("model_name") or "InfiniBox"),
                         version=str(system.get("version") or ""),
                         serial=str(system.get("serial_number") or ""))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        result = self._call("POST", "/volumes", json={
            "name": name, "size": size_bytes, "pool_id": self._pool(),
            "provtype": str(self.option("provtype")).upper()})
        return self._volume(result)

    def get_volume(self, volume_id: str) -> Volume | None:
        if self._int_id(volume_id) is None:
            return None
        result = self._call("GET", f"/volumes/{volume_id}", missing_ok=True)
        return self._volume(result) if result else None

    def find_volume(self, name: str) -> Volume | None:
        items = self._list("/volumes", name=name)
        return self._volume(items[0]) if items else None

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        result = self._call("PUT", f"/volumes/{volume_id}", json={"size": size_bytes})
        return self._volume(result)

    def delete_volume(self, volume_id: str) -> None:
        self._call("DELETE", f"/volumes/{volume_id}", params=_APPROVED)

    # -- hosts ----------------------------------------------------------------

    def ensure_host(self, spec: HostSpec) -> str:
        wanted = ([("ISCSI", a) for a in spec.addresses(Protocol.ISCSI)]
                  + [("FC", a) for a in spec.addresses(Protocol.FC)])
        hosts = self._list("/hosts", name=spec.name)
        created = not hosts
        host = hosts[0] if hosts else self._call("POST", "/hosts", json={"name": spec.name})
        host_id = int(host["id"])
        have = {_port_key(str(p.get("type", "")), str(p.get("address", "")))
                for p in host.get("ports") or []}
        try:
            for ptype, address in wanted:
                if _port_key(ptype, address) in have:
                    continue
                self._call("POST", f"/hosts/{host_id}/ports",
                           json={"type": ptype, "address": address})
        except ArrayError:
            if created:
                try:
                    self._call("DELETE", f"/hosts/{host_id}", params=_APPROVED,
                               missing_ok=True)
                except ArrayError:
                    pass
            raise
        return str(host_id)

    def delete_host(self, host_id: str) -> None:
        if self._int_id(host_id) is None:
            return
        self._call("DELETE", f"/hosts/{host_id}", params=_APPROVED, missing_ok=True)

    # -- mappings ---------------------------------------------------------------

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        luns = self._call("GET", f"/volumes/{volume_id}/luns") or []
        return [Mapping(volume_id=str(volume_id), host_id=str(m["host_id"]), lun=int(m["lun"]))
                for m in luns if m.get("host_id") and not m.get("clustered")]

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        for m in self.list_mappings(volume_id):
            if m.host_id == host_id:
                return m
        result = self._call("POST", f"/hosts/{host_id}/luns",
                            json={"volume_id": self._int_id(volume_id)})
        return Mapping(volume_id=str(volume_id), host_id=str(host_id), lun=int(result["lun"]))

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        luns = self._call("GET", f"/volumes/{volume_id}/luns", missing_ok=True) or []
        if not any(str(m.get("host_id")) == str(host_id) for m in luns):
            return
        self._call("DELETE", f"/hosts/{host_id}/luns/volume_id/{volume_id}",
                   params=_APPROVED, missing_ok=True)
