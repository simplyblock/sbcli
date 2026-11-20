"""Pure Storage FlashArray, REST API 2.x (Purity//FA 6.x and later).

Auth: an API token (``api_token`` credential field) exchanged for a session
token via ``POST /api/2.x/login`` (``api-token`` header -> ``x-auth-token``).

Objects are addressed by name (hosts) and id (volumes). Pure "destroys"
volumes into a recycle bin (recoverable for the eradication delay, 24 h by
default) before eradication; ``eradicate=true`` removes them at once.

Volume identity: SCSI ``naa.624a9370<serial>``; NVMe NGUID
``eui.00<serial[0:14]>24a937<serial[14:24]>``.
"""
from __future__ import annotations

from typing import Any

from ..base import (
    ArrayAdapter,
    ArrayConflictError,
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

_SUPPORTED_MAX = (2, 36)  # newest 2.x minor this adapter was written against
_MIN_VERSION = (2, 4)


def _ver(v: str) -> tuple[int, int]:
    major, _, minor = v.partition(".")
    return int(major), int(minor or 0)


def pure_wwn(serial: str) -> str:
    return "naa.624a9370" + serial.lower()


def pure_nguid(serial: str) -> str:
    s = serial.lower()
    return "eui.00" + s[:14] + "24a937" + s[14:]


class PureFlashArrayAdapter(ArrayAdapter):
    vendor = "pure-flasharray"
    display_name = "Pure Storage FlashArray"
    capabilities = Capabilities(
        protocols=frozenset({Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP,
                             Protocol.NVME_RDMA, Protocol.NVME_FC}),
        size_granularity_bytes=512,
        min_size_bytes=1 << 20,
        max_name_len=63,
    )
    credential_fields = (frozenset({"api_token"}),)
    option_docs = (
        OptionDoc("eradicate", "Eradicate deleted volumes immediately instead of "
                  "leaving them recoverable for the eradication delay.", default=False),
        OptionDoc("api_version", "Pin the REST 2.x version (e.g. '2.21'); "
                  "default: newest supported by both sides."),
    )

    def __init__(self, ctx):
        super().__init__(ctx)
        self._api = ""

    # -- session ------------------------------------------------------------

    def login(self) -> None:
        version = self.option("api_version")
        if not version:
            offered = json_of(self.session.get("/api/api_version")).get("version", [])
            usable = [v for v in offered
                      if v.startswith("2.") and _MIN_VERSION <= _ver(v) <= _SUPPORTED_MAX]
            if not usable:
                raise ArrayRequestError(
                    f"FlashArray offers no REST 2.x version in "
                    f"{'.'.join(map(str, _MIN_VERSION))}..{'.'.join(map(str, _SUPPORTED_MAX))}: "
                    f"{offered}")
            version = max(usable, key=_ver)
        self._api = f"/api/{version}"
        self.session.headers.pop("x-auth-token", None)
        resp = self.session.post(f"{self._api}/login",
                                 headers={"api-token": self.secret("api_token")})
        token = resp.headers.get("x-auth-token")
        if not token:
            raise ArrayRequestError("FlashArray login returned no x-auth-token")
        self.session.headers["x-auth-token"] = token

    def close(self) -> None:
        if self._api and "x-auth-token" in self.session.headers:
            try:
                self.session.post(f"{self._api}/logout")
            except Exception:
                pass

    # -- request helpers -----------------------------------------------------

    def _items(self, method: str, path: str, *, params=None, json=None,
               missing_ok: bool = False) -> list[dict[str, Any]]:
        resp = self.session.request(method, f"{self._api}{path}", params=params, json=json,
                                    ok=(400,))
        body = json_of(resp) or {}
        if resp.status_code == 400:
            messages = " ".join(
                str(e.get("message", "")) for e in body.get("errors", []))
            lowered = messages.lower()
            if "does not exist" in lowered or "not found" in lowered:
                if missing_ok:
                    return []
                raise ArrayNotFoundError(f"{method} {path}: {messages}")
            if "already" in lowered or "in use" in lowered or "exists" in lowered:
                raise ArrayConflictError(f"{method} {path}: {messages}")
            raise ArrayRequestError(f"{method} {path}: {messages}", status=400)
        return list(body.get("items", []))

    @staticmethod
    def _volume(item: dict[str, Any]) -> Volume:
        serial = item.get("serial") or ""
        return Volume(
            id=item["id"], name=item["name"], size_bytes=int(item["provisioned"]),
            wwn=pure_wwn(serial) if serial else None,
            nguid=pure_nguid(serial) if serial else None,
        )

    # -- contract -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        arr = self._items("GET", "/arrays")[0]
        model = ""
        try:
            controllers = self._items("GET", "/controllers")
            model = next((c.get("model", "") for c in controllers if c.get("model")), "")
        except ArrayRequestError:
            pass
        return ArrayInfo(vendor=self.vendor, model=model,
                         version=f"{arr.get('os', 'Purity//FA')} {arr.get('version', '')}".strip(),
                         serial=arr.get("id", ""))

    def create_volume(self, name: str, size_bytes: int) -> Volume:
        items = self._items("POST", "/volumes", params={"names": name},
                            json={"provisioned": size_bytes})
        return self._volume(items[0])

    def _live(self, items: list[dict[str, Any]]) -> Volume | None:
        live = [i for i in items if not i.get("destroyed")]
        return self._volume(live[0]) if live else None

    def get_volume(self, volume_id: str) -> Volume | None:
        return self._live(self._items("GET", "/volumes", params={"ids": volume_id},
                                      missing_ok=True))

    def find_volume(self, name: str) -> Volume | None:
        return self._live(self._items("GET", "/volumes", params={"names": name},
                                      missing_ok=True))

    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume:
        items = self._items("PATCH", "/volumes", params={"ids": volume_id},
                            json={"provisioned": size_bytes})
        return self._volume(items[0])

    def delete_volume(self, volume_id: str) -> None:
        self._items("PATCH", "/volumes", params={"ids": volume_id}, json={"destroyed": True})
        if self.option("eradicate"):
            self._items("DELETE", "/volumes", params={"ids": volume_id})

    def _host(self, name: str) -> dict[str, Any] | None:
        items = self._items("GET", "/hosts", params={"names": name}, missing_ok=True)
        return items[0] if items else None

    def ensure_host(self, spec: HostSpec) -> str:
        wanted = {
            "iqns": spec.addresses(Protocol.ISCSI),
            "wwns": [a.upper() for a in spec.addresses(Protocol.FC)],
            "nqns": spec.addresses(Protocol.NVME_TCP, Protocol.NVME_RDMA, Protocol.NVME_FC),
        }
        wanted = {k: v for k, v in wanted.items() if v}
        host = self._host(spec.name)
        if host is None:
            self._items("POST", "/hosts", params={"names": spec.name}, json=wanted)
            return spec.name
        add: dict[str, list[str]] = {}
        for key, values in wanted.items():
            have = {str(v).lower() for v in host.get(key, [])}
            missing = [v for v in values if v.lower() not in have]
            if missing:
                add[f"add_{key}"] = missing
        if add:
            self._items("PATCH", "/hosts", params={"names": spec.name}, json=add)
        return spec.name

    def delete_host(self, host_id: str) -> None:
        self._items("DELETE", "/hosts", params={"names": host_id}, missing_ok=True)

    def _connections(self, volume_id: str, host_id: str | None = None) -> list[Mapping]:
        params: dict[str, str] = {"volume_ids": volume_id}
        if host_id is not None:
            params["host_names"] = host_id
        items = self._items("GET", "/connections", params=params, missing_ok=True)
        return [Mapping(volume_id=i["volume"]["id"], host_id=i["host"]["name"],
                        lun=i.get("lun")) for i in items if i.get("host")]

    def map_volume(self, volume_id: str, host_id: str) -> Mapping:
        existing = self._connections(volume_id, host_id)
        if existing:
            return existing[0]
        items = self._items("POST", "/connections",
                            params={"host_names": host_id, "volume_ids": volume_id})
        i = items[0]
        return Mapping(volume_id=i["volume"]["id"], host_id=i["host"]["name"], lun=i.get("lun"))

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        if not self._connections(volume_id, host_id):
            return
        self._items("DELETE", "/connections",
                    params={"host_names": host_id, "volume_ids": volume_id}, missing_ok=True)

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return self._connections(volume_id)
