"""In-memory array plugin for tests. Never registered by default."""

from __future__ import annotations

import threading
from typing import ClassVar

from simplyblock_core.array import ArrayError, ArrayPlugin, ForeignLunError


class FakeArrayPlugin(ArrayPlugin):
    """An array kept in memory.

    ``calls`` records every operation as ``(op, *args)``; ``fence`` /
    ``unfence`` as ``(op, lun, host)`` and the fenced pairs in ``fenced``.
    ``luns``: lun id -> ``{"id", "name", "wwn", "size", "tags", "hosts"}``;
    ``hosts``: host id -> ``{"name", "initiators"}``. ``fail`` = operation
    names that raise :class:`ArrayError`; ``fail_falsy`` the ones that return
    False instead (fence / unfence only). ``connected`` holds the
    ``(endpoint, credentials, options)`` of every ``connect`` (not in
    ``calls``).

    State is per class (``reset()`` clears it), because the control plane
    builds a fresh instance from the cluster record each time."""

    calls: ClassVar[list] = []
    fenced: ClassVar[set] = set()
    fail: ClassVar[set] = set()
    fail_falsy: ClassVar[set] = set()
    luns: ClassVar[dict] = {}
    hosts: ClassVar[dict] = {}
    connected: ClassVar[list] = []
    max_attachments: ClassVar[int] = 16
    _lock = threading.RLock()
    _next: ClassVar[int] = 0

    @classmethod
    def reset(cls):
        with cls._lock:
            cls.calls = []
            cls.fenced = set()
            cls.fail = set()
            cls.fail_falsy = set()
            cls.luns = {}
            cls.hosts = {}
            cls.connected = []
            cls.max_attachments = 16
            cls._next = 0

    def _record(self, op, *args):
        with self._lock:
            type(self).calls.append((op, *args))
        if op in self.fail:
            raise ArrayError(f"fake array: {op} {args} failed")

    def _lun(self, lun_id):
        lun = self.luns.get(lun_id)
        if lun is None:
            # Accept the WWN as well, as the EBS plugin does.
            lun = next((x for x in self.luns.values() if x["wwn"] == lun_id), None)
        if lun is None:
            raise ArrayError(f"fake array: no LUN {lun_id}")
        self.check_owned(lun_id, lun["tags"])
        return lun

    def connect(self, endpoint, credentials, options):
        super().connect(endpoint, credentials, options)
        if "connect" in self.fail:
            raise ArrayError("fake array: connect refused")
        with self._lock:
            type(self).connected.append((endpoint, dict(credentials or {}), dict(options or {})))

    def capabilities(self):
        return {"protocols": ["fake"], "supports_reservations": False,
                "max_attachments": self.max_attachments}

    def ensure_host(self, name, initiators):
        self._record("ensure_host", name)
        host_id = f"host-{name}"
        with self._lock:
            type(self).hosts[host_id] = {"name": name, "initiators": dict(initiators or {})}
        return host_id

    def create_lun(self, name, size_bytes, tags):
        self._record("create_lun", name, size_bytes)
        all_tags = self.cluster_tags(tags, name)
        with self._lock:
            for lun in self.luns.values():
                if lun["name"] == name and lun["tags"].get("simplyblock-cluster-id") == self.cluster_id:
                    return {"id": lun["id"], "wwn": lun["wwn"], "size": lun["size"]}
            type(self)._next += 1
            lun_id = f"lun-{self._next}"
            type(self).luns[lun_id] = {"id": lun_id, "name": name, "wwn": f"wwn{self._next:04d}",
                                       "size": int(size_bytes), "tags": all_tags, "hosts": []}
        return {"id": lun_id, "wwn": f"wwn{self._next:04d}", "size": int(size_bytes)}

    def add_foreign_lun(self, lun_id, wwn="foreign", tags=None):
        """A LUN on the array that this cluster did not create (tests)."""
        with self._lock:
            type(self).luns[lun_id] = {"id": lun_id, "name": lun_id, "wwn": wwn, "size": 1,
                                       "tags": dict(tags or {}), "hosts": []}

    def attach(self, lun_id, host_id):
        self._record("attach", lun_id, host_id)
        with self._lock:
            lun = self._lun(lun_id)
            if host_id not in lun["hosts"]:
                if len(lun["hosts"]) >= self.max_attachments:
                    raise ArrayError(f"fake array: {lun_id} is attached to {self.max_attachments} hosts")
                lun["hosts"].append(host_id)

    def detach(self, lun_id, host_id, force=False):
        self._record("detach", lun_id, host_id, force)
        with self._lock:
            lun = self._lun(lun_id)
            if host_id in lun["hosts"]:
                lun["hosts"].remove(host_id)

    def resize(self, lun_id, size_bytes):
        self._record("resize", lun_id, size_bytes)
        with self._lock:
            lun = self._lun(lun_id)
            if int(size_bytes) < lun["size"]:
                raise ArrayError(f"fake array: {lun_id} cannot shrink")
            lun["size"] = int(size_bytes)

    def get(self, lun_id):
        with self._lock:
            lun = self._lun(lun_id)
            return {"id": lun["id"], "wwn": lun["wwn"], "size": lun["size"],
                    "hosts": list(lun["hosts"])}

    def delete_lun(self, lun_id):
        self._record("delete_lun", lun_id)
        with self._lock:
            try:
                self._lun(lun_id)
            except ForeignLunError:
                raise
            except ArrayError:
                return
            type(self).luns.pop(lun_id, None)

    def list_luns(self):
        with self._lock:
            return [{"id": x["id"], "wwn": x["wwn"], "size": x["size"], "hosts": list(x["hosts"])}
                    for x in self.luns.values()
                    if x["tags"].get("simplyblock-cluster-id") == self.cluster_id]

    # fence / unfence: recorded as before (phase 3 tests fence LUNs by WWN
    # that were never created here); applied to a LUN the fake knows.

    def _fence_op(self, op, lun, host):
        with self._lock:
            type(self).calls.append((op, lun, host))
        if op in self.fail:
            raise ArrayError(f"fake array: {op} of {host} on {lun} failed")
        if op in self.fail_falsy:
            return False
        with self._lock:
            known = lun in self.luns or any(x["wwn"] == lun for x in self.luns.values())
            if known:
                entry = self._lun(lun)
                if op == "fence" and host in entry["hosts"]:
                    entry["hosts"].remove(host)
                elif op == "unfence" and host not in entry["hosts"]:
                    entry["hosts"].append(host)
            if op == "fence":
                type(self).fenced.add((lun, host))
            else:
                type(self).fenced.discard((lun, host))
        return True

    def fence(self, lun_id, host_id):
        return self._fence_op("fence", lun_id, host_id)

    def unfence(self, lun_id, host_id):
        return self._fence_op("unfence", lun_id, host_id)
