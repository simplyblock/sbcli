"""In-memory array plugin for tests. Never registered by default."""

from __future__ import annotations

import threading
from typing import ClassVar

from simplyblock_core.array import ArrayError, ArrayPlugin


class FakeArrayPlugin(ArrayPlugin):
    """Records every call in ``calls`` (``(op, lun_wwn, host)``) and the
    fenced pairs in ``fenced``. ``fail`` = a set of operation names
    (``"fence"``, ``"unfence"``) that raise :class:`ArrayError`;
    ``fail_falsy`` the ones that return False instead.

    State is per class (``reset()`` clears it), because the takeover builds a
    fresh instance from the cluster record each time."""

    calls: ClassVar[list] = []
    fenced: ClassVar[set] = set()
    fail: ClassVar[set] = set()
    fail_falsy: ClassVar[set] = set()
    _lock = threading.Lock()

    @classmethod
    def reset(cls):
        with cls._lock:
            cls.calls = []
            cls.fenced = set()
            cls.fail = set()
            cls.fail_falsy = set()

    def _op(self, op, lun_wwn, host):
        with self._lock:
            type(self).calls.append((op, lun_wwn, host))
        if op in self.fail:
            raise ArrayError(f"fake array: {op} of {host} on {lun_wwn} failed")
        if op in self.fail_falsy:
            return False
        with self._lock:
            if op == "fence":
                type(self).fenced.add((lun_wwn, host))
            else:
                type(self).fenced.discard((lun_wwn, host))
        return True

    def fence(self, lun_wwn, host):
        return self._op("fence", lun_wwn, host)

    def unfence(self, lun_wwn, host):
        return self._op("unfence", lun_wwn, host)
