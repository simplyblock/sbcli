"""Storage-array integration of the multi-attach array LUN mode.

A multi-attach cluster (``Cluster.multi_attach``) runs on array LUNs that are
presented to several storage nodes. Before a LUN is reopened on another node
the control plane must fence the node that served it at the array: that node's
host multipath layer may still hold queued writes, and once released they would
land after, and overwrite, newer data written through the new node.

This package is the minimal interface the takeover needs (phase 3): an
abstract :class:`ArrayPlugin` with ``fence`` / ``unfence`` / ``capabilities``,
and a registry that loads a vendor plugin by name from the entry-point group
``simplyblock.array_plugins``. Credentials (KMS) and further operations are
phase 4.

Plugins are found only through the entry-point group or an explicit
:func:`register_plugin` call. The test double in :mod:`.fake` is never
registered by default, so a cluster cannot be configured with a plugin that
fences nothing.
"""

from __future__ import annotations

import abc
import logging
import threading
from importlib import metadata

logger = logging.getLogger()

#: The entry-point group vendor plugins register under; the entry point's
#: name is the value of ``Cluster.array_plugin``.
ENTRY_POINT_GROUP = "simplyblock.array_plugins"


class ArrayError(Exception):
    """An array operation failed, or no plugin could be loaded."""


class ArrayNotConfigured(ArrayError):
    """The cluster names no array plugin."""


class ArrayPlugin(abc.ABC):
    """The array operations the control plane needs.

    ``lun_wwn`` is the LUN's world-wide name as the hosts report it
    (``NVMeDevice.lun_wwn``); ``host`` is the storage node's identity on the
    array (see ``multi_attach.array_host``). Every operation is idempotent: a
    fence of a host that is already fenced succeeds. A method reports failure
    by raising (any exception, preferably :class:`ArrayError`) or returning a
    falsy value; the caller treats both the same way.
    """

    def __init__(self, endpoint: str = "", options: dict | None = None, credentials=None):
        self.endpoint = endpoint
        self.options = dict(options or {})
        # Phase 4 resolves these from the KMS; a placeholder until then.
        self.credentials = credentials

    @abc.abstractmethod
    def fence(self, lun_wwn: str, host: str) -> bool:
        """Revoke ``host``'s access to the LUN so no I/O of it, including I/O
        queued in its multipath layer, can reach the LUN any more. Returns
        only once the array has applied it."""

    @abc.abstractmethod
    def unfence(self, lun_wwn: str, host: str) -> bool:
        """Give ``host`` access to the LUN again."""

    def capabilities(self) -> set[str]:
        """Names of the optional operations the plugin supports (phase 4)."""
        return {"fence", "unfence"}


_registry: dict[str, type[ArrayPlugin]] = {}
_registry_lock = threading.Lock()


def register_plugin(name: str, cls: type[ArrayPlugin]) -> None:
    """Register ``cls`` under ``name`` in this process (tests; built-in
    plugins). An entry point of the same name is shadowed."""
    if not (isinstance(cls, type) and issubclass(cls, ArrayPlugin)):
        raise TypeError(f"{cls!r} is not an ArrayPlugin")
    with _registry_lock:
        _registry[name] = cls


def unregister_plugin(name: str) -> None:
    with _registry_lock:
        _registry.pop(name, None)


def _entry_point(name: str):
    eps = metadata.entry_points()
    if hasattr(eps, "select"):
        found = list(eps.select(group=ENTRY_POINT_GROUP, name=name))
    else:  # pragma: no cover - Python < 3.10
        found = [ep for ep in eps.get(ENTRY_POINT_GROUP, []) if ep.name == name]
    return found[0] if found else None


def plugin_class(name: str) -> type[ArrayPlugin]:
    """The plugin class registered as ``name``.

    Raises:
        ArrayError: no plugin of that name, or it is not an ArrayPlugin.
    """
    with _registry_lock:
        cls = _registry.get(name)
    if cls is not None:
        return cls
    ep = _entry_point(name)
    if ep is None:
        raise ArrayError(f"no array plugin {name!r} in entry-point group {ENTRY_POINT_GROUP}")
    try:
        cls = ep.load()
    except Exception as e:
        raise ArrayError(f"array plugin {name!r} failed to load: {e}") from e
    if not (isinstance(cls, type) and issubclass(cls, ArrayPlugin)):
        raise ArrayError(f"array plugin {name!r} ({ep.value}) is not an ArrayPlugin")
    return cls


def load_plugin(name: str, endpoint: str = "", options: dict | None = None,
                credentials=None) -> ArrayPlugin:
    """An instance of plugin ``name`` for ``endpoint``.

    Raises:
        ArrayNotConfigured: ``name`` is empty.
        ArrayError: the plugin cannot be found, loaded or constructed.
    """
    if not name:
        raise ArrayNotConfigured("no array plugin is configured")
    cls = plugin_class(name)
    try:
        return cls(endpoint=endpoint, options=options, credentials=credentials)
    except Exception as e:
        raise ArrayError(f"array plugin {name!r} could not be set up: {e}") from e


def plugin_for_cluster(cluster) -> ArrayPlugin:
    """The array plugin a cluster is configured with (``Cluster.array_plugin``
    / ``array_endpoint`` / ``array_options``).

    Raises:
        ArrayNotConfigured: the cluster names no plugin.
        ArrayError: it cannot be loaded.
    """
    return load_plugin(
        getattr(cluster, "array_plugin", "") or "",
        getattr(cluster, "array_endpoint", "") or "",
        getattr(cluster, "array_options", None) or {},
    )
