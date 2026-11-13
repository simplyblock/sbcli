"""Storage-array integration of the multi-attach array LUN mode.

A multi-attach cluster (``Cluster.multi_attach``) runs on array LUNs that are
presented to several storage nodes. Before a LUN is reopened on another node
the control plane must fence the node that served it at the array: that node's
host multipath layer may still hold queued writes, and once released they would
land after, and overwrite, newer data written through the new node.

This package holds the plugin interface (:class:`ArrayPlugin`: LUN create /
attach / detach / resize / get / delete / list and fence / unfence) and a
registry that loads a vendor plugin by name from the entry-point group
``simplyblock.array_plugins``. The array's credentials live in the KMS at
``cluster/<cluster id>/array/<array id>``; the Cluster record keeps the plugin
name, endpoint and options only. Built-in plugin: ``aws-ebs``
(:mod:`.aws_ebs`). Registration and the LUN lifecycle are in
:mod:`simplyblock_core.controllers.array_controller`.

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

#: Plugins shipped with simplyblock (also declared as entry points in
#: pyproject.toml).
BUILTIN_PLUGINS = {
    "aws-ebs": "simplyblock_core.array.aws_ebs:AwsEbsPlugin",
}


class ArrayError(Exception):
    """An array operation failed, or no plugin could be loaded."""


class ArrayNotConfigured(ArrayError):
    """The cluster names no array plugin."""


#: Tag every LUN a plugin creates carries, with the cluster id as its value.
#: A plugin acts only on LUNs carrying its cluster's tag (ForeignLunError).
TAG_CLUSTER_ID = "simplyblock-cluster-id"
#: Tag carrying the LUN name the control plane asked for.
TAG_LUN_NAME = "simplyblock-lun-name"


class ForeignLunError(ArrayError):
    """The LUN was not created by this cluster's plugin: it carries no, or
    another, cluster tag. It is never touched."""


class ArrayPlugin(abc.ABC):
    """The array operations the control plane needs.

    simplyblock creates, presents (multi-attaches), grows, fences and deletes
    the LUNs of a multi-attach cluster itself, through one plugin per vendor.

    Identities: ``lun_id`` is the array's own id of a LUN, as returned by
    :meth:`create_lun` (``NVMeDevice.array_lun_id``); a plugin should also
    accept the LUN's ``wwn`` as the hosts report it where it can map it
    (phase 3 fenced by ``NVMeDevice.lun_wwn``). ``host_id`` is the array's id
    of a storage node's host, as returned by :meth:`ensure_host`
    (``StorageNode.array_host_id``).

    Every operation is idempotent: creating a LUN that exists returns it,
    attaching an attached LUN, detaching a detached one and fencing a fenced
    host succeed. A method reports failure by raising (preferably
    :class:`ArrayError`); ``fence`` / ``unfence`` may also return a falsy
    value. A plugin acts only on LUNs it created for ``cluster_id``: it tags
    each with :data:`TAG_CLUSTER_ID` and raises :class:`ForeignLunError` for
    any other LUN.

    Credentials (``connect``) are a flat dict of ``SecretStr`` (from the
    KMS); a plugin unwraps them only where it hands them to its SDK and never
    logs them.
    """

    def __init__(self, endpoint: str = "", options: dict | None = None, credentials=None,
                 cluster_id: str = ""):
        self.endpoint = endpoint
        self.options = dict(options or {})
        self.credentials = dict(credentials or {})
        self.cluster_id = cluster_id

    def __repr__(self):
        # Never the credentials.
        return f"{type(self).__name__}(endpoint={self.endpoint!r}, cluster_id={self.cluster_id!r})"

    def connect(self, endpoint: str, credentials: dict, options: dict) -> None:
        """Set up the session to the array. Called once per instance, before
        any other operation; raises when the array cannot be reached or the
        credentials are refused. The default only stores the arguments."""
        self.endpoint = endpoint or ""
        self.credentials = dict(credentials or {})
        self.options = dict(options or {})

    def capabilities(self) -> dict:
        """``{"protocols": [...], "supports_reservations": bool,
        "max_attachments": int}`` (0 = no limit known)."""
        return {"protocols": [], "supports_reservations": False, "max_attachments": 0}

    # ------------------------------------------------------------ ownership

    def cluster_tags(self, tags: dict | None = None, name: str = "") -> dict:
        """``tags`` plus the ownership tags every created LUN carries."""
        if not self.cluster_id:
            raise ArrayError("the plugin has no cluster id; it cannot tag what it creates")
        out = {str(k): str(v) for k, v in (tags or {}).items()}
        out[TAG_CLUSTER_ID] = self.cluster_id
        if name:
            out[TAG_LUN_NAME] = name
        return out

    def check_owned(self, lun_id: str, tags: dict | None) -> None:
        """Raise :class:`ForeignLunError` unless ``tags`` (the LUN's tags on the
        array) carry this cluster's id."""
        if not self.cluster_id or (tags or {}).get(TAG_CLUSTER_ID) != self.cluster_id:
            raise ForeignLunError(
                f"LUN {lun_id} was not created by simplyblock cluster {self.cluster_id or '?'}; "
                "it is not touched")

    # ---------------------------------------------------------- operations

    @abc.abstractmethod
    def ensure_host(self, name: str, initiators: dict) -> str:
        """The array's id of the host ``name`` with ``initiators`` (vendor
        specific: NQNs, WWPNs, IQNs, an EC2 instance id), created when the
        array keeps host objects."""

    @abc.abstractmethod
    def create_lun(self, name: str, size_bytes: int, tags: dict) -> dict:
        """Create LUN ``name`` (at least ``size_bytes``) tagged with ``tags``
        plus :meth:`cluster_tags`, or return the existing one of that name.
        Returns ``{"id", "wwn", "size"}``; ``wwn`` is the identity the hosts
        report for it (lsblk WWN or SERIAL)."""

    @abc.abstractmethod
    def attach(self, lun_id: str, host_id: str) -> None:
        """Present the LUN to the host (multi-attach: in addition to the
        hosts it is presented to already)."""

    @abc.abstractmethod
    def detach(self, lun_id: str, host_id: str, force: bool = False) -> None:
        """Stop presenting the LUN to the host. ``force``: also when the host
        does not cooperate; returns only once the host can no longer reach
        the LUN."""

    @abc.abstractmethod
    def resize(self, lun_id: str, size_bytes: int) -> None:
        """Grow the LUN to at least ``size_bytes``; returns once the hosts can
        see the new size (a rescan away). Shrinking is refused."""

    @abc.abstractmethod
    def get(self, lun_id: str) -> dict:
        """``{"id", "wwn", "size", "hosts": [host ids it is presented to]}``."""

    @abc.abstractmethod
    def delete_lun(self, lun_id: str) -> None:
        """Delete the LUN, detaching it from every host first. Deleting a LUN
        that is gone succeeds."""

    @abc.abstractmethod
    def list_luns(self) -> list[dict]:
        """Every LUN of this cluster (tagged with its id), as :meth:`get`
        returns them."""

    # ------------------------------------------------------------- fencing

    def fence(self, lun_id: str, host_id: str) -> bool:
        """Revoke the host's access to the LUN so no I/O of it, including
        I/O queued in its multipath layer, can reach the LUN any more.
        Returns only once the array has applied it. Default: a forced
        detach."""
        self.detach(lun_id, host_id, force=True)
        return True

    def unfence(self, lun_id: str, host_id: str) -> bool:
        """Give the host access to the LUN again. Default: attach."""
        self.attach(lun_id, host_id)
        return True


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
    found = list(metadata.entry_points(group=ENTRY_POINT_GROUP, name=name))
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
    if ep is None and name in BUILTIN_PLUGINS:
        # The package's own entry point, when it runs from a source tree that
        # was never installed (entry points come from installed metadata).
        ep = metadata.EntryPoint(name=name, value=BUILTIN_PLUGINS[name], group=ENTRY_POINT_GROUP)
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
                credentials=None, cluster_id: str = "") -> ArrayPlugin:
    """A connected instance of plugin ``name`` for ``endpoint``.

    Raises:
        ArrayNotConfigured: ``name`` is empty.
        ArrayError: the plugin cannot be found, loaded, constructed or
            connected.
    """
    if not name:
        raise ArrayNotConfigured("no array plugin is configured")
    cls = plugin_class(name)
    try:
        plugin = cls(endpoint=endpoint, options=options, credentials=credentials,
                     cluster_id=cluster_id)
    except Exception as e:
        raise ArrayError(f"array plugin {name!r} could not be set up: {e}") from e
    try:
        plugin.connect(endpoint, dict(credentials or {}), dict(options or {}))
    except Exception as e:
        # An SDK error message does not carry the credentials, and they are
        # never formatted into one here.
        raise ArrayError(f"array plugin {name!r} could not connect to {endpoint or 'the array'}: {e}") from e
    return plugin


def array_credentials(cluster) -> dict:
    """The credentials of the cluster's array from the KMS (``SecretStr``
    values), or ``{}`` when the array was registered without any (e.g. the
    EBS plugin on the instance role) or no array is registered.

    Raises:
        ArrayError: a secret is recorded but cannot be read.
    """
    array_id = getattr(cluster, "array_id", "") or ""
    if not array_id or getattr(cluster, "array_has_credentials", False) is not True:
        return {}
    from simplyblock_core import kms
    try:
        with kms.create_kms_connection(cluster) as conn:
            return conn.get_secret(kms.array_secret_path(cluster.get_id(), array_id))
    except Exception as e:
        raise ArrayError(f"array credentials could not be read from the KMS: {e}") from e


def plugin_for_cluster(cluster) -> ArrayPlugin:
    """The connected array plugin a cluster is configured with
    (``Cluster.array_plugin`` / ``array_endpoint`` / ``array_options``,
    credentials from the KMS).

    Raises:
        ArrayNotConfigured: the cluster names no plugin.
        ArrayError: it cannot be loaded or connected.
    """
    name = getattr(cluster, "array_plugin", "") or ""
    if not name:
        raise ArrayNotConfigured("no array plugin is configured")
    return load_plugin(
        name,
        getattr(cluster, "array_endpoint", "") or "",
        getattr(cluster, "array_options", None) or {},
        credentials=array_credentials(cluster),
        cluster_id=cluster.get_id(),
    )
