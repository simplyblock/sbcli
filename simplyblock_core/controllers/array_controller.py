"""Multi-attach array LUN mode, phase 4: the storage array of a cluster.

simplyblock creates, presents, grows, fences and deletes the LUNs of a
multi-attach cluster itself, through the cluster's array plugin
(simplyblock_core.array). This module holds:

* registration (``register_array`` / ``get_arrays`` / ``delete_array``): the
  plugin is loaded, connected and asked for its capabilities before anything
  is saved; credentials go to the KMS at ``cluster/<id>/array/<array id>``,
  plugin name, endpoint and options to the Cluster record. One array per
  cluster.
* the LUN lifecycle, every hook a no-op unless the cluster is multi-attach
  and has an array plugin (``array_configured``), and acting only on LUNs the
  plugin created (``NVMeDevice.array_lun_id``):

  - node add: ``provision_node_lun`` creates the node's LUN
    (``Cluster.array_lun_size``), registers the host, attaches the LUN and
    waits for it in the node agent's block-device inventory; the add then
    continues as a regular lblk add with that device. With the journal on
    the array (``Cluster.array_journal`` "lun") it first creates the node's
    journal LUN the same way, with the journal options (its pool), and
    presents it to that node only.
  - LVS roles: ``reconcile_lun_attachments`` presents every owner's LUN to
    its secondary and tertiary (and the node serving it), records the path
    there in ``node_paths``, and detaches it from a node that lost its role
    -- never from the serving node, never to a node fenced from it.
  - growth: ``grow_luns`` grows every LUN by one factor at the array and
    waits until every node it is presented to sees the new size.
  - node delete: ``release_node_luns``; cluster delete:
    ``delete_cluster_luns`` (the only place a LUN is deleted).
"""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid as uuid_mod

from pydantic import SecretStr

from simplyblock_core import array, multi_attach

logger = logging.getLogger()

#: How long a node add waits for its new LUN to show in the agent inventory.
LUN_APPEAR_TIMEOUT_SEC = 120
#: How long the attachment reconcile waits for a LUN on a newly attached node.
LUN_PATH_TIMEOUT_SEC = 30
#: How long a growth waits for every node to see the new size.
LUN_GROW_VISIBLE_TIMEOUT_SEC = 180
POLL_SEC = 3

#: Cluster.array_journal: the journal on a local disk or partition of the
#: node's host, or on a LUN of its own on the array.
JOURNAL_LOCAL = "local"
JOURNAL_LUN = "lun"
JOURNAL_PLACEMENTS = (JOURNAL_LOCAL, JOURNAL_LUN)
#: A journal LUN of no given size: this share of the data LUN, at least
#: JOURNAL_LUN_MIN_BYTES, rounded up to whole GiB.
JOURNAL_LUN_PERCENT = 3
JOURNAL_LUN_MIN_BYTES = 2 << 30

_STATUS_REMOVED = "removed"
_STATUS_ONLINE = "online"


class ArrayLifecycleError(Exception):
    """A LUN lifecycle step failed; the message says which."""


def _sleep(sec):
    time.sleep(sec)


def _db():
    from simplyblock_core.db_controller import DBController
    return DBController()


def array_configured(cluster) -> bool:
    """A multi-attach cluster with an array plugin: the lifecycle hooks act."""
    return multi_attach.is_multi_attach(cluster) and bool(getattr(cluster, "array_plugin", ""))


def provisions_luns(cluster) -> bool:
    """Node adds create the node's LUN (an array is configured and a LUN
    size set)."""
    return array_configured(cluster) and int(getattr(cluster, "array_lun_size", 0) or 0) > 0


def journal_on_lun(cluster) -> bool:
    """Node adds also create the node's journal LUN (``Cluster.array_journal``
    "lun" on a cluster that creates the nodes' LUNs)."""
    return provisions_luns(cluster) and (getattr(cluster, "array_journal", "") or JOURNAL_LOCAL) == JOURNAL_LUN


def journal_lun_size(cluster) -> int:
    """Size in bytes of a node's journal LUN."""
    size = int(getattr(cluster, "array_journal_lun_size", 0) or 0)
    if size:
        return size
    size = max(JOURNAL_LUN_MIN_BYTES, math.ceil(int(cluster.array_lun_size or 0) * JOURNAL_LUN_PERCENT / 100))
    return math.ceil(size / (1 << 30)) << 30


def _event(cluster_id, message, error=False):
    try:
        from simplyblock_core.controllers import events_controller as ec
        from simplyblock_core.models.events import EventObj
        ec.log_event_cluster(
            cluster_id=cluster_id, domain=ec.DOMAIN_CLUSTER, event=ec.EVENT_ARRAY_LUN,
            db_object=_db().get_cluster_by_id(cluster_id), caused_by=ec.CAUSED_BY_MONITOR,
            message=message, event_level=EventObj.LEVEL_ERROR if error else EventObj.LEVEL_INFO)
    except Exception:                                   # noqa: BLE001 - an event is best effort
        logger.exception("could not log the array event")


# ------------------------------------------------------------ registration

def _wrap_credentials(credentials) -> dict:
    out = {}
    for k, v in (credentials or {}).items():
        if not isinstance(k, str) or not k:
            raise ValueError("credential names must be non-empty strings")
        if isinstance(v, SecretStr):
            out[k] = v
        elif isinstance(v, str):
            out[k] = SecretStr(v)
        else:
            raise ValueError(f"credential {k!r} must be a string")
    return out


def _managed_devices(nodes):
    """``(owner, device)`` of every LUN the array plugin created."""
    return [(n, d) for n in nodes for d in (n.nvme_devices or [])
            if multi_attach.is_multi_attach(d) and d.array_lun_id]


def array_view(cluster, capabilities=None) -> dict:
    """The registered array of ``cluster`` as the API shows it: everything
    but the credentials (only whether there are any)."""
    out = {
        "id": cluster.array_id,
        "plugin": cluster.array_plugin,
        "endpoint": cluster.array_endpoint,
        "options": dict(cluster.array_options or {}),
        "lun_size": int(cluster.array_lun_size or 0),
        "journal": getattr(cluster, "array_journal", "") or JOURNAL_LOCAL,
        "journal_lun_size": (journal_lun_size(cluster)
                             if (getattr(cluster, "array_journal", "") or JOURNAL_LOCAL) == JOURNAL_LUN
                             and int(cluster.array_lun_size or 0) else 0),
        "journal_options": dict(getattr(cluster, "array_journal_options", None) or {}),
        "has_credentials": bool(cluster.array_has_credentials),
        "credential_ref": array.credential_ref_view(cluster),
    }
    if capabilities is not None:
        out["capabilities"] = capabilities
    return out


def register_array(cluster_id, plugin, endpoint="", options=None, credentials=None,
                   lun_size=0, credential_ref=None, journal=JOURNAL_LOCAL, journal_lun_size=0,
                   journal_options=None, allow_local_kms_credentials=None) -> dict:
    """Register (or re-register, e.g. to rotate credentials) the storage
    array of a multi-attach cluster.

    The plugin is loaded and connected with the credentials and its
    capabilities read before anything is saved. The Cluster record keeps the
    plugin name, endpoint, options, LUN size and a credential reference
    (``Cluster.array_credential_ref``), never a credential:

    * ``credentials`` (a flat dict of strings / SecretStr) are wrapped by the
      cluster's credentials key in its KMS (Vault Transit or KMIP; the local
      KMS only with ``allow_local_kms_credentials``), bound to the array id
      and endpoint, and only the wrapped form is kept;
    * ``credential_ref`` names a secret kept outside simplyblock instead
      (``{"kind": "vault-kv", "mount", "path"[, "version"]}`` or
      ``{"kind": "kmip-secret", "uid"[, "field"]}``): simplyblock reads it
      when it connects and never stores it.

    ``journal`` places each node's journal: ``"local"`` on a disk or
    partition of its host, ``"lun"`` on a LUN of its own that node adds
    create (needs ``lun_size``) with ``journal_options`` updating
    ``options`` (e.g. ``{"pool": "fast-pool"}``) and ``journal_lun_size``
    bytes (0 = 3% of ``lun_size``, at least 2 GiB). A change applies to
    nodes added afterwards.

    ``allow_local_kms_credentials`` (True / False; None leaves it as it is)
    sets ``Cluster.allow_local_kms_credentials``, the opt-in to keeping the
    wrapped credentials in the local key store of a cluster without an
    external KMS (test environments). It is saved only with a registration
    that succeeds.

    Returns the array view (``array_view``) with the plugin's capabilities.

    Raises:
        ValueError: not a multi-attach cluster, a bad journal placement,
            both or a bad credential
            source, the credentials cannot be stored or read, the plugin
            cannot be loaded or connected, its capabilities do not fit the
            cluster, or LUNs created through another plugin exist.
    """
    from simplyblock_core import credentials as creds_mod
    from simplyblock_core import kms
    db = _db()
    cluster = db.get_cluster_by_id(cluster_id)
    if not multi_attach.is_multi_attach(cluster):
        raise ValueError("an array is only used by a multi-attach cluster")
    if not plugin:
        raise ValueError("an array needs a plugin")
    if int(lun_size or 0) < 0:
        raise ValueError("lun_size must not be negative")
    journal = (journal or JOURNAL_LOCAL).lower()
    if journal not in JOURNAL_PLACEMENTS:
        raise ValueError(f"journal must be one of {', '.join(JOURNAL_PLACEMENTS)}, not {journal!r}")
    if int(journal_lun_size or 0) < 0:
        raise ValueError("journal_lun_size must not be negative")
    journal_opts = dict(journal_options or {})
    if journal == JOURNAL_LUN and not int(lun_size or 0):
        raise ValueError("a journal LUN is created at node add next to the node's data LUN; "
                         "it needs lun_size")
    if journal != JOURNAL_LUN and (journal_opts or int(journal_lun_size or 0)):
        raise ValueError("journal_options and journal_lun_size apply to the journal placement 'lun' only")
    creds = _wrap_credentials(credentials)
    if creds and credential_ref:
        raise ValueError("give the credentials or a reference to them, not both")
    opts = dict(options or {})
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    if (cluster.array_plugin and cluster.array_plugin != plugin
            and _managed_devices(nodes)):
        raise ValueError(
            f"LUNs created through plugin {cluster.array_plugin!r} exist; the plugin cannot change")
    array_id = cluster.array_id or str(uuid_mod.uuid4())
    context = array.credential_context(array_id, endpoint or "")
    ref: dict = {}
    if credential_ref:
        try:
            parsed = creds_mod.CREDENTIAL_REF_ADAPTER.validate_python(dict(credential_ref))
        except Exception as e:                          # noqa: BLE001 - pydantic
            raise ValueError(f"credential_ref is not a valid reference: {e}") from None
        if isinstance(parsed, creds_mod.ManagedCredentialRef):
            raise ValueError("credential_ref names an external secret (vault-kv or kmip-secret); "
                             "give the credentials themselves to have them stored")
        ref = parsed.model_dump()
        try:
            resolved = creds_mod.CredentialProvider(cluster, ttl_s=0).resolve(parsed, context)
        except creds_mod.CredentialError as e:
            raise ValueError(f"the referenced credentials could not be read: {e}") from None
        creds = {name: resolved.get(name) for name in resolved.field_names
                 if name != creds_mod.CONTEXT_FIELD}
    try:
        inst = array.load_plugin(plugin, endpoint, opts, credentials=creds, cluster_id=cluster_id)
        caps = dict(inst.capabilities() or {})
    except array.ArrayError as e:
        raise ValueError(str(e)) from e
    except Exception as e:                              # noqa: BLE001 - a vendor plugin
        raise ValueError(f"array plugin {plugin!r} failed: {e}") from e
    if journal_opts:
        # The journal LUNs' options are checked by the plugin, as the
        # array's are, before anything is saved.
        try:
            array.load_plugin(plugin, endpoint, dict(opts, **journal_opts), credentials=creds,
                              cluster_id=cluster_id)
        except array.ArrayError as e:
            raise ValueError(f"journal options: {e}") from e
        except Exception as e:                          # noqa: BLE001 - a vendor plugin
            raise ValueError(f"journal options: array plugin {plugin!r} failed: {e}") from e
    max_att = int(caps.get("max_attachments") or 0)
    # Owner, secondary, tertiary: FTT + 1 hosts per LUN.
    needed = int(getattr(cluster, "max_fault_tolerance", 1) or 1) + 1
    if max_att and max_att < needed:
        raise ValueError(f"the array presents a LUN to at most {max_att} hosts; the cluster needs {needed}")

    if allow_local_kms_credentials is not None:
        # Asked for with the registration, so it applies to this very store;
        # the record keeps it only if the registration is saved below.
        cluster.allow_local_kms_credentials = bool(allow_local_kms_credentials)
    if creds and not ref:
        try:
            ref = creds_mod.CredentialProvider(cluster).store(creds, context).model_dump()
        except creds_mod.CredentialError as e:
            raise ValueError(f"the array credentials could not be stored: {e}") from None
    # A secret an earlier registration kept at the KMS path (before
    # credential references) is removed once the new registration is saved.
    legacy_secret = bool(cluster.array_has_credentials) and not cluster.array_credential_ref

    def mutate(fresh):
        fresh.array_id = array_id
        fresh.array_plugin = plugin
        fresh.array_endpoint = endpoint or ""
        fresh.array_options = opts
        fresh.array_credential_ref = ref
        fresh.array_has_credentials = bool(ref)
        fresh.array_lun_size = int(lun_size or 0)
        fresh.array_journal = journal
        fresh.array_journal_lun_size = int(journal_lun_size or 0)
        fresh.array_journal_options = journal_opts
        if allow_local_kms_credentials is not None:
            fresh.allow_local_kms_credentials = bool(allow_local_kms_credentials)

    db.atomic_update(cluster, mutate)
    if legacy_secret:
        try:
            with kms.create_kms_connection(cluster) as conn:
                conn.delete_secret(kms.array_secret_path(cluster_id, array_id))
        except Exception:                               # noqa: BLE001
            logger.exception("could not remove the array's earlier KMS secret")
    array.forget_plugin(cluster_id)
    cluster = db.get_cluster_by_id(cluster_id)
    logger.info("Cluster %s: array %s registered (plugin %s, endpoint %s)",
                cluster_id, array_id, plugin, endpoint or "-")
    return array_view(cluster, caps)


def get_arrays(cluster_id) -> list[dict]:
    """The registered arrays of a cluster (zero or one), without credentials."""
    cluster = _db().get_cluster_by_id(cluster_id)
    if not cluster.array_plugin:
        return []
    return [array_view(cluster)]


def delete_array(cluster_id, array_id, force=False) -> None:
    """Remove the cluster's array registration and its credentials.

    Refused while LUNs created through it exist (a takeover could no longer
    fence), unless ``force``.

    Raises:
        KeyError: no array ``array_id``.
        ValueError: LUNs of the array exist.
    """
    from simplyblock_core import kms
    db = _db()
    cluster = db.get_cluster_by_id(cluster_id)
    if not cluster.array_plugin or (cluster.array_id or "") != (array_id or ""):
        raise KeyError(f"array {array_id} not found")
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    managed = [d.array_lun_id for _, d in _managed_devices(nodes)]
    managed += [n.array_journal_lun_id for n in nodes if getattr(n, "array_journal_lun_id", "")]
    if managed and not force:
        raise ValueError(f"{len(managed)} LUN(s) created through the array exist; "
                         "takeovers would lose their fence")
    # A credential reference needs no cleanup: a managed one exists only in
    # the record, an external one belongs to whoever created it.
    if cluster.array_id and cluster.array_has_credentials and not cluster.array_credential_ref:
        with kms.create_kms_connection(cluster) as conn:
            conn.delete_secret(kms.array_secret_path(cluster_id, cluster.array_id))

    def mutate(fresh):
        fresh.array_id = ""
        fresh.array_plugin = ""
        fresh.array_endpoint = ""
        fresh.array_options = {}
        fresh.array_credential_ref = {}
        fresh.array_has_credentials = False
        fresh.array_lun_size = 0
        fresh.array_journal = JOURNAL_LOCAL
        fresh.array_journal_lun_size = 0
        fresh.array_journal_options = {}

    db.atomic_update(cluster, mutate)
    logger.info("Cluster %s: array %s removed", cluster_id, array_id)


# ------------------------------------------------------------------- hosts

#: SAN initiator keys the node agent reports (``san_initiators`` of its
#: /info; node_utils.get_san_initiators) and SAN plugins read.
SAN_INITIATOR_KEYS = ("iscsi_iqn", "fc_wwpns", "nvme_hostnqn", "sdc_guid")


def host_initiators(hostname="", cloud_instance=None, system_id="", ip="",
                    san_initiators=None) -> dict:
    """What identifies a host on an array: its hostname, system id, IP, its
    SAN initiators (iSCSI IQN, FC WWPNs, NVMe host NQN, PowerFlex SDC GUID,
    as its agent reports them) and, on AWS, the EC2 instance id (reported by
    the node agent from the instance metadata service when its cloud probe
    runs)."""
    out = {"hostname": hostname}
    for key in SAN_INITIATOR_KEYS:
        value = (san_initiators or {}).get(key)
        if value:
            out[key] = value
    if ip:
        out["ip"] = ip
    if system_id:
        out["system_id"] = system_id
    cloud = cloud_instance or {}
    if cloud.get("cloud") == "amazon" and str(cloud.get("id", "")).startswith("i-"):
        out["ec2_instance_id"] = cloud["id"]
    return out


def _agent_san_initiators(node) -> dict:
    """The SAN initiators the node's agent reports now ({} when it cannot be
    asked, or is older than the report)."""
    try:
        info, _ = node.client(timeout=15, retry=1).info()
    except Exception as e:                              # noqa: BLE001
        logger.warning("Node %s: SAN initiators not read from its agent: %s", node.get_id(), e)
        return {}
    return dict((info or {}).get("san_initiators") or {})


def _node_initiators(node) -> dict:
    cloud = {"cloud": getattr(node, "cloud_name", ""), "id": getattr(node, "cloud_instance_id", "")}
    return host_initiators(multi_attach.array_host(node), cloud, getattr(node, "system_uuid", ""),
                           getattr(node, "mgmt_ip", "") or "", _agent_san_initiators(node))


def ensure_node_host(plugin, node, db=None) -> str:
    """The node's host id on the array, registered and recorded on the node
    (``StorageNode.array_host_id``) the first time."""
    host_id = getattr(node, "array_host_id", "") or ""
    if host_id:
        return host_id
    host_id = plugin.ensure_host(multi_attach.array_host(node), _node_initiators(node))
    if not host_id:
        raise ArrayLifecycleError(f"the array returned no host id for node {node.get_id()}")
    db = db or _db()

    def _set(n):
        n.array_host_id = host_id
    db.atomic_update(node, _set)
    node.array_host_id = host_id
    return host_id


# ---------------------------------------------------------------- node add

def lun_name(hostname, socket, journal=False) -> str:
    return f"simplyblock-{hostname}-s{socket if socket is not None else 0}" + ("-journal" if journal else "")


def _find_in_inventory(inventory, wwn):
    """The host's device of LUN ``wwn``: its multipath map where the LUN is
    reached over several paths (FC, iSCSI with several sessions), else its
    disk."""
    want = multi_attach.normalize_wwn(wwn)
    matches = [d for d in inventory or []
               if d.get("type", "disk") in ("disk", "mpath")
               and want in (multi_attach.normalize_wwn(d.get("wwn", "")),
                            multi_attach.normalize_wwn(d.get("serial", "")))]
    maps = [d for d in matches if d.get("type") == "mpath"]
    if maps:
        return maps[0]
    # A path of a map whose map is not listed (an older agent): never alone.
    singles = [d for d in matches if not d.get("multipath_map")]
    return singles[0] if singles else None


def provision_node_lun(cluster, snode_api, node_info, node_config, lblk_configured,
                       timeout=LUN_APPEAR_TIMEOUT_SEC, node_ip="", journal=False):
    """Create (or find) the LUN of the node being added, present it to the
    node's host and wait until the host sees it.

    With ``journal`` it is the node's journal LUN (``journal_on_lun``):
    created with ``Cluster.array_journal_options`` updating the array's
    options and ``journal_lun_size`` bytes, and its entry flagged as the
    journal. It is presented to this node only, ever.

    Returns ``(entry, host_id)``: ``entry`` is an lblk config entry for the
    LUN (identified by its serial; carries ``lun_wwn`` and ``array_lun_id``)
    to add to the node's ``lblk_devices``, ``host_id`` the host's id on the
    array. A configured entry of the same kind (data or journal) that already
    carries an ``array_lun_id`` (a retried add) is reused.

    Raises:
        ArrayLifecycleError: any step failed.
    """
    hostname = node_info.get("hostname", "")
    socket = node_config.get("socket")
    try:
        plugin = array.plugin_for_cluster(
            cluster, dict(getattr(cluster, "array_journal_options", None) or {}) if journal else None)
        host_id = plugin.ensure_host(hostname, host_initiators(
            hostname, node_info.get("cloud_instance"), node_info.get("system_id", ""), node_ip,
            node_info.get("san_initiators")))
        existing = next((e for e in lblk_configured or []
                         if e.get("array_lun_id") and bool(e.get("journal")) == bool(journal)), None)
        if existing:
            lun = {"id": existing["array_lun_id"], "wwn": existing.get("lun_wwn", "")}
        else:
            tags = {"simplyblock-host": hostname,
                    "simplyblock-socket": str(socket if socket is not None else 0)}
            if journal:
                tags["simplyblock-role"] = "journal"
            lun = plugin.create_lun(lun_name(hostname, socket, journal),
                                    journal_lun_size(cluster) if journal else int(cluster.array_lun_size),
                                    tags)
        plugin.attach(lun["id"], host_id)
    except array.ArrayError as e:
        raise ArrayLifecycleError(f"array: {e}") from e
    except Exception as e:                              # noqa: BLE001 - a vendor plugin
        raise ArrayLifecycleError(f"array plugin failed: {e}") from e
    logger.info("Node %s socket %s: %sLUN %s (%s) attached as host %s",
                hostname, socket, "journal " if journal else "", lun["id"], lun["wwn"], host_id)

    deadline = time.monotonic() + timeout
    while True:
        _scan_new_luns(snode_api)
        inventory, err = snode_api.get_blockdevices()
        found = _find_in_inventory(inventory, lun["wwn"])
        if found:
            break
        if time.monotonic() >= deadline:
            raise ArrayLifecycleError(
                f"LUN {lun['id']} ({lun['wwn']}) did not appear on {hostname} within {timeout}s"
                + (f": {err}" if err else ""))
        _sleep(POLL_SEC)
    entry = {
        "name": found["name"],
        "serial": found.get("serial") or lun["wwn"],
        "size": int(found.get("size") or 0),
        "by_id": found.get("by_id_path", ""),
        "numa": int(found.get("numa_node", -1)),
        "model": found.get("model", ""),
        "type": "disk",
        # A multipath map's kernel name (dm-N) makes it a bdev_uring device.
        "kname": found.get("kname", found["name"]),
        "lun_wwn": lun["wwn"],
        "array_lun_id": lun["id"],
    }
    if journal:
        entry["journal"] = True
    return entry, host_id


def journal_lun_id(lblk_entries) -> str:
    """The array id of the journal LUN among a node's lblk entries ("")."""
    return next((e["array_lun_id"] for e in lblk_entries or []
                 if e.get("journal") and e.get("array_lun_id")), "")


def add_lun_to_config(lblk_configured, entry) -> list:
    """``lblk_configured`` with the LUN entry in place of an older entry for
    the same LUN (same serial or array id).

    On a multi-attach cluster a node's local devices can only hold its
    journal (a data device must be reachable from the standbys). When the
    host config names exactly one local device and flags none as the
    journal, that device is flagged, so the journal is never placed on the
    LUN by the smallest-device fallback."""
    out = [dict(e) for e in lblk_configured or []
           if e.get("serial") != entry["serial"]
           and not (e.get("array_lun_id") and e.get("array_lun_id") == entry["array_lun_id"])]
    local = [e for e in out if not e.get("array_lun_id")]
    if len(local) == 1 and not any(e.get("journal") for e in out):
        local[0]["journal"] = True
    out.append(entry)
    return out


# ------------------------------------------------------------- attachments

def _scan_new_luns(snode_api) -> None:
    """Have the host look for a LUN just presented to it. A SAN host sees a
    new LUN only after a SCSI / NVMe rescan (an EBS attachment shows up by
    itself, so a failure, or an agent without the endpoint, is not fatal:
    the caller keeps polling)."""
    try:
        _, err = snode_api.scan_new_luns()
        if err:
            logger.debug("scan_new_luns: %s", err)
    except Exception as e:                              # noqa: BLE001
        logger.debug("scan_new_luns: %s", e)


def _lun_path_on(node, dev, timeout):
    """The path of the device's LUN on ``node`` from its agent, or ""."""
    owner_path = (dev.node_paths or {}).get(dev.node_id, {}).get("path", "")
    deadline = time.monotonic() + timeout
    while True:
        try:
            _scan_new_luns(node.client(timeout=15, retry=1))
            entries, _ = node.client(timeout=15, retry=1).lun_info(dev.lun_wwn)
        except Exception as e:                          # noqa: BLE001
            logger.warning("lun_info on %s: %s", node.get_id(), e)
            entries = None
        for entry in entries or []:
            if owner_path and (owner_path in (entry.get("by_id_paths") or []) or owner_path == entry.get("by_id_path")):
                return owner_path
        if entries:
            first = entries[0]
            return first.get("by_id_path") or first.get("device_path") or ""
        if time.monotonic() >= deadline:
            return ""
        _sleep(POLL_SEC)


def desired_hosts(owner, dev, nodes_by_id) -> set:
    """Node ids the LUN of ``dev`` must be presented to: its owner, the
    owner's LVS secondary and tertiary, and the node serving it -- minus
    removed nodes and nodes fenced from it (the serving node always stays)."""
    serving = dev.serving_node()
    want = {owner.get_id(), getattr(owner, "secondary_node_id", ""),
            getattr(owner, "tertiary_node_id", ""), serving}
    fenced = set(dev.fenced_node_ids or [])
    out = set()
    for nid in want:
        node = nodes_by_id.get(nid)
        if not nid or node is None or node.status == _STATUS_REMOVED:
            continue
        if nid in fenced and nid != serving:
            continue
        out.add(nid)
    return out


def reconcile_lun_attachments(cluster_id, path_timeout=LUN_PATH_TIMEOUT_SEC) -> dict:
    """Present every array-created LUN of the cluster to the nodes in
    ``desired_hosts`` and only to them, and record its path on each.

    Attaches before it detaches, never detaches the serving node, never
    attaches a fenced node. A node already recorded in
    ``array_attached_node_ids`` costs no array call, so the periodic run is
    free while nothing changes. Returns ``{"attached": [...], "detached":
    [...], "errors": {device_id: msg}}``; never raises for one LUN."""
    report: dict = {"attached": [], "detached": [], "errors": {}}
    db = _db()
    cluster = db.get_cluster_by_id(cluster_id)
    if not array_configured(cluster):
        return report
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    nodes_by_id = {n.get_id(): n for n in nodes}
    work = []
    for owner, dev in _managed_devices(nodes):
        if owner.status == _STATUS_REMOVED:
            continue
        want = desired_hosts(owner, dev, nodes_by_id)
        have = set(dev.array_attached_node_ids or [])
        missing_paths = [nid for nid in want
                         if nid != owner.get_id() and nid not in (dev.node_paths or {})]
        if want != have or missing_paths:
            work.append((owner, dev, want))
    if not work:
        return report
    try:
        plugin = array.plugin_for_cluster(cluster)
    except array.ArrayError as e:
        for _, dev, _ in work:
            report["errors"][dev.get_id()] = str(e)
        logger.error("LUN attachments of cluster %s not reconciled: %s", cluster_id, e)
        return report

    for owner, dev, want in work:
        try:
            _reconcile_one(db, plugin, owner, dev, want, nodes_by_id, path_timeout, report)
        except Exception as e:                          # noqa: BLE001 - one LUN never stops the rest
            report["errors"][dev.get_id()] = str(e)
            logger.error("LUN %s of device %s: attachments not reconciled: %s",
                         dev.array_lun_id, dev.get_id(), e)
    if report["errors"]:
        _event(cluster_id, f"Array LUN attachments incomplete: {report['errors']}", error=True)
    return report


def _reconcile_one(db, plugin, owner, dev, want, nodes_by_id, path_timeout, report):
    lun_id = dev.array_lun_id
    serving = dev.serving_node()
    host_of = {}
    for nid in want:
        host_of[nid] = ensure_node_host(plugin, nodes_by_id[nid], db)
    on_array = set(plugin.get(lun_id).get("hosts") or [])
    node_of_host = {}
    for n in nodes_by_id.values():
        hid = getattr(n, "array_host_id", "") or ""
        if hid:
            node_of_host[hid] = n.get_id()
    for nid in sorted(want):
        if host_of[nid] not in on_array:
            plugin.attach(lun_id, host_of[nid])
            report["attached"].append((dev.get_id(), nid))
            logger.info("LUN %s of device %s attached to node %s", lun_id, dev.get_id(), nid)
    for host_id in sorted(on_array):
        nid = node_of_host.get(host_id)
        if nid is None:
            # A host no node of the cluster is registered as: left alone
            # (release_node_luns detaches a deleted node's host).
            logger.warning("LUN %s of device %s is presented to unknown host %s; left alone",
                           lun_id, dev.get_id(), host_id)
            continue
        if nid in want or nid == serving:
            continue
        plugin.detach(lun_id, host_id)
        report["detached"].append((dev.get_id(), nid))
        logger.info("LUN %s of device %s detached from node %s", lun_id, dev.get_id(), nid)

    paths = {}
    for nid in want:
        if nid == owner.get_id() or nid in (dev.node_paths or {}):
            continue
        path = _lun_path_on(nodes_by_id[nid], dev, path_timeout)
        if path:
            paths[nid] = {"path": path, "bdev": ""}
        else:
            report["errors"][dev.get_id()] = f"LUN not visible on node {nid} yet"
    attached = sorted(want)
    device_id = dev.get_id()

    def _mut(n):
        for d in n.nvme_devices or []:
            if d.get_id() != device_id:
                continue
            d.array_attached_node_ids = attached
            np = dict(d.node_paths or {})
            # A node that lost the role loses its path; the serving node's
            # entry (written by the takeover) is never dropped here.
            for nid in list(np):
                if nid not in want and nid != d.serving_node() and nid != n.get_id():
                    np.pop(nid)
            for nid, p in paths.items():
                np.setdefault(nid, p)
            d.node_paths = np
            return True
        return False
    db.atomic_update(owner, _mut)


_reconcile_threads: dict = {}
_reconcile_lock = threading.Lock()


def reconcile_in_background(cluster) -> bool:
    """Start ``reconcile_lun_attachments`` for the cluster in a daemon
    thread unless one is running (the periodic hook of the health-check
    service). Returns whether one was started."""
    if not array_configured(cluster):
        return False
    cid = cluster.get_id()
    with _reconcile_lock:
        t = _reconcile_threads.get(cid)
        if t is not None and t.is_alive():
            return False
        t = threading.Thread(target=_safe_reconcile, args=(cid,), daemon=True)
        _reconcile_threads[cid] = t
        t.start()
    return True


def _safe_reconcile(cluster_id):
    try:
        from simplyblock_core.models.lock import DbLock
        with DbLock(f"array_luns/{cluster_id}", timeout=0):
            reconcile_lun_attachments(cluster_id)
    except Exception as e:                              # noqa: BLE001 - a background pass
        logger.warning("LUN attachment reconcile of cluster %s: %s", cluster_id, e)


def reconcile_after_role_change(cluster_id) -> dict:
    """The hook after LVS roles were assigned (activation, expansion): one
    reconcile pass, synchronous, under the cluster's array lock. Never
    raises; problems are in the report and an event."""
    try:
        cluster = _db().get_cluster_by_id(cluster_id)
        if not array_configured(cluster):
            return {"attached": [], "detached": [], "errors": {}}
        from simplyblock_core.models.lock import DbLock
        with DbLock(f"array_luns/{cluster_id}"):
            return reconcile_lun_attachments(cluster_id)
    except Exception as e:                              # noqa: BLE001
        logger.error("LUN attachments of cluster %s not reconciled: %s", cluster_id, e)
        return {"attached": [], "detached": [], "errors": {"*": str(e)}}


# ------------------------------------------------------------------ growth

def grow_luns(cluster_id, factor=None, visible_timeout=LUN_GROW_VISIBLE_TIMEOUT_SEC, size=None) -> dict:
    """Grow every array-created data LUN of the cluster at the array, then
    wait until every ONLINE node the LUN is presented to sees the new size
    (agent rescan; dm-multipath maps are resized too).

    Either by ``factor`` (> 1, relative to each LUN's size now), or to
    ``size`` bytes: a LUN already that large is left alone, so a growth to a
    size can be repeated after a failure without growing any LUN twice, and
    the cluster's LUN size for nodes added later (``Cluster.array_lun_size``)
    becomes ``size`` once every LUN has it.

    Returns ``{"luns": {device_id: {"lun_id", "old_size", "new_size"}},
    "errors": {device_id: msg}}``. The device growth itself (alceml, weights)
    is the regular grow-devices path that follows.

    Raises:
        ValueError: not an array cluster, a bad factor or size, or both or
            neither given.
    """
    if (factor is None) == (size is None):
        raise ValueError("give a growth factor or a LUN size, not both")
    if size is not None:
        try:
            size = int(size)
        except (TypeError, ValueError) as e:
            raise ValueError("the LUN size must be a number of bytes") from e
        if size <= 0:
            raise ValueError("the LUN size must be positive")
    else:
        try:
            factor = float(factor)
        except (TypeError, ValueError) as e:
            raise ValueError("the growth factor must be a number") from e
        if not math.isfinite(factor) or factor <= 1.0:
            raise ValueError("the growth factor must be greater than 1")
    db = _db()
    cluster = db.get_cluster_by_id(cluster_id)
    if not array_configured(cluster):
        raise ValueError("LUN growth through the array needs a multi-attach cluster with an array")
    try:
        plugin = array.plugin_for_cluster(cluster)
    except array.ArrayError as e:
        raise ValueError(str(e)) from e
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    nodes_by_id = {n.get_id(): n for n in nodes}
    out: dict = {"luns": {}, "errors": {}}
    for owner, dev in _managed_devices(nodes):
        if owner.status == _STATUS_REMOVED or dev.status in ("removed", "failed", "failed_and_migrated"):
            continue
        did = dev.get_id()
        try:
            old = int(plugin.get(dev.array_lun_id).get("size") or 0) or int(dev.size)
            target = max(old, size) if size is not None else math.ceil(old * factor)
            if target > old:
                plugin.resize(dev.array_lun_id, target)
            new = int(plugin.get(dev.array_lun_id).get("size") or target)
            out["luns"][did] = {"lun_id": dev.array_lun_id, "old_size": old, "new_size": new}
        except Exception as e:                          # noqa: BLE001
            out["errors"][did] = f"array resize of LUN {dev.array_lun_id} failed: {e}"
            continue
        err = _wait_visible(dev, nodes_by_id, new, visible_timeout)
        if err:
            out["errors"][did] = err
    if size is not None and not out["errors"] and size > int(cluster.array_lun_size or 0):
        def _set(fresh):
            fresh.array_lun_size = max(int(fresh.array_lun_size or 0), size)
        db.atomic_update(cluster, _set)
    return out


def _wait_visible(dev, nodes_by_id, size, timeout) -> str:
    """Rescan the LUN on every ONLINE node it has a path on until each sees
    ``size`` bytes (EBS rounds to GiB, so at least ``size``). Returns "" or
    what is still missing."""
    pending = {nid: p.get("path") for nid, p in (dev.node_paths or {}).items()
               if p.get("path") and nid in nodes_by_id
               and nodes_by_id[nid].status == _STATUS_ONLINE}
    deadline = time.monotonic() + timeout
    last = {}
    while pending:
        for nid, path in list(pending.items()):
            try:
                res, err = nodes_by_id[nid].client(timeout=60, retry=1).rescan_block_device(path)
            except Exception as e:                      # noqa: BLE001
                res, err = None, str(e)
            seen = int((res or {}).get("size") or 0)
            last[nid] = err or f"sees {seen} bytes"
            if seen >= size:
                pending.pop(nid)
        if not pending:
            break
        if time.monotonic() >= deadline:
            return "new size not visible on " + ", ".join(f"{n} ({last.get(n)})" for n in sorted(pending))
        _sleep(POLL_SEC)
    return ""


# -------------------------------------------------------- removal / delete

def release_node_luns(node) -> None:
    """A storage node is being deleted: detach its own array-created LUNs
    from every host, and detach other nodes' LUNs from its host unless it
    serves them. Nothing is deleted (only a cluster delete deletes LUNs).
    Failures are logged and reported as an event, never raised."""
    try:
        db = _db()
        cluster = db.get_cluster_by_id(node.cluster_id)
        if not array_configured(cluster):
            return
        nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
        work: list[tuple] = []
        node_host = getattr(node, "array_host_id", "") or ""
        for owner, dev in _managed_devices(nodes):
            if owner.get_id() == node.get_id():
                work.append((dev.array_lun_id, None))
            elif node_host and dev.serving_node() != node.get_id():
                work.append((dev.array_lun_id, node_host))
        if getattr(node, "array_journal_lun_id", ""):
            work.append((node.array_journal_lun_id, None))
        if not work:
            return
        plugin = array.plugin_for_cluster(cluster)
    except Exception as e:                              # noqa: BLE001
        logger.error("LUNs of node %s not released: %s", node.get_id(), e)
        _event(node.cluster_id, f"LUNs of node {node.get_id()} not released at the array: {e}", error=True)
        return
    errors = []
    for lun_id, host in work:
        try:
            hosts = [host] if host else list(plugin.get(lun_id).get("hosts") or [])
            for h in hosts:
                # The node is gone; its host may not cooperate.
                plugin.detach(lun_id, h, force=True)
        except Exception as e:                          # noqa: BLE001
            errors.append(f"{lun_id}: {e}")
    if errors:
        logger.error("LUNs of node %s not fully released: %s", node.get_id(), errors)
        _event(node.cluster_id, f"LUNs of node {node.get_id()} not fully released: {errors}", error=True)


def delete_cluster_luns(cluster, force=False) -> None:
    """A cluster is being deleted: delete every LUN its plugin created
    (detached from every host first) and the array's KMS secret.

    Raises:
        ValueError: a LUN could not be deleted (unless ``force``, which logs
            and goes on: the LUNs left are listed in the log).
    """
    if not array_configured(cluster):
        return
    errors = []
    # The LUNs recorded on the cluster's devices, plus whatever the plugin
    # lists (SAN plugins cannot list: their recorded LUNs are deleted).
    nodes = _db().get_storage_nodes_by_cluster_id(cluster.get_id())
    recorded = [d.array_lun_id for _, d in _managed_devices(nodes)]
    recorded += [n.array_journal_lun_id for n in nodes if getattr(n, "array_journal_lun_id", "")]
    luns: list = []
    try:
        plugin = array.plugin_for_cluster(cluster)
    except Exception as e:                              # noqa: BLE001
        plugin = None
        errors.append(f"array: {e}")
    if plugin is not None:
        try:
            luns = list(plugin.list_luns())
        except NotImplementedError:
            logger.info("Cluster %s: the array plugin cannot list LUNs; deleting the %d recorded",
                        cluster.get_id(), len(recorded))
        except Exception as e:                          # noqa: BLE001
            errors.append(f"array: {e}")
        listed = {lun["id"] for lun in luns}
        luns += [{"id": lun_id} for lun_id in dict.fromkeys(recorded) if lun_id not in listed]
    for lun in luns:
        try:
            plugin.delete_lun(lun["id"])                # type: ignore[union-attr]
            logger.info("Cluster %s: LUN %s deleted", cluster.get_id(), lun["id"])
        except Exception as e:                          # noqa: BLE001
            errors.append(f"{lun['id']}: {e}")
    if errors and not force:
        raise ValueError(f"array LUNs of the cluster could not be deleted: {'; '.join(errors)}")
    if errors:
        logger.error("Cluster %s deleted with array LUNs left behind: %s", cluster.get_id(), errors)
    if cluster.array_id and cluster.array_has_credentials and not cluster.array_credential_ref:
        from simplyblock_core import kms
        try:
            with kms.create_kms_connection(cluster) as conn:
                conn.delete_secret(kms.array_secret_path(cluster.get_id(), cluster.array_id))
        except Exception as e:                          # noqa: BLE001
            if not force:
                raise ValueError(f"the array credentials could not be deleted from the KMS: {e}") from e
            logger.error("Array credentials of cluster %s left in the KMS: %s", cluster.get_id(), e)
