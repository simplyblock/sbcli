"""Speak API v2 while keeping the v1 call contract the suite is written against.

v1 is being retired, so every HTTP call the harness makes now goes to
``/api/v2``. The two APIs are not the same shape, and the difference is not
just the URL:

===========================  ==================================================
v1                           v2
===========================  ==================================================
Flask, flat blueprints       FastAPI, nested under ``/clusters/{cluster_id}``
``/lvol/{id}``               ``/clusters/{c}/storage-pools/{p}/volumes/{id}``
``{"status": …, "results"}`` the bare object or list
200 + ``status: false``      a real status code (404 / 409 / 422)
``uuid``, ``lvol_name``      ``id``, ``name``
138 fields on a node         27 -- state only, no configuration echo
===========================  ==================================================

Rewriting ~1,900 call sites across 135 files to match is not a migration, it
is a rewrite of the suite, and it cannot be validated without a parity run.
So this module moves the *transport* to v2 and leaves the *contract* alone:
:class:`V2Router` takes a v1-style verb and path, performs however many v2
calls that needs, and hands back exactly the payload v1 produced -- envelope,
single-object-in-a-list quirks, v1 field names and all.

Three consequences worth knowing before reading on:

* **A volume is only addressable through its pool** (``_lookup_volume``
  checks ``volume.pool_uuid == pool.get_id()``), and ids are UUIDs -- names
  are not accepted anywhere. So the router keeps an id -> pool index and
  rebuilds it on a miss. There is no cluster-wide volume list in v2, so
  ``GET /lvol`` fans out across pools.
* **Field names are added, never replaced.** A payload leaves here carrying
  both ``id`` and ``uuid``, both ``name`` and ``lvol_name``. Code reading the
  v2 name keeps working, code reading the v1 name keeps working, and nothing
  depends on us having guessed which one a given call site used.
* **v2 reports node state, not node configuration.** ``spdk_image``,
  ``max_prov``, ``jm_percent`` and friends are inputs to *add*, not fields on
  ``StorageNodeDTO``, so they cannot be read back. :data:`_NODE_CONFIG_ECHO`
  supplies the v2 defaults and says so out loud, once. The three add-node
  tests that copy a sampled node's configuration will add nodes with cluster
  defaults instead; that is inherent to v2 and is the one thing here that
  needs a product decision rather than harness code.

Mirrored product constants carry a comment naming their upstream definition,
per ``e2e/AGENTS.md``: nothing in this tree may import ``simplyblock_web``,
because the harness and the cluster run independently selected versions.
"""

import re
import threading

from logger_config import setup_logger


class ExpectedApiError(Exception):
    """A refusal the caller asked for via ``expected_error_code``.

    v1 answered a refusal with HTTP 200 and ``status: false``, so a caller
    expecting one simply got a body back. v2 answers with a real status code,
    which has to travel up past the route handler without being mistaken for
    a successful call -- a handler that returned its usual ``results: True``
    would turn an expected refusal into a silent pass. ``SbcliUtils`` catches
    this at the boundary and returns None, which is what v1's helpers
    returned in the same situation.
    """

    def __init__(self, status_code, message=""):
        super().__init__(f"expected refusal: {status_code} {message}".strip())
        self.status_code = status_code


class V2TranslationError(Exception):
    """A v1 call this router cannot express against v2.

    Raised rather than guessed at: a translation that silently drops a
    parameter produces a test that passes while asserting the wrong thing,
    which is worse than a test that fails.
    """


#: Node configuration fields v1 served and ``StorageNodeDTO`` does not.
#:
#: Every one of these is an *input* to ``POST /clusters/{c}/storage-nodes``
#: (``StorageNodeParams``) and not a field on the node record v2 returns, so
#: there is nothing to read them back from. The values here are that request
#: model's own defaults, mirrored -- they must move together with
#: ``simplyblock_web/api/v2/cluster/storage_node/__init__.py``.
#:
#: Only three tests read these (add_node_fio_run, test_backup_node_add,
#: data_migration_ha_fio) and all three read them off one sampled node purely
#: to pass them straight back into add_storage_node. Under v2 that round trip
#: no longer carries anything: the fields v2 still accepts are forwarded, the
#: rest are not parameters any more.
_NODE_CONFIG_ECHO = {
    "spdk_image": "",
    "spdk_debug": False,
    "spdk_cpu_mask": "",
    "num_partitions_per_dev": 1,
    "enable_test_device": False,
    "enable_ha_jm": True,
    "iobuf_small_pool_count": 0,
    "iobuf_large_pool_count": 0,
    "jm_percent": 3,
    "namespace": "default",
    "id_device_by_nqn": False,
    # Dropped outright by v2: neither reported nor accepted.
    "max_prov": 0,
    "number_of_distribs": 0,
    "cloud_instance_id": "",
    "lvstore": None,
    "jm_bdev": None,
}

#: v1 name -> v2 name, per resource. Applied additively (see module docstring).
_ALIASES = {
    "cluster": {
        "uuid": "id",
        "cluster_name": "name",
        "blk_size": "block_size",
        "tls": "tls_enabled",
        "cap_crit": "utilization_critical",
        "cap_warn": "utilization_warning",
        "prov_cap_crit": "provisioned_capacity_critical",
        "prov_cap_warn": "provisioned_capacity_warning",
        "strict_node_anti_affinity": "anti_affinity",
        "enable_node_affinity": "node_affinity",
        # Deliberately NOT aliased: v1's ha_type is the string "ha" or
        # "single"; ClusterDTO.ha is a bool. Handing a bool to something
        # comparing against "ha" would be false every time and look like a
        # single-node cluster. Nothing in the suite reads it today, and if
        # something starts to it should read `ha` and mean a bool.
    },
    "node": {
        "uuid": "id",
        "max_lvol": "lvols_max",
        "max_snap": "snapshots_max",
        "number_of_devices": "device_count",
        "cpu": "cpu_total_count",
        "hugepages": "hugepage_memory",
    },
    "pool": {
        "uuid": "id",
        "pool_name": "name",
        "pool_max": "max_size",
        "lvol_max_size": "volume_max_size",
    },
    "volume": {
        "uuid": "id",
        "lvol_name": "name",
        "cloned_from_snap": "cloned_from",
        "node_id": "storage_node_id",
        "subsys_port": "port",
    },
    "snapshot": {
        "uuid": "id",
        "snap_name": "name",
        "snapshot_name": "name",
    },
    "device": {
        "uuid": "id",
        "node_id": "storage_node_id",
        "model_id": "model",
        "device_name": "device_path",
    },
    "task": {
        "uuid": "id",
        # v1's JobSchedule record called it node_id; TaskDTO renamed it.
        # wait_migration_tasks_complete filters on function_name and status,
        # but callers that group tasks by node read this one.
        "node_id": "storage_node_id",
    },
    "mgmt_node": {
        "uuid": "id",
        "hostname": "hostname",
    },
}

_UUID = r"[0-9a-fA-F-]{8,}"


def _alias(record, kind):
    """Return *record* with its v1 field names added alongside the v2 ones."""
    if not isinstance(record, dict):
        return record
    out = dict(record)
    for v1_name, v2_name in _ALIASES.get(kind, {}).items():
        if v1_name not in out and v2_name in out:
            out[v1_name] = out[v2_name]
    return out


def _alias_all(records, kind):
    if isinstance(records, list):
        return [_alias(r, kind) for r in records]
    return _alias(records, kind)


def _envelope(results):
    """The v1 response wrapper.

    Mirrors ``simplyblock_web/utils.get_response``: ``results`` is the
    payload verbatim, never wrapped in a list by the wrapper itself. Where v1
    routes happened to return a one-element list (node detail, lvol detail,
    pool detail) the caller here passes that list in, because the suite
    indexes ``[0]`` into it.
    """
    return {"status": True, "results": results}


class V2Router:
    """Translate one v1-style request into however many v2 calls it takes.

    *call* is the raw v2 transport, supplied by :class:`SbcliUtils` so that
    retries, the API-recovery wait and ``expected_error_code`` handling all
    stay in one place and are not duplicated here. Its signature is::

        call(verb, path, body=None, query=None, **kwargs) -> parsed json | None

    where *path* is already relative to ``/api/v2``.
    """

    def __init__(self, cluster_id, call, logger=None):
        self.cluster_id = cluster_id
        self._call = call
        self.logger = logger or setup_logger(__name__)
        # Reentrant on purpose: the index helpers below both take this lock
        # and call each other (caching a new snapshot's pool needs the
        # volume's pool, which locks again). A plain Lock deadlocks the first
        # time a snapshot is created -- caught by
        # tests/unit/test_sbcli_utils_v2_transport.py.
        self._lock = threading.RLock()
        #: pool name -> pool id, and the reverse, for resolving create bodies.
        self._pool_ids = {}
        #: volume id -> pool id. Rebuilt on a miss; see module docstring.
        self._volume_pool = {}
        #: snapshot id -> pool id.
        self._snapshot_pool = {}
        #: device id -> node id.
        self._device_node = {}
        #: Fields we have already warned about, so a soak logs each once.
        self._warned = set()

    # ── context resolution ───────────────────────────────────────────────

    def _c(self, cluster_id=None):
        return cluster_id or self.cluster_id

    def _pools(self, cluster_id=None, refresh=False):
        """Every pool of the cluster, as v2 returns them."""
        cid = self._c(cluster_id)
        pools = self._call("GET", f"/clusters/{cid}/storage-pools/") or []
        with self._lock:
            for pool in pools:
                if pool.get("name") and pool.get("id"):
                    self._pool_ids[pool["name"]] = pool["id"]
        return pools

    def _pool_id_for_name(self, name, cluster_id=None):
        with self._lock:
            known = self._pool_ids.get(name)
        if known:
            return known
        for pool in self._pools(cluster_id, refresh=True):
            if pool.get("name") == name:
                return pool.get("id")
        raise V2TranslationError(
            f"no pool named {name!r} in cluster {self._c(cluster_id)}; v2 "
            f"addresses pools by id only, so the name has to resolve here"
        )

    def _index_volumes(self, cluster_id=None):
        """Rebuild the volume -> pool index and return every volume.

        One call per pool. There is no cluster-wide volume list in v2, so a
        full listing costs ``len(pools)`` requests; the suite's pools are few
        and this is the same cost the control plane pays internally.
        """
        cid = self._c(cluster_id)
        out = []
        for pool in self._pools(cid):
            pool_id = pool.get("id")
            if not pool_id:
                continue
            volumes = self._call(
                "GET", f"/clusters/{cid}/storage-pools/{pool_id}/volumes/") or []
            for volume in volumes:
                volume = dict(volume)
                # v1 carried the pool on the lvol record; keep that true.
                volume.setdefault("pool_uuid", pool_id)
                volume.setdefault("pool_name", pool.get("name"))
                if volume.get("id"):
                    with self._lock:
                        self._volume_pool[volume["id"]] = pool_id
                out.append(volume)
        return out

    def _pool_of_volume(self, volume_id, cluster_id=None):
        with self._lock:
            known = self._volume_pool.get(volume_id)
        if known:
            return known
        self._index_volumes(cluster_id)
        with self._lock:
            known = self._volume_pool.get(volume_id)
        if known:
            return known
        raise V2TranslationError(
            f"volume {volume_id} is in no pool of cluster "
            f"{self._c(cluster_id)}; v2 cannot address a volume without its "
            f"pool"
        )

    def _index_snapshots(self, cluster_id=None):
        cid = self._c(cluster_id)
        out = []
        for pool in self._pools(cid):
            pool_id = pool.get("id")
            if not pool_id:
                continue
            snaps = self._call(
                "GET", f"/clusters/{cid}/storage-pools/{pool_id}/snapshots/") or []
            for snap in snaps:
                snap = dict(snap)
                snap.setdefault("pool_uuid", pool_id)
                snap.setdefault("pool_name", pool.get("name"))
                # v1 put the source lvol's id flat on the record; v2 nests the
                # whole volume under "lvol".
                lvol = snap.get("lvol")
                if isinstance(lvol, dict):
                    snap.setdefault("lvol_id", lvol.get("id"))
                    snap.setdefault("lvol_name", lvol.get("name"))
                if snap.get("id"):
                    with self._lock:
                        self._snapshot_pool[snap["id"]] = pool_id
                out.append(snap)
        return out

    def _pool_of_snapshot(self, snapshot_id, cluster_id=None):
        with self._lock:
            known = self._snapshot_pool.get(snapshot_id)
        if known:
            return known
        self._index_snapshots(cluster_id)
        with self._lock:
            known = self._snapshot_pool.get(snapshot_id)
        if known:
            return known
        raise V2TranslationError(
            f"snapshot {snapshot_id} is in no pool of cluster "
            f"{self._c(cluster_id)}"
        )

    def _nodes(self, cluster_id=None):
        cid = self._c(cluster_id)
        return self._call("GET", f"/clusters/{cid}/storage-nodes/") or []

    def _node_of_device(self, device_id, cluster_id=None):
        with self._lock:
            known = self._device_node.get(device_id)
        if known:
            return known
        cid = self._c(cluster_id)
        for node in self._nodes(cid):
            node_id = node.get("id")
            if not node_id:
                continue
            devices = self._call(
                "GET", f"/clusters/{cid}/storage-nodes/{node_id}/devices/") or []
            for device in devices:
                if device.get("id"):
                    with self._lock:
                        self._device_node[device["id"]] = node_id
        with self._lock:
            known = self._device_node.get(device_id)
        if known:
            return known
        raise V2TranslationError(
            f"device {device_id} is on no node of cluster {self._c(cluster_id)}"
        )

    # ── node payload completion ──────────────────────────────────────────

    def _warn_once(self, key, message):
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        self.logger.warning(message)

    def _node_record(self, node, cluster_id=None, with_nics=False):
        """A v2 node payload with v1's field names and shape restored."""
        if not isinstance(node, dict):
            # Nothing to complete. A detail call can legitimately come back
            # empty (a 204, or a refusal the caller expected), and crashing
            # here would turn that into a TypeError several frames away from
            # the call that caused it.
            return node
        record = _alias(node, "node")
        missing = [f for f in _NODE_CONFIG_ECHO if f not in record]
        if missing:
            self._warn_once(
                "node_config_echo",
                "[api-v2] StorageNodeDTO reports node state only, so these v1 "
                "fields are supplied as v2 defaults rather than read back: "
                + ", ".join(sorted(missing))
                + ". A node added from a sampled node's configuration will "
                  "take the control plane's defaults instead. See "
                  "_NODE_CONFIG_ECHO in e2e/utils/api_v2.py.",
            )
        for field, default in _NODE_CONFIG_ECHO.items():
            record.setdefault(field, default)

        if with_nics and record.get("id"):
            record["data_nics"] = self._data_nics(record["id"], cluster_id)
        else:
            record.setdefault("data_nics", [])
        return record

    def _data_nics(self, node_id, cluster_id=None):
        """v1's ``data_nics``, rebuilt from ``GET …/storage-nodes/{id}/nics``.

        That route returns display-cased keys (``"Device name"``,
        ``"Address"``) because it feeds a table; the suite reads ``if_name``
        and ``ip4_address``, which is what the underlying record calls them.
        Both spellings go out, so either reader works.
        """
        cid = self._c(cluster_id)
        try:
            nics = self._call(
                "GET", f"/clusters/{cid}/storage-nodes/{node_id}/nics") or []
        except Exception as exc:                      # noqa: BLE001
            self.logger.warning(
                f"[api-v2] could not read nics for node {node_id}: {exc}")
            return []
        out = []
        for nic in nics:
            if not isinstance(nic, dict):
                continue
            out.append({
                "uuid": nic.get("ID"),
                "if_name": nic.get("Device name"),
                "ip4_address": nic.get("Address"),
                "trtype": nic.get("Net type"),
                "status": nic.get("Status"),
                # the v2 spellings, kept so nothing has to know which it got
                **nic,
            })
        return out

    def _mark_secondaries(self, nodes):
        """Restore ``is_secondary_node``, which ``StorageNodeDTO`` dropped.

        v1 carried a flag on the record. v2 carries the relationship instead:
        a node named as some other node's ``secondary_node_id`` is that
        node's secondary. Derived over the whole list rather than per record,
        because a single node's payload cannot answer the question.
        """
        secondaries = {
            node.get("secondary_node_id")
            for node in nodes
            if node.get("secondary_node_id")
        }
        for node in nodes:
            node.setdefault("is_secondary_node", node.get("id") in secondaries)
        return nodes

    # ── dispatch ─────────────────────────────────────────────────────────

    def dispatch(self, verb, path, body=None, **kwargs):
        """Serve a v1-style request out of v2, in v1's response shape."""
        verb = verb.upper()
        raw_path = path or "/"
        # A path that is already v2 (the parity audit and SbcliUtilsV2 build
        # them directly) is passed through untouched -- including its
        # response, which such a caller expects raw.
        if raw_path.lstrip("/").startswith(("clusters", "management-nodes",
                                            "_meta", "metrics")):
            return self._call(verb, "/" + raw_path.lstrip("/"), body=body,
                              **kwargs)

        path, _, query = raw_path.partition("?")
        path = "/" + path.strip("/")
        query = dict(
            part.split("=", 1) if "=" in part else (part, "")
            for part in query.split("&") if part
        )

        for pattern, handler in self._routes():
            match = re.fullmatch(pattern, path)
            if match:
                return handler(match, verb, body, query, kwargs)

        raise V2TranslationError(
            f"no v2 route for {verb} {raw_path}. Every endpoint the suite "
            f"used under v1 is mapped in V2Router._routes(); an unmapped one "
            f"is either new or was never migrated."
        )

    def _routes(self):
        return [
            # ── cluster ──────────────────────────────────────────────────
            (r"/cluster", self._cluster_collection),
            (r"/clusters", self._cluster_collection),
            (rf"/cluster/status/({_UUID})", self._cluster_status),
            (rf"/cluster/capacity/({_UUID})", self._cluster_capacity),
            (rf"/cluster/capacity/({_UUID})/history/(.+)",
             self._cluster_capacity),
            (rf"/cluster/iostats/({_UUID})", self._cluster_iostats),
            (rf"/cluster/iostats/({_UUID})/history/(.+)",
             self._cluster_iostats),
            (rf"/cluster/get-logs/({_UUID})", self._cluster_logs),
            (rf"/cluster/get-tasks/({_UUID})", self._cluster_tasks),
            (rf"/cluster/activate/({_UUID})", self._cluster_activate),
            (rf"/cluster/({_UUID})", self._cluster_instance),

            # ── storage node ─────────────────────────────────────────────
            (r"/storagenode", self._node_collection),
            (r"/storagenode/add", self._node_add),
            (r"/storagenode/restart", self._node_restart),
            (rf"/storagenode/capacity/({_UUID})", self._node_capacity),
            (rf"/storagenode/capacity/({_UUID})/history/(.+)",
             self._node_capacity),
            (rf"/storagenode/iostats/({_UUID})", self._node_iostats),
            (rf"/storagenode/suspend/({_UUID})", self._node_suspend),
            (rf"/storagenode/resume/({_UUID})", self._node_resume),
            (rf"/storagenode/shutdown/({_UUID})", self._node_shutdown),
            (rf"/storagenode/port/({_UUID})", self._node_nics),
            (rf"/storagenode/({_UUID})", self._node_instance),

            # ── device ───────────────────────────────────────────────────
            (rf"/device/list/({_UUID})", self._device_collection),
            (rf"/device/capacity/({_UUID})", self._device_capacity),
            (rf"/device/iostats/({_UUID})", self._device_iostats),
            (rf"/device/remove/({_UUID})", self._device_remove),
            (rf"/device/reset/({_UUID})", self._device_reset),
            (rf"/device/restart/({_UUID})", self._device_restart),
            (rf"/device/add/({_UUID})", self._device_add),
            (rf"/device/({_UUID})", self._device_instance),

            # ── pool ─────────────────────────────────────────────────────
            (r"/pool", self._pool_collection),
            (rf"/pool/iostats/({_UUID})", self._pool_iostats),
            (rf"/pool/({_UUID})/host/(.+)", self._pool_host_remove),
            (rf"/pool/({_UUID})/host", self._pool_host),
            (rf"/pool/({_UUID})", self._pool_instance),

            # ── lvol ─────────────────────────────────────────────────────
            (r"/lvol", self._volume_collection),
            (rf"/lvol/connect/({_UUID})", self._volume_connect),
            (rf"/lvol/resize/({_UUID})", self._volume_resize),
            (rf"/lvol/capacity/({_UUID})", self._volume_capacity),
            (rf"/lvol/iostats/({_UUID})", self._volume_iostats),
            (rf"/lvol/inflate/({_UUID})", self._volume_inflate),
            (rf"/lvol/({_UUID})", self._volume_instance),

            # ── snapshot ─────────────────────────────────────────────────
            (r"/snapshot", self._snapshot_collection),
            (r"/snapshot/clone", self._snapshot_clone),
            (rf"/snapshot/({_UUID})", self._snapshot_instance),

            # ── management node ──────────────────────────────────────────
            (r"/mgmtnode", self._mgmt_collection),
            (rf"/mgmtnode/({_UUID})", self._mgmt_instance),
        ]

    # ── cluster handlers ─────────────────────────────────────────────────

    def _cluster_collection(self, match, verb, body, query, kwargs):
        if verb == "GET":
            clusters = self._call("GET", "/clusters/", **kwargs) or []
            return _envelope(_alias_all(clusters, "cluster"))
        if verb == "POST":
            created = self._call("POST", "/clusters/", body=body,
                                 query={"response-format": "identifier"},
                                 **kwargs)
            return _envelope(created)
        raise V2TranslationError(f"{verb} /cluster has no v2 equivalent")

    def _cluster_instance(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        if verb == "GET":
            cluster = self._call("GET", f"/clusters/{cid}/", **kwargs)
            return _envelope(_alias(cluster, "cluster"))
        if verb == "PUT":
            self._call("PUT", f"/clusters/{cid}/", body=body, **kwargs)
            return _envelope(True)
        if verb == "DELETE":
            self._call("DELETE", f"/clusters/{cid}/", **kwargs)
            return _envelope(True)
        raise V2TranslationError(f"{verb} /cluster/{{id}} has no v2 equivalent")

    def _cluster_status(self, match, verb, body, query, kwargs):
        """v1's ``/cluster/status`` is a *device* table, not the cluster state.

        ``cluster_ops.get_cluster_status`` returns one row per device across
        every node, keyed ``UUID`` / ``Storage ID`` / ``Status`` / ``Health``.
        v2 has no equivalent route, so the table is rebuilt from the nodes and
        their devices -- same columns, same sort, so a caller reading
        ``row["Status"]`` is unaffected.
        """
        cid = match.group(1)
        rows = []
        for node in self._nodes(cid):
            node_id = node.get("id")
            if not node_id:
                continue
            devices = self._call(
                "GET", f"/clusters/{cid}/storage-nodes/{node_id}/devices/") or []
            for device in devices:
                rows.append({
                    "UUID": device.get("id"),
                    "Storage ID": device.get("cluster_device_order"),
                    "Physical label": device.get("physical_label"),
                    "Size": device.get("size"),
                    "Hostname": node.get("hostname"),
                    "Status": device.get("status"),
                    "IO Error": device.get("io_error"),
                    "Health": device.get("health_check"),
                })
        rows.sort(key=lambda row: (row["Storage ID"] is None,
                                   row["Storage ID"]))
        return _envelope(rows)

    def _cluster_capacity(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        q = dict(query)
        if match.lastindex and match.lastindex > 1:
            q["history"] = match.group(2)
        return _envelope(
            self._call("GET", f"/clusters/{cid}/capacity", query=q, **kwargs))

    def _cluster_iostats(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        q = dict(query)
        if match.lastindex and match.lastindex > 1:
            q["history"] = match.group(2)
        stats = self._call("GET", f"/clusters/{cid}/iostats", query=q, **kwargs)
        # v1 answered {"object_data": …, "stats": […]}; get_io_stats reads
        # results["stats"].
        if isinstance(stats, dict) and "stats" in stats:
            return _envelope(stats)
        return _envelope({"stats": stats or []})

    def _cluster_logs(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        q = {"limit": query.get("limit", "50")}
        # v1 read limit=0 as "no limit"; v2's limit is a real bound.
        if q["limit"] in ("0", ""):
            q["limit"] = "1000"
        return _envelope(
            self._call("GET", f"/clusters/{cid}/logs", query=q, **kwargs) or [])

    def _cluster_tasks(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        tasks = self._call("GET", f"/clusters/{cid}/tasks/", **kwargs) or []
        return _envelope(_alias_all(tasks, "task"))

    def _cluster_activate(self, match, verb, body, query, kwargs):
        cid = match.group(1)
        self._call("POST", f"/clusters/{cid}/activate", **kwargs)
        return _envelope(True)

    # ── storage node handlers ────────────────────────────────────────────

    def _node_collection(self, match, verb, body, query, kwargs):
        if verb != "GET":
            raise V2TranslationError(f"{verb} /storagenode has no v2 route")
        cid = self._c()
        nodes = [self._node_record(node, cid) for node in self._nodes(cid)]
        return _envelope(self._mark_secondaries(nodes))

    def _node_instance(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        if verb == "GET":
            node = self._call(
                "GET", f"/clusters/{cid}/storage-nodes/{node_id}/", **kwargs)
            if not isinstance(node, dict):
                return _envelope([])
            record = self._node_record(node, cid, with_nics=True)
            # is_secondary_node is a relationship, so it needs the full list.
            self._mark_secondaries(
                [record] + [dict(n) for n in self._nodes(cid)])
            # v1 returned node detail as a one-element list.
            return _envelope([record])
        if verb == "DELETE":
            self._call("DELETE", f"/clusters/{cid}/storage-nodes/{node_id}/",
                       query=dict(query), **kwargs)
            return _envelope(True)
        raise V2TranslationError(
            f"{verb} /storagenode/{{id}} has no v2 equivalent")

    def _node_add(self, match, verb, body, query, kwargs):
        """``POST /storagenode/add`` -> ``POST /clusters/{c}/storage-nodes``.

        v2 renamed three parameters, inverted one, and dropped five. The
        dropped ones are not sent, and the caller is told once -- silently
        forwarding them would be a 422, silently dropping them without a word
        would hide a behaviour change.
        """
        body = dict(body or {})
        cid = body.pop("cluster_id", None) or self._c()
        translated = {
            "node_address": body.pop("node_ip", None),
            "interface_name": body.pop("ifname", None),
            "max_snapshots": body.pop("max_snap", None),
            # v1 asked for the negative, v2 for the positive.
            "ha_jm": not body.pop("disable_ha_jm", False),
            "test_device": body.pop("enable_test_device", False),
            "spdk_image": body.pop("spdk_image", "") or "",
            "spdk_debug": body.pop("spdk_debug", False),
            "partitions": body.pop("partitions", 1),
            "jm_percent": body.pop("jm_percent", 3),
            "iobuf_small_pool_count": body.pop("iobuf_small_pool_count", 0),
            "iobuf_large_pool_count": body.pop("iobuf_large_pool_count", 0),
        }
        for field in ("namespace", "id_device_by_nqn", "cr_name",
                      "cr_namespace", "cr_plural", "ha_jm_count", "format_4k",
                      "spdk_proxy_image", "spdk_sys_mem", "failure_domain",
                      "site", "expand", "force_format", "data_nics"):
            if field in body:
                translated[field] = body.pop(field)
        if translated["max_snapshots"] is None:
            translated.pop("max_snapshots")

        dropped = {k: v for k, v in body.items() if v not in (None, 0, "")}
        if dropped:
            self._warn_once(
                "node_add_dropped",
                "[api-v2] StorageNodeParams does not accept these v1 add-node "
                "parameters, so they are not sent: "
                + ", ".join(sorted(dropped))
                + ". The control plane sizes them itself now.",
            )
        created = self._call(
            "POST", f"/clusters/{cid}/storage-nodes/", body=translated,
            query={"response-format": "identifier"}, **kwargs)
        return _envelope(created)

    def _node_restart(self, match, verb, body, query, kwargs):
        """v1 put the node id in the *body* of a collection-level PUT."""
        body = dict(body or {})
        node_id = body.pop("uuid", None) or body.pop("node_id", None)
        if not node_id:
            raise V2TranslationError(
                "PUT /storagenode/restart/ needs a node id in its body")
        cid = self._c()
        payload = {"force": bool(body.pop("force", False))}
        for field in ("reattach_volume", "node_address", "new_ssd_pcie"):
            if field in body:
                payload[field] = body.pop(field)
        self._call("POST",
                   f"/clusters/{cid}/storage-nodes/{node_id}/restart",
                   body=payload, **kwargs)
        return _envelope(True)

    def _node_capacity(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        q = dict(query)
        if match.lastindex and match.lastindex > 1:
            q["history"] = match.group(2)
        cid = self._c()
        return _envelope(self._call(
            "GET", f"/clusters/{cid}/storage-nodes/{node_id}/capacity",
            query=q, **kwargs))

    def _node_iostats(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        return _envelope(self._call(
            "GET", f"/clusters/{cid}/storage-nodes/{node_id}/iostats",
            query=dict(query), **kwargs))

    def _node_suspend(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        self._call("POST", f"/clusters/{cid}/storage-nodes/{node_id}/suspend",
                   query=dict(query), **kwargs)
        return _envelope(True)

    def _node_resume(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        self._call("POST", f"/clusters/{cid}/storage-nodes/{node_id}/resume",
                   **kwargs)
        return _envelope(True)

    def _node_shutdown(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        self._call("POST", f"/clusters/{cid}/storage-nodes/{node_id}/shutdown",
                   query=dict(query), **kwargs)
        return _envelope(True)

    def _node_nics(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        return _envelope(self._data_nics(node_id))

    # ── device handlers ──────────────────────────────────────────────────

    def _device_collection(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        cid = self._c()
        devices = self._call(
            "GET", f"/clusters/{cid}/storage-nodes/{node_id}/devices/",
            **kwargs) or []
        for device in devices:
            if device.get("id"):
                with self._lock:
                    self._device_node[device["id"]] = node_id
            device.setdefault("node_id", node_id)
            device.setdefault("jm_bdev", None)
        return _envelope(_alias_all(devices, "device"))

    def _device_path(self, device_id):
        node_id = self._node_of_device(device_id)
        return (f"/clusters/{self._c()}/storage-nodes/{node_id}"
                f"/devices/{device_id}")

    def _device_instance(self, match, verb, body, query, kwargs):
        device_id = match.group(1)
        if verb != "GET":
            raise V2TranslationError(
                f"{verb} /device/{{id}} has no v2 equivalent")
        device = self._call("GET", self._device_path(device_id) + "/", **kwargs)
        return _envelope([_alias(device, "device")] if device else [])

    def _device_capacity(self, match, verb, body, query, kwargs):
        return _envelope(self._call(
            "GET", self._device_path(match.group(1)) + "/capacity",
            query=dict(query), **kwargs))

    def _device_iostats(self, match, verb, body, query, kwargs):
        return _envelope(self._call(
            "GET", self._device_path(match.group(1)) + "/iostats",
            query=dict(query), **kwargs))

    def _device_remove(self, match, verb, body, query, kwargs):
        self._call("POST", self._device_path(match.group(1)) + "/remove",
                   query=dict(query), **kwargs)
        return _envelope(True)

    def _device_reset(self, match, verb, body, query, kwargs):
        self._call("POST", self._device_path(match.group(1)) + "/reset",
                   **kwargs)
        return _envelope(True)

    def _device_restart(self, match, verb, body, query, kwargs):
        self._call("POST", self._device_path(match.group(1)) + "/restart",
                   query=dict(query), **kwargs)
        return _envelope(True)

    def _device_add(self, match, verb, body, query, kwargs):
        self._call("POST", self._device_path(match.group(1)) + "/add", **kwargs)
        return _envelope(True)

    # ── pool handlers ────────────────────────────────────────────────────

    def _pool_collection(self, match, verb, body, query, kwargs):
        cid = self._c()
        if verb == "GET":
            return _envelope(_alias_all(self._pools(cid), "pool"))
        if verb == "POST":
            body = dict(body or {})
            cid = body.pop("cluster_id", None) or cid
            translated = {"name": body.pop("name", None)}
            # v1 sent the numeric limits as strings; v2's Unsigned rejects a
            # string that is not a number, and parse_size handles the rest.
            for v1_name, v2_name in (
                    ("pool_max", "pool_max"),
                    ("max_size", "pool_max"),
                    ("lvol_max_size", "volume_max_size"),
                    ("volume_max_size", "volume_max_size"),
                    ("max_rw_iops", "max_rw_iops"),
                    ("max_rw_mbytes", "max_rw_mbytes"),
                    ("max_r_mbytes", "max_r_mbytes"),
                    ("max_w_mbytes", "max_w_mbytes")):
                if v1_name in body:
                    translated[v2_name] = _as_int(body.pop(v1_name))
            for field in ("dhchap", "cr_name", "cr_namespace", "cr_plural"):
                if field in body:
                    translated[field] = body.pop(field)
            created = self._call(
                "POST", f"/clusters/{cid}/storage-pools/", body=translated,
                query={"response-format": "full"}, **kwargs)
            with self._lock:
                self._pool_ids.pop(translated.get("name"), None)
            return _envelope(_alias(created, "pool") if created else created)
        raise V2TranslationError(f"{verb} /pool has no v2 route")

    def _pool_instance(self, match, verb, body, query, kwargs):
        pool_id = match.group(1)
        cid = self._c()
        base = f"/clusters/{cid}/storage-pools/{pool_id}"
        if verb == "GET":
            pool = self._call("GET", base + "/", **kwargs)
            # v1 returned pool detail as a one-element list.
            return _envelope([_alias(pool, "pool")] if pool else [])
        if verb == "DELETE":
            # v1 required the pool secret in a header; v2 authorises by token
            # and the route takes no secret at all.
            self._call("DELETE", base + "/", **kwargs)
            with self._lock:
                self._pool_ids = {
                    name: pid for name, pid in self._pool_ids.items()
                    if pid != pool_id
                }
            return _envelope(True)
        if verb == "PUT":
            self._call("PUT", base + "/", body=dict(body or {}), **kwargs)
            return _envelope(True)
        raise V2TranslationError(f"{verb} /pool/{{id}} has no v2 equivalent")

    def _pool_iostats(self, match, verb, body, query, kwargs):
        pool_id = match.group(1)
        cid = self._c()
        return _envelope(self._call(
            "GET", f"/clusters/{cid}/storage-pools/{pool_id}/iostats",
            query=dict(query), **kwargs))

    def _pool_host(self, match, verb, body, query, kwargs):
        pool_id = match.group(1)
        cid = self._c()
        base = f"/clusters/{cid}/storage-pools/{pool_id}/host"
        if verb == "POST":
            self._call("POST", base, body=dict(body or {}), **kwargs)
            return _envelope(True)
        if verb == "DELETE":
            self._call("DELETE", base, body=dict(body or {}), **kwargs)
            return _envelope(True)
        raise V2TranslationError(f"{verb} /pool/{{id}}/host has no v2 route")

    def _pool_host_remove(self, match, verb, body, query, kwargs):
        """v1 took the NQN in the path; v2 takes it in the body."""
        pool_id, host_nqn = match.group(1), match.group(2)
        cid = self._c()
        self._call("DELETE",
                   f"/clusters/{cid}/storage-pools/{pool_id}/host",
                   body={"host_nqn": host_nqn}, **kwargs)
        return _envelope(True)

    # ── volume handlers ──────────────────────────────────────────────────

    def _volume_base(self, volume_id):
        pool_id = self._pool_of_volume(volume_id)
        return (f"/clusters/{self._c()}/storage-pools/{pool_id}"
                f"/volumes/{volume_id}")

    def _volume_collection(self, match, verb, body, query, kwargs):
        cid = self._c()
        if verb == "GET":
            return _envelope(_alias_all(self._index_volumes(cid), "volume"))
        if verb == "POST":
            body = dict(body or {})
            pool_ref = body.pop("pool", None) or body.pop("pool_name", None)
            pool_id = body.pop("pool_id", None)
            if not pool_id:
                if not pool_ref:
                    raise V2TranslationError(
                        "POST /lvol needs a pool: v2 creates a volume under "
                        "its pool's path")
                pool_id = (pool_ref if re.fullmatch(_UUID, str(pool_ref))
                           else self._pool_id_for_name(pool_ref, cid))
            translated = {
                "name": body.pop("name", None),
                "size": body.pop("size", None),
            }
            for field in ("max_rw_iops", "max_rw_mbytes", "max_r_mbytes",
                          "max_w_mbytes", "ndcs", "npcs"):
                if field in body:
                    translated[field] = _as_int(body.pop(field))
            for field in ("ha_type", "host_id", "priority_class", "namespaced",
                          "pvc_name", "allowed_hosts", "fabric",
                          "max_namespace_per_subsys", "do_replicate",
                          "replication_cluster_id", "replication_policy",
                          "consistency_group", "site"):
                if field in body:
                    translated[field] = body.pop(field)
            # v1 called it crypto, v2 calls it encrypt.
            if "crypto" in body:
                translated["encrypt"] = bool(body.pop("crypto"))
            if "encrypt" in body:
                translated["encrypt"] = bool(body.pop("encrypt"))
            # bs / chunk_bs are cluster-level in v2 and not accepted per
            # volume; sending them is a 422.
            dropped = [k for k in ("bs", "chunk_bs", "distr_bs",
                                   "distr_chunk_bs", "distr_vuid", "use_comp",
                                   "cluster_id") if k in body]
            for key in dropped:
                body.pop(key)
            if dropped:
                self._warn_once(
                    "volume_create_dropped",
                    "[api-v2] _CreateParams does not accept these v1 volume "
                    "parameters, so they are not sent: "
                    + ", ".join(sorted(dropped))
                    + ". Block size is a cluster property in v2.",
                )
            created = self._call(
                "POST", f"/clusters/{cid}/storage-pools/{pool_id}/volumes/",
                body={k: v for k, v in translated.items() if v is not None},
                query={"response-format": "identifier"}, **kwargs)
            if isinstance(created, str):
                with self._lock:
                    self._volume_pool[created] = pool_id
            return _envelope(created)
        raise V2TranslationError(f"{verb} /lvol has no v2 route")

    def _volume_instance(self, match, verb, body, query, kwargs):
        volume_id = match.group(1)
        if verb == "GET":
            pool_id = self._pool_of_volume(volume_id)
            volume = self._call(
                "GET",
                f"/clusters/{self._c()}/storage-pools/{pool_id}"
                f"/volumes/{volume_id}/", **kwargs)
            if isinstance(volume, dict):
                volume = dict(volume)
                volume.setdefault("pool_uuid", pool_id)
            # v1 returned lvol detail as a one-element list.
            return _envelope([_alias(volume, "volume")] if volume else [])
        if verb == "DELETE":
            base = self._volume_base(volume_id)
            self._call("DELETE", base + "/", **kwargs)
            with self._lock:
                self._volume_pool.pop(volume_id, None)
            return _envelope(True)
        if verb == "PUT":
            self._call("PUT", self._volume_base(volume_id) + "/",
                       body=dict(body or {}), **kwargs)
            return _envelope(True)
        raise V2TranslationError(f"{verb} /lvol/{{id}} has no v2 equivalent")

    def _volume_connect(self, match, verb, body, query, kwargs):
        volume_id = match.group(1)
        entries = self._call("GET", self._volume_base(volume_id) + "/connect",
                             query=dict(query), **kwargs) or []
        return _envelope(entries)

    def _volume_resize(self, match, verb, body, query, kwargs):
        """v1 had a dedicated resize route; v2 resizes through the update."""
        volume_id = match.group(1)
        size = (body or {}).get("size")
        if size is None:
            raise V2TranslationError("resize needs a size")
        self._call("PUT", self._volume_base(volume_id) + "/",
                   body={"size": size}, **kwargs)
        return _envelope(True)

    def _volume_capacity(self, match, verb, body, query, kwargs):
        return _envelope(self._call(
            "GET", self._volume_base(match.group(1)) + "/capacity",
            query=dict(query), **kwargs))

    def _volume_iostats(self, match, verb, body, query, kwargs):
        return _envelope(self._call(
            "GET", self._volume_base(match.group(1)) + "/iostats",
            query=dict(query), **kwargs))

    def _volume_inflate(self, match, verb, body, query, kwargs):
        self._call("POST", self._volume_base(match.group(1)) + "/inflate",
                   **kwargs)
        return _envelope(True)

    # ── snapshot handlers ────────────────────────────────────────────────

    def _snapshot_collection(self, match, verb, body, query, kwargs):
        cid = self._c()
        if verb == "GET":
            return _envelope(_alias_all(self._index_snapshots(cid), "snapshot"))
        if verb == "POST":
            body = dict(body or {})
            lvol_id = body.pop("lvol_id", None)
            name = body.pop("snapshot_name", None) or body.pop("name", None)
            if not lvol_id or not name:
                raise V2TranslationError(
                    "POST /snapshot needs lvol_id and snapshot_name")
            created = self._call(
                "POST", self._volume_base(lvol_id) + "/snapshots",
                body={"name": name},
                query={"response-format": "identifier"}, **kwargs)
            if isinstance(created, str):
                pool_id = self._pool_of_volume(lvol_id)
                with self._lock:
                    self._snapshot_pool[created] = pool_id
            return _envelope(created)
        raise V2TranslationError(f"{verb} /snapshot has no v2 route")

    def _snapshot_instance(self, match, verb, body, query, kwargs):
        snapshot_id = match.group(1)
        pool_id = self._pool_of_snapshot(snapshot_id)
        base = (f"/clusters/{self._c()}/storage-pools/{pool_id}"
                f"/snapshots/{snapshot_id}/")
        if verb == "GET":
            snapshot = self._call("GET", base, **kwargs)
            return _envelope(
                [_alias(snapshot, "snapshot")] if snapshot else [])
        if verb == "DELETE":
            self._call("DELETE", base, **kwargs)
            with self._lock:
                self._snapshot_pool.pop(snapshot_id, None)
            return _envelope(True)
        raise V2TranslationError(
            f"{verb} /snapshot/{{id}} has no v2 equivalent")

    def _snapshot_clone(self, match, verb, body, query, kwargs):
        """``POST /snapshot/clone`` -> a *volume* create with a clone body.

        v2 has no clone route under snapshots. Cloning a snapshot is
        ``POST …/storage-pools/{pool}/volumes/`` with a ``_CloneParams`` body
        (``name`` + ``snapshot_id``); the pool is the snapshot's own. The
        similarly named ``POST …/volumes/{id}/clone`` is a different feature
        -- cloning a live volume -- and is not this.
        """
        body = dict(body or {})
        snapshot_id = body.pop("snapshot_id", None)
        name = body.pop("clone_name", None) or body.pop("name", None)
        if not snapshot_id or not name:
            raise V2TranslationError(
                "POST /snapshot/clone needs snapshot_id and clone_name")
        pool_id = self._pool_of_snapshot(snapshot_id)
        translated = {"name": name, "snapshot_id": snapshot_id}
        for field in ("size", "pvc_name", "pvc_namespace",
                      "delete_snap_on_lvol_delete", "consistency_group"):
            if field in body:
                translated[field] = body.pop(field)
        created = self._call(
            "POST", f"/clusters/{self._c()}/storage-pools/{pool_id}/volumes/",
            body=translated, query={"response-format": "identifier"}, **kwargs)
        if isinstance(created, str):
            with self._lock:
                self._volume_pool[created] = pool_id
        return _envelope(created)

    # ── management node handlers ─────────────────────────────────────────

    def _mgmt_collection(self, match, verb, body, query, kwargs):
        if verb != "GET":
            raise V2TranslationError(f"{verb} /mgmtnode has no v2 route")
        # /management-nodes sits at the API root, so the cluster it belongs to
        # is a query parameter rather than a path segment.
        nodes = self._call("GET", "/management-nodes/",
                           query={"cluster_id": self._c()}, **kwargs) or []
        return _envelope(_alias_all(nodes, "mgmt_node"))

    def _mgmt_instance(self, match, verb, body, query, kwargs):
        node_id = match.group(1)
        node = self._call("GET", f"/management-nodes/{node_id}/",
                          query={"cluster_id": self._c()}, **kwargs)
        return _envelope([_alias(node, "mgmt_node")] if node else [])


def _as_int(value):
    """v1 sent numeric limits as strings; v2's ``Unsigned`` wants a number."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return value
