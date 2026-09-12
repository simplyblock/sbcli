# coding=utf-8
"""Active alerts: the small set of conditions an operator has to act on.

The cluster event log is a journal -- it records what happened, keeps it
forever, and never retracts anything. An alert is the opposite: a statement
that something is wrong *right now*, which disappears on its own when it
stops being true. Reading the journal and calling the result "alerts" gives
neither property, so this module derives alerts from two sources:

* **Current state** (node / cluster / device records) for everything whose
  truth is a state: offline, down, unreachable, hung in restart, degraded,
  suspended, device unavailable. These resolve by themselves -- the node comes
  back ONLINE and the alert is simply no longer produced. State also carries
  the operator's intent, which is what the suppression rules below need and
  what an event message cannot tell you.
* **Recent events** for faults that are instants rather than states: an
  unrecoverable IO error, a JM/JC failure. Nothing in the record set says
  "this happened"; only the journal does. These are windowed
  (``EVENT_WINDOW_SEC``) and resolve by ageing out.

Plus one metric-derived alert, the API's own request-latency histogram.

Suppression is the point of the whole exercise. Every one of these conditions
is also produced deliberately, many times a day, by ordinary operations: an
operator shuts a node down, so it is offline and the cluster is degraded; a
node restarts, so its peers are briefly down; an operator pulls a device, so
it is unavailable. An alert feed that fires on those is noise, and noise is
what stops people reading alerts at all. Each rule below therefore names the
deliberate cause it must stay quiet for, and the record field that proves it.

Everything here is pure apart from :func:`get_active_alerts`, which is the one
function that touches the DB: the rules take already-fetched records so they
can be unit-tested against hand-built states.
"""
from datetime import datetime, timezone
from typing import List, Optional

from simplyblock_core import constants, utils
from simplyblock_core.models.alert_state import AlertState
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

#: Cluster-event names for the two ends of an alert's life.
ALERT_RAISED = "ALERT_RAISED"
ALERT_RESOLVED = "ALERT_RESOLVED"


# --------------------------------------------------------------------------
# Alert kinds. Stable identifiers -- a consumer keys dedupe/routing on these,
# so treat them as API: add, never rename.
# --------------------------------------------------------------------------
NODE_OFFLINE = "node_offline"
NODE_RESTART_HUNG = "node_restart_hung"
NODE_UNAVAILABLE = "node_unavailable"
NODE_DOWN = "node_down"
CLUSTER_DEGRADED = "cluster_degraded"
CLUSTER_SUSPENDED = "cluster_suspended"
DEVICE_UNAVAILABLE = "device_unavailable"
NODE_JC_COMPRESSION_ERROR = "node_jc_compression_error"
API_SLOW = "api_slow"
NODE_IO_ERROR = "node_unrecoverable_io_error"
NODE_JC_ERROR = "node_unrecoverable_jc_error"
CLUSTER_CAPACITY_CRITICAL = "cluster_capacity_critical"
CLUSTER_PROV_CAPACITY_CRITICAL = "cluster_provisioned_capacity_critical"

SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"

STATUS_FIRING = "firing"
STATUS_RESOLVED = "resolved"

#: How long a resolved alert is kept as history, and how many are kept at
#: most. History is opt-in -- the default feed is what is wrong NOW -- so this
#: only bounds the record, which lives in one document per cluster and must
#: not grow without limit. The cap wins over the window: a cluster flapping an
#: alert every minute keeps a day of history, not a week of it.
HISTORY_RETENTION_SEC = 7 * 24 * 3600.0
HISTORY_MAX_ENTRIES = 200

#: A node in_restart for longer than this is not restarting, it is stuck.
#: A healthy restart is minutes of real work (lvstore recovery, peer
#: re-wiring), so this cannot be tight; 3 minutes is the operator's own
#: threshold for "go and look".
RESTART_HUNG_SEC = 180.0

#: A node must be DOWN for this long before it alerts. DOWN is self-healing in
#: the common case (a writer conflict resolves, a poll succeeds on the next
#: tick); alerting on the first observation would fire on every transient.
DOWN_GRACE_SEC = 60.0

#: How far back the event-derived rules look. An unrecoverable IO/JC error has
#: no "resolved" state to observe, so the alert stands for this long after the
#: last occurrence and then ages out.
EVENT_WINDOW_SEC = 3600.0

#: Mean API request latency above this is the "critically slow" alert. Matches
#: the API_request_latency_high Grafana rule so the two cannot disagree.
API_SLOW_MEAN_SEC = 15.0

#: Statuses that mean a peer is deliberately mid-operation. A node DOWN while
#: one of these is happening elsewhere in the cluster is the expected
#: short-term consequence of that operation, not an independent fault.
_PEER_TRANSITION_STATUSES = (
    StorageNode.STATUS_RESTARTING,
    StorageNode.STATUS_IN_SHUTDOWN,
    StorageNode.STATUS_IN_CREATION,
)


def _alert(kind, severity, message, cluster_id, node_id="", device_id="",
           since="", **details):
    return {
        # Stable and idempotent: the same condition on the same object yields
        # the same id on every poll, so a consumer can dedupe without state.
        "id": f"{kind}:{device_id or node_id or cluster_id}",
        "kind": kind,
        "severity": severity,
        "message": message,
        "cluster_id": cluster_id,
        "node_id": node_id,
        "device_id": device_id,
        "since": since,
        "details": details,
    }


def _age_seconds(iso_ts, now) -> Optional[float]:
    """Seconds since ``iso_ts``, or None when it is missing/unparseable."""
    if not iso_ts:
        return None
    try:
        return (now - datetime.fromisoformat(iso_ts)).total_seconds()
    except Exception:
        return None


def _node_label(node) -> str:
    """Prefer the hostname an operator recognises, fall back to the uuid."""
    return getattr(node, "hostname", "") or node.get_id()


# ==========================================================================
# Node rules
# ==========================================================================

def _node_alerts(cluster_id, nodes, now) -> List[dict]:
    alerts = []
    # A peer mid-transition anywhere in the cluster explains a DOWN elsewhere.
    # Computed once over all nodes rather than per node: the restarting node
    # is by definition a different node from the one that went DOWN.
    transitioning = {n.get_id() for n in nodes
                     if n.status in _PEER_TRANSITION_STATUSES}

    for node in nodes:
        label = _node_label(node)
        nid = node.get_id()

        if node.status == StorageNode.STATUS_OFFLINE:
            # auto_restart_disabled is set by `sn shutdown` (CLI/API, with or
            # without --force) and cleared the moment the node reaches ONLINE
            # again. So while a node sits OFFLINE it is the exact record of
            # "an operator stopped this on purpose", which is the one offline
            # that must not alert. A node the monitor shut down to recover a
            # suspended cluster keeps auto-restart enabled and DOES alert --
            # correctly, since nobody asked for it.
            if not getattr(node, "auto_restart_disabled", False):
                alerts.append(_alert(
                    NODE_OFFLINE, SEVERITY_CRITICAL,
                    f"node {label} offline",
                    cluster_id, node_id=nid, since=getattr(node, "updated_at", ""),
                    status=node.status))

        elif node.status == StorageNode.STATUS_RESTARTING:
            # updated_at is stamped by set_node_status on every transition, so
            # for a node still in_restart it is when the restart began.
            age = _age_seconds(getattr(node, "updated_at", ""), now)
            if age is not None and age >= RESTART_HUNG_SEC:
                alerts.append(_alert(
                    NODE_RESTART_HUNG, SEVERITY_CRITICAL,
                    f"node {label} hanging in restart",
                    cluster_id, node_id=nid, since=getattr(node, "updated_at", ""),
                    seconds_in_restart=int(age)))

        elif node.status == StorageNode.STATUS_UNREACHABLE:
            alerts.append(_alert(
                NODE_UNAVAILABLE, SEVERITY_CRITICAL,
                f"node {label} unavailable",
                cluster_id, node_id=nid, since=getattr(node, "updated_at", ""),
                status=node.status))

        elif node.status == StorageNode.STATUS_DOWN:
            # Two suppressions. A peer mid-restart/shutdown makes this DOWN
            # the expected short-term consequence of that operation; and a
            # DOWN younger than the grace window is usually self-healing.
            if transitioning - {nid}:
                continue
            age = _age_seconds(getattr(node, "down_since", ""), now)
            # Unparseable/absent down_since -> alert. Consistent with the
            # monitor's own _down_longer_than: never silently ignore a DOWN
            # whose entry time we cannot establish.
            if age is None or age >= DOWN_GRACE_SEC:
                alerts.append(_alert(
                    NODE_DOWN, SEVERITY_CRITICAL,
                    f"node {label} down",
                    cluster_id, node_id=nid,
                    since=getattr(node, "down_since", ""),
                    seconds_down=int(age) if age is not None else None))

    return alerts


# ==========================================================================
# Cluster rules
# ==========================================================================

def _cluster_alerts(cluster, nodes) -> List[dict]:
    alerts = []
    cid = cluster.get_id()

    if cluster.status == Cluster.STATUS_DEGRADED:
        # A cluster is degraded BECAUSE a node is missing. If that node is
        # missing because an operator stopped it, the degradation is the
        # intended consequence of a deliberate act and alerting on it just
        # duplicates the node alert we already suppressed.
        operator_stopped = any(
            getattr(n, "auto_restart_disabled", False)
            and n.status in (StorageNode.STATUS_OFFLINE,
                             StorageNode.STATUS_IN_SHUTDOWN)
            for n in nodes)
        if not operator_stopped:
            alerts.append(_alert(
                CLUSTER_DEGRADED, SEVERITY_CRITICAL,
                f"cluster {cluster.get_id()} degraded",
                cid, since=getattr(cluster, "updated_at", ""),
                status=cluster.status))

    elif cluster.status == Cluster.STATUS_SUSPENDED:
        # No suppression: a suspended cluster is not serving IO. Whatever the
        # cause, an operator needs to know now.
        alerts.append(_alert(
            CLUSTER_SUSPENDED, SEVERITY_CRITICAL,
            f"cluster {cluster.get_id()} suspended",
            cid, since=getattr(cluster, "updated_at", ""),
            status=cluster.status))

    return alerts


# ==========================================================================
# Device rules
# ==========================================================================

def _device_alerts(cluster_id, nodes) -> List[dict]:
    alerts = []
    for node in nodes:
        # ONLY on an ONLINE node. A device is reported unavailable for all
        # sorts of reasons that are about the NODE, not the device: a
        # graceful shutdown marks every one of them unavailable on purpose
        # (shutdown_storage_node, Loop 1), a restart takes them down with the
        # node, and a node that is offline or down cannot say anything
        # meaningful about its devices at all. In each of those the node
        # itself is the alert, and one node condition must not also become N
        # device alerts. On an ONLINE node the node is fine and the device is
        # not, which is the only case where the device is the fault.
        if node.status != StorageNode.STATUS_ONLINE:
            continue

        label = _node_label(node)
        for dev in (node.nvme_devices or []):
            # Removed counts: a device that vanished from an otherwise
            # healthy node is a failure or a physical pull, and either way
            # somebody has to look. The one removal that is not is the
            # operator's own, filtered next.
            if dev.status not in (NVMeDevice.STATUS_UNAVAILABLE,
                                  NVMeDevice.STATUS_FAILED,
                                  NVMeDevice.STATUS_REMOVED):
                continue
            # admin_removed is set only when the operator asked for the
            # device to go away (`sn remove-device`, API remove) and cleared
            # when it next comes ONLINE. It is the one removal that is not a
            # fault.
            if getattr(dev, "admin_removed", False):
                continue
            alerts.append(_alert(
                DEVICE_UNAVAILABLE, SEVERITY_CRITICAL,
                f"device {dev.get_id()} on node {label} unavailable: "
                f"potential device failure / removal",
                cluster_id, node_id=node.get_id(), device_id=dev.get_id(),
                device_status=dev.status,
                serial_number=getattr(dev, "serial_number", "")))
    return alerts


# ==========================================================================
# Event-derived rules
# ==========================================================================

def _is_recent(event, now_ms, window_sec) -> bool:
    date = getattr(event, "date", 0) or 0
    # EventObj.date is milliseconds on everything current; tolerate the
    # seconds-era rows the model's own get_date_string still handles.
    ms = date if date > 1e10 else date * 1000
    return (now_ms - ms) <= window_sec * 1000


def _event_alerts(cluster_id, events, now_ms) -> List[dict]:
    """Fault events that have no corresponding state to observe.

    Only the newest occurrence per (kind, node) becomes an alert: a distrib
    error storm is one fault reported many times, and one alert per report
    would be the event log again with a different name.
    """
    from simplyblock_core.controllers import events_controller as ec

    newest: dict = {}

    def _keep(kind, severity, message, node_id, event):
        key = (kind, node_id)
        prev = newest.get(key)
        if prev is None or (getattr(event, "date", 0) or 0) > (prev[1] or 0):
            newest[key] = (
                _alert(kind, severity, message, cluster_id, node_id=node_id,
                       since=event.get_date_string(),
                       event=getattr(event, "event", ""),
                       detail=getattr(event, "message", "")),
                getattr(event, "date", 0) or 0,
            )

    for event in events:
        if not _is_recent(event, now_ms, EVENT_WINDOW_SEC):
            continue
        level = getattr(event, "event_level", "")
        domain = getattr(event, "domain", "")
        node_id = getattr(event, "node_id", "") or ""

        if domain == ec.DOMAIN_JM and level in (EventObj.LEVEL_ERROR,
                                                EventObj.LEVEL_CRITICAL):
            # The JM emits compression events and other failures through the
            # same domain; the event name is what separates them.
            if getattr(event, "event", "") == "jm_compression":
                _keep(NODE_JC_COMPRESSION_ERROR, SEVERITY_CRITICAL,
                      f"node {node_id}: jc compression error - critical!",
                      node_id, event)
            else:
                _keep(NODE_JC_ERROR, SEVERITY_CRITICAL,
                      f"node {node_id}: unrecoverable JC error", node_id, event)

        elif domain == ec.DOMAIN_DISTR and level in (EventObj.LEVEL_ERROR,
                                                     EventObj.LEVEL_CRITICAL):
            # Distrib faults surface in the cluster log as "distr error";
            # what they mean to the operator is that IO could not be served.
            _keep(NODE_IO_ERROR, SEVERITY_CRITICAL,
                  f"node {node_id}: unrecoverable io error", node_id, event)

        elif (getattr(event, "event", "") == ec.EVENT_CAPACITY
                and level == EventObj.LEVEL_CRITICAL):
            msg = (getattr(event, "message", "") or "").lower()
            if "provisioned" in msg:
                _keep(CLUSTER_PROV_CAPACITY_CRITICAL, SEVERITY_WARNING,
                      f"warning: cluster {cluster_id} provisioned capacity "
                      f"limit reached", "", event)
            else:
                _keep(CLUSTER_CAPACITY_CRITICAL, SEVERITY_WARNING,
                      f"warning: cluster {cluster_id} critical capacity "
                      f"limit reached", "", event)

    return [alert for alert, _date in newest.values()]


# ==========================================================================
# API latency
# ==========================================================================

def api_mean_request_seconds() -> Optional[float]:
    """Mean API request latency from this process's own Prometheus histogram.

    The same numbers the API_request_latency_high Grafana rule evaluates
    (``http_request_duration_seconds``, cumulative since process start), read
    in-process so the alert endpoint needs no Prometheus round trip. The
    ``_meta`` handlers are excluded for the same reason the Grafana rule
    excludes them: the probes and the metrics scrape run continuously and
    their fast, constant traffic holds the mean down while the endpoints that
    matter are slow.

    Returns None when the histogram is not available (instrumentation absent,
    no requests yet) -- the caller then raises no alert rather than guessing.
    """
    try:
        from prometheus_client import REGISTRY
    except Exception:
        return None

    total, count = 0.0, 0.0
    try:
        for metric in REGISTRY.collect():
            if metric.name != "http_request_duration_seconds":
                continue
            for sample in metric.samples:
                handler = sample.labels.get("handler", "")
                if handler.startswith("/_meta"):
                    continue
                if sample.name.endswith("_sum"):
                    total += sample.value
                elif sample.name.endswith("_count"):
                    count += sample.value
    except Exception:
        return None

    if count <= 0:
        return None
    return total / count


def _api_alerts(cluster_id, mean_seconds) -> List[dict]:
    if mean_seconds is None or mean_seconds <= API_SLOW_MEAN_SEC:
        return []
    return [_alert(
        API_SLOW, SEVERITY_CRITICAL,
        "simplyblock api critically slow",
        cluster_id,
        mean_request_seconds=round(mean_seconds, 3),
        threshold_seconds=API_SLOW_MEAN_SEC)]


# ==========================================================================
# Entry points
# ==========================================================================

def evaluate_alerts(cluster, nodes, events, now=None, api_mean_seconds=None) -> List[dict]:
    """Pure evaluation: records in, alerts out. See the module docstring.

    ``nodes`` are the cluster's storage nodes (devices are read from
    ``node.nvme_devices``), ``events`` its recent event log, newest first.
    """
    now = now or datetime.now(timezone.utc)
    now_ms = now.timestamp() * 1000
    cid = cluster.get_id()

    alerts = []
    alerts += _cluster_alerts(cluster, nodes)
    alerts += _node_alerts(cid, nodes, now)
    alerts += _device_alerts(cid, nodes)
    alerts += _event_alerts(cid, events, now_ms)
    alerts += _api_alerts(cid, api_mean_seconds)

    # Critical first, then stable by kind+id so a poller sees a steady order.
    alerts.sort(key=lambda a: (a["severity"] != SEVERITY_CRITICAL, a["kind"], a["id"]))
    return alerts


# ==========================================================================
# Firing / resolution lifecycle
# ==========================================================================

def reconcile(previous, current, now) -> dict:
    """Diff the last observed alert set against the current one.

    Pure. Returns ``{"active", "resolved", "raised_ids", "resolved_ids"}``,
    where ``active``/``resolved`` are the new persisted maps and the two id
    lists are the transitions that just happened -- and only the transitions.
    An alert that was firing last time and is still firing is in neither, so
    the caller emits one event when a condition starts and one when it ends,
    never one per poll. That distinction is the whole reason this is a diff
    and not a filter.
    """
    now_iso = str(now)
    prev_active = dict(previous.get("active") or {})
    prev_resolved = dict(previous.get("resolved") or {})

    current_by_id = {a["id"]: a for a in current}

    active, raised_ids = {}, []
    for alert_id, alert in current_by_id.items():
        was = prev_active.get(alert_id)
        entry = dict(alert)
        entry["status"] = STATUS_FIRING
        # first_seen survives across polls: it is when the condition started,
        # not when this evaluation ran.
        entry["first_seen"] = (was or {}).get("first_seen") or now_iso
        entry.pop("resolved_at", None)
        active[alert_id] = entry
        if was is None:
            raised_ids.append(alert_id)

    resolved, resolved_ids = {}, []
    # Carry forward earlier resolutions that are still inside the retention
    # window and have not since re-fired.
    for alert_id, entry in prev_resolved.items():
        if alert_id in current_by_id:
            continue
        age = _age_seconds(entry.get("resolved_at", ""), now)
        if age is not None and age >= HISTORY_RETENTION_SEC:
            continue
        resolved[alert_id] = entry
    # Anything that was firing and is not in the current set has just ended.
    for alert_id, entry in prev_active.items():
        if alert_id in current_by_id:
            continue
        ended = dict(entry)
        ended["status"] = STATUS_RESOLVED
        ended["resolved_at"] = now_iso
        resolved[alert_id] = ended
        resolved_ids.append(alert_id)

    # Hard cap, newest kept. The window alone does not bound this: an alert
    # that flaps produces a new history entry every time it clears, and the
    # whole set lives in one document.
    if len(resolved) > HISTORY_MAX_ENTRIES:
        newest = sorted(resolved.items(),
                        key=lambda kv: kv[1].get("resolved_at", ""),
                        reverse=True)[:HISTORY_MAX_ENTRIES]
        resolved = dict(newest)

    return {"active": active, "resolved": resolved,
            "raised_ids": raised_ids, "resolved_ids": resolved_ids}


def _log_transitions(cluster, state, raised_ids, resolved_ids):
    """Write one cluster event per transition -- raised and resolved alike.

    The resolution is not decoration. An alert that only ever appears is a
    problem report with no end: whoever was paged has no way to learn from
    the same channel that it is over, so every alert has to be chased by
    hand. Emitting the pair makes the feed self-closing.

    Bounded by construction: only transitions reach here, so a condition that
    persists for a week is two events, not one per poll.
    """
    from simplyblock_core.controllers import events_controller as ec

    for alert_id in raised_ids:
        alert = state["active"].get(alert_id) or {}
        level = (EventObj.LEVEL_CRITICAL
                 if alert.get("severity") == SEVERITY_CRITICAL
                 else EventObj.LEVEL_WARN)
        try:
            ec.log_event_cluster(
                cluster_id=cluster.get_id(), domain=ec.DOMAIN_CLUSTER,
                event=ALERT_RAISED, db_object=cluster,
                caused_by=ec.CAUSED_BY_MONITOR, event_level=level,
                node_id=alert.get("node_id") or None,
                message=f"Alert raised: {alert.get('message', alert_id)}")
        except Exception as e:
            logger.error("Failed to log raised alert %s: %s", alert_id, e)

    for alert_id in resolved_ids:
        alert = state["resolved"].get(alert_id) or {}
        try:
            ec.log_event_cluster(
                cluster_id=cluster.get_id(), domain=ec.DOMAIN_CLUSTER,
                event=ALERT_RESOLVED, db_object=cluster,
                caused_by=ec.CAUSED_BY_MONITOR,
                event_level=EventObj.LEVEL_INFO,
                node_id=alert.get("node_id") or None,
                message=f"Alert resolved: {alert.get('message', alert_id)}")
        except Exception as e:
            logger.error("Failed to log resolved alert %s: %s", alert_id, e)


def _load_state(db, cluster_id) -> AlertState:
    existing = AlertState().read_from_db(db.kv_store, id=cluster_id)
    if existing:
        return existing[0]
    fresh = AlertState()
    fresh.cluster_uuid = cluster_id
    fresh.uuid = cluster_id
    fresh.write_to_db(db.kv_store)
    return fresh


def get_alerts(cluster_id, include_history=False, history_seconds=None) -> List[dict]:
    """Evaluate this cluster's alerts, persist the transitions, return them.

    By default this returns ONLY what is wrong now -- every alert with
    ``status: firing``. That is the question an alert feed exists to answer,
    and a feed that also carries things that are already over makes the
    answer something the caller has to compute.

    ``include_history`` adds the alerts that have since resolved, newest
    first, each with its ``resolved_at``; ``history_seconds`` narrows that to
    the recent past. History is bounded by HISTORY_RETENTION_SEC and
    HISTORY_MAX_ENTRIES -- for anything older, the cluster event log holds the
    ALERT_RAISED / ALERT_RESOLVED pair for every transition.

    Raising and resolving are written to the cluster event log once per
    transition, whatever this call is asked to return: a resolution has to
    fire whether or not anybody polls for history.

    The write on a read is deliberate: an alert lifecycle needs somebody to
    notice the change, and the poll is what notices. Concurrency is safe --
    the whole set is swapped under one compare-and-set, so two API replicas
    evaluating at the same instant cannot both claim the same transition.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    # Newest first, bounded: the window rules only ever look back
    # EVENT_WINDOW_SEC, and an unbounded scan of a months-old event log on
    # every poll is exactly the cost this endpoint must not have.
    events = db.get_events(cluster_id, limit=constants.ALERT_EVENT_SCAN_LIMIT,
                           reverse=True)
    now = datetime.now(timezone.utc)
    current = evaluate_alerts(cluster, nodes, events, now=now,
                              api_mean_seconds=api_mean_request_seconds())

    state = _load_state(db, cluster_id)
    outcome: dict = {}

    def _mutate(fresh):
        # Side-effect free: atomic_update replays this on write conflict, so
        # the event emission below happens only after the commit.
        result = reconcile(
            {"active": fresh.active, "resolved": fresh.resolved}, current, now)
        fresh.active = result["active"]
        fresh.resolved = result["resolved"]
        outcome.clear()
        outcome.update(result)
        return True

    updated = db.atomic_update(state, _mutate)
    if updated is None:
        # The state record vanished (cluster deleted mid-poll, or a store
        # without transactions). Report what is firing; the next poll
        # re-creates the record and picks the lifecycle back up.
        return sorted(
            [dict(a, status=STATUS_FIRING) for a in current],
            key=_feed_sort_key)

    _log_transitions(cluster, outcome, outcome.get("raised_ids", []),
                     outcome.get("resolved_ids", []))

    feed = list(outcome["active"].values())
    if include_history or history_seconds is not None:
        feed += _history(outcome["resolved"], history_seconds, now)
    return sorted(feed, key=_feed_sort_key)


def _history(resolved, history_seconds, now) -> List[dict]:
    """Resolved alerts, optionally only those that ended recently."""
    entries = list(resolved.values())
    if history_seconds is None:
        return entries
    kept = []
    for entry in entries:
        age = _age_seconds(entry.get("resolved_at", ""), now)
        # An entry with no usable end time cannot be placed in the requested
        # window, so it is not in it.
        if age is not None and age <= history_seconds:
            kept.append(entry)
    return kept


def get_active_alerts(cluster_id) -> List[dict]:
    """Only what is wrong now. Thin alias for the default of :func:`get_alerts`."""
    return get_alerts(cluster_id)


def _feed_sort_key(alert):
    # Firing before resolved, critical before warning, then stable by kind+id.
    return (alert.get("status") == STATUS_RESOLVED,
            alert.get("severity") != SEVERITY_CRITICAL,
            alert.get("kind", ""), alert.get("id", ""))
