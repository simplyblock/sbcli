"""Site-level synchronous replication: the control plane's record of the data
plane's sync state, the status built from it and the promote / demote gates.

The distr event collector hands every ``sync_replication_status`` event to
:func:`record_sync_event`, which maps it to its LVS and persists it as a
``SyncReplicationEvent`` (the history the disaster fail-over gate judges an
LVS by), and schedules the zone catch-up (``FN_SYNC_RESYNC``) on a zone
desync. Event format: ultra
``DISTR_v2/src_code_app_spdk/specs/message_format_rpcs__distrib__v5.txt``
(``distr_status_events_get``).

The status (:func:`cluster_sync_status`, :func:`volume_sync_status`) is
computed on request from a live ``distr_sync_replication_status`` query of
every instance, cluster-wide: the worst over all LVS. :func:`check_gate`
(planned switchover) judges the same live answers; :func:`check_disaster_gate`
(a site is lost) judges only the persisted events of the lost site's nodes.
"""
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from simplyblock_core import constants, distr_controller, storage_node_ops, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import SyncGateError, SyncReplicationSiteError, SyncReplicationUnsupportedError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.utils.ttl_cache import TTLCache

logger = utils.get_logger(__name__)

SYNC_EVENT_TYPE = "sync_replication_status"

STATUS_PRIMARY_ZONE_UNAVAILABLE = "primary_zone_unavailable"
STATUS_SECONDARY_ZONE_UNAVAILABLE = "secondary_zone_unavailable"
STATUS_REMOTE_JOURNAL_UNSYNCED = "remote_journal_unsynced"
STATUS_REMOTE_JOURNAL_SYNCED = "remote_journal_synced"

#: Event status -> (record kind, the key naming its subject). A zone event is
#: about one distrib (``vuid``) and is pushed by every instance whose state
#: flips; a remote-journal event is about a journal (``jm_vuid``, the only key
#: JC has) and is pushed by its JC leader only.
_EVENT_KINDS = {
    STATUS_PRIMARY_ZONE_UNAVAILABLE: (SyncReplicationEvent.KIND_ZONE_UNAVAILABLE, "vuid"),
    STATUS_SECONDARY_ZONE_UNAVAILABLE: (SyncReplicationEvent.KIND_ZONE_UNAVAILABLE, "vuid"),
    STATUS_REMOTE_JOURNAL_UNSYNCED: (SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED, "jm_vuid"),
    STATUS_REMOTE_JOURNAL_SYNCED: (SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED, "jm_vuid"),
}


class SyncEventSubject(NamedTuple):
    """What a ``sync_replication_status`` event is about."""
    kind: str
    #: The distrib vuid of a zone event, the jm_vuid of a journal event.
    vuid: int


def classify_sync_event(event_dict: dict) -> SyncEventSubject | None:
    """The subject of a ``sync_replication_status`` event, or None for another
    event type, an unknown status or a missing / non-integer subject key
    (``jm_vuid`` may arrive as a string)."""
    if event_dict.get("event_type") != SYNC_EVENT_TYPE:
        return None
    entry = _EVENT_KINDS.get(event_dict.get("status", ""))
    if entry is None:
        return None
    kind, key = entry
    try:
        return SyncEventSubject(kind, int(event_dict[key]))
    except (KeyError, TypeError, ValueError):
        return None


def lvs_owner_of_event(node: StorageNode, nodes: list[StorageNode],
                       subject: SyncEventSubject) -> StorageNode | None:
    """The owner of the LVS whose instance on ``node`` emitted the event: among
    the LVS with an instance on ``node`` (distr_controller.lvs_owners_on_node,
    remote-triplet refs included), the one with a distrib of that vuid (zone
    event) or with that journal (remote-journal event)."""
    for owner in distr_controller.lvs_owners_on_node(node, nodes):
        if subject.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE:
            if any(bdev.get("type") == "bdev_distr"
                   and (bdev.get("params") or {}).get("vuid") == subject.vuid
                   for bdev in owner.lvstore_stack or []):
                return owner
        elif owner.jm_vuid == subject.vuid:
            return owner
    return None


def lagging_sites(home_site: str, other_site: str,
                  events: Iterable[SyncReplicationEvent]) -> list[str]:
    """The sites whose zone the open zone events of an LVS name as behind, in
    first-seen order. The primary zone of an LVS is its home site (the site of
    its owner), the replica zone the other site."""
    zone_site = {STATUS_PRIMARY_ZONE_UNAVAILABLE: home_site,
                 STATUS_SECONDARY_ZONE_UNAVAILABLE: other_site}
    sites: dict[str, None] = {}
    for event in events:
        if event.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE and event.status in zone_site:
            sites.setdefault(zone_site[event.status], None)
    return list(sites)


#: Namespace of the event ids derived by sync_event_id.
_EVENT_ID_NAMESPACE = uuid.UUID("5b0f6c3e-3f1d-4a8e-9a57-2c1d2f6e7a10")


def sync_event_id(cluster_id: str, node_id: str, event_dict: dict) -> str:
    """A stable id of one producer event: the collector re-reads a batch it
    did not discard (an error, a restart), and a re-read must not count as a
    new, later state. Derived from what the event itself carries - node,
    status, subject key and the producer's timestamp, which differs between
    two transitions of one subject on one node - never from the receive time.
    """
    subject = event_dict.get("vuid", event_dict.get("jm_vuid", ""))
    return str(uuid.uuid5(_EVENT_ID_NAMESPACE, "/".join(str(part) for part in (
        cluster_id, node_id, event_dict.get("status", ""), subject, event_dict.get("timestamp", "")))))


def record_sync_event(node_id: str, event_dict: dict) -> SyncReplicationEvent | None:
    """Persist one ``sync_replication_status`` event emitted on ``node_id``.

    Skipped with a warning (None) on a cluster without sync replication, for
    an unknown status or subject, and when no LVS with an instance on the node
    has that distrib / journal. The record's id is the producer event's own
    (sync_event_id), so a re-read batch records nothing twice. The record
    gets its receive order in the write's own transaction (DBController.record_sync_replication_event); a
    ``remote_journal_synced`` resolves the LVS's earlier drops there. A zone
    desync then asks for the LVS's catch-up - after the record is committed,
    which is what the resync runner's DONE decision relies on
    (tasks_controller.add_sync_resync_task).
    """
    db = DBController()
    node = db.get_storage_node_by_id(node_id)
    cluster = db.get_cluster_by_id(node.cluster_id)
    if not cluster.sync_replication:
        logger.warning("Node %s: sync-replication event on cluster %s without sync replication, "
                       "ignored: %s", node_id, cluster.get_id(), event_dict)
        return None
    subject = classify_sync_event(event_dict)
    if subject is None:
        logger.warning("Node %s: unknown sync-replication event, ignored: %s", node_id, event_dict)
        return None
    nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
    owner = lvs_owner_of_event(node, nodes, subject)
    if owner is None:
        logger.warning("Node %s: sync-replication event for a %s no LVS on the node has, ignored: %s",
                       node_id, "distrib" if subject.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE
                       else "journal", event_dict)
        return None

    event = SyncReplicationEvent()
    event.uuid = sync_event_id(cluster.get_id(), node_id, event_dict)
    event.cluster_id = cluster.get_id()
    event.lvs_name = owner.lvstore
    event.node_id = node_id
    event.kind = subject.kind
    event.status = str(event_dict["status"])
    event.timestamp_utc = str(event_dict.get("timestamp", ""))
    event, recorded = db.record_sync_replication_event(event)
    logger.info("LVS %s: sync-replication event %s (%s) from node %s at %s, receive_seq %s%s",
                event.lvs_name, event.status, event.kind, node_id, event.timestamp_utc,
                event.receive_seq, "" if recorded else " (re-read, already recorded)")
    # Also on a re-read: the first pass may have stopped between the record and
    # the task. A desync a catch-up has since covered needs no new one.
    if (subject.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE
            and not db.sync_event_covered(event, db.get_sync_state(cluster.get_id(), event.lvs_name))):
        tasks_controller.add_sync_resync_task(cluster.get_id(), owner.get_id(), owner.lvstore)
    return event


# ---------------------------------------------------------------------------
# status: the live answers of the distribs, per LVS and cluster-wide
# ---------------------------------------------------------------------------

STATUS_SYNCED = "synced"
STATUS_UNKNOWN = "unknown"
#: No instance answered for the distrib (not a data-plane status).
STATUS_MISSING = "missing"
MODE_FULL = "full"

STATE_HEALTHY = "healthy"
STATE_RESYNCING = "resyncing"
STATE_DEGRADED = "degraded"
#: Cluster-wide the worst state wins: a catch-up in progress is better than a
#: zone left behind.
_STATE_RANK = {STATE_HEALTHY: 0, STATE_RESYNCING: 1, STATE_DEGRADED: 2}

ROLE_PRIMARY = "primary"
ROLE_SECONDARY = "secondary"


def status_rank(status: str) -> int:
    """How far from ``synced`` a distrib's ``status`` is: ``synced`` <
    ``*_syncing`` (a catch-up runs) < ``*_unsynced`` (a zone is behind) <
    anything else (``unknown``, no answer, no status at all), which must never
    read as synced."""
    if status == STATUS_SYNCED:
        return 0
    if status.endswith("_syncing"):
        return 1
    if status.endswith("_unsynced"):
        return 2
    return 3


def _elem_status(elem) -> str:
    """The status of one ``distr_sync_replication_status`` element; an element
    without one (mode ``disabled``, malformed) is unknown, none at all missing."""
    if elem is None:
        return STATUS_MISSING
    if not isinstance(elem, dict):
        return STATUS_UNKNOWN
    status = elem.get("status")
    return status if isinstance(status, str) and status else STATUS_UNKNOWN


def _elem_of(answer, name: str):
    """The element of distrib ``name`` in one node's answer (a list), or None."""
    return next((e for e in answer if isinstance(e, dict) and e.get("name") == name), None)


def _worst(elems):
    return max(elems, key=lambda e: status_rank(_elem_status(e)))


class LvsLayout(NamedTuple):
    """What the status of one LVS is computed over."""
    lvs_name: str
    owner_id: str
    #: Distrib name -> its page size in bytes (``pba_page_size``).
    distribs: dict[str, int]
    #: The nodes holding a built instance of the LVS.
    members: tuple[str, ...]


def lvs_layout(owner: StorageNode, default_page_size: int) -> LvsLayout:
    """The layout of ``owner``'s LVS: its distribs and every built instance
    (home and remote triplet; a remote member whose instance is still pending
    has none)."""
    distribs = {bdev["name"]: int((bdev.get("params") or {}).get("pba_page_size") or default_page_size)
                for bdev in owner.lvstore_stack or [] if bdev.get("type") == "bdev_distr"}
    members = tuple(nid for nid in storage_node_ops._lvs_member_ids(owner)
                    if nid not in owner.remote_instances_pending)
    return LvsLayout(owner.lvstore, owner.get_id(), distribs, members)


@dataclass(frozen=True)
class LvsSyncStatus:
    lvs_name: str
    owner_id: str
    state: str
    #: Distrib name -> the status judged for it (the HA leader's answer, else
    #: the worst answer of the instances; ``missing`` without any).
    distribs: dict[str, str]
    worst_status: str
    #: Every answering instance reports every distrib in
    #: ``sync_replication_mode`` ``full``; False without an answer.
    mode_full: bool
    unsynced_pages: int
    #: Upper bound: unsynced pages x page size.
    bytes_behind: int
    #: AND over the distribs of their JC leader's answer; None when one has
    #: no JC leader answer or no value.
    remote_journal_in_sync: bool | None
    #: The node answering as JC leader for every distrib, "" otherwise.
    jc_leader_id: str
    #: The largest ``lag_seconds`` of a leader answer, None without one.
    lag_seconds: int | None
    #: The instances that answered.
    answering: tuple[str, ...]
    resync_running: bool
    #: Why the planned gate refuses this LVS (empty: it passes).
    gate_problems: tuple[str, ...]
    last_replicated_at: datetime | None = None


def _journal_in_sync(layout: LvsLayout, answers: dict) -> tuple[bool | None, str]:
    values: list[bool | None] = []
    leaders: set[str] = set()
    for name in layout.distribs:
        jc = [(nid, _elem_of(answers[nid], name)) for nid in layout.members
              if answers.get(nid) is not None]
        jc = [(nid, e) for nid, e in jc if e is not None and e.get("jc_leader") is True]
        if len(jc) != 1:
            # None, or two claims from snapshots taken across a JC leadership
            # handoff: nothing is known.
            values.append(None)
            continue
        nid, elem = jc[0]
        leaders.add(nid)
        value = elem.get("remote_journal_in_sync")
        values.append(value if isinstance(value, bool) else None)
    if False in values:
        return False, ""
    if None in values or not values:
        return None, ""
    return True, next(iter(leaders)) if len(leaders) == 1 else ""


def _gate_problems(layout: LvsLayout, answers: dict, answering: list[str]) -> tuple[str, ...]:
    if not answering:
        return (f"LVS {layout.lvs_name}: no instance answered",)
    problems = []
    for nid in answering:
        for name in layout.distribs:
            elem = _elem_of(answers[nid], name)
            if elem is None:
                problems.append(f"LVS {layout.lvs_name}: {name} on {nid}: no status")
            elif elem.get("sync_replication_mode") != MODE_FULL:
                problems.append(f"LVS {layout.lvs_name}: {name} on {nid}: mode "
                                f"{elem.get('sync_replication_mode')}")
            elif _elem_status(elem) != STATUS_SYNCED:
                problems.append(f"LVS {layout.lvs_name}: {name} on {nid}: {_elem_status(elem)}")
    return tuple(problems)


def aggregate_lvs(layout: LvsLayout, answers: dict, *, resync_running: bool) -> LvsSyncStatus:
    """The status of one LVS from the live answers of its instances.

    ``answers`` maps a node id to its ``distr_sync_replication_status`` answer
    (the list of its distribs' elements), or to None when the node was not
    asked or did not answer. Per distrib the HA leader's answer is preferred
    (a non-leader reports ``*_unsynced`` while the leader catches up, and its
    ``synced`` is provisional); without a leader (an idle volume) the worst
    answer counts. A distrib nobody answered for is ``missing``.

    State: ``healthy`` = every distrib ``synced`` and no catch-up running;
    ``resyncing`` = a distrib ``*_syncing``, or the LVS's catch-up task is
    running while no distrib is unknown / missing; ``degraded`` otherwise.
    """
    answering = [nid for nid in layout.members if answers.get(nid) is not None]
    distribs, chosen = {}, []
    for name in layout.distribs:
        elems = [_elem_of(answers[nid], name) for nid in answering]
        leaders = [e for e in elems if e is not None and e.get("ha_leader") is True]
        elem = _worst(leaders or elems) if elems else None
        distribs[name] = _elem_status(elem)
        chosen.append((name, elem))
    worst = max(distribs.values(), key=status_rank, default=STATUS_SYNCED)
    rank = status_rank(worst)
    if rank == 0 and not resync_running:
        state = STATE_HEALTHY
    elif rank == 1 or (resync_running and rank < 3):
        state = STATE_RESYNCING
    else:
        state = STATE_DEGRADED
    pages = bytes_behind = 0
    lags = []
    for name, elem in chosen:
        if not isinstance(elem, dict):
            continue
        n = sum(v for v in (elem.get("n_primary_unsynced_pages"), elem.get("n_replica_unsynced_pages"))
                if isinstance(v, int))
        pages += n
        bytes_behind += n * layout.distribs[name]
        if elem.get("ha_leader") is True and isinstance(elem.get("lag_seconds"), int):
            lags.append(elem["lag_seconds"])
    in_sync, jc_leader_id = _journal_in_sync(layout, answers)
    return LvsSyncStatus(
        lvs_name=layout.lvs_name, owner_id=layout.owner_id, state=state, distribs=distribs,
        worst_status=worst,
        mode_full=bool(answering) and all(
            (_elem_of(answers[nid], name) or {}).get("sync_replication_mode") == MODE_FULL
            for nid in answering for name in layout.distribs),
        unsynced_pages=pages, bytes_behind=bytes_behind, remote_journal_in_sync=in_sync,
        jc_leader_id=jc_leader_id, lag_seconds=max(lags, default=None),
        answering=tuple(answering), resync_running=resync_running,
        gate_problems=_gate_problems(layout, answers, answering))


def parse_event_time(timestamp: str) -> datetime | None:
    """The UTC time of an event's ISO-8601 ``timestamp_utc``; None when it
    cannot be parsed."""
    try:
        parsed = datetime.fromisoformat(timestamp)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def lvs_last_replicated_at(status: LvsSyncStatus, open_zone_events: Iterable[SyncReplicationEvent],
                           now: datetime) -> datetime | None:
    """When the LVS was last known in sync: now while healthy; else the
    earliest open zone-desync event; without one (an event can be missing, e.g.
    an ``unknown`` after a restart) now - the leader's ``lag_seconds``; else
    None (not known)."""
    if status.state == STATE_HEALTHY:
        return now
    times = [t for t in (parse_event_time(e.timestamp_utc) for e in open_zone_events
                         if e.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE) if t is not None]
    if times:
        return min(times)
    if status.lag_seconds is not None:
        return now - timedelta(seconds=status.lag_seconds)
    return None


@dataclass(frozen=True)
class ClusterSyncStatus:
    """The sync-replication status of a cluster: the worst over all its LVS
    (the same answer for every volume, whatever it is queried by)."""
    state: str
    degraded: bool
    resyncing: bool
    completed: bool
    peer_ready: bool
    diverged: bool
    last_replicated_at: datetime | None
    lag_seconds: int | None
    bytes_behind: int
    computed_at: datetime
    lvs: tuple[LvsSyncStatus, ...]


def aggregate_cluster(lvs_statuses: Iterable[LvsSyncStatus], now: datetime) -> ClusterSyncStatus:
    """The cluster-wide status over the LVS statuses (their
    ``last_replicated_at`` filled): the worst state; ``completed`` when every
    distrib replicates in mode ``full``; the earliest known last in-sync time
    of the LVS that are not healthy."""
    lvs = tuple(lvs_statuses)
    state = max((s.state for s in lvs), key=_STATE_RANK.__getitem__, default=STATE_HEALTHY)
    degraded = any(s.state == STATE_DEGRADED for s in lvs)
    resyncing = any(s.state == STATE_RESYNCING for s in lvs)
    completed = all(s.mode_full for s in lvs)
    last: datetime | None
    if state == STATE_HEALTHY:
        last = now
    else:
        last = min((s.last_replicated_at for s in lvs
                    if s.state != STATE_HEALTHY and s.last_replicated_at is not None), default=None)
    lag = None if last is None else max(0, int((now - last).total_seconds()))
    return ClusterSyncStatus(
        state=state, degraded=degraded, resyncing=resyncing, completed=completed,
        peer_ready=completed and not degraded and not resyncing, diverged=state != STATE_HEALTHY,
        last_replicated_at=last, lag_seconds=lag, bytes_behind=sum(s.bytes_behind for s in lvs),
        computed_at=now, lvs=lvs)


@dataclass(frozen=True)
class VolumeSyncStatus:
    """The status of one volume as seen from one site: the cluster's status
    plus the volume's role there."""
    role: str
    site: str
    cluster: ClusterSyncStatus


def volume_role(lvol: LVol, owner: StorageNode, site: str) -> str:
    """``primary`` on the site the volume is active on, ``secondary`` elsewhere."""
    return ROLE_PRIMARY if (lvol.sync_active_site or owner.site) == site else ROLE_SECONDARY


# ---------------------------------------------------------------------------
# the disaster fail-over gate: persisted events of the lost site
# ---------------------------------------------------------------------------

def latest_journal_drop(events: Iterable[SyncReplicationEvent], drop_counts: Callable[[str], bool],
                        restore_counts: Callable[[str], bool]) -> SyncReplicationEvent | None:
    """The open remote-journal drop of an LVS as told by the drops of the
    nodes ``drop_counts`` accepts and the restores of the nodes
    ``restore_counts`` accepts (by node id) alone: the latest of those events
    by receive order, when it is a drop. A record without a receive_seq (older
    than the field) is judged by its own flag.

    Deliberately not the LVS-wide watermark: receive order across nodes is not
    real order (each node has its own collector), so a delayed ``synced`` of
    another site must not end a drop of this one.
    """
    journal = [e for e in events
               if (e.kind == SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED and drop_counts(e.node_id))
               or (e.kind == SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED and restore_counts(e.node_id))]
    legacy = [e for e in journal if e.receive_seq <= 0
              and e.kind == SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED and not e.resolved]
    if legacy:
        return legacy[0]
    latest = max((e for e in journal if e.receive_seq > 0), key=lambda e: e.receive_seq, default=None)
    if latest is not None and latest.kind == SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED:
        return latest
    return None


def lvs_active_on(owner: StorageNode, site: str) -> bool:
    """Whether ``owner``'s LVS may be led from ``site``: it is its active site,
    or a leadership move is in flight (the leader may then be on either
    side)."""
    return (owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX)
            or storage_node_ops.lvs_active_site_of(owner) == site)


def disaster_gate_problems(lvs_name: str, events: Iterable[SyncReplicationEvent], state: dict,
                           lost_site: str, node_sites: dict[str, str]) -> list[str]:
    """Why an LVS active on the lost site ``lost_site`` may not be failed over:
    a zone desync or a remote-journal drop reported by a node of that site
    (its pre-loss leader site) that is not over. Events of the surviving site
    are consequences of the loss and never count. Fail-closed on a node no
    longer known: its desync or drop counts (it cannot be shown to be of the
    surviving site), its restore does not (it cannot be shown to be of the
    lost one). ``state`` is the LVS's sync state record
    (DBController.get_sync_state), whose watermark ends zone desyncs a
    verified catch-up covered."""
    def on_lost_site(node_id: str) -> bool:
        return node_sites.get(node_id, lost_site) == lost_site

    def known_on_lost_site(node_id: str) -> bool:
        return node_sites.get(node_id) == lost_site

    events = list(events)
    problems = [f"LVS {lvs_name}: zone desync {e.status} at {e.timestamp_utc} reported by {e.node_id}"
                for e in events
                if e.kind == SyncReplicationEvent.KIND_ZONE_UNAVAILABLE and on_lost_site(e.node_id)
                and not e.resolved and not DBController.sync_event_covered(e, state)]
    drop = latest_journal_drop(events, on_lost_site, known_on_lost_site)
    if drop is not None:
        problems.append(f"LVS {lvs_name}: remote journal dropped at {drop.timestamp_utc} "
                        f"reported by {drop.node_id}")
    return problems


# ---------------------------------------------------------------------------
# DB / RPC glue
# ---------------------------------------------------------------------------

_status_cache = TTLCache()


def _sync_cluster(db: DBController, cluster_id: str):
    cluster = db.get_cluster_by_id(cluster_id)
    if not cluster.sync_replication:
        raise SyncReplicationUnsupportedError(
            f"cluster {cluster_id} is not a sync-replication cluster")
    return cluster


def _lvs_owners(nodes: list[StorageNode]) -> list[StorageNode]:
    return [n for n in nodes if n.lvstore and n.lvstore_stack and n.status != StorageNode.STATUS_REMOVED]


def _query_node(node: StorageNode):
    """One node's ``distr_sync_replication_status`` answer for all its
    distribs, or None when it does not answer."""
    try:
        answer = node.rpc_client(timeout=constants.SYNC_STATUS_RPC_TIMEOUT_SEC,
                                 retry=1).distr_sync_replication_status()
    except RPCException as e:
        logger.warning("Node %s: sync-replication status query failed: %s", node.get_id(), e)
        return None
    if not isinstance(answer, list):
        logger.warning("Node %s: unexpected sync-replication status answer: %r", node.get_id(), answer)
        return None
    return answer


def _record_live_journal_restore(db: DBController, cluster_id: str, owner: StorageNode,
                                 status: LvsSyncStatus, seq: int, node_sites: dict[str, str]) -> None:
    """Record a live ``remote_journal_in_sync: true`` of the LVS's JC leader as
    a synced journal state, when some drop is still open for it - LVS-wide, or
    for the leader's site alone (latest_journal_drop). Conditional on no event
    of the LVS having been received since ``seq`` was read, before the query:
    a drop received meanwhile may be newer than the answer."""
    if status.remote_journal_in_sync is not True or not status.jc_leader_id:
        return
    leader_site = node_sites.get(status.jc_leader_id, "")
    events = db.get_sync_replication_events(cluster_id, owner.lvstore)
    state = db.get_sync_state(cluster_id, owner.lvstore)
    open_drop = any(e.kind == SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED and not e.resolved
                    and not db.sync_event_covered(e, state) for e in events)
    def on_leader_site(node_id: str) -> bool:
        return node_sites.get(node_id) == leader_site

    if not open_drop and latest_journal_drop(events, on_leader_site, on_leader_site) is None:
        return
    timestamp = datetime.now(UTC).isoformat()
    event = SyncReplicationEvent()
    event.uuid = sync_event_id(cluster_id, status.jc_leader_id, {
        "status": STATUS_REMOTE_JOURNAL_SYNCED, "jm_vuid": owner.jm_vuid, "timestamp": timestamp})
    event.cluster_id = cluster_id
    event.lvs_name = owner.lvstore
    event.node_id = status.jc_leader_id
    event.kind = SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED
    event.status = STATUS_REMOTE_JOURNAL_SYNCED
    event.timestamp_utc = timestamp
    event, recorded = db.record_sync_replication_event(event, expect_seq=seq)
    if recorded:
        logger.info("LVS %s: live remote journal in sync on JC leader %s recorded, receive_seq %s",
                    owner.lvstore, status.jc_leader_id, event.receive_seq)
    else:
        logger.info("LVS %s: live remote journal in sync on %s not recorded: an event was "
                    "received since the query", owner.lvstore, status.jc_leader_id)


def _live_lvs_statuses(db: DBController, cluster) -> list[LvsSyncStatus]:
    """Query every online instance once and build the status of every LVS of
    the cluster (last_replicated_at filled)."""
    cluster_id = cluster.get_id()
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    node_sites = {n.get_id(): n.site for n in nodes}
    owners = _lvs_owners(nodes)
    layouts = [lvs_layout(owner, cluster.page_size_in_blocks) for owner in owners]
    # Read before the query: see _record_live_journal_restore.
    seqs = {owner.lvstore: db.get_sync_state(cluster_id, owner.lvstore)["seq"] for owner in owners}
    wanted = {nid for layout in layouts for nid in layout.members}
    answers = {n.get_id(): _query_node(n) for n in nodes
               if n.get_id() in wanted and n.status == StorageNode.STATUS_ONLINE}
    running = {t.function_params.get("lvs_name") for t in db.get_active_sync_resync_tasks(cluster_id)
               if t.status == JobSchedule.STATUS_RUNNING and not t.canceled}
    now = datetime.now(UTC)
    statuses = []
    for owner, layout in zip(owners, layouts):
        status = aggregate_lvs(layout, answers, resync_running=layout.lvs_name in running)
        _record_live_journal_restore(db, cluster_id, owner, status, seqs[owner.lvstore], node_sites)
        open_events = ([] if status.state == STATE_HEALTHY
                       else db.get_unresolved_sync_replication_events(cluster_id, owner.lvstore))
        statuses.append(replace(status, last_replicated_at=lvs_last_replicated_at(status, open_events, now)))
    return statuses


def _compute_cluster_sync_status(cluster_id: str) -> ClusterSyncStatus:
    db = DBController()
    cluster = _sync_cluster(db, cluster_id)
    return aggregate_cluster(_live_lvs_statuses(db, cluster), datetime.now(UTC))


def cluster_sync_status(cluster_id: str, *, max_age: float = 0.0) -> ClusterSyncStatus:
    """The cluster's sync-replication status, from a live query of every
    instance. ``max_age`` > 0 lets a status endpoint be served an answer up to
    that many seconds old (constants.SYNC_STATUS_CACHE_SEC); nothing that
    decides an action may pass it - the gates never do.

    Raises SyncReplicationUnsupportedError on a cluster without sync
    replication."""
    if max_age > 0:
        return _status_cache.get_or_compute(
            cluster_id, max_age, lambda: _compute_cluster_sync_status(cluster_id))
    status = _compute_cluster_sync_status(cluster_id)
    _status_cache.put(cluster_id, status)
    return status


def _check_site(db: DBController, cluster_id: str, site: str) -> None:
    sites = {n.site for n in db.get_storage_nodes_by_cluster_id(cluster_id) if n.site}
    if site not in sites:
        raise SyncReplicationSiteError(
            f"site {site!r} is not a site of cluster {cluster_id} (sites: {sorted(sites)})")


def volume_sync_status(lvol: LVol, site: str, *, max_age: float = 0.0) -> VolumeSyncStatus:
    """The sync-replication status of ``lvol`` seen from ``site``: the
    cluster-wide status and the volume's role there (cluster_sync_status for
    ``max_age``). Raises SyncReplicationUnsupportedError on a cluster without
    sync replication, SyncReplicationSiteError for a missing or unknown site."""
    db = DBController()
    owner = db.get_storage_node_by_id(lvol.node_id)
    _sync_cluster(db, owner.cluster_id)
    _check_site(db, owner.cluster_id, site)
    return VolumeSyncStatus(role=volume_role(lvol, owner, site), site=site,
                            cluster=cluster_sync_status(owner.cluster_id, max_age=max_age))


def check_gate(cluster_id: str) -> None:
    """The planned switchover gate (both sites up), always live: every
    answering instance of every LVS reports every distrib ``synced`` in mode
    ``full``; an LVS or a distrib nobody answered for fails. No journal
    condition: with both sites up the new leader levels the journal over every
    reachable copy, and a JC leader may not exist while the apps are stopped.

    Raises SyncGateError listing what failed, SyncReplicationUnsupportedError
    on a cluster without sync replication."""
    db = DBController()
    cluster = _sync_cluster(db, cluster_id)
    statuses = _live_lvs_statuses(db, cluster)
    problems = [p for status in statuses for p in status.gate_problems]
    if problems:
        raise SyncGateError("sync-replication planned", problems)


def check_disaster_gate(cluster_id: str, lost_site: str) -> None:
    """The disaster fail-over gate for the loss of ``lost_site``: only the LVS
    active on it (lvs_active_on) move, and each is judged from the persisted
    events of the lost site's nodes (disaster_gate_problems) - never a live
    query, the site is gone. LVS active on the surviving site go degraded
    because of the loss itself and do not block.

    Raises SyncGateError listing what failed, SyncReplicationUnsupportedError
    on a cluster without sync replication, SyncReplicationSiteError for an
    unknown site."""
    db = DBController()
    _sync_cluster(db, cluster_id)
    _check_site(db, cluster_id, lost_site)
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    node_sites = {n.get_id(): n.site for n in nodes}
    problems = []
    for owner in _lvs_owners(nodes):
        if not lvs_active_on(owner, lost_site):
            continue
        problems += disaster_gate_problems(
            owner.lvstore, db.get_sync_replication_events(cluster_id, owner.lvstore),
            db.get_sync_state(cluster_id, owner.lvstore), lost_site, node_sites)
    if problems:
        raise SyncGateError(f"sync-replication disaster fail-over (site {lost_site} lost)", problems)
