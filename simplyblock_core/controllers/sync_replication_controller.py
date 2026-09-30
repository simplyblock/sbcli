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
from simplyblock_core.exceptions import (
    SyncAnaError, SyncGateError, SyncGroupMemberError, SyncPromoteRefusedError, SyncReplicationSiteError,
    SyncReplicationUnsupportedError, SyncSiteOfflineError,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice
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


def zone_not_up(nodes: Iterable[StorageNode], site: str) -> list[str]:
    """What keeps ``site``'s zone from being fully up: its nodes that are not
    online and their devices that are not online (removed / migrated-away ones
    do not count, as for the other migration runners). A FAILED device counts:
    its zone is not whole. Shared by the resync runner and the site return."""
    down = []
    for node in nodes:
        if node.site != site or node.status in (StorageNode.STATUS_IN_CREATION,
                                                StorageNode.STATUS_REMOVED):
            continue
        if node.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
            down.append(f"node:{node.get_id()}")
        for dev in node.nvme_devices:
            if dev.status in (NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED_AND_MIGRATED):
                continue
            if dev.status != NVMeDevice.STATUS_ONLINE:
                down.append(f"dev:{dev.get_id()}")
    return down


def site_return_problems(nodes: Iterable[StorageNode], site: str) -> list[str]:
    """Why the lost ``site`` has not fully returned: a node of it (not removed,
    not in creation) that is not ONLINE - SUSPENDED is not back - or anything
    zone_not_up reports."""
    nodes = list(nodes)
    problems = [f"node:{n.get_id()} {n.status}" for n in nodes
                if n.site == site and n.status not in (StorageNode.STATUS_REMOVED,
                                                       StorageNode.STATUS_IN_CREATION,
                                                       StorageNode.STATUS_ONLINE)]
    return problems + [p for p in zone_not_up(nodes, site) if not p.startswith("node:")]


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


def volume_sync_status_by_id(lvol_id: str, site: str, *, max_age: float = 0.0) -> VolumeSyncStatus:
    """volume_sync_status of the stored volume ``lvol_id`` (KeyError when
    there is none)."""
    return volume_sync_status(DBController().get_lvol_by_id(lvol_id), site, max_age=max_age)


def group_role(roles: Iterable[str]) -> str:
    """The role of a consistency group on a site from its members' roles:
    ``primary`` only when it has members and every one is primary there - an
    empty or partly served group never claims the site."""
    roles = list(roles)
    return ROLE_PRIMARY if roles and all(r == ROLE_PRIMARY for r in roles) else ROLE_SECONDARY


@dataclass(frozen=True)
class GroupSyncStatus:
    """The status of a consistency group seen from one site: the cluster's
    status (the same for every member, never summed over them), the group's
    role there (group_role) and its current member count."""
    role: str
    site: str
    member_count: int
    cluster: ClusterSyncStatus


def group_sync_status(group_id: str, site: str, *, max_age: float = 0.0) -> GroupSyncStatus:
    """The sync-replication status of consistency group ``group_id`` seen from
    ``site`` (cluster_sync_status for ``max_age``). The cluster and the site
    are judged from the group's own cluster, so an empty group answers too.
    Raises SyncReplicationUnsupportedError, SyncReplicationSiteError,
    SyncGroupMemberError (a member that cannot be resolved)."""
    db = DBController()
    group = db.get_consistency_group_by_id(group_id)
    _sync_cluster(db, group.cluster_id)
    _check_site(db, group.cluster_id, site)
    volumes = _group_volumes(db, group_id)
    owners: dict[str, StorageNode] = {}
    for lvol in volumes:
        if lvol.node_id not in owners:
            owners[lvol.node_id] = db.get_storage_node_by_id(lvol.node_id)
    return GroupSyncStatus(
        role=group_role(volume_role(lv, owners[lv.node_id], site) for lv in volumes),
        site=site, member_count=len(volumes),
        cluster=cluster_sync_status(group.cluster_id, max_age=max_age))


# ---------------------------------------------------------------------------
# Site return
# ---------------------------------------------------------------------------

#: The statuses a distrib reports once it has replayed its journal: anything
#: else (``unknown`` before the first replay, a missing or malformed element)
#: is not a fact yet.
REPLAYED_STATUSES = (STATUS_SYNCED, "primary_unsynced", "replica_unsynced", "primary_syncing",
                     "replica_syncing")

#: At most one "return held" event per LVS per this many seconds (the return
#: step runs on every monitor pass).
_RETURN_EVENT_INTERVAL_SEC = 600
_return_events = TTLCache()


def replay_problems(layout: LvsLayout, answer) -> list[str]:
    """Why the leader's ``distr_sync_replication_status`` ``answer`` does not
    show every distrib of the LVS replayed yet: no answer, a distrib absent,
    not in mode ``full``, or a status outside REPLAYED_STATUSES."""
    if answer is None:
        return [f"LVS {layout.lvs_name}: the leader did not answer"]
    problems = []
    for name in layout.distribs:
        elem = _elem_of(answer, name)
        if elem is None:
            problems.append(f"LVS {layout.lvs_name}: {name}: no status")
        elif elem.get("sync_replication_mode") != MODE_FULL:
            problems.append(f"LVS {layout.lvs_name}: {name}: mode {elem.get('sync_replication_mode')}")
        elif _elem_status(elem) not in REPLAYED_STATUSES:
            problems.append(f"LVS {layout.lvs_name}: {name}: {_elem_status(elem)}")
    return problems


def _held(owner: StorageNode, leader: StorageNode, missing: list[str]) -> None:
    key = (owner.cluster_id, owner.lvstore)
    if _return_events.get(key, _RETURN_EVENT_INTERVAL_SEC):
        return
    _return_events.put(key, True)
    from simplyblock_core.controllers import storage_events
    storage_events.sync_site_return_lvols_missing(leader, owner.lvstore, missing)


def _elect_on_returned_site(db: DBController, cluster, owner: StorageNode) -> bool:
    """Give ``owner``'s LVS, led from the returned site, its leader there and
    check it can serve: every expected volume registered on the leader, every
    distrib replayed (a status that is a fact). False: not now."""
    leader_id = storage_node_ops.elect_lvs_leader_on_site(owner.get_id())
    if not leader_id:
        return False
    leader = db.get_storage_node_by_id(leader_id)
    missing = storage_node_ops.missing_lvols_on(leader, owner, db)
    if missing:
        logger.error("Site return: leader %s of LVS %s misses volumes %s", leader_id,
                     owner.lvstore, missing)
        _held(owner, leader, missing)
        return False
    problems = replay_problems(lvs_layout(owner, cluster.page_size_in_blocks), _query_node(leader))
    if problems:
        logger.info("Site return: LVS %s not replayed yet: %s", owner.lvstore, problems)
        return False
    return True


def settle_site_return(cluster_id: str) -> bool:
    """The return of a lost site, run by the storage-node monitor on every pass
    (cheap DB checks until the site is back). With a site recorded as lost -
    its fence ``done``, or ``fencing`` left behind by a disaster promote that
    no longer runs - and every node of it ONLINE with its zone up
    (site_return_problems):

    1. every LVS still led from that site (never failed over: not promoted,
       not promotable, or empty) gets its leader there
       (storage_node_ops.elect_lvs_leader_on_site) and must show every
       expected volume on it and every distrib replayed;
    2. a catch-up task for every LVS (both zones of each have diverged);
    3. ``lost_site`` / ``lost_site_state`` cleared in one transaction that
       re-reads the site's nodes (DBController.clear_lost_site).

    Any step not possible now -> False, retried by the next pass. The volumes
    of the LVS that were active on the site stay fenced there
    (``sync_demoted_sites``) until promoted. True when the site was cleared."""
    db = DBController()
    cluster = db.get_cluster_by_id(cluster_id)
    lost = cluster.lost_site
    if not cluster.sync_replication or not lost:
        return False
    state = cluster.lost_site_state
    if state == LOST_SITE_FENCING:
        if any(t.function_params.get("lost_site") == lost
               for t in db.get_active_sync_promote_tasks(cluster_id)):
            return False
    elif state != LOST_SITE_DONE:
        return False
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    if site_return_problems(nodes, lost):
        return False
    for owner in _lvs_owners(nodes):
        if storage_node_ops._led_from_lost_site(cluster, owner) and \
                not _elect_on_returned_site(db, cluster, owner):
            return False
    for owner in _lvs_owners(nodes):
        tasks_controller.add_sync_resync_task(cluster_id, owner.get_id(), owner.lvstore)
    problems = db.clear_lost_site(cluster_id, lost, (state,), [n.get_id() for n in nodes if n.site == lost],
                                  lambda _cluster, site_nodes: site_return_problems(site_nodes, lost))
    if problems:
        logger.warning("Site %s return not recorded: %s", lost, problems)
        return False
    logger.warning("Site %s returned: lost_site cleared, catch-up scheduled for every LVS", lost)
    return True


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


def check_disaster_gate(cluster_id: str, lost_site: str, lvs_names: Iterable[str] | None = None) -> None:
    """The disaster fail-over gate for the loss of ``lost_site``: only the LVS
    active on it (lvs_active_on) move, and each is judged from the persisted
    events of the lost site's nodes (disaster_gate_problems) - never a live
    query, the site is gone. LVS active on the surviving site go degraded
    because of the loss itself and do not block. ``lvs_names`` restricts it to
    the LVS a promote moves (None: every LVS): another LVS of the lost site
    that is not in sync blocks only its own promote.

    Raises SyncGateError listing what failed, SyncReplicationUnsupportedError
    on a cluster without sync replication, SyncReplicationSiteError for an
    unknown site."""
    db = DBController()
    _sync_cluster(db, cluster_id)
    _check_site(db, cluster_id, lost_site)
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    node_sites = {n.get_id(): n.site for n in nodes}
    selected = None if lvs_names is None else set(lvs_names)
    problems = []
    for owner in _lvs_owners(nodes):
        if not lvs_active_on(owner, lost_site) or (selected is not None and owner.lvstore not in selected):
            continue
        problems += disaster_gate_problems(
            owner.lvstore, db.get_sync_replication_events(cluster_id, owner.lvstore),
            db.get_sync_state(cluster_id, owner.lvstore), lost_site, node_sites)
    if problems:
        raise SyncGateError(f"sync-replication disaster fail-over (site {lost_site} lost)", problems)


# ---------------------------------------------------------------------------
# Demote / promote
# ---------------------------------------------------------------------------

def volume_site(lvol: LVol, owner: StorageNode) -> str:
    """The site ``lvol`` is recorded as active on (its LVS's home site until
    set)."""
    return lvol.sync_active_site or owner.site


def volume_open_on(lvol: LVol, owner: StorageNode, site: str) -> bool:
    """Whether ``lvol`` is recorded as served on ``site``: active there and not
    demoted there. The record view of the site rule, without the LVS's
    leadership state (storage_node_ops.lvol_site_open)."""
    return volume_site(lvol, owner) == site and site not in lvol.sync_demoted_sites


def volumes_open_on(volumes: Iterable[LVol], owner: StorageNode, site: str) -> list[str]:
    """The ids of ``volumes`` (of ``owner``'s LVS) recorded as served on
    ``site``; deleted records do not count."""
    return [v.get_id() for v in volumes
            if v.status != LVol.STATUS_DELETED and volume_open_on(v, owner, site)]


def lvs_site_triplet(owner: StorageNode, site: str) -> tuple[str, ...]:
    """The triplet of ``owner``'s LVS on ``site``, primary first: the home
    triplet on the owner's site, the remote one on the other."""
    triplet = ((owner.get_id(), owner.secondary_node_id, owner.tertiary_node_id)
               if site == owner.site else storage_node_ops.remote_triplet_refs(owner))
    return tuple(nid for nid in triplet if nid)


#: Node states whose paths a strict close skips: SPDK is down, and the node's
#: restart publishes every path by the site rule (closed once recorded).
_CLOSE_SKIPPED = (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED)


def site_triplet_nodes(db: DBController, owner: StorageNode, site: str) -> list[StorageNode]:
    """The nodes of ``site``'s triplet of ``owner``'s LVS, primary first
    (lvs_site_triplet). A member that no longer exists fails (SyncAnaError):
    its paths cannot be told closed."""
    nodes = []
    for node_id in lvs_site_triplet(owner, site):
        try:
            nodes.append(db.get_storage_node_by_id(node_id))
        except KeyError as e:
            raise SyncAnaError(f"LVS {owner.lvstore}: node {node_id} of site {site} not found") from e
    return nodes


def set_site_ana_strict(lvol: LVol, nodes: list[StorageNode], *, open_site: bool) -> None:
    """Set ``lvol``'s ANA group (anagrpid = its namespace id) on every path of
    ``nodes`` - the triplet of one site of THIS LVS, primary first
    (site_triplet_nodes) - on each node's port of the LVS: closed
    (``inaccessible``) or opened (the triplet primary ``optimized``, its
    secondary / tertiary ``non_optimized``). No retry queue: any path that
    answers false or raises fails the whole call (SyncAnaError), so the caller
    records nothing.

    A close skips members whose SPDK is down (OFFLINE / REMOVED); an open
    touches ONLINE members only - a member that is not online gets the right
    state from the site rule when it comes back. The caller holds the LVS's
    site-rule lock (storage_node_ops.sync_site_rule_locks)."""
    if not lvol.ns_id:
        raise SyncAnaError(f"volume {lvol.get_id()} has no namespace id")
    for index, node in enumerate(nodes):
        node_id = node.get_id()
        if open_site:
            if node.status != StorageNode.STATUS_ONLINE:
                logger.warning("ANA open of %s on %s skipped: node is %s", lvol.get_id(), node_id,
                               node.status)
                continue
            state = "optimized" if index == 0 else "non_optimized"
        else:
            if node.status in _CLOSE_SKIPPED:
                logger.warning("ANA close of %s on %s skipped: node is %s", lvol.get_id(), node_id,
                               node.status)
                continue
            state = "inaccessible"
        rpc = node.rpc_client(timeout=10, retry=2)
        port = node.get_lvol_subsys_port(lvol.lvs_name)
        for trtype, ip in storage_node_ops._lvol_listener_nics(lvol, node):
            where = f"{lvol.nqn} ns {lvol.ns_id} on {node_id} ({ip}:{port})"
            try:
                done = rpc.nvmf_subsystem_listener_set_ana_state(
                    lvol.nqn, ip, port, trtype=trtype, ana=state, anagrpid=lvol.ns_id)
            except RPCException as e:
                raise SyncAnaError(f"ANA {state} of {where} failed: {e}") from e
            if not done:
                raise SyncAnaError(f"ANA {state} of {where} refused")
            logger.info("ANA: %s -> %s", where, state)


def _group_volumes(db: DBController, group_id: str) -> list[LVol]:
    """Every current member of consistency group ``group_id``, as volumes; a
    member that cannot be resolved to a live volume refuses the whole group."""
    from simplyblock_core.controllers import consistency_group_controller
    group = db.get_consistency_group_by_id(group_id)
    volumes, missing = [], []
    for row in consistency_group_controller.list_members(group):
        try:
            lvol = db.get_lvol_by_id(row["lvol_id"])
        except KeyError:
            missing.append(row["lvol_id"])
            continue
        if lvol.status == LVol.STATUS_DELETED:
            missing.append(row["lvol_id"])
        else:
            volumes.append(lvol)
    if missing:
        raise SyncGroupMemberError(f"consistency group {group_id}: members not found: {missing}",
                                   missing)
    return volumes


def _volumes_context(db: DBController, volumes: list[LVol], site: str):
    """The cluster and each volume's owner (fresh) of a demote / promote
    request, after the cluster and site checks."""
    if not volumes:
        raise SyncReplicationSiteError("no volume given")
    owners = {}
    for lvol in volumes:
        if lvol.node_id not in owners:
            owners[lvol.node_id] = db.get_storage_node_by_id(lvol.node_id)
    cluster_id = next(iter(owners.values())).cluster_id
    cluster = _sync_cluster(db, cluster_id)
    _check_site(db, cluster_id, site)
    return cluster, owners


def _demote(db: DBController, volumes: list[LVol], site: str) -> list[str]:
    cluster, owners = _volumes_context(db, volumes, site)
    todo = [lv for lv in volumes if volume_open_on(lv, owners[lv.node_id], site)]
    if not todo:
        return []
    check_gate(cluster.get_id())
    demoted = []
    with storage_node_ops.sync_site_rule_locks(cluster.get_id(), {lv.lvs_name for lv in todo}):
        for lvol in todo:
            # Re-read inside the lock: what a writer sees is what counts.
            lvol = db.get_lvol_by_id(lvol.get_id())
            owner = db.get_storage_node_by_id(lvol.node_id)
            if not volume_open_on(lvol, owner, site):
                continue
            set_site_ana_strict(lvol, site_triplet_nodes(db, owner, site), open_site=False)

            def _demoted(v):
                if site in v.sync_demoted_sites:
                    return False
                v.sync_demoted_sites = [*v.sync_demoted_sites, site]
                return True
            db.atomic_update(lvol, _demoted)
            demoted.append(lvol.get_id())
            logger.info("Volume %s demoted on site %s", lvol.get_id(), site)
    return demoted


def sync_demote_lvol(lvol_id: str, site: str) -> list[str]:
    """Demote volume ``lvol_id`` on ``site``: fence its paths there. A volume
    not served on ``site`` is a no-op. Otherwise the planned gate (live), then,
    under the LVS's site-rule lock, ``inaccessible`` on every path of the
    site's triplet (set_site_ana_strict) and only then ``site`` added to
    ``sync_demoted_sites``. Returns the ids demoted (empty: no-op).

    Raises SyncGateError, SyncAnaError (nothing recorded),
    SyncReplicationUnsupportedError, SyncReplicationSiteError."""
    db = DBController()
    return _demote(db, [db.get_lvol_by_id(lvol_id)], site)


def sync_demote_group(group_id: str, site: str) -> list[str]:
    """sync_demote_lvol over every member of consistency group ``group_id``:
    one gate, the members in order, each recorded right after its own fence;
    the first failure raises (members recorded before it stay demoted - a
    retry is a no-op for them). SyncGroupMemberError when a member cannot be
    resolved, before anything is fenced."""
    db = DBController()
    return _demote(db, _group_volumes(db, group_id), site)


PROMOTE_ACTIVE = "active"
PROMOTE_IN_PROGRESS = "in_progress"
PROMOTE_ANA_ONLY = "ana_only"
PROMOTE_MOVE = "move"
PROMOTE_NOT_DEMOTED = "not_demoted"
PROMOTE_LVS_BUSY = "lvs_busy"
PROMOTE_SITE_OFFLINE = "site_offline"
PROMOTE_FORCE_ONLINE = "force_online"
PROMOTE_DISASTER = "disaster"

#: The rows that queue a promote task (after the live gate).
PROMOTE_QUEUED = (PROMOTE_MOVE, PROMOTE_ANA_ONLY)


class PromoteDecision(NamedTuple):
    """The row of the promote table a volume falls in (promote_decision)."""
    kind: str
    #: The site the volume's LVS is led from (T), "" while it is moving.
    source_site: str = ""
    #: Volumes that block it (not demoted / still served on T).
    blocking: tuple[str, ...] = ()


def promote_decision(lvol: LVol, owner: StorageNode, lvs_volumes: Iterable[LVol], site: str, *,
                     online_sites: set[str], force: bool) -> PromoteDecision:
    """The promote table for ``lvol`` to ``site`` (S). T is the site its LVS is
    led from; ``online_sites`` the sites with an online node that are not the
    cluster's lost site; ``lvs_volumes`` every volume of the LVS.

    - a leadership move in flight -> in progress
    - led from S and the volume served there -> active (200, no-op)
    - led from S, the volume not open there -> only the ANA step
    - T not online -> disaster fail-over when forced, else site offline (412)
    - forced while T is online -> refused (force never acts on a live site)
    - the volume still served on T -> not demoted (409)
    - another volume of the LVS still served on T -> LVS busy (409, the list)
    - else -> the planned leadership move
    """
    if owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX):
        return PromoteDecision(PROMOTE_IN_PROGRESS)
    source = storage_node_ops.lvs_active_site_of(owner)
    if source == site:
        kind = PROMOTE_ACTIVE if volume_open_on(lvol, owner, site) else PROMOTE_ANA_ONLY
        return PromoteDecision(kind, source)
    if source not in online_sites:
        return PromoteDecision(PROMOTE_DISASTER if force else PROMOTE_SITE_OFFLINE, source)
    if force:
        return PromoteDecision(PROMOTE_FORCE_ONLINE, source)
    if volume_open_on(lvol, owner, source):
        return PromoteDecision(PROMOTE_NOT_DEMOTED, source, (lvol.get_id(),))
    busy = volumes_open_on(lvs_volumes, owner, source)
    if busy:
        return PromoteDecision(PROMOTE_LVS_BUSY, source, tuple(busy))
    return PromoteDecision(PROMOTE_MOVE, source)


def promote_move_problems(cluster, owner: StorageNode, volumes: Iterable[LVol], expect: str,
                          target_site: str) -> list[str]:
    """What forbids marking ``owner``'s LVS as moving to ``target_site`` now,
    judged inside the promote's transaction (DBController.begin_sync_promote_moves)
    from the DB alone: a lost site, ``lvs_active_site`` no longer ``expect``,
    a volume of the LVS still served on the site it is led from."""
    lvs = owner.lvstore
    problems = []
    if cluster.lost_site:
        problems.append(f"site {cluster.lost_site} is lost; a planned promote needs both sites")
    if owner.lvs_active_site != expect:
        problems.append(f"LVS {lvs}: lvs_active_site is {owner.lvs_active_site!r}, "
                        f"expected {expect!r}")
        return problems
    source = expect or owner.site
    if source == target_site:
        problems.append(f"LVS {lvs} is already led from site {target_site}")
    busy = volumes_open_on(volumes, owner, source)
    if busy:
        problems.append(f"LVS {lvs}: volumes still active on site {source}: {busy}")
    return problems


def online_sites(db: DBController, cluster) -> set[str]:
    """The sites with an online storage node, the cluster's lost site
    excepted."""
    return {n.site for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())
            if n.site and n.status == StorageNode.STATUS_ONLINE and n.site != cluster.lost_site}


#: Node states in which SPDK may still run: a site with such a node is not
#: lost, whatever its other nodes say (DOWN: only the client port is blocked).
SITE_RUNNING_STATES = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_CREATION)


def site_running_nodes(nodes: Iterable[StorageNode], site: str) -> list[str]:
    """The nodes of ``site`` whose status says SPDK may still run there
    (SITE_RUNNING_STATES), as ``id (status)``."""
    return [f"{n.get_id()} ({n.status})" for n in nodes
            if n.site == site and n.status in SITE_RUNNING_STATES]


def disaster_move_problems(cluster, owner: StorageNode, volumes: Iterable[LVol], expect: str,
                           target_site: str, lost_site: str, request_ids: Iterable[str]) -> list[str]:
    """What forbids marking ``owner``'s LVS as moving to ``target_site`` in a
    disaster fail-over of ``lost_site``, judged inside the promote's
    transaction from the DB alone: the site steps not recorded as done,
    ``lvs_active_site`` no longer ``expect``, the LVS not led from the lost
    site, a volume of it still served there that the request does not move
    (every other one was fenced by the site steps - one appearing now was
    created or reopened since)."""
    lvs = owner.lvstore
    problems = []
    if cluster.lost_site != lost_site or cluster.lost_site_state != LOST_SITE_DONE:
        problems.append(f"the fence of site {lost_site} is not complete (lost_site="
                        f"{cluster.lost_site!r}, state={cluster.lost_site_state!r})")
    if owner.lvs_active_site != expect:
        problems.append(f"LVS {lvs}: lvs_active_site is {owner.lvs_active_site!r}, "
                        f"expected {expect!r}")
        return problems
    source = expect or owner.site
    if source != lost_site:
        problems.append(f"LVS {lvs} is led from site {source}, not from the lost site {lost_site}")
    if source == target_site:
        problems.append(f"LVS {lvs} is already led from site {target_site}")
    requested = set(request_ids)
    busy = [vid for vid in volumes_open_on(volumes, owner, source) if vid not in requested]
    if busy:
        problems.append(f"LVS {lvs}: volumes still active on site {source}: {busy}")
    return problems


#: ``Cluster.lost_site_state`` values (Technical Details): the site steps of a
#: disaster fail-over are running (a retry redoes them), or they are complete.
LOST_SITE_FENCING = "fencing"
LOST_SITE_DONE = "done"


def demote_site_volumes(db: DBController, cluster_id: str, lost_site: str,
                        exclude_ids: Iterable[str]) -> list[str]:
    """Site step of a disaster fail-over: every volume of every LVS that may be
    led from ``lost_site`` (lvs_active_on, a move in flight included) gets the
    lost site in ``sync_demoted_sites`` - fenced there for good, still closed
    on the surviving site until its own promote - except ``exclude_ids`` (the
    request's). Under the LVS's site-rule lock, field-scoped, idempotent.
    Returns the ids newly demoted."""
    exclude = set(exclude_ids)
    owners = [o for o in _lvs_owners(db.get_storage_nodes_by_cluster_id(cluster_id))
              if lvs_active_on(o, lost_site)]
    demoted: list[str] = []
    if not owners:
        return demoted
    with storage_node_ops.sync_site_rule_locks(cluster_id, {o.lvstore for o in owners}):
        for owner in owners:
            owner = db.get_storage_node_by_id(owner.get_id())
            if not lvs_active_on(owner, lost_site):
                continue
            for lvol in db.get_lvols_by_node_id(owner.get_id()):
                if (lvol.status == LVol.STATUS_DELETED or lvol.get_id() in exclude
                        or lost_site in lvol.sync_demoted_sites):
                    continue

                def _demoted(v):
                    if lost_site in v.sync_demoted_sites:
                        return False
                    v.sync_demoted_sites = [*v.sync_demoted_sites, lost_site]
                    return True
                db.atomic_update(lvol, _demoted)
                demoted.append(lvol.get_id())
    if demoted:
        logger.warning("Site %s lost: volumes fenced there: %s", lost_site, demoted)
    return demoted


@dataclass(frozen=True)
class SyncPromoteResult:
    """The answer to a promote call: still in progress (the promote task, or a
    leadership move in flight) or done, with the connection entries of every
    volume on the promoted site (``{volume id: connect_lvol entries}``)."""
    in_progress: bool
    task_id: str = ""
    connection_strings: dict | None = None


_PROMOTE_REFUSALS = {
    PROMOTE_FORCE_ONLINE: "a forced promote acts only on a lost site; site {source} is online",
    PROMOTE_NOT_DEMOTED: "volume(s) not demoted on site {source}",
    PROMOTE_LVS_BUSY: "other volumes of the LVS are still active on site {source}",
}


def _connection_strings(volumes: list[LVol], site: str) -> dict:
    from simplyblock_core.controllers import lvol_controller
    out = {}
    for lvol in volumes:
        entries, err = lvol_controller.connect_lvol(lvol.get_id(), site=site)
        if entries is False:
            raise SyncPromoteRefusedError(f"volume {lvol.get_id()}: {err}", [lvol.get_id()])
        out[lvol.get_id()] = entries
    return out


def _promote(db: DBController, volumes: list[LVol], site: str, force: bool) -> SyncPromoteResult:
    cluster, owners = _volumes_context(db, volumes, site)
    cluster_id = cluster.get_id()
    for lvs_name in sorted({lv.lvs_name for lv in volumes}):
        task = db.get_sync_promote_task(cluster_id, lvs_name)
        if task is not None and db.sync_promote_task_blocks(task):
            return SyncPromoteResult(in_progress=True, task_id=task.uuid)
    sites = online_sites(db, cluster)
    lvs_volumes = {owner_id: db.get_lvols_by_node_id(owner_id) for owner_id in owners}
    decisions = [(lv, promote_decision(lv, owners[lv.node_id], lvs_volumes[lv.node_id], site,
                                       online_sites=sites, force=force))
                 for lv in volumes]
    kinds = {d.kind for _, d in decisions}
    if PROMOTE_IN_PROGRESS in kinds:
        return SyncPromoteResult(in_progress=True)
    first: dict[str, PromoteDecision] = {}
    for _, decision in decisions:
        first.setdefault(decision.kind, decision)
    if PROMOTE_SITE_OFFLINE in kinds:
        raise SyncSiteOfflineError(
            f"site {first[PROMOTE_SITE_OFFLINE].source_site} is not online; only a forced "
            f"promote may fail it over")
    for kind, message in _PROMOTE_REFUSALS.items():
        if kind in kinds:
            blocking = sorted({vid for _, d in decisions if d.kind == kind for vid in d.blocking})
            raise SyncPromoteRefusedError(
                message.format(source=first[kind].source_site)
                + (f": {blocking}" if blocking else ""), blocking)
    lost = _lost_site_of_request(db, cluster, site, {d.source_site for _, d in decisions
                                                     if d.kind == PROMOTE_DISASTER})
    queued = [lv for lv, d in decisions if d.kind in (*PROMOTE_QUEUED, PROMOTE_DISASTER)]
    if not queued:
        return SyncPromoteResult(in_progress=False, connection_strings=_connection_strings(volumes, site))
    if lost:
        check_disaster_gate(cluster_id, lost, {lv.lvs_name for lv in queued})
    else:
        check_gate(cluster_id)
    task_owners = {lv.lvs_name: lv.node_id for lv in queued}
    task_id, _ = tasks_controller.add_sync_promote_task(
        cluster_id, queued[0].node_id, site=site, lvol_ids=[lv.get_id() for lv in queued],
        owners=task_owners, lost_site=lost)
    return SyncPromoteResult(in_progress=True, task_id=task_id)


def _lost_site_of_request(db: DBController, cluster, site: str, disaster_sources: set[str]) -> str:
    """The lost site a promote to ``site`` is judged against: the source of
    its disaster rows (a forced promote of a site that is not online), else
    the cluster's recorded lost site - with a site lost the planned gate (live,
    both sites) can never pass, and opening a volume of an LVS already led from
    the surviving site is judged by the disaster gate, which judges only what
    is still active on the lost site. "" for a planned promote.

    Refuses (SyncPromoteRefusedError) a disaster of the promoted site itself
    or while another site is already recorded as lost, and - before the site
    is recorded as lost - while a node of it may still run SPDK
    (site_running_nodes): force never acts on a live site. The runner proves
    the loss (liveness evidence) before it changes anything."""
    if len(disaster_sources) > 1:
        raise SyncPromoteRefusedError(f"a forced promote can fail over one site, not "
                                      f"{sorted(disaster_sources)}")
    lost = next(iter(disaster_sources), "") or cluster.lost_site
    if not lost:
        return ""
    if lost == site:
        raise SyncPromoteRefusedError(f"site {site} is the lost site; it cannot be promoted")
    if cluster.lost_site and cluster.lost_site != lost:
        raise SyncPromoteRefusedError(f"site {cluster.lost_site} is already lost; site {lost} "
                                      f"cannot be failed over too")
    if cluster.lost_site != lost:
        running = site_running_nodes(db.get_storage_nodes_by_cluster_id(cluster.get_id()), lost)
        if running:
            raise SyncPromoteRefusedError(
                f"a forced promote acts only on a lost site; nodes of site {lost} may still "
                f"run: {running}")
    return lost


def sync_promote_lvol(lvol_id: str, site: str, force: bool = False) -> SyncPromoteResult:
    """Promote volume ``lvol_id`` on ``site`` (promote_decision): already
    served there -> done with its connection entries on ``site``; a planned
    move or only the ANA step -> the planned gate (live), then the
    FN_SYNC_PROMOTE task is queued (tasks_runner_sync_promote) and the answer
    is "in progress" until it has finished - the call after it answers done.

    A forced promote of a volume whose LVS is led from a site that is not
    online is a disaster fail-over (_lost_site_of_request): refused while a
    node of that site may still run, else judged by the disaster gate of that
    site (persisted events of its nodes) and queued with ``lost_site``; the
    runner fences the site before it moves anything. While a site is
    recorded as lost every promote is judged by its disaster gate.

    Raises SyncGateError, SyncPromoteRefusedError (409 rows), SyncSiteOfflineError
    (412), SyncReplicationUnsupportedError (no sync cluster), SyncReplicationSiteError."""
    db = DBController()
    return _promote(db, [db.get_lvol_by_id(lvol_id)], site, force)


def sync_promote_group(group_id: str, site: str, force: bool = False) -> SyncPromoteResult:
    """sync_promote_lvol over every member of consistency group
    ``group_id``, as one promote: the LVS rule holds over the union of their
    LVS, one task moves them all, and the group is done only when every member
    is served on ``site``. SyncGroupMemberError when a member cannot be
    resolved."""
    db = DBController()
    return _promote(db, _group_volumes(db, group_id), site, force)
