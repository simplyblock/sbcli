"""Site-level synchronous replication: the control plane's record of the data
plane's sync state.

The distr event collector hands every ``sync_replication_status`` event to
:func:`record_sync_event`, which maps it to its LVS and persists it as a
``SyncReplicationEvent`` (the history the disaster fail-over gate judges an
LVS by), and schedules the zone catch-up (``FN_SYNC_RESYNC``) on a zone
desync. Event format: ultra
``DISTR_v2/src_code_app_spdk/specs/message_format_rpcs__distrib__v5.txt``
(``distr_status_events_get``).
"""
import uuid
from collections.abc import Iterable
from typing import NamedTuple

from simplyblock_core import distr_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent

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
