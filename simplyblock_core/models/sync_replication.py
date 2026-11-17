from typing import ClassVar

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.indices import Index


class SyncReplicationEvent(BaseModel):
    """A sync-replication state change of one LVS, as reported by the data plane.

    Recorded by the distr event collector from the ``sync_replication_status``
    events of the distribs (zone state) and of the journal (remote journal
    state). The disaster fail-over gate judges an LVS from these persisted
    records rather than from a live query, since the site it is judged for is
    gone by then. ``status`` (inherited) holds the raw data-plane event status,
    ``timestamp_utc`` the event's own ISO-8601 UTC timestamp.

    Records are written only through
    ``DBController.record_sync_replication_event``: it assigns ``receive_seq``
    in the same transaction as the write, over a fixed-size per-LVS state key,
    so the sequence is the commit order and "the latest event of an LVS" never
    depends on the producer nodes' clocks. What is over for the status and the
    bookkeeping is decided by that key's watermarks (a later
    ``remote_journal_restored`` of any node, a verified catch-up,
    ``DBController.finish_sync_resync``); ``resolved`` follows them.

    Commit order is production order only per node: the nodes' collectors
    run independently, so across nodes it is not causal. The disaster gate
    therefore does not use the LVS-wide journal watermark nor ``resolved``
    for a remote-journal drop: a drop is over only by a later restore of its
    own node or a later ``observed_live`` restore
    (sync_replication_controller.latest_journal_drop).
    """

    _INDEXES: ClassVar[tuple] = (
        # Prefix (cluster_id, lvs_name) = every event of an LVS; the full
        # tuple with resolved=False = its open ones.
        Index(('cluster_id', 'lvs_name', 'resolved')),
    )

    KIND_ZONE_UNAVAILABLE = "zone_unavailable"
    KIND_REMOTE_JOURNAL_DROPPED = "remote_journal_dropped"
    KIND_REMOTE_JOURNAL_RESTORED = "remote_journal_restored"

    cluster_id: str = ""
    lvs_name: str = ""
    #: The node whose instance of the LVS emitted the event.
    node_id: str = ""
    kind: str = ""
    timestamp_utc: str = ""
    resolved: bool = False
    #: Receive order within the LVS, assigned at the write: higher =
    #: committed later. 0 only on records older than the field.
    receive_seq: int = 0
    #: A ``remote_journal_restored`` sbcli observed itself (a live
    #: ``remote_journal_in_sync: true`` of the JC leader), recorded only if no
    #: event of the LVS was received since the query started - so it is
    #: causally after every event received before it, whichever node sent
    #: them. A collected restore (False) is only known to follow its own
    #: node's earlier events (one collector per node).
    observed_live: bool = False
