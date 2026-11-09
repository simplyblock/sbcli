"""Shapes of the peer API, shared by the serving and the calling control plane.

These are the wire contract rather than views of a core model, so they live here
and both ``simplyblock_web.api.v2.peer`` and ``simplyblock_core.peer.client`` use
them, instead of a DTO in ``_dtos.py`` that the client side could not import.
"""
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field

#: Bumped on an incompatible change to any peer endpoint. A caller refuses a
#: peer whose version it does not speak instead of guessing.
PEER_API_VERSION = 1

#: Peer endpoints this control plane serves, named so a caller can check for a
#: step before relying on it during a rolling upgrade of the two sites.
CAPABILITIES: frozenset[str] = frozenset({
    "info", "discard_landing", "describe", "prepare_landing", "ensure_hub", "chain", "finish",
})

#: Name prefix of a replication landing volume (``REP_<snapshot name>``). It is
#: the only mark a landing volume carries in the database, and the peer API acts
#: only on volumes that carry it.
LANDING_PREFIX = "REP_"


class PeerInfo(BaseModel):
    """What a peer control plane reports about one of its clusters."""

    cluster_id: UUID
    api_version: Annotated[int, Field(ge=1)]
    capabilities: list[str]


class ChainCheck(BaseModel):
    """Is the copy *successor_id* chained onto the copy *predecessor_id*?"""

    successor_id: str
    predecessor_id: str


class DescribeRequest(BaseModel):
    """Reads about the target side that the source needs mid-transfer and for retention.

    Every part is optional, so a caller asks only for what it needs: the transfer
    status check sends just ``landing_lvol_id`` and stays cheap.
    """

    #: Report which members of this landing volume's lvstore claim leadership.
    landing_lvol_id: str | None = None
    #: Replicated copies to report on.
    snapshot_ids: list[str] = Field(default_factory=list)
    #: Chain links to verify, by the database link or else by SPDK.
    chain_checks: list[ChainCheck] = Field(default_factory=list)


class SnapshotState(BaseModel):
    """One replicated copy as the target sees it."""

    snapshot_id: str
    exists: bool
    #: A live volume is cloned from the copy (e.g. a failed-over volume), so it
    #: must not be deleted.
    has_dependent_clone: bool = False


class ChainState(ChainCheck):
    chained: bool


class DescribeResponse(BaseModel):
    #: Ids of the online members reporting leadership of the landing volume's
    #: lvstore; None when the landing volume is gone or none was asked for.
    landing_leaders: list[str] | None = None
    snapshots: list[SnapshotState] = Field(default_factory=list)
    chains: list[ChainState] = Field(default_factory=list)


class LandingRequest(BaseModel):
    """Make a replicated snapshot's landing volume ready on the target.

    Retry key: (``source_cluster_id``, ``snapshot_id``), with the landing
    volume named after the snapshot (``REP_<name>``). A repeated request adopts
    the volume the first one made instead of making a second.
    """

    source_cluster_id: str
    #: The source snapshot. Never itself a counterpart, which matters while both
    #: clusters share one database.
    snapshot_id: str
    landing_name: Annotated[str, Field(pattern=r"^REP_.+")]
    size: Annotated[int, Field(ge=0)]
    ha_type: str
    max_namespace_per_subsys: Annotated[int, Field(ge=0)] = 0
    #: The data identity copies share; empty skips the counterpart search.
    data_uuid: str = ""
    #: The member to create the volume on; empty lets the target pick one.
    node_hint: str = ""
    #: Pools to try in order. The target takes the first one of its own, else
    #: its first active pool.
    pool_hints: list[str] = Field(default_factory=list)
    #: A copy whose pool the landing volume should share (fail-back direction).
    counterpart_hint: str = ""
    #: Only create: report a volume already wearing the name as ``exists`` and
    #: leave it alone (pre-staging a cutover's next round).
    create_only: bool = False


class LandingResponse(BaseModel):
    #: ready: ``lvol_id`` can receive (``adopted`` when it already existed).
    #: counterpart: the target already holds this data (``counterpart_snapshot_id``);
    #: nothing to transfer. retry: a leftover is being cleared (``reason``).
    #: exists: ``create_only`` found the name taken.
    state: Literal["ready", "counterpart", "retry", "exists"]
    lvol_id: str = ""
    adopted: bool = False
    counterpart_snapshot_id: str = ""
    lvs_name: str = ""
    reason: str = ""


class HubPath(BaseModel):
    """One address the source can attach the receiving hub over."""

    ip: str
    trtype: Literal["TCP", "RDMA"]


class HubAddress(BaseModel):
    """What the source needs to attach the target's receiving hub over NVMe-oF."""

    bdev_name: str
    remote_bdev_name: str
    nqn: str
    port: Annotated[int, Field(ge=0)]
    paths: list[HubPath] = Field(default_factory=list)


class EnsureHubRequest(BaseModel):
    """Get the target ready to receive a transfer into ``landing_lvol_id``."""

    landing_lvol_id: str


class EnsureHubResponse(BaseModel):
    #: ready: transfer into ``map_id`` through ``hub``, sent to ``leader_node_id``.
    #: unsettled: leadership of the landing volume's lvstore is moving, or a
    #: tertiary leads, which cannot receive; wait. not_leader: the leader lost
    #: leadership just now; wait. converted: the landing volume is already a
    #: snapshot on ``converted_on``; discard it and start over. failed: the
    #: target could not get ready (``reason``; retry with ``backoff``).
    state: Literal["ready", "unsettled", "not_leader", "converted", "failed"]
    reason: str = ""
    backoff: bool = True
    leader_node_id: str = ""
    map_id: int | None = None
    hub: HubAddress | None = None
    #: The leader has no secondary, or its secondary is online: a delta can
    #: land on every member of the pair.
    pair_online: bool = False
    converted_on: list[str] = Field(default_factory=list)


class ChainRequest(BaseModel):
    """Chain the landing volume onto the copy of the snapshot before it, on the
    leader and its secondary, so the transfer can ship only the delta.

    Retry key: ``chained_on`` is what the source recorded from earlier answers;
    a member in it is not chained again (add_clone is not idempotent in SPDK).
    """

    landing_lvol_id: str
    leader_node_id: str
    #: The predecessor's copy on this cluster.
    predecessor_id: str
    chained_on: list[str] = Field(default_factory=list)


class ChainResponse(BaseModel):
    #: chained: on every online member of the pair. declined: not (fully)
    #: chained, a full transfer is needed (``reason``). unresolvable: the
    #: predecessor's copy cannot be used here.
    state: Literal["chained", "declined", "unresolvable"]
    #: Every member the landing volume is chained on now, including earlier ones.
    chained_on: list[str] = Field(default_factory=list)
    reason: str = ""


class SourceSnapshot(BaseModel):
    """What the copy's record carries over from the source snapshot."""

    snapshot_id: str
    snap_name: str
    size: Annotated[int, Field(ge=0)]
    used_size: Annotated[int, Field(ge=0)] = 0
    data_uuid: str = ""
    group_id: str = ""
    group_seq: Annotated[int, Field(ge=0)] = 0


class FinishRequest(BaseModel):
    """Make the landing volume the copy of the source snapshot, after its transfer.

    Retry key: the copy's record id is derived from (``source_cluster_id``,
    ``snapshot.snapshot_id``, ``landing_lvol_id``); ``chained_on``,
    ``converted_on`` and ``merged`` are what earlier answers reported.
    """

    source_cluster_id: str
    landing_lvol_id: str
    snapshot: SourceSnapshot
    #: The member the transfer was sent to.
    sent_to: str = ""
    #: The predecessor's copy here, to chain onto; empty: this copy starts the chain.
    predecessor_id: str = ""
    #: The base blob of a delta transfer; non-zero means the volume must be
    #: merged with its base before the convert.
    partial_base: Annotated[int, Field(ge=0)] = 0
    chained_on: list[str] = Field(default_factory=list)
    converted_on: list[str] = Field(default_factory=list)
    merged: bool = False
    #: Which field of the copy names the source snapshot: ``source`` for a
    #: forward copy, ``target`` for a fail-back, ``none`` for an instance copy
    #: (no links, no chain back-link).
    link: Literal["source", "target", "none"]


class FinishResponse(BaseModel):
    #: done: ``copy_id`` is the copy (``record`` its record). pending: wait (a
    #: merge running, or no settled leader: ``waiting_for_leader``). retry:
    #: failed this time (``reason``). restart: the landing volume lost its
    #: delta record; transfer again. gone: the landing volume is gone and no
    #: copy was recorded; start over.
    state: Literal["done", "pending", "retry", "restart", "gone"]
    reason: str = ""
    leader_node_id: str = ""
    waiting_for_leader: bool = False
    chained_on: list[str] = Field(default_factory=list)
    converted_on: list[str] = Field(default_factory=list)
    merged: bool = False
    copy_id: str = ""
    record: dict = Field(default_factory=dict)
