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
CAPABILITIES: frozenset[str] = frozenset({"info", "discard_landing", "describe", "prepare_landing"})

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
