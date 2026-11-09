"""Shapes of the peer API, shared by the serving and the calling control plane.

These are the wire contract rather than views of a core model, so they live here
and both ``simplyblock_web.api.v2.peer`` and ``simplyblock_core.peer.client`` use
them, instead of a DTO in ``_dtos.py`` that the client side could not import.
"""
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, Field

#: Bumped on an incompatible change to any peer endpoint. A caller refuses a
#: peer whose version it does not speak instead of guessing.
PEER_API_VERSION = 1

#: Peer endpoints this control plane serves, named so a caller can check for a
#: step before relying on it during a rolling upgrade of the two sites.
CAPABILITIES: frozenset[str] = frozenset({"info", "discard_landing"})

#: Name prefix of a replication landing volume (``REP_<snapshot name>``). It is
#: the only mark a landing volume carries in the database, and the peer API acts
#: only on volumes that carry it.
LANDING_PREFIX = "REP_"


class PeerInfo(BaseModel):
    """What a peer control plane reports about one of its clusters."""

    cluster_id: UUID
    api_version: Annotated[int, Field(ge=1)]
    capabilities: list[str]
