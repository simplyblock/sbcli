from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
import uuid
import asyncio


# ---------------------------------------------------------------------------
# ENUMS
# ---------------------------------------------------------------------------

class MigrationState(str, Enum):
    NEW = "new"
    PREPARING = "preparing"
    RUNNING = "running"
    SUSPENDED = "suspended"
    FAILED = "failed"
    PARTIALLY_FAILED = "partially_failed"
    DONE = "done"


class StreamState(str, Enum):
    NEW = "new"
    RUNNING = "running"
    SUSPENDED = "suspended"
    FAILED = "failed"
    CLEANUP = "cleanup"
    DONE = "done"


class ObjectMigrationState(str, Enum):
    NEW = "new"
    RUNNING = "running"
    SUSPENDED = "suspended"
    CANCELED = "canceled"
    DONE = "done"


# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class LogicalVolumeRef:
    """Reference to a logical volume participating in a migration."""
    name: str  # "LVS/LV"
    namespace_uuid: str
    crypto_bdev_name: Optional[str] = None


@dataclass
class Snapshot:
    """
    Global snapshot object, exists only once.
    Stores all per-snapshot migration metadata.
    """
    name: str  # "LVS/LV"
    source_uuid: Optional[str] = None
    target_uuid: Optional[str] = None

    # Migration metadata
    temporary_nqn: Optional[str] = None
    temporary_namespace: Optional[str] = None
    mapid: Optional[str] = None
    status: ObjectMigrationState = ObjectMigrationState.NEW


@dataclass
class SnapshotRef:
    """Per-stream linked list node referencing a global snapshot."""
    snapshot: Snapshot
    next: Optional["SnapshotRef"] = None


@dataclass
class MigrationStream:
    """
    Each migration stream corresponds to one logical volume.
    Contains a linked list of snapshot references.
    Tracks only LV migration state and metadata.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: StreamState = StreamState.NEW

    # Logical volume info and per-LV migration metadata
    lvol_name: Optional[str] = None
    lvol_state: ObjectMigrationState = ObjectMigrationState.NEW
    lvol_namespace: Optional[str] = None
    lvol_nqn: Optional[str] = None
    lvol_source_uuid: Optional[str] = None
    lvol_target_uuid: Optional[str] = None

    # Linked list of snapshot references (per-stream)
    head_snapshot_ref: Optional[SnapshotRef] = None

    def append_snapshot(self, snapshot: Snapshot):
        """Append a snapshot reference to the stream linked list."""
        ref = SnapshotRef(snapshot=snapshot)
        if not self.head_snapshot_ref:
            self.head_snapshot_ref = ref
            return ref
        cur = self.head_snapshot_ref
        while cur.next:
            cur = cur.next
        cur.next = ref
        return ref

    def list_snapshot_names(self) -> List[str]:
        """Return list of snapshot names in this stream."""
        names = []
        cur = self.head_snapshot_ref
        while cur:
            names.append(cur.snapshot.name)
            cur = cur.next
        return names


@dataclass
class MigrationObject:
    """
    Full migration object, containing multiple streams and logical volumes.
    Snapshots exist independently and are referenced by streams.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: MigrationState = MigrationState.NEW

    primary_source: Optional[str] = None
    secondary_source: Optional[str] = None
    primary_target: Optional[str] = None
    secondary_target: Optional[str] = None

    logical_volumes: List[LogicalVolumeRef] = field(default_factory=list)

    # Top-level subsystem NQN (if any)
    nqn: Optional[str] = None

    streams: List[MigrationStream] = field(default_factory=list)

    # Global snapshot objects (shared across streams)
    snapshots: List[Snapshot] = field(default_factory=list)

    # Async queue for polling migration completion (set externally)
    completion_poll_queue: Optional[asyncio.Queue] = None


