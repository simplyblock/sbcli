from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
import uuid
import asyncio
import storage_node


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
    CANCELED = "failed"
    DONE = "done"


# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class LogicalVolumeRef:
    """Reference to a logical volume participating in a migration."""
    name: str  # "LVS/LV"
    uuid: str
    namespace_id: str
    nqn : str
    node_id: str
    sec_node_id :str
    target_node_id : str
    target_sec_node_id : str
    mapid: str
    target_uuid: str
    cloned : str
    state : ObjectMigrationState
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
    volume : LogicalVolumeRef
    # Linked list of snapshot references (per-stream)
    head_snapshot_ref: Optional[SnapshotRef] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: StreamState = StreamState.NEW

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

    main_logical_volume : LogicalVolumeRef
    node_pri : storage_node.StorageNode
    node_sec: storage_node.StorageNode
    target_node_pri: storage_node.StorageNode
    target_node_sec: storage_node.StorageNode

    clones: List[LogicalVolumeRef]
    streams: List[MigrationStream]
    # Global snapshot objects (shared across streams)
    snapshots: List[Snapshot]
    # Async queue for polling migration completion (set externally)
    completion_poll_queue: Optional[asyncio.Queue] = None

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: MigrationState = MigrationState.NEW


