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

    uuid: str
    bdev_name: str  # "LVS/LV"
    lvs_name: str
    target_lvs_name: str
    source_uuid: str
    target_uuid: str
    namespace_id: str
    nqn : str
    node_id: str
    sec_node_id :str
    target_node_id : str
    target_sec_node_id : str
    mapid: str
    cloned : str
    state : ObjectMigrationState
    crypto_bdev_name: Optional[str] = None

@dataclass
class Snapshot:
    """
    Global snapshot object, exists only once.
    Stores all per-snapshot migration metadata.
    """
    uuid : str
    bdev_name: str  # "LVS/LV"
    lvs_name: str
    target_lvs_name : str
    source_uuid : str
    target_uuid : Optional[str] = None

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

    # Global snapshot objects (shared across streams)
    snapshots: List[SnapshotRef]
    # Async queue for polling migration completion (set externally)
    completion_poll_queue: Optional[asyncio.Queue] = None

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: MigrationState = MigrationState.NEW


