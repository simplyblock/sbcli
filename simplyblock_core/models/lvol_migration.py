from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import List
import storage_node
from base_model import *
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.rpc_client import RPCClient


# ---------------------------------------------------------------------------
# ENUMS
# ---------------------------------------------------------------------------

class MigrationState(str, Enum):
    NEW = 0
    PREPARING = 1
    RUNNING = 2
    SNAPS_MIGRATED = 3
    TARGET_LVOL_CREATED = 4
    HUBLVOL_CONNECTED = 5
    TRANSFERRED_TO_TARGET = 6
    RECONNECT_DONE = 6
    CLEANUP = 7
    FAILED = 8
    FAILED_AND_CLEANED = 9
    DONE = 10

class ObjectMigrationState(str, Enum):
    NEW = 1
    LVOL_CREATED = 2
    MIG_FLAG_SET = 3
    NAMESPACE_CREATED = 4
    NQN_CREATED = 5
    LVOL_CONNECTED = 6
    LVOL_EXPORTED = 7
    TRANSFER = 8
    RETRANSFER = 9
    TRANSFERRED = 10
    CONVERTED = 11
    CLEANING = 12
    FAILED = 13
    DONE = 14

# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class LogicalVolumeRef:
    """Reference to a logical volume participating in a migration."""
    lvol: LVol = None
    target_uuid: str = ""
    mapid: int = 0
    state : ObjectMigrationState = ObjectMigrationState.NEW
    retry : int = 0

@dataclass
class Snapshot:
    snap: SnapShot = None
    lvol: LogicalVolumeRef = None
    controller: str = ""
    target_uuid: str = ""
    retry : int = 0
    # Migration metadata
    temporary_nqn: str = ""
    status: ObjectMigrationState = ObjectMigrationState.NEW

@dataclass
class MigrationObject(BaseModel):
    """
    Full migration object, containing multiple streams and logical volumes.
    Snapshots exist independently and are referenced by streams.
    """
    status: MigrationState = MigrationState.NEW
    pre_status: MigrationState = MigrationState.NEW

    vol : LogicalVolumeRef = None
    node_pri : storage_node.StorageNode = None
    node_sec: storage_node.StorageNode = None
    target_node_pri: storage_node.StorageNode = None
    target_node_sec: storage_node.StorageNode = None
    rpc_client1: RPCClient = None
    rpc_client2: RPCClient = None
    rpc_client3: RPCClient = None
    rpc_client4: RPCClient = None

    # Global snapshot objects (shared across streams)
    snapshots: List[Snapshot] = None
    # Async queue for polling migration completion (set externally)
    completion_poll_queue: List[Snapshot] = None
    rerun : int = 0


