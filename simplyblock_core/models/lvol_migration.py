from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
import uuid
import asyncio
import storage_node
from base_model import *


# ---------------------------------------------------------------------------
# ENUMS
# ---------------------------------------------------------------------------

class MigrationState(str, Enum):
    NEW = "new"
    PREPARING = "preparing"
    RUNNING = "running"
    SNAPS_MIGRATED = "migrated"
    TARGET_LVOL_CREATED = "target_lvol_created"
    HUBLVOL_CONNECTED = "hublvol_connecte"
    TRANSFERRED_TO_TARGET = "transferred_to_target"
    RECONNECT_DONE = "reconnect_done"
    CLEANUP = "cleanup"
    FAILED = "failed"
    FAILED_AND_CLEANED = "failed_and_cleaned"
    DONE = "done"

class ObjectMigrationState(str, Enum):
    NEW = "new"
    LVOL_CREATED = "lvolcreated"
    MIG_FLAG_SET = "migflagset"
    NAMESPACE_CREATED = "nscreated"
    NQN_CREATED = "nqncreated"
    LVOL_CONNECTED = "lvolconnected"
    LVOL_EXPORTED = "lvol_exported"
    TRANSFER = "transferring"
    RETRANSFER = "retransfer"
    TRANSFERRED = "transferred"
    CONVERTED = "converted"
    CLEANING = "cleaning"
    FAILED = "failed"
    DONE = "done"

# ---------------------------------------------------------------------------
# DATA MODELS
# ---------------------------------------------------------------------------

@dataclass
class LogicalVolumeRef:
    """Reference to a logical volume participating in a migration."""
    uuid: str = ""
    bdev_name: str = ""  # "LVS/LV"
    lvs_name: str = ""
    target_lvs_name: str = ""
    source_uuid: str = ""
    target_uuid: str = ""
    namespace_id: str = ""
    nqn : str = ""
    node_id: str = ""
    sec_node_id :str =""
    target_node_id : str = ""
    target_sec_node_id : str = ""
    ndcs : int = 1
    npcs : int = 1
    priority_class : int = 0
    size : int = 0
    mapid: int = 0
    cloned : str = ""
    state : ObjectMigrationState = ObjectMigrationState.NEW
    retry : int = 0
    crypto_bdev_name: str = ""

@dataclass
class Snapshot:
    uuid : str =""
    bdev_name: str = "" # "LVS/LV"
    lvs_name: str = ""
    size: int = 0
    target_lvs_name : str = ""
    source_uuid : str = ""
    target_uuid : str = ""
    retry : int = 0
    # Migration metadata
    temporary_nqn: str = ""
    temporary_namespace: str = ""
    mapid: int = 0
    status: ObjectMigrationState = ObjectMigrationState.NEW

@dataclass
class MigrationObject(BaseModel):
    """
    Full migration object, containing multiple streams and logical volumes.
    Snapshots exist independently and are referenced by streams.
    """
    status: MigrationState = MigrationState.NEW
    pre_status: MigrationState = MigrationState.NEW

    main_logical_volume : LogicalVolumeRef = None
    node_pri : storage_node.StorageNode = None
    node_sec: storage_node.StorageNode = None
    target_node_pri: storage_node.StorageNode = None
    target_node_sec: storage_node.StorageNode = None

    # Global snapshot objects (shared across streams)
    snapshots: List[Snapshot] = None
    # Async queue for polling migration completion (set externally)
    completion_poll_queue: List[Snapshot] = None
    rerun : int = 0


