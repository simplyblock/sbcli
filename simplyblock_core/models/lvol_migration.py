from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import List
import storage_node
from base_model import *
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
    rpc_client1: RPCClient = None
    rpc_client2: RPCClient = None
    rpc_client3: RPCClient = None
    rpc_client4: RPCClient = None

    # Global snapshot objects (shared across streams)
    snapshots: List[Snapshot] = None
    # Async queue for polling migration completion (set externally)
    completion_poll_queue: List[Snapshot] = None
    rerun : int = 0


