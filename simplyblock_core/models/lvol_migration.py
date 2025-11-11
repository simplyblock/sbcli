from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


class MigrationState(Enum):
    NEW = "new"
    IN_MIGRATION = "in-migration"
    MIGRATED = "migrated"


@dataclass
class StorageObject:
    """Represents an lvol/clone/snapshot or similar."""
    id: str
    lvol_ref: LVol
    snap_ref: SnapShot
    type: str  # e.g., "lvol", "clone", "snapshot"


@dataclass
class MigrationItem:
    """A single storage-object migration entry."""
    storage: StorageObject
    state: MigrationState = MigrationState.NEW

@dataclass
class LvolMigration:
    """Model representing a full logical-volume migration plan."""
    primary_source: StorageNode
    secondary_source: StorageNode
    primary_target: StorageNode
    secondary_target: StorageNode
    migrations: List[MigrationItem] = field(default_factory=list)

    def add_migration(self, storage: StorageObject) -> None:
        self.migrations.append(MigrationItem(storage))

    def update_state(self, storage_id: str, new_state: MigrationState) -> None:
        for item in self.migrations:
            if item.storage.id == storage_id:
                item.state = new_state
                return
        raise ValueError(f"No migration item with storage id={storage_id}")
