# Ticket description for live lvol migration:
# Live lvol migration moves lvols together with all related objects
# (related snapshots, related clones) from one storage node to another
# storage node in the same cluster. This happens online and very fast,
# as no actual data is copied.
#
# It is NOT possible:
# - to move snapshots or clones independently from the lvol
# - to move namespace lvols belonging to the same subsystem independently
#
# We need to implement this feature in control plane in two steps:
# a) move a specific lvol and its related objects based on the lvol name
#    or uuid from one node to another node. The other node must be online
#    and it must not be the secondary of the node the lvol is currently attached to.
# b) create an automatism, which periodically controls the balance of
#    performance and ram consumption across nodes and re-balances certain
#    lvols if a node becomes over-loaded

import asyncio
from typing import Iterable

from ..models.lvol_migration import (
    LvolMigration,
    MigrationItem,
    MigrationState,
    StorageObject
)
from enum import Enum


class ObjType(Enum):
    SNAPSHOT = "snapshot"
    CLONE = "clone"
    LVOL = "lvol"


class MigrationQueue(LvolMigration):
    def add_object(self, storage_obj: StorageObject, obj_type: ObjType):
        item = MigrationItem(storage=storage_obj, state=MigrationState.NEW)
        item.type = obj_type
        self.migrations.append(item)
        return item

    def iter_snapshots(self):
        return (m for m in self.migrations if m.type == ObjType.SNAPSHOT)

    def iter_clones(self):
        return (m for m in self.migrations if m.type == ObjType.CLONE)

    def iter_lvol(self):
        return (m for m in self.migrations if m.type == ObjType.LVOL)


# -------------------------------------------------------------------------
# Async-capable Controller
# -------------------------------------------------------------------------

class LvolMigrationController:

    # ---------------------------------------------------------------------
    # Public entry point
    # ---------------------------------------------------------------------

    async def migrate_lvol(self, lvol) -> str:
        mq = self.create_migration_queue(lvol)

        if self.all_nodes_online():
            self.freeze_snapshots_clones(lvol)

            result = await self.process_migration_queue(mq)

            if result != "DONE":
                self.register_continue(mq)
                return "SUSPENDED"

            self.unfreeze_snapshots_clones(lvol)
            return "DONE"

        return "SUSPENDED"

    # ---------------------------------------------------------------------

    def create_migration_queue(self, lvol) -> MigrationQueue:
        mq = MigrationQueue(
            primary_source=lvol.primary,
            secondary_source=lvol.secondary,
            primary_target=lvol.target_primary,
            secondary_target=lvol.target_secondary,
        )

        for s in lvol.get_snapshots():
            mq.add_object(s, ObjType.SNAPSHOT)

        for c in lvol.get_clones():
            mq.add_object(c, ObjType.CLONE)

        mq.add_object(lvol, ObjType.LVOL)
        return mq

    # ---------------------------------------------------------------------
    # Core logic with asyncio
    # ---------------------------------------------------------------------

    async def process_migration_queue(self, mq: MigrationQueue) -> str:

        if not await self._process_subset(mq, mq.iter_snapshots()):
            return "CANCELED"

        if not await self._process_subset(mq, mq.iter_clones()):
            return "CANCELED"

        if not await self._process_subset(mq, mq.iter_lvol()):
            return "CANCELED"

        return "DONE"

    # ---------------------------------------------------------------------

    async def _process_subset(self, mq: MigrationQueue, iterator: Iterable[MigrationItem]) -> bool:
        tasks = []

        for item in iterator:
            if item.state in (MigrationState.NEW, MigrationState.IN_MIGRATION):
                item.state = MigrationState.IN_MIGRATION
                tasks.append(asyncio.create_task(self.migrate_object(item)))

        if not tasks:
            return True

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if not self.all_nodes_online():
            return False

        # Check for errors
        for r in results:
            if isinstance(r, Exception):
                return False
            if getattr(r, "failed", False):
                return False

        # Mark items done
        for item in iterator:
            if item.state == MigrationState.IN_MIGRATION:
                item.state = MigrationState.MIGRATED

        return True

    # ---------------------------------------------------------------------
    # Cleanup
    # ---------------------------------------------------------------------

    async def cleanup_migration_queue(self, mq: MigrationQueue, lvol):
        mq.status = "IN_DELETION"

        tasks = []
        for item in mq.migrations:
            if item.state != MigrationState.NEW:
                tasks.append(asyncio.create_task(
                    self.async_delete(item.storage, mq.primary_target)
                ))
                tasks.append(asyncio.create_task(
                    self.register_syn_delete(item.storage, mq.secondary_target)
                ))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        mq.status = "DELETED"
        self.unfreeze_snapshots_clones(lvol)

    # ---------------------------------------------------------------------
    # Individual migration operations (async)
    # ---------------------------------------------------------------------

    async def migrate_object(self, item: MigrationItem):
        src = item.storage.source_node
        dst = item.storage.target_node

        await self.create_target_namespace(dst, item.storage)
        await self.connect_source_to_target(src, dst, item.storage)

        if item.type == ObjType.SNAPSHOT:
            return await self._migrate_snapshot(item)
        elif item.type == ObjType.CLONE:
            return await self._migrate_clone(item)
        else:
            return await self._migrate_lvol(item)

    # ---------------------------------------------------------------------
    # Snapshot migration with retries
    # ---------------------------------------------------------------------

    async def _migrate_snapshot(self, item):
        for attempt in range(5):
            result = await self.run_migration_rpc(item)
            if result.success:
                return result

            if not self.all_nodes_online():
                break

            await asyncio.sleep(self.retry_delay(attempt))

        return result

    async def _migrate_clone(self, item):
        return await self.run_migration_rpc(item)

    async def _migrate_lvol(self, item):
        return await self.run_migration_rpc(item)

    # ---------------------------------------------------------------------
    # Placeholder hooks (inject actual system implementation)
    # ---------------------------------------------------------------------

    async def create_target_namespace(self, dst, storage):
        pass

    async def connect_source_to_target(self, src, dst, storage):
        pass

    async def run_migration_rpc(self, item):
        """
        Must return an object with fields:
            .success  -> bool
            .failed   -> bool
        or raise exception.
        """
        pass

    async def async_delete(self, storage, target):
        pass

    async def register_syn_delete(self, storage, target):
        pass

    def freeze_snapshots_clones(self, lvol):
        pass

    def unfreeze_snapshots_clones(self, lvol):
        pass

    def all_nodes_online(self) -> bool:
        pass

    def register_continue(self, mq):
        pass

    def retry_delay(self, attempt: int) -> float:
        return min(2 ** attempt, 60)   # exponential backoff
