import asyncio
from typing import Optional

from ..models.lvol_migration import (
    MigrationObject, MigrationStream, Snapshot,
    MigrationState, StreamState, ObjectMigrationState
)

# ---------------------------------------------------------------------------
# CONTROLLER
# ---------------------------------------------------------------------------

class MigrationController:

    def __init__(self, migration: MigrationObject):
        self.migration = migration
        if self.migration.completion_poll_queue is None:
            self.migration.completion_poll_queue = asyncio.Queue()

    # -----------------------------------------------------------------------
    # START MIGRATION
    # -----------------------------------------------------------------------
    async def migrate_start(self):
        """Entry point: prepare snapshots and streams, start migration."""
        self.migration.status = MigrationState.PREPARING

        # 1. Check all nodes online (mocked)
        if not self._nodes_online():
            self.migration.status = MigrationState.SUSPENDED
            return

        # 2. Build streams for all logical volumes
        await self.migrate_prepare()

        self.migration.status = MigrationState.RUNNING
        await self.migration_iterate_streams()

    async def migrate_prepare(self):
        """Prepare each logical volume: build streams and snapshot references."""
        for lv in self.migration.logical_volumes:
            stream = MigrationStream(
                lvol_name=lv.name,
                lvol_state=ObjectMigrationState.NEW,
                lvol_namespace=lv.namespace_uuid
            )
            # Link snapshots if any exist for this LV
            for snapshot in self.migration.snapshots:
                if snapshot.name.startswith(lv.name):  # simple match; customize
                    stream.append_snapshot(snapshot)
            self.migration.streams.append(stream)

    # -----------------------------------------------------------------------
    # ITERATE STREAMS
    # -----------------------------------------------------------------------
    async def migration_iterate_streams(self):
        """Iterate over all streams sequentially."""
        for stream in self.migration.streams:
            if stream.status not in {StreamState.DONE, StreamState.FAILED}:
                await self.migrate_stream_start(stream)

        # If all streams done, mark migration done
        if all(s.status == StreamState.DONE for s in self.migration.streams):
            self.migration.status = MigrationState.DONE

    # -----------------------------------------------------------------------
    # STREAM OPERATIONS
    # -----------------------------------------------------------------------
    async def migrate_stream_start(self, stream: MigrationStream):
        """Start migration for a stream."""
        stream.status = StreamState.RUNNING

        # Iterate snapshots in the stream
        current = stream.head_snapshot_ref
        while current:
            snapshot = current.snapshot
            if snapshot.status == ObjectMigrationState.NEW:
                await spdk_set_migration_flag(snapshot.name)
                await spdk_transfer_snapshot(snapshot.name, stream.lvol_name)
                snapshot.status = ObjectMigrationState.DONE
                # Add to completion poll queue
                await self.migration.completion_poll_queue.put(snapshot.name)
            current = current.next

        # Once snapshots done, migrate the main LV
        await self.migrate_stream_resume(stream)

    async def migrate_stream_resume(self, stream: MigrationStream):
        """Handle LV migration after snapshots."""
        if stream.lvol_state == ObjectMigrationState.NEW:
            await spdk_final_lvol_migration(stream.lvol_name)
            stream.lvol_state = ObjectMigrationState.DONE
            stream.status = StreamState.DONE

        # Clean up intermediate resources
        await self.migrate_stream_cleanup(stream)

    async def migrate_stream_cleanup(self, stream: MigrationStream):
        """Cleanup temporary namespaces, NQNs, etc."""
        # Placeholder: remove temp subsystems or namespaces
        await asyncio.sleep(0.01)
        # No additional state changes needed for this skeleton

    # -----------------------------------------------------------------------
    # MIGRATION CLEANUP
    # -----------------------------------------------------------------------
    async def migrate_cleanup(self, failed: bool = False):
        """Global migration cleanup."""
        if failed:
            self.migration.status = MigrationState.FAILED
            # Mark streams failed if not done
            for stream in self.migration.streams:
                if stream.status != StreamState.DONE:
                    stream.status = StreamState.FAILED
        else:
            self.migration.status = MigrationState.DONE

    # -----------------------------------------------------------------------
    # HELPER FUNCTIONS
    # -----------------------------------------------------------------------
    def _nodes_online(self) -> bool:
        """Mock node health check."""
        return True
