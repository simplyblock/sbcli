class PreconditionError(Exception):
    """Raised when an operation's preconditions are not met."""


class MigrationConflictError(Exception):
    """Raised when a conflicting active migration already exists."""


class SyncReplicationUnsupportedError(PreconditionError):
    """The operation is not supported on a sync-replication cluster (a
    limitation of its first version: it would publish paths without the
    site rule)."""


class SyncReplicationSiteError(PreconditionError):
    """A ``site`` argument is missing on a sync-replication cluster, names no
    site of the cluster, or is given where the cluster has no sites."""


def reject_on_sync_replication(cluster, operation: str) -> None:
    """Raise SyncReplicationUnsupportedError when ``cluster`` is a
    sync-replication cluster."""
    if cluster.sync_replication is True:
        raise SyncReplicationUnsupportedError(
            f"{operation} is not supported on a sync-replication cluster")
