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


class SyncGateError(PreconditionError):
    """A sync-replication gate refused a promote / demote: the replicas are
    not (or were not, before a site was lost) fully in sync. ``problems``
    lists what failed, one line each."""

    def __init__(self, gate: str, problems):
        self.problems = list(problems)
        super().__init__(f"{gate} gate failed: " + "; ".join(self.problems))


def reject_on_sync_replication(cluster, operation: str) -> None:
    """Raise SyncReplicationUnsupportedError when ``cluster`` is a
    sync-replication cluster."""
    if cluster.sync_replication is True:
        raise SyncReplicationUnsupportedError(
            f"{operation} is not supported on a sync-replication cluster")
