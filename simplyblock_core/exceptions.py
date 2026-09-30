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


class SyncLeadershipMovingError(PreconditionError):
    """A volume cannot be created or cloned on an LVS whose leadership is being
    moved to the other site (``lvs_active_site`` is ``moving:<site>``); retry
    once the move has completed."""


class SyncAnaError(RuntimeError):
    """A strict sync-replication ANA change (promote / demote) failed on a
    path: the RPC answered false or raised. Nothing was recorded."""


class SyncPromoteRefusedError(PreconditionError):
    """A sync-replication promote was refused: a row of its decision table (a
    volume still served on the other site, another volume of the LVS still
    served there, a forced promote while the other site is online, the target
    site not online), a lost-site refusal of the request, a leadership move
    left unsettled by an ended promote, or a failed connect of a promoted
    volume. ``volumes`` lists the ids that block it, when the refusal is about
    volumes."""

    def __init__(self, message: str, volumes=()):
        self.volumes = list(volumes)
        super().__init__(message)


class SyncPromoteFailedError(SyncPromoteRefusedError):
    """The last promote of these volumes to the requested site ran and failed
    (its task ended ``failed: ...``); reported once per volume, to the first
    promote of that volume to that site after the failure - the next call
    judges the promote table again. ``volumes`` lists the volumes it is
    reported for, ``task_id`` the failed task."""

    def __init__(self, message: str, volumes=(), task_id: str = ""):
        self.task_id = task_id
        super().__init__(message, volumes)


class SyncSiteOfflineError(PreconditionError):
    """A planned sync-replication promote found the site the volume is active
    on not online; only a forced promote may fail it over."""


class SyncGroupMemberError(PreconditionError):
    """A member of a consistency group could not be resolved to a live volume;
    the group operation is refused as a whole. ``volumes`` lists the ids."""

    def __init__(self, message: str, volumes=()):
        self.volumes = list(volumes)
        super().__init__(message)
