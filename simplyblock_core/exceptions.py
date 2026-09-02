class PreconditionError(Exception):
    """Raised when an operation's preconditions are not met."""


class MigrationConflictError(Exception):
    """Raised when a conflicting active migration already exists."""


class InsufficientCapacityError(Exception):
    """Raised when the cluster has nowhere to place a new logical volume.

    A refusal, not a fault: the request is well-formed and the control plane is
    healthy, but no node can hold another object until capacity is freed or a
    node is added. Retrying an identical request cannot change the answer, so
    this surfaces as a 409 rather than a 5xx.
    """
