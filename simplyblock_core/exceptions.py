class PreconditionError(Exception):
    """Raised when an operation's preconditions are not met."""


class MigrationConflictError(Exception):
    """Raised when a conflicting active migration already exists."""


class NodeTransitionInProgress(Exception):
    """Raised when a node is mid-transition (e.g. its shutdown has not
    finished) and the request can only be served once it lands. Retryable:
    the v2 API answers 503."""
