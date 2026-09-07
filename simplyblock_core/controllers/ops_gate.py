"""Cluster-wide switch for object lifecycle operations.

``cluster op-stop <uuid>`` stops a cluster accepting new object lifecycle
requests; ``cluster op-start <uuid>`` resumes. While stopped, creation,
deletion and modification of lvols, snapshots, clones and pools are refused —
including parameter changes such as QoS limits and resizes.

What it deliberately does NOT touch:

  * read paths (list / get / capacity / stats), so a stopped cluster stays
    fully observable;
  * anything the cluster does to itself — restarts, migrations, rebalancing,
    the monitors' own repairs. The switch exists to stop new *requests*
    arriving, not to freeze the data plane.

The check lives in the controllers rather than in the CLI or the web layer
because that is the one place both funnel through: the CLI reaches them via
clibase and the v2 API calls them directly, so gating here covers both without
either having to remember to ask.
"""

from simplyblock_core.db_controller import DBController
from simplyblock_core import utils

logger = utils.get_logger(__name__)


def object_ops_stopped(cluster_id):
    """Whether ``cluster_id`` currently refuses object lifecycle operations."""
    if not cluster_id:
        return False
    try:
        cluster = DBController().get_cluster_by_id(cluster_id)
    except Exception:
        # A gate that cannot read its own flag must never be the reason an
        # operation is refused: fall open and let the operation's own
        # preconditions decide.
        return False
    return bool(getattr(cluster, "object_ops_stopped", False))


def assert_object_ops_allowed(operation, cluster_id=None, pool_uuid=None):
    """Raise ValueError when the owning cluster has object operations stopped.

    Give either ``cluster_id`` or ``pool_uuid``; lvols reference their pool
    rather than a cluster, so the pool is resolved to its cluster here instead
    of at every call site. A lookup that fails resolves to "allowed": this gate
    must never be the reason an operation cannot be attempted at all.
    """
    if cluster_id is None and pool_uuid:
        try:
            cluster_id = DBController().get_pool_by_id(pool_uuid).cluster_id
        except Exception:
            return

    if not object_ops_stopped(cluster_id):
        return

    raise ValueError(
        f"Object operations are stopped on cluster {cluster_id}: {operation} "
        f"is not accepted. Resume with 'sbctl cluster op-start {cluster_id}'.")
