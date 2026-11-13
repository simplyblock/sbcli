"""Mirror ``bdev_lvol_add_clone`` / ``bdev_lvol_convert`` to an lvstore's
non-leader members.

Both calls change the snapshot tree. The leader persists the change, but every
other member of the lvstore (secondary, tertiary) keeps its own in-memory tree
and learns of the change only from the same call issued to it. A member that
misses one keeps a stale tree and, once it takes leadership, acts on it: on bed
bf23 (2026-11-12, LVS_16) the tertiary never linked clone 0x16 under snapshot
0x38, became leader through IO, saw 0x38 as clone-less and deleted its data.

The call sites used to pick the members by their status in the DB (ONLINE
only, or the secondary only), so a member that was restarting, down,
suspended or simply the tertiary silently missed the change while its SPDK was
up and serving. The rule here is reachability instead:

* every member that answers gets the call, whatever its DB status;
* a member that cannot be reached at all (connection refused / no answer) is
  skipped with a warning -- there is nothing to register in, and it reads the
  tree from disk when it next loads the lvstore;
* a member that answers with an error fails the step, so the caller retries.

Both helpers are idempotent, so a retried step does not trip over a member an
earlier attempt already handled: ``add_clone`` is not ("File exists").
"""

from simplyblock_core import db_controller, utils
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import (
    RPCConnectionError,
    RPCException,
    request_reached_remote,
)

logger = utils.get_logger(__name__)

#: Outcomes of :func:`add_clone_on` / :func:`convert_on`.
DONE = "done"        # the member has the change (now, or from an earlier attempt)
SKIPPED = "skipped"  # nothing to do there: not reachable, or it lacks the bdev
FAILED = "failed"    # the member answered with an error: the step must retry

#: RPC budget per member. An unreachable member costs at most this (times the
#: client's retries) per call instead of the 180 s default.
RPC_TIMEOUT_SEC = 5
RPC_RETRY = 2


def lvstore_replicas(primary_node, exclude_ids=(), db=None):
    """``[(role, node), ...]`` for the secondary and tertiary of the lvstore
    *primary_node* is primary of, **whatever their status** (only removed
    nodes and stale ids are left out). Membership is read from the primary:
    a member's own secondary/tertiary fields describe its own lvstore.
    *db*: the caller's DB controller (a fresh one when omitted)."""
    db = db or db_controller.DBController()
    out = []
    for role, node_id in (("secondary", primary_node.secondary_node_id),
                          ("tertiary", primary_node.tertiary_node_id)):
        if not node_id or node_id in exclude_ids:
            continue
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        out.append((role, node))
    return out


def _unreachable(err):
    """The RPC never reached SPDK (as opposed to SPDK answering an error)."""
    return isinstance(err, RPCConnectionError) and not request_reached_remote(err)


def _rpc(node):
    return node.rpc_client(timeout=RPC_TIMEOUT_SEC, retry=RPC_RETRY)


def _short(name):
    return (name or "").split("/")[-1]


def _probe(node, role, bdev_name):
    """``(reached, info)``; *info* is None when the bdev is absent. Raises
    ``RPCException`` for an error answer, which the callers turn into a
    failed step."""
    try:
        return True, _rpc(node).bdev_get(bdev_name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s is not reachable (%s); it is not told about %s and "
                           "picks the change up when it next loads the lvstore",
                           role, node.get_id(), e, bdev_name)
            return False, None
        raise


def _lvol_info(info):
    if isinstance(info, list):
        info = info[0] if info else None
    return ((info or {}).get("driver_specific") or {}).get("lvol") or {}


def _is_snapshot(info):
    lv = _lvol_info(info)
    return bool(lv.get("is_snapshot") or lv.get("snapshot"))


def add_clone_on(node, role, clone_name, parent_name):
    """Link *clone_name* under *parent_name* on *node*. Returns
    ``(outcome, error)``: DONE when linked now or already linked, SKIPPED
    when the member is not reachable or does not hold *clone_name*, FAILED
    (with the error) when it answered with an error."""
    nid = node.get_id()
    try:
        reached, info = _probe(node, role, clone_name)
        if not reached:
            return SKIPPED, None
        if info is None:
            logger.warning("%s %s does not hold %s; nothing to link there",
                           role, nid, clone_name)
            return SKIPPED, None
        lv = _lvol_info(info)
        if lv.get("base_snapshot") and _short(lv.get("base_snapshot")) == _short(parent_name):
            logger.info("%s already reads through %s on %s %s", clone_name, parent_name, role, nid)
            return DONE, None
        if _is_snapshot(info):
            # converted by an earlier attempt, which linked it first
            logger.info("%s is already a snapshot on %s %s; not re-linking", clone_name, role, nid)
            return DONE, None
        ret = _rpc(node).bdev_lvol_add_clone(clone_name, parent_name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s became unreachable during add_clone %s -> %s: %s",
                           role, nid, clone_name, parent_name, e)
            return SKIPPED, None
        return FAILED, f"bdev_lvol_add_clone {clone_name} -> {parent_name} on {role} {nid}: {e}"
    if not ret:
        return FAILED, f"bdev_lvol_add_clone {clone_name} -> {parent_name} failed on {role} {nid}"
    logger.info("add_clone %s -> %s on %s %s", clone_name, parent_name, role, nid)
    return DONE, None


def convert_on(node, role, name):
    """Convert *name* into a snapshot on *node*. Returns ``(outcome, error)``
    as :func:`add_clone_on`: DONE (converted now or already a snapshot),
    SKIPPED (not reachable, or the member lacks *name*), FAILED."""
    nid = node.get_id()
    try:
        reached, info = _probe(node, role, name)
        if not reached:
            return SKIPPED, None
        if info is None:
            logger.warning("%s %s does not hold %s; nothing to convert there", role, nid, name)
            return SKIPPED, None
        if _is_snapshot(info):
            logger.info("%s is already a snapshot on %s %s", name, role, nid)
            return DONE, None
        ret = _rpc(node).bdev_lvol_convert(name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s became unreachable during convert %s: %s", role, nid, name, e)
            return SKIPPED, None
        return FAILED, f"bdev_lvol_convert {name} on {role} {nid}: {e}"
    if not ret:
        return FAILED, f"bdev_lvol_convert {name} failed on {role} {nid}"
    logger.info("convert %s on %s %s", name, role, nid)
    return DONE, None


def add_clone_on_replicas(primary_node, clone_name, parent_name, exclude_ids=(), db=None):
    """:func:`add_clone_on` for every replica of *primary_node*'s lvstore.
    Returns None on success, else the first error (the rest still ran)."""
    first_err = None
    for role, node in lvstore_replicas(primary_node, exclude_ids, db):
        outcome, err = add_clone_on(node, role, clone_name, parent_name)
        if outcome == FAILED and first_err is None:
            first_err = err
    return first_err


def convert_on_replicas(primary_node, name, exclude_ids=(), db=None):
    """:func:`convert_on` for every replica of *primary_node*'s lvstore.
    Returns None on success, else the first error (the rest still ran)."""
    first_err = None
    for role, node in lvstore_replicas(primary_node, exclude_ids, db):
        outcome, err = convert_on(node, role, name)
        if outcome == FAILED and first_err is None:
            first_err = err
    return first_err
