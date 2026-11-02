"""Consistency-group subsystem moves (design-cg-live-subsystem-flip.md §9 in
simplyblock-manager): bringing a group's namespaces into one NVMe-oF subsystem
while hosts keep using them.

A move is a resource on its group (``ConsistencyGroup.subsystem_move``) driven
step by step by a caller that coordinates the hosts (the operator):

- **create** picks the target (``cg_colocation.select_move_target``) and
  reserves it in the transaction that records the move, so no namespace-slot
  claim lands in its source or target subsystems while it is active;
- **prepare** creates a fresh target on every node of the HA set and assigns
  every moving namespace its nsid there;
- **move** takes every moving namespace out of its source and into the target,
  on every node, only while the caller's suspend deadline has not passed --
  the hosts hold their I/O until then and resume on their own afterward;
- **commit** switches the volume records and deletes the subsystems the move
  emptied; **rollback** puts the namespaces back.

Unlike ``cg_colocation.move_namespace`` this path moves attached namespaces:
the caller has suspended every host before ``move`` and resumes them after,
which is what makes it safe. ``cg_colocation.NAMESPACE_MOVES_ENABLED`` does not
apply here.

Every step is idempotent: it reads where each namespace is before acting, so a
retried call after a lost answer converges instead of repeating a side effect.
"""
from __future__ import annotations

import copy
import time
import uuid as uuid_mod

from simplyblock_core import constants, utils
from simplyblock_core.controllers import cg_colocation
from simplyblock_core.db_controller import SubsystemMoveConflictError
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)

#: A second move on the group, a move during a group migration, or a step
#: that does not fit the move's state (commit before the namespaces moved).
MoveConflictError = SubsystemMoveConflictError

SOURCE = "source"
TARGET = "target"


class DeadlinePassedError(Exception):
    """The move was asked to begin after the hosts' suspend deadline: they
    may already have resumed on the source, so nothing is touched."""


class MoveFailedError(Exception):
    """A namespace could not be moved; ``namespaces_at`` says which subsystem
    exports the moving namespaces now (the sources, once restored)."""

    def __init__(self, message, namespaces_at):
        super().__init__(message)
        self.namespaces_at = namespaces_at


def _db():
    from simplyblock_core.db_controller import DBController
    return DBController()


def _reserved_on_node(node_id):
    return _db().subsystem_move_reservations(node_id)


def _node_limit(node):
    from simplyblock_core.controllers import lvol_controller
    return lvol_controller.max_subsystems_for_node(node)


def _is_stretched(node) -> bool:
    """Whether the node's subsystems are also published on a second site. A
    move there has to reach both sites inside one window, which this path does
    not do yet (design P0-9)."""
    return bool(getattr(node, "remote_primary_node_id", "")
                or getattr(node, "remote_secondary_node_id", ""))


def _publish_listeners(lvol, snode, is_primary):
    from simplyblock_core.controllers import lvol_controller
    ok, err = lvol_controller.publish_lvol_listeners(lvol, snode, is_primary=is_primary)
    if not ok:
        raise RPCException(err or f"listeners for {lvol.nqn} on {snode.get_id()} failed")


def _connect_entries(lvol, host_nqn):
    """The connect entries a host uses to reach ``lvol``'s current subsystem,
    serialized the way the volume connect route returns them."""
    from simplyblock_core.controllers import lvol_controller
    db = _db()
    host_entry = lvol_controller.HostConnectAuth.resolve(lvol, host_nqn, db)
    entries = lvol_controller._connect_entries_for_volume(
        db, lvol, constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO, host_entry, host_nqn)
    return [e.model_dump(by_alias=True, mode="json") for e in entries]


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #

def _fresh(group):
    return _db().get_consistency_group_by_id(group.get_id())


def _active(group, move_id):
    fresh = _fresh(group)
    record = fresh.subsystem_move or {}
    if not record or record.get("id") != move_id:
        raise KeyError(f"subsystem move {move_id} is not active on consistency group {group.get_id()}")
    return fresh, record


def _update_record(group, **fields):
    def mutate(g):
        g.subsystem_move = dict(g.subsystem_move or {}, **fields)
    return _db().atomic_update(group, mutate).subsystem_move


def _nodes(record):
    db = _db()
    return [db.get_storage_node_by_id(n) for n in record["node_ids"]]


def _has(rpc, nqn, ns_uuid, nsid=None):
    sub = rpc.subsystem_get(nqn)
    if not sub:
        return False
    return any(ns.get("uuid") == ns_uuid and (nsid is None or ns.get("nsid") == nsid)
               for ns in sub.get("namespaces") or [])


def _lvol_view(record, volume):
    """The moving volume as the target exports it: its record, re-pointed."""
    lvol = _db().get_lvol_by_id(volume["lvol_id"])
    view = copy.copy(lvol)
    view.nqn = record["target_nqn"]
    view.ns_id = volume.get("target_nsid") or lvol.ns_id
    return view


# --------------------------------------------------------------------------- #
# Create
# --------------------------------------------------------------------------- #

def create_subsystem_move(group, lvol_id, request_id):
    """Choose where ``group`` ends up now that ``lvol_id`` joined it, and
    reserve it. Returns the move record; one with an empty ``id`` and no
    volumes when nothing has to move (the group's subsystem already holds the
    volume, or the group shares no subsystem).

    Idempotent on ``request_id``. Raises MoveConflictError while another move
    or a migration is active, cg_colocation.SubsystemLimitReached when the
    pinned node cannot take a fresh subsystem, and ColocationError for a
    volume that is not an open member or a stretched cluster.
    """
    db = _db()
    group = _fresh(group)
    current = group.subsystem_move or {}
    if current:
        if current.get("request_id") == request_id:
            return current
        raise MoveConflictError(
            f"consistency group {group.get_id()} already has subsystem move {current.get('id')} active")
    if group.migration:
        raise MoveConflictError(f"consistency group {group.get_id()} has an active migration")
    members = cg_colocation._open_members(group)
    if lvol_id not in members:
        raise cg_colocation.ColocationError(
            f"volume {lvol_id} is not a member of consistency group {group.get_id()}")

    node = db.get_storage_node_by_id(group.node_id)
    if _is_stretched(node):
        raise cg_colocation.ColocationError(
            f"consistency group {group.get_id()} is on a stretched cluster; moving its "
            f"namespaces between subsystems is not supported there yet")

    lvols = db.get_lvols(group.cluster_id)
    by_id = {lv.get_id(): lv for lv in lvols}
    joiner = by_id[lvol_id]
    reserved, fresh = _reserved_on_node(group.node_id)
    target = cg_colocation.select_move_target(
        group, joiner, lvols, node_max_subsystems=_node_limit(node),
        busy_nqns=cg_colocation.busy_nqns(db, group.cluster_id, lvols) | set(reserved),
        reserved_fresh=fresh)
    if not target.moving_ids:
        return {"id": "", "request_id": request_id, "target_nqn": target.target_nqn,
                "target_is_new": False, "namespaces_at": TARGET, "deadline": 0,
                "status": "done", "node_ids": [], "volumes": []}

    target_nqn = target.target_nqn
    if target.target_is_new:
        cluster = db.get_cluster_by_id(group.cluster_id)
        target_nqn = f"{cluster.nqn}:lvol:{uuid_mod.uuid4()}"
    volumes = []
    for lid in target.moving_ids:
        lv = by_id[lid]
        volumes.append({"lvol_id": lid, "source_nqn": lv.nqn, "source_nsid": lv.ns_id,
                        "ns_uuid": lv.get_ns_uuid(), "nguid": lv.guid, "bdev": lv.top_bdev,
                        "target_nsid": 0})
    record = {
        "id": str(uuid_mod.uuid4()),
        "request_id": request_id,
        "target_nqn": target_nqn,
        "target_is_new": target.target_is_new,
        "namespaces_at": SOURCE,
        "deadline": 0,
        "status": "reserved",
        "node_ids": list(joiner.nodes or [joiner.node_id]),
        "volumes": volumes,
    }
    held = {v["source_nqn"] for v in volumes} | {target_nqn}
    return db.reserve_subsystem_move(group, group.node_id, record, held,
                                     1 if target.target_is_new else 0)


# --------------------------------------------------------------------------- #
# Prepare
# --------------------------------------------------------------------------- #

def _create_target(record, root):
    """The fresh subsystem on every HA node, in the ANA state the group's
    volumes have there: optimized on the pinned (primary) node, non-optimized
    on the others. Idempotent: an existing subsystem is left as it is."""
    target = copy.copy(root)
    target.nqn = record["target_nqn"]
    target.max_namespace_per_subsys = constants.MAX_NAMESPACES_PER_SUBSYSTEM
    for index, node in enumerate(_nodes(record)):
        rpc = node.rpc_client()
        if rpc.subsystem_get(target.nqn):
            continue
        from simplyblock_core.controllers import lvol_controller
        rpc.subsystem_create(target.nqn, root.ha_type, str(uuid_mod.uuid4()),
                             lvol_controller.lvol_min_cntlid(index),
                             max_namespaces=target.max_namespace_per_subsys,
                             allow_any_host=not bool(root.allowed_hosts))
        for host in root.allowed_hosts or []:
            rpc.subsystem_add_host(target.nqn, host["nqn"] if isinstance(host, dict) else host)
        _publish_listeners(target, node, is_primary=node.get_id() == root.node_id)


def prepare_subsystem_move(group, move_id, host_nqn=None):
    """Create a fresh target on every HA node and give every moving namespace
    its nsid in the target: the lowest free on all nodes, one value shared by
    every node (the cluster-consistent-nsid rule). Returns the record and, for
    ``host_nqn``, the connect entries of the target subsystem."""
    _, record = _active(group, move_id)
    db = _db()
    root = db.get_lvol_by_id(record["volumes"][0]["lvol_id"])
    if record.get("target_is_new"):
        _create_target(record, root)
    if not all(v.get("target_nsid") for v in record["volumes"]):
        used: set = set()
        for node in _nodes(record):
            sub = node.rpc_client().subsystem_get(record["target_nqn"]) or {}
            used |= {ns["nsid"] for ns in sub.get("namespaces") or []}
        volumes = []
        nsid = 0
        for v in record["volumes"]:
            v = dict(v)
            if not v.get("target_nsid"):
                nsid += 1
                while nsid in used:
                    nsid += 1
                v["target_nsid"] = nsid
            used.add(v["target_nsid"])
            volumes.append(v)
        record = _update_record(group, volumes=volumes, status="prepared")
    entries = []
    if host_nqn is not None:
        entries = _connect_entries(_lvol_view(record, record["volumes"][0]), host_nqn)
    return record, entries


# --------------------------------------------------------------------------- #
# Move
# --------------------------------------------------------------------------- #

def _put_back(record):
    """Every moving namespace out of the target and into its source, on every
    node, wherever it is not there yet."""
    errors = []
    for node in _nodes(record):
        rpc = node.rpc_client()
        for v in record["volumes"]:
            try:
                if _has(rpc, record["target_nqn"], v["ns_uuid"]):
                    rpc.nvmf_subsystem_remove_ns(record["target_nqn"], v["target_nsid"])
                if not _has(rpc, v["source_nqn"], v["ns_uuid"]):
                    rpc.nvmf_subsystem_add_ns2(v["source_nqn"], v["bdev"], v["ns_uuid"],
                                               v["nguid"], nsid=v["source_nsid"])
            except RPCException as e:
                errors.append(f"{v['lvol_id']} on {node.get_id()}: {e}")
    return errors


def move_subsystem_move(group, move_id, deadline):
    """Move every namespace into the target on every HA node.

    ``deadline`` is the hosts' suspend deadline, in Unix seconds. The move
    refuses to begin once it has passed (DeadlinePassedError) and touches no
    node then. A failure part way puts every namespace back into its source
    and raises MoveFailedError with ``namespaces_at = "source"``. A repeat
    after success answers with ``namespaces_at = "target"`` and touches
    nothing, whatever the deadline.
    """
    _, record = _active(group, move_id)
    if record.get("namespaces_at") == TARGET:
        return record
    if not all(v.get("target_nsid") for v in record["volumes"]):
        raise MoveConflictError(f"subsystem move {move_id} is not prepared")
    if time.time() > deadline:
        raise DeadlinePassedError(
            f"subsystem move {move_id}: the suspend deadline {deadline} has passed")
    record = _update_record(group, deadline=int(deadline), status="moving")
    for node in _nodes(record):
        rpc = node.rpc_client()
        for v in record["volumes"]:
            try:
                if _has(rpc, v["source_nqn"], v["ns_uuid"]):
                    rpc.nvmf_subsystem_remove_ns(v["source_nqn"], v["source_nsid"])
                if not _has(rpc, record["target_nqn"], v["ns_uuid"]):
                    rpc.nvmf_subsystem_add_ns2(record["target_nqn"], v["bdev"], v["ns_uuid"],
                                               v["nguid"], nsid=v["target_nsid"])
            except RPCException as e:
                errors = _put_back(record)
                _update_record(group, namespaces_at=SOURCE, status="reserved")
                logger.error("subsystem move %s failed on %s for %s: %s; restored: %s",
                             move_id, node.get_id(), v["lvol_id"], e, errors or "all")
                raise MoveFailedError(
                    f"moving {v['lvol_id']} into {record['target_nqn']} on "
                    f"{node.get_id()} failed: {e}", SOURCE) from e
    return _update_record(group, namespaces_at=TARGET, status="moved")


# --------------------------------------------------------------------------- #
# Commit, rollback, read
# --------------------------------------------------------------------------- #

def _delete_if_empty(nqn, nodes):
    for node in nodes:
        rpc = node.rpc_client()
        sub = rpc.subsystem_get(nqn)
        if sub is not None and not (sub.get("namespaces") or []):
            rpc.subsystem_delete(nqn)


def _ended(group, move_id):
    last = _fresh(group).last_subsystem_move or {}
    return last.get("id") == move_id


def commit_subsystem_move(group, move_id):
    """Switch every moving volume's record to the target in one transaction,
    clear the move, and delete the source subsystems it emptied. A commit
    retried after the move was cleared is a no-op."""
    try:
        _, record = _active(group, move_id)
    except KeyError:
        if _ended(group, move_id):
            return
        raise
    if record.get("namespaces_at") != TARGET:
        raise MoveConflictError(f"subsystem move {move_id}: the namespaces are not on the target")
    db = _db()
    lvols = db.get_lvols(group.cluster_id)
    moving = {v["lvol_id"] for v in record["volumes"]}
    if record.get("target_is_new"):
        namespace = record["volumes"][0]["lvol_id"]
        max_ns = constants.MAX_NAMESPACES_PER_SUBSYSTEM
    else:
        root = next(lv for lv in lvols
                    if lv.nqn == record["target_nqn"] and lv.get_id() not in moving)
        namespace = root.namespace or root.get_id()
        max_ns = root.max_namespace_per_subsys
    updates = []
    for v in record["volumes"]:
        lvol = db.get_lvol_by_id(v["lvol_id"])
        updates.append((lvol, {"nqn": record["target_nqn"], "ns_id": v["target_nsid"],
                               "namespace": namespace, "max_namespace_per_subsys": max_ns}))
    db.release_subsystem_move(group, group.node_id, move_id, "committed", updates)
    nodes = _nodes(record)
    for nqn in sorted({v["source_nqn"] for v in record["volumes"]}):
        _delete_if_empty(nqn, nodes)


def rollback_subsystem_move(group, move_id):
    """Put every namespace back into its source, delete a fresh target the
    move left empty, and release the reservation. A rollback retried after
    the move was cleared is a no-op."""
    try:
        _, record = _active(group, move_id)
    except KeyError:
        if _ended(group, move_id):
            return
        raise
    errors = _put_back(record)
    if errors:
        _update_record(group, status="rollback_failed")
        raise MoveFailedError(f"subsystem move {move_id}: restoring failed: {'; '.join(errors)}",
                              TARGET)
    if record.get("target_is_new"):
        _delete_if_empty(record["target_nqn"], _nodes(record))
    _db().release_subsystem_move(group, group.node_id, move_id, "rolled_back")


def get_subsystem_move(group, move_id):
    """The active move ``move_id``, or the outcome of it once ended."""
    fresh = _fresh(group)
    record = fresh.subsystem_move or {}
    if record.get("id") == move_id:
        return record
    last = fresh.last_subsystem_move or {}
    if last.get("id") == move_id:
        return {"id": move_id, "status": last.get("outcome", ""),
                "namespaces_at": TARGET if last.get("outcome") == "committed" else SOURCE}
    raise KeyError(f"subsystem move {move_id} not found on consistency group {group.get_id()}")
