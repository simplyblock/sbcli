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

The steps after create run one at a time per group, behind the group's
``DbLock``: each is a loop of RPCs over the HA nodes that no transaction can
cover, and the caller may send the next step, or a rollback, to another API
replica before the last one answered. A step that arrives while another runs
waits for it; one that waits too long is refused with MoveConflictError. Every
write of the record names the move it belongs to, so a step that outlived its
move (ended meanwhile by a rollback or a commit elsewhere) cannot rebuild a
record out of the empty group.
"""
from __future__ import annotations

import contextlib
import copy
import time
import uuid as uuid_mod

from simplyblock_core import constants, utils
from simplyblock_core.controllers import cg_colocation, subsystem_access
from simplyblock_core.db_controller import (
    SubsystemCapacityError,
    SubsystemMoveConflictError,
)
from simplyblock_core.models.lock import DbLock, DbLockBusyError
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)

#: A second move on the group, a move during a group migration, a step that
#: does not fit the move's state (commit before the namespaces moved), or a
#: step that waited out another step of the same group.
MoveConflictError = SubsystemMoveConflictError

SOURCE = "source"
TARGET = "target"


class DeadlinePassedError(Exception):
    """The move was asked to begin after the hosts' suspend deadline: they
    may already have resumed on the source, so nothing is touched."""


class MoveFailedError(Exception):
    """A namespace could not be moved, or could not be put back. ``namespaces_at``
    is ``source`` only once every namespace is back in its source; it is
    ``target`` when restoring failed, since a source then lacks a namespace
    and hosts must not be resumed on it until a rollback has made it whole."""

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


def _not_active(group, move_id):
    return KeyError(f"subsystem move {move_id} is not active on consistency group {group.get_id()}")


def _active(group, move_id):
    fresh = _fresh(group)
    record = fresh.subsystem_move or {}
    if record.get("id") != move_id:
        raise _not_active(group, move_id)
    return fresh, record


def _update_record(group, move_id, **fields):
    """``fields`` onto the group's move record, as long as that record is
    still move ``move_id``'s. A step that outlived its move must not rebuild
    a record out of the empty group: one without an id could never be
    committed, rolled back, or superseded. Raises KeyError then, with the
    group left as it was."""
    def mutate(g):
        current = g.subsystem_move or {}
        if current.get("id") != move_id:
            return False
        g.subsystem_move = dict(current, **fields)
    fresh = _db().atomic_update(group, mutate)
    record = (fresh.subsystem_move if fresh is not None else None) or {}
    if record.get("id") != move_id:
        raise _not_active(group, move_id)
    return record


@contextlib.contextmanager
def _one_step_at_a_time(group, move_id):
    """The group's step lock. The RPC loop of a step cannot sit in a
    transaction, so a step arriving while another runs (on any replica) waits
    behind it, and is refused with MoveConflictError once it has waited
    constants.CG_SUBSYSTEM_MOVE_LOCK_WAIT_TIMEOUT_SEC."""
    try:
        with DbLock(f"cg_subsystem_move/{group.get_id()}",
                    timeout=constants.CG_SUBSYSTEM_MOVE_LOCK_WAIT_TIMEOUT_SEC):
            yield
    except DbLockBusyError as busy:
        raise MoveConflictError(
            f"another step of subsystem move {move_id} is running on consistency group "
            f"{group.get_id()} ({busy.owner}); retry once it has answered") from busy


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

PURPOSES = ("join", "leave")


def create_subsystem_move(group, lvol_id, request_id, purpose="join"):
    """Reserve a move of ``group``'s namespaces between subsystems.

    ``purpose="join"`` chooses where the group ends up now that ``lvol_id``
    joined it. ``purpose="leave"`` takes ``lvol_id``, still an open member,
    out of the subsystem it shares and into a fresh one of its own, which is
    how a relabeled volume leaves before it joins another group. Returns the
    move record, carrying its ``purpose``; one with an empty ``id`` and no
    volumes when nothing has to move (the group's subsystem already holds the
    joiner, the group shares no subsystem, or the leaver shares none).

    Idempotent on ``request_id``. Raises MoveConflictError while another move
    or a migration is active on the group, or holds a subsystem this move
    would take a namespace out of or into, cg_colocation.SubsystemLimitReached when the
    node cannot take a fresh subsystem, ColocationError for a volume that is
    not an open member, a stretched cluster, or volumes that would share the
    target but allow different hosts (a subsystem has one host list, so the
    move would open or close a namespace nobody asked to), and ValueError for
    an unknown ``purpose``.
    """
    if purpose not in PURPOSES:
        raise ValueError(f"subsystem move purpose must be one of {PURPOSES}, not {purpose!r}")
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
    busy = cg_colocation.busy_nqns(db, group.cluster_id, lvols) | set(reserved)
    try:
        if purpose == "leave":
            target = cg_colocation.select_leave_target(
                joiner, lvols, node_max_subsystems=_node_limit(node),
                busy_nqns=busy, reserved_fresh=fresh)
        else:
            target = cg_colocation.select_move_target(
                group, joiner, lvols, node_max_subsystems=_node_limit(node),
                busy_nqns=busy, reserved_fresh=fresh)
    except cg_colocation.SubsystemBusy as e:
        # A held subsystem frees up when that move or migration ends: a
        # conflict the caller waits out, not a refusal it takes as final.
        raise MoveConflictError(str(e)) from e
    if not target.moving_ids:
        return {"id": "", "request_id": request_id, "purpose": purpose,
                "target_nqn": target.target_nqn, "target_is_new": False,
                "namespaces_at": TARGET, "deadline": 0,
                "status": "done", "node_ids": [], "volumes": []}

    target_nqn = target.target_nqn
    if target.target_is_new:
        cluster = db.get_cluster_by_id(group.cluster_id)
        target_nqn = f"{cluster.nqn}:lvol:{uuid_mod.uuid4()}"
    sharing = [by_id[lid] for lid in target.moving_ids]
    if not target.target_is_new:
        sharing += [lv for lv in lvols if lv.nqn == target_nqn
                    and lv.get_id() not in target.moving_ids and cg_colocation._alive(lv)]
    _require_same_hosts(sharing)
    volumes = []
    for lid in target.moving_ids:
        lv = by_id[lid]
        volumes.append({"lvol_id": lid, "source_nqn": lv.nqn, "source_nsid": lv.ns_id,
                        "ns_uuid": lv.get_ns_uuid(), "nguid": lv.guid, "bdev": lv.top_bdev,
                        "target_nsid": 0})
    record = {
        "id": str(uuid_mod.uuid4()),
        "request_id": request_id,
        "purpose": purpose,
        "target_nqn": target_nqn,
        "target_is_new": target.target_is_new,
        "namespaces_at": SOURCE,
        "deadline": 0,
        "status": "reserved",
        "node_ids": list(joiner.nodes or [joiner.node_id]),
        "volumes": volumes,
    }
    held = {v["source_nqn"] for v in volumes} | {target_nqn}
    try:
        return db.reserve_subsystem_move(group, group.node_id, record, held,
                                         1 if target.target_is_new else 0)
    except SubsystemCapacityError as e:
        # The selection saw a free slot that another create took meanwhile.
        raise cg_colocation.SubsystemLimitReached(group.node_id, _node_limit(node)) from e


def _hosts_of(lvol) -> frozenset:
    return frozenset(e.get("nqn", "") for e in lvol.allowed_hosts or [])


def _require_same_hosts(sharing):
    """Every volume that will share the target allows the same hosts. A
    subsystem has one host list: with an open volume beside a restricted one
    the move would close the open one or open the restricted one, and with
    two restricted lists a host would reach a namespace its volume never
    listed. Raises ColocationError naming each volume's hosts."""
    if len({_hosts_of(lv) for lv in sharing}) <= 1:
        return
    listing = ", ".join(
        f"{lv.get_id()}: {' '.join(sorted(_hosts_of(lv))) or 'any host'}"
        for lv in sorted(sharing, key=lambda v: v.get_id()))
    raise cg_colocation.ColocationError(
        f"the volumes that would share the subsystem allow different hosts ({listing}); "
        f"a subsystem has one host list")


# --------------------------------------------------------------------------- #
# Prepare
# --------------------------------------------------------------------------- #

def _create_target(record, root):
    """The fresh subsystem on every HA node, in the ANA state the group's
    volumes have there: optimized on the pinned (primary) node, non-optimized
    on the others, admitting the hosts the moving volumes allow, each with the
    keys its volume gives it (subsystem_access.grant, the same path a volume
    create takes). Idempotent: an existing subsystem is left as it is."""
    db = _db()
    target = copy.copy(root)
    target.nqn = record["target_nqn"]
    target.max_namespace_per_subsys = constants.MAX_NAMESPACES_PER_SUBSYSTEM
    intended = subsystem_access.intended_for(
        db, target.nqn, [db.get_lvol_by_id(v["lvol_id"]) for v in record["volumes"]])
    for index, node in enumerate(_nodes(record)):
        rpc = node.rpc_client()
        if rpc.subsystem_get(target.nqn):
            continue
        from simplyblock_core.controllers import lvol_controller
        rpc.subsystem_create(target.nqn, root.ha_type, str(uuid_mod.uuid4()),
                             lvol_controller.lvol_min_cntlid(index),
                             max_namespaces=target.max_namespace_per_subsys,
                             allow_any_host=intended.any_host)
        for host in intended.hosts.values():
            subsystem_access.grant(rpc, node, target.nqn, host, db)
        _publish_listeners(target, node, is_primary=node.get_id() == root.node_id)


def prepare_subsystem_move(group, move_id, host_nqn=None):
    """Create a fresh target on every HA node and give every moving namespace
    its nsid in the target: the lowest free on all nodes, one value shared by
    every node (the cluster-consistent-nsid rule). Returns the record and, for
    ``host_nqn``, the connect entries of the target subsystem."""
    with _one_step_at_a_time(group, move_id):
        return _prepare(group, move_id, host_nqn)


def _prepare(group, move_id, host_nqn):
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
        record = _update_record(group, move_id, volumes=volumes, status="prepared")
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


def _restore(group, move_id, record, after=""):
    """Every namespace back into its source. When that fails too, the record
    says so (``rollback_failed``) and MoveFailedError reports ``target``: a
    source lacks a namespace, so the caller must not resume hosts on it; a
    rollback retries the restore. ``after`` prefixes the message with what
    failed before the restore was needed."""
    errors = _put_back(record)
    if not errors:
        return
    # The move may have been ended meanwhile; what could not be restored is
    # the answer either way.
    with contextlib.suppress(KeyError):
        _update_record(group, move_id, status="rollback_failed")
    raise MoveFailedError(
        f"subsystem move {move_id}: {after}restoring the sources failed: {'; '.join(errors)}",
        TARGET)


def move_subsystem_move(group, move_id, deadline):
    """Move every namespace into the target on every HA node.

    ``deadline`` is the hosts' suspend deadline, in Unix seconds. The move
    refuses to begin once it has passed (DeadlinePassedError) and touches no
    node then. A failure part way puts every namespace back into its source
    and raises MoveFailedError with ``namespaces_at = "source"``, or with
    ``"target"`` when the restore failed too (see MoveFailedError). A repeat
    after success answers with ``namespaces_at = "target"`` and touches
    nothing, whatever the deadline.
    """
    with _one_step_at_a_time(group, move_id):
        return _move(group, move_id, deadline)


def _move(group, move_id, deadline):
    _, record = _active(group, move_id)
    if record.get("namespaces_at") == TARGET:
        return record
    if not all(v.get("target_nsid") for v in record["volumes"]):
        raise MoveConflictError(f"subsystem move {move_id} is not prepared")
    if time.time() > deadline:
        raise DeadlinePassedError(
            f"subsystem move {move_id}: the suspend deadline {deadline} has passed")
    record = _update_record(group, move_id, deadline=int(deadline), status="moving")
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
                failure = (f"moving {v['lvol_id']} into {record['target_nqn']} on "
                           f"{node.get_id()} failed: {e}")
                logger.error("subsystem move %s: %s; restoring the sources", move_id, failure)
                _restore(group, move_id, record, after=f"{failure}; ")
                # The move may have been ended meanwhile; the namespaces are
                # back in their sources either way, which is the answer.
                with contextlib.suppress(KeyError):
                    _update_record(group, move_id, namespaces_at=SOURCE, status="reserved")
                raise MoveFailedError(failure, SOURCE) from e
    return _update_record(group, move_id, namespaces_at=TARGET, status="moved")


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
    with _one_step_at_a_time(group, move_id):
        _commit(group, move_id)


def _commit(group, move_id):
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
    with _one_step_at_a_time(group, move_id):
        _rollback(group, move_id)


def _rollback(group, move_id):
    try:
        _, record = _active(group, move_id)
    except KeyError:
        if _ended(group, move_id):
            return
        raise
    _restore(group, move_id, record)
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
