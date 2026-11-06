"""Consistency-group co-location: group-wide migration scope, the group's
subsystem at create time, the flip-out victim, the late-join plan, and the
re-pin after a group moved (docs/consistency-group-colocation.md).

A consistency group's members live on ONE node/LVS (the pin): the frozen group
snapshot operates on one store. Two operations can break that invariant and two
can establish it:

- **Live migration** moves an NVMe subsystem (every namespace in it) to another
  node. A member moved alone leaves its group split across stores. A group can
  also span several subsystems, and a subsystem can hold volumes of several
  groups or none, so the unit that must move together is the CLOSURE over
  "shares a subsystem with" and "is in the same group as" -- :func:`migration_scope`.
- **Late join** (label added to an existing PVC) must first bring the volume to
  the pin: :func:`plan_late_join` says which steps that takes.
- **Create** places a new member on the pin; it should also take the group's
  subsystem (:func:`group_subsystem_nqn`), and when that subsystem is full a
  non-member namespace is flipped out (:func:`choose_flip_victim`).

The planners are pure (records in, decisions out) so the rules are unit-tested
without a store; the ``*_for`` wrappers resolve records from the DB.

Moving a namespace between subsystems changes the NVMe-oF path a client is
attached to. That is only safe once the client stages volumes behind the
device-mapper indirection the design describes and can swap paths under a live
consumer. Until then :data:`NAMESPACE_MOVES_ENABLED` is off: co-location is
planned and reported, and the move itself is refused for attached volumes.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from simplyblock_core import utils
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCRemoteError

logger = utils.get_logger(__name__)

# Moving a namespace between subsystems (create-time flip-out, late-join
# co-location) re-points the client's NVMe-oF path. Off until the CSI node
# plugin's device-mapper swap is deployed on every node (design §5); with it
# off, a move is refused for any volume whose subsystem has a connected host.
NAMESPACE_MOVES_ENABLED = False

_GONE = (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)


class ColocationError(Exception):
    """A co-location step is refused; the message names the precondition."""


def _alive(lv) -> bool:
    return lv.status not in _GONE and not getattr(lv, "deleted", False)


def _open_members(group) -> set:
    return {lid for lid, m in (group.members or {}).items()
            if m.get("removed_seq", 0) == 0}


def _shares_subsystem(lv) -> bool:
    # A subsystem that only CAN hold several namespaces but holds one is not
    # shared; matching on NQN with max>1 mirrors _get_shared_subsystem_members.
    return getattr(lv, "max_namespace_per_subsys", 1) > 1


# --------------------------------------------------------------------------- #
# Migration scope
# --------------------------------------------------------------------------- #

def migration_scope(seed_ids, lvols, groups) -> list:
    """Every volume that must move together with ``seed_ids``.

    The closure over two relations: volumes sharing an NVMe subsystem move as
    one (a migration moves the subsystem), and open-epoch members of one
    consistency group move as one (the group must stay on one store). A group
    spanning two subsystems therefore pulls in both subsystems, and a
    non-member sharing a subsystem with a member comes along too.

    ``lvols`` is the cluster's lvol records, ``groups`` its consistency groups.
    Returns lvol ids, sorted, seeds included.
    """
    by_id = {lv.get_id(): lv for lv in lvols if _alive(lv)}
    by_nqn: dict[str, list] = {}
    for lv in by_id.values():
        if _shares_subsystem(lv):
            by_nqn.setdefault(lv.nqn, []).append(lv.get_id())
    group_of: dict[str, set] = {}
    for g in groups:
        members = _open_members(g)
        for lid in members:
            group_of[lid] = members

    scope: set = set()
    queue = [s for s in seed_ids if s in by_id]
    while queue:
        lid = queue.pop()
        if lid in scope:
            continue
        scope.add(lid)
        lv = by_id[lid]
        if _shares_subsystem(lv):
            queue.extend(x for x in by_nqn.get(lv.nqn, ()) if x not in scope)
        queue.extend(x for x in group_of.get(lid, ()) if x in by_id and x not in scope)
    return sorted(scope)


def scope_by_subsystem(scope_ids, lvols) -> dict:
    """The scope split into its subsystems: NQN -> member lvol ids (nsid
    order). One entry per migration the scope needs: a shared subsystem is one
    batch migration, a standalone one a single migration."""
    by_id = {lv.get_id(): lv for lv in lvols}
    out: dict[str, list] = {}
    for lid in scope_ids:
        lv = by_id[lid]
        out.setdefault(lv.nqn, []).append(lv)
    return {nqn: [lv.get_id() for lv in sorted(lvs, key=lambda x: x.ns_id)]
            for nqn, lvs in sorted(out.items())}


def uncovered_group_members(requested_ids, lvols, groups) -> list:
    """The volumes a migration of ``requested_ids`` would have to take along
    but does not: the scope minus what was asked for. Empty means the request
    keeps every group it touches whole."""
    scope = migration_scope(requested_ids, lvols, groups)
    return [x for x in scope if x not in set(requested_ids)]


# --------------------------------------------------------------------------- #
# Re-pin after a move
# --------------------------------------------------------------------------- #

def repin_target(group, member_lvols):
    """``(node_id, lvs_name)`` the group should be pinned to now, or None.

    A group migration moves its members one subsystem at a time, so the group
    is split while it runs (group snapshots are refused then: _precheck_members
    reports the off-store members). Once every open member lives on one store
    that is not the pin, the pin follows them.
    """
    members = _open_members(group)
    places = {(lv.node_id, lv.lvs_name) for lv in member_lvols
              if lv.get_id() in members and _alive(lv)}
    if len(places) != 1:
        return None
    place = next(iter(places))
    if place == (group.node_id, group.lvs_name):
        return None
    return place


# --------------------------------------------------------------------------- #
# The group's subsystem, and the flip-out victim
# --------------------------------------------------------------------------- #

def group_subsystem_nqn(group, member_lvols) -> str:
    """The subsystem a new member should join: the one holding the most open
    members on the pinned node, "" when no member shares a subsystem (a group
    of standalone subsystems has no common one to join). Ties break on NQN so
    a retried claim is deterministic."""
    members = _open_members(group)
    counts: dict[str, int] = {}
    for lv in member_lvols:
        if lv.get_id() not in members or not _alive(lv):
            continue
        if lv.node_id != group.node_id or not _shares_subsystem(lv):
            continue
        counts[lv.nqn] = counts.get(lv.nqn, 0) + 1
    if not counts:
        return ""
    return min(counts, key=lambda n: (-counts[n], n))


@dataclass
class FlipVictim:
    lvol_id: str
    attached: bool
    reason: str = ""


def choose_flip_victim(subsystem_lvols, attached_ids=(), migrating_ids=()):
    """The namespace to move out of a full group subsystem, or None.

    Only a volume that belongs to NO consistency group is eligible -- moving a
    member of this group out defeats the purpose, and moving a member of
    another group breaks that group. Volumes being created, deleted or migrated
    are skipped. Preference: a volume no host is connected to (its move needs
    no client swap), then the smallest (least to re-point), then the highest
    nsid (the most recently added).
    """
    attached = set(attached_ids)
    migrating = set(migrating_ids)
    candidates = [lv for lv in subsystem_lvols
                  if _alive(lv) and lv.status != LVol.STATUS_IN_CREATION
                  and not getattr(lv, "group_id", "")
                  and lv.get_id() not in migrating]
    if not candidates:
        return None
    best = min(candidates, key=lambda lv: (lv.get_id() in attached, lv.size, -lv.ns_id))
    return FlipVictim(best.get_id(), best.get_id() in attached,
                      "no consistency group; " + ("attached" if best.get_id() in attached
                                                  else "not attached"))


# --------------------------------------------------------------------------- #
# Late join
# --------------------------------------------------------------------------- #

STEP_MIGRATE = "migrate"
STEP_JOIN = "join"
STEP_COLOCATE = "colocate"


@dataclass
class JoinPlan:
    steps: list = field(default_factory=list)
    migrate_ids: list = field(default_factory=list)
    target_node_id: str = ""
    target_nqn: str = ""
    # The subsystem move the join would end in (late_join_plan only):
    # {"target_nqn", "target_is_new", "moving_lvol_ids", "refusal"}. Empty
    # when the group has no other open member to share a subsystem with.
    colocation: dict = field(default_factory=dict)


def plan_late_join(group, lvol, lvols, groups) -> JoinPlan:
    """The steps that join an EXISTING volume to ``group`` (design §4.5 with
    pre-join migration).

    - Group not pinned yet (no open member): join; the volume pins it.
    - Volume on the pin: join, then co-locate into the group's subsystem when
      it is not there.
    - Volume off the pin: live-migrate it -- with everything its migration
      scope drags along -- to the pinned node, then join and co-locate.

    Refused (ColocationError): a volume in another group, a volume in another
    pool than the members, or a scope that would drag another group's members
    along (that group would have to move to this one's node).
    """
    members = _open_members(group)
    if lvol.get_id() in members:
        plan = JoinPlan()
        nqn = group_subsystem_nqn(group, lvols)
        if nqn and lvol.nqn != nqn:
            plan.steps = [STEP_COLOCATE]
            plan.target_nqn = nqn
        return plan
    other = getattr(lvol, "group_id", "")
    if other and other != group.get_id():
        raise ColocationError(
            f"volume {lvol.get_id()} is a member of consistency group {other}; "
            f"a volume belongs to at most one group")
    by_id = {lv.get_id(): lv for lv in lvols}
    for mid in members:
        m = by_id.get(mid)
        if m is not None and m.pool_uuid != lvol.pool_uuid:
            raise ColocationError(
                f"volume {lvol.get_id()} is in pool {lvol.pool_uuid[:8]} but "
                f"consistency group {group.get_id()[:8]} members live in pool "
                f"{m.pool_uuid[:8]}")
    plan = JoinPlan()
    pinned = bool(group.node_id) and bool(members)
    if pinned and (lvol.node_id != group.node_id or lvol.lvs_name != group.lvs_name):
        scope = migration_scope([lvol.get_id()], lvols, [g for g in groups if g.get_id() != group.get_id()])
        foreign = [x for x in scope if x != lvol.get_id()
                   and getattr(by_id[x], "group_id", "")]
        if foreign:
            raise ColocationError(
                f"moving volume {lvol.get_id()} to the group's node would move "
                f"members of another consistency group with it: {', '.join(foreign)}")
        plan.steps.append(STEP_MIGRATE)
        plan.migrate_ids = scope
        plan.target_node_id = group.node_id
    plan.steps.append(STEP_JOIN)
    nqn = group_subsystem_nqn(group, lvols) if pinned else ""
    if nqn and lvol.nqn != nqn:
        plan.steps.append(STEP_COLOCATE)
        plan.target_nqn = nqn
    return plan


# --------------------------------------------------------------------------- #
# Subsystem move target (design-cg-live-subsystem-flip.md §5)
# --------------------------------------------------------------------------- #

class SubsystemLimitReached(ColocationError):
    """A group that needs a fresh subsystem is pinned to a node already holding
    as many subsystems as it may. The only capacity refusal of a move."""

    def __init__(self, node_id, limit):
        self.node_id = node_id
        self.limit = limit
        super().__init__(f"node {node_id} holds its maximum of {limit} subsystems")


@dataclass
class MoveTarget:
    """Where a joining volume's group ends up. ``target_nqn`` is empty when
    the target is a fresh subsystem (``target_is_new``); ``moving_ids`` are
    the volumes whose namespaces change subsystem, empty when nothing moves."""
    target_nqn: str = ""
    target_is_new: bool = False
    moving_ids: list = field(default_factory=list)


def _capacity(lv) -> int:
    from simplyblock_core import constants
    return min(getattr(lv, "max_namespace_per_subsys", 1) or 1,
               constants.MAX_NAMESPACES_PER_SUBSYSTEM)


def count_node_subsystems(node_id, lvols) -> int:
    """Distinct subsystems ``node_id`` is primary for, counted the way the
    slot claim counts them (lvol_controller.count_lvol_subsystems)."""
    return len({lv.nqn for lv in lvols
                if lv.node_id == node_id and lv.status not in _GONE})


def select_move_target(group, joining_lvol, lvols, *, node_max_subsystems,
                       node_subsystem_count=None, busy_nqns=(), reserved_fresh=0) -> MoveTarget:
    """The subsystem ``group`` ends up in once ``joining_lvol`` joins it.

    1. The group's subsystem -- the one holding the most of its OTHER open
       members, so the joiner never elects its own -- takes the joiner alone
       while it has a free slot. No shared group subsystem: nothing moves.
    2. Otherwise the whole group moves. A candidate is a shared subsystem on
       the pinned node made up solely of the joiner's pool and not ``busy``
       (an active migration or move); it fits when its free slots cover every
       member not already in it. Smallest moving set wins (fewest consumers
       paused), then the fuller candidate, then the lower NQN.
    3. No candidate: a fresh subsystem, unless the node's subsystem count plus
       the fresh subsystems other moves reserved reaches its limit.

    Raises SubsystemLimitReached in the last case and nowhere else.
    """
    jid = joining_lvol.get_id()
    node = group.node_id
    live = [lv for lv in lvols if _alive(lv)]
    member_ids = (_open_members(group) - {jid}) | {jid}
    g = group_subsystem_nqn(group, [lv for lv in live if lv.get_id() != jid])
    if not g or joining_lvol.nqn == g:
        return MoveTarget(g or joining_lvol.nqn, False, [])

    by_nqn: dict[str, list] = {}
    for lv in live:
        if lv.node_id == node:
            by_nqn.setdefault(lv.nqn, []).append(lv)
    in_g = by_nqn.get(g, [])
    if in_g and len(in_g) < _capacity(in_g[0]):
        return MoveTarget(g, False, [jid])

    busy = set(busy_nqns)
    best = None
    for nqn, subs in by_nqn.items():
        if nqn in busy or not _shares_subsystem(subs[0]):
            continue
        if any(lv.pool_uuid != joining_lvol.pool_uuid for lv in subs):
            continue
        moving = sorted(member_ids - {lv.get_id() for lv in subs})
        if _capacity(subs[0]) - len(subs) < len(moving):
            continue
        key = (len(moving), -len(subs), nqn)
        if best is None or key < best[0]:
            best = (key, nqn, moving)
    if best is not None:
        return MoveTarget(best[1], False, best[2])

    count = (node_subsystem_count if node_subsystem_count is not None
             else count_node_subsystems(node, lvols))
    if count + reserved_fresh < node_max_subsystems:
        return MoveTarget("", True, sorted(member_ids))
    raise SubsystemLimitReached(node, node_max_subsystems)


class SubsystemBusy(ColocationError):
    """The subsystem a volume would leave is held by an active migration or
    another subsystem move."""


def select_leave_target(leaving_lvol, lvols, *, node_max_subsystems,
                        node_subsystem_count=None, busy_nqns=(), reserved_fresh=0) -> MoveTarget:
    """Where ``leaving_lvol`` goes when it leaves its consistency group: out of
    the subsystem it shares, into a fresh one of its own, so the next group it
    joins finds it alone. A volume no other live volume shares a subsystem with
    has nothing to leave, and nothing moves.

    Raises SubsystemBusy when the shared subsystem is ``busy``, and
    SubsystemLimitReached when the node has no room for a fresh subsystem.
    """
    lid = leaving_lvol.get_id()
    shared = any(_alive(lv) and lv.nqn == leaving_lvol.nqn and lv.get_id() != lid for lv in lvols)
    if not shared:
        return MoveTarget(leaving_lvol.nqn, False, [])
    if leaving_lvol.nqn in set(busy_nqns):
        raise SubsystemBusy(f"subsystem {leaving_lvol.nqn} is held by an active migration or move")
    node = leaving_lvol.node_id
    count = (node_subsystem_count if node_subsystem_count is not None
             else count_node_subsystems(node, lvols))
    if count + reserved_fresh < node_max_subsystems:
        return MoveTarget("", True, [lid])
    raise SubsystemLimitReached(node, node_max_subsystems)


def predict_move_target(group, joining_lvol, lvols, migrate_ids, *, node_max_subsystems,
                        busy_nqns=(), reserved_fresh=0) -> MoveTarget:
    """:func:`select_move_target` against the state the pinned node has once
    the join's migration landed: the joiner and every volume migrating with it
    (``migrate_ids``) are counted on the pin, their subsystem included. This is
    the preflight -- a refusal here means the volume must not be migrated."""
    import copy
    moved = set(migrate_ids) | {joining_lvol.get_id()}
    predicted = []
    for lv in lvols:
        if lv.get_id() in moved and lv.node_id != group.node_id:
            lv = copy.copy(lv)
            lv.node_id = group.node_id
            lv.lvs_name = group.lvs_name
        predicted.append(lv)
    joiner = next(lv for lv in predicted if lv.get_id() == joining_lvol.get_id()) \
        if any(lv.get_id() == joining_lvol.get_id() for lv in predicted) else joining_lvol
    return select_move_target(group, joiner, predicted, node_max_subsystems=node_max_subsystems,
                              busy_nqns=busy_nqns, reserved_fresh=reserved_fresh)


def busy_nqns(db, cluster_id, lvols) -> set:
    """Subsystems with an active migration of any of their volumes."""
    active = {m.lvol_id for m in db.get_migrations(cluster_id) if m.is_active()}
    return {lv.nqn for lv in lvols if lv.get_id() in active}


def _reserved_on_node(node_id):
    """(subsystems held by active moves, fresh subsystems they reserved)."""
    return _db().subsystem_move_reservations(node_id)


def _colocation_outcome(group, lvol, lvols, plan) -> dict:
    from simplyblock_core.controllers import lvol_controller
    db = _db()
    node = db.get_storage_node_by_id(group.node_id)
    reserved, fresh = _reserved_on_node(group.node_id)
    try:
        t = predict_move_target(
            group, lvol, lvols, plan.migrate_ids,
            node_max_subsystems=lvol_controller.max_subsystems_for_node(node),
            busy_nqns=busy_nqns(db, group.cluster_id, lvols) | set(reserved),
            reserved_fresh=fresh)
    except SubsystemLimitReached as e:
        return {"target_nqn": "", "target_is_new": False, "moving_lvol_ids": [],
                "refusal": f"SubsystemLimitReached: {e}"}
    return {"target_nqn": t.target_nqn, "target_is_new": t.target_is_new,
            "moving_lvol_ids": t.moving_ids, "refusal": ""}


# --------------------------------------------------------------------------- #
# DB-resolving wrappers
# --------------------------------------------------------------------------- #

def _db():
    from simplyblock_core.db_controller import DBController
    return DBController()


def _cluster_records(cluster_id):
    db = _db()
    return db.get_lvols(cluster_id), db.get_consistency_groups(cluster_id)


def require_whole_groups(requested_ids, cluster_id):
    """Raise ValueError when migrating ``requested_ids`` would split a group.

    Called by the migration create paths: a single volume or one subsystem is
    refused when its migration scope is larger, naming what it would leave
    behind and the group migration that moves it all.
    """
    lvols, groups = _cluster_records(cluster_id)
    missing = uncovered_group_members(requested_ids, lvols, groups)
    if missing:
        raise ValueError(
            f"Migrating {', '.join(sorted(requested_ids))} would split a consistency "
            f"group: it must move together with {', '.join(missing)}. Migrate the "
            f"whole scope with the consistency-group migration "
            f"(POST /clusters/<id>/consistency-groups/<gid>/migrate).")


def scope_for(lvol_id, cluster_id):
    lvols, groups = _cluster_records(cluster_id)
    scope = migration_scope([lvol_id], lvols, groups)
    return scope, scope_by_subsystem(scope, lvols)


def repin_after_member_moved(lvol):
    """Hook of the migration DB switch: re-pin the group of ``lvol`` once all
    its open members share the new store. Never raises -- a failed re-pin is
    reported by the group snapshot precheck (members off the pinned store)."""
    try:
        gid = getattr(lvol, "group_id", "")
        if not gid:
            return
        db = _db()
        group = db.get_consistency_group_by_id(gid)
        members = []
        for mid in _open_members(group):
            try:
                members.append(db.get_lvol_by_id(mid))
            except KeyError:
                continue
        place = repin_target(group, members)
        if place is not None:
            logger.info("Consistency group %s re-pinned from %s/%s to %s/%s: every "
                        "member moved", group.get_id()[:8], (group.node_id or "-")[:8],
                        group.lvs_name, place[0][:8], place[1])
            group.node_id, group.lvs_name = place
            group.write_to_db(db.kv_store)
        if group.migration and place is not None:
            from simplyblock_core.controllers import migration_controller
            if migration_controller.group_migration_status(group.get_id()) in ("done", "running"):
                # Every member arrived (re-pinned): the group migration is over
                # for the group even if a non-member's subsystem still finishes.
                migration_controller.clear_group_migration(group.migration)
    except Exception as e:  # noqa: BLE001
        logger.warning("Re-pin check after migrating %s failed: %s", lvol.get_id(), e)


def subsystem_has_hosts(lvol) -> bool:
    """Whether any host is connected to ``lvol``'s subsystem on any of its
    nodes. Controllers are per subsystem, not per namespace, so for a shared
    subsystem this is conservative: any member's consumer counts."""
    db = _db()
    for node_id in (lvol.nodes or [lvol.node_id]):
        try:
            node = db.get_storage_node_by_id(node_id)
            if node.rpc_client(timeout=5, retry=1).nvmf_subsystem_get_controllers(lvol.nqn):
                return True
        except KeyError:
            continue
        except Exception as e:  # noqa: BLE001
            logger.warning("Cannot read controllers of %s on %s: %s; treating as attached",
                           lvol.nqn, node_id[:8], e)
            return True
    return False


def move_namespace(lvol, target_nqn, *, client_swap_ready=False):
    """Move ``lvol``'s namespace into subsystem ``target_nqn`` on every node
    of its HA set, and switch its record. Break-before-make: the namespace is
    removed from its subsystem, then added to the target with one nsid shared
    by every node (the cluster-consistent-nsid rule).

    Refused unless :data:`NAMESPACE_MOVES_ENABLED`. Refused for a volume a host
    is connected to unless ``client_swap_ready`` (the client stages it behind
    the device-mapper indirection and swaps paths itself, design §5). On a
    failure after the removal the namespace is put back where it was.
    """
    if not NAMESPACE_MOVES_ENABLED:
        raise ColocationError("namespace moves between subsystems are disabled "
                              "(cg_colocation.NAMESPACE_MOVES_ENABLED)")
    if lvol.nqn == target_nqn:
        return lvol
    if not client_swap_ready and subsystem_has_hosts(lvol):
        raise ColocationError(
            f"volume {lvol.get_id()} is reachable by a connected host through "
            f"{lvol.nqn}; moving it needs the client's device-mapper swap")
    from simplyblock_core.controllers import lvol_controller
    db = _db()
    nodes = [db.get_storage_node_by_id(n) for n in (lvol.nodes or [lvol.node_id])]
    rpcs = [n.rpc_client() for n in nodes]

    primary_target = rpcs[0].subsystem_get(target_nqn)
    if not primary_target:
        raise ColocationError(f"target subsystem {target_nqn} does not exist on "
                              f"{nodes[0].get_id()[:8]}")
    used = {ns["nsid"] for ns in primary_target.get("namespaces") or []}
    for rpc in rpcs[1:]:
        sub = rpc.subsystem_get(target_nqn) or {}
        used |= {ns["nsid"] for ns in sub.get("namespaces") or []}
    new_nsid = 1
    while new_nsid in used:
        new_nsid += 1

    old_nqn, old_nsid = lvol.nqn, lvol.ns_id
    removed: list = []
    for node, rpc in zip(nodes, rpcs):
        if not lvol_controller._remove_lvol_subsys_from_node(lvol, rpc):
            _restore(lvol, removed, old_nqn, old_nsid)
            raise ColocationError(f"removing {lvol.get_id()} from {old_nqn} on "
                                  f"{node.get_id()[:8]} failed")
        removed.append(rpc)
    added: list = []
    for node, rpc in zip(nodes, rpcs):
        try:
            rpc.nvmf_subsystem_add_ns2(target_nqn, lvol.top_bdev, lvol.get_ns_uuid(),
                                       lvol.guid, nsid=new_nsid)
        except RPCRemoteError as e:
            for a in added:
                a.nvmf_subsystem_remove_ns(target_nqn, new_nsid)
            _restore(lvol, removed, old_nqn, old_nsid)
            raise ColocationError(f"adding {lvol.get_id()} to {target_nqn} on "
                                  f"{node.get_id()[:8]} failed: {e}") from e
        added.append(rpc)
    root = next((x for x in db.get_lvols(nodes[0].cluster_id)
                 if x.nqn == target_nqn and x.get_id() != lvol.get_id()), None)
    lvol.nqn = target_nqn
    lvol.ns_id = new_nsid
    if root is not None:
        lvol.namespace = root.namespace or root.get_id()
        lvol.max_namespace_per_subsys = root.max_namespace_per_subsys
    lvol.write_to_db(db.kv_store)
    logger.info("Moved namespace of %s from %s to %s (nsid %d)",
                lvol.get_id(), old_nqn, target_nqn, new_nsid)
    return lvol


def _restore(lvol, rpcs, nqn, nsid):
    for rpc in rpcs:
        try:
            rpc.nvmf_subsystem_add_ns2(nqn, lvol.top_bdev, lvol.get_ns_uuid(), lvol.guid, nsid=nsid)
        except Exception as e:  # noqa: BLE001
            logger.error("Restoring %s into %s failed: %s", lvol.get_id(), nqn, e)


def colocate_new_member(group, lvol, group_nqn):
    """Create-time forcing when the group's subsystem was full: flip a
    non-member namespace out of it into ``lvol``'s subsystem and move ``lvol``
    (brand new, so no host is attached to it yet) into the group's.

    Returns a one-line outcome; never raises (the member is valid where it is,
    co-location is an efficiency the design treats as best effort until the
    client swap is deployed).
    """
    if not NAMESPACE_MOVES_ENABLED:
        return "deferred: namespace moves between subsystems are disabled"
    try:
        from simplyblock_core import constants
        db = _db()
        on_node = [x for x in db.get_lvols_by_node_id(lvol.node_id) if _alive(x)]
        own = [x for x in on_node if x.nqn == lvol.nqn]
        room = min(lvol.max_namespace_per_subsys, constants.MAX_NAMESPACES_PER_SUBSYSTEM) - len(own)
        if room < 1:
            return f"deferred: {lvol.nqn} has no free slot for the namespace flipped out"
        in_group_subsys = [x for x in on_node if x.nqn == group_nqn]
        attached = subsystem_has_hosts(in_group_subsys[0]) if in_group_subsys else False
        from simplyblock_core.controllers import migration_controller
        migrating = [x.get_id() for x in in_group_subsys
                     if migration_controller.get_active_migration_for_lvol(x.get_id())]
        victim = choose_flip_victim(in_group_subsys,
                                    attached_ids=[x.get_id() for x in in_group_subsys] if attached else (),
                                    migrating_ids=migrating)
        if victim is None:
            return "deferred: every namespace of the group's subsystem is in a consistency group"
        if victim.attached:
            return (f"deferred: flip-out candidate {victim.lvol_id} is attached; moving it "
                    f"needs the client's device-mapper swap")
        move_namespace(db.get_lvol_by_id(victim.lvol_id), lvol.nqn)
        move_namespace(lvol, group_nqn)
        return f"co-located: {victim.lvol_id} flipped out to {lvol.nqn}"
    except Exception as e:  # noqa: BLE001
        return f"deferred: {e}"


def late_join_plan(group, lvol):
    """:func:`plan_late_join` against the cluster's records, with the
    colocation outcome the join would end in. Reserves nothing."""
    lvols, groups = _cluster_records(group.cluster_id)
    plan = plan_late_join(group, lvol, lvols, groups)
    if group.node_id and _open_members(group) - {lvol.get_id()}:
        plan.colocation = _colocation_outcome(group, lvol, lvols, plan)
    return plan


def colocate_member(group, lvol, *, client_swap_ready=False):
    """Move a current member into its group's subsystem (the late join's last
    step), flipping a non-member out when the subsystem is full. Raises
    ColocationError naming why it cannot happen now."""
    from simplyblock_core import constants
    if lvol.get_id() not in _open_members(group):
        raise ColocationError(f"volume {lvol.get_id()} is not a member of group {group.get_id()}")
    plan = late_join_plan(group, lvol)
    if STEP_COLOCATE not in plan.steps:
        return lvol
    db = _db()
    in_target = [x for x in db.get_lvols_by_node_id(lvol.node_id)
                 if _alive(x) and x.nqn == plan.target_nqn]
    cap = min(in_target[0].max_namespace_per_subsys, constants.MAX_NAMESPACES_PER_SUBSYSTEM) if in_target else 0
    if in_target and len(in_target) >= cap:
        victim = choose_flip_victim(in_target)
        if victim is None:
            raise ColocationError(f"subsystem {plan.target_nqn} is full of consistency-group members")
        own = [x for x in db.get_lvols_by_node_id(lvol.node_id) if _alive(x) and x.nqn == lvol.nqn]
        if len(own) >= min(lvol.max_namespace_per_subsys, constants.MAX_NAMESPACES_PER_SUBSYSTEM):
            # The swap needs the slot this member frees; a full own subsystem
            # has none until the member left, which it cannot before the
            # victim did. A three-way move through a fresh subsystem is the
            # follow-up (design §4.3).
            raise ColocationError(f"no free slot in {lvol.nqn} for the namespace flipped out")
        move_namespace(db.get_lvol_by_id(victim.lvol_id), lvol.nqn,
                       client_swap_ready=client_swap_ready)
    return move_namespace(lvol, plan.target_nqn, client_swap_ready=client_swap_ready)

