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
* a member that cannot be reached at all (connection refused / no answer), or
  that answers without holding the bdev yet, is skipped with a warning -- and
  the change it missed is recorded (:class:`ReplicaChainPending`) and replayed
  once it answers with the bdev (:func:`replay_pending`, driven by the health
  check of every node, and before a control-plane leadership grant). A reload
  of the lvstore (restart) reads the tree from disk; the replay then finds the
  change already there and only drops the record;
* a member that answers with an error fails the step, so the caller retries
  (a caller whose step is non-fatal asks for the change to be recorded
  instead: nobody would retry it).

Both helpers are idempotent, so a retried step does not trip over a member an
earlier attempt already handled: ``add_clone`` is not ("File exists").
"""

import time

from simplyblock_core import db_controller, utils
from simplyblock_core.models.replica_chain_pending import ReplicaChainPending
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import (
    RPCConnectionError,
    RPCException,
    request_reached_remote,
)

logger = utils.get_logger(__name__)

#: Outcomes of :func:`add_clone_on` / :func:`convert_on`.
DONE = "done"        # the member has the change (now, or from an earlier attempt)
SKIPPED = "skipped"  # nothing to do there now: not reachable, or it lacks the bdev
FAILED = "failed"    # the member answered with an error: the step must retry

#: RPC budget per member. An unreachable member costs at most this (times the
#: client's retries) per call instead of the 180 s default.
RPC_TIMEOUT_SEC = 5
RPC_RETRY = 2

#: Upper bound of records one replay pass works through for one member.
REPLAY_BATCH = 200

_PENDING = ReplicaChainPending


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


def _lvs_of(name):
    return name.split("/", 1)[0] if name and "/" in name else ""


def _probe(node, role, bdev_name):
    """``(reached, info)``; *info* is None when the bdev is absent. Raises
    ``RPCException`` for an error answer, which the callers turn into a
    failed step."""
    try:
        return True, _rpc(node).bdev_get(bdev_name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s is not reachable (%s); %s is recorded as owed there",
                           role, node.get_id(), e, bdev_name)
            return False, None
        raise


def _lvol_info(info):
    if isinstance(info, list):
        info = info[0] if info else None
    return ((info or {}).get("driver_specific") or {}).get("lvol") or {}


def _is_snapshot(info):
    """Converted. SPDK's ``snapshot`` flag means "has clones": a converted
    lvol nothing is cloned from yet reports false, and only its write
    support going away shows the convert (probed on a live SPDK)."""
    if isinstance(info, list):
        info = info[0] if info else None
    lv = _lvol_info(info)
    if lv.get("is_snapshot") or lv.get("snapshot"):
        return True
    return bool(lv) and ((info or {}).get("supported_io_types") or {}).get("write") is False


def _names(info):
    """Short names a bdev answers to: its own name and every alias."""
    if isinstance(info, list):
        info = info[0] if info else None
    info = info or {}
    out = {_short(info.get("name"))}
    out.update(_short(a) for a in (info.get("aliases") or []))
    out.discard("")
    return out


def _composite(info, fallback):
    """The ``lvs/name`` alias of a bdev, else *fallback*."""
    if isinstance(info, list):
        info = info[0] if info else None
    for alias in ((info or {}).get("aliases") or []):
        if "/" in alias:
            return alias
    return fallback


def reachable(node, role="member"):
    """Whether *node*'s SPDK answers at all -- the rule this module judges a
    member by, instead of its DB status. An error answer is an answer."""
    try:
        _rpc(node).get_version()
        return True
    except RPCException as e:
        if _unreachable(e):
            logger.info("%s %s is not reachable: %s", role, node.get_id(), e)
            return False
        return True


# ---------------------------------------------------------------------------
# The calls
# ---------------------------------------------------------------------------

def _add_clone(node, role, clone_name, parent_name, strict=False):
    """``(outcome, error, reason)``; *reason* says why a SKIPPED member was
    skipped (ReplicaChainPending.REASON_*). *strict* (replay): a snapshot
    that does not read through *parent_name* is not taken as linked -- it was
    registered there after the leader converted it, not converted there."""
    nid = node.get_id()
    try:
        reached, info = _probe(node, role, clone_name)
        if not reached:
            return SKIPPED, None, _PENDING.REASON_UNREACHABLE
        if info is None:
            logger.warning("%s %s does not hold %s (yet); the link is recorded as owed",
                           role, nid, clone_name)
            return SKIPPED, None, _PENDING.REASON_ABSENT
        lv = _lvol_info(info)
        if lv.get("base_snapshot") and _short(lv.get("base_snapshot")) == _short(parent_name):
            logger.info("%s already reads through %s on %s %s", clone_name, parent_name, role, nid)
            return DONE, None, ""
        # Replay only: a member without the parent is owed the link. A live
        # step lets SPDK answer (-19) and fails, as before, so it retries
        # rather than converting a member it could not link (split state).
        reached, parent = _probe(node, role, parent_name) if strict else (True, True)
        if not reached:
            return SKIPPED, None, _PENDING.REASON_UNREACHABLE
        if parent is None:
            logger.warning("%s %s does not hold %s (yet); the link of %s is recorded as owed",
                           role, nid, parent_name, clone_name)
            return SKIPPED, None, _PENDING.REASON_ABSENT
        if _is_snapshot(info) and strict:
            logger.warning("%s is a snapshot on %s %s that does not read through %s; "
                           "it needs a reload of the lvstore there", clone_name, role, nid,
                           parent_name)
            return SKIPPED, None, _PENDING.REASON_ABSENT
        if _is_snapshot(info):
            # converted by an earlier attempt, which linked it first (a convert
            # never runs on a member that still owes the link: convert_on)
            logger.info("%s is already a snapshot on %s %s; not re-linking", clone_name, role, nid)
            return DONE, None, ""
        ret = _rpc(node).bdev_lvol_add_clone(clone_name, parent_name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s became unreachable during add_clone %s -> %s: %s",
                           role, nid, clone_name, parent_name, e)
            return SKIPPED, None, _PENDING.REASON_UNREACHABLE
        return FAILED, f"bdev_lvol_add_clone {clone_name} -> {parent_name} on {role} {nid}: {e}", ""
    if not ret:
        return FAILED, f"bdev_lvol_add_clone {clone_name} -> {parent_name} failed on {role} {nid}", ""
    logger.info("add_clone %s -> %s on %s %s", clone_name, parent_name, role, nid)
    return DONE, None, ""


def _convert(node, role, name):
    nid = node.get_id()
    try:
        reached, info = _probe(node, role, name)
        if not reached:
            return SKIPPED, None, _PENDING.REASON_UNREACHABLE
        if info is None:
            logger.warning("%s %s does not hold %s (yet); the convert is recorded as owed",
                           role, nid, name)
            return SKIPPED, None, _PENDING.REASON_ABSENT
        if _is_snapshot(info):
            logger.info("%s is already a snapshot on %s %s", name, role, nid)
            return DONE, None, ""
        ret = _rpc(node).bdev_lvol_convert(name)
    except RPCException as e:
        if _unreachable(e):
            logger.warning("%s %s became unreachable during convert %s: %s", role, nid, name, e)
            return SKIPPED, None, _PENDING.REASON_UNREACHABLE
        return FAILED, f"bdev_lvol_convert {name} on {role} {nid}: {e}", ""
    if not ret:
        return FAILED, f"bdev_lvol_convert {name} failed on {role} {nid}", ""
    logger.info("convert %s on %s %s", name, role, nid)
    return DONE, None, ""


def add_clone_on(node, role, clone_name, parent_name, owner=None, record=True,
                 record_failed=False, db=None):
    """Link *clone_name* under *parent_name* on *node*. Returns
    ``(outcome, error)``: DONE when linked now or already linked, SKIPPED
    when the member is not reachable or does not hold *clone_name*, FAILED
    (with the error) when it answered with an error.

    A SKIPPED member is recorded as owing the link (unless *record* is False)
    and gets it from :func:`replay_pending`. *owner*: the lvstore's primary,
    for the record. *record_failed*: also record a FAILED member -- for a
    caller whose step is non-fatal and so is not retried."""
    outcome, err, reason = _add_clone(node, role, clone_name, parent_name)
    if record and (outcome == SKIPPED or (outcome == FAILED and record_failed)):
        record_pending(node, role, _PENDING.OP_ADD_CLONE, clone_name, parent_name,
                       reason=reason or _PENDING.REASON_FAILED, error=err, owner=owner, db=db)
    return outcome, err


def convert_on(node, role, name, owner=None, record=True, record_failed=False, db=None):
    """Convert *name* into a snapshot on *node*. Returns ``(outcome, error)``
    as :func:`add_clone_on`: DONE (converted now or already a snapshot),
    SKIPPED (not reachable, or the member lacks *name*), FAILED.

    A member that still owes the link of *name* (an earlier add_clone was
    skipped there) is not converted before that link is replayed: a
    snapshot cannot be linked any more, and :func:`add_clone_on` reads an
    existing snapshot as already linked."""
    if record:
        owed = _owed_link(node, name, db)
        if owed is not None:
            outcome = _replay_record(owed, node, db, trusted=False)
            if outcome != DONE:
                logger.warning("%s %s still owes the link of %s; its convert is owed too",
                               role, node.get_id(), name)
                record_pending(node, role, _PENDING.OP_CONVERT, name,
                               reason=_PENDING.REASON_ABSENT, owner=owner, db=db)
                return SKIPPED, None
    outcome, err, reason = _convert(node, role, name)
    if record and (outcome == SKIPPED or (outcome == FAILED and record_failed)):
        record_pending(node, role, _PENDING.OP_CONVERT, name,
                       reason=reason or _PENDING.REASON_FAILED, error=err, owner=owner, db=db)
    return outcome, err


def add_clone_on_replicas(primary_node, clone_name, parent_name, exclude_ids=(), db=None,
                          record_failed=False):
    """:func:`add_clone_on` for every replica of *primary_node*'s lvstore.
    Returns None on success, else the first error (the rest still ran).
    *record_failed*: see :func:`add_clone_on` (non-fatal callers)."""
    first_err = None
    for role, node in lvstore_replicas(primary_node, exclude_ids, db):
        outcome, err = add_clone_on(node, role, clone_name, parent_name,
                                    owner=primary_node, record_failed=record_failed, db=db)
        if outcome == FAILED and first_err is None:
            first_err = err
    return first_err


def convert_on_replicas(primary_node, name, exclude_ids=(), db=None):
    """:func:`convert_on` for every replica of *primary_node*'s lvstore.
    Returns None on success, else the first error (the rest still ran)."""
    first_err = None
    for role, node in lvstore_replicas(primary_node, exclude_ids, db):
        outcome, err = convert_on(node, role, name, owner=primary_node, db=db)
        if outcome == FAILED and first_err is None:
            first_err = err
    return first_err


# ---------------------------------------------------------------------------
# What a member still owes
# ---------------------------------------------------------------------------

def _kv(db):
    return (db or db_controller.DBController()).kv_store


def _uuid_of(name, candidates):
    """The uuid of bdev *name*, read from the first candidate that answers
    with it. The uuid is the same on every member (a replica registers the
    leader's), and survives the rename a migration ends with."""
    for node in candidates:
        if node is None:
            continue
        try:
            info = _rpc(node).bdev_get(name)
        except Exception:                                  # noqa: BLE001
            continue
        if isinstance(info, list):
            info = info[0] if info else None
        if info and info.get("uuid"):
            return info["uuid"]
    return ""


def record_pending(node, role, op, name, parent_name="", reason="", error=None,
                   owner=None, db=None):
    """Record that *node* owes *op* on *name* (and *parent_name*). Never
    raises: a failed record is logged, the caller's step goes on."""
    try:
        db = db or db_controller.DBController()
        candidates = [owner] + [n for _, n in (lvstore_replicas(owner, (node.get_id(),), db)
                                               if owner is not None else [])]
        rec = ReplicaChainPending()
        rec.node_id = node.get_id()
        rec.role = role
        rec.primary_node_id = owner.get_id() if owner is not None else ""
        rec.lvs_name = _lvs_of(name)
        rec.op = op
        rec.bdev_name = name
        rec.bdev_uuid = _uuid_of(name, candidates)
        if op == _PENDING.OP_ADD_CLONE:
            rec.parent_name = parent_name
            rec.parent_uuid = _uuid_of(parent_name, candidates)
        rec.reason = reason
        rec.seq = time.time_ns()
        rec.last_error = error or ""
        rec.write_to_db(_kv(db))
        logger.warning("%s %s owes %s of %s%s (%s); recorded, replayed when it answers",
                       role, rec.node_id, op, name,
                       f" -> {parent_name}" if parent_name else "", reason)
    except Exception as e:                                 # noqa: BLE001
        logger.error("Could not record that %s %s owes %s of %s: %s",
                     role, node.get_id(), op, name, e)


def pending_for(node_id, db=None, lvs_name=None):
    """The changes *node_id* owes, oldest first."""
    try:
        recs = _PENDING().read_from_db(_kv(db), id=f"{node_id}:")
    except Exception as e:                                 # noqa: BLE001
        logger.error("Could not read the changes %s owes: %s", node_id, e)
        return []
    recs = [r for r in recs if r.node_id == node_id
            and (lvs_name is None or r.lvs_name == lvs_name)]
    return sorted(recs, key=lambda r: r.seq)


def _drop(rec, db, why):
    logger.info("%s %s: owed %s of %s settled (%s)", rec.role, rec.node_id, rec.op,
                rec.bdev_name, why)
    try:
        rec.remove(_kv(db))
    except Exception as e:                                 # noqa: BLE001
        logger.error("Could not drop the record %s: %s", rec.get_id(), e)


def _keep(rec, db, err=None):
    rec.attempts += 1
    if err:
        rec.last_error = err
    try:
        rec.write_to_db(_kv(db))
    except Exception as e:                                 # noqa: BLE001
        logger.error("Could not update the record %s: %s", rec.get_id(), e)


def _ref(rec):
    return rec.bdev_uuid or rec.bdev_name


def _owed_link(node, name, db):
    """The add_clone record *node* holds for *name*, or None."""
    for rec in pending_for(node.get_id(), db):
        if rec.op == _PENDING.OP_ADD_CLONE and rec.bdev_name == name:
            return rec
    return None


def _leader_of(rec, member, db):
    """``(leader, owner)``: the member of *rec*'s lvstore that leads it now
    (None when none can be found), and the lvstore's owner (None when it, or
    *member*'s membership, is gone)."""
    try:
        owner = db.get_storage_node_by_id(rec.primary_node_id) if rec.primary_node_id else None
    except KeyError:
        return None, None
    if owner is None:
        return None, None
    members = [owner] + [n for _, n in lvstore_replicas(owner, db=db)]
    if member.get_id() not in {n.get_id() for n in members}:
        return None, None
    for node in members:
        if node.get_id() == member.get_id() or node.status == StorageNode.STATUS_REMOVED:
            continue
        try:
            lvs = _rpc(node).get_lvstore(rec.lvs_name)
        except Exception:                                  # noqa: BLE001
            continue
        if lvs and lvs.get("lvs leadership"):
            return node, owner
    return None, owner


def _still_wanted(rec, leader):
    """Whether the leader still has the change *rec* records: True / False,
    or None when the leader could not tell (try again later)."""
    try:
        rpc = _rpc(leader)
        info = rpc.bdev_get(rec.bdev_uuid or rec.bdev_name)
        if info is None:
            return False
        if rec.op == _PENDING.OP_CONVERT:
            return _is_snapshot(info)
        parent = rpc.bdev_get(rec.parent_uuid or rec.parent_name)
        if parent is None:
            return False
        base = _lvol_info(info).get("base_snapshot")
        return bool(base) and _short(base) in _names(parent)
    except Exception as e:                                 # noqa: BLE001
        logger.warning("Leader %s could not confirm owed %s of %s: %s",
                       leader.get_id(), rec.op, rec.bdev_name, e)
        return None


def _member_name(member, ref, fallback):
    """``(reached, name)`` of the bdev *ref* (uuid or name) on *member*; name
    is None when the member does not hold it."""
    try:
        info = _rpc(member).bdev_get(ref)
    except RPCException as e:
        if _unreachable(e):
            return False, None
        raise
    if info is None:
        return True, None
    return True, _composite(info, fallback)


def _replay_record(rec, member, db, trusted=False):
    """Replay one owed change on *member*. Returns DONE (settled: done now,
    already there, or no longer wanted -- the record is dropped), SKIPPED or
    FAILED (the record is kept).

    The change is replayed only while the lvstore's current leader still
    has it (the bdev, its link, its convert): a record outlives deletes,
    rollbacks and re-links, and replaying a stale one would corrupt the tree
    it is meant to repair. *trusted*: replay without that check -- only for a
    leaderless lvstore about to be granted to *member*, which has nobody to
    ask (the records are the leader's own earlier changes)."""
    if not trusted:
        leader, owner = _leader_of(rec, member, db)
        if owner is None:
            _drop(rec, db, "the lvstore or this membership is gone")
            return DONE
        if leader is None:
            logger.info("%s %s owes %s of %s, but no other member leads %s now; kept",
                        rec.role, rec.node_id, rec.op, rec.bdev_name, rec.lvs_name)
            return SKIPPED
        wanted = _still_wanted(rec, leader)
        if wanted is None:
            return SKIPPED
        if not wanted:
            _drop(rec, db, f"leader {leader.get_id()} no longer has it")
            return DONE
    try:
        reached, name = _member_name(member, rec.bdev_uuid or rec.bdev_name, rec.bdev_name)
        parent = None
        if reached and name and rec.op == _PENDING.OP_ADD_CLONE:
            reached, parent = _member_name(member, rec.parent_uuid or rec.parent_name,
                                           rec.parent_name)
    except RPCException as e:
        _keep(rec, db, str(e))
        return FAILED
    if not reached:
        return SKIPPED
    if not name or (rec.op == _PENDING.OP_ADD_CLONE and not parent):
        # not registered there yet: it gets the change once it is (a reload
        # of the lvstore brings it in with the tree from disk; a single
        # registration is not relied on to link it under its parent)
        return SKIPPED
    if rec.op == _PENDING.OP_ADD_CLONE:
        outcome, err, _ = _add_clone(member, rec.role, name, parent, strict=True)
    else:
        outcome, err, _ = _convert(member, rec.role, name)
    if outcome == DONE:
        _drop(rec, db, "replayed")
    elif outcome == FAILED:
        logger.error("Replay of owed %s of %s on %s %s failed: %s",
                     rec.op, rec.bdev_name, rec.role, rec.node_id, err)
        _keep(rec, db, err)
    return outcome


def _restart_owned(node_id, lvs_name):
    """Whether a restart / activation / expansion owns *lvs_name* on the
    node: it rebuilds the lvstore (and its tree) itself."""
    try:
        from simplyblock_core.storage_node_ops import get_restart_phase
        return bool(get_restart_phase(node_id, lvs_name))
    except Exception:                                      # noqa: BLE001
        return False


def replay_pending(node, db=None, lvs_name=None, trusted=False):
    """Replay every change *node* owes (optionally only for *lvs_name*),
    oldest first. Returns ``{"done": n, "skipped": n, "failed": n}``; a
    member without records costs one prefix read.

    A convert waits for an owed link of the same bdev (see :func:`convert_on`).
    """
    db = db or db_controller.DBController()
    counts = {"done": 0, "skipped": 0, "failed": 0}
    recs = pending_for(node.get_id(), db, lvs_name)[:REPLAY_BATCH]
    if not recs:
        return counts
    logger.info("%s owes %d snapshot-tree change(s); replaying", node.get_id(), len(recs))
    owed_links = set()
    restart_owned = {}
    for rec in recs:
        if rec.lvs_name not in restart_owned:
            restart_owned[rec.lvs_name] = _restart_owned(node.get_id(), rec.lvs_name)
        if restart_owned[rec.lvs_name]:
            counts["skipped"] += 1
            continue
        if rec.op == _PENDING.OP_CONVERT and _ref(rec) in owed_links:
            counts["skipped"] += 1
            continue
        outcome = _replay_record(rec, node, db, trusted=trusted)
        if outcome == DONE:
            counts["done"] += 1
            continue
        counts["failed" if outcome == FAILED else "skipped"] += 1
        if rec.op == _PENDING.OP_ADD_CLONE:
            owed_links.add(_ref(rec))
    return counts
