"""The newest replicated recovery point of a consistency group.

Invariant (2026-10-04): the newest replicated generation of a consistency group
is never deleted by an automatic path, on either side -- the source snapshots
taken of it and their replicated copies on the peer. It is the point a promote
on the peer restores the group from, and the point a relocate back clones the
group home from after the demoted source volumes were deleted (which a relocate
requires: the returning clone would collide with them). Detach, fail-back,
retention and clone clean-up consult :func:`group_recovery_point_ids` before
deleting an internal replication snapshot.

A generation is *complete* when every snapshot taken of it on the group's own
cluster has a live replicated copy. When none of its origin snapshots survives,
its surviving copies stand for it. Generations are ordered by when they were
taken, not by their number: a group whose members all left restarts nothing,
but records made before the counter was kept monotonic may restart at 1.
"""
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.snapshot import SnapShot


def _live(snap):
    return (snap is not None and not getattr(snap, "deleted", False)
            and getattr(snap, "status", "") != SnapShot.STATUS_IN_DELETION)


def _partner_id(snap):
    return (getattr(snap, "target_replicated_snap_uuid", "")
            or getattr(snap, "source_replicated_snap_uuid", ""))


def _gid(group_id):
    return (group_id or "").split("/")[-1]


def newest_group_generation(group_id, db=None):
    """``(seq, origin_snapshots, copies)`` of the newest complete replicated
    generation of *group_id*, or ``(0, [], [])`` when there is none."""
    db = db or DBController()
    gid = _gid(group_id)
    if not gid:
        return 0, [], []
    try:
        origin_cluster = db.get_consistency_group_by_id(group_id).cluster_id
    except KeyError:
        origin_cluster = ""
    snaps = [s for s in db.get_snapshots()
             if _gid(getattr(s, "group_id", "")) == gid
             and getattr(s, "group_seq", 0) and _live(s)]
    by_id = {s.get_id(): s for s in snaps}
    by_seq: dict = {}
    for s in snaps:
        by_seq.setdefault(s.group_seq, []).append(s)

    def taken_at(seq):
        return max((getattr(s, "created_at", 0) or 0) for s in by_seq[seq])

    for seq in sorted(by_seq, key=lambda q: (taken_at(q), q), reverse=True):
        members = by_seq[seq]
        if origin_cluster:
            origins = [s for s in members if getattr(s, "cluster_id", "") == origin_cluster]
        else:
            origins = [s for s in members if getattr(s, "target_replicated_snap_uuid", "")
                       and not getattr(s, "source_replicated_snap_uuid", "")]
        origin_ids = {s.get_id() for s in origins}
        copies = [s for s in members if s.get_id() not in origin_ids]
        if not copies:
            continue
        copy_ids = {s.get_id() for s in copies}
        if all(_partner_id(o) in by_id and _partner_id(o) in copy_ids for o in origins):
            return seq, origins, copies
    return 0, [], []


def group_recovery_point_ids(group_ids, db=None):
    """Ids of every snapshot -- origin and copy -- of the newest complete
    generation of each group in *group_ids*: the snapshots no automatic path
    may delete."""
    db = db or DBController()
    keep = set()
    for gid in {_gid(g) for g in group_ids if _gid(g)}:
        _, origins, copies = newest_group_generation(gid, db=db)
        keep.update(s.get_id() for s in origins + copies)
    return keep


def protected_by_group(snap, db=None):
    """True when *snap* belongs to the newest complete generation of its group."""
    gid = _gid(getattr(snap, "group_id", ""))
    if not gid:
        return False
    return snap.get_id() in group_recovery_point_ids([gid], db=db)
