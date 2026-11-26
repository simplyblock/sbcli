"""
Journal-Manager (JM) RAID topology planner.

The per-node JM device backs the cluster journal. It used to be an N-way RAID1
mirror across the JM partition of *every* drive, so one journal record cost N
physical writes per node — and at FTT=2 the journal is replicated to 4 JMs, so
a 10-drive node meant ~40 device writes per record.

To cap that at 2× per node (independent of drive count) without hitting SPDK's
``raid5f`` full-stripe-write limitation, the JM uses a **RAID 0+1** layout:

    1 device   -> no raid (single-device / test-only node)
    2 devices  -> raid1 over two single-device legs            (a 2-way mirror)
    > 2 devices-> split drives into two ±1 balanced groups,
                  raid0 over each group, raid1 over the two raid0 legs

The top RAID1 is always exactly 2-wide, so every drive lifecycle event reduces
to "rebuild the affected leg's raid0 and let RAID1 resync"; only when *both*
legs are lost does the journal (JC) layer resync the records from peer JMs.

This module is pure logic (no RPC, no DB) so the decisions are unit-testable.
The orchestration that issues the bdev_raid RPCs lives in storage_node_ops.
"""


RAID_NONE = "none"        # single base device, no raid bdev
RAID_0PLUS1 = "raid01"    # raid1 over two raid0 legs (2 devices => 2-way mirror)
RAID_1_NWAY = "raid1_nway"  # legacy: one raid1 mirror across ALL members

# Cluster-level JM RAID geometry selectors (Cluster.jm_raid_layout).
#
# The geometry is NOT recorded on disk -- the JM raid is created with
# superblock=False -- so a change to this planner silently reinterprets the
# same on-disk bytes under a different layout. RAID 0+1 stripes each leg's data
# 4 KiB at a time across its drives, whereas the legacy N-way RAID1 kept a full
# linear copy on every drive; reading legacy-written journal storage back
# through RAID 0+1 (or vice versa) returns scrambled bytes, so the alceml PBA
# header / journal records / distrib superblock fail to parse. (Production
# incident 2026-09-08: an old cluster whose journals were built N-way was
# rebuilt RAID 0+1 on upgrade; every LVS superblock read back "unsupported
# version" and activation failed. No data was written -- a fresh RAID1 create
# with both legs present neither resyncs nor writes -- so it was recoverable by
# reproducing the original geometry.)
#
# Therefore a cluster's JM layout is pinned once and reproduced forever:
LAYOUT_LEGACY = "legacy_nway"   # journals built by the pre-RAID0+1 N-way mirror
LAYOUT_RAID01 = "raid01"        # journals built by the RAID 0+1 planner


def split_two_groups(items: list) -> tuple[list, list]:
    """Split ``items`` into two groups whose sizes differ by at most one.

    The first group gets the extra element when the count is odd, so e.g.
    3->(2,1), 4->(2,2), 5->(3,2), 6->(3,3).
    """
    half = (len(items) + 1) // 2
    return list(items[:half]), list(items[half:])


def plan_topology(members: list[str], layout: str = LAYOUT_RAID01) -> dict:
    """Decide the JM RAID topology for the given base bdevs.

    ``layout`` selects the geometry and MUST match the one the JM's storage was
    first created under (see the module docstring on why a mismatch corrupts
    the read):

      * LAYOUT_RAID01  -> RAID 0+1 (the current default for new clusters).
      * LAYOUT_LEGACY  -> one N-way RAID1 mirror across every member (the
                          pre-RAID0+1 layout that clusters upgraded from an
                          older release were built with).

    Returns:
        {'level': RAID_NONE,    'base': <bdev>, 'legs': []}                 1 member
        {'level': RAID_1_NWAY,  'base': None,   'members': [all]}           legacy, >=2
        {'level': RAID_0PLUS1,  'base': None,   'legs': [groupA, groupB]}   raid01, >=2

    Raises ValueError if there are no members.
    """
    n = len(members)
    if n == 0:
        raise ValueError("cannot plan a JM RAID over zero devices")
    if n == 1:
        return {"level": RAID_NONE, "base": members[0], "legs": []}
    if layout == LAYOUT_LEGACY:
        return {"level": RAID_1_NWAY, "base": None, "members": list(members)}
    a, b = split_two_groups(members)
    return {"level": RAID_0PLUS1, "base": None, "legs": [a, b]}


def member_size_for_usable(usable_bytes: int, member_count: int) -> int:
    """Size every member must have for a RAID 0+1 JM of ``usable_bytes``.

    The members are all of one size (a raid uses each member only up to the
    size of its smallest one). The top raid1 is as large as its smaller leg,
    and that leg stripes ``member_count // 2`` members, so the usable size is
    ``member_size * (member_count // 2)``: 2 members of S give S, 3 give S (the
    third one's S is unused), 4 give 2S, 5 give 2S. A single member is the
    journal itself.
    """
    if member_count < 1:
        raise ValueError("cannot size a JM RAID over zero devices")
    width = max(1, member_count // 2)
    return -(-int(usable_bytes) // width)


def is_balanced(legs: list[list]) -> bool:
    """True if the two legs' member counts differ by at most one."""
    return abs(len(legs[0]) - len(legs[1])) <= 1


def smaller_leg_index(legs: list[list]) -> int:
    """Index of the leg a new device should join (the smaller one; ties -> 0)."""
    return 0 if len(legs[0]) <= len(legs[1]) else 1


def plan_reconfigure(legs: list[list], *, failed=None, added=None) -> dict:
    """Plan the leg rebuild(s) for a drive lifecycle event.

    Args:
        legs: current two legs, each a list of member bdev names.
        failed: a member (or list of members) permanently removed/failed. Each
            is dropped from whichever leg holds it, and that leg is rebuilt.
        added: a new (or re-inserted) member bdev. It joins the smaller leg,
            which is then rebuilt. A re-inserted drive that's no longer in any
            leg is handled here too — it's simply "added".

    Returns:
        {'rebuild': [sorted leg indices to destroy+recreate],
         'legs':    [new leg membership lists]}

    RAID0 cannot grow or drop a member in place, so any membership change means
    destroying and recreating that leg's raid0 (then RAID1 resyncs it from the
    surviving leg). A leg that loses its last member becomes empty — the caller
    runs the array degraded on the surviving leg until a drive is available.
    """
    new = [list(legs[0]), list(legs[1])]
    rebuild = set()

    if failed is not None:
        failed_list = failed if isinstance(failed, (list, tuple, set)) else [failed]
        for dev in failed_list:
            for i in (0, 1):
                if dev in new[i]:
                    new[i].remove(dev)
                    rebuild.add(i)

    if added is not None:
        i = smaller_leg_index(new)
        new[i].append(added)
        rebuild.add(i)

    return {"rebuild": sorted(rebuild), "legs": new}


def leg_of(member: str, leg_bdevs: list[str], leg_members: list[list[str]]) -> str:
    """The top-raid1 base bdev that carries ``member``.

    Under RAID 0+1 with more than two drives a member partition sits inside a
    raid0 leg, so the unit the top raid1 adds, removes and rebuilds is that
    leg. A single-drive leg is the bare partition, and the legacy N-way mirror
    (no recorded legs) has the partitions themselves as its base bdevs.
    """
    for leg, members in zip(leg_bdevs, leg_members):
        if member in members:
            return leg
    return member


def mountable_top_legs(top_legs: list[str], missing: list[str], stale: list[str]) -> list[str]:
    """The top-raid1 base bdevs a JM can be assembled from when some of them
    cannot be built, in their planned order; empty when none holds the
    current journal.

    ``top_legs`` are the base bdevs of the planned geometry, ``missing`` those
    of them that cannot be built because a member drive is absent, ``stale``
    those recorded as having missed writes. The geometry is never re-planned
    over the drives that happen to be there: the raid has no superblock, so
    the surviving drives only read back correctly in the positions they were
    written in. A leg of a mirror is a complete copy on its own, so the JM
    runs on what is left -- unless every leg left is stale, in which case
    there is no current copy on this node at all.
    """
    present = [leg for leg in top_legs if leg not in missing]
    if len(present) == len(top_legs):
        return present
    if all(leg in stale for leg in present):
        return []
    return present


def runs_on_single_leg(raid_bdev: str, members: list[str], leg_bdevs: list[str]) -> bool:
    """True if a multi-drive JM was assembled on one surviving leg alone, with
    no top raid1 (see mountable_top_legs): its ``raid_bdev`` is then that leg
    -- a member partition or a raid0 -- and not a mirror a leg can be added
    to or removed from. The mirror comes back on the next node restart that
    finds every member."""
    return len(members) > 1 and (raid_bdev in members or raid_bdev in leg_bdevs)


def leg_in_sync(raid_info: dict, leg: str) -> bool:
    """True if ``leg`` is a configured base bdev of the raid described by
    ``raid_info`` (one ``bdev_raid_get_bdevs`` entry) and no rebuild targets it.
    """
    process = raid_info.get("process") or {}
    if process.get("target") == leg:
        return False
    return any(b.get("name") == leg and b.get("is_configured", True)
               for b in raid_info.get("base_bdevs_list", []))
