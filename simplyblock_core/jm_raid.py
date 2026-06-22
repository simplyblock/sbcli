# coding=utf-8
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

from typing import List, Tuple

RAID_NONE = "none"        # single base device, no raid bdev
RAID_0PLUS1 = "raid01"    # raid1 over two raid0 legs (2 devices => 2-way mirror)


def split_two_groups(items: list) -> Tuple[list, list]:
    """Split ``items`` into two groups whose sizes differ by at most one.

    The first group gets the extra element when the count is odd, so e.g.
    3->(2,1), 4->(2,2), 5->(3,2), 6->(3,3).
    """
    half = (len(items) + 1) // 2
    return list(items[:half]), list(items[half:])


def plan_topology(members: List[str]) -> dict:
    """Decide the JM RAID topology for the given base bdevs.

    Returns:
        {'level': RAID_NONE,   'base': <bdev>, 'legs': []}              for 1 member
        {'level': RAID_0PLUS1, 'base': None,   'legs': [groupA, groupB]} for >=2

    Raises ValueError if there are no members.
    """
    n = len(members)
    if n == 0:
        raise ValueError("cannot plan a JM RAID over zero devices")
    if n == 1:
        return {"level": RAID_NONE, "base": members[0], "legs": []}
    a, b = split_two_groups(members)
    return {"level": RAID_0PLUS1, "base": None, "legs": [a, b]}


def is_balanced(legs: List[list]) -> bool:
    """True if the two legs' member counts differ by at most one."""
    return abs(len(legs[0]) - len(legs[1])) <= 1


def smaller_leg_index(legs: List[list]) -> int:
    """Index of the leg a new device should join (the smaller one; ties -> 0)."""
    return 0 if len(legs[0]) <= len(legs[1]) else 1


def plan_reconfigure(legs: List[list], *, failed=None, added=None) -> dict:
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
