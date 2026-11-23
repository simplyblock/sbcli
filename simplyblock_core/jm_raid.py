"""
Journal-Manager (JM) RAID topology planner.

The per-node JM device backs the cluster journal. It used to be an N-way RAID1
mirror across the JM partition of *every* drive, so one journal record cost N
physical writes per node — and at FTT=2 the journal is replicated to 4 JMs, so
a 10-drive node meant ~40 device writes per record.

To cap that without hitting SPDK's ``raid5f`` full-stripe-write limitation (the
JM's storage writes single blocks, raid5f takes whole stripes only), the JM
uses a **RAID 0+1** layout over groups of at most GROUP_MAX_DEVICES devices:

    1 device     -> no raid (single-device / test-only node)
    2 devices    -> raid1 over two single-device legs          (a 2-way mirror)
    3-8 devices  -> two ±1 balanced groups, raid0 over each,
                    raid1 over the two raid0 legs
    9-12 devices -> three such groups, raid1 over three raid0 legs
    and so on: one more leg for every GROUP_MAX_DEVICES devices.

One journal record costs one device write per leg. A device that is only
missing costs the leg it sits in, and the JM runs on the others; the leg is
rebuilt from them when the device is back. A device that leaves for good, or
a new one, changes the member list: the raid is then built anew, empty, over
the new list, and the journal (JC) layer resyncs the records from the node's
other journal copies (device_controller.reshape_jm_raid).

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


#: Most devices one raid0 leg stripes over. A node with more devices gets more
#: legs, not wider ones: a leg is lost with any one of its devices.
GROUP_MAX_DEVICES = 4


def split_groups(items: list) -> list[list]:
    """Split ``items`` into the fewest groups of at most GROUP_MAX_DEVICES,
    but at least two, whose sizes differ by at most one.

    The first groups get the extra elements, so e.g. 3->(2,1), 4->(2,2),
    5->(3,2), 8->(4,4), 9->(3,3,3), 10->(4,3,3), 12->(4,4,4), 13->(4,3,3,3).
    Up to eight items this is the split into two halves every RAID 0+1 JM was
    built with before the groups were capped.
    """
    count = max(2, -(-len(items) // GROUP_MAX_DEVICES))
    base, extra = divmod(len(items), count)
    groups, start = [], 0
    for index in range(count):
        size = base + (1 if index < extra else 0)
        groups.append(list(items[start:start + size]))
        start += size
    return groups


def plan_topology(members: list[str], layout: str = LAYOUT_RAID01, legs=None) -> dict:
    """Decide the JM RAID topology for the given base bdevs.

    ``layout`` selects the geometry and MUST match the one the JM's storage was
    first created under (see the module docstring on why a mismatch corrupts
    the read):

      * LAYOUT_RAID01  -> RAID 0+1 (the current default for new clusters).
      * LAYOUT_LEGACY  -> one N-way RAID1 mirror across every member (the
                          pre-RAID0+1 layout that clusters upgraded from an
                          older release were built with).

    ``legs`` are the leg member lists the JM was built with, when a record
    of them exists. A JM that holds a journal is rebuilt with exactly those:
    the split of a member list is a rule that can change (it did, when the
    groups were capped at GROUP_MAX_DEVICES), the bytes on the drives cannot.
    They are used when they cover ``members`` exactly, and ignored otherwise
    (a record of another member list says nothing about this one).

    Returns:
        {'level': RAID_NONE,    'base': <bdev>, 'legs': []}                 1 member
        {'level': RAID_1_NWAY,  'base': None,   'members': [all]}           legacy, >=2
        {'level': RAID_0PLUS1,  'base': None,   'legs': [group, group, ...]} raid01, >=2

    Raises ValueError if there are no members.
    """
    n = len(members)
    if n == 0:
        raise ValueError("cannot plan a JM RAID over zero devices")
    if n == 1:
        return {"level": RAID_NONE, "base": members[0], "legs": []}
    if layout == LAYOUT_LEGACY:
        return {"level": RAID_1_NWAY, "base": None, "members": list(members)}
    recorded = [list(leg) for leg in legs or [] if leg]
    if len(recorded) >= 2 and [m for leg in recorded for m in leg] == list(members):
        return {"level": RAID_0PLUS1, "base": None, "legs": recorded}
    return {"level": RAID_0PLUS1, "base": None, "legs": split_groups(members)}


def layout_of_record(members: list[str], raid_bdev: str, leg_bdevs: list[str]) -> str:
    """The layout a JM record was built under, read from its shape.

    Recorded legs mean RAID 0+1. A raid over two or more members without
    recorded legs is the legacy N-way mirror, whose build path never recorded
    any. A single member is no raid at all and belongs to neither: it is read
    as RAID 0+1, the layout it grows into when the node gets a second device.
    """
    if leg_bdevs or len(members) < 2 or not raid_bdev:
        return LAYOUT_RAID01
    return LAYOUT_LEGACY


def reshaped_members(current: list[str], gone, added) -> list[str]:
    """The member list of a JM raid after its node lost the devices of
    ``gone`` for good and gained those of ``added``.

    The members that stay keep their order and the new ones follow, so the
    list does not depend on the order the changes were noticed in. The raid
    over the result is built empty (device_controller.reshape_jm_raid), so no
    member has to keep the position it was written in.
    """
    gone = set(gone)
    kept = [m for m in current if m not in gone]
    return kept + [m for m in dict.fromkeys(added) if m not in kept and m not in gone]


def is_jm_raid_bdev(name: str, node_id: str) -> bool:
    """Whether the raid bdev ``name`` is part of the JM raid of ``node_id``:
    the top raid1 or one of its raid0 legs (_create_jm_stack_on_raid names
    them raid_jm_<node> and raid_jm_<node>_l<index>)."""
    top = f"raid_jm_{node_id}"
    return name == top or name.startswith(f"{top}_l")


def usable_members(legs: list[list]) -> int:
    """How many members' worth of space a RAID 0+1 JM over ``legs`` can use:
    the top raid1 is as large as its smallest leg."""
    return min(len(leg) for leg in legs) if legs else 1


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
