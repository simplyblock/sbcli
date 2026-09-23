#!/usr/bin/env python3
"""Rewrite one storage node's core layout in FDB (the node object) to a new
layout, keeping number_of_distribs unchanged (it cannot change on a live node).
The host half is ht_enable_sn_config.py; the LAYOUT block must be identical in both.

Usage (inside a control-plane container, node must be OFFLINE/suspended):
    python3 ht_enable_snode.py <node_uuid>                      # dry run, prints diff
    python3 ht_enable_snode.py <node_uuid> --apply              # writes to FDB + backup json
    python3 ht_enable_snode.py <node_uuid> --restore            # dry run of restoring the backup
    python3 ht_enable_snode.py <node_uuid> --restore --apply    # writes the backup values back
"""
import json
import os
import sys

from simplyblock_core import utils
from simplyblock_core.db_controller import DBController

# === LAYOUT: keep identical in ht_enable_sn_config.py =========================
EXPECTED_DISTRIBS = 4  # the node's number_of_distribs; refuse anything else
CPU_MASK = "0x500002aaaaaa00000000000000000000500002aaaaaa00000000000000000000"
ISOLATED = [81, 83, 85, 87, 89, 91, 93, 95, 97, 99, 101, 103, 105, 124, 126,
            209, 211, 213, 215, 217, 219, 221, 223, 225, 227, 229, 231, 233, 252, 254]
DISTRIBUTION = {  # l-core indices into ISOLATED
    "app_thread_core": [0],
    "jm_cpu_core": [15],
    "poller_cpu_cores": [10, 25, 11, 26, 12, 27, 13, 28, 14, 29, 18, 24, 8, 23, 9],
    "alceml_cpu_cores": [2, 17, 3],
    "alceml_worker_cpu_cores": [],
    # primary lvstore is recreated first at restart and takes the first
    # number_of_distribs entries: 4 physical cores first, siblings after
    "distrib_cpu_cores": [4, 5, 6, 7, 19, 20, 21, 22],
    "jc_singleton_core": [1],
    "lvol_poller_core": [16],  # may share its index with jc_singleton_core only
}
LARGE_POOL_COUNT = 13908
HT_SIBLING_OFFSET = 128  # host CPU c and c+offset are HT siblings; None = skip check (no HT / VM)
# === FDB-only =================================================================
CPU_COUNT = 256  # host logical CPUs after the change; None to leave snode.cpu as-is
# ==============================================================================

FIELDS = [
    "spdk_cpu_mask", "l_cores", "app_thread_mask", "jc_singleton_mask",
    "jm_cpu_mask", "lvol_poller_mask", "pollers_mask", "poller_cpu_cores",
    "alceml_cpu_cores", "alceml_cpu_index", "alceml_worker_cpu_cores",
    "alceml_worker_cpu_index", "distrib_cpu_cores", "distrib_cpu_index",
    "cpu", "iobuf_large_pool_count", "number_of_distribs",
]


def l_cores():
    return ",".join(f"{i}@{c}" for i, c in enumerate(ISOLATED))


def sanity_check():
    n = len(ISOLATED)
    assert n == len(set(ISOLATED)), "ISOLATED has duplicates"
    assert sum(1 << c for c in ISOLATED) == int(CPU_MASK, 16), "CPU_MASK != ISOLATED"
    assert len(DISTRIBUTION["jc_singleton_core"]) == 1, "jc_singleton_core must be exactly one core"
    lvp = set(DISTRIBUTION["lvol_poller_core"])
    used = [i for k, v in DISTRIBUTION.items() if k != "lvol_poller_core" for i in v]
    assert len(used) == len(set(used)), "an l-core index is given to two roles"
    assert not lvp & (set(used) - set(DISTRIBUTION["jc_singleton_core"])), \
        "lvol_poller_core may share only with jc_singleton_core"
    all_used = set(used) | lvp
    assert all_used == set(range(n)), f"unassigned or out-of-range l-core indices: {sorted(all_used ^ set(range(n)))}"
    assert len(DISTRIBUTION["distrib_cpu_cores"]) >= EXPECTED_DISTRIBS, "fewer distrib cores than distribs"
    if HT_SIBLING_OFFSET:
        dist = {ISOLATED[i] for i in DISTRIBUTION["distrib_cpu_cores"]}
        for c in dist:
            assert c + HT_SIBLING_OFFSET in dist or c - HT_SIBLING_OFFSET in dist, \
                f"distrib host CPU {c} has no HT sibling in the distrib set"


def apply_layout(snode):
    if snode.number_of_distribs != EXPECTED_DISTRIBS:
        sys.exit(f"expected number_of_distribs={EXPECTED_DISTRIBS}, node has {snode.number_of_distribs}")
    d = DISTRIBUTION
    # same string as the host config's cpu_mask: restart/add-node compare them as strings
    snode.spdk_cpu_mask = CPU_MASK
    snode.l_cores = l_cores()
    snode.app_thread_mask = utils.generate_mask(d["app_thread_core"])
    snode.jc_singleton_mask = utils.decimal_to_hex_power_of_2(d["jc_singleton_core"][0])
    snode.jm_cpu_mask = utils.generate_mask(d["jm_cpu_core"])
    snode.lvol_poller_mask = utils.generate_mask(d["lvol_poller_core"])
    snode.poller_cpu_cores = d["poller_cpu_cores"]
    snode.pollers_mask = utils.generate_mask(d["poller_cpu_cores"])
    snode.alceml_cpu_cores = d["alceml_cpu_cores"]
    snode.alceml_cpu_index = 0
    snode.alceml_worker_cpu_cores = d["alceml_worker_cpu_cores"]
    snode.alceml_worker_cpu_index = 0
    snode.distrib_cpu_cores = d["distrib_cpu_cores"]
    # no distrib_cpu_mask: that node field is unused; each distrib gets its own
    # single-core mask from distrib_cpu_cores[distrib_cpu_index] when recreated
    snode.distrib_cpu_index = 0
    if CPU_COUNT:
        snode.cpu = CPU_COUNT
    snode.iobuf_large_pool_count = LARGE_POOL_COUNT


def restore_backup(snode, backup_file):
    if not os.path.exists(backup_file):
        sys.exit(f"backup not found: {backup_file}")
    with open(backup_file) as fh:
        saved = json.load(fh)
    missing = set(FIELDS) - set(saved)
    if missing:
        sys.exit(f"backup is missing fields: {sorted(missing)}")
    for f in FIELDS:
        setattr(snode, f, saved[f])


def main():
    if len(sys.argv) < 2 or sys.argv[1].startswith("--"):
        sys.exit(__doc__)
    node_id = sys.argv[1]
    apply, restore = "--apply" in sys.argv, "--restore" in sys.argv
    backup_file = f"snode_{node_id}_cores_backup.json"

    db = DBController()
    snode = db.get_storage_node_by_id(node_id)
    before = {f: getattr(snode, f) for f in FIELDS}

    if restore:
        restore_backup(snode, backup_file)
    else:
        sanity_check()
        # never clobber a backup: a second --apply would save the new values over the originals
        if apply and os.path.exists(backup_file):
            sys.exit(f"{backup_file} already exists -- move it away first if you really mean to re-apply")
        apply_layout(snode)

    changed = False
    for f in FIELDS:
        old, new = before[f], getattr(snode, f)
        if old != new:
            changed = True
            print(f"{f}:\n  - {old}\n  + {new}")
    if not changed:
        print("nothing to change")
        return

    if not apply:
        print("\ndry run -- re-run with --apply to write")
        return
    if not restore:
        with open(backup_file, "w") as fh:
            json.dump(before, fh, indent=2)
    snode.write_to_db(db.kv_store)
    print("\nrestored from " + backup_file if restore else f"\nwritten; backup in {backup_file}")


if __name__ == "__main__":
    main()
