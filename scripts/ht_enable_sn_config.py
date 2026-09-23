#!/usr/bin/env python3
"""Storage-node-host half of the core-layout change: rewrite one node entry in
sn_config_file (and its _read_only twin) to a new layout, keeping
number_of_distribs unchanged. The FDB half is ht_enable_snode.py; the LAYOUT
block must be identical in both.

Stdlib only; run on the storage node host as root.

Usage:
    sudo python3 ht_enable_sn_config.py                      # dry run, prints diff
    sudo python3 ht_enable_sn_config.py --apply              # writes both files + backup
    sudo python3 ht_enable_sn_config.py --restore            # dry run of restoring the backup
    sudo python3 ht_enable_sn_config.py --restore --apply    # writes the backup back
Options:
    --config PATH   default /etc/simplyblock/sn_config_file
                    (k8s hosts: /var/simplyblock/sn_config_file)
"""
import json
import os
import shutil
import sys

# === LAYOUT: keep identical in ht_enable_snode.py =============================
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
# === host-only: which entry in sn_config_file to change =======================
SOCKET = 1
SSD_PCIS = {"0000:a1:00.0", "0000:81:00.0"}
# ==============================================================================


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


def find_entry(cfg):
    hits = [n for n in cfg["nodes"]
            if n["socket"] == SOCKET and SSD_PCIS & set(n.get("ssd_pcis") or [])]
    if len(hits) != 1:
        sys.exit(f"expected exactly one node entry for socket={SOCKET} ssds={sorted(SSD_PCIS)}, found {len(hits)}")
    return hits[0]


def recompute_top_level(cfg):
    # same as persist_node_config: union of every entry's isolated list
    all_iso = sorted({c for n in cfg["nodes"] for c in (n.get("isolated") or [])})
    cfg["isolated_cores"] = all_iso
    cfg["host_cpu_mask"] = f"0x{sum(1 << c for c in all_iso):X}"


def write_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(data, fh, indent=4)
    os.replace(tmp, path)


def diff(old, new, prefix=""):
    for k in sorted(set(old) | set(new)):
        if old.get(k) != new.get(k):
            print(f"{prefix}{k}:\n  - {old.get(k)}\n  + {new.get(k)}")


def main():
    args = sys.argv[1:]
    apply, restore = "--apply" in args, "--restore" in args
    path = args[args.index("--config") + 1] if "--config" in args else "/etc/simplyblock/sn_config_file"
    ro_path, backup = f"{path}_read_only", f"{path}.pre_ht_backup"

    with open(path) as fh:
        cfg = json.load(fh)

    if restore:
        if not os.path.exists(backup):
            sys.exit(f"backup not found: {backup}")
        with open(backup) as fh:
            new = json.load(fh)
    else:
        sanity_check()
        if apply and os.path.exists(backup):
            sys.exit(f"{backup} already exists -- move it away first if you really mean to re-apply")
        new = json.loads(json.dumps(cfg))
        entry = find_entry(new)
        if entry["number_of_distribs"] != EXPECTED_DISTRIBS:
            sys.exit(f"expected number_of_distribs={EXPECTED_DISTRIBS}, entry has {entry['number_of_distribs']}")
        entry["cpu_mask"] = CPU_MASK
        entry["isolated"] = ISOLATED
        entry["l-cores"] = l_cores()
        # keep keys this layout doesn't set (e.g. compression_core)
        entry["distribution"] = {**entry.get("distribution", {}), **DISTRIBUTION}
        entry["large_pool_count"] = LARGE_POOL_COUNT
        recompute_top_level(new)

    if new == cfg:
        print("nothing to change")
        return
    for i, (o, n) in enumerate(zip(cfg["nodes"], new["nodes"])):
        diff(o, n, prefix=f"nodes[{i}].")
    diff({k: v for k, v in cfg.items() if k != "nodes"}, {k: v for k, v in new.items() if k != "nodes"})

    if not apply:
        print("\ndry run -- re-run with --apply to write")
        return
    if not restore:
        shutil.copy2(path, backup)
    # both files must stay identical, or /info returns an empty nodes_config
    # ("The nodes config has been changed, run configure-upgrade")
    write_json(path, new)
    write_json(ro_path, new)
    print(f"\nrestored from {backup}" if restore else f"\nwritten {path} + {ro_path}; backup in {backup}")


if __name__ == "__main__":
    main()
