#!/usr/bin/env bash
set -u
B=/root/spdk/ultra/build_bdts/bdts
ls -la "$B" 2>/dev/null || { echo "NO_BINARY $B"; exit 0; }
for p in "Failed to find available location" "Placement selection dump" "anti-affinity" "fault tolerance degraded" "affinity ladder"; do
    n=$(strings "$B" 2>/dev/null | grep -c -F -- "$p")
    printf '  [%s] %s\n' "$n" "$p"
done
