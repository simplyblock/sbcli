#!/usr/bin/env bash
set -u
B=/root/spdk/ultra/build_bdts/bdts
[ -f "$B" ] || { echo "  FAIL: $B missing"; exit 0; }
stat -c '  size=%s  mtime=%y' "$B"
for p in "fault tolerance degraded" "anti-affinity dropped" "Failed to find available location"; do
    printf '  [%s] %s\n' "$(strings -a "$B" 2>/dev/null | grep -c -F -- "$p")" "$p"
done
