#!/usr/bin/env bash
# Bound fio's --write_iolog history to KEEP_SEC seconds per volume.
#
# fio holds each iolog open and writes sequentially, so truncating it would
# leave fio writing past a shortened file and produce a sparse mess.
# fallocate --punch-hole frees the blocks of the OLD head while leaving the
# file length and fio's write offsets untouched, so the recent tail -- the part
# that explains a verify failure -- stays readable and disk stays bounded.
# Reads of the punched region return zeros, which is fine: that history is
# deliberately discarded.
#
# iolog lines carry no timestamps, so "the last hour" is enforced from the
# observed growth rate, re-measured every cycle rather than assumed.
#
# usage: iolog_trimmer.sh <run_dir> <keep_seconds> [interval_seconds]
set -u
RUN_DIR="${1:?run dir}"
KEEP_SEC="${2:?keep seconds}"
INTERVAL="${3:-300}"
cd "$RUN_DIR" || exit 0
declare -A prev
echo "$(date -u +%H:%M:%S) trimmer start: dir=$RUN_DIR keep=${KEEP_SEC}s interval=${INTERVAL}s"
while true; do
  sleep "$INTERVAL"
  for f in iolog_vol*.log*; do
    [ -f "$f" ] || continue
    sz=$(stat -c %s "$f" 2>/dev/null) || continue
    p=${prev[$f]:-0}
    prev[$f]=$sz
    [ "$p" -eq 0 ] && continue
    rate=$(( (sz - p) / INTERVAL ))
    [ "$rate" -le 0 ] && continue
    keep=$(( rate * KEEP_SEC ))
    hole=$(( sz - keep ))
    if [ "$hole" -gt 1048576 ]; then
      if fallocate -p -o 0 -l "$hole" "$f" 2>/dev/null; then
        echo "$(date -u +%H:%M:%S) $f: punched $hole B, keeping ${keep}B (~${KEEP_SEC}s at ${rate} B/s)"
      else
        echo "$(date -u +%H:%M:%S) $f: punch-hole unsupported here; iolog will grow unbounded"
      fi
    fi
  done
done
