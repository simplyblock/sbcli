#!/usr/bin/env bash
# Bound fio's --write_iolog history to KEEP_SEC seconds per volume.
#
# Why hole-punching: fio holds each iolog open and writes sequentially, so
# truncating would leave it writing past a shortened file and produce a sparse
# mess. fallocate --punch-hole frees the blocks of the OLD head while leaving
# file length and fio's write offsets untouched, so the recent tail -- the part
# that explains a verify failure -- stays readable and disk stays bounded.
# Reads of the punched region return zeros; that history is deliberately gone.
#
# Why the cadence is 60s and not 300s: on 2026-08-26 these logs grew 6.3 GiB in
# four minutes (~95 GB/h across six volumes) and filled the client's 8.8G root
# before a 300s first pass ever ran. A full disk under fio also manufactures
# I/O errors, so overshoot is not merely a space problem. At 60s the worst-case
# overshoot is about one minute of logs.
#
# iolog lines carry no timestamps, so retention is enforced from the observed
# growth rate, measured over the ACTUAL gap between passes.
#
# usage: iolog_trimmer.sh <dir> <keep_seconds> [interval=60] [first_pass=30]
set -u
DIR="${1:?dir}"
KEEP_SEC="${2:?keep seconds}"
INTERVAL="${3:-60}"
FIRST_PASS="${4:-30}"
cd "$DIR" || exit 0
declare -A prev
echo "$(date -u +%H:%M:%S) trimmer: dir=$DIR keep=${KEEP_SEC}s interval=${INTERVAL}s first=${FIRST_PASS}s"

gap="$FIRST_PASS"
while true; do
  sleep "$gap"
  for f in iolog_vol*.log*; do
    [ -f "$f" ] || continue
    sz=$(stat -c %s "$f" 2>/dev/null) || continue
    p=${prev[$f]:-0}
    prev[$f]=$sz
    if [ "$p" -eq 0 ]; then
      # No rate yet. Still protect the disk with an absolute ceiling so the
      # very first window cannot run away before a rate is known.
      hard=$(( 4 * 1024 * 1024 * 1024 ))
      if [ "$sz" -gt "$hard" ]; then
        hole=$(( sz - hard ))
        fallocate -p -o 0 -l "$hole" "$f" 2>/dev/null \
          && echo "$(date -u +%H:%M:%S) $f: first-pass cap, punched $hole B (kept ${hard}B)"
      fi
      continue
    fi
    rate=$(( (sz - p) / gap ))
    [ "$rate" -le 0 ] && continue
    keep=$(( rate * KEEP_SEC ))
    hole=$(( sz - keep ))
    if [ "$hole" -gt 1048576 ]; then
      if fallocate -p -o 0 -l "$hole" "$f" 2>/dev/null; then
        echo "$(date -u +%H:%M:%S) $f: punched $hole B, kept ${keep}B (~${KEEP_SEC}s at ${rate} B/s)"
      else
        echo "$(date -u +%H:%M:%S) $f: punch-hole unsupported -- iolog will grow UNBOUNDED"
      fi
    fi
  done
  gap="$INTERVAL"
done
