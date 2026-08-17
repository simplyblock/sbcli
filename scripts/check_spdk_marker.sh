#!/usr/bin/env bash
# Run on a storage node: find the SPDK target binary and probe it for
# placement/anti-affinity log markers (control strings included).
set -u
CN=$(sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' | head -1)
echo "container=$CN"
sudo docker exec -u root "$CN" bash -s <<'INNER'
set -u
BIN=$(ls -1 /proc/1/exe 2>/dev/null; true)
CAND=$(readlink -f /proc/1/exe 2>/dev/null)
echo "pid1 exe: ${CAND:-unknown}"
for p in "$CAND" /root/spdk/build/bin/spdk_tgt /usr/local/bin/spdk_tgt /root/spdk/build/bin/ultra /usr/bin/ultra; do
    [ -n "${p:-}" ] && [ -f "$p" ] && { echo "using: $p"; TGT="$p"; break; }
done
if [ -z "${TGT:-}" ]; then echo "NO_BINARY_FOUND"; exit 0; fi
ls -la "$TGT"
for pat in "Failed to find available location" "Placement selection dump" "anti-affinity" "fault tolerance degraded"; do
    n=$(strings "$TGT" 2>/dev/null | grep -c -- "$pat")
    printf '  [%s] %s\n' "$n" "$pat"
done
INNER
