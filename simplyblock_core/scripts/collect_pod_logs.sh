#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 -n <namespace> -f <from> -d <duration> [-o <output-dir>]"
    echo ""
    echo "  -n  Kubernetes namespace"
    echo "  -f  Start time (RFC3339 or relative, e.g. '2026-04-08T10:00:00Z' or '2h' or '30m')"
    echo "  -d  Duration to collect from start time (e.g. '30m', '1h', '2h30m')"
    echo "  -o  Output directory (default: ./pod-logs)"
    echo ""
    echo "Examples:"
    echo "  $0 -n simplyblock -f 2026-04-08T10:00:00Z -d 1h"
    echo "  $0 -n simplyblock -f 2h -d 30m -o ./logs"
    exit 1
}

parse_duration_seconds() {
    local dur="$1"
    local total=0
    local tmp="$dur"

    if [[ "$tmp" =~ ^([0-9]+)h ]]; then total=$((total + ${BASH_REMATCH[1]} * 3600)); tmp="${tmp#*h}"; fi
    if [[ "$tmp" =~ ^([0-9]+)m ]];  then total=$((total + ${BASH_REMATCH[1]} * 60));   tmp="${tmp#*m}"; fi
    if [[ "$tmp" =~ ^([0-9]+)s ]];  then total=$((total + ${BASH_REMATCH[1]}));         fi

    echo "$total"
}

to_epoch() {
    local ts="$1"
    # If it looks like a relative duration (e.g. "2h", "30m"), treat as "now minus that offset"
    if [[ "$ts" =~ ^[0-9]+[hms] ]] || [[ "$ts" =~ ^[0-9]+h[0-9]+m ]]; then
        local secs
        secs=$(parse_duration_seconds "$ts")
        echo $(( $(date -u +%s) - secs ))
    else
        date -u -d "$ts" +%s 2>/dev/null || date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s
    fi
}

NAMESPACE=""
FROM=""
DURATION=""
OUTPUT_DIR="./pod-logs"

while getopts "n:f:d:o:" opt; do
    case $opt in
        n) NAMESPACE="$OPTARG" ;;
        f) FROM="$OPTARG" ;;
        d) DURATION="$OPTARG" ;;
        o) OUTPUT_DIR="$OPTARG" ;;
        *) usage ;;
    esac
done

[[ -z "$NAMESPACE" || -z "$FROM" || -z "$DURATION" ]] && usage

FROM_EPOCH=$(to_epoch "$FROM")
DURATION_SECS=$(parse_duration_seconds "$DURATION")
UNTIL_EPOCH=$(( FROM_EPOCH + DURATION_SECS ))

FROM_TS=$(date -u -d "@$FROM_EPOCH" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -r "$FROM_EPOCH" '+%Y-%m-%dT%H:%M:%SZ')
UNTIL_TS=$(date -u -d "@$UNTIL_EPOCH" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -r "$UNTIL_EPOCH" '+%Y-%m-%dT%H:%M:%SZ')

mkdir -p "$OUTPUT_DIR"

echo "Namespace  : $NAMESPACE"
echo "From       : $FROM_TS"
echo "Until      : $UNTIL_TS"
echo "Output dir : $OUTPUT_DIR"
echo ""

pods=$(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)

if [[ -z "$pods" ]]; then
    echo "No pods found in namespace: $NAMESPACE"
    exit 0
fi

for pod in $pods; do
    containers=$(kubectl get pod "$pod" -n "$NAMESPACE" \
        -o jsonpath='{range .spec.initContainers[*]}{.name}{"\n"}{end}{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null)

    for container in $containers; do
        log_file="${OUTPUT_DIR}/${pod}_${container}.log"
        echo "  -> $pod / $container"

        {
            echo "=== Pod: $pod | Container: $container | Namespace: $NAMESPACE ==="
            echo "=== From: $FROM_TS | Until: $UNTIL_TS ==="
            echo ""
            kubectl logs "$pod" -c "$container" -n "$NAMESPACE" \
                --timestamps \
                --since-time="$FROM_TS" 2>&1 \
            | awk -v until="$UNTIL_TS" '
                /^[0-9]{4}-[0-9]{2}-[0-9]{2}T/ {
                    split($1, a, /[TZ\+]/);
                    ts = a[1] "T" a[2] "Z";
                    if (ts > until) exit
                }
                { print }
            ' || true
        } > "$log_file"
    done
done

# --- dmesg from simplyblock-csi-node pods (container: csi-node) ---
echo ""
echo "Collecting dmesg from simplyblock-csi-node pods..."

csi_node_pods=$(kubectl get pods -n "$NAMESPACE" --no-headers \
    -o custom-columns=":metadata.name" 2>/dev/null \
    | grep '^simplyblock-csi-node' || true)

if [[ -z "$csi_node_pods" ]]; then
    echo "  No simplyblock-csi-node pods found."
else
    # dmesg timestamps are seconds since boot; compute the boot time of each pod's node
    # to convert dmesg relative times to wall clock and filter by window.
    for pod in $csi_node_pods; do
        dmesg_file="${OUTPUT_DIR}/${pod}_csi-node_dmesg.log"
        echo "  -> $pod / csi-node (dmesg)"

        {
            echo "=== Pod: $pod | Container: csi-node | dmesg ==="
            echo "=== From: $FROM_TS | Until: $UNTIL_TS ==="
            echo ""

            # Get node boot epoch: current time minus kernel uptime
            boot_epoch=$(kubectl exec "$pod" -c csi-node -n "$NAMESPACE" -- \
                awk '{print int(systime() - $1)}' /proc/uptime 2>/dev/null) || boot_epoch=0

            kubectl exec "$pod" -c csi-node -n "$NAMESPACE" -- \
                dmesg --kernel --time-format=reltime --nopager 2>/dev/null \
            | awk -v boot="$boot_epoch" -v from="$FROM_EPOCH" -v until="$UNTIL_EPOCH" '
                /^\[/ {
                    # reltime format: [Mar 8 10:00:00.000000]
                    # fall back to using dmesg monotonic seconds if reltime unavailable
                }
                { print }
            ' || \
            kubectl exec "$pod" -c csi-node -n "$NAMESPACE" -- \
                dmesg --kernel --nopager 2>/dev/null \
            | awk -v boot="$boot_epoch" -v from="$FROM_EPOCH" -v until="$UNTIL_EPOCH" '
                /^\[[ ]*([0-9]+\.[0-9]+)\]/ {
                    match($0, /\[[ ]*([0-9]+\.[0-9]+)\]/, a)
                    wall = boot + int(a[1])
                    if (wall < from) next
                    if (wall > until) exit
                }
                { print }
            ' || true
        } > "$dmesg_file"
    done
fi

echo ""
echo "Done. Logs written to: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"
