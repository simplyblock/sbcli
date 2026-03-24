#!/bin/bash
# Local snapshot/clone churn driver for a management+client node.
# Phases:
# 1. Create one base volume and keep permanent fio running on it.
# 2. Create 240 snapshots and clone each snapshot as quickly as possible.
# 3. Randomly connect/disconnect 30 of those clones in batches of 10.
# 4. Delete all initial clones.
# 5. Run 10-minute churn cycles forever:
#    - randomly create/delete volumes, snapshots, and clones while keeping
#      live non-snapshot objects near the configured target
#    - after each cycle, randomly connect/disconnect 30 live volumes/clones
#      in batches of 10

set -euo pipefail

INITIAL_CLONES="${1:-240}"
POOL="${2:-pool01}"
BASE_VOLUME_SIZE="${3:-25G}"
TARGET_OBJECTS="${TARGET_OBJECTS:-220}"
CONNECT_SAMPLE_SIZE="${CONNECT_SAMPLE_SIZE:-30}"
CONNECT_BATCH_SIZE="${CONNECT_BATCH_SIZE:-10}"
CYCLE_SECONDS="${CYCLE_SECONDS:-600}"
CHURN_VOLUME_SIZE="${CHURN_VOLUME_SIZE:-1G}"
BASE_FIO_JOBS="${BASE_FIO_JOBS:-4}"
BASE_FIO_IODEPTH="${BASE_FIO_IODEPTH:-8}"
BASE_FIO_BS="${BASE_FIO_BS:-128k}"
BASE_FIO_RW="${BASE_FIO_RW:-randwrite}"
BASE_FIO_IOENGINE="${BASE_FIO_IOENGINE:-libaio}"
CHURN_SLEEP_SECS="${CHURN_SLEEP_SECS:-0.10}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
BASE_VOL_NAME="snapclone_base_${RUN_ID}"
BASE_MOUNT="/mnt/${BASE_VOL_NAME}"
LOGFILE="/tmp/snapshot_clone_loop_${RUN_ID}.log"
MAPFILE="/tmp/snapshot_clone_map_${RUN_ID}.tsv"
BASE_FIO_PID=""

declare -A OBJECT_TYPE=()
declare -A OBJECT_NAME=()
declare -A PROTECTED_OBJECT=()
declare -A SNAP_PARENT=()
declare -A SNAP_NAME=()
declare -A SNAP_CLONE_COUNT=()
declare -A PARENT_SNAPSHOT_COUNT=()
declare -A CLONE_SOURCE_SNAPSHOT=()

declare -a VOLUME_IDS=()
declare -a CLONE_IDS=()
declare -a SNAPSHOT_IDS=()
declare -a INITIAL_CLONE_IDS=()

BASE_VOL_UUID=""
BASE_DEVICE=""
RANDOM_VOL_COUNTER=0
RANDOM_SNAP_COUNTER=0
RANDOM_CLONE_COUNTER=0
INITIAL_CONNECT_OK=0
INITIAL_CONNECT_FAIL=0
CYCLE_CONNECT_OK=0
CYCLE_CONNECT_FAIL=0
CYCLE_NUMBER=0

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOGFILE"
}

extract_uuid() {
    grep -Eo '[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}' | tail -1
}

sleep_fraction() {
    python3 - "$1" <<'PY'
import sys, time
time.sleep(float(sys.argv[1]))
PY
}

nvme_devices() {
    lsblk -d -n -o NAME | grep -E '^nvme[0-9]+n1$' | sort || true
}

remove_array_value() {
    local value="$1"
    shift
    local -n arr_ref="$1"
    local filtered=()
    local item
    for item in "${arr_ref[@]:-}"; do
        if [ "$item" != "$value" ]; then
            filtered+=("$item")
        fi
    done
    arr_ref=("${filtered[@]}")
}

pick_random_from_array() {
    local -n arr_ref="$1"
    if [ "${#arr_ref[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${arr_ref[@]}" | shuf -n 1
}

lookup_lvol_uuid() {
    local name="$1"
    sbctl lvol list 2>&1 | awk -v name="$name" -F'|' '
        index($0, name) {
            for (i = 1; i <= NF; i++) {
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
                if ($i ~ /^[a-f0-9-]{36}$/) {
                    print $i
                    exit
                }
            }
        }'
}

lookup_snapshot_uuid() {
    local name="$1"
    sbctl snapshot list 2>&1 | awk -v name="$name" -F'|' '
        index($0, name) {
            for (i = 1; i <= NF; i++) {
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
                if ($i ~ /^[a-f0-9-]{36}$/) {
                    print $i
                    exit
                }
            }
        }'
}

register_volume() {
    local obj_id="$1"
    local name="$2"
    OBJECT_TYPE["$obj_id"]="volume"
    OBJECT_NAME["$obj_id"]="$name"
    PARENT_SNAPSHOT_COUNT["$obj_id"]="${PARENT_SNAPSHOT_COUNT[$obj_id]:-0}"
    VOLUME_IDS+=("$obj_id")
}

register_clone() {
    local obj_id="$1"
    local name="$2"
    local snapshot_id="$3"
    OBJECT_TYPE["$obj_id"]="clone"
    OBJECT_NAME["$obj_id"]="$name"
    CLONE_SOURCE_SNAPSHOT["$obj_id"]="$snapshot_id"
    SNAP_CLONE_COUNT["$snapshot_id"]=$(( ${SNAP_CLONE_COUNT[$snapshot_id]:-0} + 1 ))
    CLONE_IDS+=("$obj_id")
}

register_snapshot() {
    local snap_id="$1"
    local name="$2"
    local parent_id="$3"
    SNAP_PARENT["$snap_id"]="$parent_id"
    SNAP_NAME["$snap_id"]="$name"
    SNAP_CLONE_COUNT["$snap_id"]="${SNAP_CLONE_COUNT[$snap_id]:-0}"
    PARENT_SNAPSHOT_COUNT["$parent_id"]=$(( ${PARENT_SNAPSHOT_COUNT[$parent_id]:-0} + 1 ))
    SNAPSHOT_IDS+=("$snap_id")
}

unregister_clone() {
    local clone_id="$1"
    local snapshot_id="${CLONE_SOURCE_SNAPSHOT[$clone_id]:-}"
    if [ -n "$snapshot_id" ]; then
        SNAP_CLONE_COUNT["$snapshot_id"]=$(( ${SNAP_CLONE_COUNT[$snapshot_id]:-0} - 1 ))
        unset CLONE_SOURCE_SNAPSHOT["$clone_id"]
    fi
    unset OBJECT_TYPE["$clone_id"]
    unset OBJECT_NAME["$clone_id"]
    remove_array_value "$clone_id" CLONE_IDS
    remove_array_value "$clone_id" INITIAL_CLONE_IDS
}

unregister_volume() {
    local vol_id="$1"
    unset OBJECT_TYPE["$vol_id"]
    unset OBJECT_NAME["$vol_id"]
    unset PROTECTED_OBJECT["$vol_id"]
    unset PARENT_SNAPSHOT_COUNT["$vol_id"]
    remove_array_value "$vol_id" VOLUME_IDS
}

unregister_snapshot() {
    local snap_id="$1"
    local parent_id="${SNAP_PARENT[$snap_id]:-}"
    if [ -n "$parent_id" ]; then
        PARENT_SNAPSHOT_COUNT["$parent_id"]=$(( ${PARENT_SNAPSHOT_COUNT[$parent_id]:-0} - 1 ))
    fi
    unset SNAP_PARENT["$snap_id"]
    unset SNAP_NAME["$snap_id"]
    unset SNAP_CLONE_COUNT["$snap_id"]
    remove_array_value "$snap_id" SNAPSHOT_IDS
}

live_object_count() {
    echo $(( ${#VOLUME_IDS[@]} + ${#CLONE_IDS[@]} ))
}

pick_random_volume_for_snapshot() {
    local candidates=()
    local vid
    for vid in "${VOLUME_IDS[@]:-}"; do
        candidates+=("$vid")
    done
    if [ "${#candidates[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${candidates[@]}" | shuf -n 1
}

pick_random_snapshot_for_clone() {
    if [ "${#SNAPSHOT_IDS[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${SNAPSHOT_IDS[@]}" | shuf -n 1
}

pick_random_clone_for_delete() {
    if [ "${#CLONE_IDS[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${CLONE_IDS[@]}" | shuf -n 1
}

pick_random_volume_for_delete() {
    local candidates=()
    local vid
    for vid in "${VOLUME_IDS[@]:-}"; do
        if [ "${PROTECTED_OBJECT[$vid]:-0}" = "1" ]; then
            continue
        fi
        if [ "${PARENT_SNAPSHOT_COUNT[$vid]:-0}" -eq 0 ]; then
            candidates+=("$vid")
        fi
    done
    if [ "${#candidates[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${candidates[@]}" | shuf -n 1
}

pick_random_snapshot_for_delete() {
    local candidates=()
    local sid
    for sid in "${SNAPSHOT_IDS[@]:-}"; do
        if [ "${SNAP_CLONE_COUNT[$sid]:-0}" -eq 0 ]; then
            candidates+=("$sid")
        fi
    done
    if [ "${#candidates[@]}" -eq 0 ]; then
        return 1
    fi
    printf '%s\n' "${candidates[@]}" | shuf -n 1
}

connect_lvol_get_device() {
    local lvol_id="$1"
    local before after device connect_out
    before="$(nvme_devices)"
    connect_out="$(sbctl lvol connect "$lvol_id" 2>&1 | tee -a "$LOGFILE")"
    if ! printf '%s\n' "$connect_out" | grep -q '^sudo nvme connect'; then
        return 1
    fi
    while IFS= read -r cmd; do
        [ -n "$cmd" ] || continue
        eval "$cmd" >>"$LOGFILE" 2>&1
    done < <(printf '%s\n' "$connect_out" | grep '^sudo nvme connect')
    sleep_fraction 2
    after="$(nvme_devices)"
    device="$(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") | head -n 1)"
    if [ -z "$device" ]; then
        return 1
    fi
    echo "$device"
}

disconnect_device() {
    local device="$1"
    sudo nvme disconnect -d "${device%n1}" >>"$LOGFILE" 2>&1 || true
}

connect_disconnect_sample() {
    local sample_size="$1"
    local batch_size="$2"
    local phase_label="$3"
    local pool=("${VOLUME_IDS[@]}" "${CLONE_IDS[@]}")
    local sample=()
    local batch=()
    local obj_id device obj_name ok_count=0 fail_count=0

    if [ "${#pool[@]}" -eq 0 ]; then
        log "$phase_label: no live objects to connect"
        return 0
    fi

    mapfile -t sample < <(printf '%s\n' "${pool[@]}" | shuf -n "$sample_size")
    if [ "${#sample[@]}" -eq 0 ]; then
        log "$phase_label: empty sample"
        return 0
    fi

    log "$phase_label: connect/disconnect ${#sample[@]} objects in batches of $batch_size"

    local idx=0
    while [ "$idx" -lt "${#sample[@]}" ]; do
        batch=("${sample[@]:idx:batch_size}")
        declare -a connected=()
        log "$phase_label: batch $(( idx / batch_size + 1 )) starting"
        for obj_id in "${batch[@]}"; do
            obj_name="${OBJECT_NAME[$obj_id]:-$obj_id}"
            if device="$(connect_lvol_get_device "$obj_id")"; then
                log "$phase_label: connect OK $obj_name $obj_id -> $device"
                connected+=("$device")
                ok_count=$((ok_count + 1))
            else
                log "$phase_label: connect FAIL $obj_name $obj_id"
                fail_count=$((fail_count + 1))
            fi
        done

        sleep_fraction 2

        for device in "${connected[@]:-}"; do
            disconnect_device "$device"
            log "$phase_label: disconnect OK $device"
        done
        idx=$((idx + batch_size))
    done

    if [ "$phase_label" = "initial-sample" ]; then
        INITIAL_CONNECT_OK=$((INITIAL_CONNECT_OK + ok_count))
        INITIAL_CONNECT_FAIL=$((INITIAL_CONNECT_FAIL + fail_count))
    else
        CYCLE_CONNECT_OK=$((CYCLE_CONNECT_OK + ok_count))
        CYCLE_CONNECT_FAIL=$((CYCLE_CONNECT_FAIL + fail_count))
    fi
}

create_base_volume() {
    local create_out
    log "Creating base volume $BASE_VOL_NAME"
    create_out="$(sbctl lvol add "$BASE_VOL_NAME" "$BASE_VOLUME_SIZE" "$POOL" 2>&1 | tee -a "$LOGFILE" || true)"
    BASE_VOL_UUID="$(printf '%s\n' "$create_out" | extract_uuid)"
    if [ -z "$BASE_VOL_UUID" ]; then
        BASE_VOL_UUID="$(lookup_lvol_uuid "$BASE_VOL_NAME" || true)"
    fi
    if [ -z "$BASE_VOL_UUID" ]; then
        log "ERROR: failed to create base volume"
        exit 1
    fi
    register_volume "$BASE_VOL_UUID" "$BASE_VOL_NAME"
    PROTECTED_OBJECT["$BASE_VOL_UUID"]="1"
    log "Base volume UUID: $BASE_VOL_UUID"
}

prepare_base_workload() {
    local fio_target
    log "Connecting base volume $BASE_VOL_UUID"
    BASE_DEVICE="$(connect_lvol_get_device "$BASE_VOL_UUID")"
    log "Base volume connected as /dev/$BASE_DEVICE"
    if ! command -v fio >/dev/null 2>&1; then
        log "ERROR: fio not found"
        exit 1
    fi

    sudo mkfs.xfs -f "/dev/$BASE_DEVICE" >>"$LOGFILE" 2>&1
    sudo mkdir -p "$BASE_MOUNT"
    sudo mount "/dev/$BASE_DEVICE" "$BASE_MOUNT"
    fio_target="$BASE_MOUNT/fio_base_file"
    log "Starting permanent fio on $fio_target jobs=$BASE_FIO_JOBS iodepth=$BASE_FIO_IODEPTH rw=$BASE_FIO_RW bs=$BASE_FIO_BS"
    sudo fio \
        --name="base_fio_${RUN_ID}" \
        --filename="$fio_target" \
        --size=20G \
        --rw="$BASE_FIO_RW" \
        --bs="$BASE_FIO_BS" \
        --ioengine="$BASE_FIO_IOENGINE" \
        --direct=1 \
        --iodepth="$BASE_FIO_IODEPTH" \
        --numjobs="$BASE_FIO_JOBS" \
        --time_based=1 \
        --runtime=31536000 \
        --group_reporting \
        >>"$LOGFILE" 2>&1 &
    BASE_FIO_PID="$!"
    sleep_fraction 3
    if ! kill -0 "$BASE_FIO_PID" 2>/dev/null; then
        log "ERROR: fio failed to stay running on /dev/$BASE_DEVICE"
        exit 1
    fi
    log "Base fio running with PID $BASE_FIO_PID"
}

run_initial_clone_burst() {
    local i snap_name clone_name snap_out clone_out snap_uuid clone_uuid
    printf 'iteration\tsnapshot_name\tsnapshot_uuid\tclone_name\tclone_uuid_or_status\n' >"$MAPFILE"
    log "Starting initial snapshot/clone burst: $INITIAL_CLONES iterations"
    for i in $(seq 1 "$INITIAL_CLONES"); do
        snap_name="${BASE_VOL_NAME}_snap_${i}"
        clone_name="${BASE_VOL_NAME}_clone_${i}"

        snap_out="$(sbctl snapshot add "$BASE_VOL_UUID" "$snap_name" 2>&1 | tee -a "$LOGFILE" || true)"
        snap_uuid="$(printf '%s\n' "$snap_out" | extract_uuid)"
        if [ -z "$snap_uuid" ]; then
            snap_uuid="$(lookup_snapshot_uuid "$snap_name" || true)"
        fi
        if [ -z "$snap_uuid" ]; then
            log "Initial iteration $i: snapshot create failed for $snap_name"
            printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$snap_name" "FAILED" "$clone_name" "SNAPSHOT_FAILED" >>"$MAPFILE"
            continue
        fi
        register_snapshot "$snap_uuid" "$snap_name" "$BASE_VOL_UUID"

        clone_out="$(sbctl snapshot clone "$snap_uuid" "$clone_name" 2>&1 | tee -a "$LOGFILE" || true)"
        clone_uuid="$(printf '%s\n' "$clone_out" | extract_uuid)"
        if [ -z "$clone_uuid" ]; then
            clone_uuid="$(lookup_lvol_uuid "$clone_name" || true)"
        fi
        if [ -z "$clone_uuid" ]; then
            log "Initial iteration $i: clone create failed for $clone_name from $snap_uuid"
            printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$snap_name" "$snap_uuid" "$clone_name" "CLONE_FAILED" >>"$MAPFILE"
            continue
        fi

        register_clone "$clone_uuid" "$clone_name" "$snap_uuid"
        INITIAL_CLONE_IDS+=("$clone_uuid")
        printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$snap_name" "$snap_uuid" "$clone_name" "$clone_uuid" >>"$MAPFILE"
    done
    log "Initial burst complete: ${#INITIAL_CLONE_IDS[@]} clones created successfully, map=$MAPFILE"
}

delete_clone_id() {
    local clone_id="$1"
    local name="${OBJECT_NAME[$clone_id]:-$clone_id}"
    if sbctl -d lvol delete "$clone_id" >>"$LOGFILE" 2>&1; then
        unregister_clone "$clone_id"
        log "Deleted clone $name $clone_id"
        return 0
    fi
    log "Delete clone failed $name $clone_id"
    return 1
}

delete_volume_id() {
    local vol_id="$1"
    local name="${OBJECT_NAME[$vol_id]:-$vol_id}"
    if sbctl -d lvol delete "$vol_id" >>"$LOGFILE" 2>&1; then
        unregister_volume "$vol_id"
        log "Deleted volume $name $vol_id"
        return 0
    fi
    log "Delete volume failed $name $vol_id"
    return 1
}

delete_snapshot_id() {
    local snap_id="$1"
    local name="${SNAP_NAME[$snap_id]:-$snap_id}"
    if sbctl -d snapshot delete "$snap_id" >>"$LOGFILE" 2>&1; then
        unregister_snapshot "$snap_id"
        log "Deleted snapshot $name $snap_id"
        return 0
    fi
    log "Delete snapshot failed $name $snap_id"
    return 1
}

delete_all_initial_clones() {
    local clone_id
    log "Deleting all initial clones: ${#INITIAL_CLONE_IDS[@]}"
    for clone_id in "${INITIAL_CLONE_IDS[@]:-}"; do
        delete_clone_id "$clone_id" || true
    done
    INITIAL_CLONE_IDS=()
}

create_random_volume() {
    local name out uuid
    RANDOM_VOL_COUNTER=$((RANDOM_VOL_COUNTER + 1))
    name="rnd_vol_${RUN_ID}_${RANDOM_VOL_COUNTER}"
    out="$(sbctl lvol add "$name" "$CHURN_VOLUME_SIZE" "$POOL" 2>&1 | tee -a "$LOGFILE" || true)"
    uuid="$(printf '%s\n' "$out" | extract_uuid)"
    if [ -z "$uuid" ]; then
        uuid="$(lookup_lvol_uuid "$name" || true)"
    fi
    if [ -z "$uuid" ]; then
        log "Churn: create volume failed $name"
        return 1
    fi
    register_volume "$uuid" "$name"
    log "Churn: created volume $name $uuid"
}

create_random_snapshot() {
    local parent_id parent_name name out uuid
    if ! parent_id="$(pick_random_volume_for_snapshot)"; then
        return 1
    fi
    parent_name="${OBJECT_NAME[$parent_id]}"
    RANDOM_SNAP_COUNTER=$((RANDOM_SNAP_COUNTER + 1))
    name="rnd_snap_${RUN_ID}_${RANDOM_SNAP_COUNTER}"
    out="$(sbctl snapshot add "$parent_id" "$name" 2>&1 | tee -a "$LOGFILE" || true)"
    uuid="$(printf '%s\n' "$out" | extract_uuid)"
    if [ -z "$uuid" ]; then
        uuid="$(lookup_snapshot_uuid "$name" || true)"
    fi
    if [ -z "$uuid" ]; then
        log "Churn: create snapshot failed $name from $parent_name $parent_id"
        return 1
    fi
    register_snapshot "$uuid" "$name" "$parent_id"
    log "Churn: created snapshot $name $uuid from $parent_name $parent_id"
}

create_random_clone() {
    local snap_id name out uuid
    if ! snap_id="$(pick_random_snapshot_for_clone)"; then
        return 1
    fi
    RANDOM_CLONE_COUNTER=$((RANDOM_CLONE_COUNTER + 1))
    name="rnd_clone_${RUN_ID}_${RANDOM_CLONE_COUNTER}"
    out="$(sbctl snapshot clone "$snap_id" "$name" 2>&1 | tee -a "$LOGFILE" || true)"
    uuid="$(printf '%s\n' "$out" | extract_uuid)"
    if [ -z "$uuid" ]; then
        uuid="$(lookup_lvol_uuid "$name" || true)"
    fi
    if [ -z "$uuid" ]; then
        log "Churn: create clone failed $name from snapshot $snap_id"
        return 1
    fi
    register_clone "$uuid" "$name" "$snap_id"
    log "Churn: created clone $name $uuid from snapshot $snap_id"
}

delete_random_clone() {
    local clone_id
    if ! clone_id="$(pick_random_clone_for_delete)"; then
        return 1
    fi
    delete_clone_id "$clone_id"
}

delete_random_volume() {
    local vol_id
    if ! vol_id="$(pick_random_volume_for_delete)"; then
        return 1
    fi
    delete_volume_id "$vol_id"
}

delete_random_snapshot() {
    local snap_id
    if ! snap_id="$(pick_random_snapshot_for_delete)"; then
        return 1
    fi
    delete_snapshot_id "$snap_id"
}

run_fill_phase() {
    local current
    log "Starting fill phase toward $TARGET_OBJECTS live volumes+clones"
    while true; do
        current="$(live_object_count)"
        if [ "$current" -ge "$TARGET_OBJECTS" ]; then
            break
        fi
        if [ "${#SNAPSHOT_IDS[@]}" -eq 0 ]; then
            if [ $(( RANDOM % 100 )) -lt 70 ]; then
                create_random_snapshot || create_random_volume || true
            else
                create_random_volume || true
            fi
        else
            case $(( RANDOM % 100 )) in
                [0-3][0-9]) create_random_volume || true ;;
                [4-6][0-9]) create_random_clone || create_random_snapshot || true ;;
                *) create_random_snapshot || create_random_clone || create_random_volume || true ;;
            esac
        fi
        sleep_fraction "$CHURN_SLEEP_SECS"
    done
    log "Fill phase complete: $(live_object_count) live volumes+clones, ${#SNAPSHOT_IDS[@]} snapshots"
}

run_churn_cycle() {
    local cycle_end current
    CYCLE_NUMBER=$((CYCLE_NUMBER + 1))
    cycle_end=$(( $(date +%s) + CYCLE_SECONDS ))
    log "Starting churn cycle $CYCLE_NUMBER for ${CYCLE_SECONDS}s"
    while [ "$(date +%s)" -lt "$cycle_end" ]; do
        current="$(live_object_count)"
        if [ "$current" -lt $(( TARGET_OBJECTS - 5 )) ]; then
            case $(( RANDOM % 100 )) in
                [0-4][0-9]) create_random_volume || create_random_snapshot || true ;;
                [5-8][0-9]) create_random_clone || create_random_snapshot || create_random_volume || true ;;
                *) create_random_snapshot || create_random_clone || true ;;
            esac
        elif [ "$current" -gt $(( TARGET_OBJECTS + 5 )) ]; then
            case $(( RANDOM % 100 )) in
                [0-4][0-9]) delete_random_clone || delete_random_volume || delete_random_snapshot || true ;;
                [5-7][0-9]) delete_random_volume || delete_random_clone || delete_random_snapshot || true ;;
                *) delete_random_snapshot || delete_random_clone || delete_random_volume || true ;;
            esac
        else
            case $(( RANDOM % 100 )) in
                [0-1][0-9]) create_random_volume || true ;;
                [2-3][0-9]) create_random_snapshot || true ;;
                [4-5][0-9]) create_random_clone || true ;;
                [6-7][0-9]) delete_random_clone || true ;;
                [8][0-9]) delete_random_volume || delete_random_clone || true ;;
                *) delete_random_snapshot || create_random_snapshot || true ;;
            esac
        fi
        sleep_fraction "$CHURN_SLEEP_SECS"
    done
    log "Churn cycle $CYCLE_NUMBER complete: $(live_object_count) live volumes+clones, ${#SNAPSHOT_IDS[@]} snapshots"
}

print_summary() {
    if [ -n "$BASE_FIO_PID" ] && kill -0 "$BASE_FIO_PID" 2>/dev/null; then
        sudo kill "$BASE_FIO_PID" >>"$LOGFILE" 2>&1 || true
        wait "$BASE_FIO_PID" 2>/dev/null || true
        log "Stopped base fio PID $BASE_FIO_PID"
    fi
    if mountpoint -q "$BASE_MOUNT" 2>/dev/null; then
        sudo umount "$BASE_MOUNT" >>"$LOGFILE" 2>&1 || true
    fi
    log "========================================="
    log "Run summary"
    log "Base volume: $BASE_VOL_NAME $BASE_VOL_UUID"
    log "Live volumes: ${#VOLUME_IDS[@]}"
    log "Live clones: ${#CLONE_IDS[@]}"
    log "Live snapshots: ${#SNAPSHOT_IDS[@]}"
    log "Initial sample connect OK/FAIL: $INITIAL_CONNECT_OK/$INITIAL_CONNECT_FAIL"
    log "Cycle sample connect OK/FAIL: $CYCLE_CONNECT_OK/$CYCLE_CONNECT_FAIL"
    log "Completed churn cycles: $CYCLE_NUMBER"
    log "Log: $LOGFILE"
    log "Map: $MAPFILE"
}

trap print_summary EXIT

log "========================================="
log "Snapshot/Clone Churn Test Starting"
log "Initial clones: $INITIAL_CLONES"
log "Pool: $POOL"
log "Base volume size: $BASE_VOLUME_SIZE"
log "Base fio jobs / iodepth: $BASE_FIO_JOBS / $BASE_FIO_IODEPTH"
log "Target live objects: $TARGET_OBJECTS"
log "Cycle seconds: $CYCLE_SECONDS"
log "Connect sample size / batch: $CONNECT_SAMPLE_SIZE / $CONNECT_BATCH_SIZE"
log "Log: $LOGFILE"
log "Map: $MAPFILE"
log "========================================="

create_base_volume
prepare_base_workload
run_initial_clone_burst
connect_disconnect_sample "$CONNECT_SAMPLE_SIZE" "$CONNECT_BATCH_SIZE" "initial-sample"
delete_all_initial_clones
run_fill_phase

while true; do
    run_churn_cycle
    connect_disconnect_sample "$CONNECT_SAMPLE_SIZE" "$CONNECT_BATCH_SIZE" "cycle-${CYCLE_NUMBER}-sample"
done
