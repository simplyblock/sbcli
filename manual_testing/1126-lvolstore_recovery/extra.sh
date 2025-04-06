#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
# cloning for xfs does not work well
MOUNT_DIR="/mnt"


# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

get_cluster_id() {
    log "Fetching cluster ID"
    cluster_id=$($SBCLI_CMD cluster list | awk 'NR==4 {print $2}')
    log "Cluster ID: $cluster_id"
}

run_extra_workload() {
    local mount_point=$1
    local size=$2
    local nrfiles=$3
    log "Running fio workload on mount point: $mount_point/extra with size: $size"
    sudo mkdir -p $mount_point/extra
    sudo fio --directory=$mount_point/extra --readwrite=randwrite --bs=256K --size=$size --name=test --numjobs=1 \
        --nrfiles=$nrfiles --direct=1 --ioengine=aiolib --iodepth=1 --verify=md5 --verify_dump=1 --verify_fatal=0 \
        --verify_state_save=1 --verify_backlog=10
}

get_capacity_utilisation() {
    ALL_LVOLS=($($SBCLI_CMD lvol list | grep -i lvol | awk '{print $4}'))
    for lvol_name in "${ALL_LVOLS[@]}"; do
        mount_point="$MOUNT_DIR/$lvol_name"
        eval sudo fstrim ${mount_point}
        eval sudo fstrim -v ${mount_point}
    done
    df -h /mnt/lvol_*
    eval $SBCLI_CMD cluster get-capacity ${cluster_id} | sed -n '4p'
    ALL_DEVICES=($($SBCLI_CMD cluster status $cluster_id | grep -i online | awk '{print $2}'))
    log "Devices utilisation:"
    for device in "${ALL_DEVICES[@]}"; do
        eval $SBCLI_CMD sn get-capacity-device ${device} | sed -n '4p'
    done
    log "Base file checksums:"
    for lvol_name in "${ALL_LVOLS[@]}"; do
        md5sum $MOUNT_DIR/$lvol_name/base/*
    done
}

# Main script
get_cluster_id
# Step 2: Start fio on all LVOLs simultaneously and perform all operations afterward.
ALL_LVOLS=($($SBCLI_CMD lvol list | grep -i lvol | awk '{print $4}'))
for lvol_name in "${ALL_LVOLS[@]}"; do
    mount_point="$MOUNT_DIR/$lvol_name"
    run_extra_workload ${mount_point} 11G 2 &
done

sleep 120
log "TEST preparation completed"

