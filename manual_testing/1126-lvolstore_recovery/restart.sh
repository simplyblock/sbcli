#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
SN_IDS=($(${SBCLI_CMD} sn list | grep -Eo '^[|][ ]+[a-f0-9-]+[ ]+[|]' | awk '{print $2}'))
host_id=${SN_IDS[0]}
MOUNT_DIR="/mnt"


# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}


unmount_all() {
    log "Unmounting all mount points"
    mount_points=$(mount | grep /mnt | awk '{print $3}')
    for mount_point in $mount_points; do
        log "Unmounting $mount_point"
        sudo umount $mount_point
    done
}

remove_mount_dirs() {
    log "Removing all mount point directories"
    mount_dirs=$(sudo find /mnt -mindepth 1 -type d)
    for mount_dir in $mount_dirs; do
        log "Removing directory $mount_dir"
        sudo rm -rf $mount_dir
    done
}


disconnect_lvols() {
    log "Disconnecting all NVMe devices with NQN containing 'lvol'"
    subsystems=$(sudo nvme list-subsys | grep -i lvol | awk '{print $3}' | cut -d '=' -f 2)
    for subsys in $subsystems; do
        log "Disconnecting NVMe subsystem: $subsys"
        sudo nvme disconnect -n $subsys
    done
}

get_cluster_id() {
    #log "Fetching cluster ID"
    cluster_id=$($SBCLI_CMD cluster list | awk 'NR==4 {print $2}')
    #log "Cluster ID: $cluster_id"
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

get_cluster_id
get_capacity_utilisation

unmount_all
remove_mount_dirs
disconnect_lvols

$SBCLI_CMD -d sn suspend $host_id
$SBCLI_CMD -d sn shutdown $host_id
sleep 15
$SBCLI_CMD -d sn restart $host_id
