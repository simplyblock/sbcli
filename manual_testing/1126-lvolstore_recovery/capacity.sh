#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
MOUNT_DIR="/mnt"


# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

get_cluster_id() {
    #log "Fetching cluster ID"
    cluster_id=$($SBCLI_CMD cluster list | awk 'NR==4 {print $2}')
    #log "Cluster ID: $cluster_id"
}
# Main script
get_cluster_id


get_capacity_utilisation() {
    ALL_LVOLS=($($SBCLI_CMD lvol list | grep -i lvol | awk '{print $4}'))
    for lvol_name in "${ALL_LVOLS[@]}"; do
        mount_point="$MOUNT_DIR/$lvol_name"
        # eval sudo fstrim ${mount_point}
        # eval sudo fstrim -v ${mount_point}
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

get_capacity_utilisation
