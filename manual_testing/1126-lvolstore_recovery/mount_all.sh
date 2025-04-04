#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli-dev"
# cloning for xfs does not work well
MOUNT_DIR="/mnt"



# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

connect_lvol() {
    local lvol_id=$1
    log "Connecting logical volume: $lvol_id"
    connect_command=$($SBCLI_CMD lvol connect $lvol_id)
    log "Running connect command: $connect_command"
    eval sudo $connect_command
}

ALL_LVOLS=($($SBCLI_CMD lvol list | grep -i lvol | awk '{print $4}'))
for lvol_name in "${ALL_LVOLS[@]}"; do
    lvol_id=$($SBCLI_CMD lvol list | grep -i "$lvol_name " | awk '{print $2}')
    mount_point="$MOUNT_DIR/$lvol_name"
    before_lsblk=$(sudo lsblk -o name)
    connect_lvol $lvol_id
    after_lsblk=$(sudo lsblk -o name)
    device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

    mount_point="$MOUNT_DIR/$lvol_name"
    log "Creating mount point directory: $mount_point"
    sudo mkdir -p $mount_point

    log "Mounting device: /dev/$device at $mount_point"
    sudo mount /dev/$device $mount_point
done

