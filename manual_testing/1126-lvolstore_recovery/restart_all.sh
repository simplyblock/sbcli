#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
SN_IDS=($(${SBCLI_CMD} sn list | grep -Eo '^[|][ ]+[a-f0-9-]+[ ]+[|]' | awk '{print $2}'))



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


unmount_all
remove_mount_dirs
disconnect_lvols

for host_id in "${SN_IDS[@]}"; do
    $SBCLI_CMD -d sn suspend $host_id
    $SBCLI_CMD -d sn shutdown $host_id
    $SBCLI_CMD -d sn restart $host_id
done


