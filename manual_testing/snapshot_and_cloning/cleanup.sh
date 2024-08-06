#!/bin/bash

# Variables
POOL_NAME="snap_test_pool"

# Helper functions
delete_snapshots() {
    echo "Deleting all snapshots"
    snapshots=$(sbcli-lvol snapshot list | awk '{print $2}')
    for snapshot in $snapshots; do
        echo "Deleting snapshot: $snapshot"
        sbcli-lvol snapshot delete $snapshot --force
    done
}

delete_lvols() {
    echo "Deleting all logical volumes, including clones"
    lvols=$(sbcli-lvol lvol list | grep -i lvol | awk '{print $2}')
    for lvol in $lvols; do
        echo "Deleting logical volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done

    lvols=$(sbcli-lvol lvol list | grep -i clone | awk '{print $2}')
    for lvol in $lvols; do
        echo "Deleting logical volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done

    lvols=$(sbcli-lvol lvol list | grep -i cl | awk '{print $2}')
    for lvol in $lvols; do
        echo "Deleting logical volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done
}

delete_pool() {
    echo "Deleting pool: $POOL_NAME"
    pool_id=$(sbcli-lvol pool list | grep -i $POOL_NAME | awk '{print $2}')
    sbcli-lvol pool delete $pool_id
}

unmount_all() {
    echo "Unmounting all mount points"
    mount_points=$(mount | grep /mnt | awk '{print $3}')
    for mount_point in $mount_points; do
        echo "Unmounting $mount_point"
        sudo umount $mount_point
    done
}

remove_mount_dirs() {
    echo "Removing all mount point directories"
    mount_dirs=$(sudo find /mnt -mindepth 1 -type d)
    for mount_dir in $mount_dirs; do
        echo "Removing directory $mount_dir"
        sudo rm -rf $mount_dir
    done
}

disconnect_lvols() {
    echo "Disconnecting all NVMe devices with NQN containing 'lvol'"
    subsystems=$(sudo nvme list-subsys | grep -i lvol | awk '{print $3}' | cut -d '=' -f 2)
    for subsys in $subsystems; do
        echo "Disconnecting NVMe subsystem: $subsys"
        sudo nvme disconnect -n $subsys
    done
}

# Main cleanup script
unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols
delete_pool

echo "Cleanup complete."
