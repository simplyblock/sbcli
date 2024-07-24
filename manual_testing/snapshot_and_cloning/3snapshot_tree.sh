#!/bin/bash

# Variables
POOL_NAME="snap_test_pool"
LVOL_NAME="lvol_2_1"
LVOL_SIZE="250G"
FS_TYPE="ext4" # Can be changed to xfs or mixed as needed
MOUNT_DIR="/mnt"
NUM_ITERATIONS=40

# Helper function to log with timestamp
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

get_cluster_id() {
    log "Fetching cluster ID"
    cluster_id=$(sbcli-lvol cluster list | awk 'NR==4 {print $2}')
    log "Cluster ID: $cluster_id"
}

create_pool() {
    local cluster_id=$1
    log "Creating pool: $POOL_NAME with cluster ID: $cluster_id"
    sbcli-lvol pool add $POOL_NAME $cluster_id
}

create_lvol() {
    log "Creating logical volume: $LVOL_NAME with configuration 2+1 and snapshot capability"
    sbcli-lvol lvol add --distr-ndcs 2 --distr-npcs 1 --max-size $LVOL_SIZE --snapshot $LVOL_NAME $LVOL_SIZE $POOL_NAME
}

connect_lvol() {
    local lvol_id=$1
    log "Connecting logical volume: $lvol_id"
    connect_command=$(sbcli-lvol lvol connect $lvol_id)
    log "Running connect command: $connect_command"
    eval sudo $connect_command
}

format_fs() {
    local device=$1
    local fs_type=$2
    log "Formatting device: /dev/$device with filesystem: $fs_type"
    sudo mkfs.$fs_type -F /dev/$device
}

run_fio_workload() {
    local mount_point=$1
    log "Running FIO workload to fill 5 test files (each 1GB) on $mount_point"
    sudo fio --directory=$mount_point --readwrite=write --bs=4K-128K --size=1G --name=test1 --name=test2 --name=test3 --name=test4 --name=test5
}

create_snapshot_and_clone() {
    local base_name=$1
    local lvol_id=$2
    local snapshot_name="${base_name}_snapshot_$(date +%s)"
    log "Creating snapshot: $snapshot_name $lvol_id"
    sbcli-lvol snapshot add $lvol_id $snapshot_name

    snapshot_id=($(sbcli-lvol snapshot list | grep "$snapshot_name" | awk '{print $2}'))
    log "Creating clone from snapshot: $snapshot_name"
    sbcli-lvol snapshot clone $snapshot_id "${snapshot_name}_clone"
    echo "${snapshot_name}_clone"
}

mount_and_run_fio() {
    local clone_name=$1
    log "Mounting and running FIO workload on clone: $clone_name"

    clone_id=$(sbcli-lvol lvol list | grep -i $clone_name | awk '{print $2}')
    
    before_lsblk=$(sudo lsblk -o name)
    connect_lvol $clone_id
    after_lsblk=$(sudo lsblk -o name)
    clone_device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

    format_fs $clone_device $FS_TYPE

    local mount_point="$MOUNT_DIR/$clone_name"
    sudo mkdir -p $mount_point
    sudo mount /dev/$clone_device $mount_point

    run_fio_workload $mount_point
}

disconnect_lvol() {
    local lvol_id=$1
    log "Disconnecting logical volume: $lvol_id"
    subsys=$(sudo nvme list-subsys | grep -i $lvol_id | awk '{print $3}' | cut -d '=' -f 2)
    if [ -n "$subsys" ]; then
        log "Disconnecting NVMe subsystem: $subsys"
        sudo nvme disconnect -n $subsys
    else
        log "No subsystem found for $lvol_id"
    fi
}

delete_snapshots() {
    log "Deleting all snapshots"
    snapshots=$(sbcli-lvol snapshot list | grep -v "ID" | awk '{print $1}')
    for snapshot in $snapshots; do
        log "Deleting snapshot: $snapshot"
        sbcli-lvol snapshot delete $snapshot --force
    done
}

delete_lvol() {
    local lvol_id=$1
    log "Deleting LVOL/Clone with id $lvol_id"
    sbcli-lvol lvol delete $lvol_id
}

delete_lvols() {
    log "Deleting all existing LVOLs and clones"
    lvols=$(sbcli-lvol lvol list | grep -v "ID" | awk '{print $1}')
    for lvol in $lvols; do
        log "Deleting logical volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done
}

delete_pool() {
    log "Deleting pool: $POOL_NAME"
    pool_id=$(sbcli-lvol pool list | grep -i $POOL_NAME | awk '{print $2}')
    sbcli-lvol pool delete $pool_id
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

# Main cleanup script
unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols

# Main script execution
get_cluster_id
create_pool $cluster_id

for ((i=1; i<=NUM_ITERATIONS; i++)); do
    log "Iteration $i of $NUM_ITERATIONS"
    if [ $i -eq 1 ]; then
        create_lvol
        lvol_id=$(sbcli-lvol lvol list | grep $LVOL_NAME | awk '{print $2}')
        before_lsblk=$(sudo lsblk -o name)
        connect_lvol $lvol_id
        after_lsblk=$(sudo lsblk -o name)
        device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')
        mount_point="$MOUNT_DIR/$LVOL_NAME"
        sudo mkdir -p $mount_point
        format_fs $device $FS_TYPE
        sudo mount /dev/$device $mount_point
        run_fio_workload $mount_point
        current_base=$LVOL_NAME
    fi
    
    lvol_id=$(sbcli-lvol lvol list | grep $current_base | awk '{print $2}')

    clone_name=$(create_snapshot_and_clone $current_base $lvol_id)
    mount_and_run_fio $clone_name
    current_base=$clone_name
done

log "Script execution completed"

unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols
delete_pool

log "CLEANUP COMPLETE"
