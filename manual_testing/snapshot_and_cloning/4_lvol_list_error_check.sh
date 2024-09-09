#!/bin/bash

# Variables
POOL_NAME="lvol_test_pool"
LVOL_NAME="lvol_2_1"
LVOL_SIZE="250G"
FS_TYPE="ext4" # Can be changed to xfs or mixed as needed
MOUNT_DIR="/mnt"
NUM_ITERATIONS=40

# List to store timings
format_timings=()
connect_timings=()
fio_run_timings=()

# Helper function to log with timestamp, line number, and command
log() {
    local lineno=$1
    local cmd=$2
    echo "$(date +'%Y-%m-%d %H:%M:%S') - [Line $lineno] - $cmd"
}

get_cluster_id() {
    log $LINENO "Fetching cluster ID"
    cluster_id=$(sbcli-lvol cluster list | awk 'NR==4 {print $2}')
    log $LINENO "Cluster ID: $cluster_id"
}

create_pool() {
    local cluster_id=$1
    log $LINENO "Creating pool: $POOL_NAME with cluster ID: $cluster_id"
    sbcli-lvol pool add $POOL_NAME $cluster_id
}

create_lvol() {
    lvol_name=$1
    log $LINENO "Creating logical volume: $lvol_name with configuration 2+1"
    sbcli-lvol lvol add --distr-ndcs 2 --distr-npcs 1 --max-size $LVOL_SIZE $lvol_name $LVOL_SIZE $POOL_NAME
}

connect_lvol() {
    local lvol_id=$1
    log $LINENO "Connecting logical volume: $lvol_id"
    connect_command=$(sbcli-lvol lvol connect $lvol_id)
    log $LINENO "Running connect command: $connect_command"
    eval sudo $connect_command
}

format_fs() {
    local device=$1
    local fs_type=$2
    log $LINENO "Formatting device: /dev/$device with filesystem: $fs_type"
    
    start_time=$(date +%s)
    sudo mkfs.$fs_type -F /dev/$device
    end_time=$(date +%s)
    
    time_taken=$((end_time - start_time))
    format_timings+=("/dev/$device - $time_taken seconds")
}

run_fio_workload() {
    local mount_point=$1
    log $LINENO "Running FIO workload to fill 5 test files (each 1GB) on $mount_point"
    sudo fio --directory=$mount_point --readwrite=write --bs=4K-128K --size=1G --name=test1 --name=test2 --name=test3 --name=test4 --name=test5
}

mount_and_run_fio() {
    local lvol_name=$1
    log $LINENO "Mounting and running FIO workload on lvol: $lvol_name"

    lvol_id=$(sbcli-lvol lvol list | grep -i $lvol_name | awk '{print $2}')
    
    before_lsblk=$(sudo lsblk -o name)
    start_time=$(date +%s)
    connect_lvol $lvol_id
    end_time=$(date +%s)
    
    time_taken=$((end_time - start_time))
    connect_timings+=("$lvol_name - $time_taken seconds")
    
    after_lsblk=$(sudo lsblk -o name)
    lvol_device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

    format_fs $lvol_device $FS_TYPE

    local mount_point="$MOUNT_DIR/$lvol_name"
    sudo mkdir -p $mount_point
    sudo mount /dev/$lvol_device $mount_point

    start_time=$(date +%s)
    run_fio_workload $mount_point
    end_time=$(date +%s)
    
    time_taken=$((end_time - start_time))
    fio_run_timings+=("$lvol_name - $mount_point - $time_taken seconds")
}

disconnect_lvol() {
    local lvol_id=$1
    log $LINENO "Disconnecting logical volume: $lvol_id"
    subsys=$(sudo nvme list-subsys | grep -i $lvol_id | awk '{print $3}' | cut -d '=' -f 2)
    if [ -n "$subsys" ]; then
        log $LINENO "Disconnecting NVMe subsystem: $subsys"
        sudo nvme disconnect -n $subsys
    else
        log $LINENO "No subsystem found for $lvol_id"
    fi
}

delete_lvols() {
    log $LINENO "Deleting all existing LVOLs"
    lvols=$(sbcli-lvol lvol list | grep -v "ID" | awk '{print $2}')
    for lvol in $lvols; do
        log $LINENO "Deleting logical volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done
}

delete_pool() {
    log $LINENO "Deleting pool: $POOL_NAME"
    pool_id=$(sbcli-lvol pool list | grep -i $POOL_NAME | awk '{print $2}')
    sbcli-lvol pool delete $pool_id
}

unmount_all() {
    log $LINENO "Unmounting all mount points"
    mount_points=$(mount | grep /mnt | awk '{print $3}')
    for mount_point in $mount_points; do
        log $LINENO "Unmounting $mount_point"
        sudo umount $mount_point
    done
}

remove_mount_dirs() {
    log $LINENO "Removing all mount point directories"
    mount_dirs=$(sudo find /mnt -mindepth 1 -type d)
    for mount_dir in $mount_dirs; do
        log $LINENO "Removing directory $mount_dir"
        sudo rm -rf $mount_dir
    done
}

disconnect_lvols() {
    log $LINENO "Disconnecting all NVMe devices with NQN containing 'lvol'"
    subsystems=$(sudo nvme list-subsys | grep -i lvol | awk '{print $3}' | cut -d '=' -f 2)
    for subsys in $subsystems; do
        log $LINENO "Disconnecting NVMe subsystem: $subsys"
        sudo nvme disconnect -n $subsys
    done
}

# Main cleanup script
unmount_all
remove_mount_dirs
disconnect_lvols
delete_lvols

# Main script execution
get_cluster_id
create_pool $cluster_id

for ((i=1; i<=NUM_ITERATIONS; i++)); do
    log $LINENO "Iteration $i of $NUM_ITERATIONS"
    lvol_name="${LVOL_NAME}_${i}_ll"
    create_lvol $lvol_name
    mount_and_run_fio $lvol_name
done

log $LINENO "Script execution completed"

unmount_all
remove_mount_dirs
disconnect_lvols
delete_lvols
delete_pool

log $LINENO "CLEANUP COMPLETE"

# Print timings
log $LINENO "Printing timings for connect operations"
for timing in "${connect_timings[@]}"; do
    log $LINENO "$timing"
done


log $LINENO "Printing timings for mkfs operations"
for timing in "${format_timings[@]}"; do
    log $LINENO "$timing"
done

log $LINENO "Printing timings for fio operations"
for timing in "${fio_run_timings[@]}"; do
    log $LINENO "$timing"
done
