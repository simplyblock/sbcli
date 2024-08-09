#!/bin/bash

set -e

# Variables
POOL_NAME="snap_test_pool"
LVOL_SIZE="80G"
FS_TYPES=("ext4" "xfs")
CONFIGURATIONS=("1+0" "2+1" "4+1" "4+2" "8+1" "8+2")
WORKLOAD_SIZE=("5G" "10G" "20G" "40G")
MOUNT_DIR="/mnt"

# Description:
# This script performs comprehensive testing of logical volume configurations, filesystem types, and workload sizes.
# It covers a total of 48 test cases combining different configurations and workloads. The detailed combinations are:
#
# 1. Filesystem Types:
#    - ext4
#    - xfs
#
# 2. Configurations:
#    - 1+0
#    - 2+1
#    - 4+1
#    - 4+2
#    - 8+1
#    - 8+2
#
# 3. Workload Sizes:
#    - 5G
#    - 10G
#    - 20G
#    - 40G
#
# The total number of test cases is calculated by multiplying the number of filesystem types, configurations, and workload sizes:
# 2 (Filesystem Types) * 6 (Configurations) * 4 (Workload Sizes) = 48 Test Cases
#
# The combinations are:
#
# For Filesystem Type "ext4" with all configurations and workload sizes:
# - ext4, 1+0, 5G
# - ext4, 1+0, 10G
# - ext4, 1+0, 20G
# - ext4, 1+0, 40G
# - ext4, 2+1, 5G
# - ext4, 2+1, 10G
# - ext4, 2+1, 20G
# - ext4, 2+1, 40G
# - ext4, 4+1, 5G
# - ext4, 4+1, 10G
# - ext4, 4+1, 20G
# - ext4, 4+1, 40G
# - ext4, 4+2, 5G
# - ext4, 4+2, 10G
# - ext4, 4+2, 20G
# - ext4, 4+2, 40G
# - ext4, 8+1, 5G
# - ext4, 8+1, 10G
# - ext4, 8+1, 20G
# - ext4, 8+1, 40G
# - ext4, 8+2, 5G
# - ext4, 8+2, 10G
# - ext4, 8+2, 20G
# - ext4, 8+2, 40G
#
# For Filesystem Type "xfs" with all configurations and workload sizes:
# - xfs, 1+0, 5G
# - xfs, 1+0, 10G
# - xfs, 1+0, 20G
# - xfs, 1+0, 40G
# - xfs, 2+1, 5G
# - xfs, 2+1, 10G
# - xfs, 2+1, 20G
# - xfs, 2+1, 40G
# - xfs, 4+1, 5G
# - xfs, 4+1, 10G
# - xfs, 4+1, 20G
# - xfs, 4+1, 40G
# - xfs, 4+2, 5G
# - xfs, 4+2, 10G
# - xfs, 4+2, 20G
# - xfs, 4+2, 40G
# - xfs, 8+1, 5G
# - xfs, 8+1, 10G
# - xfs, 8+1, 20G
# - xfs, 8+1, 40G
# - xfs, 8+2, 5G
# - xfs, 8+2, 10G
# - xfs, 8+2, 20G
# - xfs, 8+2, 40G
#
# The script performs the following steps for each combination:
# - Creates logical volumes with specified configurations.
# - Connects logical volumes.
# - Formats the logical volumes with the specified filesystem.
# - Mounts the logical volumes.
# - Runs fio workloads of different sizes on the mounted logical volumes.
# - Generates and verifies checksums for test files.
# - Creates snapshots and clones from the snapshots.
# - Runs fio workloads on the clones.
# - Verifies the integrity of data by comparing checksums before and after workloads.
# - Cleans up by unmounting, disconnecting, and deleting logical volumes, snapshots, and pools.


# Helper functions
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
    local name=$1
    local ndcs=$2
    local npcs=$3
    log "Creating logical volume: $name with ndcs: $ndcs and npcs: $npcs"
    sbcli-lvol lvol add --distr-ndcs $ndcs --distr-npcs $npcs --max-size $LVOL_SIZE --snapshot $name $LVOL_SIZE $POOL_NAME
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
    local size=$2
    log "Running fio workload on mount point: $mount_point with size: $size"
    sudo fio --directory=$mount_point --readwrite=write --bs=4K-128K --size=$size --name=test --numjobs=3
}

generate_checksums() {
    local files=("$@")
    for file in "${files[@]}"; do
        log "Generating checksum for file: $file"
        sudo md5sum $file
    done
}

verify_checksums() {
    local files=("$@")
    local base_checksums=()
    for file in "${files[@]}"; do
        # log "Verifying checksum for file: $file"
        checksum=$(sudo md5sum $file | awk '{print $1}')
        base_checksums+=("$checksum")
    done
    echo "${base_checksums[@]}"
}

compare_checksums() {
    local files=("$@")
    local checksums=("$@")
    for i in "${!files[@]}"; do
        file="${files[$i]}"
        checksum="${checksums[$i]}"
        current_checksum=$(sudo md5sum "$file" | awk '{print $1}')
        if [ "$current_checksum" == "$checksum" ]; then
            log "Checksum OK for $file"
        else
            log "Checksum mismatch for $file"
        fi
    done
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
    snapshots=$(sbcli-lvol snapshot list | grep -i snapshot | awk '{print $2}')
    for snapshot in $snapshots; do
        log "Deleting snapshot: $snapshot"
        sbcli-lvol snapshot delete $snapshot --force
    done
}

delete_lvols() {
    log "Deleting all logical volumes, including clones"
    lvols=$(sbcli-lvol lvol list | grep -i lvol | awk '{print $2}')
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

# Main script
get_cluster_id
create_pool $cluster_id

for fs_type in "${FS_TYPES[@]}"; do
    log "Processing filesystem type: $fs_type"

    # Step 1: Add all LVOL configs, connect LVOLs, format with one particular file type, and mount all the disks.
    for config in "${CONFIGURATIONS[@]}"; do
        ndcs=${config%%+*}
        npcs=${config##*+}
        lvol_name="lvol_${ndcs}_${npcs}"
        
        create_lvol $lvol_name $ndcs $npcs
        log "Fetching logical volume ID for: $lvol_name"
        lvol_id=$(sbcli-lvol lvol list | grep -i $lvol_name | awk '{print $2}')
        
        before_lsblk=$(sudo lsblk -o name)
        connect_lvol $lvol_id
        after_lsblk=$(sudo lsblk -o name)
        device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')
        
        format_fs $device $fs_type

        mount_point="$MOUNT_DIR/$lvol_name"
        log "Creating mount point directory: $mount_point"
        sudo mkdir -p $mount_point
        
        log "Mounting device: /dev/$device at $mount_point"
        sudo mount /dev/$device $mount_point
    done

    for size in "${WORKLOAD_SIZE[@]}"; do
        log "Running fio workload with size: $size"

        # Step 2: Start fio on all workloads simultaneously and perform all operations afterward.
        for config in "${CONFIGURATIONS[@]}"; do
            ndcs=${config%%+*}
            npcs=${config##*+}
            lvol_name="lvol_${ndcs}_${npcs}"
            mount_point="$MOUNT_DIR/$lvol_name"
            # log "Unmounting $mount_point"
            # sudo umount  $mount_point

            # log "Removing mount point: $mount_point"
            # sudo rm -rf $mount_point

            # log "Creating mount point directory: $mount_point"
            # sudo mkdir -p $mount_point
            
            # log "Mounting device: /dev/$device at $mount_point"
            # sudo mount /dev/$device $mount_point
            
            run_fio_workload $mount_point $size &
        done

        wait
        sleep 10

        # Perform checksum, snapshot, clone, and other operations
        for config in "${CONFIGURATIONS[@]}"; do
            ndcs=${config%%+*}
            npcs=${config##*+}
            lvol_name="lvol_${ndcs}_${npcs}"
            mount_point="$MOUNT_DIR/$lvol_name"

            log "Finding test files in mount point: $mount_point"
            test_files=($(sudo find $mount_point -type f))

            log "Generating checksums for base volume"
            base_checksums=($(verify_checksums "${test_files[@]}"))
            echo "BASE CHECKSUM: $base_checksums"

            log "Creating snapshot for volume: $lvol_name"
            snapshot_name="${lvol_name}_ss_${size}_${fs_type}"
            lvol_id=$(sbcli-lvol lvol list | grep -i $lvol_name | awk '{print $2}')
            sbcli-lvol snapshot add $lvol_id $snapshot_name

            log "Listing snapshots"
            sbcli-lvol snapshot list

            log "Creating clone from snapshot: $snapshot_name"
            snapshot_id=$(sbcli-lvol snapshot list | grep -i $snapshot_name | awk '{print $2}')
            clone_name="${snapshot_name}_cl"
            sbcli-lvol snapshot clone $snapshot_id $clone_name

            log "Listing clones."
            sbcli-lvol lvol list

            log "Fetching clone logical volume ID for: $clone_name"
            clone_id=$(sbcli-lvol lvol list | grep -i $clone_name | awk '{print $2}')

            before_lsblk=$(sudo lsblk -o name)
            connect_lvol $clone_id
            after_lsblk=$(sudo lsblk -o name)
            clone_device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')
            
            clone_mount_point="$MOUNT_DIR/$clone_name"
            log "Creating clone mount point directory: $clone_mount_point"
            sudo mkdir -p $clone_mount_point
            
            log "Mounting clone device: /dev/$clone_device at $clone_mount_point"
            sudo mount /dev/$clone_device $clone_mount_point

            log "Finding files in clone mount point: $clone_mount_point"
            clone_files=($(sudo find $clone_mount_point -type f))
            
            log "Generating checksums for clone: $clone_mount_point"
            clone_checksums=($(verify_checksums "${clone_files[@]}"))

            log "Running fio workload on clone mount point: $clone_mount_point"
            clone_workload_dir="$clone_mount_point/clone_test"
            sudo mkdir -p $clone_workload_dir
            run_fio_workload $clone_workload_dir $size &

            wait
            
            sleep 10

            log "Verifying that the base volume has not been changed"
            base_checksums_after=($(verify_checksums "${test_files[@]}"))

            log "Deleting test files from base volumes"
            sudo rm -f ${test_files[@]}

            log "Verifying that the test files still exist on the clones"
            clone_files_after=($(sudo find $clone_mount_point -type f))
            clone_checksums_after=($(verify_checksums "${clone_files_after[@]}"))

            log "Checksum comparison"
            for i in "${!test_files[@]}"; do
                file="${test_files[$i]}"
                base_checksum="${base_checksums[$i]}"
                base_checksum_after="${base_checksums_after[$i]}"

                log "Checksum for $file on base volume Before: $base_checksum, After: $base_checksum_after"

                if [ "$base_checksum" != "$base_checksum_after" ]; then
                    log "Checksum mismatch for $file on base volume after workload"
                else
                    log "Checksum match for $file on base volume after workload"
                fi
            done

            for i in "${!clone_files[@]}"; do
                file="${clone_files[$i]}"
                clone_checksum="${clone_checksums[$i]}"
                clone_checksum_after="${clone_checksums_after[$i]}"
                log "Checksum for $file on clone volume Before: $clone_checksum, After: $clone_checksum_after"
                if [ "$clone_checksum" != "$clone_checksum_after" ]; then
                    log "Checksum mismatch for $file on clone after workload"
                else
                    log "Checksum match for $file on clone after workload"
                fi
            done

            # Disconnect and delete clone after validations
            log "Unmounting clone mount point: $clone_mount_point"
            sudo umount $clone_mount_point

            disconnect_lvol $clone_id

            log "Deleting clone logical volume: $clone_id"
            sbcli-lvol lvol delete $clone_id

            log "Deleting clone dir: $clone_mount_point"
            sudo rm -rf $clone_mount_point

            log "TEST Execution Completed for NDCS: $ndcs, NPCS: $npcs, FIO Size: $size, FS Type: $fs_type"
        done
        # delete_snapshots
    done

unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols

done

log "TEST Execution Completed"

delete_pool

log "CLEANUP COMPLETE"
