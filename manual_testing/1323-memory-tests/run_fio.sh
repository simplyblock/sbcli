#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
POOL_NAME="testing1"
LVOL_SIZE="70G"
# cloning for xfs does not work well
FS_TYPES=("xfs" "ext4")
MOUNT_DIR="/mnt"
LVOL_CFG="1+1"
WORKLOAD_SIZE="64G"


# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

get_cluster_id() {
    log "Fetching cluster ID"
    cluster_id=$($SBCLI_CMD cluster list | awk 'NR==4 {print $2}')
    log "Cluster ID: $cluster_id"
}

create_pool() {
    local cluster_id=$1
    log "Creating pool: $POOL_NAME with cluster ID: $cluster_id"
    $SBCLI_CMD pool add $POOL_NAME $cluster_id
}

create_lvol() {
    local name=$1
    local host_id=$2
    local config=$3
    ndcs=${config%%+*}
    npcs=${config##*+}

    log "Creating logical volume: $name with ndcs: $ndcs and npcs: $npcs"
    if [ $SBCLI_CMD != "sbcli-new" ]; then
        pool="$POOL_NAME"
    else
        pool=""
    fi
    cmd="$SBCLI_CMD -d lvol add --host-id $host_id $name $LVOL_SIZE $pool"
    echo "Running command: $cmd"
    $cmd

}

connect_lvol() {
    local lvol_id=$1
    log "Connecting logical volume: $lvol_id"
    connect_command="$SBCLI_CMD -d lvol connect $lvol_id"
    log "Running connect command: $connect_command"
    eval sudo $connect_command
}

connect_lvol() {
    local lvol_id=$1
    log "Connecting logical volume: $lvol_id"
    connect_command=$($SBCLI_CMD lvol connect $lvol_id)
    log "Running connect command: $connect_command"
    eval sudo $connect_command
}

format_fs() {
    local device=$1
    local fs_type=$2
    log "Formatting device: /dev/$device with filesystem: $fs_type"
    echo "sudo mkfs.$fs_type /dev/$device"
    sudo mkfs.$fs_type /dev/$device
}

run_fio_workload() {
    local mount_point=$1
    local size=$2
    local nrfiles=$3
    log "Running fio workload on mount point: $mount_point with size: $size"
    sudo fio --directory=$mount_point --readwrite=randwrite --bs=256K --size=$size --name=test --numjobs=1 \
        --nrfiles=$nrfiles --time_based=1 --runtime=36000 --direct=1 --ioengine=aiolib --iodepth=64 --verify=md5 \
        --verify_dump=1 --verify_fatal=0 --verify_state_save=1 --verify_backlog=10
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
            log "FAIL: Checksum mismatch for $file"
            exit 1
        fi
    done
}

delete_snapshots() {
    log "Deleting all snapshots"
    snapshots=$($SBCLI_CMD snapshot list | grep -i _ss_ | awk '{print $2}')
    for snapshot in $snapshots; do
        log "Deleting snapshot: $snapshot"
        $SBCLI_CMD -d snapshot delete $snapshot
    done
}

delete_lvols() {
    log "Deleting all logical volumes, including clones"
    lvols=$($SBCLI_CMD lvol list | grep -i lvol | awk '{print $2}')
    for lvol in $lvols; do
        log "Deleting logical volume: $lvol"
        $SBCLI_CMD -d lvol delete $lvol
    done
}

delete_pool() {
    log "Deleting pool: $POOL_NAME"
    pool_id=$($SBCLI_CMD pool list | grep -i $POOL_NAME | awk '{print $2}')
    $SBCLI_CMD pool delete $pool_id || true
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


pause_if_interactive_mode() {
  if [[ " $* " =~ " -i " ]]; then
    echo "Press 'c' to continue"
    while true; do
      read -n 1 -s key
      if [ "$key" = "c" ]; then
        break
      fi
    done
  fi
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
        md5sum $MOUNT_DIR/$lvol_name/base/* || true
    done
}


# Main cleanup script
unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols
delete_pool

# Main script
get_cluster_id
$SBCLI_CMD -d cluster activate $cluster_id
create_pool $cluster_id


SN_IDS=($(${SBCLI_CMD} sn list | grep -Eo '^[|][ ]+[a-f0-9-]+[ ]+[|]' | awk '{print $2}'))
log "Storage nodes used for LVOLs: ${SN_IDS[*]}"
pause_if_interactive_mode "$@"

# create and mount all LVOLs
lvol_names_arr=()
for i in "${!SN_IDS[@]}"; do
    host_id=${SN_IDS[i]}
    log "Create lvol # $i"
    fs_type=$(shuf -n 1 -e "${FS_TYPES[@]}")
    lvol_name="lvol_${fs_type}_${i}"
    create_lvol $lvol_name $host_id $LVOL_CFG
    # pause_if_interactive_mode "$@"
    log "Fetching logical volume ID for: $lvol_name"
    lvol_id=$($SBCLI_CMD lvol list | grep -i "$lvol_name " | awk '{print $2}')

    before_lsblk=$(sudo lsblk -o name)
    connect_lvol $lvol_id
    after_lsblk=$(sudo lsblk -o name)
    device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

    fs_type=$(shuf -n 1 -e "${FS_TYPES[@]}")

    format_fs $device $fs_type

    mount_point="$MOUNT_DIR/$lvol_name"
    log "Creating mount point directory: $mount_point"
    sudo mkdir -p $mount_point

    log "Mounting device: /dev/$device at $mount_point"
    sudo mount /dev/$device $mount_point
    sudo mkdir -p $mount_point/base
    lvol_names_arr[$i]=$lvol_name
done

get_capacity_utilisation
pause_if_interactive_mode "$@"

# Step 2: Start fio on all LVOLs simultaneously and perform all operations afterward.
# ALL_LVOLS=$($SBCLI_CMD lvol list | grep -i lvol | awk '{print $4}')
for lvol_name in "${lvol_names_arr[@]}"; do
    mount_point="$MOUNT_DIR/$lvol_name"
    run_fio_workload ${mount_point} $WORKLOAD_SIZE 5 &
done

log "FIO is started"

