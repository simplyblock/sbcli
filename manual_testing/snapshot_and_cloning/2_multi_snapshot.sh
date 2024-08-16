#!/bin/bash

# Variables
POOL_NAME="testing1"
LVOL_NAME="lvol_2_1"
LVOL_SIZE="12G"

# TODO: change to 2 seconds (test requirement)
SNAPSHOT_INTERVAL=20
# TODO: change to 360 seconds (test requirement)
FIO_RUNTIME=60
FS_TYPE="ext4" # Can be changed to xfs or mixed as needed
MOUNT_DIR="/mnt"
MAX_CLONES=2000

# Helper function to log with timestamp
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
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
    log "Creating logical volume: $LVOL_NAME with size: $LVOL_SIZE"
    echo "sbcli-lvol -d lvol add --distr-ndcs 2 --distr-npcs 1 --max-size $LVOL_SIZE --snapshot $LVOL_NAME $LVOL_SIZE $POOL_NAME"
    sbcli-lvol -d lvol add --distr-ndcs 2 --distr-npcs 1 --max-size $LVOL_SIZE --snapshot $LVOL_NAME $LVOL_SIZE $POOL_NAME
}

connect_lvol() {
    local lvol_id=$1
    log "Connecting logical volume: $lvol_id"
    connect_command=$(sbcli-lvol lvol connect $lvol_id)
    log "Running connect command: $connect_command"
    eval sudo $connect_command
}

run_fio_workload() {
    local mount_point=$1
    log "Starting mixed fio workload on $mount_point"
    sudo fio --name=mixed_workload --directory=$mount_point --readwrite=randrw --bs=4K-128K --size=10GiB --runtime=$FIO_RUNTIME
}

# Function to return a random element from an array
get_random_element() {
  local array=("$@")  # Capture all arguments as an array
  local index=$((RANDOM % ${#array[@]}))  # Generate a random index
  echo "${array[$index]}"  # Return the random element
}

create_snapshots() {
    local lvol_id=$1
    while true; do
        snapshot_name="${LVOL_NAME}_ss_$(date +%s)"
        log "Creating snapshot: $snapshot_name"
        echo "sbcli-lvol -d snapshot add $lvol_id $snapshot_name"
        sbcli-lvol -d snapshot add $lvol_id $snapshot_name
        sleep $SNAPSHOT_INTERVAL
    done
}

list_snapshots() {
    log "Listing all snapshots"
    sbcli-lvol snapshot list
}

# 2.d. create lvols from all of these snapshots in a random sequence
# 2.e. mount these lvols
create_lvols_from_snapshots() {
    log "Creating LVOLs from snapshots"
    snapshots=($(sbcli-lvol snapshot list | grep "${LVOL_NAME}_ss_" | awk '{print $4}'))
    clone_count=0
    for snapshot in $(shuf -e "${snapshots[@]}"); do
        if [ $clone_count -ge $MAX_CLONES ]; then
            break
        fi
        clone_name="${snapshot}_cl"
        log "Creating clone from snapshot: $snapshot as $clone_name"
        snapshot_id=($(sbcli-lvol snapshot list | grep "$snapshot" | awk '{print $2}'))
        echo "sbcli-lvol -d snapshot clone $snapshot_id $clone_name"
        # pause_if_interactive_mode "$@"
        sbcli-lvol -d snapshot clone $snapshot_id $clone_name

#        log "Fetching logical volume ID for clone: $clone_name"
#        clone_id=$(sbcli-lvol lvol list | grep -i $clone_name | awk '{print $2}')
#
#        before_lsblk=$(sudo lsblk -o name)
#        connect_lvol $clone_id
#        after_lsblk=$(sudo lsblk -o name)
#        clone_device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')
#
#        mount_lvol $clone_device $clone_name
#        clone_count=$((clone_count + 1))
    done
}

# 2.e. mount these lvols
mount_clone_lvols() {
    log "Mounting clone LVOLs"
    clone_ids=($(sbcli-lvol lvol list | grep "_cl" | awk '{print $2}'))
    clone_count=0
    for clone_id in $("${clone_ids[@]}"); do
        before_lsblk=$(sudo lsblk -o name)
        connect_lvol $clone_id
        after_lsblk=$(sudo lsblk -o name)
        clone_device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

        mount_lvol $clone_device $clone_name
        clone_count=$((clone_count + 1))
    done
}

mount_lvol() {
    local device=$1
    local name=$2
    log "Mounting LVOL: $device"
    local mount_point="$MOUNT_DIR/$name"
    sudo mkdir -p $mount_point
    sudo mount /dev/$device $mount_point
}

format_and_mount_lvol() {
    local device=$1
    local name=$2
    log "Formatting LVOL: $device"
    sudo mkfs.$FS_TYPE /dev/$device
    mount_lvol $device $name
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
    snapshots=$(sbcli-lvol snapshot list | grep -i _ss_ | awk '{print $2}')
    for snapshot in $snapshots; do
        log "Deleting snapshot: $snapshot"
        sbcli-lvol snapshot delete $snapshot --force
    done
}

delete_lvols() {
    log "Deleting all logical volumes, including clones"
    lvols=$(sbcli-lvol lvol list | grep -i _cl | awk '{print $2}')
    for lvol in $lvols; do
        log "Deleting clone volume: $lvol"
        sbcli-lvol lvol delete $lvol
    done

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

log "Now the main script starts"
pause_if_interactive_mode "$@"
# Main script execution
get_cluster_id
# create_pool $cluster_id

create_lvol

log "Connecting logical volume"
lvol_id=$(sbcli-lvol lvol list | grep $LVOL_NAME | awk '{print $2}')

before_lsblk=$(sudo lsblk -o name)
connect_lvol $lvol_id
after_lsblk=$(sudo lsblk -o name)
device=$(diff <(echo "$before_lsblk") <(echo "$after_lsblk") | grep "^>" | awk '{print $2}')

format_and_mount_lvol $device $LVOL_NAME
mount_point="/mnt/$LVOL_NAME"

log "Starting fio workload"
run_fio_workload $mount_point &
FIO_PID=$!

log "Starting snapshot creation loop"
create_snapshots $lvol_id &
SNAPSHOT_PID=$!

# Wait for FIO workload to complete
wait $FIO_PID
log "Fio workload completed"

# Stop the snapshot creation loop
log "Stopping snapshot creation loop"
kill $SNAPSHOT_PID

# List snapshots and create LVOLs from them
list_snapshots

pause_if_interactive_mode "$@"
create_lvols_from_snapshots "$@"
pause_if_interactive_mode "$@"

#log "Running FIO workload on all clone mount points"
#clone_mount_dirs=$(sudo find /mnt -mindepth 1 -type d -name "*_clone")
#for clone_mount_point in $clone_mount_dirs; do
#    run_fio_workload $clone_mount_point &
#done
#
#wait

log "Script execution completed"

unmount_all
remove_mount_dirs
disconnect_lvols
delete_snapshots
delete_lvols
#delete_pool

log "CLEANUP COMPLETE"
