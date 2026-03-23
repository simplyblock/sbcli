#!/bin/bash
# Lab lvol migration E2E test
# Runs step-by-step on lab cluster (.111-.115)
# Usage: Run each section manually or source this file section by section
#
# Prerequisites: cluster deployed with feature-lvol-migration branch,
# all nodes online, pool created.

set -euo pipefail

SSH="python3 tests/perf/ssh_run.py"
MGMT=192.168.10.111
SN1=192.168.10.112
SN2=192.168.10.113
SN3=192.168.10.114
CLIENT=192.168.10.115

echo "============================================"
echo "STEP 0: Check cluster state"
echo "============================================"
$SSH "sbctl cluster list" $MGMT 15
$SSH "sbctl sn list" $MGMT 15
$SSH "sbctl pool list" $MGMT 15

echo ""
echo "============================================"
echo "STEP 1: Get storage node UUIDs"
echo "============================================"
# Parse node UUIDs - pick source and target
NODE_LIST=$($SSH "sbctl sn list" $MGMT 15)
echo "$NODE_LIST"
echo ""
echo ">>> Pick source (first node with lvol) and target (different node)."
echo ">>> Set SRC_NODE and TGT_NODE UUIDs below, then continue."
echo ""

# Auto-detect: first two online node UUIDs
SRC_NODE=$(echo "$NODE_LIST" | grep "online" | head -1 | awk -F'|' '{print $2}' | tr -d ' ')
TGT_NODE=$(echo "$NODE_LIST" | grep "online" | sed -n '2p' | awk -F'|' '{print $2}' | tr -d ' ')
echo "SRC_NODE=$SRC_NODE"
echo "TGT_NODE=$TGT_NODE"

echo ""
echo "============================================"
echo "STEP 2: Create 100G volume"
echo "============================================"
$SSH "sbctl lvol add lvol_mig_lab 100G pool01" $MGMT 30
$SSH "sbctl lvol list" $MGMT 15
LVOL_UUID=$($SSH "sbctl lvol list" $MGMT 15 | grep "lvol_mig_lab" | awk -F'|' '{print $2}' | tr -d ' ')
echo "LVOL_UUID=$LVOL_UUID"

echo ""
echo "============================================"
echo "STEP 3: Connect volume to client"
echo "============================================"
CONNECT_CMDS=$($SSH "sbctl lvol connect $LVOL_UUID" $MGMT 15)
echo "$CONNECT_CMDS"
echo ""
echo ">>> Running connect commands on client..."

# Run each nvme connect line on the client
echo "$CONNECT_CMDS" | grep "^sudo nvme connect" | while IFS= read -r cmd; do
    echo "Running: $cmd"
    $SSH "$cmd" $CLIENT 30 || true
done
sleep 3

echo ""
echo "============================================"
echo "STEP 4: Find NVMe device on client"
echo "============================================"
$SSH "lsblk -d -n -o NAME,SIZE,TYPE | grep disk" $CLIENT 10
$SSH "nvme list" $CLIENT 10
echo ""
echo ">>> Set NVME_DEV below (e.g. /dev/nvme1n1), then continue."
NVME_DEV=$($SSH "lsblk -d -n -o NAME,TYPE" $CLIENT 10 | grep disk | grep -v nvme0 | head -1 | awk '{print "/dev/"$1}')
echo "NVME_DEV=$NVME_DEV"

echo ""
echo "============================================"
echo "STEP 5: Format XFS and mount"
echo "============================================"
$SSH "mkfs.xfs -f $NVME_DEV" $CLIENT 60
$SSH "mkdir -p /mnt/migtest" $CLIENT 5
$SSH "mount $NVME_DEV /mnt/migtest" $CLIENT 10
$SSH "df -h /mnt/migtest" $CLIENT 5

echo ""
echo "============================================"
echo "STEP 6: Start fio (background, 1 hour)"
echo "============================================"
$SSH "systemd-run --unit=fio-migtest --remain-after-exit fio --name=lvol_migration_test --directory=/mnt/migtest --direct=1 --rw=randrw --bs=4K --size=10G --numjobs=4 --iodepth=16 --ioengine=libaio --time_based --runtime=3600 --group_reporting --output=/tmp/fio_output.log" $CLIENT 15
sleep 5
$SSH "pgrep fio | wc -l" $CLIENT 5
echo ">>> fio should show > 0 processes"

echo ""
echo "============================================"
echo "STEP 7: Take 5 snapshots (30s intervals)"
echo "============================================"
for i in 0 1 2 3 4; do
    if [ $i -gt 0 ]; then
        echo "  Waiting 30s..."
        sleep 30
    fi
    $SSH "sync" $CLIENT 10
    $SSH "sbctl snapshot add $LVOL_UUID snap_mig_$i" $MGMT 30
    echo "  Snapshot $((i+1))/5: snap_mig_$i"
done
$SSH "sbctl snapshot list" $MGMT 15

echo ""
echo "============================================"
echo "STEP 8: Verify fio still running"
echo "============================================"
$SSH "pgrep fio | wc -l" $CLIENT 5

echo ""
echo "============================================"
echo "STEP 9: Trigger migration"
echo "============================================"
echo "Migrating $LVOL_UUID from $SRC_NODE to $TGT_NODE"
$SSH "sbctl lvol migrate $LVOL_UUID $TGT_NODE --max-retries 20" $MGMT 30

echo ""
echo "============================================"
echo "STEP 10: Poll migration status"
echo "============================================"
for poll in $(seq 1 120); do
    STATUS=$($SSH "sbctl lvol migrate-list" $MGMT 15 2>/dev/null)
    echo "Poll $poll: $(echo "$STATUS" | grep -v "^+" | grep -v "^|.*Migration ID" | grep "|" | head -1)"

    if echo "$STATUS" | grep -qi "done\|completed"; then
        echo ">>> MIGRATION COMPLETED!"
        break
    fi
    if echo "$STATUS" | grep -qi "failed"; then
        echo ">>> MIGRATION FAILED!"
        echo "$STATUS"
        echo ""
        echo "=== Collecting debug info ==="
        $SSH "sbctl lvol migrate-list" $MGMT 15
        $SSH "sbctl lvol list" $MGMT 15
        $SSH "sbctl snapshot list" $MGMT 15
        # Task runner logs
        CONTAINER=$($SSH "docker ps --format '{{.Names}}' | grep -i lvolmig" $MGMT 10)
        $SSH "docker logs $CONTAINER --tail 100" $MGMT 30
        echo ""
        echo ">>> STOPPING - analyze before proceeding"
        exit 1
    fi
    sleep 5
done

echo ""
echo "============================================"
echo "STEP 11: Verify fio survived"
echo "============================================"
FIO_COUNT=$($SSH "pgrep fio | wc -l" $CLIENT 5)
echo "fio processes: $FIO_COUNT"
if [ "$FIO_COUNT" -gt 0 ]; then
    echo ">>> SUCCESS: fio still running"
else
    echo ">>> FAILURE: fio died during migration"
    $SSH "cat /tmp/fio_output.log | tail -20" $CLIENT 10
fi

echo ""
echo "============================================"
echo "STEP 12: Final state"
echo "============================================"
$SSH "sbctl lvol list" $MGMT 15
$SSH "sbctl snapshot list" $MGMT 15
$SSH "sbctl lvol migrate-list" $MGMT 15

echo ""
echo "============================================"
echo "STEP 13: Cleanup"
echo "============================================"
echo ">>> Run these manually when done:"
echo "  $SSH 'killall fio' $CLIENT 5"
echo "  $SSH 'umount /mnt/migtest' $CLIENT 10"
echo "  $SSH 'nvme disconnect-all' $CLIENT 10"
echo "  $SSH 'sbctl lvol delete $LVOL_UUID --force' $MGMT 30"
