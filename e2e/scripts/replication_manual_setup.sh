#!/usr/bin/env bash
# Stand up async replication by hand and watch one volume replicate.
#
# Run this ON THE MANAGEMENT NODE, after a normal cluster bootstrap has given
# you cluster A. It builds cluster B from spare storage nodes on the same
# control plane, wires a target and a policy, creates a volume under it, and
# then tells you what to watch.
#
# Why two clusters under one control plane rather than two real sites: the
# control plane cannot live in two Kubernetes clusters, only span availability
# zones within one. That is the only shape the product supports today.
#
# Three things worth knowing before you read the output:
#
#   * EVERY TRANSFER IS A FULL TRANSFER. allow_partial is disabled because the
#     SPDK fork corrupts partial transfers (PR #1276, 52e75afb2). A 20G volume
#     on a 1-minute cadence re-sends 20G every minute, so VOL_SIZE is small.
#
#   * A migration or fail-back PARKS in cutover_pending until something sets
#     cutover_proceed. On Kubernetes the operator does that. Here nothing does,
#     so you have to -- see step 9. Without it the operation sits until the
#     safety timeout and then looks like a slow success.
#
#   * FAIL-BACK REVERSES DIRECTION. The original source becomes the TARGET of
#     the reverse relationship, so a lookup by source will not find it.
#
# Usage:
#   ./replication_manual_setup.sh                      # build + start replicating
#   ./replication_manual_setup.sh --skip-cluster-b     # cluster B already exists
#   ./replication_manual_setup.sh --cleanup            # tear the replication down
set -uo pipefail

# ─────────────────────────────────────────────────────────────── settings
SBCLI="${SBCLI_CMD:-sbcli-dev}"
# Storage nodes for cluster B. These must NOT already be in cluster A.
C2_NODES="${C2_NODES:-192.168.10.203 192.168.10.204}"
IFNAME="${IFNAME:-eth0}"
DATA_NIC="${BOOTSTRAP_DATA_NIC:-eth1}"
NDCS="${NDCS:-1}"
NPCS="${NPCS:-1}"
HA_TYPE="${HA_TYPE:-ha}"
JOURNAL_PARTITION="${BOOTSTRAP_JOURNAL_PARTITION:-0}"
HA_JM_COUNT="${BOOTSTRAP_HA_JM_COUNT:-3}"
MAX_SUBSYS="${BOOTSTRAP_MAX_SUBSYS:-1024}"
SBCLI_BRANCH="${SBCLI_BRANCH:-main}"
SPDK_IMAGE="${SPDK_IMAGE:-}"

POOL_A="${POOL_A:-poolA}"
POOL_B="${POOL_B:-poolB}"
TARGET_NAME="${TARGET_NAME:-tgt1}"
POLICY_NAME="${POLICY_NAME:-pol1}"
VOL_NAME="${VOL_NAME:-replvol1}"
VOL_SIZE="${VOL_SIZE:-2G}"          # small on purpose: full transfers
INTERVAL_MIN="${INTERVAL_MIN:-1}"
MODE="${MODE:-failover}"            # failover | migration

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '   \033[0;32m+ %s\033[0m\n' "$*"; }
warn() { printf '   \033[0;33m! %s\033[0m\n' "$*"; }
die()  { printf '\n\033[0;31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

# ─────────────────────────────────────────────────────────────── cleanup
if [[ "${1:-}" == "--cleanup" ]]; then
    say "Tearing down replication (volume, policy, target)"
    VOL_ID=$($SBCLI volume list --json 2>/dev/null \
             | python3 -c "import json,sys;print(next((v['id'] for v in json.load(sys.stdin) if v.get('lvol_name')=='$VOL_NAME'),''))" 2>/dev/null)
    [[ -n "$VOL_ID" ]] && { $SBCLI -d volume replication-policy-clear "$VOL_ID" || true
                            $SBCLI -d volume delete "$VOL_ID" --force || true; }
    $SBCLI -d cluster replication-policy-remove "$POLICY_NAME" || true
    $SBCLI -d cluster replication-target-remove "$TARGET_NAME" || true
    ok "done (clusters left alone)"
    exit 0
fi

SKIP_B=0
[[ "${1:-}" == "--skip-cluster-b" ]] && SKIP_B=1

# ─────────────────────────────────────────── 1. identify cluster A
say "1. Cluster A"
$SBCLI cluster list || die "cannot reach the control plane"
CLUSTER_A=$($SBCLI cluster list --json 2>/dev/null \
  | python3 -c "import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('results',[])
print(rows[0].get('id') or rows[0].get('uuid') if rows else '')" 2>/dev/null)
[[ -n "$CLUSTER_A" ]] || die "could not read cluster A's id from 'cluster list --json'"
ok "cluster A = $CLUSTER_A"

# ─────────────────────────────────────────── 2. build cluster B
if [[ $SKIP_B -eq 0 ]]; then
  say "2. Building cluster B from: $C2_NODES"
  for ip in $C2_NODES; do
      echo "   preparing $ip"
      ssh -o StrictHostKeyChecking=no "root@$ip" \
        "pip install --force-reinstall git+https://github.com/simplyblock-io/sbcli.git@${SBCLI_BRANCH}" \
        || die "sbcli install failed on $ip"
      ssh -o StrictHostKeyChecking=no "root@$ip" \
        "$SBCLI --dev -d sn configure --max-subsys $MAX_SUBSYS" || die "sn configure failed on $ip"
      ssh -o StrictHostKeyChecking=no "root@$ip" \
        "$SBCLI sn deploy --ifname $IFNAME" || die "sn deploy failed on $ip"
  done
  echo "   waiting 30s for SPDK containers"
  sleep 30

  $SBCLI --dev -d cluster add --ha-type "$HA_TYPE" \
      --data-chunks-per-stripe "$NDCS" --parity-chunks-per-stripe "$NPCS" \
      || die "cluster add failed"

  CLUSTER_B=$($SBCLI cluster list --json 2>/dev/null \
    | python3 -c "import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('results',[])
print(next((r.get('id') or r.get('uuid') for r in rows
            if (r.get('id') or r.get('uuid')) != '$CLUSTER_A'), ''))" 2>/dev/null)
  [[ -n "$CLUSTER_B" ]] || die "could not identify cluster B after 'cluster add'"
  ok "cluster B = $CLUSTER_B"

  ADD="$SBCLI --dev -d storage-node add-node --journal-partition $JOURNAL_PARTITION \
       --ha-jm-count $HA_JM_COUNT --data-nics $DATA_NIC"
  [[ -n "$SPDK_IMAGE" ]] && ADD="$ADD --spdk-image $SPDK_IMAGE"
  for ip in $C2_NODES; do
      echo "   adding $ip to cluster B"
      $ADD "$CLUSTER_B" "$ip:5000" "$IFNAME" || die "add-node failed for $ip"
      sleep 3
  done

  $SBCLI -d cluster activate "$CLUSTER_B" || die "cluster activate failed"
  echo "   waiting for cluster B to go ACTIVE"
  for _ in $(seq 1 60); do
      $SBCLI cluster list | grep -q "$CLUSTER_B.*ACTIVE" && break
      sleep 15
  done
  $SBCLI cluster list | grep -q "$CLUSTER_B.*ACTIVE" \
      || die "cluster B never reached ACTIVE -- check 'sbcli cluster list'"
  ok "cluster B is ACTIVE"
else
  CLUSTER_B=$($SBCLI cluster list --json 2>/dev/null \
    | python3 -c "import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('results',[])
print(next((r.get('id') or r.get('uuid') for r in rows
            if (r.get('id') or r.get('uuid')) != '$CLUSTER_A'), ''))" 2>/dev/null)
  [[ -n "$CLUSTER_B" ]] || die "--skip-cluster-b given but no second cluster found"
  ok "cluster B = $CLUSTER_B (existing)"
fi

# ─────────────────────────────────────────── 3. pools
say "3. Pools on both clusters"
$SBCLI -d pool add "$POOL_A" "$CLUSTER_A" 2>/dev/null || warn "pool $POOL_A may already exist"
$SBCLI -d pool add "$POOL_B" "$CLUSTER_B" 2>/dev/null || warn "pool $POOL_B may already exist"
$SBCLI pool list
ok "pools ready"

# ─────────────────────────────────────────── 4. replication target
say "4. Replication target: A -> B"
$SBCLI -d cluster replication-target-add "$CLUSTER_A" "$TARGET_NAME" "$CLUSTER_B" \
    --target-pool "$POOL_B" || die "replication-target-add failed"
$SBCLI cluster replication-target-list --cluster-id "$CLUSTER_A"
ok "target '$TARGET_NAME' created"

# ─────────────────────────────────────────── 5. policy
say "5. Replication policy (cadence ${INTERVAL_MIN}m, mode $MODE)"
$SBCLI -d cluster replication-policy-add "$CLUSTER_A" "$POLICY_NAME" "$TARGET_NAME" \
    --interval-min "$INTERVAL_MIN" --mode "$MODE" || die "replication-policy-add failed"
$SBCLI cluster replication-policy-list
ok "policy '$POLICY_NAME' created"

# ─────────────────────────────────────────── 6. volume under the policy
say "6. Volume $VOL_NAME ($VOL_SIZE) with the policy attached at create time"
$SBCLI -d volume add "$VOL_NAME" "$VOL_SIZE" --pool "$POOL_A" \
    --replication-policy "$POLICY_NAME" || die "volume add failed"
VOL_ID=$($SBCLI volume list --json 2>/dev/null \
  | python3 -c "import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('results',[])
print(next((v.get('id') or v.get('uuid') for v in rows
            if v.get('lvol_name')=='$VOL_NAME'), ''))" 2>/dev/null)
[[ -n "$VOL_ID" ]] || die "could not read the id of $VOL_NAME"
ok "volume id = $VOL_ID"

# ─────────────────────────────────────────── 7. what to watch
cat <<EOF

$(printf '\033[1;36m== 7. Replication is running. What to look at\033[0m')

  CLUSTER_A=$CLUSTER_A
  CLUSTER_B=$CLUSTER_B
  VOL_ID=$VOL_ID

  Lag and outstanding data (the health view):
    watch -n10 '$SBCLI volume replication-info $VOL_ID'

  Where the copy lives on the other cluster:
    $SBCLI volume replication-relationship $VOL_ID --json

  Everything replicating on this cluster:
    $SBCLI cluster replication-status $CLUSTER_A

  Snapshots on each side. Internal ones are pruned after transfer; the ones
  YOU take are replicated AND kept, which is the asymmetry worth checking:
    $SBCLI snapshot list

  STATE should be 'replicating'. The full set, from LVolReplication:
    replicating -> cutover_pending -> cutover_done     (migration / fail-back)
    replicating -> failed_over                         (fail-over)

$(printf '\033[1;36m== 8. Try a fail-over\033[0m')

  Write something first so there is data to lose, then fail the POLICY over:
    $SBCLI -d cluster replication-policy-failover $POLICY_NAME
    $SBCLI volume replication-relationship $VOL_ID --json

  There is no volume-level fail-over verb on the CLI. The surface is:
    cluster replication-policy-failover <policy>   all volumes in the policy
    cluster replication-target-failover <target>   all policies on the target
  The Kubernetes ReplicationOps CRD does take scope: volume, so single-volume
  fail-over is reachable there but not here. Worth asking dev whether that
  asymmetry is deliberate.

  Expect: a CLONE on the last replicated snapshot, state 'failed_over', and
  target_nqn / target_ns_id UNCHANGED -- the client is meant to keep the same
  NQN and namespace across the fail-over. That is a stronger check than "a
  volume appeared".

  Expect to lose up to one interval of data. Do NOT expect a torn or mixed
  state: the point of the snapshot is that it is a consistent instant.

$(printf '\033[1;36m== 9. Try a migration -- and mind the handshake\033[0m')

    $SBCLI -d volume replication-commit $VOL_ID

  It will PARK in cutover_pending. On Kubernetes the operator connects the
  target NVMe paths and signals; here nothing does, so it sits until
  REPL_CUTOVER_PROCEED_TIMEOUT_SEC expires and then looks like a slow success.
  To drive it properly, connect the target paths and then:

    curl -XPOST "\$API/lvol/$VOL_ID/replication/cutover-proceed"

  There is no CLI verb for this -- the API is the only route. On Kubernetes
  the operator calls it for you.

$(printf '\033[1;36m== 10. Logs\033[0m')

  Control plane, on the mgmt node:
    docker logs --tail 200 \$(docker ps -qf name=WebAppAPI)
    tail -f /var/log/simplyblock/tasks-runner*.log

  The replication task runner is where a stuck cycle shows up:
    grep -iE 'replicat|cutover|transfer' /var/log/simplyblock/*.log | tail -50

  SPDK on a storage node (the transfer itself):
    ssh root@<node> 'docker logs --tail 300 \$(docker ps -qf name=spdk_ | head -1)'

  What must NEVER appear, on either cluster:
    bad magic header | hdr_fail | MD corruption | Metadata page is all zero

  An md5 mismatch is NOT in that list. On devices without 4K write atomicity a
  torn write produces one legitimately, and the filesystem above is expected to
  cope. MD corruption is the one that matters: it means the metadata journal is
  not holding.

  Tear the replication down again with:
    $0 --cleanup

EOF
