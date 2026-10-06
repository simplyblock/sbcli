#!/usr/bin/env bash
# Stand up async replication by hand and watch one volume replicate.
#
# Run this ON THE MANAGEMENT NODE, after a normal cluster bootstrap has given
# you cluster A. It builds cluster B from spare storage nodes on the same
# control plane, wires a target and a policy, creates a volume under it, and
# then tells you what to watch.
#
# WHAT THIS IS NOT. This is the ENGINE path: it talks to sbctl over ssh. It
# does NOT create a Kubernetes cluster, does not read a kubeconfig, and has
# no concept of a worker node. On Kubernetes the clusters come from the
# operator's StorageCluster CR -- bring your own, then use --dr, which only
# PRINTS the csi-addons commands and assumes your current kubectl context
# already points at the right cluster.
#
# IT WILL REBUILD MACHINES. C2_NODES names the hosts that become cluster B.
# Each one is ssh'd into as root and has `sn configure` + `sn deploy` run on
# it. Run `--help` first and check that list before anything else.
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
#   * THERE ARE TWO FRONT DOORS, and this script's default is one of them.
#     Everything below drives the ENGINE through sbctl. That is the whole
#     story on docker, and it is what csi-addons calls underneath. But on
#     Kubernetes an operator -- or Ramen -- drives the same engine by flipping
#     VolumeReplication.spec.replicationState instead, and that path has its
#     own semantics. Use --dr to walk the same four steps through it.
#
# Usage:
#   ./replication_manual_setup.sh --help               # every input and its default
#   ./replication_manual_setup.sh                      # build + start replicating
#   ./replication_manual_setup.sh --skip-cluster-b     # cluster B already exists
#   ./replication_manual_setup.sh --verify             # read-only checks only
#   ./replication_manual_setup.sh --dr                 # the csi-addons walkthrough
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

# ────────────────────────────────────────────────────────────────── help
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<EOF

  replication_manual_setup.sh -- stand up async replication by hand.

  WHERE TO RUN IT
    On the MANAGEMENT NODE of an existing cluster. It needs \$SBCLI_CMD on
    PATH and passwordless root ssh to the machines named in C2_NODES.

  WHAT IT DOES
    1. Finds cluster A by asking 'sbctl cluster list' for the first cluster.
    2. Builds cluster B out of C2_NODES, on the SAME control plane.
    3. Creates a pool on each, a replication target A->B, and a policy.
    4. Creates one volume under that policy and waits for it to replicate.

  IT IS NOT A KUBERNETES SCRIPT. No kubeconfig is read, no cluster is
  created, no worker is discovered. On k8s the clusters come from the
  operator's StorageCluster CR; bring your own and use --dr, which only
  prints commands against your current kubectl context.

  ┌─ THE ONE THAT MATTERS ──────────────────────────────────────────────┐
  │ C2_NODES          these hosts are ssh'd into as root and REBUILT    │
  │                   as cluster B. They must NOT already be in         │
  │                   cluster A. Check this before anything else.       │
  │                   now: $C2_NODES
  └──────────────────────────────────────────────────────────────────────┘

  INPUTS  (environment variables; current value shown)

    SBCLI_CMD                  $SBCLI
      The CLI to call. sbcli-dev on a dev build, sbctl on a release.

    C2_NODES                   $C2_NODES
      Storage nodes for cluster B, space separated. See the box above.

    IFNAME                     $IFNAME
      Management NIC name on the cluster-B nodes.
    BOOTSTRAP_DATA_NIC         $DATA_NIC
      Data NIC. NVMe-oF runs here; it is NOT the management network.

    NDCS / NPCS                $NDCS / $NPCS
      Erasure-coding geometry for cluster B. 1/1 is the smallest that
      works and is what the automated lane uses.
    HA_TYPE                    $HA_TYPE
    BOOTSTRAP_HA_JM_COUNT      $HA_JM_COUNT
    BOOTSTRAP_JOURNAL_PARTITION $JOURNAL_PARTITION
    BOOTSTRAP_MAX_SUBSYS       $MAX_SUBSYS
    SPDK_IMAGE                 ${SPDK_IMAGE:-<cluster default>}
      Pin an SPDK image. Leave unset unless you are testing a specific one.

    POOL_A / POOL_B            $POOL_A / $POOL_B
    TARGET_NAME                $TARGET_NAME
    POLICY_NAME                $POLICY_NAME

    VOL_NAME                   $VOL_NAME
    VOL_SIZE                   $VOL_SIZE
      Deliberately small. EVERY transfer is a full copy, so volume size --
      not how much changed -- sets how long a cycle takes.
    INTERVAL_MIN               $INTERVAL_MIN
      Minutes. An INTEGER here; the CRD takes a duration string ("5m")
      instead, and the two are not interchangeable.
    MODE                       $MODE
      failover | migration.

    NAMESPACE                  \${NAMESPACE:-simplyblock}    (--dr only)
    PVC_NAME                   \${PVC_NAME:-\$VOL_NAME}        (--dr only)

  EXAMPLES

    # look before you leap
    C2_NODES="10.0.0.11 10.0.0.12" $0 --help

    # build it, on your own nodes, with a bigger volume
    C2_NODES="10.0.0.11 10.0.0.12" VOL_SIZE=10G INTERVAL_MIN=5 $0

    # cluster B already exists -- just wire the replication
    $0 --skip-cluster-b

    # read-only: works against a cluster you did not build
    VOL_NAME=myvol $0 --verify

    # the csi-addons / Ramen path (prints commands, runs nothing)
    VOL_NAME=myvol PVC_NAME=my-pvc $0 --dr

EOF
  exit 0
fi

# ─────────────────────────────────────────────────────────────── cleanup
# ──────────────────────────────────────────────────────── read-only checks
if [[ "${1:-}" == "--verify" ]]; then
  # Self-sufficient on purpose: a read-only check has to work against a
  # cluster somebody else built, so it resolves its own ids rather than
  # relying on anything this script set up earlier.
  V_CLUSTER=$($SBCLI cluster list --json 2>/dev/null \
    | python3 -c "import json,sys
d=json.load(sys.stdin); rows=d if isinstance(d,list) else d.get('results',[])
print((rows[0].get('id') or rows[0].get('uuid')) if rows else '')" 2>/dev/null)
  V_VOL=$($SBCLI volume list --json 2>/dev/null \
    | python3 -c "import json,sys
d=json.load(sys.stdin); rows=d if isinstance(d,list) else d.get('results',[])
print(next((v.get('uuid') or v.get('id') for v in rows
            if v.get('lvol_name')=='$VOL_NAME'), ''))" 2>/dev/null)
  if [[ -z "$V_VOL" ]]; then
    echo "No volume named '$VOL_NAME'. Set VOL_NAME to one that exists:"
    $SBCLI volume list 2>&1 | head -20
    exit 1
  fi
  echo "== relationship  (volume $VOL_NAME = $V_VOL)"
  $SBCLI volume replication-relationship "$V_VOL" --json 2>&1 | head -40
  echo; echo "== steady-state status (the typed read csi-addons serves)"
  $SBCLI volume replication-status "$V_VOL" 2>&1 | head -20
  echo; echo "== cluster view"
  $SBCLI cluster replication-status "$V_CLUSTER" 2>&1 | head -30
  echo; echo "== targets and policies"
  $SBCLI cluster replication-target-list --cluster-id "$V_CLUSTER" 2>&1 | head
  $SBCLI cluster replication-policy-list 2>&1 | head
  echo
  echo "What 'healthy' looks like: state=replicating, direction=to_target,"
  echo "a lag that is NOT growing cycle over cycle, and a lastReplicatedAt"
  echo "that moves. A lag that only ever rises means the cadence cannot be"
  echo "met -- with full transfers that is set by volume size, not by how"
  echo "much changed."
  exit 0
fi

# ──────────────────────────────────────────────── the csi-addons / DR door
if [[ "${1:-}" == "--dr" ]]; then
  # Same engine, different front door. Everything this does ends up as the
  # same control-plane calls the sbctl path above makes -- the point is to
  # watch it happen from the surface Ramen actually drives.
  command -v kubectl >/dev/null 2>&1 || {
    echo "--dr needs kubectl. On docker there is no Kubernetes and therefore"
    echo "no csi-addons; the engine walkthrough (no flag) is the only path."
    exit 1; }
  if ! kubectl get crd volumereplications.replication.storage.openshift.io \
       >/dev/null 2>&1; then
    echo "The VolumeReplication CRD is not installed."
    echo
    echo "This needs operator PR #548 (integrate_csi_addons) with"
    echo "csiaddons.create enabled in the chart. Until then the engine path"
    echo "(run this script with no flag) is the only way to drive replication."
    exit 1
  fi
  NS="${NAMESPACE:-simplyblock}"
  PVC="${PVC_NAME:-$VOL_NAME}"
  POLICY_ID="${POLICY_ID:-$POLICY_NAME}"
  cat <<EOF

  THE CSI-ADDONS WALKTHROUGH
  ==========================
  Four steps, and they map one-to-one onto the sbctl ones. What changes is
  that you never name an operation: you declare which side should be primary
  and the controller calls Promote/Demote/Resync on our driver for you.

  1. The class -- names the policy. The DR cluster's copy of this is the
     SAME object with an EMPTY replicationPolicyID: the driver reads empty as
     "this side is the fail-over target". Nothing in the schema says so.

     kubectl apply -f - <<'YAML'
     apiVersion: replication.storage.openshift.io/v1alpha1
     kind: VolumeReplicationClass
     metadata:
       name: sb-async
     spec:
       provisioner: csi.simplyblock.io
       parameters:
         replicationPolicyID: "$POLICY_ID"
     YAML

  2. Protect one volume.

     kubectl apply -n $NS -f - <<'YAML'
     apiVersion: replication.storage.openshift.io/v1alpha1
     kind: VolumeReplication
     metadata:
       name: $VOL_NAME-repl
     spec:
       volumeReplicationClass: sb-async
       replicationState: primary
       dataSource:
         kind: PersistentVolumeClaim
         name: $PVC
     YAML

     IMPORTANT: if this PVC already carries the annotation
     storage.simplyblock.io/replication-policy, this will be REFUSED with
     FAILED_PRECONDITION -- even if both name the same policy. The two paths
     are mutually exclusive per volume because ownership is the conflict, not
     the value. Remove the annotation first, wait for the slot to detach,
     then apply this.

  3. Watch it. These three conditions are the entire contract Ramen reads;
     it folds them into DataProtected and then PeerReady, the boolean that
     gates whether a relocation may start at all.

     kubectl get volumereplication $VOL_NAME-repl -n $NS \
       -o jsonpath='{.status.conditions}' | python3 -m json.tool
     kubectl get volumereplication $VOL_NAME-repl -n $NS \
       -o jsonpath='{.status.lastSyncTime}'

     Cross-check the SAME relationship from the engine side -- it should
     agree, because there is only one:
       $SBCLI volume replication-status $VOL_NAME

  4. Move it. There is no "failover" verb; you change which side is primary.

     # promote this side  (= failover)
     kubectl patch volumereplication $VOL_NAME-repl -n $NS --type=merge \
       -p '{"spec":{"replicationState":"primary"}}'

     # step this side down (= demote, the lossless half of a planned swap)
     kubectl patch volumereplication $VOL_NAME-repl -n $NS --type=merge \
       -p '{"spec":{"replicationState":"secondary"}}'

  THE ONE TO WATCH FOR
  --------------------
  A PLANNED promote with no prior demote is refused by our driver with
  FAILED_PRECONDITION -- and the vendored csi-addons controller escalates any
  such refusal to force=true INLINE, with no grace period. force=true clones
  the last fully replicated generation and ignores demote state entirely.

  So: demote first, wait for it to settle, THEN promote. If you promote
  straight from primary->secondary->primary you will likely get the forced
  path, lose up to one interval, and the object will report success either
  way. Write a marker file before trying it and see whether it survives.

  Teardown:
    kubectl delete volumereplication $VOL_NAME-repl -n $NS
    kubectl delete volumereplicationclass sb-async

EOF
  exit 0
fi

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
# ───────────────────────────────────────────────────────────── preflight
# The build path talks to sbctl locally and ssh's to IPs. Neither exists on
# Kubernetes, so check before producing a pile of ssh errors that look like
# a lab problem rather than a wrong-tool problem.
if ! $SBCLI cluster list >/dev/null 2>&1; then
  echo
  echo "Cannot run '$SBCLI cluster list' here."
  echo
  if command -v kubectl >/dev/null 2>&1; then
    cat <<'EOF'
  kubectl IS present, so this is probably a Kubernetes cluster -- and the
  build path below does not work there. It ssh's to IPs and runs sn
  configure / sn deploy / cluster add, none of which exist on k8s:

    * there is no management node to run sbctl on
    * nodes are identified by NAME, never by IP
    * a simplyblock cluster is a StorageCluster CR the operator reconciles
    * "two clusters" means two NAMESPACES in ONE Kubernetes cluster, because
      the control plane cannot span two

  WHAT TO DO INSTEAD ON KUBERNETES

    1. Let the pipeline build both clusters. That is
       k8s-native-cross-cluster-restore.yaml: it halves the worker_nodes
       input by NODE NAME into two StorageNodeSets, one per namespace, and
       reaches both through the single kubeconfig picked by the
       cluster_environment input.

    2. Then come back here for the parts that DO work on k8s:
         ./replication_manual_setup.sh --verify     # read-only checks
         ./replication_manual_setup.sh --dr         # the csi-addons commands

       --verify needs sbctl reachable; run it from the control-plane pod:
         kubectl -n simplyblock exec -it deploy/simplyblock-control-plane -- bash

    3. Or run the automated lane, which adopts the pipeline's two clusters
       rather than building any:
         python e2e/e2e.py --testname replication
EOF
  else
    cat <<'EOF'
  No kubectl either, so this looks like a docker lab where sbctl is simply
  not on PATH or not configured. Check:
    * SBCLI_CMD is right (currently: it is what this script will call)
    * you are on the MANAGEMENT node, not a storage node
    * the control plane is up:  docker ps | grep WebAppAPI
EOF
  fi
  echo
  exit 1
fi

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

  OTHER THINGS YOU CAN DO FROM HERE
  ---------------------------------
  Read-only checks against what you just built, or against someone else's
  cluster:
    $0 --verify

  The same four steps through the Kubernetes DR surface, which is what Ramen
  drives and what an operator will actually use on k8s:
    $0 --dr

  THE AUTOMATED SUITES. Two lanes, separate on purpose -- a soak measured in
  hours must never stand between a correctness run and its answer.

    # correctness: AR-S..AR-K, 26 classes, ~2-3h
    python e2e/e2e.py --testname replication

    # scale, endurance, upgrade: AR-P + AR-U, 7 classes, hours to a weekend
    python e2e/e2e.py --testname replication-stress

    # scale knobs (defaults shown). The right value is a property of the lab.
    AR_OVERLAP_SIZE=20G  AR_MANY_COUNT=25   AR_MANY_SIZE=1G \\
    AR_LARGE_SIZE=100G   AR_SOAK_HOURS=6    AR_SOAK_RETENTION=3 \\
      python e2e/e2e.py --testname replication-stress

  TWO NUMBERS WORTH MEASURING BY HAND, because nothing else reports them and
  both are arguments with dev rather than bugs:

    1. How long one full cycle takes on a volume the size your customers use.
       That is the shortest interval you can honestly offer, and with full
       transfers it does not improve when little changes.

    2. How long the source is FENCED during a planned relocation. Demote
       fences first and then ships, and that ship is the whole volume -- so
       the "graceful" operation's downtime scales with size. Time it with:
         time ( $SBCLI volume replication-failback <vol> --source-cluster-id <A> \\
                && $SBCLI volume replication-commit <vol> )

EOF
