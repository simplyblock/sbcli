#!/usr/bin/env bash
# Build a complete async-replication lab from nothing, on docker OR Kubernetes,
# and leave it ready for manual testing.
#
#   ./replication_lab_setup.sh --help        every input and its default
#   ./replication_lab_setup.sh               build BOTH clusters + wire replication
#
# This is the one-command entry point. It builds cluster A, builds cluster B,
# then hands off to replication_manual_setup.sh for the target/policy/volume
# wiring, so there is exactly one implementation of each half.
#
# WHY IT DELEGATES RATHER THAN REIMPLEMENTS
# Both platforms already have a proven, CI-exercised bring-up, and a second
# copy of either would drift from the one the pipelines actually run:
#
#   docker   simplyBlockDeploy/bare-metal/bootstrap-cluster.sh   (853 lines)
#   k8s      e2e/scripts/k8s_cluster_bringup.py                  (1125 lines)
#
# The k8s one matters more than it looks. The operator STOPPED RECONCILING
# StorageNodeSet -- it applies cleanly, nothing acts on it, and the cluster
# waits forever for storage nodes nobody asked for. The supported path is a
# ClusterDeploymentConfig: discovery inspects the workers, writes a draft, you
# set every value you care about (the draft is IMMUTABLE once approved), then
# approve it and the operator builds the cluster. That dance is what
# k8s_cluster_bringup.py exists for, and hand-rolling it here would reproduce
# the exact failure it was written to fix.
#
# WHAT "TWO CLUSTERS" MEANS ON EACH PLATFORM
#
#   docker   two sets of storage nodes, addressed by IP, one control plane.
#   k8s      two NAMESPACES in ONE Kubernetes cluster, one kubeconfig, workers
#            split by NODE NAME. The control plane cannot span two Kubernetes
#            clusters, so this is the only shape the product supports -- which
#            is also why no test here is a true site-loss test.
#
set -uo pipefail

PLATFORM="${PLATFORM:-auto}"          # auto | docker | k8s
SKIP_A="${SKIP_A:-0}"                 # cluster A already exists
SKIP_B="${SKIP_B:-0}"                 # cluster B already exists
SKIP_WIRING="${SKIP_WIRING:-0}"       # stop after the clusters

# ── docker inputs ───────────────────────────────────────────────────────
MNODES="${MNODES:-}"                            # management node(s)
STORAGE_PRIVATE_IPS="${STORAGE_PRIVATE_IPS:-}"  # cluster A storage nodes
C2_NODES="${C2_NODES:-}"                        # cluster B storage nodes
SBCLI_CMD="${SBCLI_CMD:-sbcli-dev}"
SSH_USER="${SSH_USER:-root}"
DEPLOY_REPO="${DEPLOY_REPO:-https://github.com/simplyblock-io/simplyBlockDeploy.git}"
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/simplyBlockDeploy}"

# ── k8s inputs ──────────────────────────────────────────────────────────
WORKER_NODES="${WORKER_NODES:-}"      # CSV of k8s NODE NAMES, split in half
NS_A="${NS_A:-simplyblock}"
NS_B="${NS_B:-simplyblock-c2}"
IFC_NAMES="${IFC_NAMES:-br-ex:enp2s0f0}"   # mgmt:data
ENVIRONMENT="${ENVIRONMENT:-openshift-local}"
DEVICE_MODE="${DEVICE_MODE:-nvme}"
SB_IMAGE="${SB_IMAGE:-}"

# ── shared ──────────────────────────────────────────────────────────────
NDCS="${NDCS:-1}"
NPCS="${NPCS:-1}"
HA_TYPE="${HA_TYPE:-ha}"
DATA_NIC="${BOOTSTRAP_DATA_NIC:-eth1}"
IFNAME="${IFNAME:-eth0}"
JOURNAL_PARTITION="${BOOTSTRAP_JOURNAL_PARTITION:-0}"
HA_JM_COUNT="${BOOTSTRAP_HA_JM_COUNT:-3}"
MAX_SUBSYS="${BOOTSTRAP_MAX_SUBSYS:-1024}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say()  { printf '\n\033[1;36m══ %s\033[0m\n' "$*"; }
ok()   { printf '   \033[0;32m✓ %s\033[0m\n' "$*"; }
warn() { printf '   \033[0;33m! %s\033[0m\n' "$*"; }
die()  { printf '\n\033[0;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

# ─────────────────────────────────────────────────────────────────── help
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<EOF

  replication_lab_setup.sh -- build a complete async-replication lab.

  It builds cluster A, builds cluster B, then wires a target, a policy and a
  volume, so you can go straight to manual testing. Each half delegates to the
  bring-up the pipelines already use; nothing is reimplemented here.

  PICK YOUR PLATFORM   PLATFORM=$PLATFORM   (auto | docker | k8s)
    auto decides by looking for a usable kubectl context first, then sbctl.

  ─────────────────────────────────────────────────────────── DOCKER
    Run from anywhere with ssh to the nodes. Needs:

      MNODES               ${MNODES:-<REQUIRED>}
        Management node(s). The control plane lands here.
      STORAGE_PRIVATE_IPS  ${STORAGE_PRIVATE_IPS:-<REQUIRED>}
        Storage nodes for cluster A.
      C2_NODES             ${C2_NODES:-<REQUIRED>}
        Storage nodes for cluster B. Must NOT overlap the two above.
        THESE MACHINES GET REBUILT.
      SSH_USER             $SSH_USER
      SBCLI_CMD            $SBCLI_CMD

    Cluster A is built by simplyBlockDeploy/bare-metal/bootstrap-cluster.sh,
    cloned to $DEPLOY_DIR. Cluster B is added to the same control plane.

  ───────────────────────────────────────────────────────────── K8S
    Run from anywhere kubectl works. ONE kubeconfig; both clusters live in it
    as two namespaces. Needs:

      WORKER_NODES         ${WORKER_NODES:-<REQUIRED>}
        CSV of Kubernetes NODE NAMES -- not IPs. Split in half: first half to
        cluster A, second to cluster B. You need (NDCS+NPCS)*2 = $(( (NDCS+NPCS) * 2 )) of them.
      NS_A / NS_B          $NS_A / $NS_B
      IFC_NAMES            $IFC_NAMES        (mgmt:data)
      ENVIRONMENT          $ENVIRONMENT
      DEVICE_MODE          $DEVICE_MODE      (nvme | lblk)
      KUBECONFIG           ${KUBECONFIG:-<your current context>}

    Both clusters are built by e2e/scripts/k8s_cluster_bringup.py, once per
    namespace. It drives the ClusterDeploymentConfig flow -- discover, patch
    the draft, approve -- which is the only path the operator still honours:
    StorageNodeSet is no longer reconciled, so applying one just hangs.

  ─────────────────────────────────────────────────────── BOTH PLATFORMS
      NDCS / NPCS          $NDCS / $NPCS        erasure coding, per cluster
      HA_TYPE              $HA_TYPE
      SKIP_A / SKIP_B      $SKIP_A / $SKIP_B        reuse an existing cluster
      SKIP_WIRING          $SKIP_WIRING          stop after the clusters

  EXAMPLES

    # docker, from scratch
    MNODES=10.0.0.5 \\
    STORAGE_PRIVATE_IPS="10.0.0.11 10.0.0.12" \\
    C2_NODES="10.0.0.13 10.0.0.14" \\
      $0

    # k8s, from scratch -- node NAMES, not IPs
    KUBECONFIG=~/.kube/config-openshift-local \\
    WORKER_NODES="worker-1,worker-2,worker-3,worker-4" \\
      $0

    # clusters already exist, just wire replication
    SKIP_A=1 SKIP_B=1 $0

    # build the lab and stop, so you can wire it by hand
    SKIP_WIRING=1 $0

  AFTERWARDS
    ./replication_manual_setup.sh --verify     read-only checks
    ./replication_manual_setup.sh --dr         the csi-addons / Ramen path
    python e2e/e2e.py --testname replication   the automated lane

EOF
  exit 0
fi

# ────────────────────────────────────────────────────── platform detection
if [[ "$PLATFORM" == "auto" ]]; then
  if command -v kubectl >/dev/null 2>&1 && kubectl get nodes >/dev/null 2>&1; then
    PLATFORM="k8s"
  elif command -v "$SBCLI_CMD" >/dev/null 2>&1 || [[ -n "$MNODES" ]]; then
    PLATFORM="docker"
  else
    die "cannot tell docker from k8s here. No working kubectl context and no
    $SBCLI_CMD on PATH. Set PLATFORM=docker or PLATFORM=k8s explicitly, and
    run --help to see what each needs."
  fi
fi
say "Platform: $PLATFORM"

# ═══════════════════════════════════════════════════════════════ DOCKER
build_docker() {
  [[ -n "$MNODES" ]] || die "MNODES is required on docker. See --help."
  [[ -n "$STORAGE_PRIVATE_IPS" ]] || die "STORAGE_PRIVATE_IPS is required. See --help."
  [[ -n "$C2_NODES" ]] || die "C2_NODES is required. See --help."

  # Overlap would mean rebuilding a node that is already carrying data for the
  # other cluster, and the failure shows up much later as a confusing outage.
  for b in $C2_NODES; do
    for a in $STORAGE_PRIVATE_IPS $MNODES; do
      [[ "$a" == "$b" ]] && die "node $b is in BOTH C2_NODES and cluster A.
    The two clusters must not share a machine -- that is not a second failure
    domain, and C2_NODES gets rebuilt."
    done
  done

  if [[ "$SKIP_A" == "1" ]]; then
    warn "SKIP_A=1, using the existing cluster A"
  else
    say "1/3  Cluster A  (simplyBlockDeploy/bare-metal/bootstrap-cluster.sh)"
    if [[ ! -f "$DEPLOY_DIR/bare-metal/bootstrap-cluster.sh" ]]; then
      echo "   cloning $DEPLOY_REPO -> $DEPLOY_DIR"
      rm -rf "$DEPLOY_DIR"
      git clone --depth 1 "$DEPLOY_REPO" "$DEPLOY_DIR" >/dev/null 2>&1 \
        || die "could not clone $DEPLOY_REPO"
    fi
    chmod +x "$DEPLOY_DIR/bare-metal/bootstrap-cluster.sh"
    ( cd "$DEPLOY_DIR/bare-metal" \
      && MNODES="$MNODES" STORAGE_PRIVATE_IPS="$STORAGE_PRIVATE_IPS" \
         SSH_USER="$SSH_USER" \
         ./bootstrap-cluster.sh \
           --sbcli-cmd "$SBCLI_CMD" \
           --max-subsys "$MAX_SUBSYS" \
           --data-chunks-per-stripe "$NDCS" \
           --parity-chunks-per-stripe "$NPCS" \
           --journal-partition "$JOURNAL_PARTITION" \
           --ha-jm-count "$HA_JM_COUNT" \
           --ha-type "$HA_TYPE" \
           --data-nics "$DATA_NIC" ) \
      || die "bootstrap-cluster.sh failed. Its own output above says why; this
    script does not second-guess it."
    ok "cluster A built"
  fi

  MGMT="$(echo "$MNODES" | awk '{print $1}')"
  CLUSTER_A="$(ssh -o StrictHostKeyChecking=no "$SSH_USER@$MGMT" \
      "$SBCLI_CMD cluster list" 2>/dev/null \
      | grep -Eo '[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}' | head -1)"
  [[ -n "$CLUSTER_A" ]] || die "cluster A has no id yet -- bring-up did not finish."
  ok "cluster A = $CLUSTER_A"
}

# ══════════════════════════════════════════════════════════════════ K8S
build_k8s() {
  command -v kubectl >/dev/null 2>&1 || die "kubectl not found."
  kubectl get nodes >/dev/null 2>&1 \
    || die "kubectl cannot reach a cluster. Check KUBECONFIG (${KUBECONFIG:-default})."
  [[ -f "$HERE/k8s_cluster_bringup.py" ]] \
    || die "k8s_cluster_bringup.py is missing from $HERE. It drives the
    ClusterDeploymentConfig flow and there is no substitute: applying a
    StorageNodeSet by hand does nothing, because the operator no longer
    reconciles that kind."

  if [[ -z "$WORKER_NODES" ]]; then
    echo "   no WORKER_NODES set; here is what this cluster has:"
    kubectl get nodes -o name | sed 's|node/|     |'
    die "set WORKER_NODES to a CSV of NODE NAMES (not IPs) -- see --help."
  fi

  IFS=',' read -r -a NODES <<< "$WORKER_NODES"
  local need=$(( (NDCS + NPCS) * 2 ))
  [[ ${#NODES[@]} -ge $need ]] || die "need at least $need worker nodes for two
    clusters at ${NDCS}+${NPCS} (each needs a full stripe of its own); got
    ${#NODES[@]}: $WORKER_NODES"

  local half=$(( ${#NODES[@]} / 2 )) a_list="" b_list="" i
  for i in "${!NODES[@]}"; do
    if [[ $i -lt $half ]]; then a_list="${a_list:+$a_list,}${NODES[$i]}"
    else                        b_list="${b_list:+$b_list,}${NODES[$i]}"; fi
  done
  ok "cluster A workers: $a_list"
  ok "cluster B workers: $b_list"

  local MGMT_IFC="${IFC_NAMES%%:*}" DATA_NICS="${IFC_NAMES#*:}"
  _bring_up_ns() {
    local ns="$1" workers="$2" label="$3"
    say "$label  namespace $ns  (k8s_cluster_bringup.py)"
    kubectl create namespace "$ns" >/dev/null 2>&1 || true
    NAMESPACE="$ns" CLUSTER_NAME="simplyblock-cluster" \
    WORKER_NODES="$workers" \
    MGMT_IFC="$MGMT_IFC" DATA_NICS="$DATA_NICS" \
    NDCS="$NDCS" NPCS="$NPCS" DEVICE_MODE="$DEVICE_MODE" \
    ENVIRONMENT="$ENVIRONMENT" FABRIC_TYPE="tcp" \
    MAX_SUBSYS="$MAX_SUBSYS" JM_COUNT="$HA_JM_COUNT" \
    ${SB_IMAGE:+NODE_AGENT_IMAGE="$SB_IMAGE" SPDK_PROXY_IMAGE="$SB_IMAGE"} \
      python3 "$HERE/k8s_cluster_bringup.py" \
      || die "bring-up failed for $ns. Its output above says which phase; the
    usual one is discovery not finding the workers, which means WORKER_NODES
    does not match 'kubectl get nodes'."

    cat <<YAML | kubectl apply -f - >/dev/null
apiVersion: storage.simplyblock.io/v1alpha2
kind: StoragePool
metadata:
  name: simplyblock-pool
  namespace: $ns
spec:
  clusterRef: simplyblock-cluster
YAML
    ok "$ns ready (cluster + pool)"
  }

  [[ "$SKIP_A" == "1" ]] && warn "SKIP_A=1, reusing $NS_A" \
    || _bring_up_ns "$NS_A" "$a_list" "1/3  Cluster A"
  [[ "$SKIP_B" == "1" ]] && warn "SKIP_B=1, reusing $NS_B" \
    || _bring_up_ns "$NS_B" "$b_list" "2/3  Cluster B"

  CLUSTER_A="$(kubectl -n "$NS_A" get storagecluster simplyblock-cluster \
      -o jsonpath='{.status.clusterID}' 2>/dev/null)"
  CLUSTER_B="$(kubectl -n "$NS_B" get storagecluster simplyblock-cluster \
      -o jsonpath='{.status.clusterID}' 2>/dev/null)"
  [[ -n "$CLUSTER_A" && -n "$CLUSTER_B" ]] \
    || die "one of the StorageClusters has no clusterID yet. Check:
      kubectl -n $NS_A get storagecluster -o yaml
      kubectl -n $NS_B get storagecluster -o yaml"
  ok "cluster A = $CLUSTER_A  ($NS_A)"
  ok "cluster B = $CLUSTER_B  ($NS_B)"
}

# ═════════════════════════════════════════════════════════════════ build
case "$PLATFORM" in
  docker) build_docker ;;
  k8s)    build_k8s ;;
  *)      die "PLATFORM must be docker or k8s, got '$PLATFORM'" ;;
esac

# ════════════════════════════════════════════════ cluster B, docker only
if [[ "$PLATFORM" == "docker" && "$SKIP_B" != "1" ]]; then
  say "2/3  Cluster B  (same control plane, nodes: $C2_NODES)"
  warn "replication_manual_setup.sh owns this half -- handing off"
fi

# ═══════════════════════════════════════════════════════════════ wiring
if [[ "$SKIP_WIRING" == "1" ]]; then
  say "Done (SKIP_WIRING=1)"
  cat <<EOF

  Both clusters are up. Wire replication when you are ready:

    CLUSTER_A=$CLUSTER_A ${CLUSTER_B:+CLUSTER_B=$CLUSTER_B} \\
      $HERE/replication_manual_setup.sh ${PLATFORM:+--skip-cluster-b}

EOF
  exit 0
fi

say "3/3  Replication wiring"
if [[ "$PLATFORM" == "k8s" ]]; then
  cat <<EOF

  The clusters are up. The wiring step needs sbctl, which on Kubernetes lives
  in the control-plane pod rather than on a management node:

    kubectl -n $NS_A exec -it deploy/simplyblock-control-plane -- bash
    # then, inside:
    sbctl cluster replication-target-add $CLUSTER_A tgt1 $CLUSTER_B
    sbctl cluster replication-policy-add $CLUSTER_A pol1 tgt1 --interval-min 1
    sbctl volume add replvol1 2G --pool simplyblock-pool --replication-policy pol1

  Or let the automated lane do it -- it adopts exactly these two clusters:

    export CLUSTER2_ID=$CLUSTER_B
    export CLUSTER2_NAMESPACE=$NS_B
    python e2e/e2e.py --testname replication

  And for the csi-addons / Ramen path:
    $HERE/replication_manual_setup.sh --dr

EOF
  exit 0
fi

# The two scripts run in DIFFERENT PLACES, and that is easy to get wrong.
# bootstrap-cluster.sh drives the lab from here, over ssh. The wiring script
# needs sbctl against the live control plane, and sbctl lives on the
# MANAGEMENT NODE -- running it here would hit its own preflight and stop.
# So ship it over and run it there.
if command -v "$SBCLI_CMD" >/dev/null 2>&1 && $SBCLI_CMD cluster list >/dev/null 2>&1; then
  CLUSTER_A="$CLUSTER_A" C2_NODES="$C2_NODES" SBCLI_CMD="$SBCLI_CMD"     exec "$HERE/replication_manual_setup.sh"
fi

echo "   $SBCLI_CMD is not usable here, so the wiring runs on $MGMT"
scp -q -o StrictHostKeyChecking=no "$HERE/replication_manual_setup.sh"     "$SSH_USER@$MGMT:/tmp/replication_manual_setup.sh"   || die "could not copy the wiring script to $MGMT"
ssh -o StrictHostKeyChecking=no "$SSH_USER@$MGMT"     "chmod +x /tmp/replication_manual_setup.sh &&      C2_NODES='$C2_NODES' SBCLI_CMD='$SBCLI_CMD'      IFNAME='$IFNAME' BOOTSTRAP_DATA_NIC='$DATA_NIC'      NDCS='$NDCS' NPCS='$NPCS' HA_TYPE='$HA_TYPE'      BOOTSTRAP_JOURNAL_PARTITION='$JOURNAL_PARTITION'      BOOTSTRAP_HA_JM_COUNT='$HA_JM_COUNT'      BOOTSTRAP_MAX_SUBSYS='$MAX_SUBSYS'      /tmp/replication_manual_setup.sh"
rc=$?
echo
if [[ $rc -eq 0 ]]; then
  ok "lab ready"
  cat <<EOF

  The wiring script now lives on the management node, which is where it has
  to run. Go there for everything else:

    ssh $SSH_USER@$MGMT
    /tmp/replication_manual_setup.sh --verify      # read-only checks
    /tmp/replication_manual_setup.sh --cleanup     # drop the replication,
                                                   # leave the clusters
EOF
fi
exit $rc
