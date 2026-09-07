#!/usr/bin/env bash
# Produce gdb backtraces for any SPDK core dumps on the K8s storage nodes.
#
# Runs on the CI runner and drives everything through kubectl, following the
# shape of e2e/scripts/fetch_distrib_dumps.sh:
#     bash e2e/scripts/analyze_core_dumps_k8s.sh <OUT_DIR> [NAMESPACE]
#
# Args:
#   $1  OUT_DIR    run directory (required)
#   $2  NAMESPACE  default "simplyblock"
#
# For every SPDK core found it writes three artifacts plus metadata to
#   <OUT_DIR>/core_backtraces/<node>/<core-basename>/
#     core.zst                      the compressed core, as systemd stored it
#     bt.txt                        gdb -ex bt
#     thread_apply_all_bt_full.txt  gdb -ex 'thread apply all bt full'
#     meta.txt                      node, pod, image, sizes, truncation, exit codes
#
# ---------------------------------------------------------------------------
# WHERE THE CORE ACTUALLY LIVES  (fixed 2026-09-07)
#
# The host's core_pattern pipes to systemd-coredump, so cores land on the NODE
# at /var/lib/systemd/coredump/, never inside the SPDK container.
#
# This script used to read "/proc/1/root/var/lib/systemd/coredump" from the
# SPDK pod, on the assumption that /proc/1/root is the host root. It is not:
# the SPDK pod does NOT set hostPID, so PID 1 in its namespace is its own
# entrypoint ("sudo -E /root/scripts/run_distr_with_ssd.sh ..."), and
# /proc/1/root therefore resolves to the CONTAINER's filesystem. Every run
# silently reported "no SPDK core dumps" while real cores sat on the hosts.
# (Confirmed on run k8s_native_resilient_failover-20260906-112437, where four
# cores existed on the nodes and none were collected.)
#
# So discovery and retrieval now go through a node debug pod (host access),
# and gdb still runs in the SPDK pod, which is the only place that has both
# gdb and the matching /root/spdk/ultra/build_bdts/bdts binary.
#
# The handover between the two needs no network copy: the SPDK pod already
# bind-mounts a host directory (/var/crash, or /var/simplyblock at
# /etc/simplyblock). Staging the core into that host directory from the debug
# pod makes it appear inside the SPDK pod immediately. The mount pair is
# discovered from the live pod spec rather than hardcoded.
#
# TRUNCATED CORES ARE USELESS. The hosts ship
# /etc/systemd/coredump.conf with ProcessSizeMax=1G and ExternalSizeMax=1G,
# while SPDK runs with --mem-size 14670MB plus ~17GB of hugepages, so cores are
# cut at exactly 1GiB and systemd marks them "truncated". A truncated SPDK core
# yields NO usable backtrace: gdb enumerates the threads but every stack read
# fails ("Backtrace stopped: Cannot access memory at address ..."), including
# the aborting reactor thread. This script therefore records the truncation
# state in meta.txt and logs it loudly, so a useless backtrace is not mistaken
# for a clean one.
# ---------------------------------------------------------------------------
#
# IMPORTANT: systemd core filenames contain ':', which `kubectl cp` parses as a
# pod:path separator. Every copy therefore goes via a colon-free name, the same
# workaround already used in k8s_utils.dump_lvstore_k8s.
#
# Best-effort by design: never exits non-zero. No `set -e` on purpose so one
# bad core does not abort the rest.
set -uo pipefail

OUT_DIR="${1:-}"
NS="${2:-simplyblock}"
MAX_MB="${CORE_DUMP_MAX_SIZE_MB:-5000}"
GDB_TIMEOUT="${GDB_TIMEOUT:-900}"
DEBUG_TIMEOUT="${CORE_DEBUG_POD_TIMEOUT:-240}"

BDTS="/root/spdk/ultra/build_bdts/bdts"
CONTAINER="spdk-container"
HOST_COREDIR="/var/lib/systemd/coredump"
# Host dirs we are willing to stage a core into, best first. Each must be a
# hostPath mount of the SPDK pod for the handover to work.
STAGE_CANDIDATES="/var/crash /var/simplyblock"

log() { echo "[core-bt][k8s] $*"; }

if [ -z "${OUT_DIR}" ]; then
    log "ERROR: no OUT_DIR given; nothing to do"
    exit 0
fi

# --- host command runner: prefer `oc debug node/`, fall back to kubectl ------
# Both mount the host root at /host. `oc debug` handles OpenShift SCC for us;
# `kubectl debug --profile=sysadmin` is the vanilla-k8s equivalent and needs an
# image, for which the SPDK image is always pullable on these clusters.
HOST_RUNNER=""
host_run() {
    # host_run <node> <image> <sh-script>
    local node="$1" image="$2" script="$3"
    case "${HOST_RUNNER}" in
        oc)
            timeout "${DEBUG_TIMEOUT}" oc debug "node/${node}" --quiet -- \
                chroot /host sh -c "${script}" 2>&1 \
                | grep -avE '^(Starting pod|Removing debug pod|Temporary namespace|To use host)'
            ;;
        kubectl)
            timeout "${DEBUG_TIMEOUT}" kubectl debug "node/${node}" -q \
                --profile=sysadmin --image="${image}" -- \
                chroot /host sh -c "${script}" 2>&1 \
                | grep -avE '^(Creating debugging pod|Warning:)'
            ;;
        *)
            return 1
            ;;
    esac
}

if command -v oc >/dev/null 2>&1; then
    HOST_RUNNER=oc
elif kubectl debug --help >/dev/null 2>&1; then
    HOST_RUNNER=kubectl
else
    log "ERROR: neither 'oc debug' nor 'kubectl debug' available; cannot reach host coredumps"
    exit 0
fi
log "host access via: ${HOST_RUNNER} debug node/"

PODS="$(kubectl get pods -n "${NS}" --no-headers -o custom-columns=':metadata.name' 2>/dev/null \
        | grep 'snode-spdk' || true)"
if [ -z "${PODS}" ]; then
    log "no snode-spdk pods found in namespace ${NS}"
    exit 0
fi

TOTAL_COPIED_MB=0
TRUNCATED_SEEN=0

for POD in ${PODS}; do
    NODE="$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo '')"
    IMAGE="$(kubectl get pod "${POD}" -n "${NS}" \
        -o jsonpath="{.spec.containers[?(@.name=='${CONTAINER}')].image}" 2>/dev/null || echo unknown)"
    if [ -z "${NODE}" ]; then
        log "${POD}: cannot resolve nodeName, skipping"
        continue
    fi

    # --- find a hostPath mount we can hand the core over through ------------
    # Emits "<hostPath> <mountPath>" for the first candidate this pod mounts.
    STAGE_PAIR="$(kubectl get pod "${POD}" -n "${NS}" -o json 2>/dev/null | python3 -c '
import json,sys
try:
    p = json.load(sys.stdin)
except Exception:
    sys.exit(0)
cands = sys.argv[1].split()
vols = {v["name"]: v for v in p["spec"].get("volumes", [])}
for c in p["spec"].get("containers", []):
    if c.get("name") != sys.argv[2]:
        continue
    found = {}
    for m in c.get("volumeMounts", []):
        hp = vols.get(m["name"], {}).get("hostPath", {}).get("path")
        if hp:
            found[hp] = m["mountPath"]
    for want in cands:
        if want in found:
            print(want, found[want])
            break
' "${STAGE_CANDIDATES}" "${CONTAINER}" 2>/dev/null)"

    STAGE_HOST="$(echo "${STAGE_PAIR}" | awk '{print $1}')"
    STAGE_POD="$(echo "${STAGE_PAIR}" | awk '{print $2}')"
    if [ -z "${STAGE_HOST}" ] || [ -z "${STAGE_POD}" ]; then
        log "${POD} (${NODE}): no usable hostPath mount (${STAGE_CANDIDATES}); cannot hand a core to the pod"
        continue
    fi

    # --- list host cores + their truncation state ---------------------------
    # coredumpctl gives the COREFILE column (present / truncated / missing);
    # ls gives the on-disk names. Both come from one debug-pod invocation.
    HOST_OUT="$(host_run "${NODE}" "${IMAGE}" "
        echo '###CORES###'
        ls -1 ${HOST_COREDIR}/*.zst 2>/dev/null
        echo '###CTL###'
        coredumpctl list --no-pager 2>/dev/null | tail -40
    ")"

    CORES="$(printf '%s\n' "${HOST_OUT}" \
        | sed -n '/###CORES###/,/###CTL###/p' | grep -E '^/.*\.zst$' \
        | grep -Ei 'bdts|spdk|reactor' || true)"
    CTL="$(printf '%s\n' "${HOST_OUT}" | sed -n '/###CTL###/,$p' | sed '1d')"

    if [ -z "${CORES}" ]; then
        log "${POD} (${NODE}): no SPDK core dumps on host ${HOST_COREDIR}"
        continue
    fi
    log "${POD} (${NODE}): $(echo "${CORES}" | wc -l) SPDK core dump(s) on host, image ${IMAGE}"

    for CORE in ${CORES}; do
        BASE="$(basename "${CORE}")"
        SAFE="$(echo "${BASE}" | tr ':' '_')"
        OUT="${OUT_DIR}/core_backtraces/${NODE}/${SAFE}"
        mkdir -p "${OUT}" 2>/dev/null

        log "--- ${NODE}: ${BASE} ---"

        # systemd records the PID in the filename: core.<comm>.<uid>.<boot>.<pid>.<ts>.zst
        CORE_PID="$(echo "${BASE}" | awk -F. '{print $(NF-2)}')"
        TRUNC="unknown"
        if [ -n "${CORE_PID}" ]; then
            case "$(printf '%s\n' "${CTL}" | grep -E "[[:space:]]${CORE_PID}[[:space:]]" | head -1)" in
                *truncated*) TRUNC=truncated ;;
                *present*)   TRUNC=present ;;
                *missing*)   TRUNC=missing ;;
            esac
        fi
        if [ "${TRUNC}" = "truncated" ]; then
            TRUNCATED_SEEN=$(( TRUNCATED_SEEN + 1 ))
            log "WARNING: ${BASE} is TRUNCATED (host ProcessSizeMax/ExternalSizeMax cap)."
            log "WARNING: a truncated SPDK core yields no usable backtrace; treat bt.txt as empty evidence."
        fi

        # --- stage host core -> hostPath dir the SPDK pod already mounts ----
        STAGED_HOST="${STAGE_HOST}/${SAFE}"
        STAGED_POD="${STAGE_POD}/${SAFE}"
        EXPANDED_POD="${STAGE_POD}/${SAFE%.zst}.core"

        STAGE_OUT="$(host_run "${NODE}" "${IMAGE}" "
            cp -f '${CORE}' '${STAGED_HOST}' || { echo COPY_FAILED; exit 0; }
            stat -c %s '${STAGED_HOST}'
        ")"
        ZST_BYTES="$(printf '%s\n' "${STAGE_OUT}" | grep -E '^[0-9]+$' | tail -1)"
        if printf '%s' "${STAGE_OUT}" | grep -q COPY_FAILED || [ -z "${ZST_BYTES}" ]; then
            log "ERROR: could not stage ${BASE} into ${STAGE_HOST} on ${NODE}"
            printf 'node=%s\npod=%s\ncore=%s\ncore_path=%s\ntruncated=%s\nstatus=stage_failed\n' \
                "${NODE}" "${POD}" "${BASE}" "${CORE}" "${TRUNC}" > "${OUT}/meta.txt"
            continue
        fi
        ZST_MB=$(( ZST_BYTES / 1024 / 1024 ))

        # --- decompress inside the SPDK pod --------------------------------
        ZSTD_PATH=pod
        if kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                sh -c "command -v zstd" >/dev/null 2>&1; then
            kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                sh -c "zstd -d -f '${STAGED_POD}' -o '${EXPANDED_POD}'" >/dev/null 2>&1
        else
            ZSTD_PATH=none
            log "ERROR: zstd not available inside ${POD}"
        fi

        if ! kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                test -s "${EXPANDED_POD}" >/dev/null 2>&1; then
            log "ERROR: decompression produced nothing for ${BASE}"
            printf 'node=%s\npod=%s\ncore=%s\ntruncated=%s\nstatus=decompress_failed\nzstd_path=%s\n' \
                "${NODE}" "${POD}" "${BASE}" "${TRUNC}" "${ZSTD_PATH}" > "${OUT}/meta.txt"
            host_run "${NODE}" "${IMAGE}" "rm -f '${STAGED_HOST}' '${STAGE_HOST}/${SAFE%.zst}.core'" >/dev/null 2>&1
            continue
        fi
        RAW_BYTES="$(kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
            sh -c "stat -c %s '${EXPANDED_POD}' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\r')"

        # --- gdb: two separate runs so one hanging does not lose the other --
        run_gdb() {
            local gdb_cmd="$1" dest="$2" raw rc
            raw="$(timeout "${GDB_TIMEOUT}" kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                sh -c "gdb -batch -ex ${gdb_cmd} '${BDTS}' '${EXPANDED_POD}' 2>&1" ; echo "EXIT_CODE=$?")"
            rc="$(printf '%s\n' "${raw}" | tail -1 | sed 's/EXIT_CODE=//')"
            # The hugepage mapping warnings are one per mapping (thousands of
            # them) and drown the actual stack; they are expected for any SPDK
            # core. Each is followed by a blank line, so squeeze runs of blanks
            # too or the file is still mostly empty lines.
            printf '%s\n' "${raw}" | sed '$d' \
                | grep -avE "^warning: Can't open file /dev/hugepages" \
                | cat -s > "${dest}"
            printf '%s' "${rc}"
        }

        BT_RC="$(run_gdb "bt" "${OUT}/bt.txt")"
        TA_RC="$(run_gdb "'thread apply all bt full'" "${OUT}/thread_apply_all_bt_full.txt")"
        log "gdb bt exit=${BT_RC}, thread-apply-all exit=${TA_RC}"

        # A core whose stacks are unreadable produces this on every thread.
        if grep -q "Backtrace stopped: Cannot access memory" "${OUT}/bt.txt" 2>/dev/null; then
            log "WARNING: bt.txt has no readable stack (truncated=${TRUNC}); backtrace is not usable"
        fi

        # --- copy the compressed core back, subject to the cap -------------
        CORE_KEPT=no
        if [ "${ZST_MB}" -le "${MAX_MB}" ] 2>/dev/null; then
            if kubectl cp -n "${NS}" "${POD}:${STAGED_POD}" -c "${CONTAINER}" \
                    "${OUT}/core.zst" >/dev/null 2>&1; then
                CORE_KEPT=yes
                TOTAL_COPIED_MB=$(( TOTAL_COPIED_MB + ZST_MB ))
            else
                log "WARN: kubectl cp of ${BASE} failed"
            fi
        else
            log "core.zst not copied: ${ZST_MB} MB exceeds cap ${MAX_MB} MB"
        fi

        {
            echo "node=${NODE}"
            echo "pod=${POD}"
            echo "core=${BASE}"
            echo "core_path=${CORE}"
            echo "status=ok"
            echo "truncated=${TRUNC}"
            echo "container=${CONTAINER}"
            echo "spdk_image=${IMAGE}"
            echo "binary=${BDTS}"
            echo "stage_host_dir=${STAGE_HOST}"
            echo "stage_pod_dir=${STAGE_POD}"
            echo "zst_size_mb=${ZST_MB}"
            echo "raw_size_bytes=${RAW_BYTES}"
            echo "zstd_path=${ZSTD_PATH}"
            echo "gdb_bt_exit=${BT_RC}"
            echo "gdb_thread_apply_all_exit=${TA_RC}"
            echo "gdb_timeout_sec=${GDB_TIMEOUT}"
            echo "core_zst_kept=${CORE_KEPT}"
            echo "max_size_mb=${MAX_MB}"
        } > "${OUT}/meta.txt"

        # Clean up both staged files. They live on the host, so one removal in
        # either view is enough; do it host-side since that is authoritative.
        host_run "${NODE}" "${IMAGE}" \
            "rm -f '${STAGED_HOST}' '${STAGE_HOST}/${SAFE%.zst}.core'" >/dev/null 2>&1
        log "wrote ${OUT}"
    done
done

log "done; ${TOTAL_COPIED_MB} MB of core.zst copied to ${OUT_DIR}/core_backtraces"
if [ "${TRUNCATED_SEEN}" -gt 0 ]; then
    log "NOTE: ${TRUNCATED_SEEN} core(s) were truncated by the host coredump size cap."
    log "NOTE: raise ProcessSizeMax/ExternalSizeMax in /etc/systemd/coredump.conf above the"
    log "NOTE: SPDK footprint (--mem-size plus hugepages) or those backtraces stay unusable."
fi
exit 0
