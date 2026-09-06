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
#   <OUT_DIR>/core_backtraces/<pod>/<core-basename>/
#     core.zst                      the compressed core, as systemd stored it
#     bt.txt                        gdb -ex bt
#     thread_apply_all_bt_full.txt  gdb -ex 'thread apply all bt full'
#     meta.txt                      pod, node, image, sizes, exit codes
#
# The decompressed core is a transient working file inside the pod; it is used
# for gdb and deleted, never copied back.
#
# Host cores live at /var/lib/systemd/coredump on the node and are reachable
# from the privileged SPDK pod via /proc/1/root (same access path as
# k8s_utils._collect_host_core_dumps_via_spdk).
#
# IMPORTANT: systemd core filenames contain ':', which `kubectl cp` parses as a
# pod:path separator. Every copy therefore goes via a colon-free /tmp name, the
# same workaround already used in k8s_utils.dump_lvstore_k8s.
#
# Best-effort by design: never exits non-zero. No `set -e` on purpose so one
# bad core does not abort the rest.
set -uo pipefail

OUT_DIR="${1:-}"
NS="${2:-simplyblock}"
MAX_MB="${CORE_DUMP_MAX_SIZE_MB:-5000}"
GDB_TIMEOUT="${GDB_TIMEOUT:-900}"

BDTS="/root/spdk/ultra/build_bdts/bdts"
CONTAINER="spdk-container"
HOST_COREDIR="/proc/1/root/var/lib/systemd/coredump"
WORKDIR="/root/core_analysis"

log() { echo "[core-bt][k8s] $*"; }

if [ -z "${OUT_DIR}" ]; then
    log "ERROR: no OUT_DIR given; nothing to do"
    exit 0
fi

PODS="$(kubectl get pods -n "${NS}" --no-headers -o custom-columns=':metadata.name' 2>/dev/null \
        | grep 'snode-spdk' || true)"
if [ -z "${PODS}" ]; then
    log "no snode-spdk pods found in namespace ${NS}"
    exit 0
fi

TOTAL_COPIED_MB=0

for POD in ${PODS}; do
    NODE="$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo unknown)"
    IMAGE="$(kubectl get pod "${POD}" -n "${NS}" \
        -o jsonpath="{.spec.containers[?(@.name=='${CONTAINER}')].image}" 2>/dev/null || echo unknown)"

    # SPDK cores only: systemd names them core.<comm>.<uid>.<boot>.<pid>.<ts>.zst
    CORES="$(kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
        bash -lc "ls -1 ${HOST_COREDIR}/*.zst 2>/dev/null" 2>/dev/null \
        | grep -Ei 'bdts|spdk' || true)"

    if [ -z "${CORES}" ]; then
        log "${POD} (${NODE}): no SPDK core dumps"
        continue
    fi
    log "${POD} (${NODE}): $(echo "${CORES}" | wc -l) SPDK core dump(s), image ${IMAGE}"

    kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
        mkdir -p "${WORKDIR}" >/dev/null 2>&1

    for CORE in ${CORES}; do
        BASE="$(basename "${CORE}")"
        SAFE="$(echo "${BASE}" | tr ':' '_')"
        OUT="${OUT_DIR}/core_backtraces/${POD}/${SAFE}"
        mkdir -p "${OUT}" 2>/dev/null

        log "--- ${POD}: ${BASE} ---"

        # Stage inside the pod under a colon-free name. This also moves the
        # core off /proc/1/root onto the pod's own writable layer, which is
        # what gdb will read from.
        STAGED="${WORKDIR}/${SAFE}"
        EXPANDED="${WORKDIR}/${SAFE%.zst}.core"

        ZST_BYTES="$(kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
            bash -lc "stat -c %s '${CORE}' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\r')"
        ZST_MB=$(( ${ZST_BYTES:-0} / 1024 / 1024 ))

        if ! kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                bash -lc "cp '${CORE}' '${STAGED}'" >/dev/null 2>&1; then
            log "ERROR: could not stage ${BASE} inside ${POD}"
            printf 'pod=%s\nnode=%s\ncore=%s\nstatus=stage_failed\n' \
                "${POD}" "${NODE}" "${BASE}" > "${OUT}/meta.txt"
            continue
        fi

        # --- decompress inside the pod -------------------------------------
        ZSTD_PATH=pod
        if kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                bash -lc "command -v zstd" >/dev/null 2>&1; then
            kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                bash -lc "zstd -d -f '${STAGED}' -o '${EXPANDED}'" >/dev/null 2>&1
        else
            ZSTD_PATH=none
            log "ERROR: zstd not available inside ${POD}"
        fi

        if ! kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                test -s "${EXPANDED}" >/dev/null 2>&1; then
            log "ERROR: decompression produced nothing for ${BASE}"
            printf 'pod=%s\nnode=%s\ncore=%s\nstatus=decompress_failed\nzstd_path=%s\n' \
                "${POD}" "${NODE}" "${BASE}" "${ZSTD_PATH}" > "${OUT}/meta.txt"
            kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                rm -f "${STAGED}" "${EXPANDED}" >/dev/null 2>&1
            continue
        fi

        # --- gdb: two separate runs so one hanging does not lose the other --
        run_gdb() {
            local gdb_cmd="$1" dest="$2" raw rc
            raw="$(timeout "${GDB_TIMEOUT}" kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
                bash -lc "gdb -batch -ex ${gdb_cmd} '${BDTS}' '${EXPANDED}' 2>&1" ; echo "EXIT_CODE=$?")"
            rc="$(printf '%s\n' "${raw}" | tail -1 | sed 's/EXIT_CODE=//')"
            printf '%s\n' "${raw}" | sed '$d' > "${dest}"
            printf '%s' "${rc}"
        }

        BT_RC="$(run_gdb "bt" "${OUT}/bt.txt")"
        TA_RC="$(run_gdb "'thread apply all bt full'" "${OUT}/thread_apply_all_bt_full.txt")"
        log "gdb bt exit=${BT_RC}, thread-apply-all exit=${TA_RC}"

        # --- copy the compressed core back, subject to the cap -------------
        # kubectl cp the colon-free staged copy, never the original path.
        CORE_KEPT=no
        if [ "${ZST_MB}" -le "${MAX_MB}" ] 2>/dev/null; then
            if kubectl cp -n "${NS}" "${POD}:${STAGED}" -c "${CONTAINER}" \
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
            echo "pod=${POD}"
            echo "node=${NODE}"
            echo "core=${BASE}"
            echo "core_path=${CORE}"
            echo "status=ok"
            echo "container=${CONTAINER}"
            echo "spdk_image=${IMAGE}"
            echo "binary=${BDTS}"
            echo "zst_size_mb=${ZST_MB}"
            echo "zstd_path=${ZSTD_PATH}"
            echo "gdb_bt_exit=${BT_RC}"
            echo "gdb_thread_apply_all_exit=${TA_RC}"
            echo "gdb_timeout_sec=${GDB_TIMEOUT}"
            echo "core_zst_kept=${CORE_KEPT}"
            echo "max_size_mb=${MAX_MB}"
        } > "${OUT}/meta.txt"

        kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
            rm -f "${STAGED}" "${EXPANDED}" >/dev/null 2>&1
        log "wrote ${OUT}"
    done

    kubectl exec "${POD}" -n "${NS}" -c "${CONTAINER}" -- \
        rmdir "${WORKDIR}" >/dev/null 2>&1
done

log "done; ${TOTAL_COPIED_MB} MB of core.zst copied to ${OUT_DIR}/core_backtraces"
exit 0
