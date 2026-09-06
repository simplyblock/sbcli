#!/usr/bin/env bash
# Produce gdb backtraces for any SPDK core dumps on this storage host.
#
# Streamed to each storage host over ssh by the docker stress/e2e workflows:
#     ssh ... "$SSH_USER@$ip" 'bash -s' < e2e/scripts/analyze_core_dumps_docker.sh -- <RUN_DIR>
#
# Args:
#   $1  RUN_DIR   run directory on the shared NFS mount (required)
#   $2  MAX_MB    max .zst size to copy back, default $CORE_DUMP_MAX_SIZE_MB or 5000
#
# For every SPDK core found it writes three artifacts plus metadata to
#   <RUN_DIR>/core_backtraces/<host>/<core-basename>/
#     core.zst                      the compressed core, as systemd stored it
#     bt.txt                        gdb -ex bt
#     thread_apply_all_bt_full.txt  gdb -ex 'thread apply all bt full'
#     meta.txt                      host, container, image, sizes, exit codes
#
# The decompressed core is a transient working file: it is created inside the
# container, used for gdb, and deleted. It is never copied back.
#
# Why the core is copied into the container: the SPDK container is where the
# matching `bdts` binary and its symbols live, so gdb has to run there. The
# container does bind-mount /var/lib/systemd/coredump (see
# simplyblock_web/api/internal/storage_node/docker.py), but the core is not
# reliably visible through it in practice, so we copy explicitly.
#
# Best-effort by design: never exits non-zero, so it can never fail a CI job.
# Deliberately no `set -e` — one bad core must not abort the remaining ones.
set -uo pipefail

RUN_DIR="${1:-}"
MAX_MB="${2:-${CORE_DUMP_MAX_SIZE_MB:-5000}}"
GDB_TIMEOUT="${GDB_TIMEOUT:-900}"

BDTS="/root/spdk/ultra/build_bdts/bdts"
# Writable layer inside the container. Deliberately NOT /dev/shm or
# /mnt/ramdisk: both are tmpfs, and these hosts are already memory-starved.
WORKDIR="/root/core_analysis"

HOST="$(hostname -s 2>/dev/null || echo unknown)"
log() { echo "[core-bt][${HOST}] $*"; }

if [ -z "${RUN_DIR}" ]; then
    log "ERROR: no RUN_DIR given; nothing to do"
    exit 0
fi

# ---------------------------------------------------------------- find cores
# systemd names cores core.<comm>.<uid>.<boot>.<pid>.<ts>.zst, so <comm> tells
# us whether SPDK crashed or something unrelated did. SPDK also drops its own
# dumps in /etc/simplyblock (see continuous_bulk_lvol_delete.py).
CORES=""
for c in $(find /var/lib/systemd/coredump -maxdepth 1 -name '*.zst' 2>/dev/null); do
    case "$(basename "$c")" in
        *bdts*|*spdk*) CORES="${CORES} ${c}" ;;
        *) log "skipping non-SPDK core: $(basename "$c")" ;;
    esac
done
for c in $(find /etc/simplyblock -maxdepth 1 -name '*core*.zst' 2>/dev/null); do
    CORES="${CORES} ${c}"
done

CORES="$(echo "${CORES}" | tr ' ' '\n' | sed '/^$/d' | sort -u)"

if [ -z "${CORES}" ]; then
    log "no SPDK core dumps found"
    exit 0
fi

log "found $(echo "${CORES}" | wc -l) SPDK core dump(s)"

# ------------------------------------------------------------ pick container
# Same host means same SPDK image, which is what makes the symbols resolve.
CONTAINER="$(sudo docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^spdk_[0-9]+$' | head -1)"
if [ -z "${CONTAINER}" ]; then
    log "ERROR: no running spdk_<port> container on this host; cannot symbolicate"
    log "       cores left in place: ${CORES}"
    exit 0
fi
IMAGE="$(sudo docker inspect --format '{{.Config.Image}}' "${CONTAINER}" 2>/dev/null || echo unknown)"
log "using container ${CONTAINER} (image ${IMAGE})"

sudo docker exec "${CONTAINER}" mkdir -p "${WORKDIR}" >/dev/null 2>&1

HAVE_ZSTD_IN_CONTAINER=no
if sudo docker exec "${CONTAINER}" bash -lc 'command -v zstd' >/dev/null 2>&1; then
    HAVE_ZSTD_IN_CONTAINER=yes
fi
log "zstd inside container: ${HAVE_ZSTD_IN_CONTAINER}"

TOTAL_COPIED_MB=0

for CORE in ${CORES}; do
    BASE="$(basename "${CORE}")"
    # Strip characters that are awkward in paths; keep it recognisable.
    SAFE="$(echo "${BASE}" | tr ':' '_')"
    OUT="${RUN_DIR}/core_backtraces/${HOST}/${SAFE}"
    ZST_MB=$(( $(stat -c %s "${CORE}" 2>/dev/null || echo 0) / 1024 / 1024 ))

    log "--- ${BASE} (${ZST_MB} MB compressed) ---"
    sudo mkdir -p "${OUT}" 2>/dev/null
    sudo chmod -R 777 "${OUT}" 2>/dev/null

    # --- space check: refuse rather than fill the container's disk ----------
    # The expanded size is unknown until we decompress. Assume 5x the
    # compressed size, which is conservative for SPDK cores (mostly zeroed
    # hugepage mappings, so they compress well).
    NEED_MB=$(( ZST_MB * 5 ))
    AVAIL_MB=$(sudo docker exec "${CONTAINER}" bash -lc \
        "df -Pm '${WORKDIR}' 2>/dev/null | awk 'NR==2 {print \$4}'" 2>/dev/null || echo 0)
    AVAIL_MB="${AVAIL_MB:-0}"
    if [ "${AVAIL_MB}" -gt 0 ] 2>/dev/null && [ "${NEED_MB}" -gt "${AVAIL_MB}" ] 2>/dev/null; then
        log "SKIP: need ~${NEED_MB} MB in container, only ${AVAIL_MB} MB free"
        {
            echo "host=${HOST}"
            echo "core=${BASE}"
            echo "status=skipped_no_space"
            echo "zst_size_mb=${ZST_MB}"
            echo "estimated_need_mb=${NEED_MB}"
            echo "container_avail_mb=${AVAIL_MB}"
        } | sudo tee "${OUT}/meta.txt" >/dev/null
        continue
    fi

    # --- copy the core into the container ----------------------------------
    if ! sudo docker cp "${CORE}" "${CONTAINER}:${WORKDIR}/${SAFE}" >/dev/null 2>&1; then
        log "ERROR: docker cp of ${BASE} into ${CONTAINER} failed"
        echo "host=${HOST}
core=${BASE}
status=copy_in_failed" | sudo tee "${OUT}/meta.txt" >/dev/null
        continue
    fi

    # --- decompress (in container, else on host then copy the expanded file) -
    EXPANDED="${WORKDIR}/${SAFE%.zst}.core"
    ZSTD_PATH=container
    if [ "${HAVE_ZSTD_IN_CONTAINER}" = yes ]; then
        sudo docker exec "${CONTAINER}" bash -lc \
            "zstd -d -f '${WORKDIR}/${SAFE}' -o '${EXPANDED}'" >/dev/null 2>&1
    else
        ZSTD_PATH=host
        HOST_TMP="/tmp/core_bt_$$_${SAFE%.zst}.core"
        if command -v zstd >/dev/null 2>&1; then
            sudo zstd -d -f "${CORE}" -o "${HOST_TMP}" >/dev/null 2>&1 \
                && sudo docker cp "${HOST_TMP}" "${CONTAINER}:${EXPANDED}" >/dev/null 2>&1
            sudo rm -f "${HOST_TMP}"
        else
            ZSTD_PATH=none
            log "ERROR: zstd available neither in container nor on host"
        fi
    fi

    if ! sudo docker exec "${CONTAINER}" test -s "${EXPANDED}" 2>/dev/null; then
        log "ERROR: decompression produced nothing for ${BASE}"
        echo "host=${HOST}
core=${BASE}
status=decompress_failed
zstd_path=${ZSTD_PATH}" | sudo tee "${OUT}/meta.txt" >/dev/null
        sudo docker exec "${CONTAINER}" rm -f "${WORKDIR}/${SAFE}" "${EXPANDED}" >/dev/null 2>&1
        continue
    fi

    # --- gdb: two separate runs so one hanging does not lose the other ------
    # gdb on a multi-GB core can hang, hence timeout + explicit exit code
    # (124 = timeout, 137 = OOM-kill), matching the idiom in ssh_utils.py.
    run_gdb() {
        local gdb_cmd="$1" dest="$2"
        local raw rc
        raw="$(sudo timeout "${GDB_TIMEOUT}" docker exec "${CONTAINER}" bash -lc \
            "gdb -batch -ex ${gdb_cmd} '${BDTS}' '${EXPANDED}' 2>&1" ; echo "EXIT_CODE=$?")"
        rc="$(printf '%s\n' "${raw}" | tail -1 | sed 's/EXIT_CODE=//')"
        # Keep the marker out of the artifact; it is recorded in meta.txt.
        printf '%s\n' "${raw}" | sed '$d' | sudo tee "${dest}" >/dev/null
        printf '%s' "${rc}"
    }

    BT_RC="$(run_gdb "bt" "${OUT}/bt.txt")"
    TA_RC="$(run_gdb "'thread apply all bt full'" "${OUT}/thread_apply_all_bt_full.txt")"
    log "gdb bt exit=${BT_RC}, thread-apply-all exit=${TA_RC}"

    # --- keep the compressed core, subject to the cap ----------------------
    CORE_KEPT=no
    if [ "${ZST_MB}" -le "${MAX_MB}" ] 2>/dev/null; then
        if sudo cp "${CORE}" "${OUT}/core.zst" 2>/dev/null; then
            CORE_KEPT=yes
            TOTAL_COPIED_MB=$(( TOTAL_COPIED_MB + ZST_MB ))
        fi
    else
        log "core.zst not copied: ${ZST_MB} MB exceeds cap ${MAX_MB} MB"
    fi

    {
        echo "host=${HOST}"
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
    } | sudo tee "${OUT}/meta.txt" >/dev/null

    # --- clean up inside the container -------------------------------------
    sudo docker exec "${CONTAINER}" rm -f "${WORKDIR}/${SAFE}" "${EXPANDED}" >/dev/null 2>&1
    log "wrote ${OUT}"
done

sudo docker exec "${CONTAINER}" rmdir "${WORKDIR}" >/dev/null 2>&1
log "done; ${TOTAL_COPIED_MB} MB of core.zst copied to ${RUN_DIR}/core_backtraces/${HOST}"
exit 0
