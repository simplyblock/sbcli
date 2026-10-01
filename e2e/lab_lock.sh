#!/usr/bin/env bash
#
# Reserve lab hosts for one GitHub Actions run.
#
# The lock is a single file, /var/lib/sb-ci/lock, on each host the run will
# touch. It lives on the host because the host is the resource: any workflow,
# on any runner, that can reach the host sees the same lock. It is persistent
# because the workflow reboots the hosts it holds.
#
# This script runs on the runner, never on the locked hosts -- the hosts are
# what the workflow wipes, so nothing may be installed there. The remote half
# below needs only coreutils, and arrives over ssh on stdin.
#
# Held means held. There is no lease, no expiry and no liveness probe: a lock
# is released by the run that took it, and anything left behind by a run that
# died is cleared by an operator with `break`.
#
# See .github/actions/lab-lock/README.md for the operator runbook.
set -euo pipefail

LOCK_PATH="${LOCK_PATH:-/var/lib/sb-ci/lock}"
SSH_USER="${SSH_USER:-root}"

usage() {
    cat >&2 <<'EOF'
usage: lab_lock.sh {acquire|verify|release|status|break} --hosts "h1 h2" [options]

  --hosts "..."        space- or comma-separated; deduped and sorted
  --job NAME           recorded in the lock, for operators reading it
  --label NAME         recorded in the lock (RUN_LABEL)
  --grace-seconds N    acquire: wait out a cancelled run still releasing (default 120)
  --force              required by `break`
  --local [--lock-dir D]  lock in a local directory instead of over ssh
EOF
    exit 2
}

MODE="${1-}"; shift || usage
HOSTS=""; JOB=""; LABEL=""; GRACE=120; FORCE=0; LOCAL=0; LOCK_DIR="/tmp/sb-ci-locks"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hosts)         HOSTS="$2"; shift 2 ;;
        --job)           JOB="$2"; shift 2 ;;
        --label)         LABEL="$2"; shift 2 ;;
        --grace-seconds) GRACE="$2"; shift 2 ;;
        --lock-dir)      LOCK_DIR="$2"; shift 2 ;;
        --force)         FORCE=1; shift ;;
        --local)         LOCAL=1; shift ;;
        *) usage ;;
    esac
done

# Callers concatenate the workflow's host variables, so duplicates and blanks
# are expected: MNODES is normally also BASTION_IP.
mapfile -t TARGETS < <(tr ' ,' '\n\n' <<<"${HOSTS}" | sed '/^$/d' | sort -u)
[[ ${#TARGETS[@]} -gt 0 ]] || { echo "ERROR: --hosts resolved to nothing" >&2; exit 2; }
[[ "${MODE}" != "break" || "${FORCE}" -eq 1 ]] || {
    echo "ERROR: break removes other runs' locks; pass --force" >&2; exit 2
}

REPO="${GITHUB_REPOSITORY:-local/local}"
RUN_ID="${GITHUB_RUN_ID:-$$}"
# Ownership is keyed on run_id alone, not run_id+run_attempt: a re-run keeps the
# id, so an attempt-scoped owner would deadlock a re-run against the lock its
# own previous attempt left behind. One string, so the remote side can settle
# ownership with a single fixed-string grep instead of parsing JSON.
OWNER="${REPO}#${RUN_ID}"

# Workflow and label names are free text, so they have to survive landing in a
# JSON string -- an operator reads this file with jq.
j() { sed 's/\\/\\\\/g; s/"/\\"/g' <<<"${1-}"; }

payload() {
    cat <<EOF
{
  "owner": "${OWNER}",
  "run_attempt": ${GITHUB_RUN_ATTEMPT:-1},
  "workflow": "$(j "${GITHUB_WORKFLOW:-local}")",
  "job": "$(j "${JOB}")",
  "label": "$(j "${LABEL}")",
  "run_url": "${GITHUB_SERVER_URL:-https://github.com}/${REPO}/actions/runs/${RUN_ID}",
  "hosts": "${TARGETS[*]}",
  "acquired_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
}

# The remote half. Static: every parameter arrives as an environment
# assignment and the payload as base64, so nothing here needs escaping through
# two shells. Exit codes: 0 ours, 1 held by another (lock echoed), 2 absent.
read -r -d '' REMOTE <<'REMOTE_EOF' || true
set -u
mine() { [ -e "$SB_LOCK" ] && grep -qF "$SB_OWNER" "$SB_LOCK"; }
case "$SB_OP" in
  create)
    mkdir -p "$(dirname "$SB_LOCK")"
    tmp="$(mktemp "$(dirname "$SB_LOCK")/.lock.XXXXXX")"
    printf '%s' "$SB_PAYLOAD" | base64 -d > "$tmp"
    # ln fails if the name exists, and publishes name and content in one step,
    # so exactly one run wins and no reader sees a half-written lock.
    if ln "$tmp" "$SB_LOCK" 2>/dev/null; then rm -f "$tmp"; exit 0; fi
    rm -f "$tmp"
    mine && exit 0
    cat "$SB_LOCK" 2>/dev/null; exit 1 ;;
  check)
    [ -e "$SB_LOCK" ] || exit 2
    mine && exit 0
    cat "$SB_LOCK" 2>/dev/null; exit 1 ;;
  remove)
    [ -e "$SB_LOCK" ] || exit 2
    mine || { cat "$SB_LOCK" 2>/dev/null; exit 1; }
    rm -f "$SB_LOCK"; exit 0 ;;
  force-remove)
    rm -f "$SB_LOCK"; exit 0 ;;
esac
REMOTE_EOF

# Runs the remote half against one host. Stdout is the holder's lock on a
# conflict, empty otherwise; the exit code carries the answer.
remote() {
    local host="$1" op="$2" payload_b64="${3-}" env_prefix
    env_prefix="$(printf 'SB_OP=%q SB_OWNER=%q SB_PAYLOAD=%q ' \
        "${op}" "${OWNER}" "${payload_b64}")"

    if [[ "${LOCAL}" -eq 1 ]]; then
        mkdir -p "${LOCK_DIR}"
        env_prefix+="$(printf 'SB_LOCK=%q ' "${LOCK_DIR}/${host}")"
        env ${env_prefix} bash -s <<<"${REMOTE}"
    else
        env_prefix+="$(printf 'SB_LOCK=%q ' "${LOCK_PATH}")"
        timeout 120 sshpass -p "${SSH_PASSWORD}" ssh \
            -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
            -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o LogLevel=ERROR \
            "${SSH_USER}@${host}" "${env_prefix} bash -s" <<<"${REMOTE}"
    fi
}

# Anything above 2 is ssh or the remote shell failing, which must never be read
# as a lock state: a host we cannot reach is a host we cannot use.
transport_error() {
    echo "::error::lab host $1: unreachable or remote failure (rc=$2)" >&2
    exit 1
}

report_conflict() {
    local host="$1" holder="$2" verb="$3"
    echo "::error::lab host ${host} ${verb}:" >&2
    echo "${holder:-  (unreadable lock)}" >&2
    echo "If that run is dead, clear it with:" >&2
    echo "  ./e2e/lab_lock.sh break --force --hosts \"${host}\"" >&2
}

echo "${MODE}: ${#TARGETS[@]} host(s): ${TARGETS[*]}"

case "${MODE}" in
acquire)
    payload_b64="$(payload | base64 -w0)"
    deadline=$((SECONDS + GRACE))
    while :; do
        taken=(); conflict_host=""; conflict_holder=""
        for host in "${TARGETS[@]}"; do
            set +e; holder="$(remote "${host}" create "${payload_b64}")"; rc=$?; set -e
            case "${rc}" in
                0) taken+=("${host}"); echo "  ${host}: acquired" ;;
                1) conflict_host="${host}"; conflict_holder="${holder}" ;;
                *) transport_error "${host}" "${rc}" ;;
            esac
            [[ -z "${conflict_host}" ]] || break
        done
        [[ -n "${conflict_host}" ]] || exit 0

        # Release what we took, so a run that loses a race never leaves the lab
        # half-held -- two runs contending for overlapping sets would otherwise
        # deadlock each other.
        if [[ ${#taken[@]} -gt 0 ]]; then
            for host in "${taken[@]}"; do
                remote "${host}" remove >/dev/null 2>&1 || true
            done
        fi

        if [[ ${SECONDS} -ge ${deadline} ]]; then
            report_conflict "${conflict_host}" "${conflict_holder}" "is held by"
            exit 1
        fi
        # cancel-in-progress is this lab's kill switch, so a new run routinely
        # starts while the run it cancelled is still releasing its locks.
        echo "  ${conflict_host} busy, retrying in 5s (handover grace)"
        sleep 5
    done ;;

verify)
    for host in "${TARGETS[@]}"; do
        set +e; holder="$(remote "${host}" check)"; rc=$?; set -e
        case "${rc}" in
            0) echo "  ${host}: held" ;;
            1) report_conflict "${host}" "${holder}" "was taken over by"; exit 1 ;;
            2) report_conflict "${host}" "" "has no lock (broken?)"; exit 1 ;;
            *) transport_error "${host}" "${rc}" ;;
        esac
    done ;;

release)
    for host in "${TARGETS[@]}"; do
        set +e; remote "${host}" remove >/dev/null; rc=$?; set -e
        case "${rc}" in
            0) echo "  ${host}: released" ;;
            1) echo "  ${host}: not ours, left alone" ;;
            2) echo "  ${host}: already free" ;;
            *) echo "  ${host}: release failed (rc=${rc})" >&2 ;;
        esac
    done ;;

status)
    for host in "${TARGETS[@]}"; do
        set +e; holder="$(remote "${host}" check)"; rc=$?; set -e
        case "${rc}" in
            0) echo "  ${host}: held by this run (${OWNER})" ;;
            1) echo "  ${host}: held -- $(tr -d '\n' <<<"${holder}")" ;;
            2) echo "  ${host}: free" ;;
            *) echo "  ${host}: unreachable (rc=${rc})" ;;
        esac
    done ;;

break)
    for host in "${TARGETS[@]}"; do
        set +e; remote "${host}" force-remove >/dev/null; rc=$?; set -e
        [[ "${rc}" -eq 0 ]] && echo "  ${host}: lock removed" || transport_error "${host}" "${rc}"
    done ;;

*) usage ;;
esac
