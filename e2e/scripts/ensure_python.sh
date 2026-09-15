#!/usr/bin/env bash
# Guarantee the e2e suite runs on the Python it is written for.
#
# Why this exists
# ---------------
# The workflows invoke the suite as a bare `python3`, so which interpreter runs
# is whatever the self-hosted runner happens to have. That is a coin toss:
# actions-runner2 ran the suite for 7h51m while actions-runner-3, on an older
# Python, died at import with
#
#   def _try_connect(self, ..., pkey: paramiko.PKey | None, ...)
#   TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
#
# because `X | None` (PEP 604) is evaluated at def time and needs 3.10+. The
# failure arrived hours into a bootstrap, on one runner and not another, which
# is the worst possible way to learn about an interpreter version.
#
# pyproject declares requires-python >= 3.11 and CI's lint/type jobs use 3.11,
# so 3.11 is the contract. This makes the runners honour it rather than hope.
#
# What it does
# ------------
#   1. Look for an interpreter >= the required minor on PATH.
#   2. If there is none, fetch one with uv -- no root, no system packages.
#   3. Build (or reuse) a venv from it and install e2e/requirements.txt.
#   4. Put that venv first on PATH, so every later `python3` in the workflow
#      resolves to it with no other edits.
#
# What it never does
# -------------------
# It does not replace, upgrade or remove any Python the host already has,
# and it writes nothing outside $HOME/.cache/sbcli-e2e. The PATH change
# applies to the GitHub job only, not to the runner's shell.
#
# Compulsory on purpose: if a suitable Python cannot be obtained this exits
# non-zero rather than falling back to the host's. A run that silently uses the
# wrong interpreter is how we got here.
#
# Usage, from a workflow step:
#     bash sbcli/e2e/scripts/ensure_python.sh
#     bash sbcli/e2e/scripts/ensure_python.sh --requirements path/to/requirements.txt
set -euo pipefail

REQUIRED_MAJOR=3
REQUIRED_MINOR=11
REQUIREMENTS=""
CACHE_ROOT="${E2E_CACHE_ROOT:-$HOME/.cache/sbcli-e2e}"
VENV_DIR="${E2E_VENV_DIR:-$CACHE_ROOT/venv${REQUIRED_MAJOR}${REQUIRED_MINOR}}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --requirements) REQUIREMENTS="$2"; shift 2 ;;
        --venv-dir)     VENV_DIR="$2";     shift 2 ;;
        --min-minor)    REQUIRED_MINOR="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

# Always stderr: install_with_uv and find_host_python are consumed through
# $(...), so anything a helper writes to stdout is captured as the
# interpreter path. A log line ending up in BASE_PYTHON is not a message
# nobody reads -- it is a path that cannot exist.
log() { echo "[ensure_python] $*" >&2; }

# Default to the suite's own requirements, resolved from this script's location
# so a caller does not have to know the layout.
if [[ -z "$REQUIREMENTS" ]]; then
    _here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -f "$_here/../requirements.txt" ]]; then
        REQUIREMENTS="$_here/../requirements.txt"
    fi
fi

version_ok() {
    # Usable without importing anything: an interpreter too old to run the
    # suite is also too old to be trusted with a fancier check.
    "$1" -c "import sys; sys.exit(0 if sys.version_info[:2] >= ($REQUIRED_MAJOR, $REQUIRED_MINOR) else 1)" \
        >/dev/null 2>&1
}

find_host_python() {
    # Newest first, then the generic names last: python3 on these hosts is the
    # one that was too old, so it should never win over an explicit 3.1x.
    local c
    for c in python3.14 python3.13 python3.12 "python${REQUIRED_MAJOR}.${REQUIRED_MINOR}" python3 python; do
        if command -v "$c" >/dev/null 2>&1 && version_ok "$c"; then
            command -v "$c"
            return 0
        fi
    done
    return 1
}

# Windows venvs put the interpreter in Scripts/, POSIX in bin/. The runners are
# Linux, but keeping this portable means the script can be exercised locally
# instead of only ever being tested by a CI run.
venv_bin_dir() {
    [[ -d "$1/Scripts" ]] && { echo "$1/Scripts"; return; }
    echo "$1/bin"
}
venv_python() {
    local d; d="$(venv_bin_dir "$1")"
    [[ -x "$d/python.exe" ]] && { echo "$d/python.exe"; return; }
    echo "$d/python"
}

install_with_uv() {
    # Same technique as simplyBlockDeploy PR #221, which installs sbctl against
    # a uv-managed 3.11 because the storage nodes do not ship one either.
    #
    # The standalone installer needs no Python at all, which matters here: a
    # runner old enough to break the suite may not have an interpreter worth
    # bootstrapping from. INSTALLER_NO_MODIFY_PATH keeps it out of the user's
    # shell profile.
    #
    # Unlike #221 this does NOT install to /usr/local/bin. That needs root and
    # is shared system state; on a runner we should own nothing outside our
    # own cache.
    local uv_bin=""
    if command -v uv >/dev/null 2>&1; then
        uv_bin="$(command -v uv)"
    elif [[ -x "$CACHE_ROOT/bin/uv" ]]; then
        uv_bin="$CACHE_ROOT/bin/uv"
    else
        log "uv not present; fetching it into $CACHE_ROOT/bin (nothing system-wide)"
        mkdir -p "$CACHE_ROOT/bin"
        curl -LsSf https://astral.sh/uv/install.sh             | env UV_INSTALL_DIR="$CACHE_ROOT/bin" INSTALLER_NO_MODIFY_PATH=1 sh >&2 || {
                log "ERROR: could not install uv"; return 1; }
        uv_bin="$CACHE_ROOT/bin/uv"
    fi
    [[ -x "$uv_bin" ]] || { log "ERROR: uv not usable at $uv_bin"; return 1; }

    log "fetching CPython ${REQUIRED_MAJOR}.${REQUIRED_MINOR} with uv"
    # A standalone build under uv's own directory. It does not replace, upgrade
    # or shadow any interpreter the host already has: no shim is created,
    # because `uv python update-shell` is never called.
    "$uv_bin" python install "${REQUIRED_MAJOR}.${REQUIRED_MINOR}" >&2 || return 1
    "$uv_bin" python find "${REQUIRED_MAJOR}.${REQUIRED_MINOR}" 2>/dev/null
}

# ── 1/2. get an interpreter ────────────────────────────────────────────────
BASE_PYTHON="$(find_host_python || true)"
if [[ -n "$BASE_PYTHON" ]]; then
    log "found $("$BASE_PYTHON" -V 2>&1) at $BASE_PYTHON"
else
    log "no Python >= ${REQUIRED_MAJOR}.${REQUIRED_MINOR} on PATH"
    BASE_PYTHON="$(install_with_uv || true)"
    if [[ -z "$BASE_PYTHON" || ! -x "$BASE_PYTHON" ]] || ! version_ok "$BASE_PYTHON"; then
        log "ERROR: could not obtain Python >= ${REQUIRED_MAJOR}.${REQUIRED_MINOR}."
        log "       The e2e suite requires it (pyproject: requires-python >= 3.11)"
        log "       and will fail at import on anything older."
        exit 1
    fi
    log "using $("$BASE_PYTHON" -V 2>&1) at $BASE_PYTHON"
fi

# ── 3. venv ────────────────────────────────────────────────────────────────
# Reused across runs and revalidated every time: a venv left over from an
# interpreter that has since been upgraded or removed is worse than none.
if [[ -x "$(venv_python "$VENV_DIR")" ]] && version_ok "$(venv_python "$VENV_DIR")"; then
    log "reusing venv at $VENV_DIR ($("$(venv_python "$VENV_DIR")" -V 2>&1))"
else
    [[ -e "$VENV_DIR" ]] && { log "discarding unusable venv at $VENV_DIR"; rm -rf "$VENV_DIR"; }
    log "creating venv at $VENV_DIR"
    mkdir -p "$(dirname "$VENV_DIR")"
    "$BASE_PYTHON" -m venv "$VENV_DIR" >&2
fi

VENV_PY="$(venv_python "$VENV_DIR")"
"$VENV_PY" -m pip install --quiet --upgrade pip >&2 || log "WARN: pip upgrade failed, continuing"
if [[ -n "$REQUIREMENTS" && -f "$REQUIREMENTS" ]]; then
    log "installing $REQUIREMENTS"
    "$VENV_PY" -m pip install --quiet -r "$REQUIREMENTS" >&2
else
    log "WARN: no requirements file found, skipping dependency install"
fi

# ── 4. make it the default for the rest of the job ─────────────────────────
log "using $("$VENV_PY" -V 2>&1)"
if [[ -n "${GITHUB_PATH:-}" ]]; then
    echo "$(venv_bin_dir "$VENV_DIR")" >> "$GITHUB_PATH"
    log "prepended $(venv_bin_dir "$VENV_DIR") to PATH for subsequent steps"
fi
if [[ -n "${GITHUB_ENV:-}" ]]; then
    {
        echo "E2E_PYTHON=$VENV_PY"
        echo "VIRTUAL_ENV=$VENV_DIR"
    } >> "$GITHUB_ENV"
fi
echo "$VENV_PY"
