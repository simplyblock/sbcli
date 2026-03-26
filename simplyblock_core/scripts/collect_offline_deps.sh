#!/usr/bin/env bash
# Collects all prerequisites needed to install sbcli on an offline machine.
# Run this on a machine WITH internet access, then transfer the tarball.
#
# Usage: ./collect_offline_deps.sh [output_dir]
#   output_dir  defaults to <repo-root>/sbcli-offline
#
# Output:
#   <output_dir>.tar.gz  — self-contained archive with all deps + install.sh

#set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
OUTPUT_DIR="${1:-${ROOT_DIR}/sbcli-offline}"
TARBALL="${OUTPUT_DIR}.tar.gz"

PYTHON_VERSION="3.9"
PLATFORM="manylinux2014_x86_64"

FDB_VERSION="7.3.56"
FDB_BASE_URL="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}"

# Detect OS family to pick the right FDB package
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID="${ID_LIKE:-${ID}}"
else
    OS_ID="unknown"
fi

case "${OS_ID}" in
    *rhel*|*fedora*|*centos*|rocky|almalinux)
        FDB_PKG="foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm"
        PKG_MANAGER="rpm"
        ;;
    *debian*|*ubuntu*)
        FDB_PKG="foundationdb-clients_${FDB_VERSION}-1_amd64.deb"
        PKG_MANAGER="dpkg"
        ;;
    *)
        echo "[warn] Unknown OS family '${OS_ID}', defaulting to RPM"
        FDB_PKG="foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm"
        PKG_MANAGER="rpm"
        ;;
esac

FDB_URL="${FDB_BASE_URL}/${FDB_PKG}"

# -----------------------------------------------------------------------

echo "==> Preparing output directory: ${OUTPUT_DIR}"
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/deps"

# -----------------------------------------------------------------------
# 1. Python dependencies — pip resolves the full transitive tree
#    (every package's deps, and their deps, and so on)
# -----------------------------------------------------------------------
echo "==> Downloading Python dependencies and all transitive deps..."
echo "    (platform=${PLATFORM}, python=${PYTHON_VERSION})"

if pip download \
    --python-version "${PYTHON_VERSION}" \
    --platform "${PLATFORM}" \
    --implementation cp \
    --abi cp39 \
    --only-binary=:all: \
    -r "${ROOT_DIR}/requirements.txt" \
    -d "${OUTPUT_DIR}/deps"; then
    echo "    All binary wheels downloaded."
else
    echo "  [warn] Some packages have no binary wheel for ${PLATFORM}."
    echo "  [info] Retrying without platform constraints (includes source dists)..."
    pip download \
        -r "${ROOT_DIR}/requirements.txt" \
        -d "${OUTPUT_DIR}/deps"
fi

# setuptools is required by pyproject.toml at build time
echo "==> Downloading setuptools (build requirement)..."
pip download "setuptools>70" -d "${OUTPUT_DIR}/deps"

# -----------------------------------------------------------------------
# 2. Build the sbcli wheel itself (no deps — already collected above)
# -----------------------------------------------------------------------
echo "==> Building sbcli wheel..."
pip wheel "${ROOT_DIR}" --no-deps -w "${OUTPUT_DIR}/deps"

# The sbcli wheel was just built — it will be the newest file in deps/
SBCLI_WHEEL=$(ls -t "${OUTPUT_DIR}/deps"/*.whl | head -1 | xargs basename)
SBCLI_PKG_NAME="${SBCLI_WHEEL%%-[0-9]*}"   # strip version suffix → e.g. sbcli_dev
echo "    Package name: ${SBCLI_PKG_NAME}"
echo "${SBCLI_PKG_NAME}" > "${OUTPUT_DIR}/.pkg_name"

# -----------------------------------------------------------------------
# 3. FoundationDB C client library
#    OS-appropriate package (.rpm for RHEL/Rocky, .deb for Debian/Ubuntu)
# -----------------------------------------------------------------------
echo "==> Downloading FoundationDB client library: ${FDB_PKG}..."
curl -fSL "${FDB_URL}" -o "${OUTPUT_DIR}/${FDB_PKG}"

# Store the pkg manager so install.sh knows what to use
echo "${PKG_MANAGER}" > "${OUTPUT_DIR}/.pkg_manager"

# -----------------------------------------------------------------------
# 4. Bundle install script
# -----------------------------------------------------------------------
cat > "${OUTPUT_DIR}/install.sh" << 'INSTALL_EOF'
#!/usr/bin/env bash
# Offline installer for sbcli.
# Run as root (or with sudo) on the target machine.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

PKG_MANAGER=$(cat "${SCRIPT_DIR}/.pkg_manager" 2>/dev/null || echo "rpm")

if [ "$(uname -m)" = "x86_64" ]; then
    case "${PKG_MANAGER}" in
        rpm)
            FDB_PKG=$(ls "${SCRIPT_DIR}"/foundationdb-clients-*.rpm 2>/dev/null | head -1)
            if [ -n "${FDB_PKG}" ]; then
                echo "==> Installing FoundationDB client library: $(basename "${FDB_PKG}")"
                rpm -Uvh --nodeps "${FDB_PKG}"
            fi
            ;;
        dpkg)
            FDB_PKG=$(ls "${SCRIPT_DIR}"/foundationdb-clients_*.deb 2>/dev/null | head -1)
            if [ -n "${FDB_PKG}" ]; then
                echo "==> Installing FoundationDB client library: $(basename "${FDB_PKG}")"
                dpkg -i "${FDB_PKG}"
            fi
            ;;
    esac
fi

SBCLI_PKG_NAME=$(cat "${SCRIPT_DIR}/.pkg_name" 2>/dev/null || echo "sbcli")

echo "==> Installing ${SBCLI_PKG_NAME} and all Python dependencies (offline)..."
pip install \
    --no-index \
    --find-links="${SCRIPT_DIR}/deps" \
    "${SBCLI_PKG_NAME}"

echo "==> Done. Run 'sbctl --help' to verify."
INSTALL_EOF
chmod +x "${OUTPUT_DIR}/install.sh"

# -----------------------------------------------------------------------
# 5. Package everything into a tarball
# -----------------------------------------------------------------------
echo "==> Creating tarball: ${TARBALL}"
tar czf "${TARBALL}" -C "$(dirname "${OUTPUT_DIR}")" "$(basename "${OUTPUT_DIR}")"
rm -rf "${OUTPUT_DIR}"

echo ""
echo "Done!"
echo "  Tarball : ${TARBALL}"
echo ""
echo "To install on an offline machine:"
echo "  scp ${TARBALL} user@target:/tmp/"
echo "  ssh user@target"
echo "  tar xzf /tmp/sbcli-offline.tar.gz -C /tmp"
echo "  sudo /tmp/sbcli-offline/install.sh"
