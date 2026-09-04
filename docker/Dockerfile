# syntax=docker/dockerfile:1

ARG UBI_VERSION=10.2
ARG FDB_VERSION=7.3.69
ARG UV_VERSION=0.12.5

# The application interpreter, free-threaded ("t" suffix). Managed by uv rather
# than dnf: UBI carries no free-threaded build, and tying the runtime to the
# platform python is what pinned the image to 3.12 while CI tested another
# version. Minor-level, so a patch release arrives with the next CACHE_KEY roll.
ARG PYTHON_VERSION=3.14

# Busts the cache of the OS-package layers below. Those layers are pinned only
# by package name, so without this they would keep serving whatever `dnf`
# resolved to the first time and never pick up a security update. CI and
# build_image.sh pass the current ISO week, giving an upstream fix at most seven
# days to reach the image. A build that passes nothing keeps this default and so
# re-uses whatever it has cached.
ARG CACHE_KEY=local


FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv


# The OS layer both the builder and the final image are derived from, so the
# expensive dnf work is done once and shared by both.
FROM registry.access.redhat.com/ubi10/ubi:${UBI_VERSION} AS base

LABEL name="simplyblock" \
      vendor="Simplyblock" \
      version="1.0.0" \
      release="1" \
      summary="Simplyblock controlplane plane component" \
      description="Simplyblock controlplane plane container" \
      maintainer="developers@simplyblock.io"

ARG TARGETARCH
ARG FDB_VERSION
ARG CACHE_KEY

# CACHE_KEY is referenced here only to tie it into this layer's cache key; the
# layers below inherit the invalidation from this one.
#
# `python3` is present for dnf's own sake, not the application's -- dnf,
# subscription-manager and friends are written in Python and removing it breaks
# the package manager. Nothing the application needs is installed into it. No
# `python3-pip` and no compiler toolchain: every dependency resolves to a
# cp314t wheel and `foundationdb` is a pure-Python sdist, so a build that would
# need to compile something fails loudly here instead of silently producing a
# slow, source-built extension.
RUN echo "OS package layer cache key: ${CACHE_KEY}" && \
    dnf install -y \
        https://dl.fedoraproject.org/pub/epel/epel-release-latest-10.noarch.rpm && \
    dnf install -y \
        ca-certificates \
        curl \
        dmidecode \
        gdisk \
        hostname \
        iproute \
        iputils \
        iscsi-initiator-utils \
        kmod \
        libcap-2.69 \
        nano \
        openssh-9.9p1 \
        openssh-clients \
        pciutils \
        pkg-config \
        procps-ng \
        python3 \
        sudo \
        systemd-257 \
        util-linux && \
    dnf clean all

# parted, libnvme and nvme-cli are not in the UBI repositories, and the
# FoundationDB client RPM is signed with a digest dnf rejects.
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) rpm_arch=x86_64; fdb_dist=el7 ;; \
        arm64) rpm_arch=aarch64; fdb_dist=el9 ;; \
        *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/foundationdb-clients.rpm \
        "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients-${FDB_VERSION}-1.${fdb_dist}.${rpm_arch}.rpm"; \
    rpm -ivh --nodigest --nofiledigest /tmp/foundationdb-clients.rpm; \
    rm -f /tmp/foundationdb-clients.rpm; \
    base_url="https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/${rpm_arch}/os/Packages"; \
    dnf install -y \
        "${base_url}/parted-3.6-7.el10.${rpm_arch}.rpm" \
        "${base_url}/libnvme-1.16.1-3.el10.${rpm_arch}.rpm" \
        "${base_url}/nvme-cli-2.16-2.el10.${rpm_arch}.rpm"; \
    dnf clean all

RUN dnf upgrade -y && dnf clean all

# The unprivileged runtime user, plus the one line that makes the venv usable
# under sudo.
#
# Several entry points start as `sudo -E python3 ...`: the storage-node API
# (storage_node_ops.py), the SPDK proxy (storage_node/docker.py and
# templates/storage_deploy_spdk.yaml.j2) and node_configure.py (invoked by the
# operator's init script, kubernetes/operator/internal/utils/storage_nodeset_ds.go).
# UBI's /etc/sudoers sets `Defaults secure_path = /sbin:/bin:/usr/sbin:/usr/bin`,
# which replaces PATH *even with -E*, so without the override below every one of
# those resolves /bin/python3 -- the platform interpreter, which has none of the
# application's dependencies. Measured on ubi10/ubi:10.2: all six sudo forms
# (bare, -E, via su, via sh -c, and the operator's `eval sudo -E`) pick the
# system interpreter without this line and the venv with it, while `dnf` and
# absolute-path invocations such as `sudo /bin/python3` are unaffected.
#
# A drop-in rather than an append to /etc/sudoers: re-running cannot duplicate
# the line, and `visudo -cf` rejects a syntax error that would otherwise lock
# out sudo entirely on first boot.
RUN set -eux; \
    useradd -ms /bin/bash simplyblock; \
    printf '%s\n' \
      'Defaults secure_path="/opt/venv/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin"' \
      'simplyblock ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/simplyblock; \
    chmod 0440 /etc/sudoers.d/simplyblock; \
    visudo -cf /etc/sudoers.d/simplyblock

COPY LICENSE /licenses/LICENSE

# Putting the venv first on PATH is what makes plain `python3`, `python`,
# `sbctl` and the background services resolve to it. `sudo` gets there through
# secure_path above instead, because it ignores this.
ENV VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH


FROM base AS builder

ARG PYTHON_VERSION

COPY --from=uv /uv /usr/local/bin/uv

# UV_PYTHON_INSTALL_DIR is a fixed path rather than the default under $HOME
# because the runtime stage has to copy it: a uv-managed venv symlinks its
# interpreter into this directory and records the absolute path in pyvenv.cfg,
# so /opt/venv alone is not self-contained.
ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

RUN uv python install "${PYTHON_VERSION}t"

WORKDIR /app

# Dependencies resolve from pyproject.toml and uv.lock alone -- `uv sync` needs
# the manifest, not the sources (a lockfile on its own is rejected), and
# --no-install-project keeps this layer independent of every source edit.
# --locked fails the build if uv.lock is stale with respect to pyproject.toml,
# rather than silently resolving something new.
#
# No `RUN --mount=type=cache`: cache mounts are not exported to the gha or
# registry cache backends, so on a fresh CI runner they are never populated.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-default-groups --no-install-project --python "${PYTHON_VERSION}t"

COPY . .

# Installed editable, so /app stays the single copy of the code. This is
# compatibility scaffolding, not the intended end state: the Helm charts, the
# Swarm compose file and the operator still name source files
# (`python3 simplyblock_core/services/<name>.py`), which only resolve because
# /app is the package. Once those consumers use the `simplyblock-service` and
# `sbctl` entry points, this becomes `--no-editable` and the `COPY . /app` in
# the runtime stage goes away. See AGENTS.md.
#
# uv creates the environment without pip, setuptools or wheel, so there is
# nothing to uninstall afterwards and no vendored pip bundle for the image scan
# to flag.
RUN uv sync --locked --no-default-groups --python "${PYTHON_VERSION}t"


FROM base AS runtime

COPY --from=builder /opt/python /opt/python
COPY --from=builder /opt/venv /opt/venv
COPY --chown=simplyblock:simplyblock . /app

# The services log through the container's gelf log driver, and a buffered
# stdout loses the tail of the log when a process dies.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
USER simplyblock
