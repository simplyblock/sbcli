# syntax=docker/dockerfile:1
#
# Simplyblock control-plane image.
#
# Stages are ordered by how often their inputs change, so a source-only edit
# rebuilds only the last one:
#
#   base-os      UBI packages, EPEL and the per-arch third-party RPMs
#                (FoundationDB client, parted, libnvme, nvme-cli), plus the
#                unprivileged runtime user. Minutes to build; only CACHE_KEY, or
#                an edit to this stage, evicts it.
#   python-deps  pip install -r requirements.txt. Evicted by requirements.txt.
#   runtime      the application. Evicted by any source change.
#
# CACHE_KEY replaces the separately published `simplyblock/simplyblock:base_image`
# tag and the cron that refreshed it. It is a truncated timestamp (weekly; see
# docker/docker-bake.hcl) that changes once per bucket, and changing it evicts
# base-os and therefore every stage below, so the rebuild picks up current RHEL
# errata. BuildKit only invalidates a layer for build args the layer actually
# *references*, so CACHE_KEY has to be consumed by a RUN in base-os -- declaring
# the ARG is not enough. Builds that do not go through bake (a plain
# `docker build`, docker-compose-dev.yml) get the "stable" default and so never
# pay for an eviction.
#
# There is deliberately no slim final stage and no venv. The runtime shells out
# to most of the OS tooling installed below (nvme, parted, sgdisk, dmidecode,
# lspci -- see storage_node_ops.py and simplyblock_web/api/internal/storage_node/),
# so a clean final stage would have to reinstall nearly all of it. A venv would be
# worse than merely pointless: the storage node is started as `sudo -E python3`
# (storage_node_ops.py), and sudoers' secure_path resets PATH even under -E, so
# `sudo python3` would resolve /usr/bin/python3 and miss the venv entirely.

ARG UBI_VERSION=10.2

# Third-party RPM versions, hoisted so a bump is a one-line change.
ARG FDB_VERSION=7.3.69
ARG PARTED_VERSION=3.6-7.el10
ARG LIBNVME_VERSION=1.16.1-3.el10
ARG NVME_CLI_VERSION=2.16-2.el10


FROM registry.access.redhat.com/ubi10/ubi:${UBI_VERSION} AS base-os

ARG CACHE_KEY=stable

# `dnf upgrade` is what makes a CACHE_KEY rollover worth anything: it pulls the
# errata published since the UBI base image itself was built, which is what the
# nightly base-image rebuild used to do. Install, upgrade and clean in a single
# RUN so the cache is all-or-nothing and `dnf clean all` actually shrinks the
# layer holding the metadata it cleans.
#
# Packages are deliberately unpinned (hadolint DL3041): pinned versions here are
# what forced the child image to re-run `dnf install libcap openssh systemd` to
# refresh them, which partly defeated the caching the base image existed to
# provide. CACHE_KEY plus `dnf upgrade` is the freshness mechanism instead.
#
# The four python3-* packages must go, because requirements.txt installs the same
# distributions from PyPI: pip cannot uninstall an RPM-provided one (it has no
# RECORD file) and fails the build, while merely shadowing them would leave their
# stale versions visible to the RPM-database scanners in security.yml.
#
# `dnf mark install` is what makes that removal safe, and is not redundant with
# installing the packages: UBI carries several of them as *dependencies*, and
# installing an already-present package does not change its install reason, so dnf
# still considers them autoremovable. Without the marks, removing python3-urllib3
# orphans subscription-manager and cascades into dmidecode, which, psmisc and
# iproute. That is a live bug, not a hypothetical -- it is why the published
# base_image and the product image built from it have no `dmidecode`, which
# simplyblock_core/utils and the storage-node APIs shell out to for the system UUID,
# one of them at module import time.
#
# The package list is a variable so `install` and `mark install` cannot drift apart.
# DL3038 misreads that as a missing `-y` (it is there), and the unquoted expansion
# SC2086 objects to is the point -- quoting would pass the whole list as one
# argument.
# hadolint ignore=DL3041,DL3038,SC2086
RUN echo "base-os cache key: ${CACHE_KEY}" \
 && dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-10.noarch.rpm \
 && PKGS="ca-certificates curl dmidecode gdisk hostname iproute iputils \
          iscsi-initiator-utils kmod libcap nano openssh openssh-clients pciutils \
          pkgconf-pkg-config procps-ng psmisc python3.13 python3-pip sudo systemd \
          util-linux wget which" \
 && dnf install -y ${PKGS} \
 && dnf mark install ${PKGS} \
 && dnf remove -y python3-urllib3 python3-requests python3-idna python3-charset-normalizer \
 && dnf upgrade -y \
 && dnf clean all \
 && rm -rf /var/cache/dnf

# Packages UBI does not carry. Kept in its own layer so bumping one of the
# versions above does not re-run the much larger dnf transaction, and run after
# `dnf upgrade` so a later upgrade cannot fight these CentOS-Stream-sourced
# packages.
#
# TARGETARCH rather than TARGETPLATFORM: platform variants such as
# linux/arm64/v8 would not match a "linux/arm64" string compare, and the old
# if/else answered an unrecognised platform by silently installing the x86_64
# RPMs. `set -eux` is not cosmetic either -- the old `;`-joined commands let a
# failed RPM download still produce a green build, because the exit status of the
# last `yum install` became the layer's, yielding an image with no FoundationDB
# client library at all.
#
# rpm --nodigest --nofiledigest: the amd64 FoundationDB client is an el7 build
# whose header digests the el10 crypto policy rejects.
ARG TARGETARCH
ARG FDB_VERSION
ARG PARTED_VERSION
ARG LIBNVME_VERSION
ARG NVME_CLI_VERSION
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) rpm_arch=x86_64;  fdb_dist=el7 ;; \
      arm64) rpm_arch=aarch64; fdb_dist=el9 ;; \
      *) echo "unsupported TARGETARCH: '${TARGETARCH}'" >&2; exit 1 ;; \
    esac; \
    fdb_rpm="foundationdb-clients-${FDB_VERSION}-1.${fdb_dist}.${rpm_arch}.rpm"; \
    curl -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${fdb_rpm}"; \
    rpm -ivh --nodigest --nofiledigest "${fdb_rpm}"; \
    rm -f "${fdb_rpm}"; \
    centos="https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/${rpm_arch}/os/Packages"; \
    dnf install -y \
      "${centos}/parted-${PARTED_VERSION}.${rpm_arch}.rpm" \
      "${centos}/libnvme-${LIBNVME_VERSION}.${rpm_arch}.rpm" \
      "${centos}/nvme-cli-${NVME_CLI_VERSION}.${rpm_arch}.rpm"; \
    dnf clean all; \
    rm -rf /var/cache/dnf

# The unprivileged runtime user. sudo is not optional: the storage-node API
# container runs as `sudo -E python3 simplyblock_web/node_webapp.py storage_node`
# (simplyblock_core/storage_node_ops.py). A drop-in file rather than an append to
# /etc/sudoers, so re-running cannot duplicate the line and visudo can validate it
# instead of a syntax error silently locking out sudo.
RUN set -eux; \
    useradd -ms /bin/bash simplyblock; \
    echo 'simplyblock ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/simplyblock; \
    chmod 0440 /etc/sudoers.d/simplyblock; \
    visudo -cf /etc/sudoers.d/simplyblock


FROM base-os AS python-deps

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# Its own layer: the pip/setuptools bootstrap is a function of CACHE_KEY, not of
# requirements.txt. The version echo is a diagnostic -- `dnf install python3.13`
# adds an interpreter but does not necessarily make it the default `python3`, and
# every runtime entry point invokes plain `python3`.
#
# Unpinned on purpose (hadolint DL3013): this is the build-tooling bootstrap, and
# the application's own versions come from requirements.txt below.
# hadolint ignore=DL3013
RUN python3 --version \
 && python3 -m pip install --upgrade pip setuptools

WORKDIR /app
COPY requirements.txt ./
# Deliberately no --ignore-installed: base-os removes the RPM copies of the
# packages requirements.txt also provides, so pip has nothing to collide with. If a
# future UBI release adds another RPM that duplicates a requirement, this fails
# loudly with `Cannot uninstall <pkg>: no RECORD file was found` -- which is the
# signal to add it to the removal in base-os, rather than silently shadowing it and
# leaving the stale version visible to the security scanners.
RUN python3 -m pip install -r requirements.txt


FROM python-deps AS runtime

# Labels belong to the stage producing the published image: they are what
# `docker inspect` reports, and declaring them here means restructuring the
# stages above cannot silently change them. name/vendor/version/release/summary/
# description plus /licenses are what Red Hat's container policy expects of a
# UBI-derived image.
ARG VCS_REF=unknown
LABEL name="simplyblock" \
      vendor="Simplyblock" \
      version="1.0.0" \
      release="1" \
      summary="Simplyblock controlplane plane component" \
      description="Simplyblock controlplane plane container" \
      maintainer="developers@simplyblock.io" \
      org.opencontainers.image.title="simplyblock" \
      org.opencontainers.image.source="https://github.com/simplyblock/sbcli" \
      org.opencontainers.image.revision="${VCS_REF}"

COPY LICENSE /licenses/LICENSE

# The services log through the container's gelf log driver, and a buffered stdout
# loses the tail of the log when a process dies.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
# --chown rather than a trailing `chown -R`, which wrote the whole application
# tree into the image a second time.
COPY --chown=simplyblock:simplyblock . /app

# Runtime entry points are `python3 <path>` run from /app
# (simplyblock_core/scripts/docker-compose-swarm.yml), but the console scripts are
# needed too: simplyblock_core/services/tasks_cluster_status.py shells out to
# `$SIMPLY_BLOCK_COMMAND_NAME cluster show`.
#   --no-deps            python-deps already installed requirements.txt; letting
#                        pip re-resolve install_requires -- which is what
#                        `setup.py install` did -- can silently move versions.
#   --no-build-isolation use the setuptools already present instead of fetching a
#                        build environment from PyPI.
# pip builds in-tree, so the detritus it leaves in /app is removed afterwards.
RUN set -eux; \
    python3 -m pip install --no-build-isolation --no-deps .; \
    rm -rf /app/build /app/*.egg-info; \
    python3 -c 'import simplyblock_core.constants'; \
    command -v sbctl

USER simplyblock
