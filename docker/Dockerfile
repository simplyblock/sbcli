# syntax=docker/dockerfile:1

ARG UBI_VERSION=10.2
ARG FDB_VERSION=7.3.69

# Busts the cache of the OS-package layers below. Those layers are pinned only
# by package name, so without this they would keep serving whatever `dnf`
# resolved to the first time and never pick up a security update. CI and
# build_image.sh pass the current ISO week, giving an upstream fix at most seven
# days to reach the image. A build that passes nothing keeps this default and so
# re-uses whatever it has cached.
ARG CACHE_KEY=local


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
        python3.13 \
        sudo \
        systemd-257 \
        util-linux \
        wget && \
    dnf clean all

# parted, libnvme and nvme-cli are not in the UBI repositories, and the
# FoundationDB client RPM is signed with a digest dnf rejects.
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) rpm_arch=x86_64; fdb_dist=el7 ;; \
        arm64) rpm_arch=aarch64; fdb_dist=el9 ;; \
        *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    wget -q -O /tmp/foundationdb-clients.rpm \
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

RUN useradd -ms /bin/bash simplyblock && \
    echo "simplyblock  ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY LICENSE /licenses/LICENSE

# The venv is built from the platform default python3 — the interpreter the
# services are started with — and put first on PATH so that plain `python3`,
# `sbctl` and the background services all resolve to it.
ENV VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH


FROM base AS builder

ARG TARGETARCH

RUN dnf install -y gcc python3-devel python3-pip && \
    python3 -m venv "${VIRTUAL_ENV}"

WORKDIR /app

# Dependencies are installed from requirements.txt alone, so that editing
# application sources does not invalidate this layer.
COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH},sharing=locked \
    pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

COPY . .

# Installed editable so /app stays the single copy of the code: the package
# resolves its scripts, templates and dashboards relative to __file__, and
# those data files are not part of the wheel.
#
# pip, setuptools and wheel are then dropped: nothing in the image installs
# packages at runtime, and shipping them only adds vulnerable code (notably
# pip's vendored bundle) to every image scan.
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH},sharing=locked \
    pip install --no-deps --no-build-isolation --editable . && \
    pip uninstall --yes setuptools wheel pip


FROM base AS runtime

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY --chown=simplyblock:simplyblock . /app

WORKDIR /app
USER simplyblock
