# syntax=docker/dockerfile:1
# Built from UBI directly rather than from the simplyblock/simplyblock:base_image
# tag: nothing rebuilds that tag any more (docker-base-image.yml only runs from
# main, which deleted it in 1b97d897b), so it stays frozen on old packages that
# no `dnf upgrade` here can fix. The krb5 security fixes, for one, exist only
# in the 10.3 base image (krb5-1.21.3-11.el10_3.2), not in the UBI repos.
FROM registry.access.redhat.com/ubi10/ubi:10.3

LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock controlplane plane component"
LABEL description="Simplyblock controlplane plane container"
LABEL maintainer="developers@simplyblock.io"

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-10.noarch.rpm

RUN dnf install -y dmidecode pciutils sudo nano gdisk openssh-clients util-linux pkg-config hostname\
    ca-certificates curl kmod python3.13 python3-pip iputils iproute iscsi-initiator-utils \
    procps-ng libcap-2.69 openssh-9.9p1 systemd-257

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        echo "ARM64 version"; \
        curl -fsSL -o /tmp/foundationdb-clients.rpm \
            https://github.com/apple/foundationdb/releases/download/7.3.69/foundationdb-clients-7.3.69-1.el9.aarch64.rpm; \
        rpm -ivh --nodigest --nofiledigest /tmp/foundationdb-clients.rpm ; \
        rm -f /tmp/foundationdb-clients.rpm ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/aarch64/os/Packages/parted-3.6-7.el10.aarch64.rpm -y ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/aarch64/os/Packages/libnvme-1.16.1-3.el10.aarch64.rpm -y ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/aarch64/os/Packages/nvme-cli-2.16-2.el10.aarch64.rpm -y ; \
    else \
        echo "AMD64 version"; \
        curl -fsSL -o /tmp/foundationdb-clients.rpm \
            https://github.com/apple/foundationdb/releases/download/7.3.69/foundationdb-clients-7.3.69-1.el7.x86_64.rpm; \
        rpm -ivh --nodigest --nofiledigest /tmp/foundationdb-clients.rpm ; \
        rm -f /tmp/foundationdb-clients.rpm ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/x86_64/os/Packages/parted-3.6-7.el10.x86_64.rpm -y ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/x86_64/os/Packages/libnvme-1.16.1-3.el10.x86_64.rpm -y ; \
        yum install https://www.rpmfind.net/linux/centos-stream/10-stream/BaseOS/x86_64/os/Packages/nvme-cli-2.16-2.el10.x86_64.rpm -y ; \
    fi

COPY LICENSE /licenses/LICENSE

WORKDIR /app
COPY requirements.txt requirements.txt
# setuptools stays in the image (setup.py install below needs it); 84.1.0 fixes
# GHSA-xmj3-9gf7-h52w.
RUN pip3 install --upgrade pip 'setuptools>=84.1.0'
RUN yum remove python-urllib3 -y
RUN pip3 install -r requirements.txt

RUN dnf upgrade -y

RUN dnf clean all

RUN useradd -ms /bin/bash simplyblock
RUN echo "simplyblock  ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

COPY . /app

RUN rm -rf /app/tests

RUN python3 setup.py install
# pip is only needed to build this layer. Its vendored bundle (pip/_vendor:
# urllib3, certifi, requests, ...) is a second copy of those packages that
# trivy reads from vendor.txt and flags the moment an advisory lands -- and no
# pip release vendors a fixed urllib3 yet (26.2.2 still ships 2.7.0). Nothing
# in the container runs pip, so drop it, as the main image does.
RUN python3 -m pip uninstall -y pip

RUN chown -R simplyblock:simplyblock /app

USER simplyblock
