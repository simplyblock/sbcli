# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

USER root

# This image inherits its packages from the base_image tag, which used to be
# refreshed daily by docker-base-image.yml. That job was removed in 1b97d897b
# and can no longer be triggered, so update explicitly here instead.
RUN dnf upgrade -y \
 && dnf install -y iproute procps-ng libcap openssh systemd \
 && dnf clean all

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

RUN rm -rf /app/tests

RUN python3 setup.py install

RUN chown -R simplyblock:simplyblock /app

USER simplyblock
