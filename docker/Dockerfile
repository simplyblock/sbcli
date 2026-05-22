# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

USER root

RUN dnf install -y iproute procps-ng libcap-2.69-7.el10_2.1 openssh-9.9p1-23.el10_2 systemd-257-23.el10_2.1

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

RUN rm -rf /app/tests

RUN python3 setup.py install

RUN chown -R simplyblock:simplyblock /app

USER simplyblock
