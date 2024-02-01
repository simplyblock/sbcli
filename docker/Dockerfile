# syntax=docker/dockerfile:1
FROM python:3.8

WORKDIR /app

COPY simplyblock_core simplyblock_core
COPY simplyblock_web simplyblock_web
COPY simplyblock_cli simplyblock_cli
COPY env_var env_var
COPY setup.py setup.py
COPY README.md README.md
COPY requirements.txt requirements.txt
COPY ./docker/motd /etc/motd

RUN apt-get update -y; apt-get install -y wget libc6 inetutils-ping vim dmidecode nvme-cli iproute2 pciutils sudo

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

RUN python setup.py install

RUN apt-get install -y openssh-server
# Configure the SSH server
RUN mkdir /var/run/sshd && \
    echo 'root:123456' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

RUN wget https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients_7.3.3-1_amd64.deb
RUN dpkg -i --force-architecture --force-depends foundationdb-clients_7.3.3-1_amd64.deb
RUN rm -f foundationdb-clients_7.3.3-1_amd64.deb
