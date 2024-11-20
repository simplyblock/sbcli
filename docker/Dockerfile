# syntax=docker/dockerfile:1
FROM python:3.8

RUN apt-get update -y; apt-get install -y \
    wget libc6 inetutils-ping vim dmidecode nvme-cli open-iscsi \
    iproute2 pciutils sudo nano parted gdisk && \
    rm -rf /var/lib/apt/lists/*

# install fdb client library (libfdb_c.dylib) for the Python bindings to interact with the database.
RUN wget https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients_7.3.3-1_amd64.deb
RUN dpkg -i --force-architecture --force-depends foundationdb-clients_7.3.3-1_amd64.deb
RUN rm -f foundationdb-clients_7.3.3-1_amd64.deb

WORKDIR /app

# pip install
COPY requirements.txt requirements.txt
COPY simplyblock_web/requirements.txt simplyblock_web/requirements.txt

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

# copy other artifacts
COPY simplyblock_core simplyblock_core
COPY simplyblock_web simplyblock_web
COPY simplyblock_cli simplyblock_cli
COPY env_var env_var
COPY setup.py setup.py
COPY README.md README.md
COPY ./docker/motd /etc/motd

# generate sbcli-dev
RUN python setup.py install
