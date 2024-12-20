# syntax=docker/dockerfile:1
FROM python:3.8-slim

RUN apt-get update; apt-get install -y \
    wget libc6 inetutils-ping dmidecode nvme-cli open-iscsi \
    iproute2 pciutils sudo nano parted gdisk ssh; \
    rm -rf /var/lib/apt/lists/*

# install fdb client library (libfdb_c.dylib) for the Python bindings to interact with the database.
RUN wget https://github.com/apple/foundationdb/releases/download/7.3.56/foundationdb-clients_7.3.56-1_amd64.deb
RUN dpkg -i foundationdb-clients_7*
RUN rm -f foundationdb-clients_7*

WORKDIR /app

# pip install
COPY requirements.txt requirements.txt
COPY simplyblock_web/requirements.txt simplyblock_web/requirements.txt

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt
