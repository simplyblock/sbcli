# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock controlplane plane component"
LABEL description="Simplyblock controlplane plane container"
LABEL maintainer="developers@simplyblock.io"

COPY LICENSE /licenses/LICENSE

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt


COPY . /app

RUN python setup.py install

RUN useradd -u 1001 -r -g 0 -d /app -s /sbin/nologin simplyblock && \
    chown -R 1001:0 /app

USER 1001
