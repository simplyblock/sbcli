FROM python:3.9-slim

RUN apt-get update ; apt-get upgrade -y

RUN apt-get install -y \
    wget libc6 inetutils-ping dmidecode nvme-cli open-iscsi iptables \
    iproute2 pciutils sudo nano parted gdisk ssh ca-certificates curl


RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null

RUN apt-get update ; apt-get install docker-ce-cli -y
RUN apt-get upgrade -y

RUN rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        echo "ARM64 version"; \
    else \
        echo "AMD64 version"; \
        wget https://github.com/apple/foundationdb/releases/download/7.3.56/foundationdb-clients_7.3.56-1_amd64.deb; \
        dpkg -i foundationdb-clients_7* ; \
        rm -f foundationdb-clients_7* ; \
    fi

WORKDIR /app
COPY . /app
RUN python3 -m ensurepip
RUN pip3 install .
