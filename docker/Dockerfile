# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:new-image-cluster

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

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

RUN pip install boto3 flask-openapi3

RUN python setup.py install
