# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

USER root

RUN dnf install -y iproute procps-ng libcap openssh systemd && \
    curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')/kubectl" && \
    chmod +x kubectl && \
    mv ./kubectl /usr/local/bin/kubectl

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

RUN rm -rf /app/tests

RUN python3 setup.py install

RUN chown -R simplyblock:simplyblock /app

USER simplyblock
