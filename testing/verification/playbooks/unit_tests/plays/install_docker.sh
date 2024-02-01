#!/bin/bash
yum update -y
yum install -y yum-utils
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum install pkg-config git wget python3-pip yum-utils docker-ce docker-ce-cli \
  containerd.io docker-buildx-plugin docker-compose-plugin nvme-cli -y
systemctl enable docker
systemctl start docker
