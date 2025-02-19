#!/usr/bin/env bash

sudo yum install -y yum-utils xorg-x11-xauth nvme-cli fio
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install hostname pkg-config git wget python3-pip yum-utils docker-ce docker-ce-cli \
  containerd.io docker-buildx-plugin docker-compose-plugin iptables -y

sudo systemctl enable docker
sudo systemctl start docker

sudo yum install -y https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients-7.3.3-1.el7.x86_64.rpm -q

sudo mkdir -p /etc/foundationdb/data /etc/foundationdb/logs
sudo chown -R foundationdb:foundationdb /etc/foundationdb
sudo chmod 777 /etc/foundationdb

sudo modprobe nvme-tcp
sudo modprobe nbd

sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1

# required for graylog
sudo sysctl -w vm.max_map_count=262144

echo "net.core.rmem_max=16777216" >> /etc/sysctl.conf
echo "net.core.rmem_default=87380" >> /etc/sysctl.conf
echo "net.ipv4.tcp_rmem=4096 87380 16777216" >> /etc/sysctl.conf
echo "net.core.somaxconn=1024" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog=4096" >> /etc/sysctl.conf
echo "net.ipv4.tcp_window_scaling=1" >> /etc/sysctl.conf
echo "net.ipv4.tcp_retries2=8" >> /etc/sysctl.conf
sysctl -p  # Apply changes immediately

sudo mkdir -p /etc/simplyblock
sudo chmod 777 /etc/simplyblock

sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

sudo sed -i 's/ProcessSizeMax=.*/ProcessSizeMax=5G/' /etc/systemd/coredump.conf

sudo systemctl restart iptables
sudo iptables -F
sudo iptables -X
sudo iptables -P INPUT ACCEPT
sudo iptables -P FORWARD ACCEPT
sudo iptables -P OUTPUT ACCEPT