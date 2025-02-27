#!/usr/bin/env bash

sudo yum install -y yum-utils xorg-x11-xauth nvme-cli fio
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install hostname pkg-config git wget python3-pip yum-utils docker-ce docker-ce-cli \
  containerd.io docker-buildx-plugin docker-compose-plugin iptables pciutils -y

sudo systemctl enable docker
sudo systemctl start docker



if [[ 1 == $(yum info foundationdb-clients &> /dev/null ; echo $?) ]]
then
  sudo yum install -y https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients-7.3.3-1.el7.x86_64.rpm
fi

sudo mkdir -p /etc/foundationdb/data /etc/foundationdb/logs
sudo chown -R foundationdb:foundationdb /etc/foundationdb
sudo chmod 777 /etc/foundationdb

sudo modprobe nvme-tcp
sudo modprobe nbd

echo -e "net.ipv6.conf.all.disable_ipv6 = 1\n
net.ipv6.conf.default.disable_ipv6 = 1\n
net.ipv6.conf.lo.disable_ipv6 = 1\n
vm.max_map_count=262144" | sudo tee "/etc/sysctl.d/disable_ipv6.conf" > /dev/null
sudo sysctl --system


sudo mkdir -p /etc/simplyblock
sudo chmod 777 /etc/simplyblock

sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'

sudo sed -i 's/ProcessSizeMax=.*/ProcessSizeMax=5G/' /etc/systemd/coredump.conf
