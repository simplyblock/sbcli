#!/usr/bin/env bash

function set_config() {
  sudo sed -i "s#\($1 *= *\).*#\1$2#" $3
}


sudo yum update -y
sudo yum install -y yum-utils xorg-x11-xauth nvme-cli fio
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install hostname pkg-config git wget python3-pip yum-utils docker-ce docker-ce-cli \
  containerd.io docker-buildx-plugin docker-compose-plugin -y
#sudo pip install -r $TD/api/requirements.txt

sudo systemctl enable docker
sudo systemctl start docker

sudo docker plugin install rexray/efs --grant-all-permissions EFS_TAG=$INSTANCE_ID REXRAY_PREEMPT=true

wget https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients-7.3.3-1.el7.x86_64.rpm -q
sudo rpm -U foundationdb-clients-7.3.3-1.el7.x86_64.rpm --quiet --reinstall
rm -f foundationdb-clients-7.3.3-1.el7.x86_64.rpm

sudo mkdir -p /etc/foundationdb/data /etc/foundationdb/logs
sudo chown -R foundationdb:foundationdb /etc/foundationdb
sudo chmod 777 /etc/foundationdb

sudo sed -i 's/#X11Forwarding no/X11Forwarding yes/g' /etc/ssh/sshd_config
sudo sed -i 's/#X11DisplayOffset 10/X11DisplayOffset 10/g' /etc/ssh/sshd_config
sudo sed -i 's/#X11UseLocalhost yes/X11UseLocalhost no/g' /etc/ssh/sshd_config

sudo service sshd restart
sudo modprobe nvme-tcp
sudo modprobe nbd

sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1

# required for graylog
sudo sysctl -w vm.max_map_count=262144

sudo mkdir -p /etc/simplyblock
sudo chmod 777 /etc/simplyblock

sudo sh -c 'echo 1 >  /proc/sys/vm/drop_caches'