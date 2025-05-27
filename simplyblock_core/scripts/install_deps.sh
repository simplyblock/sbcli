#!/usr/bin/env bash

sudo yum install -y yum-utils
sudo yum install -y https://repo.almalinux.org/almalinux/9/devel/aarch64/os/Packages/tuned-profiles-realtime-2.24.0-1.el9.noarch.rpm
sudo yum install -y yum-utils xorg-x11-xauth nvme-cli fio tuned

sudo yum install hostname pkg-config git wget python3-pip yum-utils \
  iptables pciutils -y

if [[ "$1" == "docker" ]]; then
  sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
  sudo yum install docker-ce docker-ce-cli \
    containerd.io docker-buildx-plugin docker-compose-plugin -y

  sudo systemctl enable docker
  sudo systemctl start docker

elif [[ "$1" == "kubernetes" ]]; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 777 get_helm.sh
  ./get_helm.sh
  rm -rf ./get_helm.sh

  NODE_NAME=$(hostname)

  /usr/local/bin/kubectl label nodes "$NODE_NAME" type=simplyblock-mgmt-plane --overwrite

  if [ $? -eq 0 ]; then
    echo "Node $NODE_NAME labeled successfully."
  else
    echo "Failed to label node $NODE_NAME."
    exit 1
  fi

fi

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

sudo sed -i 's/ProcessSizeMax=.*/ProcessSizeMax=10G/' /etc/systemd/coredump.conf
