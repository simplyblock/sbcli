#!/usr/bin/env bash

if [[ "$1" == "docker" ]]; then

  # AWS RHUI (rhui.<region>.aws.ce.redhat.com) intermittently throttles us-east-1
  # egress, stalling RHUI metadata downloads so every yum/dnf call hangs (not a
  # clean "no package" error -- a multi-minute hang). If the RHUI repos are
  # unreachable, disable them and install from Rocky Linux 9's public mirrors
  # instead (RHEL9-ABI-compatible). The probe is a no-op when RHUI is healthy.
  if ! sudo timeout 25 dnf -q --disablerepo='*' --enablerepo='*rhui*' makecache >/dev/null 2>&1; then
    echo "install_deps: RHUI repos unreachable, falling back to Rocky 9 mirrors"
    sudo sh -c 'grep -rIl rhui /etc/yum.repos.d/ 2>/dev/null | xargs -r sed -i "s/^enabled[[:space:]]*=[[:space:]]*1/enabled=0/g"'
    sudo tee /etc/yum.repos.d/rocky-fallback.repo >/dev/null <<'ROCKYEOF'
[rocky-baseos]
name=Rocky Linux 9 BaseOS (RHUI fallback)
baseurl=https://dl.rockylinux.org/pub/rocky/9/BaseOS/x86_64/os/
gpgcheck=0
enabled=1

[rocky-appstream]
name=Rocky Linux 9 AppStream (RHUI fallback)
baseurl=https://dl.rockylinux.org/pub/rocky/9/AppStream/x86_64/os/
gpgcheck=0
enabled=1
ROCKYEOF
  fi

  sudo yum install -y yum-utils
  sudo yum install -y https://repo.almalinux.org/almalinux/9/devel/aarch64/os/Packages/tuned-profiles-realtime-2.26.0-1.el9.noarch.rpm
  sudo yum install -y yum-utils xorg-x11-xauth nvme-cli fio tuned

  sudo yum install hostname pkg-config git wget python3-pip yum-utils \
    iptables pciutils -y

    sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    sudo yum install docker-ce-29.1.3-1.el9 docker-ce-cli-29.1.3-1.el9 \
      containerd.io-2.2.0-2.el9 docker-buildx-plugin-0.30.1-1.el9 docker-compose-plugin-5.0.1-1.el9 -y

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

  sudo sed -i 's/^#\?\s*ProcessSizeMax=.*/ProcessSizeMax=10G/' /etc/systemd/coredump.conf

fi