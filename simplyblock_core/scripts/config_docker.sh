#!/usr/bin/env bash

function create_override() {
override_dir=/etc/systemd/system/docker.service.d
sudo mkdir -p ${override_dir}
sudo tee ${override_dir}/override.conf > /dev/null <<EOM
[Service]
ExecStart=
ExecStart=-/usr/bin/dockerd --containerd=/run/containerd/containerd.sock -H tcp://${1}:2375 -H unix:///var/run/docker.sock -H fd://
EOM
}

DEV_IP=$1

if [ ! -s "/etc/docker/daemon.json" ]
then
  echo '{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "30m",
    "max-file": "3"
  }
}
' | sudo tee /etc/docker/daemon.json > /dev/null
fi

# configure node hostname resolve to mgmt ip
if [ "$(hostname -i)" !=  "$DEV_IP" ]
then
  sudo mv /etc/hosts /etc/hosts.back
  sudo sh -c "echo \"$DEV_IP   $(hostname)
127.0.0.1   localhost localhost.localdomain
\" > /etc/hosts "
fi

# Always create file to ensure content is correct
create_override ${DEV_IP}
sudo systemctl daemon-reload
sudo systemctl restart docker

activate-global-python-argcomplete --user
if [ ! -s "$HOME/.bashrc" ] ||  [ -z "$(grep "source $HOME/.bash_completion" $HOME/.bashrc)" ]
then
  echo -e "\nsource $HOME/.bash_completion\n" >> $HOME/.bashrc
fi
