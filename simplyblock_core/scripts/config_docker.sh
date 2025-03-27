#!/usr/bin/env bash

function set_config() {
  sudo sed -i "s#\($1 *= *\).*#\1$2#" $3
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

if [[ -z $(grep "tcp://${DEV_IP}:2375" /usr/lib/systemd/system/docker.service) ]]
then
  set_config ExecStart "/usr/bin/dockerd --containerd=/run/containerd/containerd.sock -H tcp://${DEV_IP}:2375 -H unix:///var/run/docker.sock -H fd://" /usr/lib/systemd/system/docker.service
  sudo systemctl daemon-reload
  sudo systemctl restart docker
fi

activate-global-python-argcomplete --user
if [ ! -s "$HOME/.bashrc" ] ||  [ -z "$(grep "source $HOME/.bash_completion" $HOME/.bashrc)" ]
then
  echo -e "\nsource $HOME/.bash_completion\n" >> $HOME/.bashrc
fi
