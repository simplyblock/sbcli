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
' > /etc/docker/daemon.json
fi


if [[ -z $(grep "tcp://${DEV_IP}:2375" /usr/lib/systemd/system/docker.service) ]]
then
  set_config ExecStart "/usr/bin/dockerd --containerd=/run/containerd/containerd.sock -H tcp://${DEV_IP}:2375 -H unix:///var/run/docker.sock -H fd://" /usr/lib/systemd/system/docker.service
  sudo systemctl daemon-reload
  sudo systemctl restart docker
  sleep 3
fi
