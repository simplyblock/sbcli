#!/usr/bin/env bash


if [ -d "/tmp/ultra" ]; then
  echo "skip clone"
else
  if [[ "1" == "$(gh auth status > /dev/null ; echo $?)" ]]; then
    gh auth login
  fi
  gh repo clone simplyblock-io/ultra /tmp/ultra
fi

cd /tmp/ultra/testing/distrib_integration/
sudo bash -x spdk_deploy.sh --force -c 0xE -m 4000 # -c the cpu mask for spdk, -m the hugepages memory

systemctl status spdk
