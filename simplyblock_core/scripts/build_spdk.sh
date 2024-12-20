#!/usr/bin/env bash


if [[ "1" == "$(gh auth status ; echo $?)" ]]
then
  gh auth login
fi

gh repo clone simplyblock-io/ultra
cd ultra/testing/distrib_integration/
sudo bash -x spdk_deploy.sh --force -c 0xE -m 4000 # -c the cpu mask for spdk, -m the hugepages memory

systemctl status spdk
