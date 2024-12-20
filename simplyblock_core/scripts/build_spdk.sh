#!/usr/bin/env bash

gh auth login

gh repo clone simplyblock-io/ultra

sudo bash -x spdk_deploy.sh --force -c 0xE -m 4000 # -c the cpu mask for spdk, -m the hugepages memory

systemctl status spdk
