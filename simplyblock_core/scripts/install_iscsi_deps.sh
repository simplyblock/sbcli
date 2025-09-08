#!/usr/bin/env bash

sudo yum install -y iscsi-initiator-utils

sudo systemctl enable iscsid
sudo systemctl start iscsid
sudo systemctl status iscsid
