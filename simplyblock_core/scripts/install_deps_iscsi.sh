#!/usr/bin/env bash

sudo yum install -y device-mapper-multipath iscsi-initiator-utils

/sbin/mpathconf --enable

sudo systemctl enable multipathd
sudo systemctl start multipathd
sudo systemctl status multipathd

sudo systemctl enable iscsid
sudo systemctl start iscsid
sudo systemctl status iscsid

/sbin/mpathconf --enable
