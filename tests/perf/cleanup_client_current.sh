#!/usr/bin/env bash
set -u

sudo pkill -f '[f]io --name=aws_dual_soak_' || true
sleep 2

for target in /home/ec2-user/aws_outage_soak_*/vol*; do
  [ -d "$target" ] || continue
  sudo umount -l "$target" || true
done

sudo nvme list-subsys | awk -F'NQN=' '/simplyblock/ {print $2}' | while read -r nqn; do
  [ -n "$nqn" ] || continue
  sudo nvme disconnect -n "$nqn" || true
done

sleep 2
for target in /home/ec2-user/aws_outage_soak_*/vol*; do
  [ -d "$target" ] || continue
  sudo umount -l "$target" || true
done

sudo rm -rf /home/ec2-user/aws_outage_soak_*

pgrep -a fio || true
findmnt | grep aws_outage_soak || true
sudo nvme list-subsys | grep simplyblock || true
