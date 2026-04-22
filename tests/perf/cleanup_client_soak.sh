#!/usr/bin/env bash
set -u

sudo pkill -f '[f]io --name=aws_dual_soak_' || true

for d in /home/ec2-user/aws_outage_soak_*; do
  [ -d "$d" ] || continue
  findmnt -R "$d" -n -o TARGET | sort -r | xargs -r -n1 sudo umount -l
done

for d in \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol1 \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol2 \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol3 \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol4 \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol5 \
  /home/ec2-user/aws_outage_soak_20260407_140808/vol6
do
  sudo umount -l "$d" || true
done

for nqn in \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:6f34e76a-4a45-4c0b-849c-59053f8bdf3e \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:a55da9d3-2f64-4425-aba3-bbd4ea8800c8 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:257b5e4c-92f6-4ac2-9cf9-ab7111433bec \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:dcb043ce-6ef8-4dd7-818f-0b76936007c1 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:0e0967e0-1f4d-4570-aeae-45812292ed01 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:d749d697-26af-4f89-8e60-84390ee8c214
do
  sudo nvme disconnect -n "$nqn" || true
done

sudo rm -rf \
  /home/ec2-user/aws_outage_soak_20260407_124431 \
  /home/ec2-user/aws_outage_soak_20260407_133236 \
  /home/ec2-user/aws_outage_soak_20260407_140549 \
  /home/ec2-user/aws_outage_soak_20260407_140808

findmnt | grep aws_outage_soak || true
sudo nvme list-subsys | grep 7155bd9c || true
