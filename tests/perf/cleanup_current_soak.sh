#!/usr/bin/env bash
set -u

sudo pkill -f '[f]io --name=aws_dual_soak_' || true

for d in /home/ec2-user/aws_outage_soak_20260407_150518/*; do
  [ -d "$d" ] || continue
  sudo umount -l "$d" || true
done

for nqn in \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:ede39fee-f871-4c40-b006-ad8ed6007184 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:f2fc7b0f-45c9-4ed8-9e6e-c1be06fbbcc6 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:65c3b8aa-c101-4753-ac0f-48f16c39bc95 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:6ed27110-336e-45e1-8a55-8bb5307ce7b7 \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:b8c78d89-763e-4a0c-baa2-ae03c1c5482c \
  nqn.2023-02.io.simplyblock:7155bd9c-3bb9-48ce-b210-c027b0ce9c9d:lvol:b4a9c544-1ca1-41a4-bdaf-4146b2e4b45b
do
  sudo nvme disconnect -n "$nqn" || true
done

sudo rm -rf /home/ec2-user/aws_outage_soak_20260407_150518
findmnt | grep aws_outage_soak || true
sudo nvme list-subsys | grep 7155bd9c || true
