#!/usr/bin/env bash

P_NODES=("192.168.10.92" "192.168.10.93" "192.168.10.94")

CMD=$(ls /usr/local/bin/sbcli-* | awk '{n=split($0,a,"/"); print a[n]}')
cl=$($CMD cluster list | tail -n -3 | awk '{print $2}')
$CMD -d cluster add --ha-type ha
$CMD cluster delete $cl
cl=$($CMD cluster list | tail -n -3 | awk '{print $2}')

for sn_id in "${P_NODES[@]}"; do

  $CMD -d --dev sn add-node $cl $sn_id:5000 eth0 --journal-partition 0  --number-of-devices 1 --max-lvol 1  \
      --max-size 1g --data-nics eth1  --vcpu-count 6 --ssd-pcie 0000:00:02.0 0000:00:03.0 0000:00:04.0 0000:00:05.0

#  $CMD -d --dev sn add-node $cl $sn_id:5000 eth0 --journal-partition 0  --number-of-devices 1 --max-lvol 1  \
#      --max-size 1g --data-nics eth1  --vcpu-count 2 --ssd-pcie 0000:00:02.0 0000:00:03.0  --spdk-mem 6g
#
#  $CMD -d --dev sn add-node $cl $sn_id:5000 eth0 --journal-partition 0  --number-of-devices 1 --max-lvol 1  \
#      --max-size 1g --data-nics eth1  --vcpu-count 2 --ssd-pcie 0000:00:04.0 0000:00:05.0 --spdk-mem 6g
done

$CMD -d pool add pool1 $cl
$CMD -d cluster activate $cl
