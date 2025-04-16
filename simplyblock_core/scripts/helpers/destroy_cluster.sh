#!/usr/bin/env bash

# destroy cluster

CMD=$(ls /usr/local/bin/sbcli-* | awk '{n=split($0,a,"/"); print a[n]}')
cl=$($CMD cluster list | tail -n -3 | awk '{print $2}')

for sn_id in $($CMD lvol list | grep line | awk '{print $2}'); do
  $CMD -d lvol delete $sn_id --force
done
for sn_id in $($CMD sn list | grep online | awk '{print $2}'); do
  $CMD -d sn shutdown --force $sn_id
done
for task_id in $($CMD -d cluster list-tasks $cl | grep -i mig | grep -v done  | awk '{print $2}'); do
  $CMD cluster cancel-task $task_id
  $CMD cluster cancel-task $task_id
done
for sn_id in $($CMD sn list | grep / | awk '{print $2}'); do
  $CMD sn remove $sn_id --force-remove --force
done
for task_id in $($CMD -d cluster list-tasks $cl | grep -i mig | grep -v done  | awk '{print $2}'); do
  $CMD cluster cancel-task $task_id
  $CMD cluster cancel-task $task_id
done
for sn_id in $($CMD sn list | grep / | awk '{print $2}'); do
  $CMD sn delete $sn_id
done

for sn_id in $($CMD pool list | grep active | awk '{print $2}'); do
  $CMD pool delete $sn_id
done