## Create database workloads

add a new database
```
sbcli-dev -d database add --type postgresql --version 15.13 --vcpu_count 2 --memory_size 4G --disk_size 100G --storage_class  simplyblock-csi-sc postgres1
```

