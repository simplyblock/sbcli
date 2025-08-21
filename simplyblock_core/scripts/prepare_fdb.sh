#!/usr/bin/env bash

cd /tmp/
rm -rf etc/
unzip -q /fdb.zip
cp -rf etc/foundationdb/data /var/fdb/
cp -rf etc/foundationdb/logs /var/fdb/

export FDB_CLUSTER_FILE_CONTENTS=$(cat /tmp/etc/foundationdb/fdb.cluster | awk '{split($0,a,"@"); print a[1]}')@$(hostname -i):4500
echo $FDB_CLUSTER_FILE_CONTENTS > /etc/foundationdb/fdb.cluster

/var/fdb/scripts/fdb.bash
