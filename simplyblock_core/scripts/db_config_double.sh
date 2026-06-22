sudo fdbcli --exec "configure FORCE double" --timeout 60 -C /etc/foundationdb/fdb.cluster
sleep 10
sudo fdbcli --exec "coordinators auto" --timeout 60 -C /etc/foundationdb/fdb.cluster
