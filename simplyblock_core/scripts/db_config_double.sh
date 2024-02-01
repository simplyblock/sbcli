sudo fdbcli --exec "configure FORCE double" --timeout 60
sleep 10
sudo fdbcli --exec "coordinators auto" --timeout 60
