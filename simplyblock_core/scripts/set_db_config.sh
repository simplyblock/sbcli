echo $1 | sudo tee /var/fdb/fdb.cluster > /dev/null
sudo chown -R foundationdb:foundationdb /var/fdb
sudo chmod 777 /var/fdb
