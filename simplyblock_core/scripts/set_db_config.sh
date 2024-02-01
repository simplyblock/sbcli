echo $1 | sudo tee /etc/foundationdb/fdb.cluster > /dev/null
sudo chown -R foundationdb:foundationdb /etc/foundationdb
sudo chmod 777 /etc/foundationdb
