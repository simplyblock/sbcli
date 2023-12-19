export TD=$1
export CLI_SSH_PASS=$2
export CLUSTER_IP=$3
if [ -s "/etc/foundationdb/fdb.cluster" ]
then
   FDB_CLUSTER_FILE_CONTENTS=$(tail /etc/foundationdb/fdb.cluster -n 1)
   export FDB_CLUSTER_FILE_CONTENTS=$FDB_CLUSTER_FILE_CONTENTS
fi
docker stack deploy --compose-file=$TD/docker-compose-swarm.yml app
