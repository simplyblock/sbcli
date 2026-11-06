#!/usr/bin/env bash

export CLI_SSH_PASS=$1
export CLUSTER_IP=$2
export SIMPLYBLOCK_DOCKER_IMAGE=$3

export GRAYLOG_ROOT_PASSWORD_SHA2=$4
export GRAYLOG_PASSWORD_SECRET="is6SP2EdWg0NdmVGv6CEp5hRHNL7BKVMFem4t9pouMqDQnHwXMSomas1qcbKSt5yISr8eBHv4Y7Dbswhyz84Ut0TW6kqsiPs"

export CLUSTER_SECRET=$5
export CLUSTER_ID=$6
export LOG_DELETION_INTERVAL=$7
export RETENTION_PERIOD=$8
export LOG_LEVEL=$9
export GRAFANA_ENDPOINT=${10}
export DISABLE_MONITORING=${11}
export DIR="$(dirname "$(realpath "$0")")"

if [ -s "/etc/foundationdb/fdb.cluster" ]
then
   FDB_CLUSTER_FILE_CONTENTS=$(tail /etc/foundationdb/fdb.cluster -n 1)
   export FDB_CLUSTER_FILE_CONTENTS=$FDB_CLUSTER_FILE_CONTENTS
fi

if [[ "$LOG_DELETION_INTERVAL" == *d ]]; then
   export MAX_NUMBER_OF_INDICES=${LOG_DELETION_INTERVAL%d}
elif [[ "$LOG_DELETION_INTERVAL" == *h || "$LOG_DELETION_INTERVAL" == *m ]]; then
   export MAX_NUMBER_OF_INDICES=1
else
    echo "Invalid LOG_DELETION_INTERVAL format. Please use a value ending in 'd', 'h', or 'm'."
    exit 1
fi

docker network create monitoring-net -d overlay --attachable

if [[ "${DISABLE_MONITORING,,}" == "false" ]]; then
   docker stack deploy --compose-file="$DIR"/docker-compose-swarm-monitoring.yml monitoring
fi

# wait for the services to become online
bash "$DIR"/stack_deploy_wait.sh monitoring

docker stack deploy --compose-file="$DIR"/docker-compose-swarm.yml app

# The database has to be configured before the rest of the app stack can
# converge. Every control-plane service reads the cluster list on startup and
# exits 1 on FDBError 1031 until a database exists, so swarm restarts it and
# its replica count oscillates. Waiting for the whole stack first therefore
# cannot finish: the wait needs the database that the step after it creates.
# That deadlock used to be survivable by luck -- the poll occasionally caught
# every service up at once, which took 420s of the caller's 720s budget on
# 2026-11-03 -- until two more services made it miss every time.
#
# So wait for the FDB processes alone, configure the database, and only then
# wait for the services that need it. cluster_ops.create_cluster still calls
# db_config_single.sh after this returns; it is a no-op once configured.
bash "$DIR"/stack_deploy_wait.sh \
    -n fdb-server -n fdb-server1 -n fdb-server2 -n fdb-server3 -n fdb-server4 app
bash "$DIR"/db_config_single.sh

# wait for the services to become online
bash "$DIR"/stack_deploy_wait.sh app
exit $?
