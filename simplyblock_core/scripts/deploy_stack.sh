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
# monitoring | fdb | app -- run in that order, with the database configured
# (db_config_single.sh) between fdb and app. See cluster_ops.create_cluster.
PHASE=${12}
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

set -e

# The fdbserver processes the configure step needs. fdb-backup-agent is left
# out: it is a database client, so it is waited for in the app phase.
FDB_SERVERS="fdb-server fdb-server1 fdb-server2 fdb-server3 fdb-server4"

case "$PHASE" in
  monitoring)
    docker network inspect monitoring-net >/dev/null 2>&1 || \
       docker network create monitoring-net -d overlay --attachable
    if [[ "${DISABLE_MONITORING,,}" == "false" ]]; then
       docker stack deploy --compose-file="$DIR"/docker-compose-swarm-monitoring.yml monitoring
       bash "$DIR"/stack_deploy_wait.sh monitoring
    fi
    ;;
  fdb)
    # Only the FoundationDB services: nothing in the stack needs the database yet.
    docker stack deploy --compose-file="$DIR"/docker-compose-swarm-fdb.yml app
    WAIT_ARGS=""
    for name in $FDB_SERVERS; do WAIT_ARGS="$WAIT_ARGS -n $name"; done
    bash "$DIR"/stack_deploy_wait.sh $WAIT_ARGS app
    ;;
  app)
    # Requires a configured database: every simplyblock service exits on an
    # unconfigured one, and the wait below would then never converge.
    docker stack deploy --compose-file="$DIR"/docker-compose-swarm-fdb.yml \
       --compose-file="$DIR"/docker-compose-swarm.yml app
    bash "$DIR"/stack_deploy_wait.sh app
    ;;
  *)
    echo "Unknown deploy phase: '$PHASE' (expected monitoring, fdb or app)" >&2
    exit 1
    ;;
esac
