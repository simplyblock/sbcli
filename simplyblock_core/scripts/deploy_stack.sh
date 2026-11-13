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
# (scripts.set_db_config_single) between fdb and app. See cluster_ops.create_cluster.
PHASE=${12}
# stack_deploy_wait.sh timeout, set by the caller below its own script timeout
# so this script reports a stuck stack before it is killed.
STACK_WAIT_TIMEOUT=${13}
# File the caller logs at error level if this script fails. It must only ever
# receive text that cannot carry a secret: the arguments above include
# passwords, and `bash -x` traces echo them, so the regular output stays at
# debug level and this file is the readable account of a failure.
REPORT_FILE=${14:-/dev/null}
# Registry/proxy prefix for the third-party images in the compose files, e.g.
# quay-proxy.grupawp.pl/docker.io. Normalised to end in exactly one '/' so the
# compose files can write ${IMAGE_PREFIX}foundationdb/foundationdb:...; empty
# keeps the images as written. $SIMPLYBLOCK_DOCKER_IMAGE arrives already prefixed.
IMAGE_PREFIX=${15:-}
IMAGE_PREFIX=${IMAGE_PREFIX%/}
if [[ -n "$IMAGE_PREFIX" ]]; then
   IMAGE_PREFIX="$IMAGE_PREFIX/"
fi
export IMAGE_PREFIX
export DIR="$(dirname "$(realpath "$0")")"

report() {
   echo "$*" | tee -a "$REPORT_FILE"
}

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
    report "Invalid LOG_DELETION_INTERVAL format. Please use a value ending in 'd', 'h', or 'm'."
    exit 1
fi

set -e -o pipefail
# BASH_COMMAND is the command as written, before expansion: no secret values.
trap 'report "Phase $PHASE: command failed: $BASH_COMMAND"' ERR

if ! [[ "$STACK_WAIT_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
   report "Invalid stack wait timeout: '$STACK_WAIT_TIMEOUT'"
   exit 1
fi

# wait_stack [stack_deploy_wait.sh options] <stack>
# On failure, show which services did not converge and why their tasks died:
# the wait script itself only reports the last state it saw.
wait_stack() {
   local stack="${!#}"
   if ! bash "$DIR"/stack_deploy_wait.sh -t "$STACK_WAIT_TIMEOUT" "$@" 2>&1 | tee -a "$REPORT_FILE"; then
      {
         echo "Stack '$stack' did not converge within ${STACK_WAIT_TIMEOUT}s. Services:"
         docker stack services --format '{{.Name}} {{.Replicas}}' "$stack" || true
         echo "Failed tasks:"
         docker stack ps --no-trunc --filter desired-state=shutdown \
            --format '{{.Name}} {{.CurrentState}} {{.Error}}' "$stack" | head -n 50 || true
      } 2>&1 | tee -a "$REPORT_FILE"
      exit 1
   fi
}

# The fdbserver processes the configure step needs. fdb-backup-agent is left
# out: it is a database client, so it is waited for in the app phase.
FDB_SERVERS="fdb-server fdb-server1 fdb-server2 fdb-server3 fdb-server4"

case "$PHASE" in
  monitoring)
    docker network inspect monitoring-net >/dev/null 2>&1 || \
       docker network create monitoring-net -d overlay --attachable
    if [[ "${DISABLE_MONITORING,,}" == "false" ]]; then
       docker stack deploy --compose-file="$DIR"/docker-compose-swarm-monitoring.yml monitoring
       wait_stack monitoring
    fi
    ;;
  fdb)
    # FoundationDB and its log sink: nothing deployed here needs the database.
    docker stack deploy --compose-file="$DIR"/docker-compose-swarm-fdb.yml app
    # HAProxy first: it is the gelf endpoint every container here logs to,
    # and docker will not create a container whose log driver cannot connect.
    wait_stack -n HAProxy app
    WAIT_ARGS=""
    for name in $FDB_SERVERS; do WAIT_ARGS="$WAIT_ARGS -n $name"; done
    wait_stack $WAIT_ARGS app
    ;;
  app)
    # Requires a configured database: every simplyblock service exits on an
    # unconfigured one, and the wait below would then never converge.
    docker stack deploy --compose-file="$DIR"/docker-compose-swarm-fdb.yml \
       --compose-file="$DIR"/docker-compose-swarm.yml app
    wait_stack app
    ;;
  *)
    report "Unknown deploy phase: '$PHASE' (expected monitoring, fdb or app)"
    exit 1
    ;;
esac
