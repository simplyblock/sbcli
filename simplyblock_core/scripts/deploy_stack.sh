#!/usr/bin/env bash

export CLI_SSH_PASS=$1
export CLUSTER_IP=$2
export SIMPLYBLOCK_DOCKER_IMAGE=$3

export DIR="$(dirname "$(realpath "$0")")"

if [ -s "/etc/foundationdb/fdb.cluster" ]
then
   FDB_CLUSTER_FILE_CONTENTS=$(tail /etc/foundationdb/fdb.cluster -n 1)
   export FDB_CLUSTER_FILE_CONTENTS=$FDB_CLUSTER_FILE_CONTENTS
fi
docker stack deploy --compose-file="$DIR"/docker-compose-swarm.yml app

# wait for the services to become online
bash "$DIR"/stack_deploy_wait.sh app
