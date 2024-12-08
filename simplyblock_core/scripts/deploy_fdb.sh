#!/usr/bin/env bash

#sudo bash -x deploy.sh $PWD/fdb.zip

export FDB_FILE=$1
docker stack deploy --compose-file=foundation.yaml app
docker service logs app_fdb-server -f

