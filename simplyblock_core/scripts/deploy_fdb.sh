#!/usr/bin/env bash

export DIR="$(dirname "$(realpath "$0")")"
export FDB_FILE=$1
docker compose -f $DIR/foundation.yml up -d
docker compose -f $DIR/foundation.yml exec -it cli bash
docker compose -f $DIR/foundation.yml down -v  --remove-orphans
