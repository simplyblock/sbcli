#!/usr/bin/env bash

export DIR="$(dirname "$(realpath "$0")")"
export FDB_FILE=$1
docker compose -f $DIR/foundation.yml up -d
docker compose -f foundation.yml exec -it cli bash
