#!/usr/bin/env bash

export DIR="$(dirname "$(realpath "$0")")"
export FDB_FILE=$1
docker compose  -f ./foundation.yml down -v  --remove-orphans
