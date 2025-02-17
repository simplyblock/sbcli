#!/usr/bin/env bash

TD=$(dirname -- "$( readlink -f -- "$0"; )")

source simplyblock_core/env_var

docker login -u $DOCKER_USER -p $DOCKER_PASS
docker build --no-cache --push -t $SIMPLY_BLOCK_DOCKER_IMAGE -f $TD/docker/Dockerfile $TD
