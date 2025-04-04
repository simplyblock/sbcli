#!/usr/bin/env bash

get_service_ids() {
  docker service ls | grep 'Monitor' | awk '{print $1}'
}

#pip install sbcli-pre --upgrade
#docker image pull simplyblock/simplyblock:new
service_ids=$(get_service_ids)
for service_id in ${service_ids}; do
  docker service rm "$service_id"
done