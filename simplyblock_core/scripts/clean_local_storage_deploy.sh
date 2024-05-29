#!/usr/bin/env bash

sudo docker rm spdk --force
sudo docker rm spdk_proxy --force
sudo docker rm SNodeAPI --force
sudo docker rm CachingNodeAPI --force
sudo docker stack rm app
sudo docker swarm leave --force
sudo docker container prune -f
sudo docker image prune -a -f
sudo docker volume prune -a -f
sudo service docker restart
echo "Done"
