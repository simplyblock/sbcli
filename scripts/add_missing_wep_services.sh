IMAGE="simplyblock/simplyblock:26.3.3.3"

for i in 2 3 4 5; do
  if docker service inspect "app_WebAppAPI$i" >/dev/null 2>&1; then
    echo "app_WebAppAPI$i already exists, skipping"
    continue
  fi

  docker service create \
    --name app_WebAppAPI$i \
    --label com.docker.stack.namespace=app \
    --container-label com.docker.stack.namespace=app \
    --mode global \
    --endpoint-mode dnsrr \
    --constraint 'node.role == manager' \
    --network name=app_localnet,alias=WebAppAPI$i \
    --mount type=bind,source=/etc/foundationdb,target=/etc/foundationdb \
    --env SIMPLYBLOCK_LOG_LEVEL="INFO" \
    --env FLASK_DEBUG=False \
    --env FLASK_ENV=production \
    --env FLASK_PORT=$((4999 + i)) \
    --log-driver gelf \
    --log-opt gelf-address=tcp://127.0.0.1:12202 \
    --log-opt mode=non-blocking \
    --log-opt max-buffer-size=40m \
    --detach \
    "$IMAGE" python3 simplyblock_web/app.py
done
