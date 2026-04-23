#!/usr/bin/env bash
set -euo pipefail

IMAGE="public.ecr.aws/simply-block/simplyblock:test_FTT2"

mapfile -t services < <(
  sudo docker service ls --format '{{.Name}} {{.Image}}' |
    awk -v image="$IMAGE" '$2 == image {print $1}'
)

if [[ ${#services[@]} -eq 0 ]]; then
  echo "No services found for $IMAGE"
  exit 1
fi

printf 'Updating %d services using %s\n' "${#services[@]}" "$IMAGE"
for service in "${services[@]}"; do
  echo "update $service"
  sudo docker service update --force --image "$IMAGE" "$service" >/dev/null
done

echo "Updated services:"
printf '%s\n' "${services[@]}"
