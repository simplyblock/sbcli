#!/usr/bin/env bash
set -u
CN=$(sudo docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$' | head -1)
[ -n "$CN" ] || { echo "FAIL: no spdk_* container"; exit 1; }
echo "container:        $CN"
echo "container image:  $(sudo docker inspect --format '{{.Config.Image}}' "$CN")"
echo "image digest:     $(sudo docker inspect --format '{{.Image}}' "$CN")"
echo "--- ultra images on this node ---"
sudo docker images --digests --format '{{.Repository}}:{{.Tag}} {{.Digest}} age={{.CreatedSince}}' | grep -i ultra || echo "  (none)"
echo "--- binary identity inside container ---"
sudo docker cp /tmp/probe_bin.sh "$CN":/tmp/probe_bin.sh 2>/dev/null
sudo docker exec -u root "$CN" bash /tmp/probe_bin.sh
