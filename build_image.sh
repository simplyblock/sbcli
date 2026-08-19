#!/usr/bin/env bash
set -euo pipefail

TD=$(dirname -- "$( readlink -f -- "$0"; )")

source "${TD}/simplyblock_core/env_var"

PLATFORMS=${PLATFORMS:-linux/amd64,linux/arm64}

# Matches what CI passes, so a local build lands on the same OS-package layers
# CI has already cached. Override to force those layers to be rebuilt.
CACHE_KEY=${CACHE_KEY:-$(date -u +%G-W%V)}

docker login -u "${DOCKER_USER}" -p "${DOCKER_PASS}"
docker buildx build \
    --platform "${PLATFORMS}" \
    --build-arg "CACHE_KEY=${CACHE_KEY}" \
    --tag "${SIMPLY_BLOCK_DOCKER_IMAGE}" \
    --file "${TD}/docker/Dockerfile" \
    --push \
    "${TD}"
