#!/usr/bin/env bash
# Storage-node image for the replication service-class benchmark.
#
# Runs on a build box with docker and AWS credentials. It layers the spdk change
# (bdev_lvol_transfer priority_class, carried in a git bundle on top of
# new_md_journal) onto the published spdk-core:new_md_journal-latest, then
# builds ultra main on that, exactly as ultra's own Dockerfile_spdk_ultra does
# with the published base, and pushes the result to ECR.
#
#   build_images.sh <spdk-bundle> <spdk-sha> <ultra.tgz> <ecr-repo-uri> <tag>
#
# The bundle is `git bundle create x.bundle origin/new_md_journal..<branch>`,
# ultra.tgz is `git -C ultra archive --format=tar.gz origin/main` (ultra is
# private, so the box is not asked to clone it). The bundle
# only applies when the base image's spdk is the bundle's base commit; the
# script stops if it is not, rather than building a mix.
set -euo pipefail
BUNDLE=$1 SHA=$2 ULTRA_TGZ=$3 REPO=$4 TAG=$5
BASE=simplyblock/spdk-core:new_md_journal-latest
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT

docker pull -q "$BASE"
cp "$BUNDLE" "$W/spdk.bundle"
cat > "$W/Dockerfile.spdk" <<EOF
FROM $BASE
COPY spdk.bundle /tmp/spdk.bundle
RUN cd /root/spdk && git log -1 --format=%H > /tmp/base && \\
    git bundle verify /tmp/spdk.bundle && \\
    git fetch /tmp/spdk.bundle '+refs/heads/*:refs/remotes/bundle/*' && git checkout -q $SHA && \\
    make --no-builtin-rules -j"\$(nproc)" && git log -1 --oneline
EOF
docker build -q -t "bench/spdk-core:$TAG" -f "$W/Dockerfile.spdk" "$W"

mkdir "$W/ultra" && tar xzf "$ULTRA_TGZ" -C "$W/ultra"
# Dockerfile_spdk_ultra records `git log` of the tree it builds; an archive has
# no history, so give it a one-commit repository
git -C "$W/ultra" init -q && git -C "$W/ultra" add -A &&
    git -C "$W/ultra" -c user.name=bench -c user.email=bench@localhost commit -qm "ultra from $(basename "$ULTRA_TGZ")"
sed -i "s#^FROM simplyblock/spdk-core:new_md_journal-latest#FROM bench/spdk-core:$TAG#" \
    "$W/ultra/docker/Dockerfile_spdk_ultra"
grep -q "^FROM bench/spdk-core:$TAG" "$W/ultra/docker/Dockerfile_spdk_ultra"
docker build -q -t "$REPO:$TAG" -f "$W/ultra/docker/Dockerfile_spdk_ultra" "$W/ultra"

REGISTRY=${REPO%%/*}
aws ecr get-login-password --region "$(cut -d. -f4 <<<"$REGISTRY")" |
    docker login --username AWS --password-stdin "$REGISTRY"
docker push -q "$REPO:$TAG"
echo "IMAGE $REPO:$TAG"
