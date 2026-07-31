# docker-bake.hcl -- the single source of truth for every image this repository
# publishes: targets, tags, platforms, build args and cache policy. CI
# (.github/workflows/build-images.yml, security.yml) and developers drive builds
# through this file, so a local bake produces exactly the references CI pushes.
#
#   docker buildx bake -f docker/docker-bake.hcl --print          # resolve, don't build
#   docker buildx bake -f docker/docker-bake.hcl --check           # lint the Dockerfiles
#   docker buildx bake -f docker/docker-bake.hcl \
#       -f docker/docker-bake.local.hcl controlplane --load        # local build
#   TAG=mybranch docker buildx bake -f docker/docker-bake.hcl      # env overrides any variable
#
# Bake binds environment variables to like-named `variable` blocks, which is why
# CI exports TAG/SHORT_SHA/... rather than passing them on the command line.
# Relative paths (context, dockerfile) resolve against the directory bake is
# invoked from -- always the repository root.

# CACHE_KEY is a truncated timestamp bounding how long the expensive `base-os`
# stage may be reused before it is evicted and rebuilt against current RHEL
# errata. It replaces the nightly rebuild of the
# `simplyblock/simplyblock:base_image` tag; computing it here rather than
# requiring it to be passed means a local bake gets the same eviction behaviour as
# CI with nothing to plumb. Force a cold base-os with
# `CACHE_KEY=$(date +%s) docker buildx bake ...`.
#
# Weekly bucket. formatdate has no ISO-week token, so the day of month is bucketed
# into 7-day windows; the YYYY-MM prefix keeps the value monotonic across month
# boundaries. Example: "2026-07-4".
#
# NOTE: timestamp() is evaluated per bake invocation, so a matrix build whose legs
# straddle a bucket boundary would build its architectures from different package
# sets. build-images.yml therefore computes this once in its `prepare` job and
# passes the same value to every leg; this default serves local builds, where a
# single invocation makes it safe.
function "weekstamp" {
  params = []
  result = "${formatdate("YYYY-MM", timestamp())}-${floor((parseint(formatdate("DD", timestamp()), 10) - 1) / 7)}"
}

variable "CACHE_KEY" { default = weekstamp() }

# Image reference components. CI exports these; the defaults make a local bake
# produce obviously-local names.
variable "TAG"       { default = "dev" }
variable "SHORT_SHA" { default = "local" }
variable "VCS_REF"   { default = "unknown" }

# Registry prefixes to publish under, including the org path. Unqualified for
# docker.io so the produced references are byte-identical to the ones the workflows
# this replaces pushed. Add quay with
# `REGISTRIES=simplyblock,public.ecr.aws/simply-block,quay.io/simplyblock-io`.
variable "REGISTRIES" { default = "simplyblock,public.ecr.aws/simply-block" }

# Registry-backed layer cache. cache-from is on by default so a developer's first
# build is not a cold one; cache-to is empty by default so a laptop can never
# poison it. Only the scheduled warm-up and pushes to main write it.
#
# The cache is per-architecture: two concurrent matrix legs writing one ref would
# race and clobber each other. CACHE_TAG defaults to the arch most developers
# build on -- override with CACHE_TAG=main-arm64 on an arm workstation.
variable "CACHE_REF" { default = "simplyblock/simplyblock-buildcache" }
variable "CACHE_TAG" { default = "main-amd64" }

variable "PLATFORMS" { default = "linux/amd64,linux/arm64" }

# This file covers the control-plane image only. docker/Dockerfile.alpine-tools
# and docker/Dockerfile.helm-deployer are unrelated images -- different base,
# different package manager, no shared stages, no shared cache -- and keep their
# own manual workflows. Their build cost was never the problem this rework solves.

group "default" {
  targets = ["controlplane"]
}

target "_common" {
  context    = "."
  platforms  = split(",", PLATFORMS)
  cache-from = ["type=registry,ref=${CACHE_REF}:${CACHE_TAG}"]
  cache-to   = []
}

# The control-plane image. Tags are enumerated here even though CI's matrix legs
# push by digest with tags cleared: the merge job reads this list back out
# (`--print | jq -r '.target.controlplane.tags[]'`), so tags are still defined in
# exactly one place, and security.yml plus local builds consume them directly.
target "controlplane" {
  inherits   = ["_common"]
  dockerfile = "docker/Dockerfile"
  target     = "runtime"
  args = {
    CACHE_KEY = CACHE_KEY
    VCS_REF   = VCS_REF
  }
  tags = flatten([
    for registry in split(",", REGISTRIES) : [
      "${registry}/simplyblock:${TAG}",
      "${registry}/simplyblock:${TAG}-${SHORT_SHA}",
    ]
  ])
}

# Single-platform, locally loadable build for the security scan. Same stage and
# same build args as what gets published, so the scan reports on the artifact that
# would actually ship -- only the tag and the platform differ.
target "scan" {
  inherits  = ["controlplane"]
  tags      = ["simplyblock/simplyblock:scan"]
  platforms = ["linux/amd64"]
}
