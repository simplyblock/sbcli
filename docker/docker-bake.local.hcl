# docker-bake.local.hcl -- developer override, layered on top of the shared
# definition:
#
#   docker buildx bake -f docker/docker-bake.hcl -f docker/docker-bake.local.hcl \
#       controlplane --load
#
# It narrows the build to the host platform, because --load cannot load a
# multi-platform result, and retags to something that cannot be mistaken for a
# published image. cache-to is already empty in docker-bake.hcl, so a laptop reads
# the shared cache but never writes to it.

target "_common" {
  platforms = []  # builder default: the host platform only
}

target "controlplane" {
  platforms = []
  tags      = ["simplyblock/simplyblock:dev"]
}
