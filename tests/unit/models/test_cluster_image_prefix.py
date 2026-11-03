import pytest

from simplyblock_core.models.cluster import Cluster, prefix_image

PREFIX = "quay-proxy.grupawp.pl/docker.io"


def _cluster(prefix):
    cluster = Cluster()
    cluster.container_image_prefix = prefix
    return cluster


@pytest.mark.parametrize("prefix", ["", "  "])
def test_no_prefix_leaves_image_unchanged(prefix):
    assert _cluster(prefix).prefixed_image("simplyblock/simplyblock:main") == "simplyblock/simplyblock:main"


@pytest.mark.parametrize("prefix", [PREFIX, f"{PREFIX}/"])
def test_prefix_joined_with_one_slash(prefix):
    assert (_cluster(prefix).prefixed_image("simplyblock/simplyblock:main")
            == f"{PREFIX}/simplyblock/simplyblock:main")


def test_prefix_is_idempotent():
    # Restart re-applies the prefix to the image stored on the node record.
    cluster = _cluster(PREFIX)
    once = cluster.prefixed_image("public.ecr.aws/simply-block/ultra:main-latest")
    assert cluster.prefixed_image(once) == once


def test_empty_image_stays_empty():
    assert _cluster(PREFIX).prefixed_image("") == ""


def test_prefix_round_trips_through_dict():
    restored = Cluster().from_dict(_cluster(PREFIX).to_dict())
    assert restored.container_image_prefix == PREFIX


@pytest.mark.parametrize("prefix", [None, ""])
def test_prefix_image_without_cluster_accepts_missing_prefix(prefix):
    # `sn deploy` runs on a host with no cluster record and passes the CLI value.
    assert prefix_image(prefix, "simplyblock/simplyblock:main") == "simplyblock/simplyblock:main"


def test_prefix_image_matches_cluster_method():
    assert (prefix_image(PREFIX, "simplyblock/simplyblock:main")
            == _cluster(PREFIX).prefixed_image("simplyblock/simplyblock:main"))
