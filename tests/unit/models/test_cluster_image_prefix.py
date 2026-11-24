import pytest

from simplyblock_core import cluster_ops
from simplyblock_core.models.cluster import (
    Cluster,
    normalize_image_prefix,
    prefix_image,
)

PREFIX = "registry-1.docker.io"


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


@pytest.mark.parametrize("raw", [PREFIX, f"{PREFIX}/", f"{PREFIX}//", f"  {PREFIX}/ "])
def test_normalize_gives_one_spelling(raw):
    # deploy_stack.sh appends the '/' the compose files rely on, and strips at
    # most one; anything else it is handed ends up inside an image name.
    assert normalize_image_prefix(raw) == PREFIX


@pytest.mark.parametrize("raw", [None, "", "  ", "/"])
def test_normalize_of_nothing_is_empty(raw):
    assert normalize_image_prefix(raw) == ""


class TestPrefixForAddedCluster:
    """`cluster add` puts a cluster on a control plane that already has one."""

    def test_unstated_inherits_from_an_existing_cluster(self):
        clusters = [_cluster(""), _cluster(f"{PREFIX}/")]
        assert cluster_ops._image_prefix_for_added_cluster(None, clusters) == PREFIX

    def test_unstated_with_no_prefixed_cluster_is_empty(self):
        assert cluster_ops._image_prefix_for_added_cluster(None, [_cluster("")]) == ""
        assert cluster_ops._image_prefix_for_added_cluster(None, []) == ""

    def test_a_given_prefix_wins_and_is_normalized(self):
        assert cluster_ops._image_prefix_for_added_cluster(" other.example/hub/ ", [_cluster(PREFIX)]) == "other.example/hub"

    def test_an_empty_prefix_opts_out_of_inheriting(self):
        assert cluster_ops._image_prefix_for_added_cluster("", [_cluster(PREFIX)]) == ""
