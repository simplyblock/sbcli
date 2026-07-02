# coding=utf-8
"""
test_cluster_duplicate_name.py – integration tests for duplicate cluster
name prevention, running against the real FoundationDB provisioned by
``tests/integration/conftest.py``.

Tests cover:
  - add_cluster() raises ValueError when a cluster with the same name exists
  - add_cluster() succeeds (and persists) when the name is unique
  - add_cluster() skips the uniqueness check when name is None
  - create_cluster() raises ValueError when a cluster with the same name exists
  - create_cluster() succeeds (and persists) when no clusters exist yet
  - set_name() raises ValueError when another cluster already has the name
  - set_name() allows renaming a cluster to its own current name
  - set_name() persists a unique new name
  - change_cluster_name() raises ValueError when another cluster has the name
  - change_cluster_name() allows renaming a cluster to its own current name
  - change_cluster_name() persists a unique new name

The database layer is NOT mocked: real ``Cluster`` objects are written to
FoundationDB through ``DBController`` and read back through the same code
paths ``cluster_ops`` uses. Only side-effects *above* the DB (docker swarm,
deploy scripts, mgmt-node provisioning, interface-IP lookup) are mocked, and
only for ``create_cluster`` — whose duplicate-name guard is the sole part
exercised for the "raises" case, so those mocks never engage there.
"""

from unittest.mock import patch

import pytest
from pydantic import SecretStr

from simplyblock_core import cluster_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster


# ---------------------------------------------------------------------------
# Fixtures — real FDB, wiped before every test for isolation.
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return controller


@pytest.fixture(autouse=True)
def _clean_keyspace(db):
    # The tier-wide fixture only wipes once per module; wipe per-test here so
    # clusters seeded by one test never leak into the next.
    db.kv_store.clear_range(b"\x00", b"\xff")
    yield


def _seed_cluster(db, uuid="cluster-1", name="test-cluster", mode="docker"):
    """Persist a real Cluster to FDB and return it."""
    c = Cluster()
    c.uuid = uuid
    c.cluster_name = name
    c.mode = mode
    c.status = Cluster.STATUS_ACTIVE
    c.write_to_db(db.kv_store)
    return c


# ---------------------------------------------------------------------------
# add_cluster()
# ---------------------------------------------------------------------------

def _add_cluster(name):
    return cluster_ops.add_cluster(
        blk_size=4096,
        page_size_in_blocks=2,
        cap_warn=80,
        cap_crit=90,
        prov_cap_warn=80,
        prov_cap_crit=90,
        distr_ndcs=1,
        distr_npcs=1,
        distr_bs=4096,
        distr_chunk_bs=4096,
        ha_type="single",
        enable_node_affinity=False,
        qpair_count=4,
        max_queue_size=128,
        inflight_io_threshold=64,
        strict_node_anti_affinity=False,
        is_single_node=True,
        name=name,
    )


class TestAddClusterDuplicateName:

    def test_raises_when_name_already_exists(self, db):
        _seed_cluster(db, uuid="cluster-1", name="my-cluster")

        with pytest.raises(ValueError, match="my-cluster"):
            _add_cluster("my-cluster")

    def test_raises_when_one_of_many_clusters_matches(self, db):
        _seed_cluster(db, uuid="cluster-1", name="alpha")
        _seed_cluster(db, uuid="cluster-2", name="beta")

        with pytest.raises(ValueError, match="beta"):
            _add_cluster("beta")

    def test_succeeds_and_persists_when_name_is_unique(self, db):
        # An existing docker cluster makes add_cluster inherit its mode and
        # skip the k8s/first-cluster provisioning branch entirely — no docker
        # or deploy side-effects on this path, so nothing needs mocking.
        _seed_cluster(db, uuid="cluster-1", name="other-cluster")

        new_id = _add_cluster("new-cluster")

        stored = db.get_cluster_by_id(new_id)
        assert stored.cluster_name == "new-cluster"

    def test_no_name_skips_duplicate_check(self, db):
        # name=None must bypass the uniqueness check: a second cluster is
        # created even though one already exists. (The str-typed cluster_name
        # field round-trips None as "None" through FDB, so assert on the
        # created id / cluster count rather than the stored name.)
        _seed_cluster(db, uuid="cluster-1", name="some-cluster")

        new_id = _add_cluster(None)

        assert db.get_cluster_by_id(new_id).uuid == new_id
        assert len(db.get_clusters()) == 2


# ---------------------------------------------------------------------------
# create_cluster()
# ---------------------------------------------------------------------------

def _create_cluster(name):
    return cluster_ops.create_cluster(
        blk_size=4096,
        page_size_in_blocks=2,
        cli_pass=SecretStr("pass"),
        cap_warn=80,
        cap_crit=90,
        prov_cap_warn=80,
        prov_cap_crit=90,
        ifname="eth0",
        mgmt_ip=None,
        log_del_interval=7,
        metrics_retention_period=30,
        contact_point=None,
        grafana_endpoint=None,
        distr_ndcs=1,
        distr_npcs=1,
        distr_bs=4096,
        distr_chunk_bs=4096,
        ha_type="single",
        mode="docker",
        enable_node_affinity=False,
        qpair_count=4,
        client_qpair_count=4,
        max_queue_size=128,
        inflight_io_threshold=64,
        disable_monitoring=True,
        strict_node_anti_affinity=False,
        name=name,
        tls_secret=None,
        ingress_host_source=None,
        dns_name=None,
        fabric="tcp",
        is_single_node=True,
        client_data_nic="",
    )


class TestCreateClusterDuplicateName:

    def test_raises_when_name_already_exists(self, db):
        # The duplicate-name guard fires before any docker/deploy work, so no
        # infra mocking is needed — the ValueError is raised straight away.
        _seed_cluster(db, uuid="cluster-1", name="first-cluster")

        with pytest.raises(ValueError, match="first-cluster"):
            _create_cluster("first-cluster")

    def test_succeeds_and_persists_when_no_clusters_exist(self, db):
        # No pre-existing cluster → the guard passes and create_cluster runs
        # its full docker deploy flow. Mock only the side-effects above the DB
        # (docker swarm, deploy scripts, mgmt-node provisioning, iface lookup);
        # the cluster is still written to and read back from the real FDB.
        with patch("simplyblock_core.cluster_ops.scripts"), \
             patch("simplyblock_core.cluster_ops.docker"), \
             patch("simplyblock_core.cluster_ops.mgmt_node_ops"), \
             patch("simplyblock_core.cluster_ops.utils.get_iface_ip", return_value="10.0.0.1"):
            new_id = _create_cluster("brand-new")

        stored = db.get_cluster_by_id(new_id)
        assert stored.cluster_name == "brand-new"


# ---------------------------------------------------------------------------
# set_name()
# ---------------------------------------------------------------------------

class TestSetNameDuplicateName:

    def test_raises_when_another_cluster_has_the_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="old-name")
        _seed_cluster(db, uuid="cluster-2", name="taken-name")

        with pytest.raises(ValueError, match="taken-name"):
            cluster_ops.set_name("cluster-1", "taken-name")

    def test_allows_renaming_to_own_current_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="my-cluster")

        result = cluster_ops.set_name("cluster-1", "my-cluster")  # must not raise

        assert result.cluster_name == "my-cluster"

    def test_persists_unique_new_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="old-name")
        _seed_cluster(db, uuid="cluster-2", name="other-cluster")

        cluster_ops.set_name("cluster-1", "brand-new-name")

        assert db.get_cluster_by_id("cluster-1").cluster_name == "brand-new-name"


# ---------------------------------------------------------------------------
# change_cluster_name()
# ---------------------------------------------------------------------------

class TestChangeClusterNameDuplicateName:

    def test_raises_when_another_cluster_has_the_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="old-name")
        _seed_cluster(db, uuid="cluster-2", name="taken-name")

        with pytest.raises(ValueError, match="taken-name"):
            cluster_ops.change_cluster_name("cluster-1", "taken-name")

    def test_allows_renaming_to_own_current_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="my-cluster")

        cluster_ops.change_cluster_name("cluster-1", "my-cluster")  # must not raise

        assert db.get_cluster_by_id("cluster-1").cluster_name == "my-cluster"

    def test_persists_unique_new_name(self, db):
        _seed_cluster(db, uuid="cluster-1", name="old-name")
        _seed_cluster(db, uuid="cluster-2", name="other-cluster")

        cluster_ops.change_cluster_name("cluster-1", "unique-name")

        assert db.get_cluster_by_id("cluster-1").cluster_name == "unique-name"
