"""The leader site of a sync-replication (stretched) cluster against the real
FoundationDB (design-stretched-sync-replication-ramen §5.1, §11.1, §13): the
create sets ``Cluster.leader_site``, the read returns it, and the hub's role
swap moves it - only to a site the cluster has, never to a site recorded
lost, idempotently, and through a compare-and-set on the record the write
lands on.

The HTTP contract is pinned by tests/unit/web/api/v2/test_cluster_leader_site.py;
the routes here run end to end on the real database.
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.exceptions import (
    SyncLeaderSiteRefusedError,
    SyncReplicationSiteError,
    SyncReplicationUnsupportedError,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_setup as setup

SITE_A, SITE_B = "site-a", "site-b"
SIZING = {"max_subsys": 40, "spdk_vcpu_count": 4}

db = setup.db
_add_cluster = setup._add_cluster


@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


def _stretched(db, *, leader="", lost="", lost_state="", sync=True):
    cluster = setup._seed_cluster(db, sync=sync)
    setup._seed_two_sites(db, cluster, per_site=1)
    cluster.leader_site = leader
    cluster.lost_site = lost
    cluster.lost_site_state = lost_state
    cluster.write_to_db(db.kv_store)
    return cluster


def _stored(db, cluster) -> Cluster:
    return db.get_cluster_by_id(cluster.get_id())


def _not_first_cluster(db):
    """add_cluster reads the DeployConfig when a cluster already exists."""
    setup._seed_deploy_config(db)
    setup._seed_cluster(db, sync=False)


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

class TestCreate:

    def test_create_records_the_leader_site(self, db):
        _not_first_cluster(db)
        cluster_id = _add_cluster("stretched", sync_replication=True, leader_site=SITE_A)
        assert db.get_cluster_by_id(cluster_id).leader_site == SITE_A

    def test_create_without_a_leader_site_leaves_it_unset(self, db):
        _not_first_cluster(db)
        cluster_id = _add_cluster("stretched", sync_replication=True)
        assert db.get_cluster_by_id(cluster_id).leader_site == ""

    def test_a_leader_site_without_sync_replication_is_refused_and_creates_nothing(self, db):
        _not_first_cluster(db)
        before = {c.get_id() for c in db.get_clusters()}
        with pytest.raises(SyncReplicationSiteError, match="sync_replication"):
            _add_cluster("plain", leader_site=SITE_A)
        assert {c.get_id() for c in db.get_clusters()} == before

    def test_route_creates_and_reads_back_the_leader_site(self, db, client):
        _not_first_cluster(db)
        response = client.post('/api/v2/clusters/', json={
            "name": "stretched", "sync_replication": True, "leader_site": SITE_A, **SIZING})
        assert response.status_code == 201, response.text
        assert response.json()["leader_site"] == SITE_A

        detail = client.get(f'/api/v2/clusters/{response.json()["id"]}/')
        assert detail.status_code == 200
        assert detail.json()["leader_site"] == SITE_A

    def test_route_refuses_a_leader_site_without_sync_replication(self, db, client):
        _not_first_cluster(db)
        before = {c.get_id() for c in db.get_clusters()}
        response = client.post('/api/v2/clusters/', json={"name": "plain", "leader_site": SITE_A, **SIZING})
        assert response.status_code == 400
        assert "sync_replication" in response.json()["detail"]["message"]
        assert {c.get_id() for c in db.get_clusters()} == before


# ---------------------------------------------------------------------------
# move (the hub's role swap)
# ---------------------------------------------------------------------------

class TestSetLeaderSite:

    def test_moves_the_leader_site(self, db):
        cluster = _stretched(db, leader=SITE_A)
        src.check_leader_site(cluster, SITE_B)
        src.set_leader_site(cluster.get_id(), SITE_B)
        assert _stored(db, cluster).leader_site == SITE_B

    def test_sets_an_unset_leader_site(self, db):
        cluster = _stretched(db)
        src.set_leader_site(cluster.get_id(), SITE_A)
        assert _stored(db, cluster).leader_site == SITE_A

    def test_is_idempotent(self, db):
        cluster = _stretched(db, leader=SITE_A)
        events_before = len(db.get_events(cluster.get_id()))
        src.check_leader_site(cluster, SITE_A)
        src.set_leader_site(cluster.get_id(), SITE_A)
        assert _stored(db, cluster).leader_site == SITE_A
        assert len(db.get_events(cluster.get_id())) == events_before

    def test_a_move_is_logged_as_a_cluster_event(self, db):
        cluster = _stretched(db, leader=SITE_A)
        src.set_leader_site(cluster.get_id(), SITE_B)
        messages = [e.message for e in db.get_events(cluster.get_id())]
        assert any(SITE_A in m and SITE_B in m and "leader site" in m for m in messages)

    def test_refused_on_a_cluster_without_sync_replication(self, db):
        cluster = setup._seed_cluster(db, sync=False)
        with pytest.raises(SyncReplicationUnsupportedError, match="not a sync-replication cluster"):
            src.check_leader_site(cluster, SITE_A)
        with pytest.raises(SyncReplicationUnsupportedError):
            src.set_leader_site(cluster.get_id(), SITE_A)
        assert _stored(db, cluster).leader_site == ""

    def test_refuses_an_empty_site(self, db):
        cluster = _stretched(db, leader=SITE_A)
        with pytest.raises(SyncReplicationSiteError, match="empty"):
            src.check_leader_site(cluster, "")

    def test_refuses_a_site_the_cluster_does_not_have(self, db):
        cluster = _stretched(db, leader=SITE_A)
        with pytest.raises(SyncReplicationSiteError, match="site-x"):
            src.check_leader_site(cluster, "site-x")
        with pytest.raises(SyncReplicationSiteError):
            src.set_leader_site(cluster.get_id(), "site-x")
        assert _stored(db, cluster).leader_site == SITE_A

    def test_a_site_with_only_removed_nodes_is_not_a_site_of_the_cluster(self, db):
        cluster = _stretched(db, leader=SITE_A)
        setup._seed_node(db, cluster, "site-c", status=StorageNode.STATUS_REMOVED)
        with pytest.raises(SyncReplicationSiteError, match="site-c"):
            src.check_leader_site(cluster, "site-c")

    @pytest.mark.parametrize("state", [
        Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN, Cluster.LOST_SITE_FENCING, Cluster.LOST_SITE_DONE,
    ])
    def test_refuses_the_recorded_lost_site(self, db, state):
        cluster = _stretched(db, leader=SITE_A, lost=SITE_B, lost_state=state)
        with pytest.raises(SyncLeaderSiteRefusedError, match="cannot lead"):
            src.check_leader_site(cluster, SITE_B)
        with pytest.raises(SyncLeaderSiteRefusedError):
            src.set_leader_site(cluster.get_id(), SITE_B)
        assert _stored(db, cluster).leader_site == SITE_A

    def test_the_other_site_may_lead_while_one_is_lost(self, db):
        cluster = _stretched(db, leader=SITE_A, lost=SITE_A, lost_state=Cluster.LOST_SITE_DONE)
        src.check_leader_site(cluster, SITE_B)
        src.set_leader_site(cluster.get_id(), SITE_B)
        assert _stored(db, cluster).leader_site == SITE_B

    def test_a_site_recorded_lost_after_the_check_is_refused_at_the_write(self, db):
        """The write judges the stored record, not the caller's copy."""
        stale = _stretched(db, leader=SITE_A)
        src.check_leader_site(stale, SITE_B)
        fresh = _stored(db, stale)
        fresh.lost_site, fresh.lost_site_state = SITE_B, Cluster.LOST_SITE_DRAINING
        fresh.write_to_db(db.kv_store)

        with pytest.raises(SyncLeaderSiteRefusedError):
            src.set_leader_site(stale.get_id(), SITE_B)
        assert _stored(db, stale).leader_site == SITE_A

    def test_the_write_keeps_fields_written_since_the_caller_read(self, db):
        """No stale full-object write: a field another writer changed after
        the caller's read survives the move."""
        stale = _stretched(db, leader=SITE_A)
        fresh = _stored(db, stale)
        fresh.lost_site, fresh.lost_site_state = SITE_A, Cluster.LOST_SITE_DRAINING
        fresh.write_to_db(db.kv_store)

        src.set_leader_site(stale.get_id(), SITE_B)

        stored = _stored(db, stale)
        assert stored.leader_site == SITE_B
        assert (stored.lost_site, stored.lost_site_state) == (SITE_A, Cluster.LOST_SITE_DRAINING)


# ---------------------------------------------------------------------------
# the PUT route, end to end
# ---------------------------------------------------------------------------

class TestUpdateRoute:

    def _put(self, client, cluster, body):
        return client.put(f'/api/v2/clusters/{cluster.get_id()}/', json=body)

    def test_moves_the_leader_site(self, db, client):
        cluster = _stretched(db, leader=SITE_A)
        response = self._put(client, cluster, {"leader_site": SITE_B})
        assert response.status_code == 204
        assert _stored(db, cluster).leader_site == SITE_B

    def test_same_value_again_succeeds(self, db, client):
        cluster = _stretched(db, leader=SITE_A)
        assert self._put(client, cluster, {"leader_site": SITE_A}).status_code == 204
        assert _stored(db, cluster).leader_site == SITE_A

    @pytest.mark.parametrize("sync, site, fragment", [
        (False, SITE_A, "not a sync-replication cluster"),
        (True, "", "empty"),
        (True, "site-x", "site-x"),
    ])
    def test_400_changes_nothing(self, db, client, sync, site, fragment):
        cluster = _stretched(db, leader=SITE_A if sync else "", sync=sync)
        name = cluster.cluster_name
        response = self._put(client, cluster, {"name": "renamed", "leader_site": site})
        assert response.status_code == 400
        assert fragment in response.json()["detail"]["message"]
        stored = _stored(db, cluster)
        assert stored.cluster_name == name
        assert stored.leader_site == (SITE_A if sync else "")

    def test_lost_site_is_409_and_changes_nothing(self, db, client):
        cluster = _stretched(db, leader=SITE_A, lost=SITE_B, lost_state=Cluster.LOST_SITE_DOWN)
        name = cluster.cluster_name
        response = self._put(client, cluster, {"name": "renamed", "leader_site": SITE_B})
        assert response.status_code == 409
        assert "cannot lead" in response.json()["detail"]["message"]
        stored = _stored(db, cluster)
        assert stored.cluster_name == name
        assert stored.leader_site == SITE_A
