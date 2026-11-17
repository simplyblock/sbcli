"""``site`` on the volume create (stretched sync replication, design §9.5),
against the real FoundationDB: the site check (lvol_controller.check_create_site),
the owner-node choice restricted to the online nodes of the site
(_get_next_3_nodes), add_lvol_ha with a site, and the v2 create route of the
production app.

Nothing here reaches a storage node: the create is stopped right after its
owner node is chosen (the site-activation check is the probe), and every
refusal comes before any RPC. The HTTP mapping alone is pinned by
tests/unit/web/api/v2/test_volume_endpoints.py.
"""
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import activation_gate, lvol_controller
from simplyblock_core.exceptions import (
    SyncReplicationSiteError,
    SyncReplicationUnsupportedError,
    SyncSiteConflictError,
)
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B

db = stack.db
web_client = publish.web_client
_layout, _pool, _volume = publish._layout, publish._pool, publish._volume
_update = stack._update


def _sync_cluster(db):
    cluster, a, b, owner = _layout(db)
    return cluster, a, b, _pool(db, cluster)


def _async_cluster(db):
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    node = stack._seed_node(db, cluster, "n0", "", max_lvol=100)
    return cluster, node, _pool(db, cluster)


class _Chosen:
    """The probe: the create stops right after its owner node is chosen
    (activation_gate.site_activation_blocks refuses), recording the node."""

    def __init__(self):
        self.nodes: list[str] = []

    def __call__(self, cluster, owner=None):
        self.nodes.append(owner.get_id())
        return "stopped by the test"


@pytest.fixture()
def chosen():
    probe = _Chosen()
    with patch.object(activation_gate, "site_activation_blocks", side_effect=probe):
        yield probe


# ---------------------------------------------------------------------------
# the site check
# ---------------------------------------------------------------------------

class TestCheckCreateSite:

    def test_a_site_of_the_cluster_passes(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        lvol_controller.check_create_site(cluster, SITE_B)

    def test_an_unknown_site_is_refused(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        with pytest.raises(SyncReplicationSiteError, match="site-x"):
            lvol_controller.check_create_site(cluster, "site-x")

    def test_a_cluster_without_sync_replication_refuses_any_site(self, db):
        cluster, node, _ = _async_cluster(db)
        with pytest.raises(SyncReplicationUnsupportedError, match="not a sync-replication cluster"):
            lvol_controller.check_create_site(cluster, SITE_A)

    @pytest.mark.parametrize("by", ["id", "hostname"])
    def test_a_host_on_another_site_conflicts_naming_both(self, db, by):
        cluster, a, b, _ = _sync_cluster(db)
        node = _update(db, b[1], hostname="host-b1")
        host = node.get_id() if by == "id" else "host-b1"
        with pytest.raises(SyncSiteConflictError) as raised:
            lvol_controller.check_create_site(cluster, SITE_A, host)
        error = raised.value
        assert (error.node_id, error.node_site, error.site) == (node.get_id(), SITE_B, SITE_A)
        assert SITE_A in str(error) and SITE_B in str(error)

    def test_a_host_on_the_site_passes(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        lvol_controller.check_create_site(cluster, SITE_A, a[2].get_id())

    def test_an_unknown_host_is_left_to_the_create(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        lvol_controller.check_create_site(cluster, SITE_A, "no-such-node")


# ---------------------------------------------------------------------------
# the owner-node choice
# ---------------------------------------------------------------------------

class TestPlacement:

    def test_the_candidates_are_the_online_nodes_of_the_site(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        _update(db, b[2], status=StorageNode.STATUS_OFFLINE)
        for _ in range(10):
            nodes = lvol_controller._get_next_3_nodes(cluster.get_id(), site=SITE_B)
            assert {n.get_id() for n in nodes} == {b[0].get_id(), b[1].get_id()}

    def test_no_site_keeps_every_online_node(self, db):
        cluster, a, b, _ = _sync_cluster(db)
        seen = set()
        for _ in range(10):
            seen |= {n.site for n in lvol_controller._get_next_3_nodes(cluster.get_id())}
        assert seen == {SITE_A, SITE_B}

    @pytest.mark.parametrize("site, other", [(SITE_A, SITE_B), (SITE_B, SITE_A)])
    def test_the_create_picks_its_owner_on_the_site(self, db, chosen, site, other):
        cluster, a, b, pool = _sync_cluster(db)
        nodes = {SITE_A: a, SITE_B: b}
        for i in range(8):
            ok, error = lvol_controller.add_lvol_ha(
                f"vol-{i}", 1024 ** 3, None, "default", pool.get_id(), site=site)
            assert ok is False and "stopped by the test" in error
        assert len(chosen.nodes) == 8
        assert set(chosen.nodes) <= {n.get_id() for n in nodes[site]}
        assert not set(chosen.nodes) & {n.get_id() for n in nodes[other]}

    def test_a_site_with_no_online_node_creates_nothing(self, db, chosen):
        cluster, a, b, pool = _sync_cluster(db)
        for node in b:
            _update(db, node, status=StorageNode.STATUS_OFFLINE)
        ok, error = lvol_controller.add_lvol_ha("vol-1", 1024 ** 3, None, "default", pool.get_id(),
                                                site=SITE_B)
        assert ok is False
        assert f"no online node of site {SITE_B}" in error
        assert chosen.nodes == []

    def test_an_explicit_host_on_the_site_is_kept(self, db, chosen):
        cluster, a, b, pool = _sync_cluster(db)
        ok, error = lvol_controller.add_lvol_ha("vol-1", 1024 ** 3, b[1].get_id(), "default",
                                                pool.get_id(), site=SITE_B)
        assert chosen.nodes == [b[1].get_id()]

    def test_an_explicit_host_on_another_site_creates_nothing(self, db, chosen):
        cluster, a, b, pool = _sync_cluster(db)
        with pytest.raises(SyncSiteConflictError):
            lvol_controller.add_lvol_ha("vol-1", 1024 ** 3, a[0].get_id(), "default",
                                        pool.get_id(), site=SITE_B)
        assert chosen.nodes == []
        assert db.get_lvols(cluster.get_id()) == []


# ---------------------------------------------------------------------------
# the v2 create route
# ---------------------------------------------------------------------------

def _create(web_client, cluster, pool, **body):
    return web_client.post(f"/api/v2/clusters/{cluster.get_id()}/storage-pools/{pool.get_id()}/volumes/",
                           json={"name": "vol-1", "size": "1G", **body})


class TestRoute:

    def test_an_unknown_site_is_400_and_creates_nothing(self, db, web_client):
        cluster, a, b, pool = _sync_cluster(db)
        response = _create(web_client, cluster, pool, site="site-x")
        assert response.status_code == 400, response.text
        assert "site-x" in response.json()["detail"]["message"]
        assert db.get_lvols(cluster.get_id()) == []

    def test_a_host_on_another_site_is_409_naming_both(self, db, web_client):
        cluster, a, b, pool = _sync_cluster(db)
        response = _create(web_client, cluster, pool, site=SITE_B, host_id=a[1].get_id())
        assert response.status_code == 409, response.text
        detail = response.json()["detail"]
        assert (detail["node_id"], detail["node_site"], detail["site"]) == (a[1].get_id(), SITE_A, SITE_B)
        assert db.get_lvols(cluster.get_id()) == []

    def test_a_site_on_a_cluster_without_sync_replication_is_400(self, db, web_client):
        cluster, node, pool = _async_cluster(db)
        response = _create(web_client, cluster, pool, site=SITE_A)
        assert response.status_code == 400, response.text
        assert "not a sync-replication cluster" in response.json()["detail"]["message"]

    def test_an_existing_name_answers_as_today_whatever_the_site(self, db, web_client):
        cluster, a, b, pool = _sync_cluster(db)
        existing = _volume(db, cluster, a[0], pool)
        response = _create(web_client, cluster, pool, name=existing.lvol_name, site="site-x")
        assert response.status_code == 409, response.text
        assert response.json()["detail"] == f"Volume {existing.lvol_name} exists"

    def test_the_owner_is_chosen_on_the_site(self, db, web_client, chosen):
        cluster, a, b, pool = _sync_cluster(db)
        response = _create(web_client, cluster, pool, site=SITE_B)
        # the probe stops the create once the owner is chosen: the refusal is a 422
        assert response.status_code == 422, response.text
        assert len(chosen.nodes) == 1 and chosen.nodes[0] in {n.get_id() for n in b}
