"""``GET /api/v2/clusters/{id}/reachability?site=`` against the real
FoundationDB (design-stretched §5.4): the storage-node monitor's
management-address probe run once on demand for the nodes of a site (every
node without one). The probe itself - a ping - is the external side effect
and is patched; the node selection and the route are real."""
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import health_controller
from simplyblock_core.models.storage_node import StorageNode
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B

db = stack.db
web_client = publish.web_client
_update = stack._update


@pytest.fixture()
def ping():
    """The probe: every address answers but those in ``down``."""
    down: set = set()
    with patch.object(health_controller, "_check_node_ping", side_effect=lambda ip: ip not in down):
        yield down


def _url(cluster, site=None):
    url = f"/api/v2/clusters/{cluster.get_id()}/reachability"
    return url if site is None else f"{url}?site={site}"


def _answers(response):
    return {row["id"]: (row["mgmt_ip"], row["site"], row["reachable"]) for row in response.json()}


def test_the_nodes_of_a_site(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    ping.add(a[1].mgmt_ip)
    response = web_client.get(_url(cluster, SITE_A))
    assert response.status_code == 200, response.text
    assert _answers(response) == {
        a[0].get_id(): (a[0].mgmt_ip, SITE_A, True),
        a[1].get_id(): (a[1].mgmt_ip, SITE_A, False),
        a[2].get_id(): (a[2].mgmt_ip, SITE_A, True),
    }


def test_every_node_without_a_site_but_the_removed(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    _update(db, b[2], status=StorageNode.STATUS_REMOVED)
    response = web_client.get(_url(cluster))
    assert response.status_code == 200, response.text
    assert set(_answers(response)) == {n.get_id() for n in (*a, b[0], b[1])}


def test_an_offline_node_is_still_probed(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    _update(db, b[0], status=StorageNode.STATUS_OFFLINE)
    assert _answers(web_client.get(_url(cluster, SITE_B)))[b[0].get_id()][2] is True


def test_an_unknown_site_is_400(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    response = web_client.get(_url(cluster, "site-x"))
    assert response.status_code == 400, response.text
    assert "site-x" in response.json()["detail"]["message"]


def test_a_site_on_a_cluster_without_sync_replication_is_400(db, web_client, ping):
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    node = stack._seed_node(db, cluster, "n0", "")
    assert web_client.get(_url(cluster, SITE_A)).status_code == 400
    response = web_client.get(_url(cluster))
    assert response.status_code == 200, response.text
    assert _answers(response) == {node.get_id(): (node.mgmt_ip, "", True)}
