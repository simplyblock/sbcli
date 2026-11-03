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


# ---------------------------------------------------------------------------
# ?address= : workers that are not storage nodes yet (the follower's preflight)
# ---------------------------------------------------------------------------

def _address_url(cluster, *addresses, site=None):
    query = "&".join(f"address={a}" for a in addresses)
    if site is not None:
        query += f"&site={site}"
    return f"/api/v2/clusters/{cluster.get_id()}/reachability?{query}"


def test_the_addresses_given_not_the_nodes(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    ping.add("192.0.2.2")
    with patch.object(health_controller, "_check_node_ping",
                      side_effect=lambda ip: ip not in ping) as probe:
        response = web_client.get(_address_url(cluster, "192.0.2.2", "2001:db8::1", "192.0.2.1"))
    assert response.status_code == 200, response.text
    assert response.json() == [
        {"id": "", "mgmt_ip": "192.0.2.2", "site": "", "reachable": False},
        {"id": "", "mgmt_ip": "2001:db8::1", "site": "", "reachable": True},
        {"id": "", "mgmt_ip": "192.0.2.1", "site": "", "reachable": True},
    ]
    assert sorted(c.args[0] for c in probe.call_args_list) == ["192.0.2.1", "192.0.2.2", "2001:db8::1"]


def test_the_address_probe_records_nothing(db, web_client, ping):
    cluster, a, b, owner = publish._layout(db)
    before = ({n.get_id(): n.to_dict() for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())},
              db.get_cluster_by_id(cluster.get_id()).to_dict())
    ping.add("192.0.2.1")
    assert web_client.get(_address_url(cluster, "192.0.2.1")).status_code == 200
    after = ({n.get_id(): n.to_dict() for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())},
             db.get_cluster_by_id(cluster.get_id()).to_dict())
    assert after == before


def test_works_on_a_cluster_without_sync_replication(db, web_client, ping):
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    response = web_client.get(_address_url(cluster, "192.0.2.1"))
    assert response.status_code == 200, response.text
    assert response.json() == [{"id": "", "mgmt_ip": "192.0.2.1", "site": "", "reachable": True}]


@pytest.mark.parametrize("addresses, site, fragment", [
    (("192.0.2.1",), SITE_A, "together"),
    (("node-1.example.com",), None, "IP address literal"),
    (("192.0.2.1", ""), None, "IP address literal"),
    (tuple(f"192.0.2.{i % 250 + 1}" for i in range(health_controller.MAX_REACHABILITY_ADDRESSES + 1)),
     None, "at most"),
])
def test_a_refused_request_is_400_and_probes_nothing(db, web_client, addresses, site, fragment):
    cluster, a, b, owner = publish._layout(db)
    with patch.object(health_controller, "_check_node_ping") as probe:
        response = web_client.get(_address_url(cluster, *addresses, site=site))
    assert response.status_code == 400, response.text
    assert fragment in response.json()["detail"]["message"]
    probe.assert_not_called()
