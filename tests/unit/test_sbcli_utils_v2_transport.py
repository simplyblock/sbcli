"""``SbcliUtils`` itself, with the socket replaced: does it speak v2?

The router's translation is covered in ``test_api_v2_router.py``. This covers
the other half -- the class the suite actually calls. Every request is
intercepted, so these tests assert on the URL that would have gone out, the
headers on it, and what each public method hands back.

Worth having separately because the two halves fail differently. A wrong
route table is a 404 on the first call; a wrong transport is subtler: v2
answers 201 and 204 where v1 only ever answered 200, so a success check
written against v1 reads every create and every delete as a failure and
retries it ten times before giving up.
"""
import pathlib
import sys

import pytest

E2E = pathlib.Path(__file__).resolve().parents[2] / "e2e"
if E2E.is_dir() and str(E2E) not in sys.path:
    sys.path.insert(0, str(E2E))

sbcli_utils = pytest.importorskip(
    "utils.sbcli_utils", reason="needs the e2e tree on sys.path")

CLUSTER = "11111111-1111-1111-1111-111111111111"
POOL = "22222222-2222-2222-2222-222222222222"
VOL = "44444444-4444-4444-4444-444444444444"
NODE_A = "77777777-7777-7777-7777-777777777777"
NODE_B = "88888888-8888-8888-8888-888888888888"


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or ""
        self.reason = "x"
        self.content = b"" if payload is None and not text else b"{}"

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class Transport:
    """Stands in for ``requests.request``."""

    def __init__(self):
        self.calls = []
        self.routes = {}

    def add(self, verb, url_suffix, status=200, payload=None):
        self.routes[(verb.upper(), url_suffix)] = FakeResponse(status, payload)

    def __call__(self, verb, url, headers=None, json=None, params=None,
                 timeout=None):
        self.calls.append({"verb": verb.upper(), "url": url, "body": json,
                           "params": params, "headers": headers})
        for (rverb, suffix), response in self.routes.items():
            if rverb == verb.upper() and url.endswith(suffix):
                return response
        return FakeResponse(200, [])

    def urls(self, verb=None):
        return [c["url"] for c in self.calls
                if verb is None or c["verb"] == verb]

    def find(self, verb, suffix):
        for call in self.calls:
            if call["verb"] == verb.upper() and call["url"].endswith(suffix):
                return call
        raise AssertionError(
            f"no {verb} ending in {suffix}; got " + ", ".join(
                f"{c['verb']} {c['url']}" for c in self.calls))


@pytest.fixture
def client(monkeypatch):
    transport = Transport()
    monkeypatch.setattr(sbcli_utils.requests, "request", transport)
    # The recovery waiter uses requests.get directly and sleeps; neither
    # belongs in a unit test.
    monkeypatch.setattr(sbcli_utils.SbcliUtils, "_wait_for_api_recovery",
                        lambda self, interval=15: False)
    monkeypatch.setattr(sbcli_utils, "sleep_n_sec", lambda *a, **k: None)
    utils = sbcli_utils.SbcliUtils(
        cluster_secret="sekrit",
        cluster_api_url="http://198.51.100.10/",
        cluster_id=CLUSTER,
    )
    return utils, transport


# ── the base URL ─────────────────────────────────────────────────────────

def test_every_call_goes_to_api_v2(client):
    utils, transport = client
    utils.get_request("/storagenode")
    assert transport.urls() == [
        f"http://198.51.100.10/api/v2/clusters/{CLUSTER}/storage-nodes/"]


@pytest.mark.parametrize("given", [
    "http://198.51.100.10",
    "http://198.51.100.10/",
    # What several workflows export. Appending /api/v2 to this would give
    # /api/v1/api/v2/... and 404 ten times before the run died.
    "http://198.51.100.10/api/v1",
    "http://198.51.100.10/api/v1/",
])
def test_a_versioned_or_trailing_slash_base_is_normalised(given, monkeypatch):
    transport = Transport()
    monkeypatch.setattr(sbcli_utils.requests, "request", transport)
    utils = sbcli_utils.SbcliUtils("s", given, CLUSTER)
    assert utils.v2_base == "http://198.51.100.10/api/v2"


def test_the_auth_header_is_bearer(client):
    """v2 is HTTPBearer; v1's "<cluster_id> <secret>" is a 403.

    The scheme is checked by FastAPI's security dependency before any
    handler runs, so getting this wrong does not fail one call -- it fails
    every call, identically, which is hard to tell from "the API is down".
    It is also what made every v2 column of the August parity report empty.
    """
    utils, transport = client
    utils.get_request("/storagenode")
    assert transport.calls[0]["headers"]["Authorization"] == "Bearer sekrit"
    assert CLUSTER not in transport.calls[0]["headers"]["Authorization"]


# ── status codes ─────────────────────────────────────────────────────────

def test_201_is_a_success(client):
    """v2 answers a create with 201; v1 only ever used 200."""
    utils, transport = client
    transport.add("POST", "/storage-pools/", status=201,
                  payload={"id": POOL, "name": "p"})
    out = utils.post_request("/pool", body={"name": "p"})
    assert out["status"] is True
    assert len(transport.calls) == 1, "a 201 must not be retried"


def test_204_is_a_success_with_no_body(client):
    """Most v2 mutations answer 204. A body-parsing success check would
    read that as a failure and retry it."""
    utils, transport = client
    transport.add("DELETE", f"/volumes/{VOL}/", status=204)
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "p"}])
    transport.add("GET", f"/storage-pools/{POOL}/volumes/",
                  payload=[{"id": VOL, "name": "lvol_a"}])
    out = utils.delete_request(f"/lvol/{VOL}")
    assert out["status"] is True


def test_a_refusal_is_not_retried_ten_times(client):
    """v1 returned 200 + status:false for a refusal, so there was nothing to
    retry. v2 returns 409, and retrying it only delays the failure."""
    utils, transport = client
    transport.add("POST", "/storage-pools/", status=409, payload=None)
    with pytest.raises(sbcli_utils.requests.exceptions.HTTPError):
        utils.post_request("/pool", body={"name": "p"})
    posts = [c for c in transport.calls if c["verb"] == "POST"]
    assert len(posts) == 1, f"retried a 409 {len(posts)} times"


def test_an_expected_error_code_returns_none_as_it_did_under_v1(client):
    utils, transport = client
    transport.add("POST", f"/storage-nodes/{NODE_A}/suspend", status=409)
    assert utils.get_request(
        f"/storagenode/suspend/{NODE_A}",
        expected_error_code=[409]) is None


def test_a_500_is_retried(client):
    utils, transport = client
    transport.add("GET", "/storage-nodes/", status=500)
    with pytest.raises(sbcli_utils.requests.exceptions.HTTPError):
        utils.get_request("/storagenode")
    assert len(transport.calls) > 1, "a 5xx is transient and must be retried"


# ── the public methods ───────────────────────────────────────────────────

def _pool_and_volume(transport, volumes=None):
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "pool_a"}])
    transport.add("GET", f"/storage-pools/{POOL}/volumes/",
                  payload=volumes if volumes is not None else [
                      {"id": VOL, "name": "lvol_a", "status": "online",
                       "storage_node_id": NODE_A}])


def test_list_storage_pools_still_maps_name_to_id(client):
    utils, transport = client
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "pool_a"}])
    assert utils.list_storage_pools() == {"pool_a": POOL}


def test_list_lvols_still_maps_name_to_id(client):
    utils, transport = client
    _pool_and_volume(transport)
    assert utils.list_lvols() == {"lvol_a": VOL}


def test_get_lvol_id_resolves_by_name(client):
    utils, transport = client
    _pool_and_volume(transport)
    assert utils.get_lvol_id("lvol_a") == VOL
    assert utils.get_lvol_id("nope") is None


def test_add_lvol_posts_under_the_named_pool(client):
    utils, transport = client
    _pool_and_volume(transport, volumes=[])
    transport.add("POST", f"/storage-pools/{POOL}/volumes/", status=201,
                  payload=VOL)
    utils.add_lvol(lvol_name="lvol_new", pool_name="pool_a", size="10G")
    call = transport.find("POST", f"/storage-pools/{POOL}/volumes/")
    assert call["body"]["name"] == "lvol_new"
    assert call["body"]["size"] == "10G"


def test_add_lvol_still_short_circuits_on_a_duplicate_name(client):
    """The guard reads list_lvols, which now fans out over pools."""
    utils, transport = client
    _pool_and_volume(transport)
    utils.add_lvol(lvol_name="lvol_a", pool_name="pool_a", size="10G")
    assert not [c for c in transport.calls if c["verb"] == "POST"]


def test_get_storage_nodes_keeps_the_results_envelope(client):
    utils, transport = client
    transport.add("GET", "/storage-nodes/", payload=[
        {"id": NODE_A, "status": "online", "lvols": 0, "lvols_max": 10}])
    data = utils.get_storage_nodes()
    assert data["results"][0]["uuid"] == NODE_A


def test_is_secondary_node_is_answered_from_the_relationship(client):
    """31 call sites, and HA node selection turns on it."""
    utils, transport = client
    transport.add("GET", "/storage-nodes/", payload=[
        {"id": NODE_A, "status": "online", "secondary_node_id": NODE_B},
        {"id": NODE_B, "status": "online"},
    ])
    assert utils.is_secondary_node(NODE_B) is True
    assert utils.is_secondary_node(NODE_A) is False


def test_get_node_without_lvols_reads_both_renamed_fields(client):
    """It filters on ``lvols`` and ``is_secondary_node`` and returns
    ``uuid`` -- one v2 name, one derived, one aliased."""
    utils, transport = client
    transport.add("GET", "/storage-nodes/", payload=[
        {"id": NODE_A, "status": "online", "lvols": 2},
        {"id": NODE_B, "status": "online", "lvols": 0},
    ])
    assert utils.get_node_without_lvols() == NODE_B


def test_get_storage_node_details_is_indexable(client):
    utils, transport = client
    transport.add("GET", f"/storage-nodes/{NODE_A}/",
                  payload={"id": NODE_A, "status": "online"})
    assert utils.get_storage_node_details(NODE_A)[0]["status"] == "online"


def test_get_lvol_connect_str_reads_the_connect_entries(client):
    utils, transport = client
    _pool_and_volume(transport)
    transport.add("GET", f"/volumes/{VOL}/connect",
                  payload=[{"connect": "sudo nvme connect --nqn=x"}])
    assert utils.get_lvol_connect_str("lvol_a") == [
        "sudo nvme connect --nqn=x"]


def test_resize_lvol_puts_through_the_update_route(client):
    utils, transport = client
    _pool_and_volume(transport)
    transport.add("PUT", f"/volumes/{VOL}/", status=204)
    utils.resize_lvol(VOL, "20G")
    assert transport.find("PUT", f"/volumes/{VOL}/")["body"] == {"size": "20G"}


def test_activate_cluster_posts(client):
    utils, transport = client
    transport.add("POST", "/activate", status=202)
    utils.activate_cluster(CLUSTER)
    transport.find("POST", f"/clusters/{CLUSTER}/activate")


def test_restart_node_posts_to_the_node(client):
    utils, transport = client
    transport.add("POST", f"/storage-nodes/{NODE_A}/restart", status=202)
    utils.restart_node(NODE_A, force=True)
    call = transport.find("POST", f"/storage-nodes/{NODE_A}/restart")
    assert call["body"] == {"force": True}


def test_delete_storage_pool_no_longer_reads_the_pool_for_its_secret(client):
    """v1 echoed the pool secret in a header; v2's route takes none, and
    StoragePoolDTO does not carry one to read."""
    utils, transport = client
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "pool_a"}])
    transport.add("DELETE", f"/storage-pools/{POOL}/", status=204)
    utils.delete_storage_pool("pool_a")
    call = transport.find("DELETE", f"/storage-pools/{POOL}/")
    assert "secret" not in call["headers"]
    assert not [c for c in transport.calls
                if c["verb"] == "GET" and c["url"].endswith(
                    f"/storage-pools/{POOL}/")], \
        "the extra detail read should be gone"


def test_add_storage_node_translates_and_drops(client):
    """v2 renamed three add-node parameters, inverted one and dropped five."""
    utils, transport = client
    transport.add("POST", "/storage-nodes/", status=201, payload=NODE_A)
    utils.add_storage_node(
        cluster_id=CLUSTER, node_ip="10.0.0.9", ifname="eth0",
        max_lvol=50, max_prov="100G", max_snap=500,
        number_of_distribs=2, number_of_devices=2, partitions=1,
        jm_percent=3, disable_ha_jm=False, enable_test_device=False,
        iobuf_small_pool_count=0, iobuf_large_pool_count=0,
        spdk_debug=False, spdk_image="sb/spdk:x", spdk_cpu_mask="0xFF",
    )
    body = transport.find("POST", f"/clusters/{CLUSTER}/storage-nodes/")["body"]
    assert body["node_address"] == "10.0.0.9"
    assert body["interface_name"] == "eth0"
    assert body["max_snapshots"] == 500
    # v1 asked for the negative of this.
    assert body["ha_jm"] is True
    assert body["spdk_image"] == "sb/spdk:x"
    for gone in ("max_lvol", "max_prov", "number_of_distribs",
                 "number_of_devices", "spdk_cpu_mask", "node_ip", "ifname"):
        assert gone not in body, f"{gone} is not a v2 parameter"


def test_get_cluster_details_finds_the_cluster_in_the_listing(client):
    utils, transport = client
    transport.add("GET", "/api/v2/clusters/", payload=[
        {"id": CLUSTER, "name": "c1", "status": "active"}])
    details = utils.get_cluster_details()
    assert details["status"] == "active"
    # v1's spellings still resolve.
    assert details["uuid"] == CLUSTER
    assert details["cluster_name"] == "c1"


def test_get_io_stats_still_digs_out_the_stats_list(client):
    utils, transport = client
    transport.add("GET", "/iostats", payload=[{"read_bytes": 7}])
    assert utils.get_io_stats(CLUSTER) == [{"read_bytes": 7}]


def test_add_snapshot_posts_under_the_volume(client):
    utils, transport = client
    _pool_and_volume(transport)
    transport.add("POST", f"/volumes/{VOL}/snapshots", status=201,
                  payload="snap-id")
    utils.add_snapshot(VOL, "snap_1")
    call = transport.find("POST", f"/volumes/{VOL}/snapshots")
    assert call["body"] == {"name": "snap_1"}


def test_add_clone_creates_a_volume_from_the_snapshot(client):
    utils, transport = client
    snap = "66666666-6666-6666-6666-666666666666"
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "pool_a"}])
    transport.add("GET", f"/storage-pools/{POOL}/snapshots/",
                  payload=[{"id": snap, "name": "snap_1",
                            "lvol": {"id": VOL, "name": "lvol_a"}}])
    transport.add("POST", f"/storage-pools/{POOL}/volumes/", status=201,
                  payload="clone-id")
    utils.add_clone(snap, "clone_1")
    call = transport.find("POST", f"/storage-pools/{POOL}/volumes/")
    assert call["body"] == {"name": "clone_1", "snapshot_id": snap}


def test_list_snapshots_flattens_the_nested_volume(client):
    """v1 had lvol_id flat on the snapshot; v2 nests the whole volume."""
    utils, transport = client
    snap = "66666666-6666-6666-6666-666666666666"
    transport.add("GET", "/storage-pools/",
                  payload=[{"id": POOL, "name": "pool_a"}])
    transport.add("GET", f"/storage-pools/{POOL}/snapshots/",
                  payload=[{"id": snap, "name": "snap_1",
                            "lvol": {"id": VOL, "name": "lvol_a"}}])
    assert utils.list_snapshots() == {"snap_1": snap}


def test_add_host_to_pool_posts_the_nqn(client):
    utils, transport = client
    transport.add("POST", f"/storage-pools/{POOL}/host", status=204)
    utils.add_host_to_pool(POOL, "nqn.2014-08.org.nvmexpress:uuid:a")
    call = transport.find("POST", f"/storage-pools/{POOL}/host")
    assert call["body"] == {"host_nqn": "nqn.2014-08.org.nvmexpress:uuid:a"}


def test_remove_host_from_pool_moves_the_nqn_into_the_body(client):
    utils, transport = client
    nqn = "nqn.2014-08.org.nvmexpress:uuid:a"
    transport.add("DELETE", f"/storage-pools/{POOL}/host", status=204)
    utils.remove_host_from_pool(POOL, nqn)
    assert transport.find(
        "DELETE", f"/storage-pools/{POOL}/host")["body"] == {"host_nqn": nqn}


def test_get_device_details_still_returns_a_list_of_devices(client):
    utils, transport = client
    transport.add("GET", f"/storage-nodes/{NODE_A}/devices/", payload=[
        {"id": "dev-1", "status": "online", "health_check": True}])
    devices = utils.get_device_details(NODE_A)
    assert devices[0]["id"] == "dev-1"
    assert devices[0]["node_id"] == NODE_A


def test_get_management_nodes_queries_by_cluster(client):
    utils, transport = client
    transport.add("GET", "/management-nodes/", payload=[
        {"id": "m1", "mgmt_ip": "10.0.0.2"}])
    data = utils.get_management_nodes()
    assert data["results"][0]["mgmt_ip"] == "10.0.0.2"
    assert transport.find(
        "GET", "/management-nodes/")["params"] == {"cluster_id": CLUSTER}


def test_get_all_nodes_ip_reads_both_node_kinds(client):
    utils, transport = client
    transport.add("GET", "/management-nodes/",
                  payload=[{"id": "m1", "mgmt_ip": "10.0.0.2"}])
    transport.add("GET", "/storage-nodes/",
                  payload=[{"id": NODE_A, "mgmt_ip": "10.0.0.10"}])
    mgmt, storage = utils.get_all_nodes_ip()
    assert mgmt == ["10.0.0.2"] and storage == ["10.0.0.10"]
