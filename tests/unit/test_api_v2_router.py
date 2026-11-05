"""Every v1 endpoint the suite used must translate to the right v2 request.

The harness moved off API v1 wholesale (``e2e/utils/api_v2.py``). The
translation cannot be checked against a live cluster on demand -- the labs
are busy and a parity run needs one -- so it is checked here instead, against
a recorded transport: the router is handed a fake ``call`` that returns canned
v2 payloads and records every request, and each test asserts the exact verb,
path, query and body that came out.

This is the only thing standing between the migration and a silent wrong
value, which is the failure mode that matters: a test that keeps passing
while asserting nothing is worse than one that fails. So the assertions here
are on the request, not just on "it didn't raise".

The ids below are real-looking UUIDs because ``_lookup_*`` in
``simplyblock_web/api/v2/_dependencies.py`` types every path id as ``UUID``
-- names are not accepted anywhere in v2, which is why the router has to
resolve them.
"""
import logging
import pathlib
import sys

import pytest

E2E = pathlib.Path(__file__).resolve().parents[2] / "e2e"
if E2E.is_dir() and str(E2E) not in sys.path:
    sys.path.insert(0, str(E2E))

api_v2 = pytest.importorskip(
    "utils.api_v2", reason="needs the e2e tree on sys.path")

CLUSTER = "11111111-1111-1111-1111-111111111111"
POOL_A = "22222222-2222-2222-2222-222222222222"
POOL_B = "33333333-3333-3333-3333-333333333333"
VOL_A = "44444444-4444-4444-4444-444444444444"
VOL_B = "55555555-5555-5555-5555-555555555555"
SNAP_A = "66666666-6666-6666-6666-666666666666"
NODE_A = "77777777-7777-7777-7777-777777777777"
NODE_B = "88888888-8888-8888-8888-888888888888"
DEV_A = "99999999-9999-9999-9999-999999999999"


class Recorder:
    """A fake v2 transport: records requests, answers from canned data."""

    def __init__(self, overrides=None):
        self.calls = []
        self.overrides = overrides or {}

    def __call__(self, verb, path, body=None, query=None, **kwargs):
        self.calls.append({"verb": verb, "path": path, "body": body,
                           "query": query})
        key = (verb, path)
        if key in self.overrides:
            return self.overrides[key]

        if verb == "GET":
            if path == f"/clusters/{CLUSTER}/storage-pools/":
                return [
                    {"id": POOL_A, "name": "pool_a", "status": "active",
                     "max_size": 100},
                    {"id": POOL_B, "name": "pool_b", "status": "active"},
                ]
            if path == f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/":
                return [{"id": VOL_A, "name": "lvol_a", "status": "online",
                         "storage_node_id": NODE_A, "cloned_from": None}]
            if path == f"/clusters/{CLUSTER}/storage-pools/{POOL_B}/volumes/":
                return [{"id": VOL_B, "name": "lvol_b", "status": "online",
                         "storage_node_id": NODE_B, "cloned_from": SNAP_A}]
            if path == f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/snapshots/":
                return [{"id": SNAP_A, "name": "snap_a",
                         "lvol": {"id": VOL_A, "name": "lvol_a"}}]
            if path == f"/clusters/{CLUSTER}/storage-pools/{POOL_B}/snapshots/":
                return []
            if path == f"/clusters/{CLUSTER}/storage-nodes/":
                return [
                    {"id": NODE_A, "status": "online", "hostname": "node-a",
                     "lvols": 0, "lvols_max": 50, "snapshots_max": 500,
                     "device_count": 2, "mgmt_ip": "10.0.0.10",
                     "rpc_port": 8080, "health_check": True,
                     "secondary_node_id": NODE_B},
                    {"id": NODE_B, "status": "online", "hostname": "node-b",
                     "lvols": 3, "lvols_max": 50, "snapshots_max": 500,
                     "device_count": 2, "mgmt_ip": "10.0.0.11",
                     "rpc_port": 8080, "health_check": True},
                ]
            if path.endswith("/devices/") and "storage-nodes" in path:
                node = path.split("/storage-nodes/")[1].split("/")[0]
                if node == NODE_A:
                    return [{"id": DEV_A, "status": "online",
                             "health_check": True, "cluster_device_order": 0,
                             "size": 100, "io_error": False,
                             "pcie_address": "0000:01:00.0"}]
                return []
            if path.endswith("/nics"):
                return [{"ID": "nic-1", "Device name": "eth0",
                         "Address": "10.0.0.10", "Net type": "tcp",
                         "Status": "online"}]
        return None

    def requests(self, verb=None):
        return [c for c in self.calls
                if verb is None or c["verb"] == verb]

    def last(self):
        return self.calls[-1]

    def find(self, verb, path):
        for call in self.calls:
            if call["verb"] == verb and call["path"] == path:
                return call
        raise AssertionError(
            f"no {verb} {path} was issued; got "
            + ", ".join(f"{c['verb']} {c['path']}" for c in self.calls))


@pytest.fixture
def router():
    recorder = Recorder()
    logger = logging.getLogger("test_api_v2_router")
    logger.addHandler(logging.NullHandler())
    return api_v2.V2Router(CLUSTER, recorder, logger), recorder


# ── the response contract ────────────────────────────────────────────────

def test_response_keeps_the_v1_envelope(router):
    r, _ = router
    out = r.dispatch("GET", "/pool")
    assert set(out) == {"status", "results"}
    assert out["status"] is True


def test_v1_field_names_are_added_not_replaced(router):
    """Both spellings leave the router, so no call site has to be found."""
    r, _ = router
    pool = r.dispatch("GET", "/pool")["results"][0]
    assert pool["id"] == pool["uuid"] == POOL_A
    assert pool["name"] == pool["pool_name"] == "pool_a"

    volume = r.dispatch("GET", "/lvol")["results"][0]
    assert volume["id"] == volume["uuid"] == VOL_A
    assert volume["name"] == volume["lvol_name"] == "lvol_a"
    assert volume["node_id"] == volume["storage_node_id"] == NODE_A


def test_detail_routes_still_answer_with_a_one_element_list(router):
    """v1 returned node/lvol/pool detail wrapped in a list.

    ``get_storage_node_details`` and ``get_lvol_details`` both index ``[0]``
    into it, in 250-odd places. Dropping the wrapper would be a TypeError at
    every one of them.
    """
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}/")] = {
        "id": NODE_A, "status": "online", "mgmt_ip": "10.0.0.10"}
    node = r.dispatch("GET", f"/storagenode/{NODE_A}")["results"]
    assert isinstance(node, list) and node[0]["status"] == "online"


# ── nesting: the structural change ───────────────────────────────────────

def test_volume_is_addressed_through_its_pool(router):
    """v2 has no flat volume route; the pool has to be resolved first."""
    r, rec = router
    r.dispatch("GET", f"/lvol/{VOL_B}")
    rec.find("GET",
             f"/clusters/{CLUSTER}/storage-pools/{POOL_B}/volumes/{VOL_B}/")


def test_volume_list_fans_out_over_every_pool(router):
    """There is no cluster-wide volume list in v2."""
    r, rec = router
    volumes = r.dispatch("GET", "/lvol")["results"]
    assert {v["id"] for v in volumes} == {VOL_A, VOL_B}
    rec.find("GET", f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/")
    rec.find("GET", f"/clusters/{CLUSTER}/storage-pools/{POOL_B}/volumes/")


def test_volume_record_carries_its_pool(router):
    """v1's lvol record had pool_uuid on it; code reads it to group lvols."""
    r, _ = router
    by_id = {v["id"]: v for v in r.dispatch("GET", "/lvol")["results"]}
    assert by_id[VOL_A]["pool_uuid"] == POOL_A
    assert by_id[VOL_B]["pool_name"] == "pool_b"


def test_pool_index_is_reused_not_rebuilt_per_volume(router):
    """The fan-out is bounded by pools, not by volumes."""
    r, rec = router
    r.dispatch("GET", "/lvol")
    before = len(rec.requests("GET"))
    r.dispatch("GET", f"/lvol/{VOL_A}")
    # One request: the detail. The pool was already known from the listing.
    assert len(rec.requests("GET")) == before + 1


def test_device_is_addressed_through_its_node(router):
    r, rec = router
    r.dispatch("GET", f"/device/{DEV_A}")
    rec.find("GET", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}"
                    f"/devices/{DEV_A}/")


def test_snapshot_is_addressed_through_its_pool(router):
    r, rec = router
    r.dispatch("DELETE", f"/snapshot/{SNAP_A}")
    rec.find("DELETE",
             f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/snapshots/{SNAP_A}/")


def test_an_unresolvable_volume_raises_rather_than_guessing(router):
    r, _ = router
    with pytest.raises(api_v2.V2TranslationError):
        r.dispatch("GET", "/lvol/aaaaaaaa-0000-0000-0000-000000000000")


def test_an_unmapped_endpoint_raises(router):
    r, _ = router
    with pytest.raises(api_v2.V2TranslationError):
        r.dispatch("GET", "/caching-node")


# ── verb and body translation ────────────────────────────────────────────

def test_volume_create_resolves_the_pool_name(router):
    """v1 named the pool in the body; v2 puts its id in the path."""
    r, rec = router
    r.dispatch("POST", "/lvol", {
        "name": "lvol_new", "size": "10G", "pool": "pool_b",
        "max_rw_iops": "0", "fabric": "tcp",
    })
    call = rec.find("POST",
                    f"/clusters/{CLUSTER}/storage-pools/{POOL_B}/volumes/")
    assert call["body"]["name"] == "lvol_new"
    assert "pool" not in call["body"]
    # v1 sent the limits as strings; v2's Unsigned wants a number.
    assert call["body"]["max_rw_iops"] == 0
    assert call["query"] == {"response-format": "identifier"}


def test_volume_create_drops_parameters_v2_does_not_accept(router):
    """bs / chunk_bs are cluster properties in v2; sending them is a 422."""
    r, rec = router
    r.dispatch("POST", "/lvol", {
        "name": "lvol_new", "size": "10G", "pool": "pool_a",
        "bs": "4096", "chunk_bs": "4096", "ndcs": 2, "npcs": 2,
    })
    body = rec.find(
        "POST", f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/")["body"]
    assert "bs" not in body and "chunk_bs" not in body
    assert body["ndcs"] == 2 and body["npcs"] == 2


def test_crypto_is_renamed_to_encrypt(router):
    r, rec = router
    r.dispatch("POST", "/lvol", {"name": "x", "size": "1G",
                                 "pool": "pool_a", "crypto": True})
    body = rec.find(
        "POST", f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/")["body"]
    assert body["encrypt"] is True and "crypto" not in body


def test_resize_goes_through_the_update_route(router):
    """v1 had /lvol/resize/{id}; v2 resizes by PUTting a new size."""
    r, rec = router
    r.dispatch("PUT", f"/lvol/resize/{VOL_A}", {"size": "20G"})
    call = rec.find(
        "PUT", f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/{VOL_A}/")
    assert call["body"] == {"size": "20G"}


def test_snapshot_create_hangs_off_the_volume(router):
    r, rec = router
    r.dispatch("POST", "/snapshot", {"lvol_id": VOL_A,
                                     "snapshot_name": "snap_new"})
    call = rec.find("POST", f"/clusters/{CLUSTER}/storage-pools/{POOL_A}"
                            f"/volumes/{VOL_A}/snapshots")
    assert call["body"] == {"name": "snap_new"}


def test_clone_is_a_volume_create_not_a_snapshot_route(router):
    """v2 clones a snapshot by creating a volume with a clone body.

    ``POST …/volumes/{id}/clone`` exists but is a different feature -- it
    clones a live volume. Routing /snapshot/clone there would silently clone
    the wrong object.
    """
    r, rec = router
    r.dispatch("POST", "/snapshot/clone", {"snapshot_id": SNAP_A,
                                           "clone_name": "clone_1"})
    call = rec.find("POST",
                    f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/volumes/")
    assert call["body"] == {"name": "clone_1", "snapshot_id": SNAP_A}


def test_node_restart_moves_the_id_from_body_to_path(router):
    """v1 PUT a collection route with the node id in the body."""
    r, rec = router
    r.dispatch("PUT", "/storagenode/restart/", {"uuid": NODE_A, "force": True})
    call = rec.find(
        "POST", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}/restart")
    assert call["body"] == {"force": True}


def test_node_shutdown_is_a_post_with_force_as_a_query(router):
    r, rec = router
    r.dispatch("GET", f"/storagenode/shutdown/{NODE_A}?force=true")
    call = rec.find(
        "POST", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}/shutdown")
    assert call["query"] == {"force": "true"}


def test_cluster_activate_is_a_post(router):
    r, rec = router
    r.dispatch("PUT", f"/cluster/activate/{CLUSTER}")
    rec.find("POST", f"/clusters/{CLUSTER}/activate")


def test_device_remove_is_a_post(router):
    """v1 removed a device with a GET, which v2 rightly does not allow."""
    r, rec = router
    r.dispatch("GET", f"/device/remove/{DEV_A}")
    rec.find("POST", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}"
                     f"/devices/{DEV_A}/remove")


def test_pool_host_removal_moves_the_nqn_into_the_body(router):
    """v1 put the NQN in the path; v2's route takes PoolHostParams."""
    r, rec = router
    nqn = "nqn.2014-08.org.nvmexpress:uuid:abc"
    r.dispatch("DELETE", f"/pool/{POOL_A}/host/{nqn}")
    call = rec.find("DELETE",
                    f"/clusters/{CLUSTER}/storage-pools/{POOL_A}/host")
    assert call["body"] == {"host_nqn": nqn}


def test_pool_create_renames_and_numbers_its_limits(router):
    r, rec = router
    r.dispatch("POST", "/pool", {"name": "p", "max_rw_iops": "5",
                                 "cluster_id": CLUSTER, "dhchap": True})
    call = rec.find("POST", f"/clusters/{CLUSTER}/storage-pools/")
    assert call["body"]["max_rw_iops"] == 5
    assert call["body"]["dhchap"] is True
    assert call["query"] == {"response-format": "full"}


def test_management_nodes_take_the_cluster_as_a_query(router):
    """/management-nodes sits at the API root, not under the cluster."""
    r, rec = router
    r.dispatch("GET", "/mgmtnode/")
    call = rec.find("GET", "/management-nodes/")
    assert call["query"] == {"cluster_id": CLUSTER}


def test_history_window_becomes_a_query_parameter(router):
    r, rec = router
    r.dispatch("GET", f"/cluster/capacity/{CLUSTER}/history/1d12h")
    call = rec.find("GET", f"/clusters/{CLUSTER}/capacity")
    assert call["query"]["history"] == "1d12h"


def test_cluster_logs_limit_zero_becomes_a_real_bound(router):
    """v1 read limit=0 as "everything"; v2's limit is an actual limit."""
    r, rec = router
    r.dispatch("GET", f"/cluster/get-logs/{CLUSTER}?limit=0")
    call = rec.find("GET", f"/clusters/{CLUSTER}/logs")
    assert call["query"]["limit"] != "0"


def test_iostats_keeps_the_stats_key_callers_read(router):
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/iostats")] = [{"read_bytes": 1}]
    out = r.dispatch("GET", f"/cluster/iostats/{CLUSTER}")
    assert out["results"]["stats"] == [{"read_bytes": 1}]


# ── fields v2 dropped ────────────────────────────────────────────────────

def test_is_secondary_node_is_derived_from_the_relationship(router):
    """``StorageNodeDTO`` has no such flag; v2 carries secondary_node_id.

    31 call sites read it, and HA node selection across the stress suite
    turns on it, so it has to be right: a node is a secondary when another
    node names it as theirs.
    """
    r, _ = router
    nodes = {n["id"]: n for n in r.dispatch("GET", "/storagenode")["results"]}
    assert nodes[NODE_B]["is_secondary_node"] is True
    assert nodes[NODE_A]["is_secondary_node"] is False


def test_node_config_fields_are_present_so_callers_do_not_keyerror(router):
    """v2 reports node state only.

    The add-node tests read these off a sampled node and pass them straight
    back into add_storage_node. They are subscripted, not ``.get``, so an
    absent key is a KeyError three files deep rather than a clear failure.
    """
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}/")] = {
        "id": NODE_A, "status": "online", "lvols_max": 50}
    node = r.dispatch("GET", f"/storagenode/{NODE_A}")["results"][0]
    for field in ("spdk_image", "max_prov", "num_partitions_per_dev",
                  "enable_ha_jm", "enable_test_device", "jm_percent",
                  "iobuf_small_pool_count", "iobuf_large_pool_count"):
        assert field in node, f"{field} would KeyError in the add-node tests"
    # max_lvol is the one of that family v2 does report, under a new name.
    assert node["max_lvol"] == 50


def test_data_nics_is_rebuilt_from_the_nics_route(router):
    """v2 serves NICs on their own route, display-cased for a table."""
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/storage-nodes/{NODE_A}/")] = {
        "id": NODE_A, "status": "online", "mgmt_ip": "10.0.0.10"}
    node = r.dispatch("GET", f"/storagenode/{NODE_A}")["results"][0]
    assert node["data_nics"][0]["if_name"] == "eth0"
    assert node["data_nics"][0]["ip4_address"] == "10.0.0.10"


def test_cluster_status_is_still_the_device_table(router):
    """v1's /cluster/status returns per-device rows, not the cluster state.

    ``cluster_ops.get_cluster_status`` builds one row per device across every
    node, keyed ``UUID`` / ``Status`` / ``Health``. v2 has no such route, so
    the router rebuilds it -- and anything reading ``row["Status"]`` keeps
    working. Worth knowing when reading a caller that treats this as the
    cluster's own status: it never was.
    """
    r, _ = router
    rows = r.dispatch("GET", f"/cluster/status/{CLUSTER}")["results"]
    assert rows and set(rows[0]) >= {"UUID", "Status", "Health", "Hostname"}
    assert rows[0]["UUID"] == DEV_A


# ── passthrough ──────────────────────────────────────────────────────────

def test_a_v2_native_path_is_passed_through_untouched(router):
    """SbcliUtilsV2 and the parity audit build v2 paths themselves."""
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/tasks")] = [{"id": "t"}]
    out = r.dispatch("GET", f"/clusters/{CLUSTER}/tasks")
    # Raw, not enveloped: such a caller expects v2's own shape.
    assert out == [{"id": "t"}]


def test_task_records_keep_the_fields_the_waiters_filter_on(router):
    """``wait_migration_tasks_complete`` filters on function_name + status.

    If either went missing the filter would match nothing, the waiter would
    return at once and report a migration complete that had not started --
    a silent pass, which is the failure mode this whole file exists to
    prevent. ``node_id`` was renamed to ``storage_node_id`` in TaskDTO.
    """
    r, rec = router
    rec.overrides[("GET", f"/clusters/{CLUSTER}/tasks/")] = [{
        "id": "aaaa", "status": "running", "canceled": False,
        "function_name": "failed_device_migration",
        "storage_node_id": NODE_A,
    }]
    task = r.dispatch("GET", f"/cluster/get-tasks/{CLUSTER}?limit=0")[
        "results"][0]
    assert task["function_name"] == "failed_device_migration"
    assert task["status"] == "running"
    assert task["node_id"] == NODE_A
    assert task["uuid"] == "aaaa"
