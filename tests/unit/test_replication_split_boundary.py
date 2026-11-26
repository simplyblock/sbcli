"""The replication service never acts on the destination cluster directly.

Snapshot replication runs on the source cluster's control plane. Everything it
does on the destination -- create or clear a landing volume, chain, merge and
convert it, settle or recover the destination's leadership -- goes through the
peer client (simplyblock_core.peer.client), and the destination side runs in
simplyblock_core.peer.service:

* in process (LocalPeerClient) when this control plane manages both clusters;
* over the peer API (HttpPeerClient, /api/v2/_peer) when the destination is a
  peer control plane (the control-plane split), whose nodes and records this
  control plane cannot reach.

A destination-side call written straight into snapshot_replication works in
the first case and every unit test, and fails only across a split. This test
catches it where it is written. Put the operation in peer.service (and the
protocol, if the source must pass something), not here. See
simplyblock_core/services/AGENTS.md, "Replication: the destination goes
through the peer client".
"""

import ast
import inspect

from simplyblock_core.services import snapshot_replication

#: Operations on the destination's storage or records. Their names are enough:
#: the source's own volumes never take these calls in this module.
DESTINATION_OPERATIONS = {
    # the landing volume's lifecycle
    "add_lvol_ha", "delete_lvol", "delete_lvol_from_node", "add_lvol_sync_del_task",
    # the destination's snapshot tree
    "bdev_lvol_add_clone", "bdev_lvol_convert", "bdev_lvol_merge_base",
    "bdev_lvol_merge_base_stat", "add_clone_on", "convert_on", "record_pending",
    "lvstore_replicas",
    # the destination's leadership (is_node_leader is not listed: the source
    # probes its own lvstore with it too)
    "restore_healthy_lvs_leadership",
}

#: Calls that need both clusters' records in one database and run only behind
#: _target_is_local (the destination is managed here): (function, call name).
ALLOWED_WHEN_LOCAL = {
    ("_adopt_spare", "adopt"),
    ("process_snap_replicate_start", "prestage"),
}

#: The modules whose functions act on the destination's records: called here
#: only behind _target_is_local (see ALLOWED_WHEN_LOCAL).
DESTINATION_MODULES = {"replica_chain", "replication_spare"}


def _calls():
    """(enclosing function, called name, receiver name) for every call in the
    module's code; comments and docstrings are not code."""
    tree = ast.parse(inspect.getsource(snapshot_replication))
    out = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                recv = node.func.value
                recv_name = recv.id if isinstance(recv, ast.Name) else ""
                out.append((fn.name, node.func.attr, recv_name))
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                out.append((fn.name, node.func.id, ""))
    return out


def test_no_destination_operation_is_called_from_the_replication_service():
    offending = sorted({(fn, name) for fn, name, _ in _calls()
                        if name in DESTINATION_OPERATIONS})
    assert offending == [], (
        "snapshot_replication acts on the destination directly; put these in "
        f"simplyblock_core.peer.service behind the peer client: {offending}")


def test_destination_modules_are_used_only_where_the_destination_is_local():
    used = sorted({(fn, name) for fn, name, recv in _calls() if recv in DESTINATION_MODULES})
    unexpected = [u for u in used if u not in ALLOWED_WHEN_LOCAL]
    assert unexpected == [], (
        "snapshot_replication calls a module that acts on the destination's records; "
        f"go through the peer client, or guard it with _target_is_local: {unexpected}")


def test_the_allowed_calls_are_guarded_by_target_is_local():
    src = inspect.getsource(snapshot_replication.process_snap_replicate_start)
    for call in ("_adopt_spare(", "replication_spare.prestage("):
        at = src.index(call)
        assert src.rfind("_target_is_local(", 0, at) != -1, call
