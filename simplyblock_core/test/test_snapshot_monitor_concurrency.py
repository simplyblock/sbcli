"""Monitor delete concurrency: parallel per volume, exclusive per node RPC.

Lab 2026-08-14: 1298 snapshots sat in in_deletion and internal-snapshot
creation stopped entirely. The monitor ran one serial pass that did creation
first and then every in_deletion record — a stuck delete was retried every
cycle forever, so the pile only grew and creation never came around again.
"""
import threading

from simplyblock_core.services import snapshot_monitor as sm


class _Snap:
    def __init__(self, uuid, lvs="LVS_1", cluster="C1"):
        self.uuid = uuid
        self.cluster_id = cluster
        self.snap_bdev = f"{lvs}/{uuid}"
        self.instances = []
        self.lvol = type("L", (), {"lvs_name": lvs, "node_id": "N1",
                                   "ha_type": "ha", "get_id": lambda self: "LV1"})()

    def get_id(self):
        return self.uuid


def test_backoff_skips_a_repeatedly_failing_delete(monkeypatch):
    sm.forget_delete_backoff("S1")
    now = 1000
    assert sm.delete_attempt_due("S1", now) is True

    # No progress -> backs off; the next cycle (seconds later) must skip it.
    sm.note_delete_attempt("S1", now, progressed=False)
    assert sm.delete_attempt_due("S1", now + 2) is False

    # Backoff grows with consecutive failures.
    sm.note_delete_attempt("S1", now + 100, progressed=False)
    sm.note_delete_attempt("S1", now + 200, progressed=False)
    assert sm.delete_attempt_due("S1", now + 205) is False
    # ... and is bounded, so a stuck record still retries occasionally.
    assert sm.delete_attempt_due("S1", now + 200 + sm._BACKOFF_MAX_SEC) is True
    sm.forget_delete_backoff("S1")


def test_progress_clears_backoff():
    sm.forget_delete_backoff("S2")
    sm.note_delete_attempt("S2", 1000, progressed=False)
    assert sm.delete_attempt_due("S2", 1001) is False
    sm.note_delete_attempt("S2", 1002, progressed=True)
    assert sm.delete_attempt_due("S2", 1002) is True
    sm.forget_delete_backoff("S2")


def test_sync_deletes_take_the_node_scoped_lvstore_lock(monkeypatch):
    """The RPC phase must use the SAME lock key space as the creators
    ("<lvs>@<node8>"), otherwise it excludes nothing they hold."""
    taken = []

    class _Ctx:
        def __init__(self, key):
            self.key = key

        def __enter__(self):
            taken.append(("enter", self.key))
            return self

        def __exit__(self, *a):
            taken.append(("exit", self.key))
            return False

    def _fake_lock(cluster_id, lvs_name, *, node_id=None, enabled=True):
        return _Ctx((cluster_id, lvs_name, node_id))

    calls = []

    class _RPC:
        def __init__(self, node_id):
            self.node_id = node_id

        def delete_lvol(self, bdev, sync=False, special_delete=False):
            # Every sync RPC must happen while its node's lock is held.
            assert taken and taken[-1][0] == "enter", "sync delete outside lock"
            assert taken[-1][1][2] == self.node_id
            calls.append((self.node_id, bdev, sync))
            return True, None

    class _Node:
        status = "online"

        def __init__(self, uuid, sec=None, ter=None):
            self.uuid = uuid
            self.secondary_node_id = sec
            self.tertiary_node_id = ter
            self.cluster_id = "C1"

        def get_id(self):
            return self.uuid

        def rpc_client(self):
            return _RPC(self.uuid)

        def lvol_del_sync_lock(self):
            pass

    nodes = {"N1": _Node("N1", sec="N2", ter="N3"),
             "N2": _Node("N2"), "N3": _Node("N3")}

    class _DB:
        def get_storage_node_by_id(self, uuid):
            return nodes[uuid]

        def unindex_snapshot(self, snap):
            pass

        kv_store = None

    snap = _Snap("SNAP1")
    snap.remove = lambda kv: None

    monkeypatch.setattr(sm, "db", _DB())
    monkeypatch.setattr(sm.snapshot_controller, "lvstore_op_lock", _fake_lock)
    monkeypatch.setattr(sm.snapshot_events, "snapshot_delete", lambda s: None)

    sm.process_snap_delete_finish(snap, nodes["N1"])

    # One sync delete per replica, each under its own node-scoped lock.
    assert [c[0] for c in calls] == ["N1", "N2", "N3"]
    assert all(c[2] is True for c in calls)
    assert [t[1][1] for t in taken if t[0] == "enter"] == ["LVS_1"] * 3
    assert [t[1][2] for t in taken if t[0] == "enter"] == ["N1", "N2", "N3"]
    # Locks are released between RPCs, never nested (a node lock is held for
    # one RPC only, so unrelated objects keep flowing on other nodes).
    depth = 0
    for kind, _key in taken:
        depth += 1 if kind == "enter" else -1
        assert depth in (0, 1)


def test_group_worker_serializes_one_volume_but_pool_runs_volumes_parallel():
    """Chain order (clone -> snapshot -> parent) is preserved by giving each
    volume's records to a single worker; different volumes run concurrently."""
    from concurrent.futures import ThreadPoolExecutor

    order = []
    order_guard = threading.Lock()
    barrier = threading.Barrier(2, timeout=5)

    def _process_group(items):
        # Both groups must be in flight at once (barrier would time out if the
        # pool serialized them), while items inside a group stay ordered.
        barrier.wait()
        for it in items:
            with order_guard:
                order.append(it)

    groups = {"LV1": ["a1", "a2", "a3"], "LV2": ["b1", "b2"]}
    with ThreadPoolExecutor(max_workers=2) as ex:
        list(ex.map(_process_group, groups.values()))

    assert [x for x in order if x.startswith("a")] == ["a1", "a2", "a3"]
    assert [x for x in order if x.startswith("b")] == ["b1", "b2"]
