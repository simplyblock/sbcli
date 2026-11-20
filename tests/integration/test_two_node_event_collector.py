"""Two-node event collector (docs/design/two-node-arbitration.md, section 4)
against a real FoundationDB. The storage node is mocked: a plain fake RPC
client stands in for the SPDK JSON-RPC endpoint.
"""

import threading
import time
import uuid as _uuid

import pytest

from simplyblock_core import db_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import two_node_event_collector as collector


class FakeRPC:
    def __init__(self):
        self.ha_status: dict = {}
        self.wait_responses: list[dict] = []

    def jc_wait_events(self, instance, after_seq, timeout_ms=20000, max_events=256):
        if self.wait_responses:
            return self.wait_responses.pop(0)
        return {"instance": instance, "seq": after_seq, "overflow": False, "events": []}

    def jc_ha_status(self):
        return self.ha_status


def _cluster(cid: str, arbitration: bool = True) -> Cluster:
    c = Cluster()
    c.uuid = cid
    c.two_node_arbitration = arbitration
    return c


def _node(nid: str, cid: str) -> StorageNode:
    n = StorageNode()
    n.uuid = nid
    n.cluster_id = cid
    n.status = StorageNode.STATUS_ONLINE
    n.mgmt_ip = "127.0.0.1"
    n.rpc_port = 8080
    return n


@pytest.fixture
def db():
    return db_controller.DBController()


@pytest.fixture
def ids():
    return f"cl-{_uuid.uuid4()}", f"n-{_uuid.uuid4()}"


def test_poll_once_queues_new_events_in_order(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    rpc = FakeRPC()
    rpc.wait_responses = [
        {"instance": "i1", "seq": 2, "overflow": False,
         "events": [{"seq": 1, "status": "ha_solo", "jm_vuid": 3},
                    {"seq": 2, "status": "ha_fenced", "jm_vuid": 3}]},
    ]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 0)
    assert (instance, after_seq) == ("i1", 2)
    events = db.get_arbitration_events(cid)
    assert [e.seq for e in events] == [1, 2]
    assert [e.payload["status"] for e in events] == ["ha_solo", "ha_fenced"]


def test_poll_once_ignores_redelivered_events(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.wait_responses = [{"instance": "i1", "seq": 5, "overflow": False,
                           "events": [{"seq": 5, "status": "ha_solo", "jm_vuid": 3}]}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 5)
    assert after_seq == 5
    assert db.get_arbitration_events(cid) == []


def test_poll_once_empty_answer_advances_to_reported_seq(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.wait_responses = [{"instance": "i1", "seq": 7, "overflow": False, "events": []}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 3)
    assert (instance, after_seq) == ("i1", 7)
    assert db.get_arbitration_events(cid) == []


def test_poll_once_resyncs_on_first_call(db, ids):
    """A collector thread starts with instance="" (nothing seen yet), which
    never matches a node's real instance -- so the very first poll always
    resyncs instead of trickling in from seq 0."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.ha_status = {"instance": "i1", "seq": 3, "lvs": []}
    rpc.wait_responses = [{"instance": "i1", "seq": 2, "overflow": False, "events": []}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "", 0)
    assert (instance, after_seq) == ("i1", 3)
    assert db.get_arbitration_events(cid)[0].payload["reason"] == "instance"


def test_poll_once_resyncs_on_instance_change(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.ha_status = {"instance": "i2", "seq": 9, "lvs": []}
    rpc.wait_responses = [{"instance": "i2", "seq": 1, "overflow": False, "events": []}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 40)
    assert (instance, after_seq) == ("i2", 9)
    events = db.get_arbitration_events(cid)
    assert len(events) == 1
    assert events[0].payload["status"] == collector.STATUS_RESYNC
    assert events[0].payload["reason"] == "instance"


def test_poll_once_resyncs_on_overflow(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.ha_status = {"instance": "i1", "seq": 100, "lvs": []}
    rpc.wait_responses = [{"instance": "i1", "seq": 50, "overflow": True, "events": []}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 10)
    assert (instance, after_seq) == ("i1", 100)
    events = db.get_arbitration_events(cid)
    assert events[0].payload["reason"] == "overflow"


def test_poll_once_superseded_is_a_noop(db, ids):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.wait_responses = [{"superseded": True}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 10)
    assert (instance, after_seq) == ("i1", 10)
    assert db.get_arbitration_events(cid) == []


def test_reconcile_starts_and_stops_collector_threads(db, ids, monkeypatch):
    cid, nid = ids
    cluster = _cluster(cid)
    cluster.write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    started = threading.Event()

    def fake_collect(node_id, cluster_id, stop):
        started.set()
        stop.wait()

    monkeypatch.setattr(collector, "_collect", fake_collect)
    collector._workers.clear()
    try:
        collector._reconcile()
        assert started.wait(timeout=2)
        assert nid in collector._workers

        cluster.two_node_arbitration = False
        cluster.write_to_db(db.kv_store)
        collector._reconcile()
        assert nid not in collector._workers
    finally:
        for thread, stop in collector._workers.values():
            stop.set()
            thread.join(timeout=2)
        collector._workers.clear()


def test_reconcile_skips_a_pacemaker_authority_cluster(db, ids, monkeypatch):
    """Mode pacemaker (docs/design/two-node-kubernetes-fencing.md, section
    4.2): the node agent owns the single jc_wait_events waiter, so the
    collector must leave the cluster alone."""
    cid, nid = ids
    cluster = _cluster(cid)
    cluster.two_node_fencing_authority = "pacemaker"
    cluster.write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    monkeypatch.setattr(collector, "_collect", lambda *a: None)
    collector._workers.clear()
    try:
        collector._reconcile()
        assert nid not in collector._workers
    finally:
        collector._workers.clear()


def test_reconcile_skips_a_removed_node(db, ids, monkeypatch):
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    node = _node(nid, cid)
    node.status = StorageNode.STATUS_REMOVED
    node.write_to_db(db.kv_store)

    monkeypatch.setattr(collector, "_collect", lambda *a: None)
    collector._workers.clear()
    try:
        collector._reconcile()
        assert nid not in collector._workers
    finally:
        collector._workers.clear()


def test_poll_once_store_failure_does_not_advance_seq_and_is_safe_to_retry(db, ids, monkeypatch):
    """A failed ArbitrationEvent write (FDB unavailable, a conflict) must not
    advance after_seq -- the event has to be re-deliverable from the same
    (instance, after_seq) the caller already held, same as Go's
    TestFailedStoreWriteKeepsTheAcknowledgement."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    event = {"seq": 5, "status": "ha_solo", "jm_vuid": 3}
    rpc.wait_responses = [{"instance": "i1", "seq": 5, "overflow": False, "events": [event]}]

    original_queue_event = collector.arbitration_events.queue_event
    monkeypatch.setattr(
        collector.arbitration_events, "queue_event",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("fdb write failed")))
    with pytest.raises(RuntimeError):
        collector._poll_once(rpc, cid, nid, "i1", 0)
    assert db.get_arbitration_events(cid) == []

    # Same (instance, after_seq) as before the failed attempt: the server
    # would redeliver the same event, since nothing acknowledged it.
    monkeypatch.setattr(collector.arbitration_events, "queue_event", original_queue_event)
    rpc.wait_responses = [{"instance": "i1", "seq": 5, "overflow": False, "events": [dict(event)]}]
    instance, after_seq = collector._poll_once(rpc, cid, nid, "i1", 0)
    assert (instance, after_seq) == ("i1", 5)
    events = db.get_arbitration_events(cid)
    assert len(events) == 1
    assert events[0].seq == 5


def test_collect_thread_survives_a_transient_store_write_failure(db, ids, monkeypatch):
    """The collector thread must not die on a store-write error: it should
    back off and retry in place, the same contract a transport error gets,
    rather than rely on the supervisor to notice and restart it from scratch
    on the next reconcile tick."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    class _ResyncRPC:
        """Always reports an instance the worker hasn't seen yet, so every
        poll (while the worker's local ``instance`` stays "") tries a
        resync -- exactly the retry this test needs to observe."""

        def jc_wait_events(self, instance, after_seq, timeout_ms=20000, max_events=256):
            return {"instance": "i1", "seq": 1, "overflow": False, "events": []}

        def jc_ha_status(self):
            return {"instance": "i1", "seq": 1, "lvs": []}

    rpc = _ResyncRPC()
    monkeypatch.setattr(StorageNode, "rpc_client", lambda self, **kw: rpc)

    calls = {"n": 0}
    original_queue_event = collector.arbitration_events.queue_event

    def flaky_queue_event(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("fdb write failed")
        return original_queue_event(*a, **kw)

    monkeypatch.setattr(collector.arbitration_events, "queue_event", flaky_queue_event)
    monkeypatch.setattr(collector, "BACKOFF_MIN_S", 0.01)
    monkeypatch.setattr(collector, "BACKOFF_MAX_S", 0.02)

    stop = threading.Event()
    thread = threading.Thread(target=collector._collect, args=(nid, cid, stop), daemon=True)
    thread.start()
    try:
        for _ in range(200):
            if calls["n"] >= 2:
                break
            time.sleep(0.02)
        assert calls["n"] >= 2, "the collector thread died instead of retrying"
        assert thread.is_alive()
    finally:
        stop.set()
        thread.join(timeout=2)
    events = db.get_arbitration_events(cid)
    assert len(events) == 1
    assert events[0].payload["reason"] == "instance"


def test_reconcile_does_not_restart_an_unchanged_worker(db, ids, monkeypatch):
    """A reconcile tick with no topology change must leave a running worker's
    thread in place -- restarting it would drop its in-memory (instance,
    after_seq) and force a needless resync."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    def fake_collect(node_id, cluster_id, stop):
        stop.wait()

    monkeypatch.setattr(collector, "_collect", fake_collect)
    collector._workers.clear()
    try:
        collector._reconcile()
        assert nid in collector._workers
        thread_before = collector._workers[nid][0]

        collector._reconcile()
        assert collector._workers[nid][0] is thread_before
        assert thread_before.is_alive()
    finally:
        for thread, stop in collector._workers.values():
            stop.set()
            thread.join(timeout=2)
        collector._workers.clear()


def test_collect_picks_up_a_node_address_change_without_a_worker_restart(db, ids, monkeypatch):
    """The worker re-fetches the StorageNode every poll and builds a fresh RPC
    client from it (see _collect), so a changed mgmt_ip takes effect on the
    next poll -- the supervisor does not need to recreate the thread for it."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)

    addresses: list[tuple] = []

    class _SlowRPC:
        def jc_wait_events(self, instance, after_seq, timeout_ms=20000, max_events=256):
            time.sleep(0.02)
            return {"instance": instance, "seq": after_seq, "overflow": False, "events": []}

        def jc_ha_status(self):
            return {}

    def fake_rpc_client(self, **kwargs):
        addresses.append((self.mgmt_ip, self.rpc_port))
        return _SlowRPC()

    monkeypatch.setattr(StorageNode, "rpc_client", fake_rpc_client)
    collector._workers.clear()
    try:
        collector._reconcile()
        assert nid in collector._workers
        thread_before = collector._workers[nid][0]

        for _ in range(200):
            if addresses:
                break
            time.sleep(0.02)
        assert addresses, "the worker never polled"

        node = db.get_storage_node_by_id(nid)
        node.mgmt_ip = "10.0.0.9"
        node.write_to_db(db.kv_store)

        # A reconcile tick with the node's cluster membership unchanged must
        # not touch the running worker.
        collector._reconcile()
        assert collector._workers[nid][0] is thread_before

        for _ in range(200):
            if any(addr == ("10.0.0.9", 8080) for addr in addresses):
                break
            time.sleep(0.02)
        else:
            pytest.fail(f"worker never used the new address: {addresses}")
        assert collector._workers[nid][0] is thread_before
    finally:
        for thread, stop in collector._workers.values():
            stop.set()
            thread.join(timeout=2)
        collector._workers.clear()


def test_arbitration_event_key_matches_the_pinned_fdb_layout(db, ids):
    """Pins the on-disk key format (docs: models/arbitration.py), the same
    guarantee Go's TestEventKeyMatchesThePythonModel gave when both
    implementations wrote the same store -- now that only this model writes
    it, nothing else protects the layout the arbiter's prefix-reads depend on."""
    cid, nid = ids
    _cluster(cid).write_to_db(db.kv_store)
    _node(nid, cid).write_to_db(db.kv_store)
    rpc = FakeRPC()
    rpc.wait_responses = [{"instance": "i1", "seq": 5, "overflow": False,
                           "events": [{"seq": 5, "status": "ha_solo", "jm_vuid": 3}]}]
    collector._poll_once(rpc, cid, nid, "i1", 0)

    expected_key = f"object/ArbitrationEvent/{cid}/{nid}/i1/{5:020d}".encode()
    raw = db.kv_store[expected_key]
    assert raw is not None, "ArbitrationEvent key layout changed"
    import json
    stored = json.loads(bytes(raw))
    assert stored["cluster_id"] == cid
    assert stored["node_id"] == nid
    assert stored["instance"] == "i1"
    assert stored["seq"] == 5

    events = db.get_arbitration_events(cid)
    assert len(events) == 1 and events[0].seq == 5


def test_idle_and_active_rescan_cadence_across_a_topology_change(db, ids, monkeypatch):
    """TWO_NODE_IDLE_RESCAN_S while nothing is arbitrated, TWO_NODE_COLLECTOR_RECONCILE_S
    while a worker runs, and back to idle once arbitration is turned off --
    driven through the real main() loop and a real reconcile against FDB."""
    from simplyblock_core import constants

    cid, nid = ids
    cluster = _cluster(cid, arbitration=False)
    cluster.write_to_db(db.kv_store)

    monkeypatch.setattr(collector, "_collect", lambda *a: None)
    sleeps: list[float] = []

    def fake_sleep(duration):
        sleeps.append(duration)
        if len(sleeps) == 1:
            cluster.two_node_arbitration = True
            cluster.write_to_db(db.kv_store)
            _node(nid, cid).write_to_db(db.kv_store)
        elif len(sleeps) == 2:
            cluster.two_node_arbitration = False
            cluster.write_to_db(db.kv_store)
        elif len(sleeps) >= 3:
            raise SystemExit

    monkeypatch.setattr(collector.time, "sleep", fake_sleep)
    collector._workers.clear()
    try:
        with pytest.raises(SystemExit):
            collector.main()
    finally:
        collector._workers.clear()

    assert sleeps == [
        constants.TWO_NODE_IDLE_RESCAN_S,
        constants.TWO_NODE_COLLECTOR_RECONCILE_S,
        constants.TWO_NODE_IDLE_RESCAN_S,
    ]
