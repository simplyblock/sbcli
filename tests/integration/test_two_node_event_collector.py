"""Two-node event collector (docs/design/two-node-arbitration.md, section 4)
against a real FoundationDB. The storage node is mocked: a plain fake RPC
client stands in for the SPDK JSON-RPC endpoint.
"""

import threading
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
