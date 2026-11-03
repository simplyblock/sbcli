"""Merge-free landing of delta transfers (SPDK ``cow_landing``).

A delta transfer writes only the dirty ranges of a partly covered cluster. The
source used to have the destination pre-allocate every such cluster, and a
merge job (bdev_lvol_merge_base) then filled the rest of each from the landing
volume's base -- 7.25 GB and 66 s per cycle on bf11 for ~0.5 GB of delta. With
page copy on the destination, the first write into an unallocated cluster of a
landing volume that is already chained to its base copies the cluster from the
base by metadata, so the pre-allocation and the merge are not needed.

These tests pin when the control plane asks for that, that the finish skips
the merge exactly then, and that an SPDK without the parameter falls back to
the merge path.
"""
import pytest

from simplyblock_core.rpc_client import RPCClient, RPCRemoteError
from simplyblock_core.services import snapshot_replication as sr

# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------

class _Task:
    def __init__(self, **params):
        self.uuid = "TASK"
        self.function_params = dict(params)

    def write_to_db(self):
        return True


class _LV:
    top_bdev = "LVS_T/REP_LV"

    def __init__(self, uuid="REP_LV"):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


class _Node:
    def __init__(self, uuid="N_TGT", cluster_id="C_TGT", rpc=None):
        self.uuid = uuid
        self.cluster_id = cluster_id
        self._rpc = rpc

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return self._rpc


class _Cluster:
    def __init__(self, page_copy):
        self.page_copy = page_copy


class _DB:
    def __init__(self, page_copy=True):
        self.cluster = _Cluster(page_copy)

    def get_cluster_by_id(self, cid):
        return self.cluster


@pytest.fixture
def page_copy_db(monkeypatch):
    db = _DB(page_copy=True)
    monkeypatch.setattr(sr, "db", db)
    return db


def _eligible_task(**extra):
    p = {"allow_partial_landing": "REP_LV", "prechained_node_ids": ["N_TGT", "N_TGT_SEC"]}
    p.update(extra)
    return _Task(**p)


def _decide(task, allow_partial=True, partial_base=42, offset=0, lv=None, node=None):
    return sr._cow_landing_decision(task, lv or _LV(), node or _Node(), allow_partial,
                                    partial_base, offset)


# --------------------------------------------------------------------------
# when cow_landing is requested
# --------------------------------------------------------------------------

def test_requested_for_a_fresh_prechained_delta_on_a_page_copy_target(page_copy_db):
    assert _decide(_eligible_task()) is True


def test_not_requested_without_page_copy_on_the_target(page_copy_db):
    page_copy_db.cluster.page_copy = False
    assert _decide(_eligible_task()) is False


def test_not_requested_for_a_full_transfer(page_copy_db):
    assert _decide(_eligible_task(), allow_partial=False) is False
    assert _decide(_eligible_task(), partial_base=0) is False


def test_not_requested_when_the_landing_is_not_prechained(page_copy_db):
    # chained on the secondary only, or for a different landing volume
    assert _decide(_eligible_task(prechained_node_ids=["N_TGT_SEC"])) is False
    assert _decide(_eligible_task(allow_partial_landing="OTHER_LV")) is False


def test_not_requested_when_the_chain_is_deferred_to_the_finish(page_copy_db):
    """A landing volume chained only after the transfer would copy zeroes."""
    assert _decide(_eligible_task(chain_deferred_to="SNAP_1")) is False


def test_not_requested_for_a_resumed_transfer(page_copy_db):
    assert _decide(_eligible_task(), offset=128) is False


def test_not_requested_for_a_second_attempt_into_the_same_landing(page_copy_db):
    """An earlier attempt may have pre-allocated and partly filled clusters."""
    assert _decide(_eligible_task(xfer_landing_submitted="REP_LV")) is False
    # a fresh landing volume after a discard is a first attempt again
    assert _decide(_eligible_task(xfer_landing_submitted="OLD_LV", xfer_submit_t=1.0)) is True


def test_not_requested_after_an_attempt_by_an_older_control_plane(page_copy_db):
    assert _decide(_eligible_task(xfer_submit_t=1.0)) is False


def test_unreadable_target_cluster_falls_back_to_the_merge(monkeypatch):
    class _Broken:
        def get_cluster_by_id(self, cid):
            raise KeyError(cid)
    monkeypatch.setattr(sr, "db", _Broken())
    assert _decide(_eligible_task()) is False


# --------------------------------------------------------------------------
# the finish skips the merge exactly when the landing was merge-free
# --------------------------------------------------------------------------

class _MergeRPC:
    def __init__(self):
        self.calls = []

    def bdev_lvol_merge_base_stat(self, name):
        self.calls.append(("stat", name))
        return {"state": "none"}

    def bdev_lvol_merge_base(self, name, priority_class=None):
        self.calls.append(("merge", name))
        return {"state": "done", "ms": 1}


def test_merge_is_skipped_for_a_cow_landing(monkeypatch):
    rpc = _MergeRPC()
    task = _Task(partial_base=42, cow_landing="REP_LV")
    assert sr._merge_landing(task, _LV(), _Node(rpc=rpc)) == "ok"
    assert rpc.calls == []


def test_merge_runs_when_cow_landing_names_another_landing(monkeypatch):
    monkeypatch.setattr(sr, "_replication_class_on", lambda cid: False)
    monkeypatch.setattr(sr.xfer_timing, "stamp", lambda *a, **k: None)
    rpc = _MergeRPC()
    task = _Task(partial_base=42, cow_landing="OLD_LV")
    assert sr._merge_landing(task, _LV(), _Node(rpc=rpc)) == "ok"
    assert ("merge", "LVS_T/REP_LV") in rpc.calls


def test_discarded_landing_forgets_cow_landing(monkeypatch):
    monkeypatch.setattr(sr.lvol_controller, "delete_lvol", lambda *a, **k: True)
    seen = {}
    monkeypatch.setattr(sr, "_suspend_for_retry",
                        lambda task, msg, **k: seen.update(task.function_params))
    task = _Task(cow_landing="REP_LV", merged_landing="REP_LV")
    sr._discard_landing_for_retry(task, _LV(), "x")
    assert "cow_landing" not in seen


# --------------------------------------------------------------------------
# rpc_client: cow_landing on the wire, and the fallback for an older SPDK
# --------------------------------------------------------------------------

class _C(RPCClient):
    def __init__(self, refuse=()):
        self.host, self.port = "h", 0
        self.sent = []
        self._refuse = set(refuse)

    def _request(self, method, **kwargs):
        self.sent.append(kwargs)
        if self._refuse & set(kwargs):
            raise RPCRemoteError("Invalid parameters: failed to decode", -32602)
        return True


def _transfer(c, **kw):
    applied = {}
    ret = c.bdev_lvol_transfer(name="LVS/SNAP_2", offset=0, batch_size=256, bdev_name="hub0",
                               operation="replicate", lvol_id=7, allow_partial=True,
                               partial_base=42, priority_class=7, applied=applied, **kw)
    return ret, applied


def test_cow_landing_is_sent_with_the_delta():
    c = _C()
    ret, applied = _transfer(c, cow_landing=True)
    assert ret is True
    assert c.sent[-1]["cow_landing"] is True
    assert c.sent[-1]["allow_partial"] is True and c.sent[-1]["partial_base"] == 42
    assert applied == {"cow_landing": True}


def test_cow_landing_is_not_sent_unless_asked():
    c = _C()
    _, applied = _transfer(c)
    assert "cow_landing" not in c.sent[-1]
    assert applied == {"cow_landing": False}


def test_cow_landing_is_never_sent_without_a_delta():
    c = _C()
    applied = {}
    c.bdev_lvol_transfer(name="LVS/SNAP_2", offset=0, batch_size=16, bdev_name="hub0",
                         operation="replicate", lvol_id=7, cow_landing=True, applied=applied)
    assert all("cow_landing" not in s for s in c.sent)
    assert applied == {"cow_landing": False}


def test_older_spdk_falls_back_to_the_delta_with_merge():
    c = _C(refuse={"cow_landing"})
    ret, applied = _transfer(c, cow_landing=True)
    assert ret is True
    assert len(c.sent) == 2
    assert "cow_landing" not in c.sent[-1] and c.sent[-1]["allow_partial"] is True
    assert applied == {"cow_landing": False}


def test_non_decode_errors_are_not_swallowed():
    class _Boom(_C):
        def _request(self, method, **kwargs):
            raise RPCRemoteError("No such device", -19)
    with pytest.raises(RPCRemoteError):
        _transfer(_Boom(), cow_landing=True)
