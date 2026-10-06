"""A snapshot can only be replicated forward if its volume has a destination.

replication_backlog walks the clone ancestry ACROSS volumes. On a failed-over
volume that ancestry runs into the target-side REP_* receiving volumes, which
exist only to receive a transfer and therefore carry replication_node_id="" and
do_replicate=False. Queueing forward tasks for them produced tasks that could
never resolve a destination node — 330 x "StorageNode lookup with a blank id" in
lab 2026-08-19, the same bug that used to surface as "Multiple values present" —
and because the runner retried them for ever they starved replication and wedged
every volume delete waiting behind their snapshots (15 volumes stuck
in_deletion, 83 x "Snapshot is in deletion").

Two defences: do not enqueue such a task, and if one exists anyway, end it
instead of retrying.
"""
import inspect
from types import SimpleNamespace

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr


def _lvol(uuid, repl_node=""):
    lv = LVol()
    lv.uuid = uuid
    lv.replication_node_id = repl_node
    lv.node_id = "N_HOST"
    return lv


def _snap(uuid, lvol, data_uuid="D1"):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.data_uuid = data_uuid
    s.target_replicated_snap_uuid = ""
    return s


class _Task:
    def __init__(self):
        self.function_params = {"replicate_to_source": False, "snapshot_id": "S1"}
        self.node_id = "N_HOST"
        self.status = JobSchedule.STATUS_NEW
        self.function_result = ""
        self.retry = 0
        self.written = 0

    def write_to_db(self, kv=None):
        self.written += 1


def test_backlog_skips_snapshots_whose_volume_has_no_destination():
    """Guard rail on the enqueue side: the source of the 330 doomed tasks."""
    src = inspect.getsource(lvol_controller.replication_start)
    assert "if not snap.lvol.replication_node_id:" in src, \
        "backlog enqueue must skip volumes without a replication destination"
    # the skip has to come before the task is created
    skip_at = src.index("if not snap.lvol.replication_node_id:")
    enqueue_at = src.index("add_snapshot_replication_task(snap.cluster_id")
    assert skip_at < enqueue_at


def test_undeliverable_forward_task_is_ended_not_retried(monkeypatch):
    """A task for a destination-less volume must be completed, so it cannot
    starve the runner or block deletes behind its snapshot."""
    task = _Task()
    snap = _snap("S1", _lvol("REP_LV", repl_node=""))

    # A blank id must never reach the node lookup.
    def _boom(_id):
        raise AssertionError("looked up a storage node with a blank id")

    monkeypatch.setattr(sr.db, "get_storage_node_by_id", _boom)
    monkeypatch.setattr(sr, "_select_source_node",
                        lambda s, exclude=(): (SimpleNamespace(get_id=lambda: "N_SRC"), "primary"))
    # The chain-completeness gate has its own suite
    # (test_replication_chain_completeness); this test is about the
    # destination guard behind it.
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor",
                        lambda snode, snapshot, to_source: ("ok", None, ""))

    sr.process_snap_replicate_start(task, snap)

    assert task.status == JobSchedule.STATUS_DONE, "the task must end, not retry"
    assert "no replication destination" in task.function_result
    assert task.retry == 0, "ending a doomed task must not consume the retry budget"


def test_volume_with_a_destination_is_still_processed(monkeypatch):
    """The guard must not swallow legitimate forward replication."""
    task = _Task()
    snap = _snap("S1", _lvol("LV1", repl_node="N_DEST"))
    reached = {}

    def _node(node_id):
        reached["node"] = node_id
        raise RuntimeError("stop here — past the guard is all this test needs")

    monkeypatch.setattr(sr.db, "get_storage_node_by_id", _node)
    monkeypatch.setattr(sr, "_select_source_node",
                        lambda s, exclude=(): (SimpleNamespace(get_id=lambda: "N_SRC"), "primary"))
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor",
                        lambda snode, snapshot, to_source: ("ok", None, ""))
    try:
        sr.process_snap_replicate_start(task, snap)
    except RuntimeError:
        pass
    assert reached.get("node") == "N_DEST"
    assert task.status != JobSchedule.STATUS_DONE


def test_landing_copy_inherits_source_subsystem_packing(monkeypatch):
    """Regression (2026-09-30): the REP_* landing copy a transfer creates was
    added WITHOUT the source's namespaced/max_namespace_per_subsys, so it
    defaulted to a one-namespace subsystem. Every replicated copy — and every
    clone taken from it (test-failover, fail-over) — then landed in its own
    subsystem at NSID 1, ignoring a max_namespace_per_subsys=10 storage class:
    a namespaced subsystem capped at one namespace can never be joined, so the
    packing silently degenerates to one-subsystem-per-volume. The landing copy
    must carry the source volume's subsystem-packing capacity."""
    lvol = _lvol("LV1", repl_node="N_DEST")
    lvol.max_namespace_per_subsys = 10
    lvol.ha_type = "ha"
    snap = _snap("S1", lvol)
    snap.snap_name = "snap1"
    snap.size = 1073741824
    snap.cluster_id = "CL_SRC"
    task = _Task()

    class _Node:
        cluster_id = "CL_SRC"

        def get_id(self):
            return "N_DEST"

    monkeypatch.setattr(sr, "_select_source_node", lambda s, exclude=(): (_Node(), "primary"))
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor",
                        lambda snode, snapshot, to_source: ("ok", None, ""))
    monkeypatch.setattr(sr.db, "get_storage_node_by_id", lambda _id: _Node())
    monkeypatch.setattr(sr, "_destination_pool_uuid", lambda *a, **k: "POOL_REMOTE")

    def _no_existing(name, include_deleted=False):
        raise KeyError(name)

    monkeypatch.setattr(sr.db, "get_lvol_by_name", _no_existing)

    captured = {}

    def _fake_add(*args, **kwargs):
        captured["kwargs"] = kwargs
        return None, "stop"          # end the function cleanly after the call

    monkeypatch.setattr(sr.lvol_controller, "add_lvol_ha", _fake_add)

    sr.process_snap_replicate_start(task, snap)

    assert captured, "never reached the landing-copy creation"
    kw = captured["kwargs"]
    assert kw.get("max_namespace_per_subsys") == 10, \
        "the landing copy must inherit the source's max_namespace_per_subsys"
    assert kw.get("namespaced") is True, \
        "a source with a shareable subsystem must produce a namespaced landing copy"
